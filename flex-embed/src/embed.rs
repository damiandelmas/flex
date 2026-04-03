//! Nomic embedding pipeline — tokenize, infer, truncate, normalize.
//! Supports both single and batched inference.

use anyhow::Result;
use ort::session::Session;
use ort::value::Tensor;
use std::path::Path;
use tokenizers::Tokenizer;

const MAX_CHARS: usize = 4096;
const MAX_LENGTH: usize = 512;
const DOC_PREFIX: &str = "search_document: ";

pub struct NomicEmbedder {
    session: Session,
    tokenizer: Tokenizer,
    dim: usize,
    has_type_ids: bool,
}

impl NomicEmbedder {
    pub fn new(model_path: &Path, tokenizer_path: &Path, dim: usize) -> Result<Self> {
        ort::init().commit();

        let session = Session::builder()
            .map_err(|e| anyhow::anyhow!("Session builder: {}", e))?
            .with_optimization_level(ort::session::builder::GraphOptimizationLevel::Level3)
            .map_err(|e| anyhow::anyhow!("Set optimization: {}", e))?
            .with_intra_threads(4)
            .map_err(|e| anyhow::anyhow!("Set threads: {}", e))?
            .with_inter_threads(1)
            .map_err(|e| anyhow::anyhow!("Set inter threads: {}", e))?
            .commit_from_file(model_path)
            .map_err(|e| anyhow::anyhow!("Load model: {}", e))?;

        let has_type_ids = session.inputs().iter().any(|i| i.name() == "token_type_ids");

        let mut tokenizer = Tokenizer::from_file(tokenizer_path)
            .map_err(|e| anyhow::anyhow!("Load tokenizer: {}", e))?;

        tokenizer
            .with_truncation(Some(tokenizers::TruncationParams {
                max_length: MAX_LENGTH,
                strategy: tokenizers::TruncationStrategy::LongestFirst,
                ..Default::default()
            }))
            .map_err(|e| anyhow::anyhow!("Set truncation: {}", e))?;

        // Padding configured per-batch in encode_batch
        tokenizer.with_padding(Some(tokenizers::PaddingParams {
            ..Default::default()
        }));

        Ok(Self { session, tokenizer, dim, has_type_ids })
    }

    /// Encode a batch of texts. Returns one Vec<f32> per text.
    pub fn encode_batch(&mut self, texts: &[String]) -> Result<Vec<Vec<f32>>> {
        if texts.is_empty() {
            return Ok(vec![]);
        }

        // Prefix + truncate all texts
        let prefixed: Vec<String> = texts.iter()
            .map(|t| {
                let end = t.len().min(MAX_CHARS);
                let end = t.floor_char_boundary(end);
                format!("{}{}", DOC_PREFIX, &t[..end])
            })
            .collect();

        let str_refs: Vec<&str> = prefixed.iter().map(|s| s.as_str()).collect();

        // Batch tokenize — tokenizer handles padding to max length in batch
        let encodings = self.tokenizer
            .encode_batch(str_refs, true)
            .map_err(|e| anyhow::anyhow!("Batch tokenize: {}", e))?;

        let batch_size = encodings.len();
        let seq_len = encodings[0].get_ids().len(); // all padded to same length

        // Flatten into contiguous arrays
        let mut all_ids = Vec::with_capacity(batch_size * seq_len);
        let mut all_mask = Vec::with_capacity(batch_size * seq_len);
        let mut all_types = Vec::with_capacity(batch_size * seq_len);

        let mut masks_per_sample: Vec<Vec<f32>> = Vec::with_capacity(batch_size);

        for enc in &encodings {
            let ids = enc.get_ids();
            let mask = enc.get_attention_mask();

            // Pad if shorter (shouldn't happen with padding enabled, but safety)
            for j in 0..seq_len {
                all_ids.push(if j < ids.len() { ids[j] as i64 } else { 0 });
                all_mask.push(if j < mask.len() { mask[j] as i64 } else { 0 });
                all_types.push(0i64);
            }

            masks_per_sample.push(
                (0..seq_len).map(|j| if j < mask.len() { mask[j] as f32 } else { 0.0 }).collect()
            );
        }

        // Create tensors [batch_size, seq_len]
        let ids_tensor = Tensor::<i64>::from_array(([batch_size, seq_len], all_ids.into_boxed_slice()))
            .map_err(|e| anyhow::anyhow!("Create ids tensor: {}", e))?;
        let mask_tensor = Tensor::<i64>::from_array(([batch_size, seq_len], all_mask.into_boxed_slice()))
            .map_err(|e| anyhow::anyhow!("Create mask tensor: {}", e))?;
        let type_tensor = Tensor::<i64>::from_array(([batch_size, seq_len], all_types.into_boxed_slice()))
            .map_err(|e| anyhow::anyhow!("Create type tensor: {}", e))?;

        // Run inference — one call for the whole batch
        let outputs = if self.has_type_ids {
            self.session.run(ort::inputs![
                "input_ids" => ids_tensor,
                "attention_mask" => mask_tensor,
                "token_type_ids" => type_tensor,
            ]).map_err(|e| anyhow::anyhow!("Run inference: {}", e))?
        } else {
            self.session.run(ort::inputs![
                "input_ids" => ids_tensor,
                "attention_mask" => mask_tensor,
            ]).map_err(|e| anyhow::anyhow!("Run inference: {}", e))?
        };

        // Extract [batch_size, seq_len, hidden_size]
        let output = outputs[0]
            .try_extract_array::<f32>()
            .map_err(|e| anyhow::anyhow!("Extract output: {}", e))?;
        let hidden_size = output.shape()[2];

        // Mean pool (only self.dim dims — Matryoshka truncation is safe
        // before pooling since mean is linear) + normalize per sample
        let pool_dim = self.dim.min(hidden_size);
        let mut results = Vec::with_capacity(batch_size);
        for i in 0..batch_size {
            let mask = &masks_per_sample[i];
            let mut pooled = vec![0.0f32; pool_dim];
            let mut mask_sum = 0.0f32;

            for j in 0..seq_len {
                let m = mask[j];
                mask_sum += m;
                for k in 0..pool_dim {
                    pooled[k] += output[[i, j, k]] * m;
                }
            }
            if mask_sum > 1e-9 {
                for v in pooled.iter_mut() {
                    *v /= mask_sum;
                }
            }

            l2_normalize(&mut pooled);

            results.push(pooled);
        }

        Ok(results)
    }

    /// Single text convenience — calls encode_batch with batch of 1.
    pub fn encode(&mut self, text: &str) -> Result<Vec<f32>> {
        let results = self.encode_batch(&[text.to_string()])?;
        Ok(results.into_iter().next().unwrap_or_default())
    }
}

fn l2_normalize(vec: &mut [f32]) {
    let norm: f32 = vec.iter().map(|v| v * v).sum::<f32>().sqrt();
    if norm > 1e-9 {
        for v in vec.iter_mut() {
            *v /= norm;
        }
    }
}
