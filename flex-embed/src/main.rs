//! flex-embed — standalone embedding binary for flex knowledge cells.
//!
//! Reads chunks with NULL embeddings from a cell SQLite database,
//! embeds them using the Nomic ONNX model, writes embeddings back,
//! and mean-pools source embeddings.
//!
//! Usage: flex-embed <db_path> [--model-dir DIR] [--dim 128]

mod db;
mod embed;

use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "flex-embed", about = "Embed chunks in a flex cell")]
struct Args {
    /// Path to the cell SQLite database
    db_path: PathBuf,

    /// Path to model directory containing model.onnx + tokenizer.json
    #[arg(long, default_value_os_t = default_model_dir())]
    model_dir: PathBuf,

    /// Matryoshka truncation dimension
    #[arg(long, default_value_t = 128)]
    dim: usize,

    /// Commit every N rows (WAL safety)
    #[arg(long, default_value_t = 500)]
    commit_every: usize,
}

fn default_model_dir() -> PathBuf {
    let home = std::env::var("FLEX_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            dirs::home_dir()
                .unwrap_or_else(|| PathBuf::from("."))
                .join(".flex")
        });
    home.join("models")
}

fn main() -> Result<()> {
    let args = Args::parse();

    // Resolve model files — prefer fp32/ subdir if it exists
    let (model_path, tokenizer_path) = resolve_model_files(&args.model_dir)?;

    // Initialize embedder
    eprintln!("[flex-embed] Loading model from {}", model_path.display());
    let mut embedder = embed::NomicEmbedder::new(&model_path, &tokenizer_path, args.dim)?;
    eprintln!("[flex-embed] Model loaded ({}-dim)", args.dim);

    // Open database
    let conn = db::open_cell(&args.db_path)?;

    // Read unembedded chunks, sorted by length (short first)
    let mut chunks = db::read_unembedded_chunks(&conn)?;
    if chunks.is_empty() {
        // Still try mean-pooling (orphan recovery)
        let pooled = db::mean_pool_sources(&conn, args.commit_every)?;
        if pooled > 0 {
            println!("embedded 0 chunks");
            println!("pooled {} sources", pooled);
        } else {
            println!("embedded 0 chunks");
            println!("pooled 0 sources");
        }
        return Ok(());
    }

    // Sort by content length — short chunks first so batches have uniform padding
    chunks.sort_by_key(|(_, content)| content.len());

    eprintln!("[flex-embed] Embedding {} chunks...", chunks.len());

    let max_batch: usize = 128;
    let mut count = 0;
    let mut batch_start = 0;
    let total = chunks.len();
    let t0 = std::time::Instant::now();
    conn.execute_batch("BEGIN")?;

    while batch_start < total {
        // Adaptive batch size based on longest chunk in this window
        let longest_chars = chunks[(batch_start + max_batch - 1).min(total - 1)].1.len();
        let est_tokens = (longest_chars / 3 + 20).min(512); // conservative chars-to-tokens + prefix, capped at max_length
        let batch_size = adaptive_batch_size(est_tokens).min(total - batch_start);
        let batch_end = batch_start + batch_size;

        let batch: Vec<String> = chunks[batch_start..batch_end]
            .iter()
            .map(|(_, content)| content.clone())
            .collect();

        let embeddings = embedder.encode_batch(&batch)?;

        for (j, embedding) in embeddings.iter().enumerate() {
            let idx = batch_start + j;
            let blob = embedding_to_blob(embedding);
            conn.execute(
                "UPDATE _raw_chunks SET embedding = ?1 WHERE id = ?2",
                rusqlite::params![blob, chunks[idx].0],
            )?;
            count += 1;
        }

        if count % args.commit_every < batch_size || batch_end == total {
            conn.execute_batch("COMMIT; BEGIN")?;
        }

        if count % 2000 < batch_size || batch_end == total {
            let elapsed = t0.elapsed().as_secs_f64();
            let rate = count as f64 / elapsed;
            let eta = (total - count) as f64 / rate;
            eprintln!(
                "[flex-embed] {} / {} embedded ({:.0}/s, ETA {:.0}s, batch={})",
                count, total, rate, eta, batch_size
            );
        }

        batch_start = batch_end;
    }
    conn.execute_batch("COMMIT")?;
    let elapsed = t0.elapsed().as_secs_f64();
    eprintln!("[flex-embed] {} chunks embedded in {:.1}s ({:.0}/s)", count, elapsed, count as f64 / elapsed);

    // Mean-pool source embeddings
    let pooled = db::mean_pool_sources(&conn, args.commit_every)?;

    println!("embedded {} chunks", count);
    println!("pooled {} sources", pooled);

    Ok(())
}

fn resolve_model_files(model_dir: &PathBuf) -> Result<(PathBuf, PathBuf)> {
    // Prefer fp32 subdir if it exists
    let fp32_dir = model_dir.join("fp32");
    let base_dir = if fp32_dir.join("model.onnx").exists() {
        eprintln!("[flex-embed] Using fp32 model");
        fp32_dir
    } else {
        model_dir.clone()
    };

    let model_path = base_dir.join("model.onnx");
    if !model_path.exists() {
        anyhow::bail!(
            "Model not found at {}. Run 'flex init' to download.",
            model_path.display()
        );
    }

    // Tokenizer can be in base_dir or parent model_dir
    let tokenizer_path = if base_dir.join("tokenizer.json").exists() {
        base_dir.join("tokenizer.json")
    } else if model_dir.join("tokenizer.json").exists() {
        model_dir.join("tokenizer.json")
    } else {
        anyhow::bail!(
            "tokenizer.json not found in {} or {}",
            base_dir.display(),
            model_dir.display()
        );
    };

    Ok((model_path, tokenizer_path))
}

/// Scale batch size inversely with sequence length.
/// Budget: ~512MB attention memory. Attention = heads * seq^2 * 4 bytes per sample.
fn adaptive_batch_size(est_tokens: usize) -> usize {
    let budget: usize = 512 * 1024 * 1024; // 512MB
    let heads: usize = 12;
    let seq = est_tokens.max(32); // minimum 32 tokens
    let per_sample = heads * seq * seq * 4;
    let bs = budget / per_sample;
    bs.max(1).min(128) // clamp to [1, 128]
}

fn embedding_to_blob(embedding: &[f32]) -> Vec<u8> {
    embedding.iter().flat_map(|f| f.to_le_bytes()).collect()
}
