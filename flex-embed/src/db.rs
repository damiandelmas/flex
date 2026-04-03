//! SQLite operations — read NULL chunks, write embeddings, mean-pool sources.

use anyhow::{Context, Result};
use rusqlite::{params, Connection};
use std::path::Path;

/// Open a cell database with WAL mode and busy timeout.
pub fn open_cell(path: &Path) -> Result<Connection> {
    let conn = Connection::open(path)
        .context(format!("Failed to open database: {}", path.display()))?;
    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
         PRAGMA synchronous=NORMAL;
         PRAGMA busy_timeout=30000;
         PRAGMA cache_size=-20000;
         PRAGMA temp_store=MEMORY;",
    )?;
    Ok(conn)
}

/// Read all chunks that need embedding.
pub fn read_unembedded_chunks(conn: &Connection) -> Result<Vec<(String, String)>> {
    let mut stmt = conn
        .prepare("SELECT id, content FROM _raw_chunks WHERE embedding IS NULL")
        .context("Failed to prepare SELECT for unembedded chunks")?;

    let chunks = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
            ))
        })?
        .filter_map(|r| r.ok())
        .collect();

    Ok(chunks)
}

/// Mean-pool source embeddings from their chunk embeddings.
/// Returns number of sources pooled.
pub fn mean_pool_sources(conn: &Connection, commit_every: usize) -> Result<usize> {
    // Find sources with NULL embedding that have embedded chunks
    let mut stmt = conn.prepare(
        "SELECT DISTINCT e.source_id FROM _edges_source e
         JOIN _raw_sources s ON e.source_id = s.source_id
         WHERE s.embedding IS NULL",
    )?;

    let source_ids: Vec<String> = stmt
        .query_map([], |row| row.get::<_, String>(0))?
        .filter_map(|r| r.ok())
        .collect();

    if source_ids.is_empty() {
        return Ok(0);
    }

    let dim = detect_dim(conn)?;
    let mut count = 0;

    conn.execute_batch("BEGIN")?;

    for (i, source_id) in source_ids.iter().enumerate() {
        // Get chunk embeddings for this source
        let mut chunk_stmt = conn.prepare_cached(
            "SELECT c.embedding FROM _raw_chunks c
             JOIN _edges_source e ON c.id = e.chunk_id
             WHERE e.source_id = ?1 AND c.embedding IS NOT NULL",
        )?;

        let embeddings: Vec<Vec<f32>> = chunk_stmt
            .query_map(params![source_id], |row| {
                let blob: Vec<u8> = row.get(0)?;
                Ok(blob_to_embedding(&blob))
            })?
            .filter_map(|r| r.ok())
            .collect();

        if embeddings.is_empty() {
            continue;
        }

        // Mean pool
        let mut mean = vec![0.0f32; dim];
        for emb in &embeddings {
            for (j, v) in emb.iter().enumerate() {
                if j < dim {
                    mean[j] += v;
                }
            }
        }
        let n = embeddings.len() as f32;
        for v in mean.iter_mut() {
            *v /= n;
        }

        // L2 normalize
        let norm: f32 = mean.iter().map(|v| v * v).sum::<f32>().sqrt();
        if norm > 1e-9 {
            for v in mean.iter_mut() {
                *v /= norm;
            }
        }

        // Write back
        let blob: Vec<u8> = mean.iter().flat_map(|f| f.to_le_bytes()).collect();
        conn.execute(
            "UPDATE _raw_sources SET embedding = ?1 WHERE source_id = ?2",
            params![blob, source_id],
        )?;

        count += 1;
        if (i + 1) % commit_every == 0 {
            conn.execute_batch("COMMIT; BEGIN")?;
        }
    }

    conn.execute_batch("COMMIT")?;
    Ok(count)
}

/// Detect embedding dimension from an existing embedded chunk.
fn detect_dim(conn: &Connection) -> Result<usize> {
    let result: Option<Vec<u8>> = conn
        .query_row(
            "SELECT embedding FROM _raw_chunks WHERE embedding IS NOT NULL LIMIT 1",
            [],
            |row| row.get(0),
        )
        .ok();

    match result {
        Some(blob) => Ok(blob.len() / 4), // f32 = 4 bytes
        None => Ok(128),                    // default Matryoshka dim
    }
}

fn blob_to_embedding(blob: &[u8]) -> Vec<f32> {
    blob.chunks_exact(4)
        .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
        .collect()
}
