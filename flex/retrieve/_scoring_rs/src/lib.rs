//! Flex scoring engine — composable modulations for single-pass retrieval.
//!
//! Pure math: arrays in, scored results out. No SQLite, no Python callbacks.
//! Python pre-computes any text→vector conversions before calling in.

use ndarray::{Array1, Array2, ArrayView1};
use numpy::{PyReadonlyArray1, PyReadonlyArray2};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use std::collections::HashMap;

/// Parse a modifier string into modulation parameters.
///
/// Returns a dict with keys: recent, recent_days, unlike, diverse, limit,
/// like, trajectory_from, trajectory_to, local_communities.
#[pyfunction]
#[pyo3(signature = (modifier_str=None))]
fn parse_modifiers(py: Python<'_>, modifier_str: Option<&str>) -> PyResult<Py<PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("recent", false)?;
    dict.set_item("recent_days", py.None())?;
    dict.set_item("unlike", py.None())?;
    dict.set_item("diverse", false)?;
    dict.set_item("limit", py.None())?;
    dict.set_item("like", py.None())?;
    dict.set_item("trajectory_from", py.None())?;
    dict.set_item("trajectory_to", py.None())?;
    dict.set_item("local_communities", false)?;

    let modifier_str = match modifier_str {
        Some(s) if !s.is_empty() => s.to_string(),
        _ => return Ok(dict.into()),
    };

    // Extract trajectory: find "from:TEXT to:TEXT" span
    // Known tokens that terminate the to: value
    let terminators = [
        "diverse", "recent:", "unlike:", "like:", "limit:",
        "local_communities", "detect_communities",
    ];
    let mut remaining = modifier_str.clone();
    if let Some(from_pos) = modifier_str.find("from:") {
        let after_from = &modifier_str[from_pos + 5..];
        if let Some(to_rel) = after_from.find(" to:") {
            let from_text = after_from[..to_rel].trim();
            let after_to = &after_from[to_rel + 4..];
            // Find where to: value ends (next known token or end of string)
            let to_end = terminators
                .iter()
                .filter_map(|t| after_to.find(t))
                .min()
                .unwrap_or(after_to.len());
            let to_text = after_to[..to_end].trim();
            if !from_text.is_empty() && !to_text.is_empty() {
                dict.set_item("trajectory_from", from_text)?;
                dict.set_item("trajectory_to", to_text)?;
                let span_end = from_pos + 5 + to_rel + 4 + to_end;
                remaining = format!(
                    "{}{}",
                    &modifier_str[..from_pos],
                    &modifier_str[span_end..]
                );
            }
        }
    }

    for token in remaining.split_whitespace() {
        if token == "diverse" {
            dict.set_item("diverse", true)?;
        } else if token == "recent" {
            dict.set_item("recent", true)?;
        } else if let Some(rest) = token.strip_prefix("recent:") {
            dict.set_item("recent", true)?;
            if let Ok(days) = rest.parse::<i64>() {
                dict.set_item("recent_days", days)?;
            }
        } else if let Some(rest) = token.strip_prefix("unlike:") {
            dict.set_item("unlike", rest)?;
        } else if let Some(rest) = token.strip_prefix("limit:") {
            if let Ok(lim) = rest.parse::<usize>() {
                dict.set_item("limit", lim)?;
            }
        } else if let Some(rest) = token.strip_prefix("like:") {
            let ids: Vec<&str> = rest.split(',').collect();
            dict.set_item("like", ids)?;
        } else if token == "local_communities" || token == "detect_communities" {
            dict.set_item("local_communities", true)?;
        }
        // kind: and community: silently ignored (dead tokens)
    }

    Ok(dict.into())
}

// ─── score_candidates ──────────────────────────────────────────────────────

/// Score and select candidates with composable landscape modulations.
///
/// All text→vector conversions must be done in Python before calling.
/// This function takes pure arrays and returns scored results.
///
/// Args:
///     matrix: Normalized embedding matrix (n, dims), float32.
///     ids: Document IDs aligned with matrix rows.
///     id_to_idx: {id: row_index} mapping.
///     query_vec: Query embedding (dims,), will be normalized.
///     timestamps: Optional (n,) float64 epoch seconds.
///     pre_filter_ids: Optional set of IDs to restrict search.
///     not_like_vec: Optional negative query for contrastive.
///     diverse: Enable MMR diversity selection.
///     limit: Max results.
///     oversample: Candidate pool size.
///     threshold: Minimum similarity cutoff.
///     mmr_lambda: Relevance vs diversity (0-1).
///     recent: Apply temporal decay.
///     recent_days: Half-life in days for temporal decay.
///     like_ids: Optional list of IDs for centroid blending.
///     traj_from_vec: Optional trajectory start vector.
///     traj_to_vec: Optional trajectory end vector.
///
/// Returns list of dicts with 'id' and 'score' keys.
#[pyfunction]
#[pyo3(signature = (
    matrix, ids, id_to_idx, query_vec,
    timestamps=None, pre_filter_ids=None, not_like_vec=None,
    diverse=false, limit=10, oversample=200,
    threshold=0.0, mmr_lambda=0.7,
    recent=false, recent_days=None,
    like_ids=None, traj_from_vec=None, traj_to_vec=None,
    mask=None
))]
#[allow(clippy::too_many_arguments)]
fn score_candidates<'py>(
    py: Python<'py>,
    matrix: PyReadonlyArray2<'py, f32>,
    ids: Vec<String>,
    id_to_idx: HashMap<String, usize>,
    query_vec: PyReadonlyArray1<'py, f32>,
    timestamps: Option<PyReadonlyArray1<'py, f64>>,
    pre_filter_ids: Option<Vec<String>>,
    not_like_vec: Option<PyReadonlyArray1<'py, f32>>,
    diverse: bool,
    limit: usize,
    oversample: usize,
    threshold: f32,
    mmr_lambda: f64,
    recent: bool,
    recent_days: Option<f64>,
    like_ids: Option<Vec<String>>,
    traj_from_vec: Option<PyReadonlyArray1<'py, f32>>,
    traj_to_vec: Option<PyReadonlyArray1<'py, f32>>,
    mask: Option<PyReadonlyArray1<'py, bool>>,
) -> PyResult<Py<PyList>> {
    let mat = matrix.as_array();
    let dims = mat.ncols();

    if ids.is_empty() {
        return Ok(PyList::empty(py).into());
    }

    let qv = query_vec.as_array();
    if qv.len() != dims {
        return Err(pyo3::exceptions::PyValueError::new_err(format!(
            "Query vector dimension {} doesn't match cache dimension {}",
            qv.len(),
            dims
        )));
    }

    // Normalize query
    let mut query = qv.to_owned();
    let qnorm = norm(&query.view());
    if qnorm > 0.0 {
        query.mapv_inplace(|x| x / qnorm);
    }

    // === CENTROID: like_ids blends with query_vec ===
    if let Some(ref like) = like_ids {
        let valid_indices: Vec<usize> = like
            .iter()
            .filter_map(|id| id_to_idx.get(id.as_str()).copied())
            .collect();
        if valid_indices.is_empty() {
            return Ok(PyList::empty(py).into());
        }
        let mut centroid = Array1::<f32>::zeros(dims);
        for &idx in &valid_indices {
            centroid += &mat.row(idx);
        }
        centroid /= valid_indices.len() as f32;
        let cn = norm(&centroid.view());
        if cn > 0.0 {
            centroid.mapv_inplace(|x| x / cn);
        }
        if qnorm > 0.0 {
            query = &query * 0.5 + &centroid * 0.5;
            let qn2 = norm(&query.view());
            if qn2 > 0.0 {
                query.mapv_inplace(|x| x / qn2);
            }
        } else {
            query = centroid;
        }
    }

    // === TRAJECTORY: pre-computed direction vector ===
    let traj_direction = match (traj_from_vec, traj_to_vec) {
        (Some(from_arr), Some(to_arr)) => {
            let from = from_arr.as_array();
            let to = to_arr.as_array();
            let mut direction = to.to_owned() - &from;
            let dn = norm(&direction.view());
            if dn > 0.0 {
                direction.mapv_inplace(|x| x / dn);
            }
            Some(direction)
        }
        _ => None,
    };

    // === SQL PRE-FILTER: subset the matrix ===
    let ts_full = timestamps.as_ref().map(|t| t.as_array().to_owned());
    let mask_full = mask.as_ref().map(|m| m.as_array().to_owned());

    let (active_mat, active_ids, active_ts, active_mask, _active_idx_map) =
        if let Some(ref filter_ids) = pre_filter_ids {
            let indices: Vec<usize> = filter_ids
                .iter()
                .filter_map(|id| id_to_idx.get(id.as_str()).copied())
                .collect();
            if indices.is_empty() {
                return Ok(PyList::empty(py).into());
            }
            let sub_mat = select_rows(&mat, &indices);
            let sub_ids: Vec<String> = indices.iter().map(|&i| ids[i].clone()).collect();
            let sub_ts = ts_full
                .as_ref()
                .map(|ts| select_elements(ts, &indices));
            let sub_mask = mask_full
                .as_ref()
                .map(|m| select_bool_elements(m, &indices));
            let sub_idx: HashMap<String, usize> = sub_ids
                .iter()
                .enumerate()
                .map(|(i, id)| (id.clone(), i))
                .collect();
            (sub_mat, sub_ids, sub_ts, sub_mask, sub_idx)
        } else {
            let idx_map: HashMap<String, usize> = id_to_idx.clone();
            (
                mat.to_owned(),
                ids.clone(),
                ts_full.clone(),
                mask_full.clone(),
                idx_map,
            )
        };

    let n = active_mat.nrows();

    // 1. Matrix multiply — all similarities at once
    let mut similarities = active_mat.dot(&query);

    // Trajectory blend: 0.7 * query_score + 0.3 * direction_score
    if let Some(ref dir) = traj_direction {
        let traj_scores = active_mat.dot(dir);
        similarities = &similarities * 0.7 + &traj_scores * 0.3;
    }

    // === LANDSCAPE MODULATIONS ===

    // Temporal decay: scores *= 1 / (1 + days_ago / half_life)
    if recent {
        if let Some(ref ts) = active_ts {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs_f64();
            let half_life = recent_days.unwrap_or(30.0);
            for i in 0..n {
                if ts[i] > 0.0 {
                    let days_ago = ((now - ts[i]) / 86400.0).max(0.0);
                    similarities[i] *= (1.0 / (1.0 + days_ago as f32 / half_life as f32)) as f32;
                }
            }
        }
    }

    // Apply mask
    if let Some(ref m) = active_mask {
        for i in 0..n {
            if !m[i] {
                similarities[i] = f32::NEG_INFINITY;
            }
        }
    }

    // Apply threshold
    if threshold > 0.0 {
        for i in 0..n {
            if similarities[i] < threshold {
                similarities[i] = f32::NEG_INFINITY;
            }
        }
    }

    // 2. Contrastive — penalize similarity to negative query
    if let Some(nlv) = not_like_vec {
        let mut neg_query = nlv.as_array().to_owned();
        let nn = norm(&neg_query.view());
        if nn > 0.0 {
            neg_query.mapv_inplace(|x| x / nn);
        }
        let neg_sims = active_mat.dot(&neg_query);
        similarities = &similarities - &(&neg_sims * 0.5);
    }

    // Get candidate pool
    let pool_size = if diverse { oversample } else { limit };
    let top_indices = top_k_indices(&similarities, pool_size);

    // Filter -inf
    let top_indices: Vec<usize> = top_indices
        .into_iter()
        .filter(|&i| similarities[i].is_finite())
        .collect();

    // 3. MMR diversity — iterative selection
    if diverse && top_indices.len() > limit {
        let mmr_results = mmr_select(&top_indices, &similarities, &active_mat, limit, mmr_lambda);
        let results = PyList::empty(py);
        for (idx, score) in mmr_results {
            let d = PyDict::new(py);
            d.set_item("id", &active_ids[idx])?;
            d.set_item("score", (score * 10000.0).round() / 10000.0)?;
            results.append(d)?;
        }
        return Ok(results.into());
    }

    // Build results
    let results = PyList::empty(py);
    for &idx in top_indices.iter().take(limit) {
        let d = PyDict::new(py);
        d.set_item("id", &active_ids[idx])?;
        d.set_item("score", (similarities[idx] as f64 * 10000.0).round() / 10000.0)?;
        results.append(d)?;
    }
    Ok(results.into())
}

// ─── _mmr_select ───────────────────────────────────────────────────────────

/// MMR: iteratively select for relevance minus redundancy.
/// Returns list of (index, mmr_score) tuples.
#[pyfunction]
fn mmr_select_py<'py>(
    py: Python<'py>,
    candidates: Vec<usize>,
    similarities: PyReadonlyArray1<'py, f32>,
    matrix: PyReadonlyArray2<'py, f32>,
    k: usize,
    lambda_: f64,
) -> PyResult<Py<PyList>> {
    let sims = similarities.as_array();
    let mat = matrix.as_array();

    let results = mmr_select(&candidates, &sims, &mat, k, lambda_);

    let list = PyList::empty(py);
    for (idx, score) in results {
        list.append((idx, score))?;
    }
    Ok(list.into())
}

fn mmr_select<S: ndarray::Data<Elem = f32>, T: ndarray::Data<Elem = f32>>(
    candidates: &[usize],
    similarities: &ndarray::ArrayBase<S, ndarray::Ix1>,
    matrix: &ndarray::ArrayBase<T, ndarray::Ix2>,
    k: usize,
    lambda_: f64,
) -> Vec<(usize, f64)> {
    if candidates.is_empty() {
        return Vec::new();
    }

    // Build candidate sub-matrix and pairwise similarities
    let cand_mat = select_rows(matrix, candidates);
    let cand_sims = cand_mat.dot(&cand_mat.t());

    let n = candidates.len();
    let mut max_sim_to_selected = vec![f64::NEG_INFINITY; n];
    let mut selected_mask = vec![false; n];
    let relevance: Vec<f64> = candidates.iter().map(|&i| similarities[i] as f64).collect();

    let lambda_f = lambda_;

    // First selection
    let mut selected = vec![(candidates[0], lambda_f * relevance[0])];
    selected_mask[0] = true;
    for j in 0..n {
        max_sim_to_selected[j] = max_sim_to_selected[j].max(cand_sims[(0, j)] as f64);
    }

    for _ in 1..k {
        if selected_mask.iter().all(|&s| s) {
            break;
        }

        let mut best_idx = 0;
        let mut best_score = f64::NEG_INFINITY;
        for j in 0..n {
            if selected_mask[j] {
                continue;
            }
            let mmr = lambda_f * relevance[j] - (1.0 - lambda_f) * max_sim_to_selected[j];
            if mmr > best_score {
                best_score = mmr;
                best_idx = j;
            }
        }

        if best_score == f64::NEG_INFINITY {
            break;
        }

        selected.push((candidates[best_idx], best_score));
        selected_mask[best_idx] = true;
        for j in 0..n {
            max_sim_to_selected[j] =
                max_sim_to_selected[j].max(cand_sims[(best_idx, j)] as f64);
        }
    }

    selected
}

// ─── Helpers ───────────────────────────────────────────────────────────────

fn norm(v: &ArrayView1<f32>) -> f32 {
    v.dot(v).sqrt()
}

fn select_rows<S: ndarray::Data<Elem = f32>>(mat: &ndarray::ArrayBase<S, ndarray::Ix2>, indices: &[usize]) -> Array2<f32> {
    let cols = mat.ncols();
    let mut out = Array2::<f32>::zeros((indices.len(), cols));
    for (i, &idx) in indices.iter().enumerate() {
        out.row_mut(i).assign(&mat.row(idx));
    }
    out
}

fn select_elements(arr: &Array1<f64>, indices: &[usize]) -> Array1<f64> {
    Array1::from_iter(indices.iter().map(|&i| arr[i]))
}

fn select_bool_elements(arr: &Array1<bool>, indices: &[usize]) -> Array1<bool> {
    Array1::from_iter(indices.iter().map(|&i| arr[i]))
}

/// Partial sort: return indices of top-k elements, sorted descending.
fn top_k_indices(arr: &Array1<f32>, k: usize) -> Vec<usize> {
    let n = arr.len();
    if k >= n {
        // Full sort
        let mut indices: Vec<usize> = (0..n).collect();
        indices.sort_unstable_by(|&a, &b| {
            arr[b].partial_cmp(&arr[a]).unwrap_or(std::cmp::Ordering::Equal)
        });
        return indices;
    }

    // Partial sort via selection
    let mut indices: Vec<usize> = (0..n).collect();
    indices.select_nth_unstable_by(k, |&a, &b| {
        arr[b].partial_cmp(&arr[a]).unwrap_or(std::cmp::Ordering::Equal)
    });
    indices.truncate(k);
    indices.sort_unstable_by(|&a, &b| {
        arr[b].partial_cmp(&arr[a]).unwrap_or(std::cmp::Ordering::Equal)
    });
    indices
}

// ─── Module ────────────────────────────────────────────────────────────────

#[pymodule]
fn _scoring_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(parse_modifiers, m)?)?;
    m.add_function(wrap_pyfunction!(score_candidates, m)?)?;
    m.add_function(wrap_pyfunction!(mmr_select_py, m)?)?;
    Ok(())
}
