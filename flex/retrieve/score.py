"""
Flex scoring engine — composable modulations for single-pass retrieval.

This module contains the core scoring logic: token parsing, vector search
with landscape modulations (diversity, contrastive, temporal decay, centroid,
trajectory), and MMR selection.

Imported by vec_ops.py for query-time scoring.
"""

import re
import sys
import time

import numpy as np
from typing import Optional, List, Dict, Any


def parse_modifiers(modifier_str: str, extra_boundaries: Optional[set] = None) -> dict:
    """Parse a modifier string into modulation parameters.

    Tokens (space-separated, composable):

        similar:TEXT         embed text, cosine search
        centroid:id1,id2,... mean-pool example chunks, search from centroid
        diverse              MMR diversity selection
        decay[:N]            temporal decay (optional N-day half-life)
        suppress:TEXT        contrastive — demote similarity to TEXT
        from:TEXT to:TEXT    trajectory — direction through embedding space
        pool:N               candidate count (default 500)

    Unknown tokens are collected and passed through for external consumers.
    """
    result = {
        'recent': False,
        'recent_days': None,
        'unlike': [],
        'diverse': False,
        'limit': None,
        'like': None,
        'similar': None,
        'trajectory_from': None,
        'trajectory_to': None,
        'extra_tokens': [],
    }

    if not modifier_str:
        return result

    # ── Legacy alias shim ─────────────────────────────────────
    # Rewrite deprecated names to canonical names before parsing.
    # One code path per token after this point.
    modifier_str = modifier_str.replace('unlike:', 'suppress:')
    modifier_str = modifier_str.replace('like:', 'centroid:')
    modifier_str = modifier_str.replace('limit:', 'pool:')
    modifier_str = re.sub(r'\brecent:', 'decay:', modifier_str)
    modifier_str = re.sub(r'\brecent\b', 'decay', modifier_str)
    # Token boundary detection for multi-word token parsing.
    _CORE_BOUNDARIES = r'diverse|decay:|suppress:|centroid:|pool:|from:|similar:'
    if extra_boundaries:
        _TOKEN_BOUNDARY = _CORE_BOUNDARIES + '|' + '|'.join(extra_boundaries)
    else:
        _TOKEN_BOUNDARY = _CORE_BOUNDARIES

    # Extract similar:TEXT (multi-word, up to next token boundary)
    similar_match = re.search(
        r'similar:(.*?)(?=\s+(?:' + _TOKEN_BOUNDARY + r')\b|\s*$)',
        modifier_str
    )
    if similar_match:
        result['similar'] = similar_match.group(1).strip()
        modifier_str = modifier_str[:similar_match.start()] + modifier_str[similar_match.end():]

    # Extract ALL suppress:TEXT (multi-word, up to next token boundary)
    # Multiple suppress: tokens compose additively — each is independent.
    suppress_matches = list(re.finditer(
        r'suppress:(.*?)(?=\s+(?:' + _TOKEN_BOUNDARY + r')\b|\s*$)',
        modifier_str
    ))
    for m in suppress_matches:
        text = m.group(1).strip()
        if text:
            result['unlike'].append(text)
    # Remove matched spans in reverse order to preserve offsets
    for m in reversed(suppress_matches):
        modifier_str = modifier_str[:m.start()] + modifier_str[m.end():]

    # Extract trajectory (spans tokens) before splitting
    traj_match = re.search(
        r'from:(.*?)\s+to:(.*?)(?=\s+(?:' + _TOKEN_BOUNDARY + r')\b|\s*$)',
        modifier_str
    )
    if traj_match:
        result['trajectory_from'] = traj_match.group(1).strip()
        result['trajectory_to'] = traj_match.group(2).strip()
        modifier_str = modifier_str[:traj_match.start()] + modifier_str[traj_match.end():]

    for token in modifier_str.strip().split():
        if token == 'diverse':
            result['diverse'] = True
        elif token == 'decay':
            result['recent'] = True
        elif token.startswith('decay:'):
            result['recent'] = True
            try:
                result['recent_days'] = int(token.split(':', 1)[1])
            except ValueError:
                result['recent_days'] = None
        elif token.startswith('suppress:'):
            # Single-word suppress that wasn't caught by regex (edge case)
            result['unlike'].append(token.split(':', 1)[1])
        elif token.startswith('pool:'):
            try:
                result['limit'] = int(token.split(':', 1)[1])
            except ValueError:
                pass
        elif token.startswith('centroid:'):
            result['like'] = token.split(':', 1)[1].split(',')
        else:
            result['extra_tokens'].append(token)

    return result


def score_candidates(
    matrix: np.ndarray,
    ids: List[str],
    id_to_idx: Dict[str, int],
    query_vec: np.ndarray,
    *,
    timestamps: Optional[np.ndarray] = None,
    pre_filter_ids: Optional[set] = None,
    not_like_vec: Optional[np.ndarray] = None,
    diverse: bool = False,
    limit: int = 10,
    oversample: int = 200,
    mask: Optional[np.ndarray] = None,
    threshold: float = 0.0,
    mmr_lambda: float = 0.7,
    modifiers: Optional[dict] = None,
    config: Optional[dict] = None,
    embed_fn=None,
    embed_doc_fn=None,
    token_resolver=None,
) -> List[Dict[str, Any]]:
    """Score and select candidates with composable landscape modulations.

    Single-pass scoring: cosine similarity modulated by temporal decay,
    contrastive demotion, centroid blending, trajectory projection, and
    MMR diversity — all composed in one pass before candidate selection.

    Args:
        matrix: Normalized embedding matrix (n, dims), float32.
        ids: Document IDs aligned with matrix rows.
        id_to_idx: {id: row_index} mapping.
        query_vec: Query embedding (dims,), will be normalized.
        timestamps: Optional (n,) float64 epoch seconds for temporal decay.
        pre_filter_ids: Set of IDs to restrict search (from SQL pre-filter).
        not_like_vec: Negative query for contrastive demotion.
        diverse: Enable MMR diversity selection.
        limit: Max results.
        oversample: Candidate pool size for diversity/contrastive.
        mask: Boolean mask (n,) — True = include.
        threshold: Minimum cosine similarity cutoff.
        mmr_lambda: Relevance vs diversity tradeoff (0-1).
        modifiers: Parsed modifier dict from parse_modifiers().
        config: Cell config dict from _meta (vec:* keys).
        embed_fn: Embedding function for query-space text.
        embed_doc_fn: Embedding function for document-space text.

    Returns:
        List of {id, score} sorted by score desc.
    """
    if matrix is None or len(ids) == 0:
        return []

    dims = matrix.shape[1]

    # Validate dimensions
    if query_vec.shape != (dims,):
        raise ValueError(
            f"Query vector dimension {query_vec.shape} doesn't match "
            f"cache dimension ({dims},)"
        )

    # Normalize query
    query_vec = query_vec.astype(np.float32)
    query_norm = np.linalg.norm(query_vec)
    if query_norm > 0:
        query_vec = query_vec / query_norm

    # === CENTROID: like:id1,id2,... replaces or blends with query_vec ===
    like_ids = modifiers.get('like') if modifiers else None
    if like_ids:
        valid_indices = [id_to_idx[id_] for id_ in like_ids if id_ in id_to_idx]
        if not valid_indices:
            return []
        vecs = matrix[np.array(valid_indices)]
        centroid = vecs.mean(axis=0)
        c_norm = np.linalg.norm(centroid)
        if c_norm > 0:
            centroid /= c_norm
        if query_norm > 0:
            query_vec = 0.5 * query_vec + 0.5 * centroid
            q_norm = np.linalg.norm(query_vec)
            if q_norm > 0:
                query_vec /= q_norm
        else:
            query_vec = centroid

    # === TRAJECTORY: from:TEXT to:TEXT biases query via score combination ===
    traj_from = modifiers.get('trajectory_from') if modifiers else None
    traj_to = modifiers.get('trajectory_to') if modifiers else None
    _traj_direction = None
    if traj_from and traj_to and embed_fn:
        _embed_for_traj = embed_doc_fn if embed_doc_fn is not None else embed_fn
        start_vec = np.squeeze(_embed_for_traj(traj_from)).astype(np.float32)
        end_vec = np.squeeze(_embed_for_traj(traj_to)).astype(np.float32)
        direction = end_vec - start_vec
        d_norm = np.linalg.norm(direction)
        if d_norm > 0:
            direction /= d_norm
        _traj_direction = direction

    # === SQL PRE-FILTER: fancy-index into warm matrix ===
    if pre_filter_ids is not None:
        indices = np.array([
            id_to_idx[id_] for id_ in pre_filter_ids
            if id_ in id_to_idx
        ], dtype=np.int64)
        if len(indices) == 0:
            return []
        active_matrix = matrix[indices]
        active_ids = [ids[i] for i in indices]
        active_timestamps = timestamps[indices] if timestamps is not None else None
        active_id_to_idx = {id_: i for i, id_ in enumerate(active_ids)}
    else:
        active_matrix = matrix
        active_ids = ids
        active_timestamps = timestamps
        active_id_to_idx = id_to_idx
        indices = None

    # 1. Matrix multiply — all similarities at once
    similarities = active_matrix @ query_vec

    # Trajectory blend: 0.5 * query_score + 0.5 * direction_score
    if _traj_direction is not None:
        traj_scores = active_matrix @ _traj_direction
        similarities = 0.5 * similarities + 0.5 * traj_scores

    # === LANDSCAPE MODULATIONS (on active array, before candidate selection) ===
    if modifiers:
        cfg = config or {}

        # Temporal decay: scores *= 1 / (1 + days_ago / half_life)
        if modifiers.get('recent') and active_timestamps is not None:
            if np.any(active_timestamps > 0):
                half_life = float(
                    modifiers.get('recent_days')
                    or cfg.get('vec:recent:half_life', 30)
                )
                days_ago = np.maximum(
                    (time.time() - active_timestamps) / 86400.0, 0.0
                )
                similarities *= 1.0 / (1.0 + days_ago / half_life)

        # Contrastive from modifier string — multiple suppress tokens compose additively
        for unlike_text in (modifiers.get('unlike') or []):
            if embed_fn is not None:
                neg_vec = np.squeeze(embed_fn(unlike_text)).astype(np.float32)
                neg_norm = np.linalg.norm(neg_vec)
                if neg_norm > 0:
                    neg_vec = neg_vec / neg_norm
                neg_sims = active_matrix @ neg_vec
                similarities -= 0.5 * neg_sims

        # Override diverse/limit from modifiers
        if modifiers.get('diverse'):
            diverse = True
        if modifiers.get('limit'):
            limit = modifiers['limit']

    # Apply mask
    if mask is not None:
        if pre_filter_ids is not None:
            sub_mask = mask[indices]
            similarities = np.where(sub_mask, similarities, -np.inf)
        else:
            similarities = np.where(mask, similarities, -np.inf)

    # Apply threshold
    if threshold > 0:
        similarities = np.where(similarities >= threshold, similarities, -np.inf)

    # 2. Contrastive — penalize similarity to negative query
    if not_like_vec is not None:
        not_like_vec = not_like_vec.astype(np.float32)
        nl_norm = np.linalg.norm(not_like_vec)
        if nl_norm > 0:
            not_like_vec = not_like_vec / nl_norm
        neg_sims = active_matrix @ not_like_vec
        similarities -= 0.5 * neg_sims

    # Get candidate pool
    pool_size = oversample if diverse else limit
    if pool_size >= len(similarities):
        top_indices = np.argsort(similarities)[::-1]
    else:
        top_indices = np.argpartition(similarities, -pool_size)[-pool_size:]
        top_indices = top_indices[np.argsort(similarities[top_indices])[::-1]]

    # Filter -inf
    top_indices = [i for i in top_indices if similarities[i] != -np.inf]

    # === Extra enrichments on candidate set ===

    extra_enrichments = {}  # {cand_position: {_col: val}}
    cand_pool = min(len(top_indices), limit)
    cand_indices = np.array(top_indices[:cand_pool]) if top_indices else np.array([], dtype=int)
    cand_vecs = active_matrix[cand_indices] if len(cand_indices) > 0 else np.array([])
    cand_ids = [active_ids[i] for i in cand_indices] if len(cand_indices) > 0 else []

    def _merge_enrichment(enrichment: dict):
        """Merge {position: {col: val}} into extra_enrichments."""
        for pos, cols in enrichment.items():
            if pos not in extra_enrichments:
                extra_enrichments[pos] = {}
            extra_enrichments[pos].update(cols)

    # Extra tokens — resolved by caller-provided handler if present.
    if token_resolver and modifiers and len(cand_indices) > 0 and modifiers.get('extra_tokens'):
        for token_name in modifiers['extra_tokens']:
            enrichment = token_resolver(
                token_name, cand_ids, cand_vecs, modifiers)
            if enrichment:
                _merge_enrichment(enrichment)

    # === Apply enrichments to results ===
    def _attach_enrichments(results_list):
        """Attach _-prefixed columns to result dicts."""
        if not extra_enrichments:
            return
        cand_list = list(cand_indices)
        for r in results_list:
            orig_idx = active_id_to_idx.get(r['id'])
            if orig_idx is not None and orig_idx in cand_list:
                pos = cand_list.index(orig_idx)
                if pos in extra_enrichments:
                    r.update(extra_enrichments[pos])

    # 3. MMR diversity — iterative selection (returns MMR scores)
    if diverse and len(top_indices) > 1:
        mmr_results = _mmr_select(
            top_indices, similarities, active_matrix, limit, lambda_=mmr_lambda)
        results = [{'id': active_ids[idx], 'score': float(score)}
                   for idx, score in mmr_results]
        _attach_enrichments(results)
        return results

    # Build results (cosine/modulated scores)
    results = [{'id': active_ids[idx], 'score': float(similarities[idx])}
               for idx in top_indices[:limit]]
    _attach_enrichments(results)
    return results


def _mmr_select(candidates: list, similarities: np.ndarray,
                matrix: np.ndarray, k: int, lambda_: float = 0.7) -> list:
    """MMR: iteratively select for relevance minus redundancy.

    Returns list of (index, mmr_score) tuples.
    """
    if not candidates:
        return []

    cand_vecs = matrix[candidates]
    cand_sims = cand_vecs @ cand_vecs.T

    n = len(candidates)
    max_sim_to_selected = np.full(n, -np.inf)
    selected_mask = np.zeros(n, dtype=bool)
    relevance = similarities[candidates]

    selected = [(candidates[0], lambda_ * float(relevance[0]))]
    selected_mask[0] = True
    max_sim_to_selected = np.maximum(max_sim_to_selected, cand_sims[0])

    for _ in range(k - 1):
        if selected_mask.all():
            break

        mmr_scores = lambda_ * relevance - (1 - lambda_) * max_sim_to_selected
        mmr_scores[selected_mask] = -np.inf

        best = np.argmax(mmr_scores)
        if mmr_scores[best] == -np.inf:
            break

        selected.append((candidates[best], float(mmr_scores[best])))
        selected_mask[best] = True
        max_sim_to_selected = np.maximum(max_sim_to_selected, cand_sims[best])

    return selected
