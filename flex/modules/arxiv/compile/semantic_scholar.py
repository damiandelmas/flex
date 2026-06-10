"""
Semantic Scholar API client for citation graph enrichment.

Adds citation/reference edges to the arxiv cell.
Used for snowball expansion: seed papers → their references → their citations.

API docs: https://api.semanticscholar.org/api-docs/
Rate limit: 1 req/s (with key), shared pool without key.
Auth: Optional but recommended. Set SEMANTIC_SCHOLAR_API_KEY env var.

Non-destructive: we store S2 paper IDs and TLDRs as enrichment,
never overwrite arXiv metadata.
"""

import json
import os
import sys
import time
import urllib.request
import urllib.parse
from typing import Optional


BASE_URL = "https://api.semanticscholar.org/graph/v1"
API_KEY = os.environ.get("SEMANTIC_SCHOLAR_API_KEY", "")
DELAY = 1.1  # slightly over 1 req/s to be safe
BATCH_SIZE = 500  # max for batch endpoint


def _headers() -> dict:
    h = {"User-Agent": "flex-arxiv/1.0"}
    if API_KEY:
        h["x-api-key"] = API_KEY
    return h


def _fetch(url: str, timeout: int = 30) -> dict | None:
    """Fetch from S2 API. Returns parsed JSON or None on error."""
    req = urllib.request.Request(url, headers=_headers())
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        if e.code == 429:
            print(f"  [!] S2 rate limited. Waiting 10s...", file=sys.stderr)
            time.sleep(10)
            return _fetch(url, timeout)  # retry once
        print(f"  [!] S2 HTTP {e.code}: {url[:80]}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"  [!] S2 error: {e}", file=sys.stderr)
        return None


def _post(url: str, data: dict, timeout: int = 30) -> dict | None:
    """POST to S2 API. Returns parsed JSON or None."""
    body = json.dumps(data).encode()
    req = urllib.request.Request(url, data=body, headers={
        **_headers(), "Content-Type": "application/json"
    }, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        if e.code == 429:
            print(f"  [!] S2 rate limited. Waiting 10s...", file=sys.stderr)
            time.sleep(10)
            return _post(url, data, timeout)
        print(f"  [!] S2 HTTP {e.code}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"  [!] S2 error: {e}", file=sys.stderr)
        return None


PAPER_FIELDS = "paperId,externalIds,title,abstract,year,citationCount,referenceCount,influentialCitationCount,isOpenAccess,fieldsOfStudy,publicationDate,tldr"

# For references/citations endpoints, nested paper fields use different prefix
REF_FIELDS = "paperId,externalIds,title,abstract,year,citationCount,fieldsOfStudy,publicationDate"


def get_paper(arxiv_id: str) -> dict | None:
    """Get paper details by arXiv ID.

    Returns dict with S2 metadata including TLDR, citation count, fields of study.
    """
    url = f"{BASE_URL}/paper/ArXiv:{arxiv_id}?fields={PAPER_FIELDS}"
    time.sleep(DELAY)
    return _fetch(url)


def get_references(arxiv_id: str, limit: int = 100) -> list[dict]:
    """Get papers that this paper cites (its bibliography).

    Returns list of dicts with arXiv IDs where available.
    """
    url = (f"{BASE_URL}/paper/ArXiv:{arxiv_id}/references"
           f"?fields={REF_FIELDS}&limit={limit}")
    time.sleep(DELAY)
    data = _fetch(url)
    if not data:
        return []
    return [r["citedPaper"] for r in data.get("data", []) if r.get("citedPaper")]


def get_citations(arxiv_id: str, limit: int = 100) -> list[dict]:
    """Get papers that cite this paper.

    Returns list of dicts with arXiv IDs where available.
    """
    url = (f"{BASE_URL}/paper/ArXiv:{arxiv_id}/citations"
           f"?fields={REF_FIELDS}&limit={limit}")
    time.sleep(DELAY)
    data = _fetch(url)
    if not data:
        return []
    return [c["citingPaper"] for c in data.get("data", []) if c.get("citingPaper")]


def extract_arxiv_id(paper: dict) -> str | None:
    """Extract arXiv ID from S2 paper dict."""
    ext = paper.get("externalIds", {})
    if ext and "ArXiv" in ext:
        return ext["ArXiv"]
    return None


def normalize_s2_paper(paper: dict) -> dict | None:
    """Convert S2 paper dict to our arXiv cell format.

    Returns None if no arXiv ID (can't link to our cell).
    """
    arxiv_id = extract_arxiv_id(paper)
    if not arxiv_id:
        return None

    title = paper.get("title", "")
    abstract = paper.get("abstract", "")
    if not title and not abstract:
        return None

    # TLDR
    tldr = ""
    if paper.get("tldr") and paper["tldr"].get("text"):
        tldr = paper["tldr"]["text"]

    year = paper.get("year", 0)
    pub_date = paper.get("publicationDate", "")

    # Approximate timestamp from year
    created_utc = 0
    if pub_date:
        try:
            from datetime import datetime, timezone
            created_utc = int(datetime.fromisoformat(pub_date).replace(
                tzinfo=timezone.utc).timestamp())
        except (ValueError, OSError):
            pass
    elif year:
        from datetime import datetime, timezone
        created_utc = int(datetime(year, 6, 1, tzinfo=timezone.utc).timestamp())

    fields = paper.get("fieldsOfStudy") or []

    return {
        "id": arxiv_id,
        "arxiv_id": arxiv_id,
        "arxiv_id_base": arxiv_id,
        "title": title,
        "abstract": abstract or "",
        "tldr": tldr,
        "authors": [],
        "authors_str": "",
        "primary_category": fields[0] if fields else "",
        "categories": fields,
        "categories_str": ", ".join(fields),
        "published": pub_date,
        "updated": "",
        "created_utc": created_utc,
        "pdf_url": f"https://arxiv.org/pdf/{arxiv_id}",
        "abs_url": f"https://arxiv.org/abs/{arxiv_id}",
        "comment": "",
        "journal_ref": "",
        "doi": (paper.get("externalIds") or {}).get("DOI", ""),
        # S2-specific
        "s2_paper_id": paper.get("paperId", ""),
        "citation_count": paper.get("citationCount", 0),
        "reference_count": paper.get("referenceCount", 0),
        "influential_citation_count": paper.get("influentialCitationCount", 0),
        "is_open_access": paper.get("isOpenAccess", False),
    }


def snowball(seed_arxiv_ids: list[str], max_per_seed: int = 50,
             quiet: bool = False) -> list[dict]:
    """Snowball expansion: for each seed, get references + citations.

    Returns deduplicated list of normalized paper dicts (arXiv-linked only).
    Seeds themselves are NOT included in the output.
    """
    seen = set(seed_arxiv_ids)
    results = {}

    for i, seed_id in enumerate(seed_arxiv_ids):
        if not quiet:
            print(f"  [{i+1}/{len(seed_arxiv_ids)}] Snowballing {seed_id}...")

        # References (papers this one cites)
        refs = get_references(seed_id, limit=max_per_seed)
        for ref in refs:
            norm = normalize_s2_paper(ref)
            if norm and norm["arxiv_id"] not in seen:
                seen.add(norm["arxiv_id"])
                results[norm["arxiv_id"]] = norm

        # Citations (papers that cite this one)
        cites = get_citations(seed_id, limit=max_per_seed)
        for cite in cites:
            norm = normalize_s2_paper(cite)
            if norm and norm["arxiv_id"] not in seen:
                seen.add(norm["arxiv_id"])
                results[norm["arxiv_id"]] = norm

        if not quiet:
            print(f"    refs: {len(refs)}, cites: {len(cites)}, "
                  f"new: {len(results)} total unique")

    return list(results.values())
