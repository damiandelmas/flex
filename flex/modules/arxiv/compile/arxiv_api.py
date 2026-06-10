"""
arXiv API client.

Pulls paper metadata and LaTeX source from arXiv.
Used by both the one-shot worker and the incremental refresh script.

API docs: https://info.arxiv.org/help/api/user-manual.html
Rate limit: 1 request per 3 seconds (respect this).

LaTeX source: downloaded per-paper via export.arxiv.org/e-print/{id}.
Returns a tarball containing .tex files. We extract and concatenate.
"""

import io
import gzip
import json
import re
import sys
import tarfile
import time
import urllib.request
import urllib.parse
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path


BASE_URL = "http://export.arxiv.org/api/query"
EPRINT_URL = "https://export.arxiv.org/e-print"
USER_AGENT = "flex-arxiv/1.0"
BATCH_SIZE = 50  # public default request size; API max is higher
DELAY = 3.0  # arXiv asks clients to wait at least 3 seconds between requests

ATOM_NS = "http://www.w3.org/2005/Atom"
ARXIV_NS = "http://arxiv.org/schemas/atom"


def api_fetch(params: dict) -> str:
    """Fetch from arXiv API. Returns raw XML string."""
    qs = urllib.parse.urlencode(params)
    url = f"{BASE_URL}?{qs}"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return resp.read().decode("utf-8")
    except Exception as e:
        print(f"  [!] arXiv API — {e}", file=sys.stderr)
        return ""


def _parse_entry(entry: ET.Element) -> dict:
    """Parse a single Atom entry into a normalized dict."""
    def text(tag, ns=ATOM_NS):
        el = entry.find(f"{{{ns}}}{tag}")
        return el.text.strip() if el is not None and el.text else ""

    # arXiv ID from the <id> URL
    raw_id = text("id")
    arxiv_id = raw_id.split("/abs/")[-1] if "/abs/" in raw_id else raw_id

    # Strip version suffix for source_id (keep raw for reference)
    arxiv_id_base = re.sub(r"v\d+$", "", arxiv_id)

    # Authors
    authors = []
    for author_el in entry.findall(f"{{{ATOM_NS}}}author"):
        name_el = author_el.find(f"{{{ATOM_NS}}}name")
        if name_el is not None and name_el.text:
            authors.append(name_el.text.strip())

    # Categories
    categories = []
    for cat_el in entry.findall(f"{{{ATOM_NS}}}category"):
        term = cat_el.get("term", "")
        if term:
            categories.append(term)

    primary_cat_el = entry.find(f"{{{ARXIV_NS}}}primary_category")
    primary_category = primary_cat_el.get("term", "") if primary_cat_el is not None else ""

    # Links
    pdf_url = ""
    for link_el in entry.findall(f"{{{ATOM_NS}}}link"):
        if link_el.get("title") == "pdf":
            pdf_url = link_el.get("href", "")

    # Dates
    published = text("published")
    updated = text("updated")
    pub_ts = 0
    if published:
        try:
            pub_ts = int(datetime.fromisoformat(published.replace("Z", "+00:00")).timestamp())
        except (ValueError, OSError):
            pass

    # Optional arXiv-specific fields
    comment = text("comment", ARXIV_NS)
    journal_ref = text("journal_ref", ARXIV_NS)
    doi = text("doi", ARXIV_NS)

    return {
        "id": arxiv_id_base,
        "arxiv_id": arxiv_id,
        "arxiv_id_base": arxiv_id_base,
        "title": " ".join(text("title").split()),  # collapse whitespace
        "abstract": " ".join(text("summary").split()),
        "authors": authors,
        "authors_str": ", ".join(authors),
        "primary_category": primary_category,
        "categories": categories,
        "categories_str": ", ".join(categories),
        "published": published,
        "updated": updated,
        "created_utc": pub_ts,
        "pdf_url": pdf_url,
        "abs_url": f"https://arxiv.org/abs/{arxiv_id_base}",
        "comment": comment,
        "journal_ref": journal_ref,
        "doi": doi,
    }


def pull_papers(query: str, after_ts: int = 0, max_total: int = 500,
                sort_by: str = "submittedDate", sort_order: str = "descending",
                quiet: bool = False) -> list[dict]:
    """Pull papers matching query from arXiv API.

    Args:
        query: arXiv search query (supports field prefixes: ti:, au:, abs:, cat:, all:)
        after_ts: Only return papers published after this unix timestamp
        max_total: Maximum papers to return
        sort_by: relevance | lastUpdatedDate | submittedDate
        sort_order: ascending | descending

    Returns:
        List of normalized paper dicts.
    """
    all_papers = []
    start = 0

    while start < max_total:
        batch_size = min(BATCH_SIZE, max_total - start)
        params = {
            "search_query": query,
            "start": start,
            "max_results": batch_size,
            "sortBy": sort_by,
            "sortOrder": sort_order,
        }

        xml_str = api_fetch(params)
        if not xml_str:
            break

        try:
            root = ET.fromstring(xml_str)
        except ET.ParseError as e:
            print(f"  [!] XML parse error: {e}", file=sys.stderr)
            break

        entries = root.findall(f"{{{ATOM_NS}}}entry")
        if not entries:
            break

        for entry in entries:
            paper = _parse_entry(entry)

            # Filter by timestamp if requested
            if after_ts and paper["created_utc"] < after_ts:
                continue

            all_papers.append(paper)

        if not quiet:
            print(f"  papers: {len(all_papers)} (batch {start}-{start + len(entries)})",
                  end="\r")

        start += len(entries)
        if len(entries) < batch_size:
            break
        if start >= max_total:
            break

        time.sleep(DELAY)

    if not quiet:
        print(f"  papers: {len(all_papers)} total{' ' * 30}")

    return all_papers


def pull_by_ids(ids: list[str], quiet: bool = False) -> list[dict]:
    """Pull papers by arXiv ID using the id_list API parameter.

    Args:
        ids: list of arXiv IDs (e.g. ["2007.04612", "1703.05175"])

    Returns:
        List of normalized paper dicts.
    """
    all_papers = []
    # API accepts up to ~50 IDs per request
    for i in range(0, len(ids), BATCH_SIZE):
        batch = ids[i:i + BATCH_SIZE]
        params = {
            "id_list": ",".join(batch),
            "max_results": len(batch),
        }
        xml_str = api_fetch(params)
        if not xml_str:
            continue

        try:
            root = ET.fromstring(xml_str)
        except ET.ParseError as e:
            print(f"  [!] XML parse error: {e}", file=sys.stderr)
            continue

        entries = root.findall(f"{{{ATOM_NS}}}entry")
        for entry in entries:
            paper = _parse_entry(entry)
            if paper["title"]:  # skip error entries
                all_papers.append(paper)

        if not quiet:
            print(f"  fetched {len(all_papers)}/{len(ids)} papers")

        if i + BATCH_SIZE < len(ids):
            time.sleep(DELAY)

    return all_papers


def download_source(arxiv_id: str, quiet: bool = False) -> str | None:
    """Download LaTeX source for a paper. Returns raw .tex content or None.

    The e-print endpoint returns either:
    - A gzipped tar archive containing .tex files
    - A single gzipped .tex file
    - A PDF (if no source is available)
    """
    arxiv_id_clean = re.sub(r"v\d+$", "", arxiv_id)
    url = f"{EPRINT_URL}/{arxiv_id_clean}"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})

    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            data = resp.read()
            content_type = resp.headers.get("Content-Type", "")
    except Exception as e:
        if not quiet:
            print(f"  [!] source download {arxiv_id}: {e}", file=sys.stderr)
        return None

    # PDF — no source available
    if content_type.startswith("application/pdf"):
        return None

    # Try as tar.gz first
    try:
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as tar:
            tex_contents = []
            for member in tar.getmembers():
                if member.name.endswith(".tex") and member.isfile():
                    f = tar.extractfile(member)
                    if f:
                        tex_contents.append(f.read().decode("utf-8", errors="replace"))
            if tex_contents:
                # Return the longest .tex file (likely the main paper)
                return max(tex_contents, key=len)
    except (tarfile.TarError, gzip.BadGzipFile):
        pass

    # Try as single gzipped file
    try:
        decompressed = gzip.decompress(data).decode("utf-8", errors="replace")
        if "\\begin{document}" in decompressed or "\\section" in decompressed:
            return decompressed
    except (gzip.BadGzipFile, UnicodeDecodeError):
        pass

    # Try as plain text
    try:
        text = data.decode("utf-8", errors="replace")
        if "\\begin{document}" in text or "\\section" in text:
            return text
    except UnicodeDecodeError:
        pass

    return None
