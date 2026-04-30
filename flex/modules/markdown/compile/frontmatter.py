"""Frontmatter parsing for markdown files — tags, aliases, dates, flattening."""

import json
from datetime import datetime, date

from flex.compile.markdown import extract_frontmatter as _base_extract


def parse_frontmatter(text: str) -> tuple[dict, str]:
    """Extract YAML frontmatter and return (metadata_dict, body_without_frontmatter).

    Returns ({}, text) if no frontmatter found.
    """
    return _base_extract(text)


def extract_tags(fm: dict) -> list[str]:
    """Normalize tags from frontmatter. Handles list, CSV string, single value."""
    raw = fm.get("tags")
    if raw is None:
        return []

    if isinstance(raw, list):
        return [str(x).casefold().strip() for x in raw if x and str(x).strip()]

    if not isinstance(raw, str) or not raw.strip():
        return []

    trimmed = raw.strip()

    # Try JSON array
    try:
        parsed = json.loads(trimmed)
        if isinstance(parsed, list):
            return [str(x).casefold().strip() for x in parsed if x and str(x).strip()]
    except (json.JSONDecodeError, ValueError):
        pass

    # CSV or single value
    if ',' in trimmed:
        return [t.casefold().strip() for t in trimmed.split(',') if t.strip()]

    return [trimmed.casefold()]


def extract_aliases(fm: dict) -> list[str]:
    """Normalize aliases from frontmatter. Handles list, JSON string, single value."""
    raw = fm.get("aliases")
    if raw is None:
        return []

    if isinstance(raw, list):
        return [str(x).strip() for x in raw if x and str(x).strip()]

    if not isinstance(raw, str) or not raw.strip():
        return []

    trimmed = raw.strip()
    try:
        parsed = json.loads(trimmed)
        if isinstance(parsed, list):
            return [str(x).strip() for x in parsed if x and str(x).strip()]
    except (json.JSONDecodeError, ValueError):
        pass

    return [trimmed]


def parse_date(value) -> str | None:
    """Parse frontmatter date to ISO 8601 string.

    Handles: datetime objects (yaml.safe_load produces these),
    date objects, and strings in common formats.
    """
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day).isoformat()
    if isinstance(value, str):
        for fmt in ["%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"]:
            try:
                return datetime.strptime(value, fmt).isoformat()
            except ValueError:
                continue
    return None


def extract_created_date(fm: dict) -> str | None:
    """Extract created date from frontmatter with fallback priority."""
    for key in ('created', 'date_created', 'date'):
        val = fm.get(key)
        if val is not None:
            result = parse_date(val)
            if result:
                return result
    return None


def flatten_frontmatter(fm: dict, prefix: str = "") -> list[tuple[str, str]]:
    """Flatten nested dict to (dotted_key, string_value) pairs."""
    fields = []
    for key, value in fm.items():
        if not key or not str(key).strip():
            continue
        field_name = f"{prefix}.{key}" if prefix else str(key)

        if value is None:
            continue
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    fields.extend(flatten_frontmatter(item, field_name))
                elif item is not None:
                    sv = str(item).strip()
                    if sv:
                        fields.append((field_name, sv))
        elif isinstance(value, dict):
            fields.extend(flatten_frontmatter(value, field_name))
        else:
            sv = str(value).strip()
            if sv:
                fields.append((field_name, sv))

    return fields
