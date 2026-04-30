"""
Coding-agent cell contract.

Every flex module that ingests a coding-agent session store (claude_code,
or another coding-agent source) is expected to produce a cell that
preserves tool-call structure, file bodies, and content bodies — not just
message text. This module defines the contract + a validator any coding-
agent module can call after ingest to catch degradation early.

The contract is imported by coding-agent modules the same way they import
`bootstrap_claude_code_cell`, `ENRICHMENT_STUBS`, etc. — directly from
`flex.modules.claude_code`. One shared canonical check; no per-agent
divergence.
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field


# Tables that MUST exist after ingest for any coding-agent cell.
# A missing table = schema-level violation (probably a transpiler bug).
REQUIRED_TABLES: tuple[str, ...] = (
    "_raw_sources",
    "_raw_chunks",
    "_raw_content",
    "_edges_source",
    "_edges_tool_ops",
    "_edges_raw_content",
    "_types_message",
    "_types_file_body",
)


# Tables that SHOULD have non-zero rows when the source has N or more sources
# and is non-trivial. Zero rows with a non-trivial source = structural loss.
# Each entry: table -> (min_sources_before_check, min_rows_at_that_point)
#
# Defaults are conservative. A few short sessions might legitimately have zero
# tool_ops; ten or more sessions without any tool_ops is a red flag.
STRUCTURED_MINIMUMS: dict[str, tuple[int, int]] = {
    "_edges_tool_ops":      (10, 1),   # at least 1 tool-op total for 10+ sources
    "_types_file_body":     (20, 1),   # at least 1 file body for 20+ sources
    "_raw_content":         (10, 1),   # tool results captured
    "_edges_raw_content":   (10, 1),   # bridge rows linking chunks to content
    "_types_message":       (1,  1),   # every source should produce messages
}


# Tables that are OPTIONAL — enrichment may or may not have run, or the source
# may genuinely lack the signal.
# Documented for operator clarity, not enforced.
OPTIONAL_TABLES: tuple[str, ...] = (
    "_edges_soft_ops",           # soft file-op detection from assistant text
    "_edges_delegations",        # parent→child agent spawns
    "_edges_content_identity",   # SOMA content hashes
    "_edges_file_identity",      # SOMA file UUIDs
    "_edges_repo_identity",      # SOMA repo roots
    "_types_source_warmup",      # warmup classification (runs in enrichment)
    "_enrich_source_graph",      # similarity graph (runs in enrichment)
    "_enrich_file_graph",
    "_enrich_delegation_graph",
    "_enrich_session_summary",   # fingerprints
    "_enrich_repo_identity",
)


@dataclass
class ContractViolation:
    severity: str           # "error" | "warn"
    table: str
    message: str


@dataclass
class ContractReport:
    cell_type: str
    n_sources: int
    violations: list[ContractViolation] = field(default_factory=list)

    @property
    def errors(self) -> list[ContractViolation]:
        return [v for v in self.violations if v.severity == "error"]

    @property
    def warnings(self) -> list[ContractViolation]:
        return [v for v in self.violations if v.severity == "warn"]

    @property
    def ok(self) -> bool:
        return not self.errors

    def summary(self) -> str:
        if self.ok and not self.warnings:
            return f"[contract] ok — {self.n_sources} sources, schema complete"
        out = []
        if self.errors:
            out.append(f"[contract] {len(self.errors)} ERROR(s):")
            for v in self.errors:
                out.append(f"  - {v.table}: {v.message}")
        if self.warnings:
            out.append(f"[contract] {len(self.warnings)} warning(s):")
            for v in self.warnings:
                out.append(f"  - {v.table}: {v.message}")
        return "\n".join(out)


def validate_coding_agent_cell(
    conn: sqlite3.Connection,
    cell_type: str = "unknown",
) -> ContractReport:
    """
    Validate a coding-agent cell against the canonical contract.

    Call after ingest + enrichment. Returns a ContractReport; caller decides
    whether to abort (errors) or warn (warnings).

    Schema-level violations (missing required tables) are errors.
    Structural losses (zero rows when source has N+ sources) are warnings —
    they indicate a lossy transpile but don't break the cell.
    """
    n_sources = conn.execute("SELECT COUNT(*) FROM _raw_sources").fetchone()[0]
    report = ContractReport(cell_type=cell_type, n_sources=n_sources)

    # Schema presence
    existing = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
        )
    }
    for tbl in REQUIRED_TABLES:
        if tbl not in existing:
            report.violations.append(ContractViolation(
                severity="error",
                table=tbl,
                message="required table missing",
            ))

    # Structured minimums
    for tbl, (min_sources, min_rows) in STRUCTURED_MINIMUMS.items():
        if tbl not in existing:
            continue  # already flagged as error above
        if n_sources < min_sources:
            continue  # too small to judge
        n_rows = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        if n_rows < min_rows:
            report.violations.append(ContractViolation(
                severity="warn",
                table=tbl,
                message=(
                    f"expected ≥ {min_rows} rows for {n_sources} sources, "
                    f"got {n_rows}. source data may be lossy (e.g. "
                    f"flattened tool_use into plain text before ingest)."
                ),
            ))

    return report
