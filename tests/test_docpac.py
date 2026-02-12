"""
Tests for flexsearch/compile/docpac.py

Tests parse_docpac(): walk a doc-pac directory, map folders to semantic metadata.

Contract:
  parse_docpac(root, pattern='**/*.md') -> list[DocPacEntry]
  DocPacEntry: path, temporal, doc_type, title, file_date, skip

Key principles tested:
  - Temporal identity is relative (frame resets at doc-pac boundaries)
  - Any dir with indicator folders (changes/, current/, intended/) is a boundary
  - file_date (calendar) and temporal (semantic) are separate dimensions
  - Facets are NOT auto-detected (domain concept, assigned by init script)

Run with: pytest tests/test_docpac.py -v
"""
import pytest
from pathlib import Path


def _can_import():
    try:
        from flexsearch.compile.docpac import parse_docpac, DocPacEntry
        return True
    except ImportError:
        return False


pytestmark = pytest.mark.skipif(
    not _can_import(),
    reason="flexsearch.compile.docpac not yet implemented (Plan 1)"
)


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def docpac_tree(tmp_path):
    """Create a realistic doc-pac folder structure with .md files."""
    root = tmp_path / "context"
    # changes/code — past, changelog
    (root / "changes" / "code").mkdir(parents=True)
    (root / "changes" / "code" / "refactor-log.md").write_text("# Refactor log")

    # changes/design — future, design
    (root / "changes" / "design").mkdir(parents=True)
    (root / "changes" / "design" / "new-idea.md").write_text("# Design idea")

    # changes/testing — past, testing
    (root / "changes" / "testing").mkdir(parents=True)
    (root / "changes" / "testing" / "test-results.md").write_text("# Test results")

    # current — present, architecture
    (root / "current").mkdir(parents=True)
    (root / "current" / "architecture.md").write_text("# Architecture")
    (root / "current" / "schema.md").write_text("# Schema")

    # intended/proximate — future, plan
    (root / "intended" / "proximate").mkdir(parents=True)
    (root / "intended" / "proximate" / "migration.md").write_text("# Migration plan")

    # knowledge — exogenous, knowledge
    (root / "knowledge").mkdir(parents=True)
    (root / "knowledge" / "sqlite-fts.md").write_text("# SQLite FTS5 reference")

    # buffer — skip
    (root / "buffer").mkdir(parents=True)
    (root / "buffer" / "scratch.md").write_text("# Scratch notes")

    # _raw — skip
    (root / "_raw").mkdir(parents=True)
    (root / "_raw" / "dump.md").write_text("raw data")

    return root


@pytest.fixture
def nested_docpac(tmp_path):
    """Doc-pac with nested doc-pacs under intended/proximate/ and plans/."""
    root = tmp_path / "context"

    # Top-level current (makes root a doc-pac)
    (root / "current").mkdir(parents=True)
    (root / "current" / "arch.md").write_text("# Top arch")

    # Nested under intended/proximate/sql-first/ — IS a doc-pac
    feature = root / "intended" / "proximate" / "sql-first"
    (feature / "changes" / "code").mkdir(parents=True)
    (feature / "changes" / "code" / "sql-change.md").write_text("# SQL change")
    (feature / "current").mkdir(parents=True)
    (feature / "current" / "status.md").write_text("# SQL status")

    # Nested under plans/plan-a/ — also a doc-pac
    plan = root / "plans" / "plan-a"
    (plan / "changes" / "code").mkdir(parents=True)
    (plan / "changes" / "code" / "impl-log.md").write_text("# Implementation log")
    (plan / "current").mkdir(parents=True)
    (plan / "current" / "progress.md").write_text("# Progress")
    (plan / "intended").mkdir(parents=True)
    (plan / "intended" / "next-steps.md").write_text("# Next")

    return root


# =============================================================================
# Folder Mapping Tests
# =============================================================================

class TestFolderMapping:
    """Folder names map to (temporal, doc_type) tuples."""

    def test_changes_code_is_past_changelog(self, docpac_tree):
        from flexsearch.compile.docpac import parse_docpac
        entries = parse_docpac(str(docpac_tree))
        code_entries = [e for e in entries if 'changes/code' in e.path]
        assert len(code_entries) == 1
        assert code_entries[0].temporal == 'past'
        assert code_entries[0].doc_type == 'changelog'

    def test_changes_design_is_future_design(self, docpac_tree):
        from flexsearch.compile.docpac import parse_docpac
        entries = parse_docpac(str(docpac_tree))
        design_entries = [e for e in entries if 'changes/design' in e.path]
        assert len(design_entries) == 1
        assert design_entries[0].temporal == 'future'
        assert design_entries[0].doc_type == 'design'

    def test_current_is_present_architecture(self, docpac_tree):
        from flexsearch.compile.docpac import parse_docpac
        entries = parse_docpac(str(docpac_tree))
        current_entries = [e for e in entries if '/current/' in e.path and not e.skip]
        assert len(current_entries) == 2  # architecture.md + schema.md
        for e in current_entries:
            assert e.temporal == 'present'
            assert e.doc_type == 'architecture'

    def test_intended_proximate_is_future_plan(self, docpac_tree):
        from flexsearch.compile.docpac import parse_docpac
        entries = parse_docpac(str(docpac_tree))
        plan_entries = [e for e in entries if 'intended/proximate' in e.path]
        assert len(plan_entries) >= 1
        for e in plan_entries:
            assert e.temporal == 'future'
            assert e.doc_type == 'plan'

    def test_knowledge_is_exogenous(self, docpac_tree):
        from flexsearch.compile.docpac import parse_docpac
        entries = parse_docpac(str(docpac_tree))
        know_entries = [e for e in entries if '/knowledge/' in e.path]
        assert len(know_entries) == 1
        assert know_entries[0].temporal == 'exogenous'
        assert know_entries[0].doc_type == 'knowledge'


# =============================================================================
# Skip Folders
# =============================================================================

class TestSkipFolders:
    """buffer/, _raw/, _qmem/, cache/ are marked skip=True."""

    def test_buffer_is_skipped(self, docpac_tree):
        from flexsearch.compile.docpac import parse_docpac
        entries = parse_docpac(str(docpac_tree))
        buffer_entries = [e for e in entries if '/buffer/' in e.path]
        assert all(e.skip for e in buffer_entries), "buffer/ files should be skipped"

    def test_raw_is_skipped(self, docpac_tree):
        from flexsearch.compile.docpac import parse_docpac
        entries = parse_docpac(str(docpac_tree))
        raw_entries = [e for e in entries if '/_raw/' in e.path]
        assert all(e.skip for e in raw_entries), "_raw/ files should be skipped"

    def test_indexable_excludes_skipped(self, docpac_tree):
        from flexsearch.compile.docpac import parse_docpac
        entries = parse_docpac(str(docpac_tree))
        indexable = [e for e in entries if not e.skip]
        for e in indexable:
            assert '/buffer/' not in e.path
            assert '/_raw/' not in e.path


# =============================================================================
# Frame Boundary Tests — the core fix
# =============================================================================

class TestFrameBoundary:
    """Temporal resolution resets at doc-pac boundaries.
    Any dir with indicator folders is a boundary.
    Inner changes/code/ is past even under intended/proximate/."""

    def test_nested_changelog_is_past(self, nested_docpac):
        """The whole point: changes/code inside a nested doc-pac is PAST,
        not future. Frame resets at the boundary."""
        from flexsearch.compile.docpac import parse_docpac
        entries = parse_docpac(str(nested_docpac))
        nested_code = [e for e in entries if 'sql-first/changes/code' in e.path]
        assert len(nested_code) == 1
        assert nested_code[0].temporal == 'past'
        assert nested_code[0].doc_type == 'changelog'

    def test_nested_current_is_present(self, nested_docpac):
        """current/ inside a nested doc-pac is PRESENT, not future."""
        from flexsearch.compile.docpac import parse_docpac
        entries = parse_docpac(str(nested_docpac))
        nested_current = [e for e in entries if 'sql-first/current' in e.path]
        assert len(nested_current) == 1
        assert nested_current[0].temporal == 'present'

    def test_plans_changelog_is_past(self, nested_docpac):
        """plans/plan-a/ is also a doc-pac boundary. Its changes/code/ is past."""
        from flexsearch.compile.docpac import parse_docpac
        entries = parse_docpac(str(nested_docpac))
        plan_code = [e for e in entries if 'plan-a/changes/code' in e.path]
        assert len(plan_code) == 1
        assert plan_code[0].temporal == 'past'
        assert plan_code[0].doc_type == 'changelog'

    def test_plans_intended_is_future(self, nested_docpac):
        """plans/plan-a/intended/ resolves to future relative to plan-a boundary."""
        from flexsearch.compile.docpac import parse_docpac
        entries = parse_docpac(str(nested_docpac))
        plan_intended = [e for e in entries if 'plan-a/intended' in e.path]
        assert len(plan_intended) == 1
        assert plan_intended[0].temporal == 'future'

    def test_top_level_unaffected(self, nested_docpac):
        """Top-level current/ still resolves to present at root boundary."""
        from flexsearch.compile.docpac import parse_docpac
        entries = parse_docpac(str(nested_docpac))
        top = [e for e in entries if e.path.endswith('/current/arch.md')]
        assert len(top) == 1
        assert top[0].temporal == 'present'


# =============================================================================
# Facet — NOT auto-detected
# =============================================================================

class TestFacetRemoved:
    """Facets are domain concepts assigned by init scripts, not docpac."""

    def test_entries_have_no_facet_attribute(self, nested_docpac):
        from flexsearch.compile.docpac import parse_docpac
        entries = parse_docpac(str(nested_docpac))
        assert len(entries) > 0
        assert not hasattr(entries[0], 'facet'), \
            "DocPacEntry should not have facet — facets are assigned by init scripts"


# =============================================================================
# file_date — Calendar Time (Separate from Semantic Temporal)
# =============================================================================

class TestFileDate:
    """file_date carries calendar time from filename.
    temporal carries semantic time from folder.
    They never conflate."""

    def test_yymmdd_extracted_to_file_date(self, tmp_path):
        from flexsearch.compile.docpac import parse_docpac
        root = tmp_path / "ctx"
        (root / "changes" / "code").mkdir(parents=True)
        (root / "changes" / "code" / "260211-refactor.md").write_text("# Log")
        entries = parse_docpac(str(root))
        code = [e for e in entries if 'changes/code' in e.path]
        assert code[0].file_date == '260211'
        assert code[0].temporal == 'past'  # semantic, not overridden

    def test_yymmdd_hhmm_extracted(self, tmp_path):
        from flexsearch.compile.docpac import parse_docpac
        root = tmp_path / "ctx"
        (root / "changes" / "code").mkdir(parents=True)
        (root / "changes" / "code" / "260211-1538_sql-refactor.md").write_text("# Log")
        entries = parse_docpac(str(root))
        code = [e for e in entries if 'changes/code' in e.path]
        assert code[0].file_date == '260211-1538'
        assert code[0].temporal == 'past'

    def test_no_date_prefix_is_none(self, tmp_path):
        from flexsearch.compile.docpac import parse_docpac
        root = tmp_path / "ctx"
        (root / "current").mkdir(parents=True)
        (root / "current" / "architecture.md").write_text("# Arch")
        entries = parse_docpac(str(root))
        assert entries[0].file_date is None


# =============================================================================
# Edge Cases
# =============================================================================

class TestAllFolderMappings:
    """Parametrized test covering all 22 FOLDER_MAP entries."""

    @pytest.mark.parametrize("folder_key,expected", [
        ('changes/code',     ('past', 'changelog')),
        ('changes/testing',  ('past', 'testing')),
        ('changes/workflow', ('past', 'workflow')),
        ('changes/states',   ('past', 'states')),
        ('changes/tracking', ('past', 'tracking')),
        ('changes/audits',   ('past', 'audit')),
        ('changes/review',   ('past', 'review')),
        ('changes/design',   ('future', 'design')),
        ('changes/session',  ('past', 'session')),
        ('current/ast',      ('present', 'ast')),
        ('current',          ('present', 'architecture')),
        ('intended/proximate', ('future', 'plan')),
        ('intended/ultimate',  ('future', 'vision')),
        ('intended',         ('future', 'plan')),
        ('knowledge',        ('exogenous', 'knowledge')),
        ('philosophy',       ('exogenous', 'philosophy')),
        ('onboard',          ('present', 'onboard')),
        ('lexicon',          ('present', 'lexicon')),
        ('reference',        ('present', 'reference')),
        ('specs',            ('future', 'spec')),
        ('slots',            ('future', 'slot')),
        ('plans',            ('future', 'plan')),
    ])
    def test_folder_map_entry(self, tmp_path, folder_key, expected):
        from flexsearch.compile.docpac import parse_docpac
        root = tmp_path / "ctx"
        target = root / folder_key
        target.mkdir(parents=True)
        (target / "test-file.md").write_text("# Test")
        entries = parse_docpac(str(root))
        indexable = [e for e in entries if not e.skip]
        assert len(indexable) == 1, f"Expected 1 entry for {folder_key}, got {len(indexable)}"
        assert indexable[0].temporal == expected[0], \
            f"{folder_key}: expected temporal={expected[0]}, got {indexable[0].temporal}"
        assert indexable[0].doc_type == expected[1], \
            f"{folder_key}: expected doc_type={expected[1]}, got {indexable[0].doc_type}"


class TestSegmentMatching:
    """Path matching is segment-based, not substring-based."""

    def test_no_false_positive_on_substring(self, tmp_path):
        """'changes' should NOT match a folder named 'prechanges'."""
        from flexsearch.compile.docpac import parse_docpac
        root = tmp_path / "ctx"
        (root / "prechanges" / "code").mkdir(parents=True)
        (root / "prechanges" / "code" / "file.md").write_text("# Test")
        entries = parse_docpac(str(root))
        indexable = [e for e in entries if not e.skip]
        # Should NOT match changes/code — 'prechanges' is not 'changes'
        for e in indexable:
            assert e.temporal is None or e.doc_type != 'changelog', \
                f"False positive: 'prechanges/code' matched as changelog"


class TestTitle:
    """_extract_title produces human-readable titles."""

    def test_strips_temporal_prefix(self, tmp_path):
        from flexsearch.compile.docpac import parse_docpac
        root = tmp_path / "ctx"
        (root / "changes" / "code").mkdir(parents=True)
        (root / "changes" / "code" / "260211-1538_sql-refactor.md").write_text("# Log")
        entries = parse_docpac(str(root))
        code = [e for e in entries if 'changes/code' in e.path]
        assert 'sql refactor' in code[0].title.lower()


class TestEdgeCases:
    """Edge cases and return format."""

    def test_returns_list_of_dataclass(self, docpac_tree):
        from flexsearch.compile.docpac import parse_docpac, DocPacEntry
        entries = parse_docpac(str(docpac_tree))
        assert isinstance(entries, list)
        assert all(isinstance(e, DocPacEntry) for e in entries)

    def test_paths_are_absolute(self, docpac_tree):
        from flexsearch.compile.docpac import parse_docpac
        entries = parse_docpac(str(docpac_tree))
        for e in entries:
            assert Path(e.path).is_absolute(), f"Path should be absolute: {e.path}"

    def test_empty_directory(self, tmp_path):
        from flexsearch.compile.docpac import parse_docpac
        empty = tmp_path / "empty"
        empty.mkdir()
        entries = parse_docpac(str(empty))
        assert entries == []

    def test_custom_pattern(self, docpac_tree):
        from flexsearch.compile.docpac import parse_docpac
        entries_md = parse_docpac(str(docpac_tree), pattern='**/*.md')
        assert len(entries_md) > 0
