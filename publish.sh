#!/bin/bash
# publish.sh — push filtered dev branch to public GitHub + publish to PyPI
# Private modules stripped, public gets a clean snapshot.
#
# Usage:
#   ./publish.sh              # push to public remote + publish to PyPI
#   ./publish.sh --dry-run    # show what would be removed, don't push
#
# PyPI token: store in ~/.flex/pypi-token (never commit)

set -euo pipefail

REMOTE="public"
BRANCH="_pub"
TARGET="main"

# Private paths to exclude from public
PRIVATE=(
    flex/modules/claude_chat
    flex/modules/docpac
    flex/modules/soma
    flex/compile/docpac.py
    flex/compile/markdown.py
    views/claude_chat
    views/docpac
    operations
    scripts
    tests/test_docpac.py
    tests/test_docpac_worker.py
    tests/test_fingerprint.py
    tests/test_markdown.py
    tests/test_soma_module.py
    tests/test_unified_sync.py
)

DRY_RUN=false
if [[ "${1:-}" == "--dry-run" ]]; then
    DRY_RUN=true
fi

# Verify remote exists
if ! git remote get-url "$REMOTE" &>/dev/null; then
    echo "Remote '$REMOTE' not found. Add it:"
    echo "  git remote add $REMOTE git@github.com:axpsystems/flex.git"
    exit 1
fi

# Verify clean working tree
if [[ -n "$(git status --porcelain --untracked-files=no)" ]]; then
    echo "Working tree not clean. Commit or stash first."
    exit 1
fi

echo "Publishing dev → $REMOTE/$TARGET"
echo "Stripping ${#PRIVATE[@]} private paths"

if $DRY_RUN; then
    echo ""
    echo "Would remove:"
    for p in "${PRIVATE[@]}"; do
        if git ls-files "$p" | grep -q .; then
            echo "  $p"
        fi
    done
    echo ""
    echo "Dry run — no changes made."
    exit 0
fi

# Create orphan branch from dev
git checkout --orphan "$BRANCH" dev

# Remove private files from index (keep on disk via orphan reset)
for p in "${PRIVATE[@]}"; do
    git rm -r --cached "$p" 2>/dev/null || true
done

# Commit
VERSION=$(git describe --tags 2>/dev/null || echo "dev")
git commit -m "release $VERSION"

# Push
git push "$REMOTE" "$BRANCH:$TARGET" --force

# Cleanup
git checkout -f dev
git branch -D "$BRANCH"

echo ""
echo "Published to $REMOTE/$TARGET"

# ── PyPI ──────────────────────────────────────────────────────────────────────
PYPI_TOKEN_FILE="$HOME/.flex/pypi-token"

if [[ ! -f "$PYPI_TOKEN_FILE" ]]; then
    echo ""
    echo "PyPI token not found at $PYPI_TOKEN_FILE — skipping PyPI publish."
    echo "  echo '<your-token>' > $PYPI_TOKEN_FILE"
    exit 0
fi

VERSION=$(grep '^version' pyproject.toml | head -1 | grep -oP '"\K[^"]+')
echo "Building getflex==$VERSION for PyPI..."

rm -rf dist/
python -m build 2>&1 | tail -3

TWINE_USERNAME=__token__ \
TWINE_PASSWORD="$(cat "$PYPI_TOKEN_FILE")" \
twine upload dist/getflex-"$VERSION"* 2>&1

echo ""
echo "Published getflex==$VERSION to PyPI"
