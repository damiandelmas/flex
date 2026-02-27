#!/bin/bash
# publish.sh — incremental publish from dev to public GitHub + tag for PyPI
# Private modules stripped. Commit history preserved on public repo.
# PyPI publishing is handled by GitHub Actions on tag push.
#
# Usage:
#   ./publish.sh              # sync to public, commit, tag, push
#   ./publish.sh --dry-run    # show what would change, don't push

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
    publish.sh
    bump.sh
    bin/flex
    flex/mcp_server.service
    flex/modules/claude_code/scripts
    flex/modules/claude_code/manage/enrich_soma_repos.py
    flex/modules/claude_code/manage/backfill_metadata.py
    flex/modules/claude_code/resume.py
)

DRY_RUN=false
if [[ "${1:-}" == "--dry-run" ]]; then
    DRY_RUN=true
fi

# Verify remote exists
if ! git remote get-url "$REMOTE" &>/dev/null; then
    echo "Remote '$REMOTE' not found. Add it:"
    echo "  git remote add $REMOTE git@github.com:damian-delmas/flex.git"
    exit 1
fi

# Verify clean working tree
if [[ -n "$(git status --porcelain --untracked-files=no)" ]]; then
    echo "Working tree not clean. Commit or stash first."
    exit 1
fi

VERSION=$(grep '^version' pyproject.toml | head -1 | grep -oP '"\K[^"]+')

echo "Publishing dev → $REMOTE/$TARGET (v$VERSION)"
echo "Stripping ${#PRIVATE[@]} private paths"

# Fetch latest public history
git fetch "$REMOTE" "$TARGET" 2>/dev/null || true

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

# Check out the existing public branch (preserves history)
git checkout -B "$BRANCH" "$REMOTE/$TARGET"

# Overlay all dev files onto the public branch
git checkout dev -- .

# Remove private files from index
for p in "${PRIVATE[@]}"; do
    git rm -r --cached "$p" 2>/dev/null || true
done

# Only commit+push if there are actual changes
if git diff --cached --quiet; then
    echo "No changes to publish."
    git checkout -f dev
    git branch -D "$BRANCH"
    exit 0
fi

# Commit + tag
git commit -m "release v$VERSION"
git tag -f "v$VERSION"

# Push (regular — preserves history)
git push "$REMOTE" "$BRANCH:$TARGET"
git push "$REMOTE" "v$VERSION" --force

# Cleanup
git checkout -f dev
git branch -D "$BRANCH"
git tag -d "v$VERSION"

echo ""
echo "Published to $REMOTE/$TARGET — tag v$VERSION pushed, PyPI release triggered"
