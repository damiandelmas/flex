#!/bin/bash
# bump.sh — increment patch version, commit, and publish
# Usage: ./bump.sh

set -euo pipefail
cd "$(dirname "$0")"

CURRENT=$(grep '^version' pyproject.toml | head -1 | grep -oP '"\K[^"]+')
IFS='.' read -r MAJOR MINOR PATCH <<< "$CURRENT"
NEW="$MAJOR.$MINOR.$((PATCH + 1))"

sed -i "s/version = \"$CURRENT\"/version = \"$NEW\"/" pyproject.toml
echo "$CURRENT → $NEW"

git add pyproject.toml
git commit -m "release: v$NEW"
bash publish.sh
