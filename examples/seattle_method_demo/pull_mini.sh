#!/usr/bin/env bash
# Pull Charlie Hoffman's mini reporting framework from xbrlsite into
# local/taxonomies/mini/ (gitignored).
#
# Parses mini-entryPoint.xsd for the linkbase file list, then curls each
# referenced file. Idempotent — re-runs overwrite the local copy.
#
# Authoritative upstream: https://github.com/seattlemethod/mini
# We pull from xbrlsite because that's where the URLs in the entry
# point's linkbaseRef hrefs resolve. The GitHub repo has additional
# context (fac/, disclosure-mechanics/, disclosures-topics/,
# reporting-checklist/, type-subtype/) that future work — the rule
# corpus port and the enrichment engine — will want; for Test Case 1
# the base-taxonomy files are sufficient.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
OUT_DIR="$REPO_ROOT/local/taxonomies/mini"

BASE_URL="https://xbrlsite.azurewebsites.net/2023/reporting-scheme/mini/base-taxonomy"

mkdir -p "$OUT_DIR"

echo "Pulling mini taxonomy → $OUT_DIR"

# Pull the entry point + concept schema first.
for f in mini-entryPoint.xsd mini.xsd; do
  if curl -fsSL "$BASE_URL/$f" -o "$OUT_DIR/$f"; then
    echo "  ✓ $f"
  else
    echo "  ✗ $f (required — aborting)" >&2
    exit 1
  fi
done

# Parse linkbase file refs from the entry point. Each linkbaseRef has
# `xlink:href='mini-foo.xml'`; we extract the unique set.
mapfile -t LINKBASES < <(
  grep -oE "xlink:href='[^']+'" "$OUT_DIR/mini-entryPoint.xsd" \
    | sed -E "s/xlink:href='([^']+)'/\1/" \
    | sort -u
)

echo
echo "Found ${#LINKBASES[@]} linkbase reference(s) in mini-entryPoint.xsd"
echo

pulled=0
failed=()
for f in "${LINKBASES[@]}"; do
  # Skip schema files (already pulled).
  case "$f" in
    *.xsd) continue ;;
  esac
  if curl -fsSL "$BASE_URL/$f" -o "$OUT_DIR/$f"; then
    pulled=$((pulled + 1))
  else
    failed+=("$f")
  fi
done

echo "Pulled $pulled linkbase file(s) to $OUT_DIR"
if [ ${#failed[@]} -gt 0 ]; then
  echo
  echo "Failed (${#failed[@]}):"
  for f in "${failed[@]}"; do
    echo "  ✗ $f"
  done
fi

echo
echo "Total files: $(ls "$OUT_DIR" | wc -l | tr -d ' ')"
echo
echo "Next: uv run python -m examples.seattle_method_demo.load_taxonomy"
