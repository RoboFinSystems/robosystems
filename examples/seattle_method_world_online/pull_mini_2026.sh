#!/usr/bin/env bash
# Pull Charlie Hoffman's MINI 2026 reporting framework from xbrlsite into
# local/taxonomies/mini-2026/ (gitignored).
#
# Mirrors examples/seattle_method_demo/pull_mini.sh, but points at the
# 2026 "reporting-framework" path (the 2023 "reporting-scheme" path is a
# different, older release). The World Online dataset is tagged against
# MINI 2026, so we load that release rather than reusing the 2023 mini.
#
# The 2026 entry point references a richer linkbase set than 2023 — it
# adds TrialBalance, subclassification Detail networks, and policy/
# disclosure notes. load_taxonomy.py only parses mini.xsd + mini-lab.xml
# + mini-pre-*.xml; the extra files are pulled (harmless) and ignored by
# the parser. Idempotent — re-runs overwrite the local copy.
#
# Authoritative upstream:
#   https://xbrlsite.azurewebsites.net/2026/reporting-framework/mini/base-taxonomy/

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
OUT_DIR="$REPO_ROOT/local/taxonomies/mini-2026"

BASE_URL="https://xbrlsite.azurewebsites.net/2026/reporting-framework/mini/base-taxonomy"

mkdir -p "$OUT_DIR"

echo "Pulling MINI 2026 taxonomy → $OUT_DIR"

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
  # Skip schema files (already pulled) and absolute URLs (xbrl.org core).
  case "$f" in
    *.xsd) continue ;;
    http*) continue ;;
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
echo "Next: uv run python -m examples.seattle_method_world_online.main --step load"
