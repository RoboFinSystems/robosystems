#!/usr/bin/env bash
# Pull Charlie Hoffman's "The World Online" demo dataset from the
# seattlemethod/prototypes GitHub repo into
# local/datasets/seattle_method_world_online/ (gitignored).
#
# Three files (the same shape an accounting system would export):
#   - GeneralLedger.csv       — 22,288 GL lines (the transactions)
#   - ChartOfAccounts.csv     — 239 GL accounts → mini concept mapping
#   - SummaryOfTransactions.csv — the (line-item × business-event) pivot
#                                 Charlie publishes as the expected result;
#                                 reconcile.py validates against this.
#
# Authoritative upstream:
#   https://github.com/seattlemethod/prototypes/tree/main/the-world-online-demo-data
#
# Idempotent — re-runs overwrite the local copy.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
OUT_DIR="$REPO_ROOT/local/datasets/seattle_method_world_online"

BASE_URL="https://raw.githubusercontent.com/seattlemethod/prototypes/main/the-world-online-demo-data"

mkdir -p "$OUT_DIR"

echo "Pulling The World Online dataset → $OUT_DIR"

failed=()
for f in GeneralLedger.csv ChartOfAccounts.csv SummaryOfTransactions.csv; do
  if curl -fsSL "$BASE_URL/$f" -o "$OUT_DIR/$f"; then
    lines=$(wc -l < "$OUT_DIR/$f" | tr -d ' ')
    echo "  ✓ $f ($lines lines)"
  else
    echo "  ✗ $f" >&2
    failed+=("$f")
  fi
done

if [ ${#failed[@]} -gt 0 ]; then
  echo
  echo "Failed to pull ${#failed[@]} required file(s) — aborting." >&2
  exit 1
fi

echo
echo "Next: uv run python -m examples.seattle_method_world_online.main --step load"
