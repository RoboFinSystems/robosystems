#!/usr/bin/env bash
# Pull Charlie Hoffman's 14-JE lemonade-stand GeneralJournal.csv from
# the seattlemethod/prototypes GitHub repo into local/datasets/seattle_method/
# (gitignored). Idempotent — re-runs overwrite the local copy.
#
# Authoritative upstream:
#   https://github.com/seattlemethod/prototypes/tree/main/record-to-report
#
# Charlie's repo has three input shapes for the same 14 JEs:
#   - journal-entries-csv/GeneralJournal.csv       (what this script pulls)
#   - journal-entries-xbrl-global-ledger/...       (XBRL GL instance XML)
#   - journal-entries-typed-members/...            (XBRL Dimensions typed)
# Test Case 1 ingests the CSV; XBRL GL + typed-members ingestors are
# forward work (a different ingest adapter, same downstream pipeline).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
OUT_DIR="$REPO_ROOT/local/datasets/seattle_method"
OUT_FILE="$OUT_DIR/GeneralJournal.csv"

BASE_URL="https://raw.githubusercontent.com/seattlemethod/prototypes/main/record-to-report/journal-entries-csv"

mkdir -p "$OUT_DIR"

echo "Pulling GeneralJournal.csv → $OUT_FILE"

if curl -fsSL "$BASE_URL/GeneralJournal.csv" -o "$OUT_FILE"; then
  lines=$(wc -l < "$OUT_FILE" | tr -d ' ')
  echo "  ✓ GeneralJournal.csv ($lines lines)"
else
  echo "  ✗ GeneralJournal.csv (required — aborting)" >&2
  exit 1
fi

echo
echo "Next: uv run python -m examples.seattle_method_demo.ingest_transactions"
