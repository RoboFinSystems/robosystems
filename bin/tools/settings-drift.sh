#!/bin/bash
# Which settings does a release know about that this deployment's .env does not?
#
#   bin/tools/settings-drift.sh [.env] [.env.example] [--strict]
#
# The example file is the catalog: a line `KEY=value` is a setting the
# example sets, a line `# KEY=value` is one it names as optional. A key counts
# as present in .env whether it is set or commented out there, because a
# commented key shows the operator has seen it. Prints the two groups that are
# missing. Exits 0, or 2 with --strict when a key the example sets is missing,
# so an upgrade can refuse to proceed on an unread release.
set -euo pipefail

ENV_FILE=.env
EXAMPLE_FILE=.env.example
STRICT=0
POSITIONAL=0
for ARG in "$@"; do
  case "$ARG" in
  --strict) STRICT=1 ;;
  -h | --help)
    sed -n '2,12p' "$0"
    exit 0
    ;;
  *)
    POSITIONAL=$((POSITIONAL + 1))
    case "$POSITIONAL" in
    1) ENV_FILE=$ARG ;;
    2) EXAMPLE_FILE=$ARG ;;
    *)
      echo "usage: $0 [.env] [.env.example] [--strict]" >&2
      exit 2
      ;;
    esac
    ;;
  esac
done
for F in "$ENV_FILE" "$EXAMPLE_FILE"; do
  if [ ! -f "$F" ]; then
    echo "❌ no such file: $F" >&2
    exit 2
  fi
done

# Keys the deployment mentions, set or commented. `export KEY=` counts, and a
# file saved with CRLF line endings reads the same as one without.
keys() { tr -d '\r' <"$2" | sed -nE "$1" | sort -u; }
PRESENT=$(keys 's/^#? ?(export[[:space:]]+)?([A-Z][A-Z0-9_]*)=.*/\2/p' "$ENV_FILE")
SET_KEYS=$(keys 's/^(export[[:space:]]+)?([A-Z][A-Z0-9_]*)=.*/\2/p' "$EXAMPLE_FILE")
OPTIONAL_KEYS=$(keys 's/^# (export[[:space:]]+)?([A-Z][A-Z0-9_]*)=.*/\2/p' "$EXAMPLE_FILE")

missing() {
  # keys in $1 that are not in PRESENT
  printf '%s\n' "$1" | while IFS= read -r KEY; do
    [ -z "$KEY" ] && continue
    if ! printf '%s\n' "$PRESENT" | grep -qx "$KEY"; then
      echo "$KEY"
    fi
  done
}

MISSING_SET=$(missing "$SET_KEYS")
MISSING_OPTIONAL=$(missing "$OPTIONAL_KEYS")
# Keys the deployment carries that the catalog no longer names: retired
# settings, or local additions the release does not know about.
CATALOG=$(printf '%s\n%s\n' "$SET_KEYS" "$OPTIONAL_KEYS" | sort -u)
UNKNOWN=$(printf '%s\n' "$PRESENT" | while IFS= read -r KEY; do
  [ -z "$KEY" ] && continue
  printf '%s\n' "$CATALOG" | grep -qx "$KEY" || echo "$KEY"
done)

if [ -z "$MISSING_SET" ] && [ -z "$MISSING_OPTIONAL" ]; then
  echo "✅ ${ENV_FILE} names every setting in ${EXAMPLE_FILE}"
  if [ -n "$UNKNOWN" ]; then
    echo "ℹ️  In ${ENV_FILE} but not in the catalog (retired, or local to this deployment):"
    printf '%s\n' "$UNKNOWN" | sed 's/^/   /'
  fi
  exit 0
fi
if [ -n "$MISSING_SET" ]; then
  echo "⚠️  Set in ${EXAMPLE_FILE}, missing from ${ENV_FILE}:"
  printf '%s\n' "$MISSING_SET" | sed 's/^/   /'
fi
if [ -n "$MISSING_OPTIONAL" ]; then
  echo "ℹ️  Optional in ${EXAMPLE_FILE}, not mentioned in ${ENV_FILE}:"
  printf '%s\n' "$MISSING_OPTIONAL" | sed 's/^/   /'
fi
if [ -n "$UNKNOWN" ]; then
  echo "ℹ️  In ${ENV_FILE} but not in the catalog (retired, or local to this deployment):"
  printf '%s\n' "$UNKNOWN" | sed 's/^/   /'
fi
echo "   The release notes say what each one does; a commented line in ${ENV_FILE} records that you read it."
if [ "$STRICT" = 1 ] && [ -n "$MISSING_SET" ]; then
  exit 2
fi
exit 0
