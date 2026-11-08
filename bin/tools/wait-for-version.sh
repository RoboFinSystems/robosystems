#!/bin/bash
# Wait until the API reports a version.
#
#   bin/tools/wait-for-version.sh http://localhost:8000/v1/status 1.22.0 [timeout-seconds]
#
# Polls the status endpoint until `details.version` equals the version given
# (a leading "v" is dropped), then exits 0. Exits 1 on timeout, naming the last
# version seen, so a deploy that left the old process serving is reported as
# such and not as "healthy". Needs curl and jq.
set -euo pipefail

URL=${1:-}
WANT=${2:-}
TIMEOUT=${3:-300}
if [ -z "$URL" ] || [ -z "$WANT" ]; then
  echo "usage: $0 <status-url> <version> [timeout-seconds]" >&2
  exit 2
fi
WANT=${WANT#v}

DEADLINE=$(($(date +%s) + TIMEOUT))
SEEN="(no answer)"
while [ "$(date +%s)" -lt "$DEADLINE" ]; do
  if BODY=$(curl -sf --max-time 5 "$URL" 2>/dev/null); then
    GOT=$(printf '%s' "$BODY" | jq -r '.details.version // .version // empty' 2>/dev/null || true)
    if [ "$GOT" = "$WANT" ]; then
      echo "✅ ${URL} reports ${GOT}"
      exit 0
    fi
    SEEN=${GOT:-"(no version in the answer)"}
  fi
  sleep 3
done

echo "❌ ${URL} did not report ${WANT} within ${TIMEOUT}s (last seen: ${SEEN})" >&2
exit 1
