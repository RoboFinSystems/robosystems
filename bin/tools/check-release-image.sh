#!/bin/bash
# Does a published image exist for this reference?
#
#   bin/tools/check-release-image.sh robofinsystems/robosystems:v1.22.0
#
# Exit 0 when the registry has a manifest for it, 1 when it does not, 2 on a
# bad argument. Asks the Docker daemon first; where that cannot answer (no
# daemon, or a macOS credential helper that needs a desktop session) and the
# image is on Docker Hub, asks the Hub's tag API anonymously instead.
set -euo pipefail

REF=${1:-}
case "$REF" in
*:*) ;;
*)
  echo "usage: $0 <image>:<tag>" >&2
  exit 2
  ;;
esac

if command -v docker >/dev/null 2>&1; then
  ERR=$(mktemp)
  trap 'rm -f "$ERR"' EXIT
  if docker manifest inspect "$REF" >/dev/null 2>"$ERR"; then
    echo "✅ ${REF} is published"
    exit 0
  fi
  # Only the registry's own answer means "no such image". Anything else (no
  # daemon, a credential helper that is missing or wants a desktop session)
  # falls through to the registry's HTTP API.
  if grep -qiE "manifest unknown|no such manifest|MANIFEST_UNKNOWN" "$ERR"; then
    echo "❌ ${REF} is not published" >&2
    exit 1
  fi
fi

NAME=${REF%:*}
TAG=${REF##*:}
case "$NAME" in
docker.io/*) NAME=${NAME#docker.io/} ;;
*.*/* | *:*/*)
  echo "❌ cannot reach a registry for ${NAME} without the Docker daemon" >&2
  exit 1
  ;;
esac
case "$NAME" in
*/*) ;;
*) NAME="library/${NAME}" ;;
esac

CODE=$(curl -s -o /dev/null -w '%{http_code}' --max-time 20 \
  "https://hub.docker.com/v2/repositories/${NAME}/tags/${TAG}")
case "$CODE" in
200)
  echo "✅ ${REF} is published"
  exit 0
  ;;
404)
  echo "❌ ${REF} is not published" >&2
  exit 1
  ;;
*)
  echo "❌ Docker Hub answered ${CODE} for ${NAME}:${TAG}" >&2
  exit 1
  ;;
esac
