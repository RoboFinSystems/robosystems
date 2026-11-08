#!/bin/bash
# Move a deployment that runs the published images to a release.
#
#   bin/tools/upgrade-to.sh v1.22.0 [--dry-run]
#   bin/tools/upgrade-to.sh latest  [--dry-run]   # the newest release tag
#
# For a stack that pins ROBOSYSTEMS_IMAGE to a registry image (a self-hosted
# server; not a laptop that builds from source). "latest" is resolved to a
# release tag before anything moves; the pin written to .env is always that
# tag, never "latest", so the deployment stays on a version it can name. In
# order:
#   1. preflight: clean tree, the tag exists, the image for it is published
#   2. settings drift: what the release's .env.example has that .env lacks
#   3. the checkout moves to the tag, so compose, scripts and image are one ref
#   4. ROBOSYSTEMS_IMAGE is pinned to the tag (the old .env is kept under .local/env-backups)
#   5. upgrade.sh pulls and recreates what changed
#   6. the API must report the new version before this exits 0
# The last line is the rollback command. Migrations run forward only.
set -euo pipefail

TAG=${1:-}
DRY_RUN=0
[ "${2:-}" = "--dry-run" ] && DRY_RUN=1
case "$TAG" in
v[0-9]*.[0-9]*.[0-9]* | latest) ;;
*)
  echo "usage: $0 v<major>.<minor>.<patch>|latest [--dry-run]" >&2
  exit 2
  ;;
esac

HERE=$(cd "$(dirname "$0")" && pwd)
PROFILE=${PROFILE:-robosystems}
STATUS_URL=${STATUS_URL:-http://localhost:8000/v1/status}

die() {
  echo "❌ $*" >&2
  exit 1
}

[ -f .env ] || die "no .env here; run from the checkout of the deployment"
IMAGE_LINE=$(grep -E '^ROBOSYSTEMS_IMAGE=' .env | head -1) || true
[ -n "$IMAGE_LINE" ] || die ".env does not set ROBOSYSTEMS_IMAGE"
IMAGE_REF=${IMAGE_LINE#ROBOSYSTEMS_IMAGE=}
IMAGE_REF=${IMAGE_REF%%[[:space:]]*}
IMAGE_REPO=${IMAGE_REF%:*}
CURRENT_TAG=${IMAGE_REF##*:}
case "$IMAGE_REPO" in
*/*) ;;
*) die "ROBOSYSTEMS_IMAGE=${IMAGE_REF} is built from source here; upgrade-to is for a deployment on published images (see docs/technical/Self-Hosting-on-a-Server.md)" ;;
esac
[ "$IMAGE_REF" != "$IMAGE_REPO" ] || die "ROBOSYSTEMS_IMAGE=${IMAGE_REF} carries no tag to move"

[ -z "$(git status --porcelain --untracked-files=no)" ] || die "the working tree has uncommitted changes to tracked files; commit, stash or discard first"
git fetch --tags --quiet origin
if [ "$TAG" = "latest" ]; then
  TAG=$(git tag --list 'v[0-9]*.[0-9]*.[0-9]*' --sort=-v:refname | head -1)
  [ -n "$TAG" ] || die "no release tag in the repository"
  echo "🔍 latest → ${TAG}"
fi
echo "🔍 Preflight for ${TAG} (currently ${CURRENT_TAG}, checkout $(git describe --tags --exact-match 2>/dev/null || git rev-parse --short HEAD))"
git rev-parse -q --verify "refs/tags/${TAG}" >/dev/null || die "tag ${TAG} is not in the repository"
"$HERE/check-release-image.sh" "${IMAGE_REPO}:${TAG}"

echo "🔍 Settings the release knows about:"
EXAMPLE_TMP=$(mktemp)
trap 'rm -f "$EXAMPLE_TMP"' EXIT
git show "${TAG}:.env.example" >"$EXAMPLE_TMP"
"$HERE/settings-drift.sh" .env "$EXAMPLE_TMP" | sed "s#${EXAMPLE_TMP}#.env.example@${TAG}#g"

if [ "$DRY_RUN" = 1 ]; then
  echo ""
  echo "Dry run. Would: git switch --detach ${TAG}; pin ROBOSYSTEMS_IMAGE=${IMAGE_REPO}:${TAG}; run upgrade.sh ${PROFILE}; wait for ${STATUS_URL} to report ${TAG#v}."
  echo "Rollback afterwards would be: just upgrade-to ${CURRENT_TAG}"
  exit 0
fi

echo "📌 Checkout → ${TAG}"
git switch --detach "${TAG}" --quiet
mkdir -p .local/env-backups
BACKUP=".local/env-backups/env-$(date +%Y%m%d-%H%M%S)"
cp .env "$BACKUP"
NEW_ENV=$(mktemp)
awk -v repo="$IMAGE_REPO" -v tag="$TAG" '
  /^ROBOSYSTEMS_IMAGE=/ && !done { print "ROBOSYSTEMS_IMAGE=" repo ":" tag; done = 1; next }
  { print }
' .env >"$NEW_ENV"
mv "$NEW_ENV" .env
echo "📌 ROBOSYSTEMS_IMAGE=${IMAGE_REPO}:${TAG} (previous .env kept as ${BACKUP})"

echo "🚀 Upgrading"
"$HERE/upgrade.sh" "$PROFILE"

echo "🩺 Waiting for the API"
"$HERE/wait-for-version.sh" "$STATUS_URL" "${TAG#v}" 300
docker compose -f compose.yaml --env-file .env --profile "$PROFILE" ps --format 'table {{.Name}}\t{{.Status}}\t{{.Image}}'

echo ""
echo "✅ On ${TAG}. Rollback: just upgrade-to ${CURRENT_TAG}  (migrations run forward only; a release that migrated the database does not roll back)"
