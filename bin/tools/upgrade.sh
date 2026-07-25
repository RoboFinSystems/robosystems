#!/bin/bash
set -e

# Upgrade the local Docker stack, brew-style.
#
# Compose caches images with pull_policy=if_not_present, so a published image
# is never re-fetched once it is on disk — pulling the repo does not update
# the containers. This refreshes them:
#
#   image mode (ROBOSYSTEMS_IMAGE points at a registry)  -> pull --policy always
#   build mode (image is a bare local tag)               -> build --pull
#
# Then recreates only the containers whose image actually changed.
#
# Usage: ./bin/tools/upgrade.sh [profile] [scope]
#   profile  compose profile to upgrade (default: robosystems)
#   scope    "all" also refreshes upstream images (postgres, valkey, opensearch, ...)

PROFILE=${1:-robosystems}
SCOPE=${2:-}

if ! command -v jq >/dev/null 2>&1; then
  echo "❌ jq is required by this command (brew install jq)"
  exit 1
fi

test -f .env || cp .env.example .env

COMPOSE="docker compose -f compose.yaml --env-file .env --profile ${PROFILE}"

echo "🔍 Resolving images for profile '${PROFILE}'..."

# Services with a build: section are ours; everything else is an upstream image.
SERVICES=$(${COMPOSE} config --format json |
  jq -r '.services | to_entries[] | select(.value.build != null) | "\(.key) \(.value.image)"')

if [ -z "$SERVICES" ]; then
  echo "❌ No RoboSystems services found in profile '${PROFILE}'"
  exit 1
fi

# A registry reference contains a slash (robofinsystems/robosystems:latest);
# a bare tag (robosystems-local) means the service is built from source here.
PULL_SVCS=""
BUILD_SVCS=""
PULL_IMAGES=""
while read -r SVC IMAGE; do
  [ -z "$SVC" ] && continue
  case "$IMAGE" in
  */*)
    PULL_SVCS="$PULL_SVCS $SVC"
    case " $PULL_IMAGES " in
    *" $IMAGE "*) ;;
    *) PULL_IMAGES="$PULL_IMAGES $IMAGE" ;;
    esac
    ;;
  *) BUILD_SVCS="$BUILD_SVCS $SVC" ;;
  esac
done <<EOF
$SERVICES
EOF

# Snapshot image IDs so we can report what actually moved.
BEFORE=""
for IMAGE in $PULL_IMAGES; do
  BEFORE="$BEFORE$IMAGE $(docker image inspect --format '{{.Id}}' "$IMAGE" 2>/dev/null || echo absent)
"
done

if [ -n "$PULL_SVCS" ]; then
  echo "📥 Pulling published images:$PULL_SVCS"
  ${COMPOSE} pull --policy always $PULL_SVCS
fi

if [ "$SCOPE" = "all" ]; then
  echo "📥 Refreshing upstream images (postgres, valkey, opensearch, ...)"
  ${COMPOSE} pull --policy always --ignore-buildable --ignore-pull-failures
fi

if [ -n "$BUILD_SVCS" ]; then
  echo "🔨 Rebuilding from source:$BUILD_SVCS"
  ${COMPOSE} build --pull $BUILD_SVCS
fi

echo "🚀 Recreating containers whose image changed..."
${COMPOSE} up --detach

short_id() {
  case "$1" in
  sha256:*) echo "${1:7:12}" ;;
  *) echo "$1" ;;
  esac
}

echo ""
echo "📋 Result:"
for IMAGE in $PULL_IMAGES; do
  OLD=$(echo "$BEFORE" | awk -v i="$IMAGE" '$1 == i {print $2}')
  NEW=$(docker image inspect --format '{{.Id}}' "$IMAGE" 2>/dev/null || echo absent)
  if [ "$OLD" = "$NEW" ]; then
    echo "  ✅ ${IMAGE} already up to date"
  else
    echo "  ⬆️  ${IMAGE} updated ($(short_id "$OLD") → $(short_id "$NEW"))"
  fi
done

if [ -n "$BUILD_SVCS" ]; then
  echo "  🔨 rebuilt from source:$BUILD_SVCS"
fi

if [ "$SCOPE" != "all" ]; then
  echo ""
  echo "💡 Upstream images (postgres, valkey, ...) were left alone — 'just upgrade ${PROFILE} all' refreshes those too"
fi
