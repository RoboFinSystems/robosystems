#!/bin/bash
# Create the directories compose mounts from the checkout, before Docker does.
#
# The platform image runs as its own user (appuser in the Dockerfile, uid 1000).
# On Linux a mounted directory keeps its host owner, and one Docker creates is
# root's, so the Graph API cannot create a database in it and Dagster cannot
# write a run. Made here first, they belong to whoever runs the stack. Docker
# Desktop on a Mac hides file ownership, so there this only makes them.
set -euo pipefail

IMAGE_UID=1000
DIRS="data/lbug-dbs data/staging data/artifacts data/lance dagster_home/storage"

# shellcheck disable=SC2086
mkdir -p $DIRS

[ "$(uname -s)" = "Linux" ] || exit 0

WRONG=""
for DIR in $DIRS; do
  [ "$(stat -c %u "$DIR")" = "$IMAGE_UID" ] || WRONG="$WRONG $DIR"
done
[ -n "$WRONG" ] || exit 0

# shellcheck disable=SC2086
if chown -R "$IMAGE_UID:$IMAGE_UID" $WRONG 2>/dev/null; then
  exit 0
fi
echo "These directories are not owned by the image's user (uid $IMAGE_UID), so the"
echo "stack cannot write to them and graph creation will fail:$WRONG"
echo "Fix it once with: sudo chown -R $IMAGE_UID:$IMAGE_UID$WRONG"
