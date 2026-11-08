#!/bin/bash
# Brings up the services the test suite expects (mirrors .github/workflows/test.yml)
# from the compose stack, so a later `just start` reuses the same containers.
# Connection settings match pytest.ini.
set -uo pipefail

if [ "${CLAUDE_CODE_REMOTE:-}" != "true" ]; then
  exit 0
fi

cd "$CLAUDE_PROJECT_DIR" || exit 0
LOG=/var/tmp/session-start.log
: > "$LOG"

test -f .env || cp .env.example .env
test -f .env.local || cp .env.local.example .env.local
git config core.hooksPath .githooks

# actionlint-py's build downloads a binary with Python's strict X.509 checks,
# which reject the web sandbox's proxy CA; it only lints workflow files.
uv sync --frozen --all-extras --no-install-package actionlint-py --quiet \
  || echo "uv sync failed; see $LOG" >&2

# Docker would create these bind mounts as root; the containers run as uid 1000
mkdir -p data/lbug-dbs data/staging data/artifacts data/lance
chown -R 1000:1000 data

pgrep -x dockerd > /dev/null || (dockerd >> "$LOG" 2>&1 &)
if ! timeout 30 bash -c 'until docker info > /dev/null 2>&1; do sleep 1; done'; then
  echo "Docker daemon did not start; test services are down (see $LOG)" >&2
  exit 0
fi

docker compose -f compose.yaml --env-file .env up -d --wait pg valkey >> "$LOG" 2>&1 \
  || echo "pg/valkey did not come up; see $LOG" >&2

export PGPASSWORD=postgres
for db in robosystems_test extensions; do
  psql -h localhost -U postgres -tAc "SELECT 1 FROM pg_database WHERE datname='$db'" | grep -q 1 \
    || psql -h localhost -U postgres -qc "CREATE DATABASE $db" >> "$LOG" 2>&1
done

EXTENSIONS_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/extensions \
  uv run alembic -c migrations/extensions.ini upgrade head >> "$LOG" 2>&1 \
  || echo "extensions migration failed; see $LOG" >&2

# S3: only a handful of S3-backed tests need it, so SeaweedFS and the bucket
# init come up in the background and never block session start.
docker compose -f compose.yaml --env-file .env --profile robosystems up -d storage-init \
  > /var/tmp/storage-setup.log 2>&1 &
disown

exit 0
