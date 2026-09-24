#!/bin/bash
# Brings up the services the test suite expects (mirrors .github/workflows/test.yml):
# PostgreSQL with the platform + extensions databases, Valkey, and — when Docker
# is available — LocalStack. Connection settings match pytest.ini.
set -euo pipefail

if [ "${CLAUDE_CODE_REMOTE:-}" != "true" ]; then
  exit 0
fi

cd "$CLAUDE_PROJECT_DIR"

PG_BIN=$(ls -d /usr/lib/postgresql/*/bin 2>/dev/null | sort -V | tail -1)
PG_DATA=/var/tmp/pgdata
export PGPASSWORD=postgres

# actionlint-py's build downloads a binary with Python's strict X.509 checks,
# which reject the web sandbox's proxy CA; it only lints workflow files.
uv sync --frozen --all-extras --no-install-package actionlint-py --quiet

# PostgreSQL
if ! pg_isready -h localhost -q; then
  if [ ! -f "$PG_DATA/PG_VERSION" ]; then
    rm -rf "$PG_DATA"
    mkdir -p "$PG_DATA"
    chown postgres "$PG_DATA"
    echo postgres > /var/tmp/pgpass
    chmod 644 /var/tmp/pgpass
    su postgres -c "$PG_BIN/initdb -D $PG_DATA -U postgres --pwfile=/var/tmp/pgpass -A md5" > /dev/null
  fi
  su postgres -c "$PG_BIN/pg_ctl -D $PG_DATA -l $PG_DATA/log -w \
    -o '-c listen_addresses=localhost -c max_connections=300' start" > /dev/null
fi

for db in robosystems_test extensions; do
  if ! psql -h localhost -U postgres -tAc "SELECT 1 FROM pg_database WHERE datname='$db'" | grep -q 1; then
    psql -h localhost -U postgres -qc "CREATE DATABASE $db"
  fi
done

EXTENSIONS_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/extensions \
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/robosystems_test \
  uv run alembic -c migrations/extensions.ini upgrade head > /dev/null 2>&1

# Valkey (redis-server speaks the same protocol); no snapshots, so no dump.rdb in the repo
if ! redis-cli -a valkey ping 2>/dev/null | grep -q PONG; then
  redis-server --port 6379 --requirepass valkey --save "" --appendonly no \
    --dir /var/tmp --daemonize yes > /dev/null
fi

mkdir -p data/lbug-dbs

# LocalStack: only a handful of S3-backed tests need it, and pulling the image
# is slow, so it comes up in the background and never blocks session start.
if command -v docker > /dev/null && timeout 10 docker info > /dev/null 2>&1; then
  (
    if ! docker ps --format '{{.Names}}' | grep -qx localstack; then
      docker rm -f localstack > /dev/null 2>&1 || true
      docker run -d --name localstack -p 4566:4566 \
        -e SERVICES=s3,secretsmanager,iam -e S3_SKIP_SIGNATURE_VALIDATION=1 \
        localstack/localstack:3.0 > /dev/null
    fi
    timeout 300 bash -c 'until curl -s localhost:4566/_localstack/health | grep -q "\"s3\": \"available\""; do sleep 3; done'
    AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test uv run python -c "
import boto3
s3 = boto3.client('s3', endpoint_url='http://localhost:4566', region_name='us-east-1')
if 'robosystems-local' not in [b['Name'] for b in s3.list_buckets()['Buckets']]:
    s3.create_bucket(Bucket='robosystems-local')
"
  ) > /var/tmp/localstack-setup.log 2>&1 &
  disown
else
  echo "Docker unavailable; skipping LocalStack (S3-backed tests will fail)" >&2
fi
