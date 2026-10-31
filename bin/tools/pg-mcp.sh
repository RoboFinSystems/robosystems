#!/bin/bash

# RoboSystems Postgres MCP launcher
#
# Starts the Postgres MCP server (postgres-mcp) against a hosted environment's
# database with nothing secret stored on this machine: the connection string
# is built in memory from the environment's Postgres secret in AWS Secrets
# Manager, read under the caller's AWS session at launch, so a password
# rotation never leaves a stale copy behind. The SSM tunnel is opened on
# demand if its local port is closed, so a fresh MCP connection works without
# a separate `just tunnel` first.
#
# Usage: just pg-mcp <prod|staging> <robosystems|extensions> [--check] [--no-tunnel]
#        (or bin/tools/pg-mcp.sh with the same arguments)
#
# Wire it into an MCP client as the server command, one entry per database:
#   { "command": "just", "args": ["--justfile", "/path/to/robosystems/justfile", "pg-mcp", "prod", "robosystems"] }
#
# The MCP runs in read-only ("restricted") mode by default; set
# POSTGRES_MCP_ACCESS_MODE=unrestricted to allow writes. --check verifies the
# tunnel and the secret's reachability (metadata only, never the value) and
# prints the masked connection target, then exits.
#
# stdout is the MCP protocol channel: every message here goes to stderr.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AWS_REGION="${AWS_REGION:-us-east-1}"
LOCAL_PORT_POSTGRES="${LOCAL_PORT_POSTGRES:-15432}"
POSTGRES_MCP_ACCESS_MODE="${POSTGRES_MCP_ACCESS_MODE:-restricted}"
POSTGRES_MCP_OVERRIDES="${POSTGRES_MCP_OVERRIDES:-$HOME/.config/postgres-mcp/overrides.txt}"
TUNNEL_WAIT_SECONDS="${PG_MCP_TUNNEL_WAIT_SECONDS:-150}"
STATE_DIR="${XDG_CACHE_HOME:-$HOME/.cache}/robosystems"

log() { echo "[pg-mcp] $*" >&2; }
die() { log "ERROR: $*"; exit 1; }

usage() {
    cat >&2 <<EOF
Usage: $(basename "$0") <prod|staging> <robosystems|extensions> [--check] [--no-tunnel]

  prod|staging            which environment's database
  robosystems|extensions  the platform database or the extensions (per-graph OLTP) database
  --check                 verify tunnel + secret access and print the masked target; do not start the MCP
  --no-tunnel             do not open the SSM tunnel; fail if the local port is closed

Environment: AWS_PROFILE (default: robosystems-sso when that profile exists), AWS_REGION,
LOCAL_PORT_POSTGRES (default 15432), POSTGRES_MCP_ACCESS_MODE (default restricted),
POSTGRES_MCP_OVERRIDES (uv override file for the MCP's pinned dependencies),
PG_MCP_TUNNEL_WAIT_SECONDS (default 150; covers a cold bastion start).
EOF
    exit 2
}

ENVIRONMENT=""
DATABASE=""
CHECK_ONLY="false"
AUTO_TUNNEL="true"
for arg in "$@"; do
    case "$arg" in
        prod|staging) ENVIRONMENT="$arg" ;;
        robosystems|extensions) DATABASE="$arg" ;;
        --check) CHECK_ONLY="true" ;;
        --no-tunnel) AUTO_TUNNEL="false" ;;
        -h|--help) usage ;;
        *) log "unknown argument: $arg"; usage ;;
    esac
done
[[ -n "$ENVIRONMENT" && -n "$DATABASE" ]] || usage

SECRET_ID="robosystems/${ENVIRONMENT}/postgres"

# AWS identity: an explicit AWS_PROFILE wins; otherwise the SSO profile the
# admin CLI uses, when configured; otherwise whatever the default chain finds.
if [[ -z "${AWS_PROFILE:-}" ]] && aws configure list-profiles 2>/dev/null | grep -qx "robosystems-sso"; then
    export AWS_PROFILE="robosystems-sso"
fi

port_open() {
    nc -z 127.0.0.1 "$LOCAL_PORT_POSTGRES" >/dev/null 2>&1
}

ensure_tunnel() {
    if port_open; then
        log "tunnel already open on 127.0.0.1:$LOCAL_PORT_POSTGRES"
        return 0
    fi
    [[ "$AUTO_TUNNEL" == "true" ]] || die "no tunnel on 127.0.0.1:$LOCAL_PORT_POSTGRES (run: just tunnel $ENVIRONMENT postgres)"

    mkdir -p "$STATE_DIR"
    local lock="$STATE_DIR/tunnel-postgres-${ENVIRONMENT}.lock"
    local tunnel_log="$STATE_DIR/tunnel-postgres-${ENVIRONMENT}.log"

    # One launcher opens the tunnel; a second one started at the same moment
    # (the two database entries of one MCP client) waits for the port instead.
    if mkdir "$lock" 2>/dev/null; then
        log "opening the $ENVIRONMENT postgres tunnel (log: $tunnel_log)"
        nohup "$SCRIPT_DIR/tunnels.sh" "$ENVIRONMENT" postgres </dev/null >"$tunnel_log" 2>&1 &
        disown || true
        trap 'rmdir "$lock" 2>/dev/null || true' EXIT
    else
        log "another launcher is opening the tunnel; waiting for the port"
    fi

    local waited=0
    until port_open; do
        if (( waited >= TUNNEL_WAIT_SECONDS )); then
            rmdir "$lock" 2>/dev/null || true
            die "tunnel did not open within ${TUNNEL_WAIT_SECONDS}s; see $tunnel_log (AWS session expired? run: aws sso login)"
        fi
        sleep 2
        waited=$((waited + 2))
    done
    rmdir "$lock" 2>/dev/null || true
    trap - EXIT
    log "tunnel open after ${waited}s"
}

check_secret_access() {
    aws secretsmanager describe-secret --secret-id "$SECRET_ID" --region "$AWS_REGION" \
        --query 'Name' --output text >/dev/null 2>&1 \
        || die "cannot read secret metadata for $SECRET_ID (AWS session expired? run: aws sso login)"
}

ensure_tunnel
check_secret_access

if [[ "$CHECK_ONLY" == "true" ]]; then
    log "ok: tunnel on 127.0.0.1:$LOCAL_PORT_POSTGRES, secret $SECRET_ID reachable"
    log "target: postgresql://<user from secret>:***@127.0.0.1:$LOCAL_PORT_POSTGRES/$DATABASE?sslmode=require"
    log "mode: --access-mode=$POSTGRES_MCP_ACCESS_MODE"
    exit 0
fi

# The secret value is read into shell variables only; nothing below prints it
# and the password reaches the MCP through its environment, never through argv.
SECRET_JSON="$(aws secretsmanager get-secret-value --secret-id "$SECRET_ID" --region "$AWS_REGION" \
    --query SecretString --output text)" || die "could not read $SECRET_ID"
PG_USER="$(jq -r '.POSTGRES_USER // .username // empty' <<<"$SECRET_JSON")"
PG_PASSWORD="$(jq -r '.POSTGRES_PASSWORD // .password // empty' <<<"$SECRET_JSON")"
unset SECRET_JSON
[[ -n "$PG_USER" && -n "$PG_PASSWORD" ]] || die "secret $SECRET_ID has no POSTGRES_USER / POSTGRES_PASSWORD"

PG_PASSWORD_ENCODED="$(PW="$PG_PASSWORD" python3 -c 'import os, urllib.parse; print(urllib.parse.quote(os.environ["PW"], safe=""))')"
export DATABASE_URI="postgresql://${PG_USER}:${PG_PASSWORD_ENCODED}@127.0.0.1:${LOCAL_PORT_POSTGRES}/${DATABASE}?sslmode=require"
unset PG_PASSWORD PG_PASSWORD_ENCODED

log "starting postgres-mcp ($ENVIRONMENT/$DATABASE, --access-mode=$POSTGRES_MCP_ACCESS_MODE)"

UVX_ARGS=()
if [[ -f "$POSTGRES_MCP_OVERRIDES" ]]; then
    UVX_ARGS+=(--overrides "$POSTGRES_MCP_OVERRIDES")
fi
exec uvx "${UVX_ARGS[@]}" --with "mcp[cli]<2" postgres-mcp "--access-mode=$POSTGRES_MCP_ACCESS_MODE"
