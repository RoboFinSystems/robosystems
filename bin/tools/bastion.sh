#!/bin/bash

# RoboSystems bastion host control
#
# The bastion stops itself only once, after its first boot; a tunnel started
# from a script (the Postgres MCP launcher, `just tunnel ... all`) leaves it
# running. This is the switch for it.
#
# Usage: bin/tools/bastion.sh <prod|staging> <status|stop|start> [--wait]

set -euo pipefail

AWS_REGION="${AWS_REGION:-us-east-1}"

usage() {
    echo "Usage: $(basename "$0") <prod|staging> <status|stop|start> [--wait]" >&2
    exit 2
}

ENVIRONMENT=""
ACTION=""
WAIT="false"
for arg in "$@"; do
    case "$arg" in
        prod|staging) ENVIRONMENT="$arg" ;;
        status|stop|start) ACTION="$arg" ;;
        --wait) WAIT="true" ;;
        *) usage ;;
    esac
done
[[ -n "$ENVIRONMENT" && -n "$ACTION" ]] || usage

STACK_NAME="RoboSystemsBastion$(tr '[:lower:]' '[:upper:]' <<<"${ENVIRONMENT:0:1}")${ENVIRONMENT:1}"

INSTANCE_ID="$(aws ec2 describe-instances \
    --filters "Name=tag:aws:cloudformation:stack-name,Values=${STACK_NAME}" \
              "Name=instance-state-name,Values=pending,running,stopping,stopped" \
    --query 'Reservations[0].Instances[0].InstanceId' --output text --region "$AWS_REGION")"
if [[ -z "$INSTANCE_ID" || "$INSTANCE_ID" == "None" ]]; then
    echo "No bastion instance found for $ENVIRONMENT (stack $STACK_NAME)" >&2
    exit 1
fi

describe() {
    aws ec2 describe-instances --instance-ids "$INSTANCE_ID" --region "$AWS_REGION" \
        --query 'Reservations[0].Instances[0].[State.Name,InstanceType,LaunchTime]' --output text
}

active_sessions() {
    aws ssm describe-sessions --state Active --filters "key=Target,value=${INSTANCE_ID}" \
        --query 'length(Sessions)' --output text --region "$AWS_REGION" 2>/dev/null || echo "?"
}

read -r STATE TYPE LAUNCHED <<<"$(describe)"

case "$ACTION" in
    status)
        echo "$ENVIRONMENT bastion $INSTANCE_ID ($TYPE): $STATE"
        if [[ "$STATE" == "running" ]]; then
            echo "  running since $LAUNCHED; active SSM sessions: $(active_sessions)"
        fi
        ;;
    stop)
        if [[ "$STATE" == "stopped" || "$STATE" == "stopping" ]]; then
            echo "$ENVIRONMENT bastion $INSTANCE_ID is already $STATE"
            exit 0
        fi
        SESSIONS="$(active_sessions)"
        [[ "$SESSIONS" != "0" ]] && echo "Closing $SESSIONS active SSM session(s) (open tunnels will drop)"
        aws ec2 stop-instances --instance-ids "$INSTANCE_ID" --region "$AWS_REGION" >/dev/null
        # A stopped instance ends its sessions without telling the local
        # session-manager-plugin, which keeps listening on the tunnel port and
        # would make the next launcher believe the tunnel is still up.
        pkill -f "session-manager-plugin.*\"Target\": *\"${INSTANCE_ID}\"" 2>/dev/null || true
        echo "$ENVIRONMENT bastion $INSTANCE_ID: stopping (was running since $LAUNCHED)"
        if [[ "$WAIT" == "true" ]]; then
            aws ec2 wait instance-stopped --instance-ids "$INSTANCE_ID" --region "$AWS_REGION"
            echo "$ENVIRONMENT bastion $INSTANCE_ID: stopped"
        fi
        ;;
    start)
        if [[ "$STATE" == "running" || "$STATE" == "pending" ]]; then
            echo "$ENVIRONMENT bastion $INSTANCE_ID is already $STATE"
            exit 0
        fi
        aws ec2 start-instances --instance-ids "$INSTANCE_ID" --region "$AWS_REGION" >/dev/null
        echo "$ENVIRONMENT bastion $INSTANCE_ID: starting"
        if [[ "$WAIT" == "true" ]]; then
            aws ec2 wait instance-running --instance-ids "$INSTANCE_ID" --region "$AWS_REGION"
            echo "$ENVIRONMENT bastion $INSTANCE_ID: running (allow ~30s for the SSM agent)"
        fi
        ;;
esac
