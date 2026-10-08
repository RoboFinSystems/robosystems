#!/bin/bash
# Graph Database Lifecycle Management Script
# Handles graceful shutdown and database migration for LadybugDB
# Supports: instance termination, database migration

set -e

# ==================================================================================
# ENVIRONMENT VALIDATION
# ==================================================================================
: ${DATABASE_TYPE:?"DATABASE_TYPE must be set (ladybug)"}
: ${NODE_TYPE:?"NODE_TYPE must be set"}

INSTANCE_ID=$(ec2-metadata --instance-id | cut -d " " -f 2)
ENVIRONMENT="${ENVIRONMENT:-prod}"
REGION="${AWS_REGION:-us-east-1}"
GRAPH_REGISTRY_TABLE="${GRAPH_REGISTRY_TABLE:-robosystems-graph-${ENVIRONMENT}-graph-registry}"
INSTANCE_REGISTRY_TABLE="${INSTANCE_REGISTRY_TABLE:-robosystems-graph-${ENVIRONMENT}-instance-registry}"

# ==================================================================================
# DATABASE-SPECIFIC CONFIGURATION
# ==================================================================================
# run-graph-container.sh owns the NODE_TYPE -> container name mapping.
CONTAINER_NAME=$(/usr/local/bin/run-graph-container.sh --print-container-name) || {
    echo "ERROR: could not determine container name from run-graph-container.sh" >&2
    exit 1
}

# ==================================================================================
# LOGGING
# ==================================================================================
log() {
    echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] [${DATABASE_TYPE}] $1"
    logger -t "${DATABASE_TYPE}-lifecycle" "$1"
}

# ==================================================================================
# GRACEFUL SHUTDOWN HANDLER
# ==================================================================================
handle_termination() {
    log "Starting graceful termination for ${DATABASE_TYPE} instance $INSTANCE_ID"

    # 1. Mark instance as terminating in DynamoDB. On an ASG termination the
    # detachment Lambda has already removed the row; don't recreate it.
    log "Marking instance as terminating in registry..."
    aws dynamodb update-item \
        --table-name "$INSTANCE_REGISTRY_TABLE" \
        --key "{\"instance_id\": {\"S\": \"$INSTANCE_ID\"}}" \
        --condition-expression "attribute_exists(instance_id)" \
        --update-expression "SET #status = :status, terminating_at = :time" \
        --expression-attribute-names '{"#status": "status"}' \
        --expression-attribute-values "{\":status\": {\"S\": \"terminating\"}, \":time\": {\"S\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}}" \
        --region "$REGION" || log "Instance status not updated (no registry row)"

    # 2. Get all databases on this instance
    log "Querying databases on this instance..."
    DATABASES=$(aws dynamodb query \
        --table-name "$GRAPH_REGISTRY_TABLE" \
        --index-name "instance-index" \
        --key-condition-expression "instance_id = :iid" \
        --filter-expression "#status = :status" \
        --expression-attribute-names '{"#status": "status"}' \
        --expression-attribute-values "{\":iid\": {\"S\": \"$INSTANCE_ID\"}, \":status\": {\"S\": \"active\"}}" \
        --query 'Items[*].graph_id.S' \
        --output text \
        --region "$REGION")

    log "Found databases to migrate: ${DATABASES:-none}"

    # 3. Mark each database for migration
    for DB in $DATABASES; do
        if [ -n "$DB" ]; then
            log "Marking database $DB for migration"
            aws dynamodb update-item \
                --table-name "$GRAPH_REGISTRY_TABLE" \
                --key "{\"graph_id\": {\"S\": \"$DB\"}}" \
                --update-expression "SET migration_required = :true, migration_source = :instance, backend_type = :backend" \
                --expression-attribute-values "{\":true\": {\"BOOL\": true}, \":instance\": {\"S\": \"$INSTANCE_ID\"}, \":backend\": {\"S\": \"${DATABASE_TYPE}\"}}" \
                --region "$REGION" || log "WARNING: Failed to mark $DB for migration"
        fi
    done

    # 4. Stop Docker container. On an ASG termination the detachment Lambda
    # has already stopped it, unmounted the volume and detached it (taking the
    # pre_detach snapshot); this covers a shutdown outside the ASG.
    log "Stopping ${DATABASE_TYPE} container: ${CONTAINER_NAME}"
    docker stop ${CONTAINER_NAME} 2>/dev/null || true
    docker rm ${CONTAINER_NAME} 2>/dev/null || true

    # For docker-compose based deployments
    if [ -f "/opt/${DATABASE_TYPE}/docker-compose.yml" ]; then
        cd "/opt/${DATABASE_TYPE}"
        docker compose down || true
    fi

    # 5. Complete lifecycle action (if using lifecycle hooks)
    if [ -n "$LIFECYCLE_HOOK_NAME" ] && [ -n "$LIFECYCLE_ACTION_TOKEN" ]; then
        log "Completing lifecycle action..."
        ASG_NAME=$(aws ec2 describe-instances \
            --instance-ids "$INSTANCE_ID" \
            --query 'Reservations[0].Instances[0].Tags[?Key==`aws:autoscaling:groupName`].Value' \
            --output text \
            --region "$REGION" || echo "${ENVIRONMENT}-${DATABASE_TYPE^}WriterASG")

        aws autoscaling complete-lifecycle-action \
            --lifecycle-hook-name "$LIFECYCLE_HOOK_NAME" \
            --auto-scaling-group-name "$ASG_NAME" \
            --lifecycle-action-token "$LIFECYCLE_ACTION_TOKEN" \
            --lifecycle-action-result CONTINUE \
            --region "$REGION" || log "WARNING: Failed to complete lifecycle action"
    fi

    log "Graceful termination completed for ${DATABASE_TYPE} instance $INSTANCE_ID"
}

# ==================================================================================
# SIGNAL HANDLING
# ==================================================================================
trap handle_termination SIGTERM SIGINT

# If called with "terminate" argument, run termination immediately
if [ "$1" = "terminate" ]; then
    handle_termination
    exit 0
fi

# Otherwise, wait for signal
log "Lifecycle handler started for ${DATABASE_TYPE} (${NODE_TYPE}), waiting for termination signal..."
log "Container: ${CONTAINER_NAME}"

while true; do
    # Check if container is still running
    if ! docker ps | grep -q ${CONTAINER_NAME}; then
        log "WARNING: Container ${CONTAINER_NAME} is not running! Lifecycle handler may not work correctly."
    fi
    sleep 30
done
