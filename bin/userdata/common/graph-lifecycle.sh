#!/bin/bash
# Universal Graph Database Lifecycle Management Script
# Handles graceful shutdown and database migration for both LadybugDB and Neo4j
# Supports: instance termination, database migration, volume snapshots

set -e

# ==================================================================================
# ENVIRONMENT VALIDATION
# ==================================================================================
: ${DATABASE_TYPE:?"DATABASE_TYPE must be set (ladybug|neo4j)"}
: ${NODE_TYPE:?"NODE_TYPE must be set"}

INSTANCE_ID=$(ec2-metadata --instance-id | cut -d " " -f 2)
ENVIRONMENT="${ENVIRONMENT:-prod}"
REGION="${AWS_REGION:-us-east-1}"
GRAPH_REGISTRY_TABLE="${GRAPH_REGISTRY_TABLE:-robosystems-graph-${ENVIRONMENT}-graph-registry}"
INSTANCE_REGISTRY_TABLE="${INSTANCE_REGISTRY_TABLE:-robosystems-graph-${ENVIRONMENT}-instance-registry}"

# ==================================================================================
# DATABASE-SPECIFIC CONFIGURATION
# ==================================================================================
case "${DATABASE_TYPE}" in
    ladybug)
        if [ "${NODE_TYPE}" = "shared_master" ] || [ "${NODE_TYPE}" = "shared_replica" ]; then
            CONTAINER_NAME="lbug-shared-writer"
        else
            CONTAINER_NAME="lbug-writer"
        fi
        GRAPH_API_PORT="8001"
        DRAIN_ENDPOINT="http://localhost:${GRAPH_API_PORT}/admin/drain"
        CONNECTIONS_ENDPOINT="http://localhost:${GRAPH_API_PORT}/admin/connections"
        ;;
    neo4j)
        if [ "${NODE_TYPE}" = "shared_master" ] || [ "${NODE_TYPE}" = "shared_replica" ]; then
            CONTAINER_NAME="neo4j-shared-writer"
        else
            CONTAINER_NAME="neo4j-writer"
        fi
        GRAPH_API_PORT="8001"
        NEO4J_HTTP_PORT="7474"
        NEO4J_BOLT_PORT="7687"
        DRAIN_ENDPOINT="http://localhost:${GRAPH_API_PORT}/admin/drain"
        CONNECTIONS_ENDPOINT="http://localhost:${GRAPH_API_PORT}/admin/connections"
        ;;
    *)
        echo "ERROR: Unsupported DATABASE_TYPE: ${DATABASE_TYPE}"
        exit 1
        ;;
esac

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

    # 1. Mark instance as terminating in DynamoDB
    log "Marking instance as terminating in registry..."
    aws dynamodb update-item \
        --table-name "$INSTANCE_REGISTRY_TABLE" \
        --key "{\"instance_id\": {\"S\": \"$INSTANCE_ID\"}}" \
        --update-expression "SET #status = :status, terminating_at = :time" \
        --expression-attribute-names '{"#status": "status"}' \
        --expression-attribute-values "{\":status\": {\"S\": \"terminating\"}, \":time\": {\"S\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}}" \
        --region "$REGION" || log "WARNING: Failed to update instance status"

    # 2. Stop accepting new connections through Graph API
    log "Draining connections via Graph API..."
    docker exec ${CONTAINER_NAME} curl -X POST ${DRAIN_ENDPOINT} 2>/dev/null || {
        log "WARNING: Failed to drain connections via Graph API, attempting direct container drain..."

        # Database-specific fallback drain commands
        case "${DATABASE_TYPE}" in
            neo4j)
                # Neo4j: Set database to read-only mode
                docker exec ${CONTAINER_NAME} cypher-shell -u neo4j -p "${NEO4J_PASSWORD}" \
                    "CALL dbms.setConfigValue('dbms.read_only', 'true')" 2>/dev/null || true
                ;;
            ladybug)
                # LadybugDB: No specific drain command needed, Graph API handles it
                ;;
        esac
    }

    # 3. Get all databases on this instance
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

    # 4. Mark each database for migration
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

    # 5. Wait for active connections to complete (max 5 minutes)
    log "Waiting for active connections to close..."
    TIMEOUT=300
    ELAPSED=0
    while [ $ELAPSED -lt $TIMEOUT ]; do
        ACTIVE_CONNECTIONS=$(docker exec ${CONTAINER_NAME} curl -s ${CONNECTIONS_ENDPOINT} 2>/dev/null | jq '.active_connections // 0' || echo "0")

        if [ "$ACTIVE_CONNECTIONS" = "0" ] || [ "$ACTIVE_CONNECTIONS" -eq 0 ]; then
            log "All connections closed"
            break
        fi
        log "Waiting for $ACTIVE_CONNECTIONS active connections to close... (${ELAPSED}s/${TIMEOUT}s)"
        sleep 10
        ELAPSED=$((ELAPSED + 10))
    done

    if [ $ELAPSED -ge $TIMEOUT ]; then
        log "WARNING: Timeout waiting for connections to close, forcing shutdown"
    fi

    # 6. Database-specific graceful shutdown
    case "${DATABASE_TYPE}" in
        neo4j)
            log "Performing Neo4j graceful shutdown..."
            # Use Cypher to stop database gracefully
            docker exec ${CONTAINER_NAME} cypher-shell -u neo4j -p "${NEO4J_PASSWORD}" \
                "CALL dbms.shutdown()" 2>/dev/null || {
                log "WARNING: Neo4j graceful shutdown failed, will force stop"
            }
            sleep 5
            ;;
        ladybug)
            log "Performing LadybugDB graceful shutdown..."
            # LadybugDB embedded database - just ensure connections are closed (done above)
            ;;
    esac

    # 7. Create final EBS snapshot
    log "Creating final volume snapshot..."
    VOLUME_ID=$(aws ec2 describe-instances \
        --instance-ids "$INSTANCE_ID" \
        --query 'Reservations[0].Instances[0].BlockDeviceMappings[?DeviceName==`/dev/xvdf`].Ebs.VolumeId' \
        --output text \
        --region "$REGION")

    if [ -n "$VOLUME_ID" ] && [ "$VOLUME_ID" != "None" ]; then
        log "Creating snapshot of volume $VOLUME_ID..."
        SNAPSHOT_ID=$(aws ec2 create-snapshot \
            --volume-id "$VOLUME_ID" \
            --description "Final snapshot before termination of $INSTANCE_ID (${DATABASE_TYPE})" \
            --tag-specifications "ResourceType=snapshot,Tags=[{Key=Name,Value=${ENVIRONMENT}-${DATABASE_TYPE}-final-${INSTANCE_ID}},{Key=InstanceId,Value=$INSTANCE_ID},{Key=DatabaseType,Value=${DATABASE_TYPE}},{Key=Type,Value=final}]" \
            --query 'SnapshotId' \
            --output text \
            --region "$REGION")
        log "Created snapshot $SNAPSHOT_ID"
    else
        log "WARNING: No data volume found, skipping snapshot creation"
    fi

    # 8. Stop Docker container
    log "Stopping ${DATABASE_TYPE} container: ${CONTAINER_NAME}"
    docker stop ${CONTAINER_NAME} 2>/dev/null || true
    docker rm ${CONTAINER_NAME} 2>/dev/null || true

    # For docker-compose based deployments
    if [ -f "/opt/${DATABASE_TYPE}/docker-compose.yml" ]; then
        cd "/opt/${DATABASE_TYPE}"
        docker compose down || true
    fi

    # 9. Mark instance as terminated in registry
    log "Marking instance as terminated in registry..."
    aws dynamodb update-item \
        --table-name "$INSTANCE_REGISTRY_TABLE" \
        --key "{\"instance_id\": {\"S\": \"$INSTANCE_ID\"}}" \
        --update-expression "SET #status = :status, terminated_at = :time" \
        --expression-attribute-names '{"#status": "status"}' \
        --expression-attribute-values "{\":status\": {\"S\": \"terminated\"}, \":time\": {\"S\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}}" \
        --region "$REGION" || log "WARNING: Failed to update final status"

    # 10. Complete lifecycle action (if using lifecycle hooks)
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
log "Container: ${CONTAINER_NAME}, Graph API: ${GRAPH_API_PORT}"

while true; do
    # Check if container is still running
    if ! docker ps | grep -q ${CONTAINER_NAME}; then
        log "WARNING: Container ${CONTAINER_NAME} is not running! Lifecycle handler may not work correctly."
    fi
    sleep 30
done
