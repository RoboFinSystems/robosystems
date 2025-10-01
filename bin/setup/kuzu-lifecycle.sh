#!/bin/bash
# Kuzu Instance Lifecycle Management Script
# This script handles graceful shutdown and database migration on instance termination

set -e

INSTANCE_ID=$(ec2-metadata --instance-id | cut -d " " -f 2)
ENVIRONMENT="${ENVIRONMENT:-prod}"
REGION="${AWS_REGION:-us-east-1}"
GRAPH_REGISTRY_TABLE="${ENVIRONMENT}-kuzu-graph-registry"
INSTANCE_REGISTRY_TABLE="${ENVIRONMENT}-kuzu-instance-registry"

log() {
    echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $1"
    logger -t kuzu-lifecycle "$1"
}

handle_termination() {
    log "Starting graceful termination for instance $INSTANCE_ID"
    
    # 1. Mark instance as terminating in DynamoDB
    aws dynamodb update-item \
        --table-name "$INSTANCE_REGISTRY_TABLE" \
        --key "{\"instance_id\": {\"S\": \"$INSTANCE_ID\"}}" \
        --update-expression "SET #status = :status, terminating_at = :time" \
        --expression-attribute-names '{"#status": "status"}' \
        --expression-attribute-values "{\":status\": {\"S\": \"terminating\"}, \":time\": {\"S\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}}" \
        --region "$REGION" || log "Failed to update instance status"
    
    # 2. Stop accepting new connections
    docker exec kuzu-writer curl -X POST http://localhost:8001/admin/drain || true
    
    # 3. Get all databases on this instance
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
    
    log "Found databases to migrate: $DATABASES"
    
    # 4. Mark each database for migration
    for DB in $DATABASES; do
        if [ -n "$DB" ]; then
            log "Marking database $DB for migration"
            aws dynamodb update-item \
                --table-name "$GRAPH_REGISTRY_TABLE" \
                --key "{\"graph_id\": {\"S\": \"$DB\"}}" \
                --update-expression "SET migration_required = :true, migration_source = :instance" \
                --expression-attribute-values "{\":true\": {\"BOOL\": true}, \":instance\": {\"S\": \"$INSTANCE_ID\"}}" \
                --region "$REGION" || log "Failed to mark $DB for migration"
        fi
    done
    
    # 5. Wait for active connections to complete (max 5 minutes)
    TIMEOUT=300
    ELAPSED=0
    while [ $ELAPSED -lt $TIMEOUT ]; do
        ACTIVE_CONNECTIONS=$(docker exec kuzu-writer curl -s http://localhost:8001/admin/connections | jq '.active_connections // 0')
        if [ "$ACTIVE_CONNECTIONS" -eq 0 ]; then
            log "All connections closed"
            break
        fi
        log "Waiting for $ACTIVE_CONNECTIONS active connections to close..."
        sleep 10
        ELAPSED=$((ELAPSED + 10))
    done
    
    # 6. Create final EBS snapshot
    VOLUME_ID=$(aws ec2 describe-instances \
        --instance-ids "$INSTANCE_ID" \
        --query 'Reservations[0].Instances[0].BlockDeviceMappings[?DeviceName==`/dev/xvdf`].Ebs.VolumeId' \
        --output text \
        --region "$REGION")
    
    if [ -n "$VOLUME_ID" ] && [ "$VOLUME_ID" != "None" ]; then
        log "Creating final snapshot of volume $VOLUME_ID"
        SNAPSHOT_ID=$(aws ec2 create-snapshot \
            --volume-id "$VOLUME_ID" \
            --description "Final snapshot before termination of $INSTANCE_ID" \
            --tag-specifications "ResourceType=snapshot,Tags=[{Key=Name,Value=${ENVIRONMENT}-kuzu-final-${INSTANCE_ID}},{Key=InstanceId,Value=$INSTANCE_ID},{Key=Type,Value=final}]" \
            --query 'SnapshotId' \
            --output text \
            --region "$REGION")
        log "Created snapshot $SNAPSHOT_ID"
    fi
    
    # 7. Stop Docker container
    log "Stopping Kuzu container"
    docker compose down || true
    
    # 8. Mark instance as terminated in registry
    aws dynamodb update-item \
        --table-name "$INSTANCE_REGISTRY_TABLE" \
        --key "{\"instance_id\": {\"S\": \"$INSTANCE_ID\"}}" \
        --update-expression "SET #status = :status, terminated_at = :time" \
        --expression-attribute-names '{"#status": "status"}' \
        --expression-attribute-values "{\":status\": {\"S\": \"terminated\"}, \":time\": {\"S\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}}" \
        --region "$REGION" || log "Failed to update final status"
    
    # 9. Complete lifecycle action (if using lifecycle hooks)
    if [ -n "$LIFECYCLE_HOOK_NAME" ] && [ -n "$LIFECYCLE_ACTION_TOKEN" ]; then
        aws autoscaling complete-lifecycle-action \
            --lifecycle-hook-name "$LIFECYCLE_HOOK_NAME" \
            --auto-scaling-group-name "${ENVIRONMENT}-KuzuWriterASG" \
            --lifecycle-action-token "$LIFECYCLE_ACTION_TOKEN" \
            --lifecycle-action-result CONTINUE \
            --region "$REGION" || log "Failed to complete lifecycle action"
    fi
    
    log "Graceful termination completed"
}

# Handle signals for termination
trap handle_termination SIGTERM SIGINT

# If called with "terminate" argument, run termination immediately
if [ "$1" = "terminate" ]; then
    handle_termination
    exit 0
fi

# Otherwise, wait for signal
log "Lifecycle handler started, waiting for termination signal..."
while true; do
    sleep 30
done