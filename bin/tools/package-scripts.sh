#!/bin/bash
# Package and upload Lambda functions and UserData scripts to S3 for CloudFormation deployment

set -e

ENVIRONMENT="${1:-prod}"
BUCKET_NAME="robosystems-${ENVIRONMENT}-deployment"
REGION="${AWS_REGION:-us-east-1}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

echo "📦 Packaging Lambda functions and scripts for ${ENVIRONMENT} environment..."

# Create temporary directory
TEMP_DIR=$(mktemp -d)
trap "rm -rf $TEMP_DIR" EXIT

# Function to package a Lambda function
package_lambda() {
    local lambda_name=$1
    local source_file=$2
    local requirements=$3

    echo "📦 Packaging ${lambda_name} Lambda..."
    mkdir -p "$TEMP_DIR/${lambda_name}"

    # Copy the Lambda function
    cp "${REPO_ROOT}/bin/lambda/${source_file}" "$TEMP_DIR/${lambda_name}/index.py"

    # Create requirements.txt if needed
    if [ -n "$requirements" ]; then
        echo "$requirements" > "$TEMP_DIR/${lambda_name}/requirements.txt"

        # Install dependencies
        cd "$TEMP_DIR/${lambda_name}"
        pip install -r requirements.txt -t . --platform manylinux2014_x86_64 --only-binary=:all: --quiet
    fi

    # Create deployment package
    cd "$TEMP_DIR/${lambda_name}"
    zip -r "../${lambda_name}.zip" . -x "*.pyc" -x "__pycache__/*" > /dev/null

    # Calculate hash of the zip file
    local code_hash=$(openssl dgst -sha256 -binary "../${lambda_name}.zip" | openssl enc -base64)
    # Create a short hash for the filename (first 8 chars, alphanumeric only)
    local short_hash=$(echo "$code_hash" | tr -d '/+=' | cut -c1-8)
    echo "  Hash: ${code_hash} (short: ${short_hash})"

    # Upload to S3 with hash in filename
    echo "📤 Uploading ${lambda_name} to S3..."
    local s3_key="lambda/${lambda_name}-${ENVIRONMENT}-${short_hash}.zip"
    aws s3 cp "../${lambda_name}.zip" "s3://${BUCKET_NAME}/${s3_key}" --region "$REGION"

    # Store the S3 key for CloudFormation to use
    echo "${s3_key}" > "../${lambda_name}.s3key"

    # Also store the full hash for reference
    echo "${code_hash}" > "../${lambda_name}.hash"
    aws s3 cp "../${lambda_name}.hash" "s3://${BUCKET_NAME}/lambda/${lambda_name}-${ENVIRONMENT}.hash" --region "$REGION"
}

# Package all Lambda functions

# postgres-init: Updates secrets AND creates additional databases (dagster, etc.)
# psycopg2-binary needed for database creation
package_lambda "postgres-init" "postgres_init.py" "psycopg2-binary==2.9.11"

package_lambda "postgres-rotation" "postgres_rotation.py" "psycopg2-binary==2.9.11"

package_lambda "valkey-rotation" "valkey_rotation.py" "redis==5.0.1"

package_lambda "graph-api-rotation" "graph_api_rotation.py" ""

# Package volume management Lambda functions
package_lambda "graph-volume-manager" "graph_volume_manager.py" ""

package_lambda "graph-volume-monitor" "graph_volume_monitor.py" "urllib3==2.6.0"

package_lambda "graph-volume-detachment" "graph_volume_detachment.py" ""

# Upload UserData scripts
echo "📤 Uploading UserData scripts to S3..."

# Upload all userdata scripts
for script in "${REPO_ROOT}"/bin/userdata/*.sh; do
    script_name=$(basename "$script")
    echo "  Uploading ${script_name}..."
    aws s3 cp "$script" "s3://${BUCKET_NAME}/userdata/${script_name}" --region "$REGION"
done

# Upload common userdata scripts (shared between LadybugDB and Neo4j)
echo "📤 Uploading common userdata scripts to S3..."
for script in "${REPO_ROOT}"/bin/userdata/common/*.sh; do
    if [ -f "$script" ]; then
        script_name=$(basename "$script")
        echo "  Uploading ${script_name}..."
        aws s3 cp "$script" "s3://${BUCKET_NAME}/userdata/common/${script_name}" --region "$REGION"
    fi
done

# Upload large CloudFormation templates to S3
echo "📤 Uploading CloudFormation templates to S3..."

# Function to upload CloudFormation template if it's large
upload_cf_template() {
    local template_file=$1
    local template_name=$(basename "$template_file" .yaml)
    local file_size=$(stat -f%z "$template_file" 2>/dev/null || stat -c%s "$template_file" 2>/dev/null)
    local size_kb=$((file_size / 1024))

    # Upload if template is larger than 40KB
    # AWS CloudFormation inline template limit: 51,200 bytes (51KB)
    # Using 40KB threshold provides 11KB safety margin for:
    # - JSON formatting differences
    # - Parameter substitutions
    # - Metadata additions during deployment
    if [ $size_kb -gt 40 ]; then
        echo "  Uploading ${template_name}.yaml (${size_kb}KB) - exceeds inline limit..."
        aws s3 cp "$template_file" "s3://${BUCKET_NAME}/cloudformation/${template_name}-${ENVIRONMENT}.yaml" --region "$REGION"
    else
        echo "  Skipping ${template_name}.yaml (${size_kb}KB) - can be inlined"
    fi
}

# Upload CloudFormation templates that might exceed inline limits
for template in "${REPO_ROOT}"/cloudformation/*.yaml; do
    upload_cf_template "$template"
done

# Create a manifest file with all Lambda S3 keys and hashes for CloudFormation
echo "📝 Creating manifest for CloudFormation..."
MANIFEST="$TEMP_DIR/lambda-manifest-${ENVIRONMENT}.json"
echo "{" > "$MANIFEST"
echo "  \"Environment\": \"${ENVIRONMENT}\"," >> "$MANIFEST"
echo "  \"Timestamp\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"," >> "$MANIFEST"
echo "  \"Lambdas\": {" >> "$MANIFEST"

# Create manifest with S3 keys and hashes
FIRST=true
for lambda_name in postgres-init postgres-rotation valkey-rotation graph-api-rotation graph-volume-manager graph-volume-monitor graph-volume-detachment; do
    if [ "$FIRST" = false ]; then
        echo "," >> "$MANIFEST"
    fi

    # Read the S3 key and hash from local files
    if [ -f "../${lambda_name}.s3key" ] && [ -f "../${lambda_name}.hash" ]; then
        S3_KEY=$(cat "../${lambda_name}.s3key")
        HASH=$(cat "../${lambda_name}.hash")
        echo -n "    \"${lambda_name}\": {\"s3_key\": \"${S3_KEY}\", \"hash\": \"${HASH}\"}" >> "$MANIFEST"
        FIRST=false
    fi
done

echo "" >> "$MANIFEST"
echo "  }" >> "$MANIFEST"
echo "}" >> "$MANIFEST"

# Upload the manifest
aws s3 cp "$MANIFEST" "s3://${BUCKET_NAME}/lambda/manifest-${ENVIRONMENT}.json" --region "$REGION"
echo "📤 Lambda manifest uploaded successfully"

echo "✅ Lambda functions, scripts, and CloudFormation templates packaged and uploaded successfully"
echo ""
echo "📍 S3 Artifacts:"
LAMBDA_COUNT=0
for lambda_name in postgres-init postgres-rotation valkey-rotation graph-api-rotation graph-volume-manager graph-volume-monitor graph-volume-detachment; do
    if [ -f "../${lambda_name}.s3key" ]; then
        LAMBDA_COUNT=$((LAMBDA_COUNT + 1))
    fi
done
echo "  Lambda packages uploaded: $LAMBDA_COUNT functions"
echo "  Lambda manifest uploaded"
echo "  UserData scripts uploaded: 3 scripts (bastion, ladybug-writer, neo4j-writer)"
echo "  Shared userdata scripts uploaded: 5 scripts (graph-lifecycle, graph-health-check, register-graph-instance, run-graph-container, setup-cloudwatch-graph)"
echo ""
echo "📋 CloudFormation Parameters:"
echo "  LambdaCodeBucket configured"
echo "  Lambda Code Keys: Configured for $LAMBDA_COUNT functions"
echo "  UserData Script Keys: 3 main scripts + 5 shared scripts configured"
echo ""
echo "  CloudFormation Templates: Configured for ${ENVIRONMENT} environment"
