#!/bin/bash
# Package and upload UserData scripts and CloudFormation templates to S3 for deployment
# Note: Lambda functions are now deployed via container images (see Dockerfile.lambda and build.yml)

set -e

ENVIRONMENT="${1:-prod}"
BUCKET_NAME="robosystems-${ENVIRONMENT}-deployment"
REGION="${AWS_REGION:-us-east-1}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

echo "📦 Packaging scripts and templates for ${ENVIRONMENT} environment..."

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

echo "✅ Scripts and CloudFormation templates packaged and uploaded successfully"
echo ""
echo "📍 S3 Artifacts:"
echo "  UserData scripts uploaded: 3 scripts (bastion, ladybug-writer, neo4j-writer)"
echo "  Shared userdata scripts uploaded: 5 scripts (graph-lifecycle, graph-health-check, register-graph-instance, run-graph-container, setup-cloudwatch-graph)"
echo "  CloudFormation Templates: Configured for ${ENVIRONMENT} environment"
echo ""
echo "ℹ️  Lambda functions are now deployed via container images."
echo "   See Dockerfile.lambda and .github/workflows/build.yml for details."
