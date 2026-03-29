#!/bin/bash
# Package and upload UserData scripts to S3 for deployment
# Note: Lambda functions are now deployed via container images (see Dockerfile.lambda and build.yml)
# Note: CloudFormation templates are deployed inline (all under 40KB limit)

set -e

ENVIRONMENT="${1:-prod}"
REGION="${AWS_REGION:-us-east-1}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# Determine S3 stack name based on environment
if [ "$ENVIRONMENT" = "prod" ]; then
    S3_STACK_NAME="RoboSystemsS3Prod"
else
    S3_STACK_NAME="RoboSystemsS3Staging"
fi

# Get deployment bucket name from S3 stack output (single source of truth)
# Falls back to default naming if stack query fails
BUCKET_NAME=$(aws cloudformation describe-stacks \
    --stack-name "$S3_STACK_NAME" \
    --query "Stacks[0].Outputs[?OutputKey=='DeploymentBucketName'].OutputValue" \
    --output text \
    --region "$REGION" 2>/dev/null) || BUCKET_NAME=""

if [ -z "$BUCKET_NAME" ] || [ "$BUCKET_NAME" = "None" ]; then
    echo "Warning: Could not get bucket name from S3 stack ($S3_STACK_NAME), using default"
    BUCKET_NAME="robosystems-deployment-${ENVIRONMENT}"
fi

echo "Packaging UserData scripts for ${ENVIRONMENT} environment..."
echo "Using deployment bucket: ${BUCKET_NAME}"

# Upload UserData scripts
echo "Uploading UserData scripts to S3..."

# Upload all userdata scripts
for script in "${REPO_ROOT}"/bin/userdata/*.sh; do
    script_name=$(basename "$script")
    echo "  Uploading ${script_name}..."
    aws s3 cp "$script" "s3://${BUCKET_NAME}/userdata/${script_name}" --region "$REGION"
done

# Upload common userdata scripts shared across graph instance types
echo "Uploading common userdata scripts to S3..."
for script in "${REPO_ROOT}"/bin/userdata/common/*.sh; do
    if [ -f "$script" ]; then
        script_name=$(basename "$script")
        echo "  Uploading ${script_name}..."
        aws s3 cp "$script" "s3://${BUCKET_NAME}/userdata/common/${script_name}" --region "$REGION"
    fi
done

echo "UserData scripts packaged and uploaded successfully"
