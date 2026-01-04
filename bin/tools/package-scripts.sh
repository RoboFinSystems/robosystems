#!/bin/bash
# Package and upload UserData scripts to S3 for deployment
# Note: Lambda functions are now deployed via container images (see Dockerfile.lambda and build.yml)
# Note: CloudFormation templates are deployed inline (all under 40KB limit)

set -e

ENVIRONMENT="${1:-prod}"
BUCKET_NAME="robosystems-${ENVIRONMENT}-deployment"
REGION="${AWS_REGION:-us-east-1}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

echo "Packaging UserData scripts for ${ENVIRONMENT} environment..."

# Upload UserData scripts
echo "Uploading UserData scripts to S3..."

# Upload all userdata scripts
for script in "${REPO_ROOT}"/bin/userdata/*.sh; do
    script_name=$(basename "$script")
    echo "  Uploading ${script_name}..."
    aws s3 cp "$script" "s3://${BUCKET_NAME}/userdata/${script_name}" --region "$REGION"
done

# Upload common userdata scripts (shared between LadybugDB and Neo4j)
echo "Uploading common userdata scripts to S3..."
for script in "${REPO_ROOT}"/bin/userdata/common/*.sh; do
    if [ -f "$script" ]; then
        script_name=$(basename "$script")
        echo "  Uploading ${script_name}..."
        aws s3 cp "$script" "s3://${BUCKET_NAME}/userdata/common/${script_name}" --region "$REGION"
    fi
done

echo "UserData scripts packaged and uploaded successfully"
