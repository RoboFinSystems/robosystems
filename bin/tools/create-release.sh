#!/bin/bash
set -e

# Create release branch script using GitHub Actions
# Creates a new release branch from main with version bump
# Usage: ./bin/tools/create-release.sh [major|minor|patch] [staging|prod|none]

# Default values
VERSION_TYPE=${1:-patch}
DEPLOY_TARGET=${2:-staging}

# Validate the version type
if [[ "$VERSION_TYPE" != "major" && "$VERSION_TYPE" != "minor" && "$VERSION_TYPE" != "patch" ]]; then
  echo "❌ Invalid version type: $VERSION_TYPE. Use major, minor, or patch."
  exit 1
fi

# Validate the deploy target
if [[ "$DEPLOY_TARGET" != "staging" && "$DEPLOY_TARGET" != "prod" && "$DEPLOY_TARGET" != "all" && "$DEPLOY_TARGET" != "none" ]]; then
  echo "❌ Invalid deploy target: $DEPLOY_TARGET. Use staging, prod, all, or none."
  exit 1
fi

# Check current branch
CURRENT_BRANCH=$(git branch --show-current)
if [ "$CURRENT_BRANCH" != "main" ]; then
  echo "⚠️  Warning: You're not on the main branch (current: $CURRENT_BRANCH)"
  echo "Release branches should typically be cut from main."
  echo "Switching to main branch..."
  git checkout main
  git pull origin main
fi

# Check for uncommitted changes
if ! git diff --quiet || ! git diff --cached --quiet; then
  echo "❌ You have uncommitted changes. Please commit or stash them first."
  echo ""
  echo "Uncommitted files:"
  git status --porcelain
  exit 1
fi

# Make sure we have the latest main
echo "📥 Fetching latest changes from main..."
git fetch origin main

# Check if local main is behind remote
LOCAL_MAIN=$(git rev-parse main)
REMOTE_MAIN=$(git rev-parse origin/main)
if [ "$LOCAL_MAIN" != "$REMOTE_MAIN" ]; then
  echo "⚠️  Your local main is not up to date with origin/main"
  echo "Updating main branch..."
  git checkout main
  git pull origin main
fi

echo "🚀 Cutting release from main branch..."

# Get current version to calculate new branch name
CURRENT_VERSION=$(awk -F'"' '/^version = / {print $2}' pyproject.toml)
IFS='.' read -r MAJOR MINOR PATCH <<< "$CURRENT_VERSION"

# Calculate new version based on type
if [ "$VERSION_TYPE" = "major" ]; then
    MAJOR=$((MAJOR + 1))
    MINOR=0
    PATCH=0
elif [ "$VERSION_TYPE" = "minor" ]; then
    MINOR=$((MINOR + 1))
    PATCH=0
else # patch
    PATCH=$((PATCH + 1))
fi

NEW_VERSION="$MAJOR.$MINOR.$PATCH"
BRANCH_NAME="release/$NEW_VERSION"

echo "📋 Release Details:"
echo "  Current Version: $CURRENT_VERSION"
echo "  New Version: $NEW_VERSION"
echo "  Branch Name: $BRANCH_NAME"
echo "  Version Type: $VERSION_TYPE"
echo "  Deploy Target: $DEPLOY_TARGET"
echo ""

# Proceeding with release creation
echo "✅ Proceeding with release creation..."

echo "🚀 Triggering GitHub Actions workflow to create release..."
gh workflow run create-release.yml \
  --field version_type="$VERSION_TYPE" \
  --field deploy_target="$DEPLOY_TARGET"

echo "⏳ Waiting for release branch to be created..."

# Wait for the branch to be created (check every 10 seconds for up to 3 minutes)
MAX_ATTEMPTS=18
ATTEMPT=1

while [ $ATTEMPT -le $MAX_ATTEMPTS ]; do
    echo "Attempt $ATTEMPT/$MAX_ATTEMPTS: Checking if branch exists..."

    # Fetch latest changes from remote
    git fetch origin --quiet

    # Check if the branch exists on remote
    if git show-ref --verify --quiet refs/remotes/origin/$BRANCH_NAME; then
        echo "✅ Branch $BRANCH_NAME found! Checking it out..."
        git checkout $BRANCH_NAME
        git pull origin $BRANCH_NAME

        echo "📦 Installing dependencies..."
        just install

        echo "🎉 Successfully cut release $NEW_VERSION"
        echo ""

        if [ "$DEPLOY_TARGET" = "staging" ]; then
          echo "📝 Next steps:"
          echo "  1. Staging deployment has been triggered automatically"
          echo "  2. Monitor the deployment: gh run list --workflow=staging.yml"
          echo "  3. Test in staging environment"
          echo "  4. When ready for production: just deploy prod"
        elif [ "$DEPLOY_TARGET" = "prod" ]; then
          echo "📝 Next steps:"
          echo "  1. Production deployment has been triggered from the release branch"
          echo "  2. Monitor the deployment: gh run list --workflow=prod.yml"
          echo "  3. This release branch is preserved as a deployment artifact"
        else
          echo "📝 Next steps:"
          echo "  1. The release branch has been created but not deployed"
          echo "  2. To deploy to staging:"
          echo "     gh workflow run staging.yml --ref $BRANCH_NAME"
          echo "  3. To deploy to production:"
          echo "     Create a PR to merge into main: just create-pr main release"
        fi

        exit 0
    fi

    if [ $ATTEMPT -eq $MAX_ATTEMPTS ]; then
        echo "❌ Timeout: Branch $BRANCH_NAME was not created after 3 minutes"
        echo "Check the GitHub Actions workflow status: gh run list --workflow=create-release.yml"
        exit 1
    fi

    echo "Branch not yet available, waiting 10 seconds..."
    sleep 10
    ATTEMPT=$((ATTEMPT + 1))
done
