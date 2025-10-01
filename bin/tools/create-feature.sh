#!/bin/bash
set -e

# Create feature branch script - local Git operations
# Creates a new feature/bugfix/hotfix branch locally and pushes to remote
# Usage: ./bin/tools/create-feature.sh [type] [name] [base_branch]

# Default values
BRANCH_TYPE=${1:-feature}
BRANCH_NAME=${2:-}
BASE_BRANCH=${3:-main}

# Validate branch type
if [[ "$BRANCH_TYPE" != "feature" && "$BRANCH_TYPE" != "bugfix" && "$BRANCH_TYPE" != "hotfix" && "$BRANCH_TYPE" != "chore" && "$BRANCH_TYPE" != "refactor" ]]; then
  echo "❌ Invalid branch type: $BRANCH_TYPE"
  echo "Valid types: feature, bugfix, hotfix, chore, refactor"
  exit 1
fi

# Check if branch name was provided
if [ -z "$BRANCH_NAME" ]; then
  echo "❌ Branch name is required"
  echo "Usage: $0 [type] [name] [base_branch]"
  echo "Example: $0 feature add-user-auth main"
  exit 1
fi

# Sanitize branch name
SANITIZED_NAME=$(echo "$BRANCH_NAME" | sed 's/[^a-zA-Z0-9._-]/-/g' | sed 's/--*/-/g' | sed 's/^-//;s/-$//')
FULL_BRANCH="${BRANCH_TYPE}/${SANITIZED_NAME}"

echo "🚀 Creating feature branch locally..."
echo "📋 Details:"
echo "  Type: $BRANCH_TYPE"
echo "  Name: $SANITIZED_NAME"
echo "  Full Branch: $FULL_BRANCH"
echo "  Base Branch: $BASE_BRANCH"
echo ""

# Check for uncommitted changes
if ! git diff --quiet || ! git diff --cached --quiet; then
  echo "⚠️  You have uncommitted changes. Auto-stashing..."
  git stash push -m "Auto-stash before creating branch $FULL_BRANCH"
  echo "✅ Changes stashed"
fi

# Fetch latest changes from remote
echo "📥 Fetching latest changes from remote..."
git fetch origin

# Check if branch already exists (local or remote)
if git show-ref --verify --quiet refs/heads/$FULL_BRANCH; then
  echo "❌ Branch $FULL_BRANCH already exists locally"
  exit 1
fi

if git show-ref --verify --quiet refs/remotes/origin/$FULL_BRANCH; then
  echo "❌ Branch $FULL_BRANCH already exists on remote"
  echo "💡 To check it out: git checkout -b $FULL_BRANCH origin/$FULL_BRANCH"
  exit 1
fi

# Check if base branch exists on remote
if ! git show-ref --verify --quiet refs/remotes/origin/$BASE_BRANCH; then
  echo "❌ Base branch $BASE_BRANCH does not exist on remote"
  echo "💡 Available branches:"
  git branch -r | grep -v HEAD | sed 's/origin\///' | head -10
  exit 1
fi

# Create and checkout the new branch from the base branch
echo "🔨 Creating branch $FULL_BRANCH from origin/$BASE_BRANCH..."
git checkout -b $FULL_BRANCH origin/$BASE_BRANCH

# Push the new branch to remote with upstream tracking
echo "📤 Pushing branch to remote..."
git push -u origin $FULL_BRANCH

echo ""
echo "🎉 Successfully created and checked out $FULL_BRANCH"
echo ""
echo "📝 Next steps:"
echo "  1. Make your changes and commit them"
echo "  2. Push your changes: git push"
echo "  3. Create a PR: gh pr create --base $BASE_BRANCH --title \"Your PR title\" --body \"Your PR description\""
echo "     or use: just create-pr $BASE_BRANCH $BRANCH_TYPE"

# Check if we had stashed changes and auto-apply them
if git stash list | grep -q "Auto-stash before creating branch $FULL_BRANCH"; then
  echo ""
  echo "Auto-applying stashed changes..."
  git stash pop
  echo "✅ Stashed changes applied"
fi