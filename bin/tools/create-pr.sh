#!/bin/bash
set -e

# Create PR script using GitHub Actions
# Analyzes current branch changes and creates a comprehensive PR with Claude
# Usage: ./bin/tools/create-pr.sh [target_branch] [claude_review]

# Default values
TARGET_BRANCH=${1:-main}
CLAUDE_REVIEW=${2:-true}

# Get current branch
CURRENT_BRANCH=$(git branch --show-current)

# Validate current branch is not target branch
if [ "$CURRENT_BRANCH" = "$TARGET_BRANCH" ]; then
  echo "❌ Cannot create PR: you're currently on the target branch ($TARGET_BRANCH)"
  echo "Switch to your feature/release branch first: git checkout <your-branch>"
  exit 1
fi

# Validate target branch (only main is allowed as target)
if [[ "$TARGET_BRANCH" != "main" ]]; then
  echo "❌ Invalid target branch: $TARGET_BRANCH. Only 'main' is allowed as target."
  exit 1
fi

# Determine PR type from branch prefix
if [[ "$CURRENT_BRANCH" == release/* ]]; then
  PR_TYPE="release"
elif [[ "$CURRENT_BRANCH" == feature/* ]]; then
  PR_TYPE="feature"
elif [[ "$CURRENT_BRANCH" == bugfix/* ]]; then
  PR_TYPE="bugfix"
elif [[ "$CURRENT_BRANCH" == hotfix/* ]]; then
  PR_TYPE="hotfix"
elif [[ "$CURRENT_BRANCH" == chore/* ]]; then
  PR_TYPE="chore"
elif [[ "$CURRENT_BRANCH" == refactor/* ]]; then
  PR_TYPE="refactor"
else
  # Default to feature for any other branch names
  PR_TYPE="feature"
  echo "⚠️  Could not determine PR type from branch name '$CURRENT_BRANCH', defaulting to 'feature'"
fi

# Validate Claude review parameter
if [[ "$CLAUDE_REVIEW" != "true" && "$CLAUDE_REVIEW" != "false" ]]; then
  echo "❌ Invalid claude_review value: $CLAUDE_REVIEW. Use 'true' or 'false'."
  exit 1
fi

echo "🤖 Creating Claude-powered PR..."
echo "📋 Details:"
echo "  Source Branch: $CURRENT_BRANCH"
echo "  Target Branch: $TARGET_BRANCH"
echo "  PR Type: $PR_TYPE"
echo "  Claude Review: $CLAUDE_REVIEW"
echo ""

# Check for uncommitted changes
if ! git diff --quiet || ! git diff --cached --quiet; then
  echo "⚠️  You have uncommitted changes. Please commit or stash them first."
  echo ""
  echo "Uncommitted files:"
  git status --porcelain
  exit 1
fi

# Push current branch to remote if needed
echo "📤 Ensuring current branch is pushed to remote..."
if ! git show-ref --verify --quiet refs/remotes/origin/$CURRENT_BRANCH; then
  echo "Branch $CURRENT_BRANCH doesn't exist on remote. Pushing..."
  git push -u origin $CURRENT_BRANCH
else
  echo "Pushing latest changes..."
  git push origin $CURRENT_BRANCH
fi

echo ""
echo "🚀 Triggering Claude analysis workflow..."

# Trigger the GitHub Actions workflow
gh workflow run create-pr.yml \
  --field source_branch="$CURRENT_BRANCH" \
  --field target_branch="$TARGET_BRANCH" \
  --field pr_type="$PR_TYPE" \
  --field claude_review="$CLAUDE_REVIEW"

echo "⏳ Waiting for Claude to analyze changes and create PR..."

# Wait for workflow to complete and find the PR
MAX_ATTEMPTS=30  # 5 minutes with 10-second intervals
ATTEMPT=1

while [ $ATTEMPT -le $MAX_ATTEMPTS ]; do
    echo "Attempt $ATTEMPT/$MAX_ATTEMPTS: Checking for created PR..."

    # Check if the workflow run failed
    LATEST_RUN_STATUS=$(gh run list --workflow=create-pr.yml --limit=1 --json status --jq '.[0].status' 2>/dev/null || echo "")
    if [ "$LATEST_RUN_STATUS" = "failure" ] || [ "$LATEST_RUN_STATUS" = "cancelled" ]; then
        echo "❌ GitHub Actions workflow failed or was cancelled"
        echo "Check the workflow run details:"
        echo "  gh run list --workflow=create-pr.yml"
        echo "  gh run view --workflow=create-pr.yml"
        echo ""
        echo "You can create the PR manually:"
        echo "  gh pr create --base $TARGET_BRANCH --head $CURRENT_BRANCH"
        exit 1
    fi

    # Check if PR exists from current branch to target
    PR_URL=$(gh pr list --head "$CURRENT_BRANCH" --base "$TARGET_BRANCH" --json url --jq '.[0].url' 2>/dev/null || echo "")

    if [ -n "$PR_URL" ] && [ "$PR_URL" != "null" ]; then
        echo "✅ PR created successfully!"
        echo "🔗 PR URL: $PR_URL"
        echo ""
        echo "🎉 Claude has analyzed your changes and created a comprehensive PR"
        echo "📝 Review the PR description and make any necessary adjustments"

        # Try to open PR in browser
        if command -v open >/dev/null 2>&1; then
            echo "🌐 Opening PR in browser..."
            open "$PR_URL"
        elif command -v xdg-open >/dev/null 2>&1; then
            echo "🌐 Opening PR in browser..."
            xdg-open "$PR_URL"
        else
            echo "💡 Open the PR manually: $PR_URL"
        fi

        exit 0
    fi

    if [ $ATTEMPT -eq $MAX_ATTEMPTS ]; then
        echo "❌ Timeout: PR was not created after 5 minutes"
        echo "Check the GitHub Actions workflow status:"
        echo "  gh run list --workflow=create-pr.yml"
        echo ""
        echo "You can also create the PR manually:"
        echo "  gh pr create --base $TARGET_BRANCH --head $CURRENT_BRANCH"
        exit 1
    fi

    echo "PR not yet created, waiting 10 seconds..."
    sleep 10
    ATTEMPT=$((ATTEMPT + 1))
done
