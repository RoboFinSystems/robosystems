Create a GitHub issue for the current repository based on the user's input.

## Instructions

1. **Determine Issue Type** - Based on the user's description, determine which type to use:
   - **Bug**: Defects or unexpected behavior
   - **Task**: Specific, bounded work items that can be completed in one PR
   - **Feature**: Request a new capability (no design required)
   - **RFC**: Propose a design for discussion before implementation (if available)
   - **Spec**: Approved implementation plan ready for execution (if available)

   Note: Not all repos have RFC/Spec templates. Check `.github/ISSUE_TEMPLATE/` first.

2. **Gather Context** - If the user provides a file path or references existing code:
   - Read the relevant files to understand the current implementation
   - Check related configuration files
   - Review any referenced documentation

3. **Draft the Issue** - Use the YAML templates in `.github/ISSUE_TEMPLATE/`:
   - `bug.yml` - Include reproduction steps, impact, environment
   - `task.yml` - Be specific about scope and acceptance criteria
   - `feature.yml` - Capture the need and why it matters
   - `spec.yml` - Fill in all sections with technical detail (if available)
   - `rfc.yml` - Comprehensive design with alternatives considered (if available)

4. **Sanitize for Public Visibility** - Before creating:
   - Remove any internal pricing, margins, or cost details
   - Remove specific customer names or data
   - Generalize any sensitive business metrics
   - Keep technical implementation details (these are fine to share)

5. **Create the Issue** - Use `gh issue create` with:
   - Clear, concise title (no prefixes like [SPEC] - types handle categorization)
   - Well-formatted markdown body matching the template structure
   - Appropriate metadata labels (see below)

6. **Set Issue Type** - After creation, set the issue type via GraphQL:
   ```bash
   # Get repo info from current directory
   REPO=$(gh repo view --json nameWithOwner -q .nameWithOwner)
   OWNER=$(echo $REPO | cut -d'/' -f1)
   NAME=$(echo $REPO | cut -d'/' -f2)

   # Get issue ID
   gh api graphql -f query="{ repository(owner: \"$OWNER\", name: \"$NAME\") { issue(number: NUMBER) { id } } }"

   # Get available issue types for this repo
   gh api graphql -f query="{ repository(owner: \"$OWNER\", name: \"$NAME\") { issueTypes(first: 10) { nodes { id name } } } }"

   # Set type (use correct type ID from above)
   gh api graphql -f query='mutation { updateIssue(input: { id: "ISSUE_ID", issueTypeId: "TYPE_ID" }) { issue { number } } }'
   ```

## Labels

Issue types handle primary categorization. Use labels for metadata (varies by repo):

**Priority** (when to do it):
- `priority:critical` - Drop everything
- `priority:high` - Next up
- `priority:low` - Backlog

**Size** (how long):
- `size:small` - < 1 day
- `size:medium` - 1-3 days
- `size:large` - > 3 days

**Status**:
- `blocked` - Waiting on something
- `needs-review` - Ready for review

Check `gh label list` for available labels in the current repo.

## Example Usage

User: "We need to add export functionality"

Response: I'll create a feature issue for export functionality. Let me first understand the current state...

[Read relevant files to understand current implementation]
[Draft issue matching the template structure]
[Create issue with gh issue create]
[Set issue type via GraphQL]
[Add appropriate labels]

## Output Format

After creating the issue, provide:
1. The issue URL
2. Brief summary of what was created
3. Issue type and labels applied
4. Any suggested follow-up tasks or related issues to create

$ARGUMENTS
