Create a GitHub issue for the RoboSystems repository based on the user's input.

## Instructions

1. **Determine Issue Type** - Based on the user's description, determine which type to use:
   - **Bug**: Defects or unexpected behavior
   - **Task**: Specific, bounded work items that can be completed in one PR
   - **Feature**: User-facing feature suggestions
   - **Spec**: Features requiring technical design, multiple phases, or architectural changes
   - **RFC**: Significant design proposals needing team discussion before implementation

2. **Gather Context** - If the user provides a file path or references existing code:
   - Read the relevant files to understand the current implementation
   - Check related configuration in `/robosystems/config/`
   - Review any referenced roadmap docs in `/local/docs/`

3. **Draft the Issue** - Use the YAML templates in `.github/ISSUE_TEMPLATE/`:
   - `bug.yml` - Include reproduction steps, impact, environment
   - `task.yml` - Be specific about scope and acceptance criteria
   - `feature.yml` - Simple "I wish I could..." format
   - `spec.yml` - Fill in all sections with technical detail
   - `rfc.yml` - Comprehensive design with alternatives considered

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
   # Get issue ID
   gh api graphql -f query='{ repository(owner: "RoboFinSystems", name: "robosystems") { issue(number: NUMBER) { id } } }'

   # Set type (use correct type ID)
   gh api graphql -f query='mutation { updateIssue(input: { id: "ISSUE_ID", issueTypeId: "TYPE_ID" }) { issue { number } } }'
   ```

   Issue Type IDs:
   - Task: `IT_kwDODL_jkM4BnUUo`
   - Bug: `IT_kwDODL_jkM4BnUUp`
   - Feature: `IT_kwDODL_jkM4BnUUq`
   - Spec: `IT_kwDODL_jkM4B0XrY`
   - RFC: `IT_kwDODL_jkM4B0XrZ`

## Labels

Issue types handle primary categorization. Use labels for metadata:

**Area** (which component):
- `area:api`, `area:dagster`, `area:infrastructure`, `area:graph-api`
- `area:billing`, `area:auth`, `area:frontend`

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

## Example Usage

User: "We need to add support for Plaid bank connections"

Response: I'll create a spec issue for Plaid integration. Let me first check the existing adapter scaffolding...

[Read /robosystems/adapters/plaid/ to understand current state]
[Draft spec with implementation phases]
[Create issue with gh issue create]
[Set issue type to Spec via GraphQL]
[Add labels: area:dagster, size:large]

## Output Format

After creating the issue, provide:
1. The issue URL
2. Brief summary of what was created
3. Issue type and labels applied
4. Any suggested follow-up tasks or related issues to create

$ARGUMENTS
