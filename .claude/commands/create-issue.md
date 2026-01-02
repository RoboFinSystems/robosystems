Create a GitHub issue for the RoboSystems repository based on the user's input.

## Instructions

1. **Determine Issue Type** - Based on the user's description, determine if this should be:
   - **Spec** (`[SPEC]` prefix): For features requiring technical design, multiple phases, or architectural changes
   - **Task**: For specific, bounded work items that can be completed in one PR
   - **Bug**: For defects or unexpected behavior
   - **Feature Request**: For user-facing feature suggestions without deep technical detail

2. **Gather Context** - If the user provides a file path or references existing code:
   - Read the relevant files to understand the current implementation
   - Check related configuration in `/robosystems/config/`
   - Review any referenced roadmap docs in `/local/docs/`

3. **Draft the Issue** - Using the appropriate template from `.github/ISSUE_TEMPLATE/`:
   - For Specs: Use spec.md template - fill in all sections with technical detail
   - For Tasks: Use task.md template - be specific about scope and acceptance criteria
   - For Bugs: Use bug.md template - include reproduction steps
   - For Features: Use feature.md template - simple "I wish I could..." format

4. **Sanitize for Public Visibility** - Before creating:
   - Remove any internal pricing, margins, or cost details
   - Remove specific customer names or data
   - Generalize any sensitive business metrics
   - Keep technical implementation details (these are fine to share)

5. **Create the Issue** - Use `gh issue create` with:
   - Appropriate labels based on type
   - Well-formatted markdown body
   - Clear, concise title

## Example Usage

User: "We need to add support for Plaid bank connections"

Response: I'll create a spec issue for Plaid integration. Let me first check the existing adapter scaffolding...

[Read /robosystems/adapters/plaid/ to understand current state]
[Draft spec with implementation phases]
[Create issue with gh issue create]

## Output Format

After creating the issue, provide:
1. The issue URL
2. Brief summary of what was created
3. Any suggested follow-up tasks or related issues to create

$ARGUMENTS
