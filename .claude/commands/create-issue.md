---
description: Create a GitHub issue from the repo's templates, with the right type and labels.
argument-hint: "[what the issue is about]"
---

Create a GitHub issue for the current repository based on the user's input.

## Instructions

1. **Determine Issue Type** - Based on the user's description, pick one:
   - **Bug**: Defects or unexpected behavior
   - **Task**: Specific, bounded work items that can be completed in one PR
   - **Feature**: Request a new capability (no design required)
   - **RFC**: Propose a design for discussion before implementation
   - **Spec**: Approved implementation plan ready for execution

   Confirm what this repo actually offers before assuming — `ls .github/ISSUE_TEMPLATE/` for the templates and `gh issue create --help` for whether `--type` is supported.

2. **Gather Context** - If the user provides a file path or references existing code:
   - Read the relevant files to understand the current implementation
   - Check related configuration files
   - Review any referenced documentation

3. **Draft the Issue** - Read the matching YAML template in `.github/ISSUE_TEMPLATE/` and mirror its structure. Each template declares its own `type:` in frontmatter and marks which fields are required — read the file rather than guessing the sections. Fill the optional fields too where you have the information; they're the ones that make an issue actionable later.

   Note `gh issue create --title/--body` **bypasses templates entirely** — nothing prefills and nothing validates. That's exactly why the body has to be hand-matched to the template structure.

4. **Sanitize for Public Visibility** - This repo is public and the issue is world-readable immediately. Before creating:
   - Remove any internal pricing, margins, or cost details
   - Remove specific customer names or data
   - Generalize any sensitive business metrics
   - For anything security-adjacent, keep the text terse and non-actionable — no exploit mechanics, no affected-endpoint enumerations, no payloads. Detailed root-cause belongs in the private vault, referenced by filename only; for coordinated disclosure use a private GitHub Security Advisory, never a public issue.
   - Keep ordinary technical implementation details (these are fine to share)

5. **Create the Issue** - One command, with the type set inline:

   ```bash
   gh issue create \
     --type <Bug|Task|Feature|RFC|Spec> \
     --title "<clear, concise title>" \
     --body-file /tmp/issue-body.md \
     --label "<labels>"
   ```

   No prefixes like `[SPEC]` in the title — the type handles categorization. Write the body to a file rather than inlining it, to avoid shell-escaping problems.

   To change the type on an **existing** issue: `gh issue edit <n> --type <Type>` (or `--remove-type`). The old GraphQL `updateIssue` mutation is no longer needed for either case.

## Labels

Issue types handle primary categorization; labels carry the metadata. Always enumerate what actually exists rather than working from memory — and raise the limit, since the default truncates at 30:

```bash
gh label list --limit 100
```

The families to expect in this repo:

- **`area:*`** — the primary routing dimension (api, graph-api, auth, billing, dagster, adapters, mcp, infrastructure, frontend, ci-cd, schemas). **Always apply one.** This is the most commonly forgotten label and the most useful.
- **`priority:*`** — when to do it. Note the ladder is `critical` / `high` / `low` — there is **no `priority:medium`**.
- **`size:*`** — rough effort: `small` (< 1 day), `medium` (1–3 days), `large` (> 3 days).
- **Status** — `blocked`, `needs-review`.

## Questions vs issues

`.github/ISSUE_TEMPLATE/config.yml` disables blank issues and routes open-ended questions to GitHub Discussions. `gh issue create` bypasses that chooser entirely, so apply the intent yourself: if the user's input is a question or a discussion starter rather than actionable work, say so and suggest a Discussion instead of filing it.

## Example Usage

User: "We need to add export functionality"

Response: I'll create a feature issue for export functionality. Let me first understand the current state...

[Read relevant files to understand current implementation]
[Read feature.yml and draft a body matching its structure]
[Create with `gh issue create --type Feature --label area:api,size:medium`]

## Output Format

After creating the issue, provide:
1. The issue URL
2. Brief summary of what was created
3. Issue type and labels applied
4. Any suggested follow-up tasks or related issues to create

$ARGUMENTS
