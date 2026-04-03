Review a pull request by gathering all PR metadata, diff, and review comments, then provide a comprehensive review summary.

## Instructions

### 1. Identify the PR

The user may provide a PR URL, number, or nothing:

- **URL provided** (e.g., `https://github.com/RoboFinSystems/robosystems/pull/577`): Extract the repo and PR number
- **Number provided** (e.g., `577`): Use the current repository
- **Nothing provided**: Detect from the current branch using `gh pr view --json number,url` — if no open PR exists for the current branch, ask the user which PR to review

### 2. Gather PR Data

Run these `gh` commands to collect all context:

```bash
# PR metadata — use only valid fields
gh pr view <NUMBER> --json title,body,author,state,labels,reviews,reviewRequests,statusCheckRollup,mergeStateStatus,headRefName,baseRefName,additions,deletions,changedFiles,createdAt,updatedAt

# PR diff (the actual code changes)
gh pr diff <NUMBER>

# Inline review comments (gh api needs owner/repo — use gh repo view to get it)
gh api repos/$(gh repo view --json nameWithOwner -q .nameWithOwner)/pulls/<NUMBER>/comments --paginate

# Top-level PR conversation comments
gh api repos/$(gh repo view --json nameWithOwner -q .nameWithOwner)/issues/<NUMBER>/comments --paginate
```

**Important `gh pr view --json` field reference** (common mistakes to avoid):
- Use `reviews` not `reviewers` (reviewers is not a valid field)
- Use `reviewRequests` for pending review requests
- Use `headRefOid` for the HEAD commit SHA

### 3. Categorize Review Feedback

Organize all comments and checks into categories:

- **Human Reviews**: Comments from human reviewers (approve, request changes, general feedback)
- **AI Reviews**: Comments from Claude, Copilot, or other AI review bots
- **Code Quality**: Comments from linters, formatters, type checkers (e.g., CodeRabbit, SonarCloud, Codacy)
- **Security**: Findings from security scanners (e.g., Snyk, Dependabot, CodeQL, GitGuardian)
- **CI/CD**: Build status, test results, deployment checks

### 4. Review the Diff

With the full PR diff in hand, perform your own review focusing on:

- **Correctness**: Does the code do what the PR description says?
- **Patterns**: Does it follow existing codebase patterns (check CLAUDE.md)?
- **Security**: Any OWASP top 10 concerns?
- **Multi-tenancy**: Are graph operations scoped to `graph_id`?
- **Error handling**: Appropriate for the context?
- **Tests**: Are changes covered by tests?
- **Missing changes**: Any files that should have been updated but weren't?

### 5. Output Format

Provide a structured review:

```
## PR Summary
**Title**: ...
**Author**: ... | **Branch**: ... → ...
**Status**: ... | **Changes**: +X / -Y across Z files

<Brief summary of what the PR does>

## Existing Review Feedback

### Human Reviews
<Summarize human reviewer comments and their status>

### AI Reviews
<Summarize AI review comments — highlight unresolved items>

### Code Quality
<Summarize code quality bot findings>

### Security
<Summarize security scanner findings — flag anything critical>

### CI/CD Status
<Pass/fail status of all checks>

## My Review

### Issues (should fix before merge)
<Numbered list of problems found>

### Suggestions (non-blocking improvements)
<Numbered list of suggestions>

### Questions
<Anything unclear that needs clarification>

## Verdict
<APPROVE / REQUEST_CHANGES / NEEDS_DISCUSSION — with brief rationale>
```

### Notes

- If the PR diff is very large (>2000 lines), focus on the most important files and note which files were skimmed
- For security findings, always err on the side of flagging — false positives are better than missed vulnerabilities
- Cross-reference the PR description with the actual diff to catch scope creep or missing implementation
- If the PR references an issue, check that the issue requirements are met

$ARGUMENTS