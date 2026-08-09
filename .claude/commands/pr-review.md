---
description: Review a pull request — gather metadata, diff, and existing feedback, then give a verdict.
argument-hint: "[pr-number-or-url]"
---

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
# PR metadata + conversation comments in one call
gh pr view <NUMBER> --json number,url,title,body,author,state,isDraft,labels,comments,reviews,reviewDecision,latestReviews,reviewRequests,statusCheckRollup,mergeStateStatus,headRefName,headRefOid,baseRefName,additions,deletions,changedFiles,files,closingIssuesReferences,createdAt,updatedAt

# PR diff (the actual code changes)
gh pr diff <NUMBER>

# Inline review comments — no --json equivalent exists, so this call is still required
gh api repos/$(gh repo view --json nameWithOwner -q .nameWithOwner)/pulls/<NUMBER>/comments --paginate
```

**Field notes:**
- `reviews` not `reviewers` — `reviewers` is not a valid field and errors.
- `reviewDecision` is the single field that answers "has this been approved."
- `comments` covers the top-level conversation, so no separate `issues/<n>/comments` call is needed.
- `closingIssuesReferences` gives the linked issue (needed for step 5's requirements check); `files` gives per-file add/delete counts (needed for triaging a large diff); `headRefOid` is the HEAD SHA.
- Keep `--paginate` **bare**. Adding `-q`/`--jq` makes gh emit one JSON document *per page* instead of a merged array, and `--slurp` can't be combined with `--jq`. Pipe to `jq` after the call, not through it.

### 3. Categorize Review Feedback

Organize all comments and checks into categories:

- **Human Reviews**: Comments from human reviewers (approve, request changes, general feedback)
- **AI Reviews**: Comments from Claude, Copilot, or other AI review bots
- **Code Quality**: Comments from linters, formatters, type checkers (e.g., CodeRabbit, SonarCloud, Codacy)
- **Security**: Findings from security scanners (e.g., Snyk, Dependabot, CodeQL, GitGuardian)
- **CI/CD**: Build status, test results, deployment checks

**How feedback actually arrives in this repo** — don't read the categories too literally:
- Formal `reviews` and inline `pulls/<n>/comments` are typically **empty**, and `reviewDecision` is usually blank. That's the norm here, not a signal that review was skipped. Don't report "no review feedback" on the strength of an empty `reviews` array.
- The AI reviewer posts as a **bot account in the conversation `comments`**, not as a formal review. That's where AI findings will be.
- In `statusCheckRollup`, checks expose `.name` while legacy statuses expose `.context`, and a `conclusion` of `NEUTRAL` or `SKIPPED` is not a failure. Read the conclusion, don't pattern-match on non-`SUCCESS`.

### 4. Review the Diff

With the full PR diff in hand, perform your own review focusing on:

- **Correctness**: Does the code do what the PR description says?
- **Patterns**: Does it follow existing codebase patterns (check CLAUDE.md)? Business logic belongs in the operations kernel — logic added directly in a router, GraphQL resolver, or MCP tool handler is a layering mistake, not a style preference.
- **Security**: Any OWASP top 10 concerns?
- **Multi-tenancy**: Are graph operations scoped to `graph_id`?
- **SDK contract**: The published clients are post-1.0 semver contracts with external integrators, but the contract is **two-tier** — ask which tier this touches, not merely whether it breaks something. The **stable tier** is the SDK facades plus the symbols `robosystems-integration-template` imports (its emit path); breaking that propagates as a **client major**, and an uncoordinated one is a blocking issue. The **generated tier** is everything else the OpenAPI spec produces; it moves with the API on a client minor, so removing an endpoint or response field nothing consumes is a release-note item, not a blocker. Dead surface — never reachable, no consumer, removal changes only symbol resolution — skips deprecation entirely. Additive fields are free either way. Don't manufacture a blocking finding out of a generated-tier removal.
- **Disclosure hygiene** (this repo is public): does the PR *text* over-disclose? A security-fix description should name the area hardened, never the mechanism — no exploit mechanics, attack scenarios, affected-endpoint enumerations, payloads, or "previously protected only by X" tells. Detailed root cause belongs in the git-ignored vault, referenced by filename. Also check sequencing: if the vulnerable code is still live in production, the diff discloses a live bug the moment it merges — flag it so the deploy can be sequenced.
- **Error handling**: Appropriate for the context?
- **Tests**: Are changes covered by tests? Read the test, don't trust that it's green — a test that asserts the buggy behavior passes just as happily as a correct one.
- **Missing changes**: Any files that should have been updated but weren't? Migrations for model changes, and both databases have independent histories.

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