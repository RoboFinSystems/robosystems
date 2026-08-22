---
description: Open a pull request for the current branch, writing the description from the work actually done.
argument-hint: "[target-branch] [review]"
---

Create a GitHub pull request for the current branch, writing the title and description from the actual work done in this session — not reconstructed from the diff.

## Why this command exists

The previous flow outsourced PR-description authoring to a GitHub Action that only saw the diff and commit messages. It could not know _why_ the changes were made, so it frequently described things that weren't true. Those inaccurate descriptions then fed `@claude` reviews, compounding the bad information. This command fixes that at the root: **you author the description here, where the full context of what was done and why is available.**

This is a Python backend service (FastAPI + LadybugDB graphs, managed with `uv` and `just`). Ground every description in the actual code that changed, and stay mindful of multi-tenancy (graph operations scoped to `graph_id`).

**This repository is public.** The PR title and body are world-readable the moment they're pushed — routinely *before* the change is deployed, since deploys are manual. Treat the description as a publication, not a work note.

## Instructions

### 1. Preflight

Run these checks before touching anything:

```bash
# Current and target branches
CURRENT=$(git branch --show-current)
TARGET=${1:-main}            # override target via the first argument
```

- **Never PR from the default branch.** If `CURRENT` is `main` (or `master`/`staging`), stop and tell the user to switch to a feature branch first.
- **Source ≠ target.** If `CURRENT == TARGET`, stop.
- **Uncommitted changes.** Run `git status --porcelain`. If there are uncommitted/staged changes, surface them and ask whether to commit them (respecting the repo's commit rules — never on `main`, stage files by name, no `git add -A`) or proceed without them. The PR description must reflect committed state.
- **Existing PR.** Check `gh pr list --head "$CURRENT" --base "$TARGET" --json url,number`. If a PR already exists, do **not** create a duplicate — offer to update its title/body with `gh pr edit` instead.
- **Security fixes — check deployment first.** A security-fix commit discloses the bug through its diff the moment it's pushed. If this branch carries one, check whether the vulnerable code is still live in production (`git show <prod-tag>:<file>` against the fix) and tell the user, so they can sequence the deploy with — or ahead of — the public push rather than opening a window.
- **Push the branch.** `gh pr create` requires the branch on the remote. Ensure it's pushed: `git push -u origin "$CURRENT"` (the user invoking `/create-pr` is the explicit, in-the-moment request that authorizes pushing _this feature branch_ — this is the one push allowed without a separate ask; never push `main`).

### 2. Gather the real change context

This is the whole point — ground the description in what actually happened:

- **Primary source: this session.** Use what was actually changed and why from the conversation context. This is the information the old GHA workflow never had.
- **Corroborate against the branch:**
  ```bash
  git log --oneline "$TARGET".."$CURRENT"     # commits on this branch
  git diff --stat "$TARGET"..."$CURRENT"      # files + churn
  git diff "$TARGET"..."$CURRENT"             # full diff — read it, don't guess
  ```
- **Hard rule — no confabulation.** Every claim in the description must be supported by the diff. If you didn't touch an endpoint, don't write "API changes." If a behavior isn't in the diff, don't mention it. When the session context and the diff disagree, the diff wins and you investigate the discrepancy.

### 3. Compose the PR

- **Type** — derive from the branch prefix (`feature/` → feat, `bugfix/`/`fix/` → fix, `hotfix/` → fix, `chore/` → chore, `refactor/` → refactor, `release/` → release). Default to `feat` if unprefixed.
- **Title** — concise (~50–72 chars), conventional-commit style, e.g. `feat(graph): scope materialize to graph_id`. Match the style in `git log`.
- **Body** — markdown. **Match the headings in `.github/PULL_REQUEST_TEMPLATE.md`**, because `--body-file` bypasses template prefill entirely and a hand-written body silently drops whatever sections it omits:
  - **Summary** — 1–3 sentences: what this PR does and why.
  - **Changes** — bullets grouped by area/module/file, describing real edits. Call out anything reviewers should look at closely.
  - **Breaking Changes** — "None" if there are none, and say so explicitly rather than omitting the section. See the SDK contract below; this is where it goes.
  - **Testing** — state truthfully what was run. The repo gate is `just test-all`; `just test-code` is the code-quality half without the ~6-minute test run. The justfile holds the stage list — don't restate it here, since the copy that is not the recipe is the one that goes stale. If you ran any of these this session, say which and give the result. The test portion needs a local env and is often not runnable in-session — if you couldn't run it, say so plainly. If nothing was run, say "Not run" — never claim passing tests that weren't executed.

  - **Certification** — reproduce the template's checkbox verbatim and tick it: `- [x] I have the right to submit this work under the Apache 2.0 license, and do so. Where any part of it is owned by my employer, I have their permission.` Because `--body-file` bypasses prefill, omitting this section silently drops a required provenance assertion.

  The template has no Related Issues section — put `Closes #123` / `Fixes #456` as the last line of the Summary. GitHub links it from anywhere in the body.

- **SDK contract.** `robosystems-python-client` and `robosystems-typescript-client` are post-1.0 semver contracts with external integrators, on a **two-tier** rule. Changes reaching the **stable tier** — the SDK facades, or the symbols `robosystems-integration-template` imports for its emit path — propagate as a **client major**: call those out under Breaking Changes and say so explicitly in the body, so the regen lands as a coordinated major with deprecation notes rather than silent drift. Changes to the **generated tier** (everything else the OpenAPI spec produces) ride a client minor; note the removal so it reaches the release notes, but don't file it under Breaking Changes. Removing surface that never worked skips deprecation entirely — record the three facts that justify it: it never functioned, no consumer exists, and removal changes only symbol resolution. Additive changes are free; note them as an SDK regen opportunity.

- **Security-fix disclosure.** If the PR fixes a security issue, the prose is often *more* actionable than the diff — keep it terse and non-actionable. Describe the area hardened, never the mechanism: "harden write-path authorization on the query surface", not the how. **No** exploit mechanics, attack scenarios, affected-endpoint enumerations, payloads/regexes, or "previously protected only by X" tells. Detailed root cause and any PoC go in the git-ignored vault under `local/RoboSystems/specs/`, referenced **by filename only** — never pasted into the PR. For coordinated disclosure use a private GitHub Security Advisory, never a public issue.
- **Attribution** — attribute to the user only. Do **not** add a "🤖 Generated with Claude Code" footer or a `Co-Authored-By: Claude` trailer (per the repo's commit conventions). Include such a line only if the user explicitly asks.

### 4. Create the PR

Write the body to a temp file to avoid shell-escaping problems, then:

```bash
gh pr create \
  --base "$TARGET" \
  --head "$CURRENT" \
  --title "<title>" \
  --body-file /tmp/pr-body.md
```

Print the resulting PR URL.

### 5. Request the Claude review — always

Every pull request gets a `@claude` review. This is a change-management control, not a convenience: this is a single-maintainer repository where GitHub forbids self-approval, so an automated second reader on every change is the compensating control that stands in for independent human review. Skipping it on a given PR puts a hole in the control.

```bash
gh pr comment <number> --body "@claude please review this PR"
```

Post it unconditionally, immediately after creating the PR. Do not ask first, and do not skip it for small or mechanical changes — a control that only runs on changes deemed interesting is not a control.

Two things this is **not**:

- **Not an approval.** The review posts as a comment from `claude[bot]`, not as an approving review, and it must stay that way. An unconditional bot approval on every PR is a rubber stamp, and it would be worse evidence than the documented exception it replaced. The reviewer's job is to find problems, not to sign off.
- **Not a substitute for `/pr-review`.** That command runs locally with full session context and is the deeper pass. This is the standing automatic one.

If the workflow does not fire (it is gated to `OWNER`/`MEMBER`/`COLLABORATOR` authors, so fork PRs are excluded by design), say so in the output rather than silently moving on.

**One expected exception — a PR that edits `.github/workflows/claude.yml` will not be reviewed.** `claude-code-action` refuses to run whenever the workflow file on the PR branch differs from the version on the default branch. That is an anti-tampering guard: without it, a pull request could rewrite the reviewer to exfiltrate secrets. The run still completes green and posts nothing, logging `Workflow validation failed... your workflow will begin working once you merge your PR`.

This is normal, not a fault. Do not report it as a broken review, and do not go hunting for a misconfiguration — check whether the PR touches `claude.yml` first. It also means a change to the review workflow itself can never be validated by its own PR; it has to be merged and then exercised by the next unrelated PR.

## Output

After creating the PR, report:

1. The PR URL.
2. A one-line summary of the title.
3. Target ← source branches.
4. Confirmation that the `@claude` review was requested — or, if it wasn't, why.

## Arguments

`$ARGUMENTS` may contain:

- A target branch (default `main`).
- Freeform guidance on what to emphasize in the description.

`review` / `--review` is accepted and ignored — the review is now unconditional (§5).

$ARGUMENTS
