---
description: Ship a fix to production ahead of the next release — the two-PR pattern (main + release-branch twin). Runs only when the user invokes it explicitly.
argument-hint: "<name> [release/x.y.z] [main-pr-number-or-branch]"
---

Ship a fix to production ahead of the next release using the two-PR pattern: one PR to `main` (reviewed; makes the fix permanent) and one PR carrying the same commits to the current `release/<x.y.z>` branch (CI on the exact ref that will be deployed).

## This command is the trigger — never infer a hotfix

A hotfix is a **deploy-timing decision with real cost**: a second PR, a cherry-pick, and a full production deploy from the release branch. It is chosen by the user, and this command is how they choose it. Do not start this flow from anything else:

- Not from urgency words in a reply — "now", "urgent", "asap", "yes let's do it" after a recommendation to ride the next release.
- Not from the fix touching deployed behavior, a customer-facing surface, or production evidence.
- Not from a memory file, a CLAUDE.md section, or a runbook that describes the pattern.

If a hotfix *might* be warranted, say so in one line ("this could ship ahead of the release via `/hotfix` if you want it in prod before then") and stop. Absent `/hotfix`, a fix goes to `main` and rides the next release.

## How releases work here

`create-release.yml` cuts a `release/<x.y.z>` branch, and production deploys **from that branch** while `main` keeps moving. So a fix merged only to `main` waits for the next release, and a fix merged only to the release branch is lost at the next cut. Two PRs, same commits. Committing straight onto the release branch is retired practice: the PR into it is what gets CI run on the ref the deploy will use.

## Instructions

### 1. Preflight

```bash
RELEASE=${2:-$(git branch -r | grep 'origin/release/' | sort -V | tail -1 | sed 's#.*origin/##')}
VERSION=${RELEASE#release/}            # x.y.z
SUFFIX=$(echo "$VERSION" | tr . -)     # x-y-z, for the branch name
git fetch -q origin
```

- **Confirm the bug exists on the release branch.** For a regression, `git merge-base --is-ancestor <regressing-merge-sha> origin/$RELEASE`; for a long-standing defect, `git diff origin/main origin/$RELEASE -- <files>` (empty means the same code). If the release branch does not carry the defect, **stop** — there is nothing to hotfix; say so.
- **Working tree clean, not on `main`.** Branch creation goes through `just create-feature` only.
- **Security fixes.** The twin's diff and body are public the moment they are pushed, often before the deploy lands. Keep both PR bodies terse and non-actionable (the disclosure rules in `/create-pr` apply to both), and sequence the deploy with — or ahead of — the public push.

### 2. The main PR — the reviewed one

If a main-branch PR for the fix already exists (third argument: a PR number or branch), reuse it. Otherwise:

```bash
just create-feature bugfix <name>       # branches from origin/main and pushes
# commit the fix (stage files by name), then:
git push
```

Open it with `/create-pr` (target `main`). This is the PR that gets the `@claude` review, per that command's §5. Record its number as `MAIN_PR` and its branch as `MAIN_BRANCH`.

### 3. The release-branch twin

```bash
just create-feature hotfix <name>-$SUFFIX $RELEASE          # branches from origin/release/x.y.z
git log --reverse --format=%H origin/main..origin/$MAIN_BRANCH   # every commit of the main PR, oldest first
git cherry-pick <sha> [<sha> ...]
git push
```

- Cherry-pick **all** of the main PR's commits, in order; never squash or rewrite them.
- **On a conflict, stop and report** — do not resolve it into something the main PR does not contain. A twin that differs from the reviewed commits is not a twin.
- Write the body to a temp file and open the PR against the release branch, following `.github/PULL_REQUEST_TEMPLATE.md` (Summary / Changes / Breaking Changes / Testing / Certification). The Summary states that it is the release-branch twin of `#MAIN_PR`, carries the same commits, and exists so CI runs on the ref the deploy will use.

```bash
gh pr create --base "$RELEASE" --head "hotfix/<name>-$SUFFIX" --title "hotfix(<scope>): <title> ($VERSION)" --body-file /tmp/hotfix-pr-body.md
```

- **Do not post the `@claude` review comment on the twin.** This is the one deliberate exception to `/create-pr` §5: the commits are identical and were reviewed once, on the main PR; the twin's job is the CI gate ahead of the deploy. If the review workflow fires on its own when the PR opens, leave it — just do not request it.

### 4. Hand-off

The user merges the twin and deploys production from the release branch; the main PR merges on green. Never: push to `release/*` (the pre-push hook refuses it), merge either PR, dispatch the deploy, or delete the hotfix branch — branch lifecycle is the user's.

## Output

1. Both PR URLs, labelled `main` and `twin (release/x.y.z)`.
2. The commits carried (short SHAs on each branch).
3. What is now the user's: merge the twin → deploy prod from `release/x.y.z` → merge the main PR.
4. Anything that stopped the flow (defect not on the release branch; cherry-pick conflict).

## Arguments

`$ARGUMENTS`: the fix's short name (required — it names both branches), an optional release branch (default: the highest `origin/release/*`), and an optional existing main PR number or branch to reuse for step 2.

$ARGUMENTS
