Monitor a deployment run — pinpoint why it failed, drive it to green on a re-run, and verify health. Deploys go through GitHub Actions (`workflow_dispatch`); this skill is about watching and diagnosing them, not replacing the pipeline. Pairs with the `deployment-monitoring` runbook for account-specific stack names and failure classes.

## When this runs

Most deploys (~95%) go green untouched and need no attention. The real use case is the other 5%: **a deploy failed, and you're re-running it and want eyes on this one.** Optimize for that — get to the failing job fast, classify it, fix the cause, re-trigger, and confirm the app is healthy afterward.

## Scope & guardrails

- **`gh` reads are free; the deploy trigger is not.** Reading runs, jobs, and logs (`gh run list/view/watch`) needs no confirmation. **Triggering or re-triggering a deploy** (`gh workflow run`, `just deploy`) is an outward-facing action — confirm the target (env + ref) with the user first, and default to watching a run they already started.
- **AWS is read-only here.** `describe-*` / `list-*` only. CloudFormation changes and stack deletions are the user's to run — never `create-stack`/`update-stack`/`delete-stack` directly.
- **Never deploy the default branch to prod without the release flow.** Production deploys ride a version tag / release branch produced by the release workflow; ad-hoc `main`→prod is not the path.
- **Output can be sensitive.** Failure logs name internal hostnames, stack names, and resource IDs. Don't paste raw infra detail into anything public; summarize.

## 1. Find the run

Identify env (`staging` | `prod`) and which run you're looking at. If the user didn't say, ask or infer from context.

```bash
gh run list --workflow=staging.yml --limit 5     # or prod.yml
gh run view <run-id>                              # job-level status
gh run watch <run-id>                             # live, if it's in flight
```

The deploy is one large workflow of dependent jobs (build/test → infra stacks → app services → post-deploy refresh). A single failed job fails the run. Find the **first** failed job — downstream failures are usually just the cascade.

## 2. Pinpoint the failure

```bash
gh run view <run-id> --log-failed      # logs for only the failed step(s)
```

Classify by which stage broke — each has a different fix and blast radius:

- **Test / build** — code problem, no infra touched. Safe to fix and re-run; nothing was deployed.
- **Infrastructure stack (CloudFormation)** — a stack update failed. The dangerous class: a stack left in a rollback state usually **can't be updated again** until it's resolved, so a naive re-run fails identically. See the runbook.
- **App service (API / orchestrator)** — the container-based services roll out on the new image tag; a failure here is often a bad image, a failed health check, or a migration abort (see `/migrate`).
- **Post-deploy refresh** — the graph tier cycles its instances separately from the app services; this can wait on in-flight work or fail on health, without the app deploy being bad.

## 3. Remediate, then re-deploy

Fix the root cause first (code fix + merge, a stuck stack resolved, a config/variable corrected). Then, **with the user's confirmation**, re-trigger:

```bash
just deploy staging          # or: just deploy prod <tag>
# equivalently: gh workflow run staging.yml --ref <branch-or-tag>
```

Note the deploy workflow is **serialized** (a concurrency group) — a new run queues behind any in-flight one rather than cancelling it. Don't fire a second deploy expecting it to preempt the first.

## 4. Verify health

A green workflow means the pipeline finished, not that the app is serving. Confirm the API process is answering:

```bash
curl -sf https://<api-host>/v1/status && echo OK      # public API over HTTPS
# if the API is in internal mode: tunnel first, then curl http://localhost:8000/v1/status
```

`/v1/status` is a **liveness** probe — it returns healthy whenever the process is up; it does **not** check the database, cache, or graph. A 200 means "the API is serving HTTP," not "the stack is healthy." Confirm the rest yourself: the app service (running vs desired count), recent errors in its log group, and — if the deploy carried a migration — the orchestrator boot logs (`/migrate`).

## Output

A short status: what failed and at which stage, the root cause, what you changed, the re-run link, and the post-deploy health result. If nothing failed, say so — don't manufacture work.
