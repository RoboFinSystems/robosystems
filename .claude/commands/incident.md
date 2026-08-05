---
description: Triage a production signal to a subsystem, gather evidence, and hand back an incident picture.
argument-hint: "[what's wrong]"
---

A signal says something is wrong — a customer report, an internal user, an alarm email, a spike. This skill gives you a fast, ordered headstart on **where to look**: triage the signal to a likely subsystem, gather the state that confirms or rules it out, and hand back a crisp picture (impact, likely cause, next action). Pairs with the `incident-triage` runbook in `local/RoboSystems/runbooks/` for the account's known failure classes and exact log groups — read it alongside this file.

## Goal

Compress time-to-orientation. You're not expected to fix everything from here — you're expected to answer, quickly and with evidence: **what's broken, how bad, since when, and what changed.** Breadth before depth: check the obvious surfaces in parallel, then drill into the one that lights up.

## Scope & guardrails

- **Read-only triage.** `describe-*` / `list-*` / `get-*`, log reads, `gh` reads. Any remediation — restarting a service, cycling an instance, rolling back, editing data — is a separate, **confirmed** step, not part of triage. Never `get-secret-value`.
- **Output is sensitive — never commit it.** Incident notes name live hostnames, resource IDs, error contents, maybe customer identifiers. Keep them in the scratchpad or a private channel; never in the repo, never in a public Artifact.
- **State the confidence.** Distinguish *confirmed* (you saw the error/alarm) from *suspected* (fits the pattern). A wrong confident diagnosis sends everyone the wrong way.

## 1. Pin the signal

Get concrete before searching. From whoever/whatever raised it: **what** is failing (an endpoint? a product surface? everything?), **who** is affected (one tenant or all?), **when** it started, and **what changed** near then. A single-tenant issue and a platform-wide outage need completely different first moves.

Immediately check "what changed". A recent deploy is the highest-prior cause — but it is **not the only kind of change**, and the other kinds are invisible to `gh`:

```bash
gh run list --workflow=prod.yml --limit 5      # last deploys: when, success/fail, by whom
```

**Runtime configuration changes without a deploy.** Feature flags and tuning parameters live in SSM Parameter Store under a per-environment path and are re-read at runtime on a short TTL — a change reaches production within minutes and leaves no GitHub Actions trace. They cover exactly the things incidents are made of: pool sizing, timeouts, load-shedding pressures, admission thresholds, query limits. Check them alongside the deploy list:

```bash
aws ssm describe-parameters --region us-east-1 \
  --parameter-filters "Key=Path,Option=Recursive,Values=/robosystems/<env>" \
  --query 'reverse(sort_by(Parameters,&LastModifiedDate))[:10].[Name,LastModifiedDate]' --output text
```

A parameter modified near the incident start is as strong a signal as a deploy; CloudTrail `PutParameter` gives you the actor. **An agent that runs only `gh run list`, finds no recent deploy, and concludes "not a release" will take the wrong branch.**

**The customer-facing web apps deploy from separate repositories** on their own pipeline — this repo's workflow list will never show them. If the signal is "the site is down" rather than "the API is erroring," check those repos' runs before concluding it's a backend release.

## 2. Is the platform up? (breadth, in parallel)

Sweep the front-to-back surfaces at once; whichever is unhealthy narrows everything. Fan these out:

- **API liveness** — the status endpoint (`/v1/status`); is the API answering HTTP at all? It's a **liveness** probe only — a 200 says the process is up, *not* that its dependencies are healthy, so don't stop here.
- **Active alarms** — `aws cloudwatch describe-alarms --state-value ALARM` — what's already firing, and since when. (The response splits `MetricAlarms` from `CompositeAlarms`; read both keys.)
- **App/orchestrator services** — running-vs-desired counts; anything crash-looping, or scaled to zero **unexpectedly**. Note at least one orchestrator service is designed to sit at desired 0 and is scaled up on demand by the tunnel tooling — zero there is normal, not a symptom. Confirm intent before reporting it.
- **Datastores** — the **primary platform database** *and* the **extensions database** (a separate database with its own migration head and schema-per-graph tenancy; they fail independently, and a ledger/investor-only outage localizes there), the cache, and the graph tier.
- **Graph-tier fleet capacity** — the graph tier is EC2 behind Auto Scaling groups, so its capacity check is `aws autoscaling describe-auto-scaling-groups` (desired vs in-service) — a *different* API from the container services' running-vs-desired. A partially drained graph ASG is invisible to the bullet above.
- **Search cluster** — the managed search domain's cluster health. It backs document and text search and the search MCP tools, and fails independently of the primary database — a search-only outage won't show up anywhere else in this sweep.
- **Recent errors** — the **API, worker, orchestrator, and graph-tier** log groups for an error spike aligned to the start time. The orchestrator log is where migration aborts surface, and a crash-looping daemon right after a release is one of the highest-prior causes here.

A denied/empty read is itself information (permission gap, or the thing genuinely isn't there) — say which.

## 3. Drill into the surface that lit up

Follow the one unhealthy signal down. Correlate its onset to the deploy timeline from step 1 — an error wall that starts at a deploy time points at that release (roll forward/back); one with no deploy nearby points at infra, a dependency, data, or load. The runbook lists this system's **recurring failure classes** and the tell for each — consult it, because the same handful of issues recur and each has a known fingerprint and fix.

## 4. Hand back a picture

Produce a short incident summary:

- **Impact** — what's broken, who's affected, blast radius.
- **Timeline** — when it started; what changed near then (deploy? config? load?).
- **Evidence** — the alarms/log lines/health results you actually saw (confirmed vs suspected).
- **Likely cause** — best current hypothesis, with confidence.
- **Recommended next action** — roll back, restart, scale, escalate — framed as a proposal for the user to approve, not an action already taken.

Keep it tight enough to paste into a status update. If triage is inconclusive, say so and name the next diagnostic — don't force a conclusion.
