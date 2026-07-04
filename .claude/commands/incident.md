A signal says something is wrong — a customer report, an internal user, an alarm email, a spike. This skill gives you a fast, ordered headstart on **where to look**: triage the signal to a likely subsystem, gather the state that confirms or rules it out, and hand back a crisp picture (impact, likely cause, next action). Pairs with the `incident-triage` runbook for the account's known failure classes and exact log groups.

## Goal

Compress time-to-orientation. You're not expected to fix everything from here — you're expected to answer, quickly and with evidence: **what's broken, how bad, since when, and what changed.** Breadth before depth: check the obvious surfaces in parallel, then drill into the one that lights up.

## Scope & guardrails

- **Read-only triage.** `describe-*` / `list-*` / `get-*`, log reads, `gh` reads. Any remediation — restarting a service, cycling an instance, rolling back, editing data — is a separate, **confirmed** step, not part of triage. Never `get-secret-value`.
- **Output is sensitive — never commit it.** Incident notes name live hostnames, resource IDs, error contents, maybe customer identifiers. Keep them in the scratchpad or a private channel; never in the repo, never in a public Artifact.
- **State the confidence.** Distinguish *confirmed* (you saw the error/alarm) from *suspected* (fits the pattern). A wrong confident diagnosis sends everyone the wrong way.

## 1. Pin the signal

Get concrete before searching. From whoever/whatever raised it: **what** is failing (an endpoint? a product surface? everything?), **who** is affected (one tenant or all?), **when** it started, and **what changed** near then. A single-tenant issue and a platform-wide outage need completely different first moves.

Immediately check "what changed": the most recent deploy is the highest-prior cause.

```bash
gh run list --workflow=prod.yml --limit 5      # last deploys: when, success/fail, by whom
```

## 2. Is the platform up? (breadth, in parallel)

Sweep the front-to-back surfaces at once; whichever is unhealthy narrows everything. Fan these out:

- **API liveness** — the status endpoint (`/v1/status`); is the API answering HTTP at all? It's a **liveness** probe only — a 200 says the process is up, *not* that its dependencies are healthy, so don't stop here.
- **Active alarms** — `aws cloudwatch describe-alarms --state-value ALARM` — what's already firing, and since when.
- **App/orchestrator services** — running-vs-desired counts; anything crash-looping or scaled to zero.
- **Datastores** — primary database, cache, and the graph tier reachable and healthy.
- **Recent errors** — the API and worker log groups for an error spike aligned to the start time.

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
