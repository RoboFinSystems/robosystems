---
description: Sweep the production logs for errors and warnings, separate bugs from noise, and hand back the signals worth acting on.
argument-hint: "[window, e.g. 24h | since-deploy | 4d] [env]"
---

The logs are a bug detector only if someone reads them. This skill reads them: pull every ERROR and WARNING over a window, collapse them into distinct messages, set aside the known noise, and trace each remaining signal far enough to say whether it is a bug, a transient, or noise nobody has named yet. Pairs with the `log-review` runbook in `local/RoboSystems/runbooks/`, which holds this environment's **known-noise list**, the signals already accepted or routed, and the last run's findings. Read it first: a review that rediscovers last week's accepted noise wastes the pass.

## When

- After every production deploy, over the window since it went out (the deploy is the highest-prior cause of a new error class).
- Weekly otherwise, over the last 7 days.
- When the user asks, over the window they name (default 24h).

## Scope & guardrails

- **Read-only.** Logs Insights, `filter-log-events`, `describe-*`, `gh` reads. Fixing what you find is a separate step on a feature branch, proposed and confirmed.
- **The findings are sensitive — never commit them.** They name user ids, IPs, hostnames and error bodies. Keep raw output in the scratchpad; the durable record is the runbook, which is git-ignored.
- **A finding needs a cause, not a pattern.** "Errno 5 on the graph tier" is a symptom; the finding is what tore the volume away. Trace each signal to the code or the event that produced it before calling it a bug, and say when you could not.
- **Bug bar.** Report highs, medium-highs and the mediums worth fixing. Lows stay in the raw output.

## 1. Pull the window

The application log groups are `/robosystems/<env>/{api,worker,dagster,graph-api}`. The API and the graph API write JSON with a `level` field; Dagster's own lines and uvicorn access lines are text with the level word inline (and ANSI colour codes). Run the four queries in parallel with `aws logs start-query`, polling `get-query-results` until `Complete`:

```text
# api, worker: structured
filter level in ["ERROR","CRITICAL","WARNING"]
| stats count(*) as n, min(@timestamp) as first, max(@timestamp) as last
    by level, component, substr(message, 0, 180) as msg
| sort n desc

# dagster, graph-api: text or mixed
filter @message like /ERROR|CRITICAL|WARNING|Traceback|Errno| 5\d\d /
| stats count(*) as n, min(@timestamp) as first, max(@timestamp) as last
    by substr(@message, 0, 200) as msg
| sort n desc
```

Also check the response side, which a level filter misses: `filter status_code >= 500` on the API group, and `503`/`500` access lines on the graph API.

Messages carry ids, timestamps and numbers, so the same error appears as thousands of distinct rows. Normalise before counting: strip UUIDs, `kg…` graph ids, `user_…` ids, timestamps and digits, then sum by the normalised text. Insights `@timestamp` is UTC; a Grafana panel shows local time.

## 2. Set aside the known noise

Match each group against the runbook's known-noise list and drop the matches. Count the remainder per service. If a known-noise entry has grown by an order of magnitude, it is no longer noise; say so.

## 3. Trace each remaining signal

For each distinct message left, in rough order of severity:

- **Who emitted it and when.** `filter-log-events` on the exact text gives the log stream (container, instance, run). Read the stream around the first and last occurrence, not just the line.
- **Dagster run failures.** Search the daemon log for the run id: `RUN_FAILURE` with a step error is a code or upstream failure; `Run timed out due to taking longer than … to start` is capacity (the task never started); a run worker that goes silent and fails during a deploy window is the deploy.
- **What changed near it.** `gh run list --workflow=prod.yml`, the ECS service events for a restart, recently modified SSM parameters. A signal that starts at a deploy belongs to that release.
- **Who caused it.** A caller's mistake (bad query, unknown field, refused argument) is not a platform bug unless the platform logs it at ERROR. That is a logging bug, and worth fixing so the next pass is quieter.
- **Is the data actually wrong?** Where a warning claims data was skipped or saved without a schema, check the data. A warning can be cosmetic.

## 4. Hand back

A short report, grouped:

- **Bugs**: what, where in the code, evidence, severity, proposed fix.
- **Transient**: what happened, why it recovered, what would make it recur.
- **New noise**: what emits it, and the one-line change that would quiet it.
- **Decisions needed**: anything whose fix changes behaviour rather than logging.

Then update the runbook: append new noise to the known-noise list (with the reason it is noise), record the run's findings and where each went, and bump `last_run`.
