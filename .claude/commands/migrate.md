---
description: Verify a release's database migrations applied cleanly on the orchestrator daemon's boot.
argument-hint: "[staging|prod]"
---

Verify that a release's database migrations applied cleanly. In this system migrations run automatically on the orchestrator daemon's boot during a deploy — so "running a migration" is almost always **watching that boot succeed**, not invoking anything by hand. Pairs with the `migration-monitoring` runbook in `local/RoboSystems/runbooks/` for exact service/log-group names and the manual fallback — read it alongside this file.

## How migrations actually run here

- The **orchestrator daemon** is a singleton (desired count 1). On boot it runs the schema migrations *before* the daemon comes up: platform database first, then the extensions database (only when a product domain that uses it is enabled). The behavior is gated by an environment flag rather than by environment name, so the same path runs locally too.
- Migrations are **fail-closed**: if a migration errors, the entrypoint aborts and the daemon does **not** start. A bad migration therefore surfaces as a daemon that won't boot / a crash-looping task — not as silent drift.
- One seam to know: the *schema* migrations are fail-closed, but the orchestrator's own storage-database existence check immediately before them is deliberately fail-**open** — it logs and continues. So a daemon that boots but misbehaves around its own run storage points there, not at the migrations.
- This means the schema is migrated as part of the normal deploy of the release that carries it. There is no separate "migrate" button for the common case.

## When this runs

- **Common (~99%)**: a release includes a migration and you want to confirm it landed → tail the daemon boot logs during/after the deploy for the success marker, and confirm the daemon reached a running state.
- **Rare (<1%)**: migrations must be applied out-of-band (daemon can't boot, or you're forcing a schema change ahead of the app). That's the manual path — a tunnel to the bastion + the migrate recipe — and it's in the runbook.

## Scope & guardrails

- **Reading logs is free; applying a migration is not.** Tailing the daemon's log group and checking task health need no confirmation. The **manual apply path mutates the production schema** — confirm with the user, and never run it speculatively.
- **Two databases, two histories.** Platform and extensions migrate independently. When something's off, say *which* database — the log lines and the migrate recipe are per-database.
- **Autogenerate only.** If a fix requires a *new* migration, it comes from updating the model + autogenerating — never hand-author one against prod.

## 1. Watch the boot (common path)

During the deploy, the daemon's boot log is where migrations report. You're looking for the migration-start lines, the per-database success markers, and then the daemon actually starting. Read the log group for the orchestrator daemon (name in the runbook) around the deploy time, or scope to the newest log stream.

Confirm two things:
1. The migration success marker printed (platform, and extensions if enabled).
2. The daemon task is **running** (not crash-looping) — a booted daemon is the real proof the migration passed, because a failed migration aborts boot.

## 2. If the daemon won't boot

A daemon stuck restarting right after a release almost always means the migration failed. Pull the boot log and find the abort line — it names which database and the underlying error (a bad revision, a type/constraint autogenerate missed, a missing dependency). This is a code/migration fix, re-cut into a new build — not something to paper over by editing prod.

## 3. Manual apply (rare fallback)

Only when the automatic path can't run and the user has confirmed. There are two manual paths and they differ in **coverage**:

1. **Tunnel + per-database migrate recipe** — covers **both** databases (`just migrate-up` for platform, `just migrate-up extensions` for extensions, each against its own config). **This is the working path — prefer it.**
2. **Bastion-run helper** — targets the **platform** database only, with credentials handled for you.

⚠️ **Verify the bastion helper before relying on it.** It predates the two-database split and invokes Alembic without naming a per-database config — the repo has no single top-level config anymore, only per-database ones (`migrations/platform.ini`, `migrations/extensions.ini`). That makes it fail, or run against the wrong history. Before using it in an incident, confirm it passes an explicit config; if it hasn't been fixed, use path 1.

Check the current revision before and after, on whichever database you touched. Exact commands are in the runbook.

## A note on the other "migration"

This system also has a **graph-data version migration** (export/import of graph databases across engine versions, run as orchestrator jobs around a graph-engine upgrade). That's a different operation from the schema migrations above — don't conflate them. If the task is a graph-engine version bump, that's those jobs, not this.

Note it's a **three**-job sequence, not two: export (pre-deploy), import (post-deploy), and a **cleanup** job that removes the pre-migration rollback backups. Skipping the third leaves rollback copies occupying disk across the fleet.

## Output

State plainly: did the migration apply, on which database(s), and is the daemon healthy? If it failed, name the database, quote the abort line, and describe the fix — don't declare success off a green *workflow* alone; confirm the daemon booted.
