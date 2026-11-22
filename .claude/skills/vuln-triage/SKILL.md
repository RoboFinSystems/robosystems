---
name: vuln-triage
description: "Triage the vulnerability queue across the GitHub org and Vanta: Dependabot and code-scanning alerts, plus Vanta's vulnerability list and failing vulnerability-management tests (which carry the AWS Inspector host and container findings). Each item ends fixed, riding an open PR, waiting on the monthly AMI roll, or dismissed or deactivated with a reason, inside the Vanta SLA. Use for \"vuln triage\", \"dependabot alerts\", \"code scanning alerts\", \"security alerts\", \"what's due in Vanta\", \"vanta vulnerabilities\", or monthly."
metadata:
  lane: operate
---

Work the vulnerability queue from its two sources, GitHub (Dependabot + code scanning, every repo in the org) and Vanta (its vulnerability list and the vulnerability-management tests that fail on it), and leave every item with an owner and a date.

**Scope boundary.** `/security-audit` asks whether the detective controls are on; this works through what they find, against the SLA. `/security-review` and `/pr-review` look at code changes. A code-scanning alert that is a real defect in our code leaves this sweep as a fix in its own repo.

Pairs with the `vulnerability-triage` runbook in `local/RoboSystems/runbooks/` — **read it first.** It holds the standing decisions (what is dismissed and why, which repos are excluded, which Vanta deactivations are live and when they expire). This file is the method; the runbook is the state, and it wins for this environment.

## Guardrails

- **Reads are free; writes are records.** A GitHub dismissal and a Vanta deactivation are reversible, but they are the audit trail for the SOC 2 vulnerability-management controls. Make them when the request asks for non-issues to be cleared; otherwise propose the list. A fix is a branch and a PR in the owning repo, never a commit to `main`.
- **Public repos.** Dismissal comments are visible only to people with security access, but write them as if public: say why it is not exploitable here, not how it would be. A real finding in our code follows the disclosure rules in `CLAUDE.local.md` (terse public text, detail in the vault).
- **Never commit the output.** The triage table is a CVE inventory of live infrastructure. It goes in the runbook or the scratchpad.

## Phase 1 — GitHub, every repo

```bash
for r in $(gh repo list RoboFinSystems --limit 100 --json name,isArchived --jq '.[] | select(.isArchived|not) | .name'); do
  d=$(gh api "repos/RoboFinSystems/$r/dependabot/alerts?state=open&per_page=100" --jq length 2>/dev/null || echo n/a)
  c=$(gh api "repos/RoboFinSystems/$r/code-scanning/alerts?state=open&per_page=100" --paginate --jq length 2>/dev/null || echo n/a)
  echo "$r dependabot=$d codescan=$c"
done
```

- **Quote the `?` URLs.** zsh treats an unquoted `?` as a glob and fails with `no matches found`.
- `--paginate --jq length` prints one count per page; read every line before believing a total.
- `n/a` means the feature is off or never ran (403 / 404), not zero. A fork or a repo with no code needs neither; a repo of our own code with a lockfile should have Dependabot alerts on.
- Detail per alert: `dependabot/alerts` → `security_advisory.{severity,summary,ghsa_id}`, `dependency.{manifest_path,scope,relationship}`, `security_vulnerability.first_patched_version`. Code scanning → `rule.id`, `most_recent_instance.location.path`, and `uniq -c` on rule × path, since one generated file can carry dozens of alerts.

## Phase 2 — Vanta

- `getSlas` — the windows the dates come from (vulnerabilities: critical, high, medium, low).
- `listTests` with `categoryFilter: [ENGINEERING]` and `statusFilter: [NEEDS_REMEDIATION, DUE_SOON, OVERDUE]` — which vulnerability tests are at risk, per source (GitHub Dependabot, AWS Inspector, AWS container) and severity.
- `listVulnerabilities` — the full list runs to hundreds of KB and spills to a file. Summarize it with `python3 -I` instead of reading it: group by `integrationId` × `severity` × active/deactivated (`deactivateMetadata`), then by `targetId` × `packageIdentifier` × `fixedVersion`, and take the `remediateByDate` range.
- `listVulnerableAssets(integrationId: aws)` — maps the opaque `targetId`s to names: graph writer and replica ASGs and the bastion are **hosts**; ECR repositories are **container images**.
- Deactivated items carry `deactivatedUntilDate`. Any that lapse before the next run go on the list now.

## Phase 3 — Classify

| What it is | Outcome |
|---|---|
| **EC2 host OS packages** (kernel, glibc, rpm, docker, … on graph writers, replicas, bastion) | **Wait for the monthly AMI.** Hosts change only when the AMI id updates (`graph-maintenance.yml`: staging on the 1st, prod on the 2nd–3rd), and cycling instances is disruptive. Writers are replaced only by an opted-in roll (`cycle_writers=true`, `graph-writer-roll` runbook), so writer findings clear when that roll runs, not when the AMI lands. Act early only on a critical or actively exploited CVE (CISA KEV), which is rare. If a due date lands before the roll will, deactivate in Vanta until after it. |
| **Container image packages** (ECR: the API image, the three app images) | **Be proactive**: shipping an image is cheap. Bump the base image or the dependency and let the next release carry it. A finding can sit in a library the language runtime bundles inside its own binary, where a package bump cannot reach it; the fix then waits for a runtime release that carries it (the runtime's release index lists the bundled versions). If the fix is not yet upstream, deactivate in Vanta as "fix not yet available" with an expiry, and record the trigger. |
| **Dependabot, runtime** | Fix. Look for an open Dependabot PR first (`gh pr list --author app/dependabot`): merging it closes the alert. Python lockfiles sit behind uv's 7-day release cooldown. |
| **Dependabot, dev-only transitive** | Check whether the parent's latest release already takes the fixed version (`npm view <parent>@latest dependencies.<pkg>`). Lint/build tooling fed our own inputs is not exploitable: dismiss `tolerable_risk`, naming the parent and the bump that will clear it. |
| **Server-side code path we never run** (e.g. an OAuth *server* flaw in a library used as a client) | Not exploitable; still prefer the bump when it is a lockfile line. Dismiss `not_used` only when no bump exists. |
| **Code scanning in generated or vendored files** (Upptime workflows, forks) | `won't fix`: the file is overwritten upstream. |
| **`actions/missing-workflow-permissions`** | Moot while the org and repo default `GITHUB_TOKEN` is read-only (`gh api repos/<o>/<r>/actions/permissions/workflow`). `won't fix`, or add `permissions: contents: read` when the file is being edited anyway. |
| **Code scanning, real, in our code** | Fix in its repo. Public text stays terse. |

## Phase 4 — Act

GitHub dismissals (`dismissed_comment` ≤ 280 chars):

```bash
gh api -X PATCH repos/RoboFinSystems/<repo>/dependabot/alerts/<n> \
  -f state=dismissed -f dismissed_reason=tolerable_risk -f dismissed_comment="<why>"
# reasons: fix_started | inaccurate | no_bandwidth | not_used | tolerable_risk

gh api 'repos/RoboFinSystems/<repo>/code-scanning/alerts?state=open&per_page=100' --paginate --jq '.[].number' > "$SCRATCH/ids.txt"
while read -r n; do
  gh api -X PATCH repos/RoboFinSystems/<repo>/code-scanning/alerts/$n \
    -f state=dismissed -f "dismissed_reason=won't fix" -f dismissed_comment="<why>" --silent
done < "$SCRATCH/ids.txt"
# reasons: false positive | won't fix | used in tests
```

**Never loop over `for n in $ids`**: zsh does not word-split an unquoted variable, so the loop runs once on the whole list and fails. Read ids from a file with `while read`.

Vanta: `deactivateTestEntities(testId, entities, reason, expirationDays)` on the failing test. Always give an expiry for a "fix not yet available" or "waits for the AMI roll" deactivation, so it comes back on its own; ask before making one permanent.

## Phase 5 — Verify and record

- Re-run the Phase 1 loop: the only open alerts left should be the ones with a fix in flight.
- **Vanta lags GitHub.** A dismissal shows in Vanta after its next GitHub sync, hours to a day later. Re-check the tests' status then; if the alerts are still active there, deactivate them in Vanta too.
- Update the runbook: `last_run`, the standing decisions, every deactivation with its expiry, and what each waiting item waits on.
- Report: the counts by source, then the decision per group, the fixes in flight with their PR links, and the next date anything comes due.
