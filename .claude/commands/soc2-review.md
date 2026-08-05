---
description: Assess SOC 2 readiness against the Trust Services Criteria and produce a readiness dashboard.
---

Assess SOC 2 readiness: map the security posture to the Trust Services Criteria, inventory policies and evidence, and produce a readiness dashboard plus a before-vs-with-the-auditor remediation plan. This is the compliance-framed companion to `/security-audit` (which gathers the raw posture).

Pairs with the `soc2-readiness-review` runbook in `local/RoboSystems/runbooks/` — **read it alongside this file.** This file is the public, generic methodology; the runbook is the live record of the current TSC mapping, which evidence slots are filled, and what the compliance platform already automates. It is authoritative for this environment.

## Scope & guardrails

- **Read-only by default**, same as `/security-audit` — describe/list/get and `gh` reads only; never `get-secret-value`; every change needs explicit user confirmation.
- **The readiness report is sensitive — never commit it.** The *methodology* (this skill) is public-safe; the *output* names your exact unmet controls, disabled services, and audit weaknesses — a roadmap for both attackers and adversarial due-diligence. Write it to a git-ignored path (`local/`, scratchpad) or an ephemeral private Artifact. Never commit the readiness report to the repo.
- **Not attestation or legal advice.** This is engineering-readiness guidance. Only a **licensed CPA firm** can determine SOC 2 compliance and issue a report — say so in the output.

## Step 1 — Gather the posture

Run `/security-audit` (or its Phase 1–6 sweep) for the live control state + findings. Then read the code-level and documentary controls:
- `SECURITY.md` (control catalog) and `robosystems/security/` modules.
- The written **policy set** and any risk-acceptance register (in the git-ignored design vault `local/RoboSystems/` if present — e.g. `ref/security.md`, `policies/`). Note which of the auditor-expected policies exist vs are missing (infosec, access control, change management, incident response, BCDR, vendor/supplier risk, risk assessment, data classification/retention, encryption/key management, SDLC, HR/acceptable-use).

## Step 2 — Map to the Trust Services Criteria

Assess each criterion **Have / Partial / Gap** on both control *design* AND whether *operating evidence* exists:

- **CC1** Control Environment — governance, org structure, policies (note: a single-member org can't meet segregation-of-duties; document as a compensating control, not "met").
- **CC2** Communication & Information — internal + external commitments.
- **CC3** Risk Assessment — a dated, standalone assessment + register (not just folded into a policy).
- **CC4** Monitoring — CloudTrail, security alarms, Config/Security Hub (control-scoring is dormant while Config is off).
- **CC5** Control Activities — the technical control set in practice.
- **CC6** Logical & Physical Access (the largest) — auth, RBAC, encryption in transit + at rest, **MFA enforcement**, tenant isolation.
- **CC7** System Operations — GuardDuty/Inspector detection, incident response, recovery/DR.
- **CC8** Change Management — PR gates, branch protection/required reviews, CI/CD.
- **CC9** Risk Mitigation — vendor/supplier management, BCDR, insurance.
- Optional categories that fit a financial-data platform: **Availability (A1)** (backups, Multi-AZ, capacity, DR-test evidence) and **Confidentiality (C1)** (classification, retention, disposal).

## Step 3 — Separate design from operating evidence (the core SOC 2 truth)

A SOC 2 report attests to *an organization operating a system over time* — not to code.
- **Type I** = controls designed + in place at a point in time. The reference architecture nearly pre-meets the *technical* half → a fast Type I head start.
- **Type II** = operating effectiveness across an observation window (commonly 3–12 months). It **can't be inherited or backdated** — evidence must accrue while controls run.
- So tag every gap: **"before the auditor"** = technical toggles you flip yourself (do these *early* — the Type II clock only counts time a control was actually running) vs **"with the auditor"** = scoping the report type + window, finalizing policies, the in-period pen test, and the attestation itself.

## Step 4 — Produce the deliverables

1. **Readiness dashboard** as an Artifact (load the `artifact-design` skill first): a scorecard (solid / partial / gap), the **before-vs-with-the-auditor** split, a turn-on queue of the off-by-default controls, the TSC coverage matrix (CC1–CC9 + A/C), a "what's already solid" list, and the path to a report (flip toggles → readiness/gap assessment → observation window → CPA attestation).
2. **Prioritized readiness plan** — ranked, each item flagged before/with-auditor and one-variable-toggle vs real work.

## SOC 2 posture notes (weave into the narrative)

- Only a licensed CPA firm can issue the report; compliance platforms (Vanta / Drata / Secureframe) automate evidence collection and connect you to a partner audit firm — the common fast path.
- **Fork inheritance:** a fork inherits control *design* (a Type I head start), never Type II operating evidence — the forker runs their own audit. See `local/RoboSystems/` specs on the managed-BYOC model, where an operator running client deployments carries the SOC 2 and clients inherit it as a subservice organization (enables a fleet audit + audit-firm partnership).
- Keep the framing **modest and honest** — "audit-ready design + evidence infrastructure," never "SOC 2 certified."
