---
description: Read-only AWS + GitHub security-posture audit with a prioritized remediation plan.
---

Run a read-only AWS + GitHub security-posture review, then produce a findings report and a prioritized remediation plan.

**Scope boundary.** This is an infrastructure/cloud posture audit — detective controls, live findings, data protection. It is *not* a code review. `/security-review` reviews pending code changes on the current branch; `/pr-review` covers a PR's diff. Those look at what the code does; this looks at what the account is running. Keep them separate — don't fold diff review into this sweep.

Pairs with the `security-posture-audit` runbook in `local/RoboSystems/runbooks/` — **read it alongside this file.** This file is the public, generic methodology; the runbook is the live, account-specific record of what's currently enabled, suppressed, accepted, or retired, and it is authoritative for this environment.

## Scope & guardrails

- **Read-only by default.** Use only `describe-*` / `list-*` / `get-*` and `gh` reads. **Never** run `get-secret-value`. Any change — enabling a service, creating a suppression/archive rule, deleting a resource, deploying a stack — requires **explicit in-the-moment user confirmation**, and destructive/CloudFormation actions are the user's to run. One carve-out: `generate-credential-report` is technically a write (it produces a report) but carries no configuration change — it's in scope for Phase 4.
- **Outputs are sensitive — never commit them.** This skill (the *methodology*) is safe in a public repo; its *output* — a report of live findings, exposed resources, and disabled controls — is a reconnaissance roadmap for an attacker. Write reports to a git-ignored path (`local/`, the scratchpad) or an ephemeral private Artifact. **Never commit a findings report, account ID, resource ARN, or CVE inventory to the repo.**
- **Never state live posture in this file.** Naming the *configuration* — a stack, a log group, an ECR repo, a GitHub variable — is fine and makes the sweep usable; it's already in the public IaC. Naming its *current value* is not: no "X is enabled", no finding counts, no account IDs or ARNs. Those rot silently and are a disclosure besides. Every posture question below resolves to a command you run now; anything you learn from running it belongs in the runbook, not here.
- **Context.** Discover the account at runtime (`aws sts get-caller-identity`); region defaults to `us-east-1`; the robosystems deployment runs `prod` + `staging` in one shared account, so expect per-environment resource pairs. Detective services are account-global singletons gated by GitHub variables (see Phase 6). Frontend images live in ECR repos `robosystems-app` / `roboledger-app` / `roboinvestor-app`; the backend image is `robosystems`. Confirm what's actually deployed with `aws cloudformation list-stacks --stack-status-filter CREATE_COMPLETE UPDATE_COMPLETE`.
- **Parallelize the sweep.** Phases 1–6 are independent read-only sweeps — fan them out with the Agent tool (one agent per phase) and synthesize the results in the main loop. A denied/empty CLI call is itself a finding (service not enabled, or the role lacks the read perm — say which).

## Phase 1 — Detective controls: are they ON?

The core question. For each, report ENABLED / PARTIAL / MISSING with the value seen:

```bash
aws sts get-caller-identity
aws cloudtrail describe-trails --region us-east-1        # + get-trail-status: IsLogging, multi-region, log-file validation
aws configservice describe-configuration-recorders --region us-east-1        # recorder running?
aws guardduty list-detectors --region us-east-1
aws securityhub describe-hub --region us-east-1           # InvalidAccessException = not subscribed
aws inspector2 batch-get-account-status --region us-east-1
aws accessanalyzer list-analyzers --region us-east-1
aws s3control get-public-access-block --account-id <ACCOUNT_ID>   # account-level BPA
aws ec2 get-ebs-encryption-by-default --region us-east-1
aws ec2 describe-flow-logs --region us-east-1            # per VPC (check the default VPC too)
```

## Phase 2 — Triage the live findings

For each detective service that IS on, pull and *triage* findings (don't just dump counts):

**Amazon Inspector** (usually the loudest — container/host CVEs):
- Aggregate by severity, by repo (`list-finding-aggregations --aggregation-type REPOSITORY`), and by CVE (`--aggregation-type TITLE`). **Dedup**: the same CVE is replicated across many image copies — N findings ≈ a handful of distinct CVEs × many images.
- Inspector also scans Lambda when enabled — check `batch-get-account-status` for which resource types are on, and don't assume container-only.
- Check **fix availability** (`fixAvailable` filter) — if ~100% have fixes, it's a stale-image backlog, not an incident.
- Classify each CVE: **base-image OS/runtime** (openssl, glibc, node) → one base-image bump clears many at once (stay **in-major** when the runtime/`engines` constrains it); **language deps** (npm/pip) → if the flagged dep is a **devDependency-only transitive** (test/build tooling — jsdom, vitest, eslint), exclude it from the runtime image (`npm ci --omit=dev`, don't copy dev `node_modules` into the runner stage) rather than override it — that clears the whole devDep CVE class; only override a genuine *runtime* transitive; **no upstream fix** (`fixedInVersion=NotAvailable`) → suppress.
- Watch for **inert vendored lockfiles** (e.g. a `package.json`/`package-lock.json` bundled in a non-node image with no `node_modules`) — Inspector flags declared deps that never run; strip the lockfile from the image instead of chasing the CVE.
- Check the Inspector rescan configuration — that, not the ECR lifecycle policy, is the real scan-surface knob. **Mind the nesting**: the value sits one level deeper than it looks, and the shallow path returns `null` while appearing to succeed.
  ```bash
  aws inspector2 get-configuration --region us-east-1 \
    --query 'ecrConfiguration.{rescan:rescanDurationState.rescanDuration,pullDate:rescanDurationState.pullDateRescanDuration,mode:rescanDurationState.pullDateRescanMode}'
  ```

**IAM Access Analyzer** (external/public exposure — the highest-signal, fastest-actionable):
- `list-findings-v2` **requires `--analyzer-arn`** — resolve it first:
  ```bash
  ANALYZER=$(aws accessanalyzer list-analyzers --region us-east-1 --query 'analyzers[0].arn' --output text)
  aws accessanalyzer list-findings-v2 --analyzer-arn "$ANALYZER" --region us-east-1
  ```
- Classify each ACTIVE finding: **by-design** (SSO/OIDC federation roles, intentionally-public content/CDN buckets) → archive; **real exposure** (a resource that shouldn't be public/shared) → fix or retire.
- Confirm "intentional" against IaC/source before accepting — grep the CloudFormation/config that defines the resource.

**GuardDuty**: `list-findings` **requires `--detector-id`**:
```bash
DETECTOR=$(aws guardduty list-detectors --region us-east-1 --query 'DetectorIds[0]' --output text)
aws guardduty list-findings --detector-id "$DETECTOR" --region us-east-1
```
GuardDuty flags *activity*, not configuration, so a freshly enabled detector may be baselining — but don't assume zero. Read the count, and pull `get-findings` on anything present.

**Security Hub**: `get-enabled-standards` — FSBP `INCOMPLETE` with `NO_AVAILABLE_CONFIGURATION_RECORDER` is expected when Config is off (the control-scoring half is dormant); note it, don't treat it as failure.

## Phase 3 — Data protection & resilience

```bash
aws rds describe-db-instances --region us-east-1        # StorageEncrypted, BackupRetentionPeriod, MultiAZ, DeletionProtection, PubliclyAccessible
aws elasticache describe-replication-groups --region us-east-1   # AtRest + Transit encryption
aws s3api list-buckets                                  # per relevant bucket: get-bucket-encryption, get-public-access-block, get-bucket-versioning
aws secretsmanager list-secrets --region us-east-1      # RotationEnabled (never read values)
aws backup list-backup-plans --region us-east-1         # centralized backup vs service-native snapshots only
```

**KMS** — `list-keys` returns bare key IDs with no manager distinction, so it can't answer "CMK vs AWS-managed" on its own. Loop into `describe-key`:
```bash
aws kms list-keys --region us-east-1 --query 'Keys[].KeyId' --output text | tr '\t' '\n' | while read -r k; do
  aws kms describe-key --key-id "$k" --region us-east-1 --query 'KeyMetadata.{id:KeyId,mgr:KeyManager,usage:KeyUsage}'
done
# then get-key-rotation-status on each CUSTOMER-managed key
```

**Managed search** — `list-domain-names` returns only `{DomainName, EngineType}`; the encryption fields require `describe-domain`:
```bash
aws opensearch list-domain-names --region us-east-1
aws opensearch describe-domain --domain-name <d> --region us-east-1 \
  --query 'DomainStatus.{enc:EncryptionAtRestOptions.Enabled,n2n:NodeToNodeEncryptionOptions.Enabled,https:DomainEndpointOptions.EnforceHTTPS,policy:AccessPolicies}'
```

**Edge protection** — check WAF association on both scopes; a regional ACL does **not** protect CloudFront:
```bash
aws wafv2 list-web-acls --scope REGIONAL   --region us-east-1
aws wafv2 list-web-acls --scope CLOUDFRONT --region us-east-1
aws cloudfront list-distributions --query 'DistributionList.Items[].{id:Id,enabled:Enabled,waf:WebACLId}'
```
An enabled distribution with an empty `waf` is an unprotected public edge — report it.

**Lambda & DynamoDB** — often skipped, and both sit on critical paths (secret rotation, volume management, graph routing registries):
```bash
aws lambda list-functions --region us-east-1 --query 'Functions[].{n:FunctionName,role:Role,runtime:Runtime}'
aws dynamodb list-tables --region us-east-1
aws dynamodb describe-table --table-name <t> --region us-east-1 --query 'Table.SSEDescription'
```

## Phase 4 — IAM hygiene

`get-account-summary`, `get-account-password-policy`, `list-users`, and the credential report (`generate-credential-report` → `get-credential-report`) for: root MFA, root access keys, IAM users without MFA, oldest active access-key age. If these reads are blocked by the session's permission policy, mark them **"verify"**, not "gap".

## Phase 5 — GitHub posture (optional; `gh`)

Org 2FA (`gh api orgs/<org>`), branch protection / rulesets on `main` (required approvals, admin bypass, strict checks, signed commits), secret scanning + push protection, Dependabot alerts + updates, CodeQL. For a single-member org, "require independent review" is structurally unmet — document as a compensating control, don't assert it.

## Phase 6 — Codebase controls inventory (optional)

Read `SECURITY.md` (control catalog) and the `robosystems/security/` modules.

The optional compliance stacks and their GitHub-variable toggles: `SECURITY_ENABLED` (`cloudformation/security.yaml` — the detective baseline; needs `just bootstrap` re-run first so the deploy role carries the IAM), `SECURITY_CONFIG_ENABLED` (AWS Config — the cost outlier, and the one most often left off deliberately), `CLOUDTRAIL_ENABLED` (`cloudformation/cloudtrail.yaml`), `AUDIT_ENABLED_*`, `VPC_FLOW_LOGS_ENABLED`, `SECRETS_ROTATION_ENABLED_*`, `WAF_ENABLED_*`.

**Which of these are currently on is a runtime question — read it, don't assume it:**

```bash
gh variable list | grep -iE 'security|cloudtrail|audit|flow_log|rotation|waf|endpoint'
```

Cross-check against what's actually deployed (`aws cloudformation list-stacks`), since a variable can be flipped without the stack having been redeployed. Where a control is off, determine *why* before filing it as a gap — the runbook records which have been formally accepted as risk decisions.

## Output — report + prioritized plan

Produce two things:

1. **Report** — a per-control table (ENABLED / PARTIAL / MISSING / VERIFY) grouped by area, plus a triaged findings summary (deduped counts by severity, what's actionable vs suppress vs accepted). Call out what's already solid so it isn't re-litigated.
2. **Prioritized remediation plan** — ranked by impact × effort, each item with the concrete action. Where SOC 2 is in play, tag each item **"before"** (technical toggles you flip yourself — do these early so evidence accrues; the Type II clock only counts time a control was running) vs **"with the auditor"** (scoping, policies, attestation). Flag which items are one-variable toggles vs real work.

If the result is worth sharing visually, offer to render it as an Artifact (a readiness dashboard).

## Remediation patterns (the playbook)

- **Detective services off** → enable via the gated compliance stacks (flip the GH var + deploy; re-run the bootstrap first where the deploy role needs new IAM). Turn the cheap/free-trial ones (GuardDuty, Access Analyzer, Security Hub, Inspector) on and leave them on; gate **Config** separately (cost).
- **Inspector no-fix CVE** → suppress with a filter that **auto-un-suppresses when a patch ships**. Note `create-filter` requires `--name` and `--filter-criteria`, and each criterion is an object with `comparison`/`value` — not a bare string (this is a write; the user runs it):
  ```bash
  aws inspector2 create-filter --action SUPPRESS --name "suppress-<cve>-nofix" --region us-east-1 \
    --filter-criteria '{"vulnerabilityId":[{"comparison":"EQUALS","value":"<CVE>"}],
                        "fixAvailable":[{"comparison":"EQUALS","value":"NO"}]}'
  ```
- **Inspector base-image CVEs** → bump the base image, **staying in-major** when `engines`/runtime constrain it (e.g. node `>=22` → latest `22.x`); verify the lockfile churn is scoped.
- **Inspector inert vendored lockfile** → strip it from the image (`RUN find … -name 'package*.json' -delete`), don't bump a dep that never runs.
- **Access Analyzer intentional finding** → `create-archive-rule` scoped to the resource ARN / path, then `apply-archive-rule` to clear existing.
- **Access Analyzer orphaned exposed resource** → confirm migration/references, then **retire** it (delete removes the exposure at the source — user runs the delete).
- **Stale-image drift** → the deployed image lags the lockfile; a rebuild alone clears already-fixed deps.

## Notes

- Detective services are account-global singletons — a per-env stack would collide; they live in one shared stack (`cloudformation/security.yaml`, like `cloudtrail.yaml`).
- `AWS::SecurityHub::Hub` auto-enables default standards — set `EnableDefaultStandards: false` if the stack also declares explicit `Standard` resources, or they collide.
- Security Hub's FSBP standard reports `INCOMPLETE` with `NO_AVAILABLE_CONFIGURATION_RECORDER` whenever Config is off. That's expected, not a failure — the control-scoring half is simply dormant. Note it and move on.
- A `CREATE_FAILED`→`ROLLBACK_COMPLETE` stack must be deleted before redeploy; `DeletionPolicy: Retain` buckets survive and must be cleaned up manually.
