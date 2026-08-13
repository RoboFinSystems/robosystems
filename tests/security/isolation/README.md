# Tenant isolation harness

A **black-box** authorization harness: it provisions real test tenants and
fires a cross-tenant + privilege-escalation matrix at a *live deployment* over
HTTP, classifying each response as PASS / LEAK / … . It is the internal,
repeatable "simulated pentest" for **R-11 (multi-tenant isolation)** — the
load-bearing claim of the product and, until this, the one HIGH-inherent risk
never tested by anyone.

Design + rationale: `local/RoboSystems/specs/security/tenant-isolation-harness.md`.
Execution runbook: `local/RoboSystems/runbooks/simulated-pentest.md`.

## Running it

Opt-in — it skips unless `TARGET_API_URL` is set, so it is inert in a normal
`just test` / CI run:

```bash
# against the local stack
TARGET_API_URL=http://localhost:8000 uv run pytest tests/security/isolation -m isolation -s

# the oracle's own unit tests run in the normal suite (no live target)
uv run pytest tests/security/isolation/test_oracle.py
```

A JSON report lands at `.local/isolation-report.json` (override with
`ISOLATION_REPORT`). That file is the artifact to attach to SOC 2 evidence and
to hand the paid gray-box tester as the scope fixture.

## What it tests

- **Horizontal (R-11)** — `test_horizontal.py`. A principal in tenant A attempts
  to read/write tenant B's graph, in **both directions**, across:
  - REST reads (`/info`, `/schema`, `/members`, `/limits`, `/credits`, `/tables`,
    `/backups`, `/subgraphs`),
  - a Cypher read (`/query/cypher` — the strongest data probe: it reads node
    counts),
  - the `add-member` write, plus the destructive/sensitive **core ops**
    (`delete-graph`, `materialize`, `change-tier`, `create-backup`,
    `create-subgraph`, `update-graph-metadata`),
  - MCP (`read-graph-cypher` + `write-graph-cypher` via `/mcp/call-tool`),
  - GraphQL (`/extensions/{graph_id}/graphql`): `{ hello }` (access probe) and
    `{ entity { name } }` (data-leak probe),
  - the **extensions command surfaces** for both products — roboledger
    (`build-fact-grid`, `compute-metrics`, `create-event-block`, `create-report`,
    `close-period`) and roboinvestor (`create-portfolio-block`, `create-security`,
    `update-portfolio-block`, `delete-security`). Graphs are provisioned with both
    extensions.
- **Vertical** — `test_vertical.py`. Privilege escalation within one org
  (viewer ≠ write, member ≠ administer, org-admin = implicit graph admin).
  **Currently provision-skipped** — see the blocker below.

## The oracle (why a status code isn't enough)

Isolation is judged from **API responses only, never the database**. A status
code cannot tell a leak from a correctly-scoped empty result, and it passes the
known real leak shape — a `200` carrying another tenant's `org_name` with empty
collections. So the verdict compares the attacker's response against what the
**owner** legitimately sees (the positive control is the truth source):

- **PASS** — attacker denied (401/403/404), or a 2xx that carries no content.
- **LEAK** — attacker got owner data (identical fingerprint), or *any* content
  for a graph it doesn't own, or an accepted cross-tenant write.
- **INVALID** — the owner's own request failed → harness misconfigured, abort;
  a green run that never proved the owner's access proves nothing.
- **INCONCLUSIVE** — a 2xx with no content (authz didn't deny but nothing
  leaked), or a validation-rejected write — worth a human look.

`test_oracle.py` feeds the classifier synthetic **leaks** and asserts it fires —
including the `org_name` F5 shape — so a green live run (where the attacker is
always denied and the LEAK path never executes) isn't the only evidence the
classifier works.

## Connection model (for a prod run)

Two legs, only one is the test — see the runbook. The **API leg** is public
HTTPS from the operator's machine (the whole matrix). The **DB leg** is an SSM
tunnel used *outside* the matrix only, for teardown-disposal verification and
provisioning fallback. The harness here uses the API leg exclusively.

## Vertical-axis provisioning blocker (spec OQ1)

The vertical axis needs a second principal inside one org. The only way to add
one is an email invitation, and the **raw invite token is neither returned by
the API nor recoverable from the DB** (stored as `sha256(token)`; the raw token
is emailed). So a multi-user org can't be provisioned black-box *or* via the DB
side channel. Activating the vertical axis needs email interception or a
test-support seam that surfaces the raw token in a non-prod build. The intended
matrix is written out (behind the skip) in `test_vertical.py`.
