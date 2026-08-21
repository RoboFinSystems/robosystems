# Tenant isolation harness

A **black-box** authorization harness: it provisions real test tenants and
fires a cross-tenant + privilege-escalation matrix at a *live deployment* over
HTTP, classifying each response as PASS / LEAK / … . It is the internal,
repeatable "simulated pentest" for multi-tenant isolation.

## Running it

Opt-in — it skips unless `TARGET_API_URL` is set, so it is inert in a normal
`just test` / CI run:

```bash
# against the local stack (default), or pass a target
just test-isolation
just test-isolation https://staging.api.robosystems.ai

# equivalently, by hand
TARGET_API_URL=http://localhost:8000 uv run pytest tests/security/isolation -m isolation -s

# the oracle's own unit tests run in the normal suite (no live target)
uv run pytest tests/security/isolation/test_oracle.py
```

A JSON report lands at `.local/isolation-report.json` (override with
`ISOLATION_REPORT`).

## What it tests

- **Horizontal** — `test_horizontal.py`. A principal in tenant A attempts
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
- **Scoping** — `test_scoping.py`. Isolation units finer than the user↔graph
  pair: **subgraph isolation** (tenant B denied on tenant A's `{parent}_{name}`
  subgraph id — a distinct access path) and **graph-scoped API keys** (a key
  minted for graph A is denied on `graph_a2`, a second graph the *same user*
  owns, proving the denial is the key's scope and not user access).
- **Vertical** — `test_vertical.py`. Privilege escalation within one org: a graph
  VIEWER can't write (REST or Cypher), a MEMBER can't administer, and an org ADMIN
  receives implicit graph ADMIN (derived-privilege positive). Runs when the target
  has both `ORG_MEMBER_INVITATIONS_ENABLED` and `AUTH_INVITE_TOKEN_IN_RESPONSE` on
  (see provisioning below); otherwise it skips.

## The oracle (why a status code isn't enough)

Isolation is judged from **API responses only, never the database**. A status
code cannot tell a leak from a correctly-scoped empty result: a `200` carrying
another tenant's org metadata with empty collections is indistinguishable from a
scoped empty response by status alone. So the verdict compares the attacker's response against what the
**owner** legitimately sees (the positive control is the truth source):

- **PASS** — attacker denied (401/403/404), or a 2xx that carries no content.
- **LEAK** — attacker got owner data (identical fingerprint), or *any* content
  for a graph it doesn't own, or an accepted cross-tenant write.
- **INVALID** — the owner's own request failed → harness misconfigured, abort;
  a green run that never proved the owner's access proves nothing.
- **INCONCLUSIVE** — a 2xx with no content (authz didn't deny but nothing
  leaked), or a validation-rejected write — worth a human look.

`test_oracle.py` feeds the classifier synthetic **leaks** and asserts it fires —
including the empty-collections shape above — so a green live run (where the
attacker is always denied and the LEAK path never executes) isn't the only
evidence the classifier works.

## Connection model (for a prod run)

Two legs, only one is the test. The **API leg** is public
HTTPS from the operator's machine (the whole matrix). The **DB leg** is an SSM
tunnel used *outside* the matrix only, for teardown-disposal verification and
provisioning fallback. The harness here uses the API leg exclusively.

## Vertical-axis provisioning

The vertical axis needs a second principal inside one org, added by email
invitation — and the **raw invite token is neither returned by the API nor
recoverable from the DB** (stored as `sha256(token)`; the raw token is emailed).
Rather than intercept email, this is unblocked by a **test-support seam**:
`AUTH_INVITE_TOKEN_IN_RESPONSE` (default off) returns the raw token in the
invitation create response, guarded by `env.expose_invite_token_in_response()`
which forces it off in production regardless of the flag. Turn it on (plus
`ORG_MEMBER_INVITATIONS_ENABLED`) in a non-prod target and the axis provisions
invite → register → grant and runs; leave either off and it skips.
