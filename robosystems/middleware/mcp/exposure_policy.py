"""Which operations reach MCP, and why the rest deliberately do not.

Write operations reach clients on two surfaces: the REST operations routes
under ``/extensions/{domain}/{graph_id}/operations/{name}`` and the MCP tool
list. An operation registered through ``OperationRegistrar`` gets both from a
single ``OperationSpec`` — the REST route is mounted and the MCP tool is
auto-generated from the same declaration, so the two cannot drift.

Operations mounted as hand-written ``@router.post`` blocks have no such
guarantee. For those, *absence from MCP is indistinguishable from a decision
not to expose* — which is how capabilities have ended up built, complete, and
unreachable. This module removes the ambiguity: every hand-written operation
without an MCP tool must appear in :data:`MCP_EXPOSURE_HOLDS` with a reason,
and ``tests/middleware/mcp/test_operation_exposure.py`` fails the build
otherwise.

Adding a hand-written operation therefore forces a choice: expose it, or say
why not. Removing an operation forces the stale hold to be cleaned up — the
test rejects entries that no longer name a real operation.

Prefer the registrar. A hold is for operations that should not be driven by an
AI operator at all, not for ones nobody has got around to wiring.
"""

from __future__ import annotations

# Operation name → why it is REST-only. Keep the reason specific enough that a
# future reader can tell whether it still applies.
MCP_EXPOSURE_HOLDS: dict[str, str] = {
  # ── Configuration, not workflow ────────────────────────────────────────
  "initialize": (
    "One-time tenant setup. Provisioning is a control-plane action taken by "
    "the account owner, not a step inside a close."
  ),
  "set-close-target": (
    "Declares which period the owner intends to close through — a "
    "configuration decision that bounds the operator's work, so the "
    "operator must not be able to move it. close-period auto-advances the "
    "target when it is reached."
  ),
  # ── Security controls ──────────────────────────────────────────────────
  "block-source-graph": (
    "Cross-graph trust control. Reachable by the graph owner over REST only; "
    "an operator with a compromised or confused context must not be able to "
    "change which graphs may push data here."
  ),
  "unblock-source-graph": (
    "Inverse of block-source-graph, and the more dangerous direction — "
    "re-opening a source a human deliberately blocked is an owner decision."
  ),
  # ── External disclosure surface ────────────────────────────────────────
  "share-report": (
    "Mints an externally-reachable link to financial statements. Publishing "
    "outside the tenant is a human decision; revisit only with an explicit "
    "confirmation design."
  ),
  "revoke-report-share": (
    "Paired with share-report. Held together so the sharing surface has one "
    "owner rather than a half-exposed lifecycle."
  ),
  # ── Destructive ────────────────────────────────────────────────────────
  "delete-report": (
    "Destroys generated statement facts. The regenerate-report path covers "
    "the operator's legitimate need (refresh against newer ledger state) "
    "without the destructive edge."
  ),
  "delete-publish-list": "Destructive, and paired with the publish-list holds below.",
  # ── Distribution lists: follow the sharing decision ────────────────────
  "create-publish-list": (
    "Publish lists exist to drive report distribution. Held until the "
    "share-report decision above is revisited — exposing list management "
    "without the send verb buys nothing."
  ),
  "update-publish-list": "See create-publish-list.",
  "add-publish-list-members": "See create-publish-list.",
  "remove-publish-list-member": "See create-publish-list.",
  # ── Filing lifecycle ───────────────────────────────────────────────────
  "file-report": (
    "Marks a report as filed with an external authority — an assertion only "
    "a human should make."
  ),
  "transition-filing-status": (
    "Moves a filing through its regulatory state machine; same reasoning as "
    "file-report."
  ),
  # ── Superseded by a better operator path ───────────────────────────────
  "auto-map-elements": (
    "Dispatches a background bulk-mapping job. The operator's supported path "
    "is the reviewable one: get-unmapped-elements → suggest-mapping → "
    "create-mapping-association, one arc at a time with the reasoning "
    "visible. Bulk auto-mapping is a bootstrap shortcut and should not grow "
    "an agent-facing surface."
  ),
}

__all__ = ["MCP_EXPOSURE_HOLDS"]
