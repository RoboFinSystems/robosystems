"""Tests for the cross-graph share controls — the recipient's and sender's exit.

Sharing is authorized capability-style: whoever holds a graph's id can copy a
published report into it. These are the tests for the three controls that make
that model sound — the recipient can delete what landed and block the sender,
and the sender can withdraw. See
``local/RoboSystems/specs/extensions/cross-graph-share-controls.md``.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from unittest.mock import MagicMock, patch

import pytest

from robosystems.models.api.extensions.blocked_source_graphs import (
  BlockedSourceGraphResponse,
  BlockSourceGraphRequest,
  UnblockSourceGraphRequest,
)
from robosystems.models.api.extensions.reports import RevokeReportShareRequest
from robosystems.operations.roboledger.commands.blocked_source_graphs import (
  SelfBlockError,
  SourceGraphNotBlockedError,
  block_source_graph,
  unblock_source_graph,
)
from robosystems.operations.roboledger.commands.reports import (
  NotAuthorizedError,
  ReportNotFoundError,
  ReportShareNotFoundError,
  _share_to_target,
  delete_report,
  revoke_report_share,
)

_SOURCE_GRAPH = "kg1111111111111111"
_TARGET_GRAPH = "kg2222222222222222"

_COMMANDS = "robosystems.operations.roboledger.commands"
_BLOCK_CMD = f"{_COMMANDS}.blocked_source_graphs"
_REPORTS_CMD = f"{_COMMANDS}.reports"


def _make_report(
  *,
  created_by: str = "user_sender",
  source_graph_id: str | None = None,
  filing_status: str = "draft",
) -> MagicMock:
  r = MagicMock()
  r.id = "rpt_01"
  r.created_by = created_by
  r.source_graph_id = source_graph_id
  r.source_report_id = "rpt_source" if source_graph_id else None
  r.filing_status = filing_status
  r.generation_status = "published"
  return r


# ─── G1: recipient delete ───────────────────────────────────────────────────
#
# The copied report carries the *sender's* user id in `created_by`, so the
# plain owner rule can never match anyone in the receiving graph. A graph admin
# there is who gets to remove it — and only for copies, never for native rows.


def test_owner_can_delete_their_own_report() -> None:
  session = MagicMock()
  session.get.return_value = _make_report(created_by="user_owner")

  assert delete_report(session, "rpt_01", acting_user_id="user_owner") is True


def test_graph_admin_can_delete_a_shared_copy() -> None:
  session = MagicMock()
  session.get.return_value = _make_report(
    created_by="user_sender", source_graph_id=_SOURCE_GRAPH
  )

  assert (
    delete_report(
      session,
      "rpt_01",
      acting_user_id="user_recipient_admin",
      acting_user_is_graph_admin=True,
    )
    is True
  )


def test_non_admin_cannot_delete_a_shared_copy() -> None:
  """Membership alone is not enough — removing inbound data is admin-only."""
  session = MagicMock()
  session.get.return_value = _make_report(
    created_by="user_sender", source_graph_id=_SOURCE_GRAPH
  )

  with pytest.raises(NotAuthorizedError):
    delete_report(
      session,
      "rpt_01",
      acting_user_id="user_recipient_member",
      acting_user_is_graph_admin=False,
    )


def test_graph_admin_cannot_delete_a_native_report_they_do_not_own() -> None:
  """The widening is scoped to shared copies. Being an admin does not hand
  someone the right to delete a colleague's own report."""
  session = MagicMock()
  session.get.return_value = _make_report(created_by="user_colleague")

  with pytest.raises(NotAuthorizedError):
    delete_report(
      session,
      "rpt_01",
      acting_user_id="user_admin",
      acting_user_is_graph_admin=True,
    )


# ─── G2: recipient block ────────────────────────────────────────────────────


def _block_session(existing=None) -> MagicMock:
  session = MagicMock()
  session.execute.return_value.scalar_one_or_none.return_value = existing
  return session


def _block_response() -> BlockedSourceGraphResponse:
  """A real response model — `enrich_blocks` is patched out in these tests, but
  its return value feeds Pydantic validation, so a MagicMock won't do."""
  return BlockedSourceGraphResponse(
    id="blk_01",
    source_graph_id=_SOURCE_GRAPH,
    source_graph_name="Acme Inc",
    blocked_by="user_recipient",
    blocked_at=datetime(2026, 8, 9, tzinfo=UTC),
    reason=None,
  )


def test_block_creates_a_row_and_reports_not_already_blocked() -> None:
  session = _block_session(existing=None)
  body = BlockSourceGraphRequest(source_graph_id=_SOURCE_GRAPH, reason="Exited.")

  with patch(f"{_BLOCK_CMD}.enrich_blocks") as enrich:
    enrich.return_value = [_block_response()]
    result = block_source_graph(
      session, body, created_by="user_recipient", graph_id=_TARGET_GRAPH
    )

  assert result.already_blocked is False
  assert result.purged_report_count == 0
  session.add.assert_called_once()


def test_reblocking_is_idempotent_and_does_not_rewrite_the_record() -> None:
  """A retry must not be usable to rewrite who blocked whom, and when."""
  existing = MagicMock()
  existing.blocked_by = "user_first"
  existing.blocked_at = datetime(2026, 8, 1, tzinfo=UTC)
  session = _block_session(existing=existing)

  with patch(f"{_BLOCK_CMD}.enrich_blocks") as enrich:
    enrich.return_value = [_block_response()]
    result = block_source_graph(
      session,
      BlockSourceGraphRequest(source_graph_id=_SOURCE_GRAPH),
      created_by="user_second",
      graph_id=_TARGET_GRAPH,
    )

  assert result.already_blocked is True
  session.add.assert_not_called()
  assert existing.blocked_by == "user_first"
  assert existing.blocked_at == datetime(2026, 8, 1, tzinfo=UTC)


def test_a_graph_cannot_block_itself() -> None:
  with pytest.raises(SelfBlockError):
    block_source_graph(
      MagicMock(),
      BlockSourceGraphRequest(source_graph_id=_TARGET_GRAPH),
      created_by="user_recipient",
      graph_id=_TARGET_GRAPH,
    )


def test_block_with_purge_reports_the_number_of_reports_removed() -> None:
  session = MagicMock()
  session.execute.return_value.scalar_one_or_none.return_value = None
  session.execute.return_value.scalars.return_value.all.return_value = [
    "rpt_a",
    "rpt_b",
  ]
  body = BlockSourceGraphRequest(source_graph_id=_SOURCE_GRAPH, purge=True)

  with patch(f"{_BLOCK_CMD}.enrich_blocks") as enrich:
    enrich.return_value = [_block_response()]
    result = block_source_graph(
      session, body, created_by="user_recipient", graph_id=_TARGET_GRAPH
    )

  assert result.purged_report_count == 2


def test_unblock_raises_when_the_source_was_not_blocked() -> None:
  session = _block_session(existing=None)

  with pytest.raises(SourceGraphNotBlockedError):
    unblock_source_graph(
      session, UnblockSourceGraphRequest(source_graph_id=_SOURCE_GRAPH)
    )


def test_unblock_deletes_the_row() -> None:
  row = MagicMock()
  row.id = "blk_01"
  row.source_graph_id = _SOURCE_GRAPH
  row.blocked_by = "user_recipient"
  row.blocked_at = datetime(2026, 8, 1, tzinfo=UTC)
  row.reason = None
  session = _block_session(existing=row)

  response = unblock_source_graph(
    session, UnblockSourceGraphRequest(source_graph_id=_SOURCE_GRAPH)
  )

  session.delete.assert_called_once_with(row)
  assert response.source_graph_id == _SOURCE_GRAPH


# ─── G2 enforcement: the block is honoured by the share path ────────────────


def _target_graph_row() -> MagicMock:
  graph = MagicMock()
  graph.schema_extensions = ["roboledger"]
  return graph


def _patched_share(*, blocked: bool):
  """Patch _share_to_target's two DB seams: the platform lookup and the
  target tenant session."""
  platform = MagicMock()
  platform.__enter__.return_value.execute.return_value.scalar_one_or_none.return_value = _target_graph_row()

  target_session = MagicMock()
  extensions = MagicMock()
  extensions.return_value.__enter__.return_value = target_session

  return (
    patch("robosystems.db.platform.SessionFactory", return_value=platform),
    patch("robosystems.db.extensions.extensions_session", extensions),
    patch(f"{_REPORTS_CMD}.is_source_blocked", return_value=blocked),
    target_session,
  )


def test_share_to_a_blocking_recipient_returns_an_error_and_writes_nothing() -> None:
  """Blocked senders are told, not silently dropped — under the capability
  model the two already had a relationship, so a bounce beats a shadow ban."""
  platform_p, ext_p, blocked_p, target_session = _patched_share(blocked=True)

  with platform_p, ext_p, blocked_p:
    result = _share_to_target(
      source_graph_id=_SOURCE_GRAPH,
      report_snapshot={"id": "rpt_source", "name": "Q1", "taxonomy_id": "tax_01"},
      source_facts=[],
      target_graph_id=_TARGET_GRAPH,
      shared_by="user_sender",
    )

  assert result.status == "error"
  assert "blocked" in (result.error or "").lower()
  assert result.fact_count == 0
  target_session.add.assert_not_called()
  target_session.commit.assert_not_called()


def test_share_to_an_unblocking_recipient_proceeds_to_the_copy() -> None:
  """Negative control for the test above: with no block in place the same
  fixture reaches the write, so the block assertion isn't passing vacuously."""
  platform_p, ext_p, blocked_p, target_session = _patched_share(blocked=False)

  with (
    platform_p,
    ext_p,
    blocked_p,
    patch(f"{_REPORTS_CMD}.create_fact_set"),
    patch(f"{_REPORTS_CMD}._ensure_linked_entity"),
  ):
    result = _share_to_target(
      source_graph_id=_SOURCE_GRAPH,
      report_snapshot={
        "id": "rpt_source",
        "name": "Q1",
        "description": None,
        "taxonomy_id": "tax_01",
        "mapping_id": None,
        "period_type": "quarterly",
        "period_start": date(2026, 1, 1),
        "period_end": date(2026, 3, 31),
        "comparative": False,
        "periods": None,
      },
      source_facts=[],
      target_graph_id=_TARGET_GRAPH,
      shared_by="user_sender",
    )

  assert result.status == "shared"
  target_session.add.assert_called()
  target_session.commit.assert_called_once()


# ─── G3: sender revoke ──────────────────────────────────────────────────────


def _revoke_sessions(*, report, share) -> MagicMock:
  """extensions_session stand-in for the source graph."""
  source_session = MagicMock()
  source_session.get.return_value = report
  source_session.execute.return_value.scalar_one_or_none.return_value = share

  factory = MagicMock()
  factory.return_value.__enter__.return_value = source_session
  return factory, source_session


def test_revoke_requires_the_report_to_exist() -> None:
  factory, _ = _revoke_sessions(report=None, share=None)

  with patch("robosystems.db.extensions.extensions_session", factory):
    with pytest.raises(ReportNotFoundError):
      revoke_report_share(
        _SOURCE_GRAPH,
        "rpt_01",
        RevokeReportShareRequest(target_graph_id=_TARGET_GRAPH),
        acting_user_id="user_sender",
      )


def test_only_the_reports_owner_may_revoke_its_shares() -> None:
  factory, _ = _revoke_sessions(
    report=_make_report(created_by="user_owner"), share=None
  )

  with patch("robosystems.db.extensions.extensions_session", factory):
    with pytest.raises(NotAuthorizedError):
      revoke_report_share(
        _SOURCE_GRAPH,
        "rpt_01",
        RevokeReportShareRequest(target_graph_id=_TARGET_GRAPH),
        acting_user_id="user_someone_else",
      )


def test_revoking_a_share_that_does_not_exist_raises() -> None:
  factory, _ = _revoke_sessions(
    report=_make_report(created_by="user_owner"), share=None
  )

  with patch("robosystems.db.extensions.extensions_session", factory):
    with pytest.raises(ReportShareNotFoundError):
      revoke_report_share(
        _SOURCE_GRAPH,
        "rpt_01",
        RevokeReportShareRequest(target_graph_id=_TARGET_GRAPH),
        acting_user_id="user_owner",
      )


def test_revoke_stamps_revoked_at_and_reports_the_copy_deleted() -> None:
  share = MagicMock()
  share.revoked_at = None
  factory, _ = _revoke_sessions(
    report=_make_report(created_by="user_owner"), share=share
  )

  with (
    patch("robosystems.db.extensions.extensions_session", factory),
    patch(f"{_REPORTS_CMD}._delete_shared_copy", return_value=True) as delete_copy,
  ):
    response = revoke_report_share(
      _SOURCE_GRAPH,
      "rpt_01",
      RevokeReportShareRequest(target_graph_id=_TARGET_GRAPH),
      acting_user_id="user_owner",
    )

  assert response.copy_deleted is True
  assert response.target_graph_id == _TARGET_GRAPH
  assert isinstance(response.revoked_at, datetime)
  assert share.revoked_at is not None
  delete_copy.assert_called_once_with(
    source_graph_id=_SOURCE_GRAPH,
    source_report_id="rpt_01",
    target_graph_id=_TARGET_GRAPH,
  )


def test_revoke_succeeds_when_the_recipient_already_deleted_the_copy() -> None:
  """Not an error: the recipient exercising their own exit first still leaves
  the sender's record honest."""
  share = MagicMock()
  share.revoked_at = None
  factory, _ = _revoke_sessions(
    report=_make_report(created_by="user_owner"), share=share
  )

  with (
    patch("robosystems.db.extensions.extensions_session", factory),
    patch(f"{_REPORTS_CMD}._delete_shared_copy", return_value=False),
  ):
    response = revoke_report_share(
      _SOURCE_GRAPH,
      "rpt_01",
      RevokeReportShareRequest(target_graph_id=_TARGET_GRAPH),
      acting_user_id="user_owner",
    )

  assert response.copy_deleted is False
  assert share.revoked_at is not None


# Silence an unused-import lint if `date` is dropped from a future edit.
_ = date
