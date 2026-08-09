"""DB-backed two-tenant proof of the cross-graph share controls.

The mock-based tests in ``test_share_controls.py`` prove the authorization
rules. This module proves the thing mocks structurally cannot: that a share is
genuinely a *cross-schema copy* — the rows land in the recipient's schema and
nowhere else — and that the block and the recipient delete really keep them out
and take them away.

Runs against the real extensions database (``EXTENSIONS_DATABASE_URL``) with two
throwaway tenant schemas created from the model metadata, the same mechanism
``create_tenant_schema`` uses. The taxonomy-library copy is skipped: none of
these paths read library content.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine, text

import robosystems.models.extensions  # noqa: F401  (register models on the Base)
from robosystems.config import env
from robosystems.db.extensions import ExtensionsBase, extensions_session
from robosystems.models.api.extensions.blocked_source_graphs import (
  BlockSourceGraphRequest,
)
from robosystems.models.api.extensions.reports import RevokeReportShareRequest
from robosystems.models.extensions import BlockedSourceGraph, Report, ReportShare
from robosystems.operations.roboledger.commands.blocked_source_graphs import (
  block_source_graph,
)
from robosystems.operations.roboledger.commands.reports import (
  ReportHasActiveSharesError,
  _share_to_target,
  delete_report,
  revoke_report_share,
)

pytestmark = pytest.mark.integration

# Valid graph-id shapes (kg + 16+ lowercase hex) so `_sanitize_schema` accepts
# them as schema names.
SOURCE_GRAPH = "kgaaaaaaaaaaaaaaaa01"
TARGET_GRAPH = "kgbbbbbbbbbbbbbbbb02"

_ENTITY_ID = "ent_test_0001"
_SENDER = "user_sender"
_RECIPIENT_ADMIN = "user_recipient_admin"

_REPORT_SNAPSHOT = {
  "id": "rpt_source_0001",
  "name": "Q1 2026 Statements",
  "description": "Quarterly distribution.",
  "taxonomy_id": "tax_rsgaap_reporting",
  "mapping_id": None,
  "period_type": "quarterly",
  "period_start": date(2026, 1, 1),
  "period_end": date(2026, 3, 31),
  "comparative": False,
  "periods": None,
}

_SOURCE_FACTS = [
  {
    "id": "fact_0001",
    "element_id": "rs-gaap:Revenues",
    "value": 1000,
    "string_value": None,
    "fact_type": "Numeric",
    "value_type": "inline",
    "content_type": None,
    "decimals": 0,
    "period_start": date(2026, 1, 1),
    "period_end": date(2026, 3, 31),
    "period_type": "duration",
    "unit": "USD",
    "entity_id": _ENTITY_ID,
    "created_at": datetime(2026, 4, 1, tzinfo=UTC),
  },
  {
    "id": "fact_0002",
    "element_id": "rs-gaap:Assets",
    "value": 5000,
    "string_value": None,
    "fact_type": "Numeric",
    "value_type": "inline",
    "content_type": None,
    "decimals": 0,
    "period_start": None,
    "period_end": date(2026, 3, 31),
    "period_type": "instant",
    "unit": "USD",
    "entity_id": _ENTITY_ID,
    "created_at": datetime(2026, 4, 1, tzinfo=UTC),
  },
]


def _tenant_tables():
  return [t for t in ExtensionsBase.metadata.sorted_tables if t.schema is None]


@pytest.fixture(scope="module")
def two_tenants():
  """Two real tenant schemas, dropped on the way out."""
  url = env.EXTENSIONS_DATABASE_URL
  if not url:
    pytest.skip("EXTENSIONS_DATABASE_URL not configured")

  engine = create_engine(url)
  tables = _tenant_tables()
  try:
    with engine.begin() as conn:
      for schema in (SOURCE_GRAPH, TARGET_GRAPH):
        conn.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
        conn.execute(text(f"CREATE SCHEMA {schema}"))
        ExtensionsBase.metadata.create_all(
          bind=conn.execution_options(schema_translate_map={None: schema}),
          tables=tables,
        )
    yield
  finally:
    with engine.begin() as conn:
      for schema in (SOURCE_GRAPH, TARGET_GRAPH):
        conn.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
    engine.dispose()


@pytest.fixture(autouse=True)
def _clean_tenants(two_tenants):
  """Truncate the tables these tests touch between cases."""
  for graph_id in (SOURCE_GRAPH, TARGET_GRAPH):
    with extensions_session(graph_id) as session:
      session.execute(text("DELETE FROM facts"))
      session.execute(text("DELETE FROM fact_sets"))
      session.execute(text("DELETE FROM reports"))
      session.execute(text("DELETE FROM blocked_source_graphs"))
      session.execute(text("DELETE FROM report_shares"))
  yield


def _patch_platform_graph_lookup():
  """`_share_to_target` validates the target against the platform DB; these
  graph ids exist only in the extensions database."""
  target_graph = MagicMock()
  target_graph.schema_extensions = ["roboledger"]

  platform = MagicMock()
  platform.__enter__.return_value.execute.return_value.scalar_one_or_none.return_value = target_graph
  return patch("robosystems.db.platform.SessionFactory", return_value=platform)


def _share() -> object:
  with _patch_platform_graph_lookup():
    return _share_to_target(
      source_graph_id=SOURCE_GRAPH,
      report_snapshot=_REPORT_SNAPSHOT,
      source_facts=_SOURCE_FACTS,
      target_graph_id=TARGET_GRAPH,
      shared_by=_SENDER,
    )


def _counts(graph_id: str) -> tuple[int, int, int]:
  with extensions_session(graph_id) as session:
    reports = session.execute(text("SELECT count(*) FROM reports")).scalar_one()
    fact_sets = session.execute(text("SELECT count(*) FROM fact_sets")).scalar_one()
    facts = session.execute(text("SELECT count(*) FROM facts")).scalar_one()
  return reports, fact_sets, facts


def test_share_writes_into_the_target_schema_only() -> None:
  """The isolation claim: a share is a copy into the recipient, and it leaves
  the sender's schema untouched."""
  result = _share()

  assert result.status == "shared"
  assert result.fact_count == len(_SOURCE_FACTS)

  assert _counts(TARGET_GRAPH) == (1, 1, len(_SOURCE_FACTS))
  assert _counts(SOURCE_GRAPH) == (0, 0, 0)


def test_the_copy_carries_honest_provenance() -> None:
  """The recipient's data is never silently commingled — the copy names where
  it came from, and stamps its arrival."""
  _share()

  with extensions_session(TARGET_GRAPH) as session:
    row = session.execute(
      text(
        "SELECT source_graph_id, source_report_id, shared_at, created_by FROM reports"
      )
    ).one()

  assert row.source_graph_id == SOURCE_GRAPH
  assert row.source_report_id == _REPORT_SNAPSHOT["id"]
  assert row.shared_at is not None
  # The copy is stamped with the *sender's* user id — which is exactly why the
  # recipient's delete had to be widened beyond the owner rule.
  assert row.created_by == _SENDER


def test_a_block_keeps_the_copy_out() -> None:
  with extensions_session(TARGET_GRAPH) as session:
    block_source_graph(
      session,
      BlockSourceGraphRequest(source_graph_id=SOURCE_GRAPH),
      created_by=_RECIPIENT_ADMIN,
      graph_id=TARGET_GRAPH,
    )

  result = _share()

  assert result.status == "error"
  assert "blocked" in (result.error or "").lower()
  assert _counts(TARGET_GRAPH) == (0, 0, 0)


def test_block_and_purge_removes_what_already_landed() -> None:
  _share()
  assert _counts(TARGET_GRAPH)[0] == 1

  with extensions_session(TARGET_GRAPH) as session:
    result = block_source_graph(
      session,
      BlockSourceGraphRequest(source_graph_id=SOURCE_GRAPH, purge=True),
      created_by=_RECIPIENT_ADMIN,
      graph_id=TARGET_GRAPH,
      acting_user_is_graph_admin=True,
    )

  assert result.purged_report_count == 1
  assert _counts(TARGET_GRAPH) == (0, 0, 0)

  with extensions_session(TARGET_GRAPH) as session:
    blocked = session.execute(
      text("SELECT source_graph_id FROM blocked_source_graphs")
    ).scalar_one()
  assert blocked == SOURCE_GRAPH


def test_a_recipient_admin_can_delete_the_copy() -> None:
  """G1 end to end: the report is owned by the sender's user id, and an admin
  of the receiving graph removes it anyway."""
  _share()

  with extensions_session(TARGET_GRAPH) as session:
    copy_id = session.execute(text("SELECT id FROM reports")).scalar_one()
    deleted = delete_report(
      session,
      copy_id,
      acting_user_id=_RECIPIENT_ADMIN,
      acting_user_is_graph_admin=True,
    )

  assert deleted is True
  assert _counts(TARGET_GRAPH) == (0, 0, 0)


def test_purge_never_touches_a_report_the_recipient_authored() -> None:
  """The purge predicate is `source_graph_id`, which a native report cannot
  match — but prove it rather than assert it."""
  _share()

  with extensions_session(TARGET_GRAPH) as session:
    session.add(
      Report(
        name="Recipient's own report",
        taxonomy_id="tax_rsgaap_reporting",
        period_type="quarterly",
        comparative=False,
        generation_status="published",
        created_by=_RECIPIENT_ADMIN,
      )
    )

  with extensions_session(TARGET_GRAPH) as session:
    block_source_graph(
      session,
      BlockSourceGraphRequest(source_graph_id=SOURCE_GRAPH, purge=True),
      created_by=_RECIPIENT_ADMIN,
      graph_id=TARGET_GRAPH,
      acting_user_is_graph_admin=True,
    )

  with extensions_session(TARGET_GRAPH) as session:
    remaining = session.execute(text("SELECT name, source_graph_id FROM reports")).all()

  assert len(remaining) == 1
  assert remaining[0].name == "Recipient's own report"
  assert remaining[0].source_graph_id is None


def test_resharing_replaces_the_copy_rather_than_duplicating_it() -> None:
  """Sharing one report to one recipient twice is ordinary — two overlapping
  publish lists, or a resend after a correction. The recipient must end up with
  one copy of the statement, not two."""
  _share()
  _share()

  with extensions_session(TARGET_GRAPH) as session:
    rows = session.execute(
      text("SELECT id, source_report_id FROM reports WHERE source_graph_id = :sgid"),
      {"sgid": SOURCE_GRAPH},
    ).all()

  assert len(rows) == 1
  assert rows[0].source_report_id == _REPORT_SNAPSHOT["id"]
  # The replaced copy took its fact sets and facts with it.
  assert _counts(TARGET_GRAPH) == (1, 1, len(_SOURCE_FACTS))


def test_revoke_clears_every_active_share_row_for_the_recipient() -> None:
  """`_delete_shared_copy` removes every copy carrying the provenance pair, so
  revocation has to stamp every active share row to match. Two rows used to
  raise `MultipleResultsFound` and surface as a 500."""
  _share()

  with extensions_session(SOURCE_GRAPH) as session:
    session.add(
      Report(
        id=_REPORT_SNAPSHOT["id"],
        name=_REPORT_SNAPSHOT["name"],
        taxonomy_id=_REPORT_SNAPSHOT["taxonomy_id"],
        period_type="quarterly",
        comparative=False,
        generation_status="published",
        filing_status="draft",
        created_by=_SENDER,
      )
    )
    for n in (1, 2):
      session.add(
        ReportShare(
          id=f"share_000{n}",
          report_id=_REPORT_SNAPSHOT["id"],
          target_graph_id=TARGET_GRAPH,
          shared_by=_SENDER,
          fact_count=len(_SOURCE_FACTS),
        )
      )

  response = revoke_report_share(
    SOURCE_GRAPH,
    _REPORT_SNAPSHOT["id"],
    RevokeReportShareRequest(target_graph_id=TARGET_GRAPH),
    acting_user_id=_SENDER,
  )

  assert response.copy_deleted is True
  assert _counts(TARGET_GRAPH) == (0, 0, 0)

  with extensions_session(SOURCE_GRAPH) as session:
    still_active = session.execute(
      text("SELECT count(*) FROM report_shares WHERE revoked_at IS NULL")
    ).scalar_one()

  assert still_active == 0


def test_a_report_with_live_shares_cannot_be_deleted_out_from_under_recipients() -> (
  None
):
  """Deleting the source report first would make its delivered copies
  unrevocable — `revoke_report_share` reads the report to authorize the
  withdrawal, so the recipient would be stuck with it permanently."""
  _share()

  with extensions_session(SOURCE_GRAPH) as session:
    session.add(
      Report(
        id=_REPORT_SNAPSHOT["id"],
        name=_REPORT_SNAPSHOT["name"],
        taxonomy_id=_REPORT_SNAPSHOT["taxonomy_id"],
        period_type="quarterly",
        comparative=False,
        generation_status="published",
        filing_status="draft",
        created_by=_SENDER,
      )
    )
    session.add(
      ReportShare(
        id="share_0001",
        report_id=_REPORT_SNAPSHOT["id"],
        target_graph_id=TARGET_GRAPH,
        shared_by=_SENDER,
        fact_count=len(_SOURCE_FACTS),
      )
    )

  with extensions_session(SOURCE_GRAPH) as session:
    with pytest.raises(ReportHasActiveSharesError):
      delete_report(session, _REPORT_SNAPSHOT["id"], acting_user_id=_SENDER)

  # Revoke first, and the same delete goes through.
  revoke_report_share(
    SOURCE_GRAPH,
    _REPORT_SNAPSHOT["id"],
    RevokeReportShareRequest(target_graph_id=TARGET_GRAPH),
    acting_user_id=_SENDER,
  )
  with extensions_session(SOURCE_GRAPH) as session:
    assert (
      delete_report(session, _REPORT_SNAPSHOT["id"], acting_user_id=_SENDER) is True
    )


def test_sender_revoke_pulls_the_copy_and_stamps_the_share() -> None:
  """G3 end to end, through the real two-schema path."""
  _share()

  # The sender's own Report row + its ReportShare record — `share_report`
  # writes these; this test drives `_share_to_target` directly, so stand them up.
  with extensions_session(SOURCE_GRAPH) as session:
    session.add(
      Report(
        id=_REPORT_SNAPSHOT["id"],
        name=_REPORT_SNAPSHOT["name"],
        taxonomy_id=_REPORT_SNAPSHOT["taxonomy_id"],
        period_type="quarterly",
        comparative=False,
        generation_status="published",
        filing_status="draft",
        created_by=_SENDER,
      )
    )
    session.add(
      ReportShare(
        id="share_0001",
        report_id=_REPORT_SNAPSHOT["id"],
        target_graph_id=TARGET_GRAPH,
        shared_by=_SENDER,
        fact_count=len(_SOURCE_FACTS),
      )
    )

  response = revoke_report_share(
    SOURCE_GRAPH,
    _REPORT_SNAPSHOT["id"],
    RevokeReportShareRequest(target_graph_id=TARGET_GRAPH),
    acting_user_id=_SENDER,
  )

  assert response.copy_deleted is True
  assert _counts(TARGET_GRAPH) == (0, 0, 0)

  with extensions_session(SOURCE_GRAPH) as session:
    revoked_at = session.execute(
      text("SELECT revoked_at FROM report_shares WHERE id = 'share_0001'")
    ).scalar_one()
    # The sender keeps their own report; only the recipient's copy is pulled.
    own_reports = session.execute(text("SELECT count(*) FROM reports")).scalar_one()

  assert revoked_at is not None
  assert own_reports == 1


def test_blocks_are_per_tenant_and_do_not_leak_across_schemas() -> None:
  """A block written in the target must be invisible in the source — the
  schema-per-graph isolation the whole model rests on."""
  with extensions_session(TARGET_GRAPH) as session:
    session.add(
      BlockedSourceGraph(source_graph_id=SOURCE_GRAPH, blocked_by=_RECIPIENT_ADMIN)
    )

  with extensions_session(SOURCE_GRAPH) as session:
    count = session.execute(
      text("SELECT count(*) FROM blocked_source_graphs")
    ).scalar_one()

  assert count == 0
