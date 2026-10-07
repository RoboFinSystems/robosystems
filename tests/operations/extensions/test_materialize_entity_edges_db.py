"""The materialized graph hangs each ledger row off the entity whose books it
is in, and links a group parent to its subsidiaries.

Runs the materializer's own DuckDB SQL through ``postgres_scan`` against a
throwaway tenant schema in the real extensions database.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

import robosystems.models.extensions  # noqa: F401  (register models on the Base)
from robosystems.config import env
from robosystems.db.extensions import ExtensionsBase, extensions_session
from robosystems.models.api.fact_provenance import AssertedProvenance
from robosystems.models.extensions.entity import Entity
from robosystems.models.extensions.roboledger import Agent
from robosystems.models.extensions.roboledger.event import Event
from robosystems.models.extensions.roboledger.report import Report
from robosystems.models.extensions.roboledger.transaction import Transaction
from robosystems.operations.extensions.materialize import (
  _staging_sql,
  build_postgres_connstr,
)
from robosystems.operations.roboledger.fact_set import create_fact_set

pytestmark = pytest.mark.integration

GRAPH = "kgdddddddddddddddd39"
PARENT, SUB = "ent_harbor", "ent_maple"
LINKED_UNKEYED, LINKED_SUB = "ent_linked_unkeyed", "ent_linked_sub"
LINKED_OTHER = "ent_linked_other"
T0 = datetime(2026, 7, 1, tzinfo=UTC)


@pytest.fixture(scope="module")
def tenant():
  url = env.EXTENSIONS_DATABASE_URL
  if not url:
    pytest.skip("EXTENSIONS_DATABASE_URL not configured")
  engine = create_engine(url)
  try:
    with engine.connect() as probe:
      probe.execute(text("SELECT 1"))
  except OperationalError as exc:
    engine.dispose()
    pytest.skip(f"extensions database unreachable: {exc.orig}")
  tables = [t for t in ExtensionsBase.metadata.sorted_tables if t.schema is None]
  try:
    with engine.begin() as conn:
      conn.execute(text(f"DROP SCHEMA IF EXISTS {GRAPH} CASCADE"))
      conn.execute(text(f"CREATE SCHEMA {GRAPH}"))
      ExtensionsBase.metadata.create_all(
        bind=conn.execution_options(schema_translate_map={None: GRAPH}),
        tables=tables,
      )
    _seed()
    yield
  finally:
    with engine.begin() as conn:
      conn.execute(text(f"DROP SCHEMA IF EXISTS {GRAPH} CASCADE"))
    engine.dispose()


def _fact_set(session, fact_set_id: str, entity_id: str, report_id: str, at: datetime):
  fact_set = create_fact_set(
    session,
    id=fact_set_id,
    period_end=date(2026, 7, 31),
    factset_type="report",
    entity_id=entity_id,
    report_id=report_id,
    provenance=AssertedProvenance(
      source_system="test", asserted_by="usr_seed", basis_note="fixture"
    ),
    created_by="usr_seed",
  )
  fact_set.created_at = at


def _seed() -> None:
  """A parent and a subsidiary with a transaction, an event and a report
  each, a report with no facts yet, and reports shared in: from a group whose
  parent and subsidiary are both linked here, and from a sender linked here by
  one keyed row."""
  with extensions_session(GRAPH) as session:
    session.add(Entity(id=PARENT, name="Harbor Holdings", created_by="usr_seed"))
    session.flush()
    session.add_all(
      [
        Entity(
          id=SUB,
          name="Maple Court LLC",
          is_parent=False,
          parent_entity_id=PARENT,
          created_by="usr_seed",
        ),
        Entity(
          id=LINKED_UNKEYED,
          name="Sender Holdings",
          source="linked",
          is_parent=False,
          metadata_={"source_graph_id": "kg_sender"},
          created_by="usr_seed",
        ),
        Entity(
          id=LINKED_SUB,
          name="Sender Sub LLC",
          source="linked",
          is_parent=False,
          metadata_={"source_graph_id": "kg_sender", "source_entity_id": "ent_src_sub"},
          created_by="usr_seed",
        ),
        Entity(
          id=LINKED_OTHER,
          name="Other Sender Inc",
          source="linked",
          is_parent=False,
          metadata_={"source_graph_id": "kg_other", "source_entity_id": "ent_other"},
          created_by="usr_seed",
        ),
        Agent(id="agt_tenant", name="Tenant", agent_type="customer", created_by="u"),
      ]
    )
    for entity_id in (PARENT, SUB):
      session.add(
        Transaction(
          id=f"txn_{entity_id}",
          entity_id=entity_id,
          type="invoice",
          amount=100,
          date=date(2026, 7, 15),
          created_by="usr_seed",
        )
      )
      session.add(
        Event(
          id=f"evt_{entity_id}",
          entity_id=entity_id,
          event_type="invoice_issued",
          event_category="sales",
          occurred_at=T0,
          source="manual",
          created_by="usr_seed",
        )
      )
    for report_id, source_graph_id in (
      ("rpt_parent", None),
      ("rpt_sub", None),
      ("rpt_empty", None),
      ("rpt_draft", None),
      ("rpt_shared", "kg_sender"),
      ("rpt_shared_empty", "kg_sender"),
      ("rpt_other_empty", "kg_other"),
    ):
      session.add(
        Report(
          id=report_id,
          name=report_id,
          taxonomy_id="tax_1",
          generation_status="pending" if report_id == "rpt_draft" else "published",
          source_graph_id=source_graph_id,
          created_by="usr_seed",
        )
      )
    session.flush()
    _fact_set(session, "fs_parent", PARENT, "rpt_parent", T0)
    _fact_set(session, "fs_sub", SUB, "rpt_sub", T0)
    _fact_set(session, "fs_draft", SUB, "rpt_draft", T0)
    # A shared report's facts keep the sender's own entity id.
    _fact_set(session, "fs_shared", "ent_src_sub", "rpt_shared", T0 + timedelta(1))
    session.commit()


def _edges(table: str) -> set[tuple[str, str]]:
  duckdb = pytest.importorskip("duckdb")
  sql = _staging_sql(GRAPH, PARENT, build_postgres_connstr())[table]
  con = duckdb.connect()
  try:
    con.execute("INSTALL postgres; LOAD postgres")
    con.execute(sql)
    return set(con.execute(f"SELECT src, dst FROM {table}").fetchall())
  finally:
    con.close()


def test_a_parent_owns_its_subsidiary_and_no_linked_company(tenant):
  assert _edges("ENTITY_OWNS_ENTITY") == {(PARENT, SUB)}


def test_a_transaction_hangs_off_its_own_entity(tenant):
  assert _edges("ENTITY_HAS_TRANSACTION") == {
    (PARENT, f"txn_{PARENT}"),
    (SUB, f"txn_{SUB}"),
  }


def test_an_event_hangs_off_its_own_entity(tenant):
  assert _edges("ENTITY_HAS_EVENT") == {
    (PARENT, f"evt_{PARENT}"),
    (SUB, f"evt_{SUB}"),
  }


def test_a_counterparty_is_the_groups(tenant):
  assert _edges("ENTITY_HAS_AGENT") == {(PARENT, "agt_tenant")}


def test_a_report_hangs_off_the_entity_it_was_generated_for(tenant):
  """A published report follows its fact sets' entity; one with no facts yet
  is the parent's; a shared one goes to the linked row keyed to the sender's
  company, once, though the sender's parent is linked here too."""
  assert _edges("ENTITY_HAS_REPORT") == {
    (PARENT, "rpt_parent"),
    (SUB, "rpt_sub"),
    (PARENT, "rpt_empty"),
    (LINKED_SUB, "rpt_shared"),
    # No facts to name the sender's company: the sender's own row, which is
    # the unkeyed one where there is one.
    (LINKED_UNKEYED, "rpt_shared_empty"),
    (LINKED_OTHER, "rpt_other_empty"),
  }
