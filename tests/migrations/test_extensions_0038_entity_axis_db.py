"""Migration 0038 against a tenant schema as the previous release left it:
existing books land on the group parent, a task from before the migration
keeps writing through the column default, and the calendar becomes one per
entity. Runs the migration's own per-schema function on real Postgres."""

from __future__ import annotations

import importlib.util
import os
import uuid
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError

import robosystems.models.extensions  # noqa: F401  (register models on ExtensionsBase)
from robosystems.db.extensions import ExtensionsBase
from robosystems.models.extensions import Entity, Structure, Taxonomy
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.event import Event
from robosystems.models.extensions.roboledger.fiscal_calendar import FiscalCalendar
from robosystems.models.extensions.roboledger.fiscal_period import FiscalPeriod
from robosystems.models.extensions.roboledger.transaction import Transaction

pytestmark = pytest.mark.unit

_PATH = (
  Path(__file__).resolve().parents[2]
  / "migrations"
  / "extensions"
  / "versions"
  / "0038_entity_axis_expand.py"
)
_spec = importlib.util.spec_from_file_location("mig_0038", _PATH)
assert _spec is not None and _spec.loader is not None
mig = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(mig)

GRAPH_ID = "kg0123456789abcdef38"
T0 = datetime(2026, 1, 1, tzinfo=UTC).replace(tzinfo=None)
LEDGER_TABLES = (
  "entries",
  "events",
  "transactions",
  "fiscal_calendar",
  "fiscal_periods",
)


class Tenant:
  """One tenant schema and a connection that writes to it."""

  def __init__(self, conn, schema: str) -> None:
    self.conn = conn
    self.schema = schema
    self._scoped = conn.execution_options(schema_translate_map={None: schema})

  def insert(self, model, **values) -> None:
    """Insert as a task from before the migration would: no ``entity_id``."""
    self._scoped.execute(model.__table__.insert().values(**values))

  def entity(self, entity_id: str, *, is_parent: bool, source: str, at: datetime):
    self.insert(
      Entity,
      id=entity_id,
      name=entity_id,
      is_parent=is_parent,
      source=source,
      created_at=at,
      created_by="usr_1",
    )

  def books(self) -> None:
    """A row in each ledger table, a schedule and a statement structure."""
    self.insert(Entry, id="je_1", posting_date=date(2026, 3, 5), created_by="usr_1")
    self.insert(
      Event,
      id="evt_1",
      event_type="invoice_issued",
      event_category="sales",
      occurred_at=T0,
      source="manual",
      created_by="usr_1",
    )
    self.insert(
      Transaction,
      id="txn_1",
      type="invoice",
      amount=100,
      date=date(2026, 3, 5),
      created_by="usr_1",
    )
    self.insert(FiscalCalendar, id="fcal_1", graph_id=GRAPH_ID)
    self.insert(
      FiscalPeriod,
      id="fp_1",
      graph_id=GRAPH_ID,
      name="2026-03",
      start_date=date(2026, 3, 1),
      end_date=date(2026, 3, 31),
      period_type="monthly",
    )
    self.insert(Taxonomy, id="tax_1", name="Schedules", taxonomy_type="schedule")
    for structure_id, block_type in (
      ("struct_schedule", "schedule"),
      ("struct_reconciliation", "reconciliation"),
      ("struct_statement", "balance_sheet"),
    ):
      self.insert(
        Structure,
        id=structure_id,
        name=structure_id,
        block_type=block_type,
        taxonomy_id="tax_1",
      )

  def expand(self) -> None:
    mig._expand(self.conn, self.schema)

  def column(self, table: str, row_id: str) -> str | None:
    return self.conn.execute(
      text(f'SELECT entity_id FROM "{self.schema}".{table} WHERE id = :id'),
      {"id": row_id},
    ).scalar_one()

  def default(self, table: str) -> str | None:
    return self.conn.execute(
      text(
        "SELECT column_default FROM information_schema.columns "
        "WHERE table_schema = :schema AND table_name = :table "
        "AND column_name = 'entity_id'"
      ),
      {"schema": self.schema, "table": table},
    ).scalar_one()

  def entities(self) -> list[str]:
    return list(
      self.conn.execute(
        text(f'SELECT id FROM "{self.schema}".entities ORDER BY id')
      ).scalars()
    )


@pytest.fixture()
def tenant():
  """A tenant schema in the shape the release before 0038 provisions."""
  database_url = os.environ.get("TEST_DATABASE_URL")
  if not database_url:
    pytest.skip("TEST_DATABASE_URL not configured")
  schema = f"ext_mig_{uuid.uuid4().hex[:12]}"
  engine = create_engine(database_url)
  tables = [t for t in ExtensionsBase.metadata.sorted_tables if t.schema is None]
  conn = engine.connect()
  try:
    conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    ExtensionsBase.metadata.create_all(
      bind=conn.execution_options(schema_translate_map={None: schema}),
      tables=tables,
    )
    mig._revert(conn, schema)
    conn.commit()
    yield Tenant(conn, schema)
  finally:
    conn.rollback()
    conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    conn.commit()
    conn.close()
    engine.dispose()


def test_existing_books_land_on_the_group_parent(tenant):
  # The linked counterparty is older than the parent: heap order must not
  # decide whose books these are.
  tenant.entity("ent_linked", is_parent=False, source="linked", at=T0)
  tenant.entity("ent_parent", is_parent=True, source="native", at=T0 + timedelta(1))
  tenant.books()

  tenant.expand()

  for table, row_id in (
    ("entries", "je_1"),
    ("events", "evt_1"),
    ("transactions", "txn_1"),
    ("fiscal_calendar", "fcal_1"),
    ("fiscal_periods", "fp_1"),
    ("structures", "struct_schedule"),
    ("structures", "struct_reconciliation"),
  ):
    assert tenant.column(table, row_id) == "ent_parent", table
  assert tenant.column("structures", "struct_statement") is None


def test_a_task_from_before_the_migration_keeps_writing(tenant):
  tenant.entity("ent_parent", is_parent=True, source="native", at=T0)
  tenant.books()
  tenant.expand()

  tenant.insert(Entry, id="je_late", posting_date=date(2026, 3, 9), created_by="usr_1")
  tenant.insert(
    FiscalPeriod,
    id="fp_late",
    graph_id=GRAPH_ID,
    name="2026-04",
    start_date=date(2026, 4, 1),
    end_date=date(2026, 4, 30),
    period_type="monthly",
  )

  assert tenant.column("entries", "je_late") == "ent_parent"
  assert tenant.column("fiscal_periods", "fp_late") == "ent_parent"


def test_the_calendar_becomes_one_per_entity(tenant):
  tenant.entity("ent_parent", is_parent=True, source="native", at=T0)
  tenant.books()
  tenant.expand()

  # A second entity keeps its own calendar and its own March.
  tenant.insert(FiscalCalendar, id="fcal_2", graph_id=GRAPH_ID, entity_id="ent_sub")
  tenant.insert(
    FiscalPeriod,
    id="fp_2",
    graph_id=GRAPH_ID,
    entity_id="ent_sub",
    name="2026-03",
    start_date=date(2026, 3, 1),
    end_date=date(2026, 3, 31),
    period_type="monthly",
  )
  tenant.conn.commit()

  # One entity still cannot have two.
  with pytest.raises(IntegrityError):
    tenant.insert(FiscalCalendar, id="fcal_3", graph_id=GRAPH_ID, entity_id="ent_sub")
  tenant.conn.rollback()
  with pytest.raises(IntegrityError):
    tenant.insert(
      FiscalPeriod,
      id="fp_3",
      graph_id=GRAPH_ID,
      entity_id="ent_sub",
      name="2026-03",
      start_date=date(2026, 3, 1),
      end_date=date(2026, 3, 31),
      period_type="monthly",
    )
  tenant.conn.rollback()


def test_books_with_no_entity_get_the_one_the_graph_assumed(tenant):
  """A graph created with ``create_entity=false`` has books and no entity;
  the materializer has always used ``entity_<graph>`` for it."""
  tenant.books()

  tenant.expand()

  anchor = f"entity_{tenant.schema}"
  assert tenant.entities() == [anchor]
  assert tenant.column("entries", "je_1") == anchor
  assert tenant.column("fiscal_calendar", "fcal_1") == anchor
  assert anchor in tenant.default("entries")


def test_a_schema_with_no_books_is_left_alone(tenant):
  tenant.expand()

  assert tenant.entities() == []
  assert tenant.default("entries") is None


def test_books_with_entities_but_no_parent_stop_the_migration(tenant):
  tenant.entity("ent_orphan", is_parent=False, source="native", at=T0)
  tenant.books()

  with pytest.raises(RuntimeError, match=tenant.schema):
    tenant.expand()


def test_running_it_again_changes_nothing(tenant):
  tenant.entity("ent_parent", is_parent=True, source="native", at=T0)
  tenant.books()
  tenant.expand()
  tenant.insert(
    Entry,
    id="je_sub",
    posting_date=date(2026, 3, 9),
    created_by="usr_1",
    entity_id="ent_sub",
  )

  tenant.expand()

  assert tenant.column("entries", "je_1") == "ent_parent"
  assert tenant.column("entries", "je_sub") == "ent_sub"
  assert tenant.entities() == ["ent_parent"]


def test_the_downgrade_restores_the_previous_shape(tenant):
  tenant.entity("ent_parent", is_parent=True, source="native", at=T0)
  tenant.books()
  tenant.expand()

  mig._revert(tenant.conn, tenant.schema)

  columns = set(
    tenant.conn.execute(
      text(
        "SELECT table_name FROM information_schema.columns "
        "WHERE table_schema = :schema AND column_name = 'entity_id'"
      ),
      {"schema": tenant.schema},
    ).scalars()
  )
  assert columns.isdisjoint({*LEDGER_TABLES, "structures"})
  with pytest.raises(IntegrityError):
    tenant.insert(FiscalCalendar, id="fcal_2", graph_id=GRAPH_ID)
  tenant.conn.rollback()
