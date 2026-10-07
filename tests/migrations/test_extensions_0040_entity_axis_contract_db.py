"""Migration 0040 against a tenant schema as 0038 left it: the column goes
NOT NULL with no default, shared blocks lose the stamp the default gave them,
a NULL row or a missing parent stops it, and a schema 0038 never reached is
expanded first. Runs the migration's own per-schema functions on real
Postgres."""

from __future__ import annotations

import importlib.util
import os
import uuid
from datetime import UTC, date, datetime
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

_VERSIONS = (
  Path(__file__).resolve().parents[2] / "migrations" / "extensions" / "versions"
)


def _load(name: str):
  spec = importlib.util.spec_from_file_location(name, _VERSIONS / f"{name}.py")
  assert spec is not None and spec.loader is not None
  module = importlib.util.module_from_spec(spec)
  spec.loader.exec_module(module)
  return module


expand = _load("0038_entity_axis_expand")
contract = _load("0040_entity_axis_contract")

GRAPH_ID = "kg0123456789abcdef40"
T0 = datetime(2026, 1, 1, tzinfo=UTC).replace(tzinfo=None)


class Tenant:
  def __init__(self, conn, schema: str) -> None:
    self.conn = conn
    self.schema = schema
    self._scoped = conn.execution_options(schema_translate_map={None: schema})

  def insert(self, model, **values) -> None:
    self._scoped.execute(model.__table__.insert().values(**values))

  def entity(self, entity_id: str, *, is_parent: bool = True) -> None:
    self.insert(
      Entity,
      id=entity_id,
      name=entity_id,
      is_parent=is_parent,
      source="native",
      created_at=T0,
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
    expand._expand(self.conn, self.schema)

  def contract(self) -> None:
    contract._contract(self.conn, self.schema)

  def relax(self) -> None:
    contract._relax(self.conn, self.schema)

  def column(self, table: str, row_id: str) -> str | None:
    return self.conn.execute(
      text(f'SELECT entity_id FROM "{self.schema}".{table} WHERE id = :id'),
      {"id": row_id},
    ).scalar_one()

  def check_constraints(self, table: str) -> list[str]:
    return list(
      self.conn.execute(
        text(
          "SELECT conname FROM pg_constraint c JOIN pg_class r ON r.oid = c.conrelid "
          "JOIN pg_namespace n ON n.oid = r.relnamespace "
          "WHERE n.nspname = :schema AND r.relname = :table AND c.contype = 'c'"
        ),
        {"schema": self.schema, "table": table},
      ).scalars()
    )

  def shape(self, table: str) -> tuple[str, str | None]:
    """``(is_nullable, column_default)`` of the table's entity column."""
    return self.conn.execute(
      text(
        "SELECT is_nullable, column_default FROM information_schema.columns "
        "WHERE table_schema = :schema AND table_name = :table "
        "AND column_name = 'entity_id'"
      ),
      {"schema": self.schema, "table": table},
    ).one()


@pytest.fixture()
def tenant():
  """A tenant schema in the shape the release before 0038 provisions."""
  database_url = os.environ.get("TEST_DATABASE_URL")
  if not database_url:
    pytest.skip("TEST_DATABASE_URL not configured")
  schema = f"ext_mig40_{uuid.uuid4().hex[:12]}"
  engine = create_engine(database_url)
  tables = [t for t in ExtensionsBase.metadata.sorted_tables if t.schema is None]
  conn = engine.connect()
  try:
    conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    ExtensionsBase.metadata.create_all(
      bind=conn.execution_options(schema_translate_map={None: schema}), tables=tables
    )
    expand._revert(conn, schema)
    conn.commit()
    yield Tenant(conn, schema)
  finally:
    conn.rollback()
    conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    conn.commit()
    conn.close()
    engine.dispose()


def test_the_column_becomes_not_null_with_no_default(tenant):
  tenant.entity("ent_parent")
  tenant.books()
  tenant.expand()

  tenant.contract()

  for table in contract.LEDGER_TABLES:
    assert tenant.shape(table) == ("NO", None), table
    # The CHECK that spared the scan is gone once the column is NOT NULL.
    assert not [c for c in tenant.check_constraints(table) if "entity_id" in c], table
  assert tenant.shape("structures") == ("YES", None)
  # What the default stamped stays stamped; the shared block is cleared.
  assert tenant.column("entries", "je_1") == "ent_parent"
  assert tenant.column("structures", "struct_schedule") == "ent_parent"
  assert tenant.column("structures", "struct_statement") is None


def test_a_writer_that_names_no_entity_now_fails_at_the_database(tenant):
  tenant.entity("ent_parent")
  tenant.books()
  tenant.expand()
  tenant.contract()
  tenant.conn.commit()

  with pytest.raises(IntegrityError):
    tenant.insert(
      Entry, id="je_late", posting_date=date(2026, 3, 9), created_by="usr_1"
    )
  tenant.conn.rollback()


def test_a_null_row_stops_the_migration(tenant):
  tenant.entity("ent_parent")
  tenant.books()
  tenant.expand()
  tenant.conn.execute(text(f'UPDATE "{tenant.schema}".events SET entity_id = NULL'))

  with pytest.raises(RuntimeError, match=f"{tenant.schema}.*events"):
    tenant.contract()


def test_books_with_two_parents_stop_the_migration(tenant):
  tenant.entity("ent_parent")
  tenant.entity("ent_other_parent")
  tenant.books()
  tenant.expand()

  with pytest.raises(RuntimeError, match="2 group parents"):
    tenant.contract()


def test_a_schema_the_expand_never_reached_is_expanded_first(tenant):
  """A tenant a task of the previous release provisioned while 0038 ran has
  books and no column."""
  tenant.entity("ent_parent")
  tenant.books()

  tenant.contract()

  assert tenant.shape("entries") == ("NO", None)
  assert tenant.column("entries", "je_1") == "ent_parent"
  assert tenant.column("fiscal_calendar", "fcal_1") == "ent_parent"


def test_a_schema_with_nothing_contracts_too(tenant):
  tenant.expand()

  tenant.contract()

  assert tenant.shape("entries") == ("NO", None)


def test_the_downgrade_restores_the_expand_shape(tenant):
  tenant.entity("ent_parent")
  tenant.books()
  tenant.expand()
  tenant.contract()

  tenant.relax()

  nullable, default = tenant.shape("entries")
  assert nullable == "YES"
  assert default is not None and "ent_parent" in default
  assert "ent_parent" in (tenant.shape("structures")[1] or "")
