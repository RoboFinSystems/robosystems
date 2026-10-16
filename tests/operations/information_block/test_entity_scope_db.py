"""Block reads in a reporting group show one entity's books.

Statement structures come from the reporting style and are shared by the
group, so two entities on the same style write sets against one structure.
A read that picks "the newest set of this structure" then shows whichever
entity closed last. These tests hold a parent and a subsidiary on one
structure and check every read keeps them apart.
"""

from __future__ import annotations

import os
import uuid
from datetime import date

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

import robosystems.models.extensions  # noqa: F401  (register models)
from robosystems.db.extensions import ExtensionsBase
from robosystems.models.api.information_block import ForecastMechanics
from robosystems.operations.information_block.forecast_history import (
  newest_actual_structure_id,
)
from robosystems.operations.information_block.reads import (
  get_information_block,
  list_information_block_summaries,
  list_information_blocks,
)
from robosystems.operations.roboledger.entity_scope import EntityNotInGraphError
from robosystems.operations.roboledger.fact_set import create_fact_set

pytestmark = pytest.mark.unit

PARENT = "ent_parent"
SUB = "ent_sub"
JUNE = (date(2026, 6, 1), date(2026, 6, 30))
JULY = (date(2026, 7, 1), date(2026, 7, 31))


@pytest.fixture()
def ext_session():
  """Extensions schema in the test Postgres DB, one throwaway schema per test."""
  database_url = os.environ.get("TEST_DATABASE_URL")
  if not database_url:
    pytest.skip("TEST_DATABASE_URL not configured")

  schema = f"ext_scope_{uuid.uuid4().hex[:12]}"
  engine = create_engine(database_url)
  with engine.begin() as conn:
    conn.execute(text(f'CREATE SCHEMA "{schema}"'))

  session = sessionmaker(bind=engine)()
  session.execute(text(f'SET search_path TO "{schema}"'))
  ExtensionsBase.metadata.create_all(bind=session.connection())
  session.commit()
  session.execute(text(f'SET search_path TO "{schema}"'))

  try:
    yield session
  finally:
    session.rollback()
    session.close()
    with engine.begin() as conn:
      conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    engine.dispose()


class _Group:
  """A parent and a subsidiary sharing one balance sheet structure."""

  def __init__(self, session) -> None:
    from robosystems.models.extensions import (
      Association,
      Element,
      Entity,
      Taxonomy,
    )

    self.session = session
    session.add_all(
      [
        Entity(
          id=PARENT, name="Parent Co", is_parent=True, source="native", created_by="t"
        ),
        Entity(
          id=SUB,
          name="Sub LLC",
          is_parent=False,
          parent_entity_id=PARENT,
          source="native",
          created_by="t",
        ),
      ]
    )
    taxonomy = Taxonomy(name="rs-gaap", taxonomy_type="reporting_extension")
    session.add(taxonomy)
    session.flush()
    self.taxonomy_id = taxonomy.id

    self.balance_sheet = self.structure("Balance Sheet", "balance_sheet")
    self.assets = Element(
      name="Assets",
      qname="rs-gaap:Assets",
      balance_type="debit",
      period_type="instant",
      taxonomy_id=taxonomy.id,
    )
    self.cash = Element(
      name="Cash",
      qname="rs-gaap:Cash",
      balance_type="debit",
      period_type="instant",
      taxonomy_id=taxonomy.id,
    )
    session.add_all([self.assets, self.cash])
    session.flush()
    session.add(
      Association(
        structure_id=self.balance_sheet.id,
        from_element_id=self.assets.id,
        to_element_id=self.cash.id,
        association_type="presentation",
        order_value=1.0,
      )
    )
    session.flush()

  def structure(self, name: str, block_type: str, **kwargs):
    from robosystems.models.extensions import Structure

    structure = Structure(
      name=name, block_type=block_type, taxonomy_id=self.taxonomy_id, **kwargs
    )
    self.session.add(structure)
    self.session.flush()
    return structure

  def add_report_set(
    self, entity_id: str, period, cash: float, structure=None, scenario_id=None
  ) -> str:
    from robosystems.models.extensions.roboledger import Fact

    structure = structure or self.balance_sheet
    period_start, period_end = period
    provenance = (
      {
        "origin": "forecast",
        "scenario_structure_id": scenario_id,
        "base_period": "2026-06",
        "month_index": 1,
        "drivers": [],
      }
      if scenario_id
      else {"origin": "pivot", "mapping_id": "m", "period": "2026-06"}
    )
    fact_set = create_fact_set(
      self.session,
      structure_id=structure.id,
      period_start=period_start,
      period_end=period_end,
      factset_type="report",
      entity_id=entity_id,
      scenario_id=scenario_id,
      provenance=provenance,
      created_by="t",
    )
    self.session.flush()
    self.session.add(
      Fact(
        element_id=self.cash.id,
        value=cash,
        fact_type="Numeric",
        period_start=period_start,
        period_end=period_end,
        period_type="instant",
        entity_id=entity_id,
        structure_id=structure.id,
        fact_set_id=fact_set.id,
      )
    )
    self.session.flush()
    return fact_set.id

  def add_forecast(self, entity_id: str, name: str = "Plan"):
    """A forecast block whose lever set, its entity's record, names
    ``entity_id``."""
    mechanics = ForecastMechanics(
      scenario_kind="budget", horizon_months=1, base_period="2026-06", levers=[]
    )
    forecast = self.structure(
      name, "forecast", artifact_mechanics=mechanics.model_dump(mode="json")
    )
    create_fact_set(
      self.session,
      structure_id=forecast.id,
      period_end=JULY[1],
      factset_type="custom",
      entity_id=entity_id,
      scenario_id=forecast.id,
      provenance={
        "origin": "asserted",
        "source_system": "forecast-levers",
        "asserted_by": "t",
      },
      created_by="t",
    )
    self.session.flush()
    return forecast


def _cash_values(envelope) -> list:
  rendering = envelope.view.rendering
  assert rendering is not None
  (cash,) = [row for row in rendering.rows if row.element_qname == "rs-gaap:Cash"]
  return list(cash.values)


class TestSharedStatement:
  def test_defaults_to_the_parent_even_when_the_sub_closed_last(
    self, ext_session
  ) -> None:
    group = _Group(ext_session)
    parent_set = group.add_report_set(PARENT, JUNE, 100.0)
    group.add_report_set(SUB, JULY, 7.0)

    envelope = get_information_block(ext_session, group.balance_sheet.id)

    assert envelope is not None
    assert envelope.entity_id == PARENT
    assert envelope.fact_set is not None
    assert envelope.fact_set.id == parent_set
    assert 7.0 not in _cash_values(envelope)

  def test_names_the_subsidiary(self, ext_session) -> None:
    group = _Group(ext_session)
    group.add_report_set(PARENT, JULY, 100.0)
    sub_set = group.add_report_set(SUB, JUNE, 7.0)

    envelope = get_information_block(ext_session, group.balance_sheet.id, entity_id=SUB)

    assert envelope is not None
    assert envelope.entity_id == SUB
    assert envelope.fact_set is not None
    assert envelope.fact_set.id == sub_set

  def test_series_holds_one_entity(self, ext_session) -> None:
    group = _Group(ext_session)
    group.add_report_set(PARENT, JUNE, 100.0)
    group.add_report_set(SUB, JUNE, 7.0)
    group.add_report_set(SUB, JULY, 8.0)

    parent = get_information_block(ext_session, group.balance_sheet.id, series=True)
    sub = get_information_block(
      ext_session, group.balance_sheet.id, series=True, entity_id=SUB
    )

    assert parent is not None and sub is not None
    assert _cash_values(parent) == [100.0]
    assert _cash_values(sub) == [7.0, 8.0]

  def test_an_entity_outside_the_graph_is_refused(self, ext_session) -> None:
    group = _Group(ext_session)

    with pytest.raises(EntityNotInGraphError):
      get_information_block(ext_session, group.balance_sheet.id, entity_id="ent_nope")

  def test_a_scenario_reads_its_own_entitys_books(self, ext_session) -> None:
    group = _Group(ext_session)
    forecast = group.add_forecast(SUB)
    group.add_report_set(PARENT, JUNE, 100.0)
    group.add_report_set(SUB, JUNE, 7.0)
    group.add_report_set(SUB, JULY, 9.0, scenario_id=forecast.id)

    envelope = get_information_block(
      ext_session, group.balance_sheet.id, scenario_id=forecast.id, series=True
    )

    assert envelope is not None
    assert envelope.entity_id == SUB
    assert _cash_values(envelope) == [7.0, 9.0]


class TestListing:
  def test_lists_only_the_entitys_own_forecasts(self, ext_session) -> None:
    group = _Group(ext_session)
    parent_plan = group.add_forecast(PARENT, "Parent plan")
    sub_plan = group.add_forecast(SUB, "Sub plan")

    parent = list_information_blocks(ext_session, block_type="forecast")
    sub = list_information_blocks(ext_session, block_type="forecast", entity_id=SUB)

    assert [(b.id, b.entity_id) for b in parent] == [(parent_plan.id, PARENT)]
    assert [(b.id, b.entity_id) for b in sub] == [(sub_plan.id, SUB)]

  def test_shared_blocks_list_for_every_entity_with_its_sets(self, ext_session) -> None:
    group = _Group(ext_session)
    group.add_report_set(PARENT, JUNE, 100.0)
    sub_set = group.add_report_set(SUB, JUNE, 7.0)

    blocks = list_information_blocks(
      ext_session, block_type="balance_sheet", entity_id=SUB
    )

    assert [b.id for b in blocks] == [group.balance_sheet.id]
    assert blocks[0].entity_id == SUB
    assert blocks[0].fact_set is not None
    assert blocks[0].fact_set.id == sub_set

  def test_a_subsidiarys_schedule_reads_its_own_books_by_id(self, ext_session) -> None:
    group = _Group(ext_session)
    schedule = group.structure(
      "Sub prepaid",
      "rollforward",
      entity_id=SUB,
      artifact_mechanics={
        "kind": "rollforward",
        "bs_source_element_id": group.cash.id,
        "bs_source_qname": group.cash.qname,
      },
    )

    assert list_information_blocks(ext_session, block_type="rollforward") == []
    envelope = get_information_block(ext_session, schedule.id)
    assert envelope is not None
    assert envelope.entity_id == SUB


class TestForecastHistory:
  def test_newest_actual_structure_is_the_entitys_own(self, ext_session) -> None:
    """Each entity can be on a different style, so a different structure."""
    group = _Group(ext_session)
    parent_is = group.structure("Income Statement (corp)", "income_statement")
    sub_is = group.structure("Income Statement (llc)", "income_statement")
    group.add_report_set(PARENT, JUNE, 1.0, structure=parent_is)
    group.add_report_set(SUB, JULY, 2.0, structure=sub_is)

    assert newest_actual_structure_id(ext_session, "income_statement", PARENT) == (
      parent_is.id
    )
    assert newest_actual_structure_id(ext_session, "income_statement", SUB) == (
      sub_is.id
    )


class TestListInformationBlockSummaries:
  def test_summaries_are_the_lists_rows_without_the_envelopes(
    self, ext_session
  ) -> None:
    """Same rows, same order, same entity as the full list — read without
    building an envelope per block."""
    group = _Group(ext_session)
    group.add_report_set(PARENT, JUNE, 100.0)
    parent_plan = group.add_forecast(PARENT, "Parent plan")
    group.add_forecast(SUB, "Sub plan")

    full = list_information_blocks(ext_session, limit=200)
    summaries = list_information_block_summaries(ext_session, limit=200)

    assert [(s.id, s.block_type, s.entity_id) for s in summaries] == [
      (b.id, b.block_type, b.entity_id) for b in full
    ]
    by_id = {s.id: s for s in summaries}
    balance_sheet = by_id[group.balance_sheet.id]
    assert balance_sheet.display_name == "Balance Sheet"
    assert balance_sheet.taxonomy_name == "rs-gaap"
    assert (balance_sheet.fact_count, balance_sheet.connection_count) == (1, 1)
    assert by_id[parent_plan.id].fact_count == 0

  def test_fact_count_is_the_entitys_own_on_a_shared_block(self, ext_session) -> None:
    group = _Group(ext_session)
    group.add_report_set(PARENT, JUNE, 100.0)
    group.add_report_set(SUB, JUNE, 7.0)
    group.add_report_set(SUB, JULY, 8.0)

    parent = list_information_block_summaries(ext_session, block_type="balance_sheet")
    sub = list_information_block_summaries(
      ext_session, block_type="balance_sheet", entity_id=SUB
    )

    assert [(s.id, s.entity_id, s.fact_count) for s in parent] == [
      (group.balance_sheet.id, PARENT, 1)
    ]
    assert [(s.id, s.entity_id, s.fact_count) for s in sub] == [
      (group.balance_sheet.id, SUB, 2)
    ]

  def test_a_schedules_history_is_not_counted(self, ext_session) -> None:
    """A schedule's closed periods are history, already in its opening
    balances; its envelope carries only what is in scope, and so does the
    count — a schedule that has run off lists with none."""
    from robosystems.models.extensions.roboledger import Fact

    group = _Group(ext_session)
    run_off = group.structure("Run-off prepaid", "schedule", entity_id=PARENT)
    live = group.structure("Live prepaid", "schedule", entity_id=PARENT)
    for structure, scope in ((run_off, "historical"), (live, "in_scope")):
      fact_set = create_fact_set(
        ext_session,
        structure_id=structure.id,
        period_start=JUNE[0],
        period_end=JUNE[1],
        factset_type="schedule",
        entity_id=PARENT,
        provenance={"origin": "pivot", "mapping_id": "m", "period": "2026-06"},
        created_by="t",
      )
      ext_session.flush()
      ext_session.add(
        Fact(
          element_id=group.cash.id,
          value=10.0,
          fact_type="Numeric",
          period_start=JUNE[0],
          period_end=JUNE[1],
          period_type="duration",
          entity_id=PARENT,
          structure_id=structure.id,
          fact_set_id=fact_set.id,
          fact_scope=scope,
        )
      )
    ext_session.flush()

    counts = {
      s.name: s.fact_count
      for s in list_information_block_summaries(ext_session, block_type="schedule")
    }

    assert counts == {"Run-off prepaid": 0, "Live prepaid": 1}
