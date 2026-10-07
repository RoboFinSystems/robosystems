"""Which chart, and which book mapping, are an entity's.

Before a chart was linked to an entity, a graph's chart was its earliest one
and its book mapping the earliest chart's that had one. A graph with one
entity still reads that way; a subsidiary gets only what is linked to it.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from robosystems.models.extensions import EntityTaxonomy, Taxonomy
from robosystems.operations.taxonomy_block.coa_mappings import (
  BOOK_FRAMEWORK,
  create_mapping_structure,
  entity_chart_id,
  find_entity_mapping,
)

pytestmark = pytest.mark.unit

NOW = datetime(2026, 9, 1, tzinfo=UTC).replace(tzinfo=None)


def _chart(
  session, name: str, *, days_old: int, owner: str | None = None, active: bool = True
) -> str:
  chart = Taxonomy(
    name=name,
    taxonomy_type="chart_of_accounts",
    is_active=active,
    created_at=NOW - timedelta(days=days_old),
    created_by="usr_1",
  )
  session.add(chart)
  session.flush()
  if owner is not None:
    session.add(
      EntityTaxonomy(entity_id=owner, taxonomy_id=chart.id, basis="chart_of_accounts")
    )
    session.flush()
  return str(chart.id)


def _book_mapping(session, chart_id: str) -> str:
  if not session.query(Taxonomy).filter_by(taxonomy_type="reporting_standard").count():
    session.add(
      Taxonomy(
        name=BOOK_FRAMEWORK,
        taxonomy_type="reporting_standard",
        standard=BOOK_FRAMEWORK,
        created_by="usr_1",
      )
    )
    session.flush()
  mapping = create_mapping_structure(
    session,
    chart_id=chart_id,
    framework=BOOK_FRAMEWORK,
    name="CoA mapping",
    created_by="usr_1",
  )
  return str(mapping.id)


class TestTheParentsChart:
  def test_it_is_the_earliest_linked_to_it_or_to_no_one(self, two_entities):
    t = two_entities
    imported = _chart(t.session, "Imported", days_old=2)
    _chart(t.session, "From a template", days_old=1, owner=t.parent.id)

    assert entity_chart_id(t.session, t.parent.id) == imported

  def test_a_subsidiarys_chart_is_never_it(self, two_entities):
    t = two_entities
    _chart(t.session, "Maple Court", days_old=3, owner=t.sub.id)
    own = _chart(t.session, "Harbor", days_old=1, owner=t.parent.id)

    assert entity_chart_id(t.session, t.parent.id) == own


class TestASubsidiarysChart:
  def test_it_is_only_one_linked_to_it(self, two_entities):
    t = two_entities
    _chart(t.session, "Imported", days_old=2)
    assert entity_chart_id(t.session, t.sub.id) is None

    own = _chart(t.session, "Maple Court", days_old=1, owner=t.sub.id)
    assert entity_chart_id(t.session, t.sub.id) == own


class TestTheBookMapping:
  def test_it_is_found_on_whichever_of_the_entitys_charts_has_one(self, two_entities):
    t = two_entities
    _chart(t.session, "Never mapped", days_old=3, owner=t.parent.id)
    mapping = _book_mapping(t.session, _chart(t.session, "Imported", days_old=2))

    assert find_entity_mapping(t.session, t.parent.id).id == mapping
    assert find_entity_mapping(t.session, t.sub.id) is None

  def test_a_retired_chart_keeps_its_mapping(self, two_entities):
    t = two_entities
    retired = _chart(t.session, "Retired", days_old=2, active=False)
    mapping = _book_mapping(t.session, retired)

    assert entity_chart_id(t.session, t.parent.id) is None
    assert find_entity_mapping(t.session, t.parent.id).id == mapping

  def test_each_entity_finds_its_own(self, two_entities):
    t = two_entities
    parents = _book_mapping(
      t.session, _chart(t.session, "Harbor", days_old=2, owner=t.parent.id)
    )
    subs = _book_mapping(
      t.session, _chart(t.session, "Maple Court", days_old=3, owner=t.sub.id)
    )

    assert find_entity_mapping(t.session, t.parent.id).id == parents
    assert find_entity_mapping(t.session, t.sub.id).id == subs

  def test_a_graph_with_no_entity_reads_its_unlinked_chart(self, tenant_session):
    mapping = _book_mapping(
      tenant_session, _chart(tenant_session, "Imported", days_old=1)
    )

    assert find_entity_mapping(tenant_session, None).id == mapping
