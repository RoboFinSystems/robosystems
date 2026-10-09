"""A chart's mappings, each anchored to a mapping taxonomy the chart owns —
real Postgres.

Covers the anchor (source = the chart, target = the framework), selection by
framework rather than by row order, the chart's Taxonomy Block owning its
mappings through create / read / update / delete, and migration 0037, which
re-anchors a mapping left on the chart.
"""

from __future__ import annotations

import importlib.util
import os
import uuid
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

import robosystems.models.extensions  # noqa: F401  (register models on ExtensionsBase)
from robosystems.db.extensions import ExtensionsBase
from robosystems.models.api.taxonomy_block import (
  CreateTaxonomyBlockRequest,
  DeleteTaxonomyBlockRequest,
  StructureUpdatePatch,
  TaxonomyBlockElementRequest,
  TaxonomyBlockStructureRequest,
  UpdateTaxonomyBlockRequest,
)
from robosystems.models.extensions import (
  Association,
  Element,
  Structure,
  Taxonomy,
  Trait,
)
from robosystems.operations.roboledger.reads.taxonomies import list_mappings
from robosystems.operations.taxonomy_block import chart_of_accounts as chart_block
from robosystems.operations.taxonomy_block import (
  custom_ontology as custom_ontology_block,
)
from robosystems.operations.taxonomy_block.coa_mappings import (
  FrameworkNotInLibraryError,
  MappingAlreadyExistsError,
  MappingOutsideChartError,
  find_mapping_structure,
)

pytestmark = pytest.mark.unit

GAAP_MAPPING = "CoA to US GAAP Mapping"
US_GAAP_MAPPING = "CoA to us-gaap Mapping"


@pytest.fixture()
def ext_session():
  database_url = os.environ.get("TEST_DATABASE_URL")
  if not database_url:
    pytest.skip("TEST_DATABASE_URL not configured")

  schema = f"ext_coamap_{uuid.uuid4().hex[:12]}"
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


@pytest.fixture()
def library(ext_session):
  """rs-gaap and us-gaap in the library, one concept each, and the EFS traits."""
  concepts = {}
  for framework, qname in (
    ("rs-gaap", "rs-gaap:RentExpense"),
    ("us-gaap", "us-gaap:OperatingLeaseExpense"),
  ):
    taxonomy = Taxonomy(
      name=framework,
      taxonomy_type="reporting_standard",
      standard=framework,
      version="v1",
      is_shared=True,
      is_locked=True,
      created_by="library-seeder",
    )
    ext_session.add(taxonomy)
    ext_session.flush()
    concept = Element(
      name=qname,
      qname=qname,
      balance_type="debit",
      period_type="duration",
      taxonomy_id=taxonomy.id,
      source=framework,
      created_by="library-seeder",
    )
    ext_session.add(concept)
    concepts[framework] = (taxonomy, concept)
  ext_session.add_all(
    [
      Trait(category="elementsOfFinancialStatements", identifier="asset"),
      Trait(category="elementsOfFinancialStatements", identifier="expense"),
    ]
  )
  ext_session.flush()
  return concepts


def _chart_payload(*structures: TaxonomyBlockStructureRequest):
  return CreateTaxonomyBlockRequest(
    name="Tenant CoA",
    taxonomy_type="chart_of_accounts",
    elements=[
      TaxonomyBlockElementRequest(
        qname="coa:1000", name="Cash", code="1000", trait="asset"
      ),
      TaxonomyBlockElementRequest(
        qname="coa:6000", name="Rent", code="6000", trait="expense"
      ),
    ],
    structures=list(structures),
  )


def _mapping(name: str, framework: str | None = None):
  return TaxonomyBlockStructureRequest(
    name=name, block_type="coa_mapping", target_framework=framework
  )


def _map_rent(session, structure: Structure, concept: Element) -> Association:
  rent = session.query(Element).filter_by(qname="coa:6000").one()
  arc = Association(
    structure_id=structure.id,
    from_element_id=rent.id,
    to_element_id=concept.id,
    association_type="mapping",
  )
  session.add(arc)
  session.flush()
  return arc


class TestAnchor:
  def test_a_chart_mapping_hangs_off_a_mapping_taxonomy_the_chart_owns(
    self, ext_session, library
  ):
    chart_id = chart_block.create(
      ext_session, _chart_payload(_mapping(GAAP_MAPPING)), "u"
    )

    mapping = find_mapping_structure(ext_session, "rs-gaap", chart_id=chart_id)
    assert mapping is not None
    anchor = ext_session.get(Taxonomy, mapping.taxonomy_id)
    assert anchor is not None
    assert anchor.taxonomy_type == "mapping"
    assert anchor.source_taxonomy_id == chart_id
    assert anchor.target_taxonomy_id == library["rs-gaap"][0].id
    assert anchor.name == GAAP_MAPPING

  def test_selection_is_by_framework_not_row_order(self, ext_session, library):
    # The us-gaap mapping is created first and sorts first by name.
    chart_id = chart_block.create(
      ext_session,
      _chart_payload(_mapping("A us-gaap mapping", "us-gaap"), _mapping(GAAP_MAPPING)),
      "u",
    )

    book = find_mapping_structure(ext_session, chart_id=chart_id)
    other = find_mapping_structure(ext_session, "us-gaap", chart_id=chart_id)
    assert book is not None and other is not None
    assert book.name == GAAP_MAPPING
    assert other.name == "A us-gaap mapping"
    assert (
      find_mapping_structure(ext_session, "rs-gaap", chart_id="tax_no_such_chart")
      is None
    )

    listed = list_mappings(ext_session).structures
    assert [(s.name, s.framework) for s in listed] == [
      (GAAP_MAPPING, "rs-gaap"),
      ("A us-gaap mapping", "us-gaap"),
    ]
    assert {s.taxonomy_id for s in listed}.isdisjoint({chart_id})

  def test_one_mapping_per_framework(self, ext_session, library):
    with pytest.raises(MappingAlreadyExistsError):
      chart_block.create(
        ext_session,
        _chart_payload(_mapping(GAAP_MAPPING), _mapping("Second", "rs-gaap")),
        "u",
      )

  def test_a_framework_outside_the_library_is_refused(self, ext_session, library):
    # A ValueError, so the operation answers 422 rather than 500.
    with pytest.raises(FrameworkNotInLibraryError) as exc:
      chart_block.create(
        ext_session, _chart_payload(_mapping("Call report", "rs-call-report")), "u"
      )
    assert isinstance(exc.value, ValueError)

  def test_the_database_holds_one_active_mapping_per_framework(
    self, ext_session, library
  ):
    chart_id = chart_block.create(
      ext_session, _chart_payload(_mapping(GAAP_MAPPING)), "u"
    )
    ext_session.add(
      Taxonomy(
        name="Bypassing the app check",
        taxonomy_type="mapping",
        source_taxonomy_id=chart_id,
        target_taxonomy_id=library["rs-gaap"][0].id,
      )
    )
    with pytest.raises(IntegrityError):
      ext_session.flush()

  def test_a_mapping_belongs_only_to_a_chart(self, ext_session, library):
    with pytest.raises(MappingOutsideChartError):
      custom_ontology_block.create(
        ext_session,
        CreateTaxonomyBlockRequest(
          name="Ontology",
          taxonomy_type="custom_ontology",
          structures=[_mapping(GAAP_MAPPING)],
        ),
        "u",
      )


class TestTheChartBlockOwnsItsMappings:
  def test_the_envelope_reads_them_with_their_framework(self, ext_session, library):
    chart_id = chart_block.create(
      ext_session,
      _chart_payload(_mapping(GAAP_MAPPING), _mapping(US_GAAP_MAPPING, "us-gaap")),
      "u",
    )
    envelope = chart_block.build_envelope(ext_session, chart_id)
    assert envelope is not None
    assert {(s.name, s.target_framework) for s in envelope.structures} == {
      (GAAP_MAPPING, "rs-gaap"),
      (US_GAAP_MAPPING, "us-gaap"),
    }

  def test_update_adds_renames_and_removes_a_mapping(self, ext_session, library):
    chart_id = chart_block.create(
      ext_session, _chart_payload(_mapping(GAAP_MAPPING)), "u"
    )

    chart_block.update(
      ext_session,
      UpdateTaxonomyBlockRequest(
        taxonomy_id=chart_id, structures_to_add=[_mapping(US_GAAP_MAPPING, "us-gaap")]
      ),
      "u",
    )
    other = find_mapping_structure(ext_session, "us-gaap", chart_id=chart_id)
    assert other is not None
    other_id, other_anchor_id = str(other.id), str(other.taxonomy_id)

    chart_block.update(
      ext_session,
      UpdateTaxonomyBlockRequest(
        taxonomy_id=chart_id,
        structures_to_update=[
          StructureUpdatePatch(structure_id=other_id, name="Full us-gaap mapping")
        ],
      ),
      "u",
    )
    anchor = ext_session.get(Taxonomy, other_anchor_id)
    assert anchor is not None and anchor.name == "Full us-gaap mapping"

    chart_block.update(
      ext_session,
      UpdateTaxonomyBlockRequest(taxonomy_id=chart_id, structures_to_remove=[other_id]),
      "u",
    )
    ext_session.expire_all()
    assert ext_session.get(Taxonomy, other_anchor_id) is None
    assert find_mapping_structure(ext_session, "rs-gaap", chart_id=chart_id) is not None

  def test_removing_a_mapped_account_takes_its_arc(self, ext_session, library):
    chart_id = chart_block.create(
      ext_session, _chart_payload(_mapping(GAAP_MAPPING)), "u"
    )
    mapping = find_mapping_structure(ext_session, chart_id=chart_id)
    assert mapping is not None
    arc_id = str(_map_rent(ext_session, mapping, library["rs-gaap"][1]).id)

    chart_block.update(
      ext_session,
      UpdateTaxonomyBlockRequest(taxonomy_id=chart_id, elements_to_remove=["coa:6000"]),
      "u",
    )
    ext_session.expire_all()
    assert ext_session.get(Association, arc_id) is None

  def test_deleting_the_chart_deletes_its_mappings(self, ext_session, library):
    chart_id = chart_block.create(
      ext_session,
      _chart_payload(_mapping(GAAP_MAPPING), _mapping(US_GAAP_MAPPING, "us-gaap")),
      "u",
    )
    mapping = find_mapping_structure(ext_session, chart_id=chart_id)
    assert mapping is not None
    _map_rent(ext_session, mapping, library["rs-gaap"][1])

    chart_block.delete(
      ext_session, DeleteTaxonomyBlockRequest(taxonomy_id=chart_id, reason="test"), "u"
    )
    ext_session.expire_all()
    assert ext_session.query(Structure).filter_by(block_type="coa_mapping").all() == []
    assert (
      ext_session.query(Taxonomy)
      .filter(
        Taxonomy.taxonomy_type == "mapping", Taxonomy.source_taxonomy_id.isnot(None)
      )
      .all()
      == []
    )
    assert (
      ext_session.query(Association).filter_by(association_type="mapping").all() == []
    )


def _load_migration():
  path = (
    Path(__file__).resolve().parents[3]
    / "migrations"
    / "extensions"
    / "versions"
    / "0037_anchor_coa_mappings_to_mapping_taxonomies.py"
  )
  spec = importlib.util.spec_from_file_location("migration_0037", path)
  assert spec is not None and spec.loader is not None
  module = importlib.util.module_from_spec(spec)
  spec.loader.exec_module(module)
  return module


class TestMigration0037:
  def _legacy_mapping(self, session, name: str) -> tuple[str, str]:
    """A chart with a mapping anchored on the chart itself, as before 0037."""
    chart = Taxonomy(
      name="Tenant CoA", taxonomy_type="chart_of_accounts", created_by="u"
    )
    session.add(chart)
    session.flush()
    structure = Structure(
      name=name, block_type="coa_mapping", taxonomy_id=chart.id, created_by="u"
    )
    session.add(structure)
    session.flush()
    return str(chart.id), str(structure.id)

  def _second_legacy_mapping(
    self, session, chart_id: str, name: str, *, is_active: bool = True
  ) -> str:
    structure = Structure(
      name=name,
      block_type="coa_mapping",
      taxonomy_id=chart_id,
      is_active=is_active,
      created_by="u",
    )
    session.add(structure)
    session.flush()
    return str(structure.id)

  def _schema(self, session) -> str:
    return str(session.execute(text("SELECT current_schema()")).scalar_one())

  def test_re_anchors_to_the_framework_its_arcs_target(self, ext_session, library):
    chart_id, structure_id = self._legacy_mapping(ext_session, US_GAAP_MAPPING)
    ext_session.add(
      Element(
        name="Rent",
        qname="coa:6000",
        code="6000",
        taxonomy_id=chart_id,
        created_by="u",
      )
    )
    ext_session.flush()
    structure = ext_session.get(Structure, structure_id)
    _map_rent(ext_session, structure, library["us-gaap"][1])

    _load_migration()._upgrade(ext_session.connection(), self._schema(ext_session))
    ext_session.expire_all()

    assert find_mapping_structure(ext_session, "us-gaap", chart_id=chart_id) is not None
    assert find_mapping_structure(ext_session, "rs-gaap", chart_id=chart_id) is None

  def test_an_unmapped_structure_targets_rs_gaap(self, ext_session, library):
    chart_id, structure_id = self._legacy_mapping(ext_session, GAAP_MAPPING)

    migration = _load_migration()
    migration._upgrade(ext_session.connection(), self._schema(ext_session))
    ext_session.expire_all()

    mapping = find_mapping_structure(ext_session, "rs-gaap", chart_id=chart_id)
    assert mapping is not None and str(mapping.id) == structure_id

    migration._downgrade(ext_session.connection(), self._schema(ext_session))
    ext_session.expire_all()
    structure = ext_session.get(Structure, structure_id)
    assert structure is not None and str(structure.taxonomy_id) == chart_id

  def test_an_inactive_mapping_does_not_take_the_slot(self, ext_session, library):
    chart_id, retired_id = self._legacy_mapping(ext_session, "Retired mapping")
    ext_session.get(Structure, retired_id).is_active = False
    live_id = self._second_legacy_mapping(ext_session, chart_id, GAAP_MAPPING)
    ext_session.flush()

    _load_migration()._upgrade(ext_session.connection(), self._schema(ext_session))
    ext_session.expire_all()

    mapping = find_mapping_structure(ext_session, "rs-gaap", chart_id=chart_id)
    assert mapping is not None and str(mapping.id) == live_id

  def test_two_active_mappings_into_one_framework_keep_the_mapped_one(
    self, ext_session, library
  ):
    chart_id, empty_id = self._legacy_mapping(ext_session, "Empty mapping")
    ext_session.add(
      Element(
        name="Rent",
        qname="coa:6000",
        code="6000",
        taxonomy_id=chart_id,
        created_by="u",
      )
    )
    mapped_id = self._second_legacy_mapping(ext_session, chart_id, GAAP_MAPPING)
    _map_rent(ext_session, ext_session.get(Structure, mapped_id), library["rs-gaap"][1])

    _load_migration()._upgrade(ext_session.connection(), self._schema(ext_session))
    ext_session.expire_all()

    mapping = find_mapping_structure(ext_session, "rs-gaap", chart_id=chart_id)
    assert mapping is not None and str(mapping.id) == mapped_id
    empty = ext_session.get(Structure, empty_id)
    assert empty is not None
    empty_anchor = ext_session.get(Taxonomy, empty.taxonomy_id)
    assert empty_anchor is not None and empty_anchor.is_active is False


class TestMappingsNameTheirEntity:
  """Each entity keeps its own chart, so a mapping is one entity's: the list
  says whose, and keeps one entity's when asked."""

  def _group(self, ext_session) -> tuple[str, str]:
    from robosystems.models.extensions import Entity
    from tests.ledger_entity import seed_parent_entity

    parent = seed_parent_entity(ext_session)
    ext_session.add(
      Entity(
        id="ent_sub",
        name="Sub LLC",
        ticker="SUB",
        is_parent=False,
        parent_entity_id=parent,
        source="native",
        created_by="test",
      )
    )
    ext_session.flush()
    chart_block.create(ext_session, _chart_payload(_mapping(GAAP_MAPPING)), "u")
    sub_chart = _chart_payload(_mapping("Sub to US GAAP Mapping"))
    for element in sub_chart.elements:
      element.qname = element.qname.replace("coa:", "coa-sub:")
    chart_block.create(ext_session, sub_chart, "u", entity_id="ent_sub")
    return parent, "ent_sub"

  def test_each_mapping_names_the_entity_whose_chart_it_maps(
    self, ext_session, library
  ):
    parent, sub = self._group(ext_session)

    listed = {s.name: s.entity_id for s in list_mappings(ext_session).structures}

    assert listed == {GAAP_MAPPING: parent, "Sub to US GAAP Mapping": sub}

  def test_one_entitys_mappings(self, ext_session, library):
    _, sub = self._group(ext_session)

    listed = list_mappings(ext_session, sub).structures

    assert [(s.name, s.entity_id) for s in listed] == [("Sub to US GAAP Mapping", sub)]
