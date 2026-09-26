"""Library element reads: listing, search, presentation tree, equivalents, arcs.

Runs against the real test Postgres with a throwaway tenant schema.
"""

from __future__ import annotations

import os
import uuid

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

import robosystems.models.extensions  # noqa: F401  (register models on ExtensionsBase)
from robosystems.db.extensions import ExtensionsBase
from robosystems.models.extensions import (
  Association,
  Element,
  ElementLabel,
  ElementReference,
  ElementTrait,
  Structure,
  Taxonomy,
  Trait,
)
from robosystems.operations.library.reads.elements import (
  get_element_arcs,
  get_element_equivalents,
  get_element_tree,
  list_elements,
  search_elements,
)

pytestmark = pytest.mark.unit


@pytest.fixture()
def ext_session():
  database_url = os.environ.get("TEST_DATABASE_URL")
  if not database_url:
    pytest.skip("TEST_DATABASE_URL not configured")

  schema = f"ext_lib_{uuid.uuid4().hex[:12]}"
  engine = create_engine(database_url)
  with engine.begin() as conn:
    conn.execute(text(f'CREATE SCHEMA "{schema}"'))

  session = sessionmaker(bind=engine)()
  session.execute(text(f'SET search_path TO "{schema}"'))
  ExtensionsBase.metadata.create_all(
    bind=session.connection(),
    tables=[t for t in ExtensionsBase.metadata.sorted_tables if t.schema is None],
  )
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


def _el(id_, qname, **kw):
  return Element(id=id_, name=kw.pop("name", qname.split(":")[-1]), qname=qname, **kw)


@pytest.fixture()
def library(ext_session):
  """A small presentation tree, an equivalence pair, and a CoA mapping arc.

  Assets
  ├── AssetsCurrent
  │   └── Cash
  └── AssetsNoncurrent   (only in the classified layout)
  """
  s = ext_session
  s.add_all(
    [
      Taxonomy(
        id="tax_rs", name="rs-gaap", taxonomy_type="reporting", standard="rs-gaap"
      ),
      Taxonomy(
        id="tax_bridge",
        name="us-gaap bridge",
        taxonomy_type="mapping",
        standard="us-gaap-bridge",
      ),
      Taxonomy(id="tax_coa", name="Chart", taxonomy_type="chart_of_accounts"),
      Trait(
        id="trt_asset", category="elementsOfFinancialStatements", identifier="Asset"
      ),
      Trait(id="trt_op", category="activityType", identifier="operating"),
    ]
  )
  s.flush()
  s.add_all(
    [
      _el("el_assets", "rs-gaap:Assets", source="rs-gaap", taxonomy_id="tax_rs"),
      _el(
        "el_current", "rs-gaap:AssetsCurrent", source="rs-gaap", taxonomy_id="tax_rs"
      ),
      _el(
        "el_noncurrent",
        "rs-gaap:AssetsNoncurrent",
        source="rs-gaap",
        taxonomy_id="tax_rs",
      ),
      _el(
        "el_cash",
        "rs-gaap:Cash",
        name="Cash",
        source="rs-gaap",
        taxonomy_id="tax_rs",
      ),
      _el("el_usgaap_cash", "us-gaap:Cash", source="us-gaap"),
      _el(
        "el_abstract",
        "rs-gaap:BalanceSheetAbstract",
        source="rs-gaap",
        is_abstract=True,
        element_type="abstract",
      ),
      _el("el_old", "rs-gaap:Retired", source="rs-gaap", is_active=False),
      Element(id="el_checking", name="Checking", code="1010", taxonomy_id="tax_coa"),
      Structure(
        id="bs_classified",
        name="Classified BS",
        block_type="balance_sheet",
        taxonomy_id="tax_rs",
      ),
      Structure(
        id="bs_simple",
        name="Unclassified BS",
        block_type="balance_sheet",
        taxonomy_id="tax_rs",
      ),
      Structure(
        id="bridge",
        name="Bridge",
        block_type="taxonomy_mapping",
        taxonomy_id="tax_bridge",
      ),
      Structure(
        id="map_1", name="CoA mapping", block_type="coa_mapping", taxonomy_id="tax_coa"
      ),
    ]
  )
  s.flush()

  def pres(structure, parent, child, order):
    return Association(
      structure_id=structure,
      from_element_id=parent,
      to_element_id=child,
      association_type="presentation",
      order_value=order,
    )

  s.add_all(
    [
      pres("bs_classified", "el_assets", "el_noncurrent", 2),
      pres("bs_classified", "el_assets", "el_current", 1),
      pres("bs_classified", "el_current", "el_cash", 1),
      pres("bs_simple", "el_assets", "el_cash", 1),
      Association(
        structure_id="bridge",
        from_element_id="el_usgaap_cash",
        to_element_id="el_cash",
        association_type="equivalence",
      ),
      Association(
        structure_id="map_1",
        from_element_id="el_checking",
        to_element_id="el_cash",
        association_type="mapping",
      ),
      ElementTrait(element_id="el_cash", trait_id="trt_asset"),
      ElementTrait(element_id="el_current", trait_id="trt_asset"),
      ElementTrait(element_id="el_cash", trait_id="trt_op"),
      ElementLabel(id="lbl_1", element_id="el_cash", text="Cash and cash equivalents"),
      ElementLabel(id="lbl_2", element_id="el_cash", role="terse", text="Cash"),
      ElementReference(
        id="ref_1", element_id="el_cash", ref_type="ASC", citation="FASB ASC 305-10"
      ),
    ]
  )
  s.flush()
  return s


class TestListElements:
  def test_hides_inactive_unless_asked(self, library):
    active = {e.id for e in list_elements(library, limit=500)}
    everything = {
      e.id for e in list_elements(library, limit=500, include_inactive=True)
    }
    assert "el_old" not in active
    assert everything - active == {"el_old"}

  def test_trait_and_activity_filters_combine(self, library):
    assets = list_elements(library, trait="Asset")
    assert [e.qname for e in assets] == ["rs-gaap:AssetsCurrent", "rs-gaap:Cash"]
    assert all(e.trait == "Asset" for e in assets)

    both = list_elements(library, trait="Asset", activity_type="operating")
    assert [e.id for e in both] == ["el_cash"]

  def test_filters_by_source_type_and_abstract(self, library):
    assert [e.id for e in list_elements(library, source="us-gaap")] == [
      "el_usgaap_cash"
    ]
    assert [e.id for e in list_elements(library, element_type="abstract")] == [
      "el_abstract"
    ]
    assert "el_abstract" not in {
      e.id for e in list_elements(library, is_abstract=False)
    }

  def test_labels_and_references_only_when_asked(self, library):
    plain = list_elements(
      library, taxonomy_id="tax_rs", source="rs-gaap", trait="Asset"
    )
    assert all(e.labels == [] and e.references == [] for e in plain)

    rich = list_elements(
      library, trait="Asset", include_labels=True, include_references=True
    )
    cash = next(e for e in rich if e.id == "el_cash")
    assert [(lbl.role, lbl.text) for lbl in cash.labels] == [
      ("standard", "Cash and cash equivalents"),
      ("terse", "Cash"),
    ]
    assert [r.citation for r in cash.references] == ["FASB ASC 305-10"]

  def test_limit_is_clamped_and_empty_page_is_empty(self, library):
    assert len(list_elements(library, limit=0)) == 1
    assert list_elements(library, offset=1000) == []

  def test_code_only_element_falls_back_to_name_as_qname(self, library):
    [checking] = list_elements(library, taxonomy_id="tax_coa")
    assert checking.qname == "Checking"


class TestSearchElements:
  def test_matches_qname_name_or_label_text(self, library):
    by_label = search_elements(library, "EQUIVALENTS")
    assert [e.id for e in by_label] == ["el_cash"]
    assert by_label[0].labels  # single-element responses carry labels

    by_qname = search_elements(library, "assets")
    assert [e.qname for e in by_qname] == [
      "rs-gaap:Assets",
      "rs-gaap:AssetsCurrent",
      "rs-gaap:AssetsNoncurrent",
    ]

  def test_source_filter_and_no_duplicate_rows_per_label(self, library):
    hits = search_elements(library, "cash", source="rs-gaap")
    # el_cash has two labels matching "cash"; it still appears once.
    assert [e.id for e in hits] == ["el_cash"]


class TestElementTree:
  def test_orders_children_by_arc_order(self, library):
    tree = get_element_tree(library, "el_assets", structure_id="bs_classified")
    assert tree is not None
    assert [c.element.id for c in tree.children] == ["el_current", "el_noncurrent"]
    assert [c.element.id for c in tree.children[0].children] == ["el_cash"]
    assert tree.children[0].children[0].element.trait == "Asset"

  def test_structure_scopes_the_layout(self, library):
    simple = get_element_tree(library, "el_assets", structure_id="bs_simple")
    assert [c.element.id for c in simple.children] == ["el_cash"]

    # Without a structure the children blend across both layouts.
    blended = get_element_tree(library, "el_assets")
    assert {c.element.id for c in blended.children} == {
      "el_current",
      "el_noncurrent",
      "el_cash",
    }

  def test_max_depth_stops_the_walk(self, library):
    tree = get_element_tree(
      library, "el_assets", max_depth=1, structure_id="bs_classified"
    )
    assert [c.element.id for c in tree.children] == ["el_current", "el_noncurrent"]
    assert tree.children[0].children == []

  def test_unknown_root_is_none(self, library):
    assert get_element_tree(library, "el_missing") is None


class TestEquivalents:
  @pytest.mark.parametrize(
    ("element_id", "peer"),
    [("el_cash", "el_usgaap_cash"), ("el_usgaap_cash", "el_cash")],
  )
  def test_peers_are_found_in_both_directions(self, library, element_id, peer):
    result = get_element_equivalents(library, element_id)
    assert result.element.id == element_id
    assert [e.id for e in result.equivalents] == [peer]

  def test_an_element_without_arcs_has_no_equivalents(self, library):
    assert get_element_equivalents(library, "el_assets").equivalents == []

  def test_unknown_element_is_none(self, library):
    assert get_element_equivalents(library, "el_missing") is None


class TestElementArcs:
  def test_cross_taxonomy_arcs_in_both_directions(self, library):
    arcs = get_element_arcs(library, "el_cash")
    assert [(a.direction, a.association_type, a.peer.id) for a in arcs] == [
      ("incoming", "mapping", "el_checking"),
      ("incoming", "equivalence", "el_usgaap_cash"),
    ]
    assert arcs[1].taxonomy_standard == "us-gaap-bridge"
    assert arcs[0].structure_name == "CoA mapping"

  def test_presentation_arcs_are_excluded(self, library):
    assert get_element_arcs(library, "el_current") == []

  def test_outgoing_direction_is_from_this_side(self, library):
    [arc] = get_element_arcs(library, "el_usgaap_cash")
    assert (arc.direction, arc.peer.id) == ("outgoing", "el_cash")
