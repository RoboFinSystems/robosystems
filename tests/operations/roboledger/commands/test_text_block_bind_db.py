"""bind-text-block binds only an element its structure arranges, and never
stamps a library concept. Runs against the real extensions database with a
throwaway tenant schema carrying the library-immutability triggers.
"""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

import robosystems.models.extensions  # noqa: F401  (register models on the Base)
from robosystems.config import env
from robosystems.db.extensions import (
  ExtensionsBase,
  _install_library_immutability_triggers,
  extensions_session,
)
from robosystems.models.api.extensions.text_blocks import BindTextBlockRequest
from robosystems.models.extensions import Element
from robosystems.operations.roboledger.commands import text_blocks as tb

pytestmark = pytest.mark.integration

GRAPH = "kgdddddddddddddddd3a"
STRUCTURE = "struct_policies"

_DOC = SimpleNamespace(
  id="doc_1",
  title="Policies",
  content="# Revenue\n\nRevenue is recognized when the order ships.\n",
)


def _tenant_tables():
  return [t for t in ExtensionsBase.metadata.sorted_tables if t.schema is None]


@pytest.fixture(scope="module", autouse=True)
def tenant():
  """A tenant text-block structure arranging a tenant concept and a library
  concept, plus a tenant concept on no structure."""
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
  try:
    with engine.begin() as conn:
      conn.execute(text(f"DROP SCHEMA IF EXISTS {GRAPH} CASCADE"))
      conn.execute(text(f"CREATE SCHEMA {GRAPH}"))
      ExtensionsBase.metadata.create_all(
        bind=conn.execution_options(schema_translate_map={None: GRAPH}),
        tables=_tenant_tables(),
      )
    _seed()
    with engine.begin() as conn:
      _install_library_immutability_triggers(conn, GRAPH)
    yield
  finally:
    with engine.begin() as conn:
      conn.execute(text(f"DROP SCHEMA IF EXISTS {GRAPH} CASCADE"))
    engine.dispose()


def _seed() -> None:
  from robosystems.models.extensions import Association, Structure, Taxonomy
  from robosystems.models.extensions.entity import Entity

  with extensions_session(GRAPH) as session:
    session.add_all(
      [
        Entity(
          id="ent_1", name="Acme", is_parent=True, source="native", created_by="u"
        ),
        Taxonomy(
          id="tax_notes", name="Notes", taxonomy_type="reporting", created_by="u"
        ),
      ]
    )
    session.flush()
    session.add_all(
      [
        Structure(
          id=STRUCTURE,
          name="Accounting policies",
          block_type="regulatory_disclosure",
          concept_arrangement="text_block",
          taxonomy_id="tax_notes",
          created_by="u",
        ),
        Element(
          id="el_root",
          name="Policies",
          qname="acme:PoliciesAbstract",
          is_abstract=True,
          taxonomy_id="tax_notes",
          created_by="u",
        ),
        Element(
          id="el_revenue_policy",
          name="Revenue policy",
          qname="acme:RevenuePolicyTextBlock",
          taxonomy_id="tax_notes",
          created_by="u",
        ),
        Element(
          id="el_lease_policy",
          name="Lease policy",
          qname="acme:LeasePolicyTextBlock",
          taxonomy_id="tax_notes",
          created_by="u",
        ),
        Element(
          id="el_cash",
          name="Cash",
          qname="rs-gaap:Cash",
          created_by="library-seeder",
        ),
      ]
    )
    session.flush()
    session.add_all(
      [
        Association(
          structure_id=STRUCTURE,
          from_element_id="el_root",
          to_element_id=to_id,
          association_type="presentation",
          order_value=order,
          created_by="u",
        )
        for order, to_id in enumerate(("el_revenue_policy", "el_cash"))
      ]
    )


def _bind(**element_ref):
  body = BindTextBlockRequest(
    document_id=_DOC.id,
    structure_id=STRUCTURE,
    period_start=date(2026, 1, 1),
    period_end=date(2026, 12, 31),
    entity_id="ent_1",
    **element_ref,
  )
  with patch.object(tb.Document, "get_by_id_and_graph", return_value=_DOC):
    with extensions_session(GRAPH) as session:
      return tb.bind_text_block(session, MagicMock(), GRAPH, body, "u")


def test_binds_an_element_on_the_structure_and_stamps_it():
  resp = _bind(element_qname="acme:RevenuePolicyTextBlock")
  assert resp.element_id == "el_revenue_policy"
  with extensions_session(GRAPH) as session:
    assert session.get(Element, "el_revenue_policy").item_type == "text_block"


def test_refuses_an_element_the_structure_does_not_arrange():
  with pytest.raises(tb.TextBlockElementError, match="not on structure"):
    _bind(element_id="el_lease_policy")
  with extensions_session(GRAPH) as session:
    assert session.get(Element, "el_lease_policy").item_type is None


def test_refuses_a_library_concept_before_the_immutability_trigger():
  with pytest.raises(tb.TextBlockElementError, match="library concept"):
    _bind(element_qname="rs-gaap:Cash")
