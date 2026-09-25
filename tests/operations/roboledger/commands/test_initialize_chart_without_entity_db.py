"""Every shipped chart template initializes on a graph with no entity yet.

With no entity and no Reporting Style resolved, the mapping leaf check falls
back to the static subtotal denylist, so the denylist must not hold a concept
a template maps to. Runs against the real extensions database with a
throwaway tenant schema.
"""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

import robosystems.models.extensions  # noqa: F401  (register models on the Base)
from robosystems.config import env
from robosystems.db.extensions import ExtensionsBase, extensions_session
from robosystems.models.api.extensions.chart_of_accounts import (
  InitializeChartOfAccountsRequest,
)
from robosystems.operations.roboledger.commands.chart_of_accounts import (
  initialize_chart_of_accounts,
)
from robosystems.operations.taxonomy_block.chart_templates import CHART_TEMPLATES

pytestmark = pytest.mark.integration

GRAPH = "kgdddddddddddddddd24"


def _tenant_tables():
  return [t for t in ExtensionsBase.metadata.sorted_tables if t.schema is None]


@pytest.fixture()
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
  try:
    with engine.begin() as conn:
      conn.execute(text(f"DROP SCHEMA IF EXISTS {GRAPH} CASCADE"))
      conn.execute(text(f"CREATE SCHEMA {GRAPH}"))
      ExtensionsBase.metadata.create_all(
        bind=conn.execution_options(schema_translate_map={None: GRAPH}),
        tables=_tenant_tables(),
      )
    yield
  finally:
    with engine.begin() as conn:
      conn.execute(text(f"DROP SCHEMA IF EXISTS {GRAPH} CASCADE"))
    engine.dispose()


@pytest.mark.parametrize("template", sorted(CHART_TEMPLATES))
def test_a_template_initializes_before_any_entity_exists(tenant, template):
  from robosystems.models.extensions import Element

  targets = sorted(
    {
      qname
      for _code, qname in CHART_TEMPLATES[template]
      .mappings["rs-gaap"]
      .arcs_for("corporation")
    }
  )
  with extensions_session(GRAPH) as session:
    for i, qname in enumerate(targets):
      session.add(
        Element(
          id=f"lib_{i}",
          name=qname,
          qname=qname,
          source="rs-gaap",
          created_by="library-seeder",
        )
      )
    session.commit()
    result = initialize_chart_of_accounts(
      session, InitializeChartOfAccountsRequest(template=template), "u"
    )
  assert result.mappings_created > 0
