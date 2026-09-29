"""A re-pointed library arc is retired in the library and in every tenant.

An association id is ``uuid5(structure:from:to:type)``, so re-pointing an arc
mints a new row. Before the retire step the old row lingered beside it and the
parent summed both (repaired by hand in migration 0030). Runs against the test
Postgres with two throwaway schemas standing in for ``public`` and a tenant.
"""

from __future__ import annotations

import os
import uuid

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

import robosystems.models.extensions  # noqa: F401  (register models on ExtensionsBase)
from robosystems.db.extensions import ExtensionsBase
from robosystems.operations.taxonomy_block.library_creator import (
  create_library_arcs,
  create_library_taxonomy_elements,
  find_unseeded_library_arcs,
)
from robosystems.taxonomy.model import (
  AssociationSpec,
  ElementSpec,
  LabelSpec,
  TaxonomyPackage,
)
from robosystems.taxonomy.writer import (
  SET_LIBRARY_RESYNC,
  find_unsourced_library_arcs,
  resync_library_into_tenant,
)

pytestmark = pytest.mark.unit

PIN = {"fac": "v1"}
CALC = "http://www.xbrl.org/2003/arcrole/summation-item"


def _element(name: str) -> ElementSpec:
  return ElementSpec(
    qname=f"fac:{name}",
    namespace="fac",
    namespace_uri="http://fac.example.com/",
    name=name,
    balance_type="debit",
    period_type="instant",
    source="fac",
    labels=[LabelSpec(role="standard", language="en", text=name)],
  )


def _package(*arcs: tuple[str, str]) -> TaxonomyPackage:
  return TaxonomyPackage(
    name="Retirement Test",
    standard="fac",
    version="v1",
    namespace_uri="http://fac.example.com/",
    taxonomy_type="reporting_standard",
    elements=[_element(n) for n in ("Parent", "OldChild", "NewChild")],
    associations=[
      AssociationSpec(
        from_qname=f"fac:{p}",
        to_qname=f"fac:{c}",
        association_type="calculation",
        arcrole=CALC,
        weight=1.0,
      )
      for p, c in arcs
    ],
  )


@pytest.fixture()
def schemas():
  database_url = os.environ.get("TEST_DATABASE_URL")
  if not database_url:
    pytest.skip("TEST_DATABASE_URL not configured")

  suffix = uuid.uuid4().hex[:10]
  source, tenant = f"lib_src_{suffix}", f"lib_tnt_{suffix}"
  engine = create_engine(database_url)
  tables = [t for t in ExtensionsBase.metadata.sorted_tables if t.schema is None]
  with engine.begin() as conn:
    for schema in (source, tenant):
      conn.execute(text(f'CREATE SCHEMA "{schema}"'))
      conn.execute(text(f'SET search_path TO "{schema}"'))
      ExtensionsBase.metadata.create_all(bind=conn, tables=tables)

  session = sessionmaker(bind=engine)()
  try:
    yield session, source, tenant
  finally:
    session.rollback()
    session.close()
    with engine.begin() as conn:
      for schema in (source, tenant):
        conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
    engine.dispose()


def _seed(session, source: str, package: TaxonomyPackage, **kwargs) -> dict[str, int]:
  session.execute(text(f'SET search_path TO "{source}"'))
  create_library_taxonomy_elements(session, package)
  counts = create_library_arcs(session, package, **kwargs)
  session.flush()
  return counts


def _resync(session, source: str, tenant: str):
  session.execute(text(SET_LIBRARY_RESYNC))
  return resync_library_into_tenant(
    session.connection(), tenant, pin=PIN, source_schema=source
  )


def _children_of_parent(session, schema: str) -> list[str]:
  return list(
    session.execute(
      text(f"""
        SELECT c.qname FROM "{schema}".associations a
        JOIN "{schema}".elements p ON p.id = a.from_element_id
        JOIN "{schema}".elements c ON c.id = a.to_element_id
        WHERE p.qname = 'fac:Parent' AND a.association_type = 'calculation'
        ORDER BY c.qname
      """)
    ).scalars()
  )


def test_repointed_arc_is_retired_in_library_and_tenant(schemas):
  session, source, tenant = schemas
  _seed(session, source, _package(("Parent", "OldChild")))
  _resync(session, source, tenant)
  assert _children_of_parent(session, tenant) == ["fac:OldChild"]

  counts = _seed(session, source, _package(("Parent", "NewChild")))
  assert counts["associations_retired"] == 1
  assert _children_of_parent(session, source) == ["fac:NewChild"]

  stats = _resync(session, source, tenant)
  assert stats.associations_retired == 1
  assert _children_of_parent(session, tenant) == ["fac:NewChild"]


def test_detectors_find_the_double_count_shape_before_retirement(schemas):
  session, source, tenant = schemas
  _seed(session, source, _package(("Parent", "OldChild")))
  _resync(session, source, tenant)

  repointed = _package(("Parent", "NewChild"))
  _seed(session, source, repointed, retire_stale=False)
  assert _children_of_parent(session, source) == ["fac:NewChild", "fac:OldChild"]
  session.execute(text(f'SET search_path TO "{source}"'))
  assert len(find_unseeded_library_arcs(session, repointed)) == 1

  _seed(session, source, repointed)
  assert find_unseeded_library_arcs(session, repointed) == []
  assert (
    len(
      find_unsourced_library_arcs(
        session.connection(), tenant, PIN, source_schema=source
      )
    )
    == 1
  )

  _resync(session, source, tenant)
  assert (
    find_unsourced_library_arcs(session.connection(), tenant, PIN, source_schema=source)
    == []
  )


def test_tenant_authored_arcs_are_never_retired(schemas):
  session, source, tenant = schemas
  _seed(session, source, _package(("Parent", "OldChild")))
  _resync(session, source, tenant)

  library_arc = session.execute(
    text(f'SELECT structure_id, from_element_id FROM "{tenant}".associations')
  ).one()
  new_child_id = session.execute(
    text(f"SELECT id FROM \"{tenant}\".elements WHERE qname = 'fac:NewChild'")
  ).scalar_one()
  session.execute(
    text(f"""
      INSERT INTO "{tenant}".associations
        (id, structure_id, from_element_id, to_element_id, association_type,
         confidence, metadata, created_by, created_at, updated_at)
      VALUES (:id, :sid, :fid, :tid, 'calculation', 1.0, '{{}}', 'tenant-user',
              now(), now())
    """),
    {
      "id": str(uuid.uuid4()),
      "sid": library_arc.structure_id,
      "fid": library_arc.from_element_id,
      "tid": new_child_id,
    },
  )

  _seed(session, source, _package())
  _resync(session, source, tenant)
  assert _children_of_parent(session, source) == []
  assert _children_of_parent(session, tenant) == ["fac:NewChild"]


def test_retirement_is_skipped_when_an_arc_does_not_resolve(schemas):
  session, source, _tenant = schemas
  _seed(session, source, _package(("Parent", "OldChild")))

  unresolvable = _package(("Parent", "NewChild"))
  unresolvable.associations.append(
    AssociationSpec(
      from_qname="fac:Parent",
      to_qname="fac:NotInTheLibrary",
      association_type="calculation",
      arcrole=CALC,
    )
  )
  counts = _seed(session, source, unresolvable)
  assert counts["associations_retired"] == 0
  assert _children_of_parent(session, source) == ["fac:NewChild", "fac:OldChild"]
