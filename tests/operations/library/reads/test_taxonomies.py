"""Tests for the structure-scoped filter on ``list_taxonomy_arcs``.

The filter exists so the library Structures view can render arcs for one
presentation/calculation hierarchy at a time without paging through every
arc the taxonomy contributes.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from robosystems.operations.library.reads.taxonomies import (
  count_taxonomy_arcs,
  list_taxonomy_arcs,
)


def _capture_query(session: MagicMock) -> str:
  """Render the compiled SQL of the most recent ``session.execute`` call.

  Used to assert WHERE clauses without standing up a real database.
  """
  call_args = session.execute.call_args
  assert call_args is not None, "session.execute was never called"
  query = call_args.args[0]
  return str(query.compile(compile_kwargs={"literal_binds": True}))


def _empty_result() -> MagicMock:
  result = MagicMock()
  result.all.return_value = []
  result.scalar.return_value = 0
  return result


def test_list_taxonomy_arcs_filters_by_structure_id_when_set() -> None:
  session = MagicMock()
  session.execute.return_value = _empty_result()

  list_taxonomy_arcs(
    session,
    taxonomy_id="tax_fac_v1",
    structure_id="struct_bs_classified",
  )

  sql = _capture_query(session).lower().replace('"', "")
  assert "associations.structure_id = 'struct_bs_classified'" in sql


def test_list_taxonomy_arcs_omits_structure_filter_when_none() -> None:
  session = MagicMock()
  session.execute.return_value = _empty_result()

  list_taxonomy_arcs(session, taxonomy_id="tax_fac_v1")

  sql = _capture_query(session).lower().replace('"', "")
  # Taxonomy-id WHERE clause stays, no literal structure id appears
  assert "structures.taxonomy_id = 'tax_fac_v1'" in sql
  assert "associations.structure_id = 'struct_" not in sql


def test_list_taxonomy_arcs_combines_structure_and_association_type() -> None:
  session = MagicMock()
  session.execute.return_value = _empty_result()

  list_taxonomy_arcs(
    session,
    taxonomy_id="tax_fac_v1",
    association_type="presentation",
    structure_id="struct_is_multistep",
  )

  sql = _capture_query(session).replace('"', "").lower()
  assert "associations.association_type" in sql
  assert "presentation" in sql
  assert "associations.structure_id" in sql
  assert "struct_is_multistep" in sql


def test_count_taxonomy_arcs_filters_by_structure_id_when_set() -> None:
  session = MagicMock()
  session.execute.return_value = _empty_result()

  count_taxonomy_arcs(
    session,
    taxonomy_id="tax_fac_v1",
    structure_id="struct_bs_classified",
  )

  sql = _capture_query(session).replace('"', "").lower()
  assert "associations.structure_id = 'struct_bs_classified'" in sql


def test_count_taxonomy_arcs_omits_structure_filter_when_none() -> None:
  session = MagicMock()
  session.execute.return_value = _empty_result()

  count_taxonomy_arcs(session, taxonomy_id="tax_fac_v1")

  sql = _capture_query(session).replace('"', "").lower()
  assert "structures.taxonomy_id = 'tax_fac_v1'" in sql
  assert "associations.structure_id = 'struct_" not in sql
