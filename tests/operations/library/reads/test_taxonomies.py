"""Tests for the structure-scoped filter on ``list_taxonomy_arcs``.

The filter exists so the library Structures view can render arcs for one
presentation/calculation hierarchy at a time without paging through every
arc the taxonomy contributes.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

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


def _arc_row(
  from_id: str, to_id: str, *, weight: float, from_abstract: bool
) -> MagicMock:
  row = MagicMock()
  row.Association.id = f"arc_{from_id}_{to_id}"
  row.Association.structure_id = "struct_calc"
  row.Association.from_element_id = from_id
  row.Association.to_element_id = to_id
  row.Association.association_type = "calculation"
  row.Association.arcrole = None
  row.Association.order_value = 1.0
  row.Association.weight = weight
  row.structure_name = "rs-gaap calc"
  row.from_qname = "rs-gaap:Assets"
  row.from_name = "Assets"
  row.from_is_abstract = from_abstract
  row.to_qname = "rs-gaap:CashAndCashEquivalents"
  row.to_name = "Cash"
  row.to_is_abstract = False
  return row


def test_list_taxonomy_arcs_enriches_elements_with_trait_and_abstract() -> None:
  """The hierarchy view colours nodes by EFS + badges abstracts, so the arc
  read must carry the primary EFS trait + is_abstract for each endpoint —
  batch-loaded via ``efs_trait_by_element`` and stamped onto the response."""
  session = MagicMock()
  result = MagicMock()
  result.all.return_value = [
    _arc_row("e_assets", "e_cash", weight=1.0, from_abstract=True),
  ]
  session.execute.return_value = result

  with patch(
    "robosystems.operations.library.reads.taxonomies.efs_trait_by_element",
    return_value={"e_assets": "asset", "e_cash": "asset"},
  ) as efs:
    arcs = list_taxonomy_arcs(session, taxonomy_id="tax_rs_gaap_calc_v1")

  # Both endpoints are batch-resolved in a single trait lookup.
  efs.assert_called_once()
  assert sorted(efs.call_args.args[1]) == ["e_assets", "e_cash"]

  assert len(arcs) == 1
  arc = arcs[0]
  assert arc.from_element_trait == "asset"
  assert arc.to_element_trait == "asset"
  assert arc.from_element_is_abstract is True
  assert arc.to_element_is_abstract is False
  assert arc.weight == 1.0
