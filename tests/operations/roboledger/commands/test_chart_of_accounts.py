"""Unit tests for ``initialize_chart_of_accounts`` (mocked session).

The declarative CoA handler and the mapping command are each covered by
their own suites; here they are stubbed so the tests pin the orchestration:
the one-time rule, the template lookup, the entity-form default, the
envelope handed to the handler, which frameworks get a mapping structure
(the ones the tenant's library carries), and how library misses are
reported.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from robosystems.models.api.extensions.chart_of_accounts import (
  InitializeChartOfAccountsRequest,
)
from robosystems.operations.roboledger.commands.chart_of_accounts import (
  DEFAULT_CHART_NAME,
  MAPPING_STRUCTURE_NAME,
  ChartAlreadyExistsError,
  ChartTemplateNotFoundError,
  initialize_chart_of_accounts,
)
from robosystems.operations.roboledger.commands.taxonomies import (
  MappingAssociationExistsError,
)
from robosystems.operations.taxonomy_block.chart_templates import CHART_TEMPLATES

_MOD = "robosystems.operations.roboledger.commands.chart_of_accounts"


def _row_result(rows):
  result = MagicMock()
  result.all.return_value = rows
  return result


def _scalar_result(value):
  result = MagicMock()
  result.scalar.return_value = value
  result.scalar_one_or_none.return_value = value
  result.scalar_one.return_value = value
  return result


def _session_for(
  template_key: str,
  *,
  existing_chart=None,
  library_misses=(),
  has_rs_gaap: bool = True,
):
  """A session whose ``execute`` answers, in call order: the active-chart
  probe, the "does this tenant carry rs-gaap" probe, the new chart's
  elements, then — when rs-gaap is present — the mapping-structure lookup
  and the library elements for the template's targets (minus
  ``library_misses``)."""
  template = CHART_TEMPLATES[template_key]
  coa_rows = [(code, f"elem_{code}") for code, *_ in template.accounts]
  targets = sorted(
    {q for _c, q in template.mappings["rs-gaap"].arcs_for("corporation")}
  )
  library_rows = [
    (qname, f"lib_{i}")
    for i, qname in enumerate(targets)
    if qname not in library_misses
  ]
  session = MagicMock()
  answers = [
    _scalar_result(existing_chart),
    _scalar_result(has_rs_gaap),
    _row_result(coa_rows),
  ]
  if has_rs_gaap:
    answers += [_scalar_result("struct_map"), _row_result(library_rows)]
  session.execute.side_effect = answers
  return session


@pytest.mark.unit
class TestInitializeChartOfAccounts:
  @pytest.fixture(autouse=True)
  def _stubs(self):
    with (
      patch(f"{_MOD}.create_chart_block", return_value="tax_new") as create,
      patch(f"{_MOD}.create_mapping_association") as map_assoc,
      patch(f"{_MOD}.resolve_parent_entity", return_value=None) as entity,
    ):
      self.create = create
      self.map_assoc = map_assoc
      self.entity = entity
      yield

  def test_refuses_when_a_chart_exists(self) -> None:
    session = _session_for("saas", existing_chart="tax_existing")
    with pytest.raises(ChartAlreadyExistsError) as exc:
      initialize_chart_of_accounts(
        session, InitializeChartOfAccountsRequest(template="saas"), "usr_1"
      )
    assert exc.value.taxonomy_id == "tax_existing"
    self.create.assert_not_called()

  def test_unknown_template_is_refused_before_any_query(self) -> None:
    session = MagicMock()
    body = InitializeChartOfAccountsRequest.model_construct(template="retail")
    with pytest.raises(ChartTemplateNotFoundError):
      initialize_chart_of_accounts(session, body, "usr_1")
    session.execute.assert_not_called()

  def test_creates_chart_then_maps_every_row(self) -> None:
    session = _session_for("saas")
    response = initialize_chart_of_accounts(
      session, InitializeChartOfAccountsRequest(template="saas"), "usr_1"
    )

    template = CHART_TEMPLATES["saas"]
    payload = self.create.call_args.args[1]
    assert payload.taxonomy_type == "chart_of_accounts"
    assert payload.name == DEFAULT_CHART_NAME
    assert [e.qname for e in payload.elements] == [
      f"coa:{code}" for code, *_ in template.accounts
    ]
    assert [e.code for e in payload.elements] == [c for c, *_ in template.accounts]
    assert all(e.trait for e in payload.elements)
    assert [s.block_type for s in payload.structures] == ["coa_mapping"]
    assert payload.structures[0].name == MAPPING_STRUCTURE_NAME
    assert payload.metadata == {
      "template": "saas",
      "template_version": "v1",
      "entity_type": "corporation",
      "frameworks": ["rs-gaap"],
    }

    expected = template.mappings["rs-gaap"].arcs_for("corporation")
    assert self.map_assoc.call_count == len(expected)
    first = self.map_assoc.call_args_list[0].args[1]
    assert first.mapping_id == "struct_map"
    assert first.from_element_id == f"elem_{expected[0][0]}"
    assert first.association_type == "mapping"
    assert first.suggested_by == "template"

    assert response.taxonomy_id == "tax_new"
    assert response.template == "saas"
    assert response.entity_type == "corporation"
    assert response.elements_created == template.account_count
    assert response.mappings_created == len(expected)
    assert response.frameworks == ["rs-gaap"]
    assert response.unresolved == []

  def test_a_framework_the_tenant_lacks_gets_no_structure(self) -> None:
    """The op follows the pin through the library copy: no rs-gaap
    concepts in this graph → no rs-gaap mapping structure, no arcs, and
    the skip is reported rather than every target listed as unresolved."""
    session = _session_for("saas", has_rs_gaap=False)

    response = initialize_chart_of_accounts(
      session, InitializeChartOfAccountsRequest(template="saas"), "usr_1"
    )

    payload = self.create.call_args.args[1]
    assert payload.structures == []
    assert payload.metadata["frameworks"] == []
    self.map_assoc.assert_not_called()
    assert response.frameworks == []
    assert response.mappings_created == 0
    assert response.unresolved == ["rs-gaap: not in this graph's library"]
    assert response.elements_created == CHART_TEMPLATES["saas"].account_count

  def test_entity_type_defaults_to_the_graphs_entity(self) -> None:
    entity = MagicMock()
    entity.entity_type = "LLC"
    self.entity.return_value = entity
    session = _session_for("services")

    response = initialize_chart_of_accounts(
      session, InitializeChartOfAccountsRequest(template="services"), "usr_1"
    )

    assert response.entity_type == "llc"
    targets = {call.args[1].to_element_id for call in self.map_assoc.call_args_list}
    assert targets  # mapped through the llc equity rows without error

  def test_explicit_entity_type_wins_over_the_entity(self) -> None:
    entity = MagicMock()
    entity.entity_type = "corporation"
    self.entity.return_value = entity
    session = _session_for("product")

    response = initialize_chart_of_accounts(
      session,
      InitializeChartOfAccountsRequest(template="product", entity_type="Partnership"),
      "usr_1",
    )
    assert response.entity_type == "partnership"
    self.entity.assert_not_called()

  def test_unknown_entity_type_reports_the_form_actually_used(self) -> None:
    """An unrecognised legal form maps the corporation equity rows; the
    response and the chart's metadata say so instead of echoing the input."""
    session = _session_for("saas")

    response = initialize_chart_of_accounts(
      session,
      InitializeChartOfAccountsRequest(template="saas", entity_type="sole-prop"),
      "usr_1",
    )

    assert response.entity_type == "corporation"
    payload = self.create.call_args.args[1]
    assert payload.metadata["entity_type"] == "corporation"

  def test_library_misses_are_reported_not_fatal(self) -> None:
    missing = "rs-gaap:DeferredRevenueCurrent"
    session = _session_for("saas", library_misses={missing})

    response = initialize_chart_of_accounts(
      session, InitializeChartOfAccountsRequest(template="saas"), "usr_1"
    )

    expected = CHART_TEMPLATES["saas"].mappings["rs-gaap"].arcs_for("corporation")
    misses = [c for c, q in expected if q == missing]
    assert response.unresolved == [missing]
    assert response.mappings_created == len(expected) - len(misses)

  def test_duplicate_mapping_is_skipped_quietly(self) -> None:
    self.map_assoc.side_effect = [MappingAssociationExistsError("m", "a", "b")] + [
      None
    ] * 100
    session = _session_for("saas")

    response = initialize_chart_of_accounts(
      session, InitializeChartOfAccountsRequest(template="saas"), "usr_1"
    )

    expected = CHART_TEMPLATES["saas"].mappings["rs-gaap"].arcs_for("corporation")
    assert response.mappings_created == len(expected) - 1
    assert response.unresolved == []

  def test_custom_name_is_kept(self) -> None:
    session = _session_for("saas")
    response = initialize_chart_of_accounts(
      session,
      InitializeChartOfAccountsRequest(template="saas", name="  Cadence Books "),
      "usr_1",
    )
    assert response.name == "Cadence Books"
    assert self.create.call_args.args[1].name == "Cadence Books"
