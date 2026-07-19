"""Tests for the text-block disclosure envelope builder + CAP dispatch."""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from robosystems.models.api.information_block import FactSetLite
from robosystems.operations.information_block import disclosure as disclosure_handlers
from robosystems.operations.information_block import text_block as text_block_mod
from robosystems.operations.information_block.envelope import BaseEnvelopeAtoms


def _structure(cap: str = "text_block") -> MagicMock:
  s = MagicMock()
  s.id = "struct_policy"
  s.block_type = "regulatory_disclosure"
  s.name = "Significant Accounting Policies"
  s.description = None
  s.taxonomy_id = "tax_ext"
  s.artifact_mechanics = None
  s.concept_arrangement = cap
  s.member_arrangement = None
  s.renderer_note = None
  s.metadata_ = {}
  return s


def _element() -> SimpleNamespace:
  return SimpleNamespace(
    id="elem_policy",
    qname="driftline:InventoryPolicyTextBlock",
    name="Inventory Policy",
    code=None,
    element_type="concept",
    is_abstract=False,
    is_monetary=False,
    balance_type="debit",
    period_type="duration",
  )


def _text_fact() -> SimpleNamespace:
  return SimpleNamespace(
    id="fact_txt",
    element_id="elem_policy",
    value=None,
    string_value="# Inventory Policy\n\nFIFO, lower of cost or NRV.",
    fact_type="Nonnumeric",
    content_type="text/markdown",
    period_start=date(2026, 1, 1),
    period_end=date(2026, 12, 31),
    period_type="duration",
    unit="USD",
    fact_scope="in_scope",
    fact_set_id="fs_standing",
  )


def _atoms(structure: MagicMock, *, with_fact_set: bool = True) -> BaseEnvelopeAtoms:
  fact_set = None
  if with_fact_set:
    fact_set = FactSetLite(
      id="fs_standing",
      structure_id="struct_policy",
      period_start=date(2026, 1, 1),
      period_end=date(2026, 12, 31),
      factset_type="disclosure",
      entity_id="ent_1",
    )
  return BaseEnvelopeAtoms(
    structure=structure,
    taxonomy_name="Driftline Policy Notes",
    associations=[],
    elements=[_element()],  # type: ignore[list-item]
    element_ids=["elem_policy"],
    rules=[],
    classifications_by_assoc={},
    fact_set=fact_set,
    verification_results=[],
    verification_summary=None,
  )


class TestBuildTextBlockEnvelope:
  def test_envelope_carries_text_value_in_facts_and_rendering(self) -> None:
    structure = _structure()
    session = MagicMock()
    facts_result = MagicMock()
    facts_result.scalars.return_value.all.return_value = [_text_fact()]
    session.execute.return_value = facts_result

    with patch.object(
      text_block_mod, "load_base_envelope_atoms", return_value=_atoms(structure)
    ):
      envelope = text_block_mod.build_text_block_envelope(session, "struct_policy")

    assert envelope is not None
    assert envelope.information_model.concept_arrangement == "text_block"
    assert len(envelope.facts) == 1
    assert envelope.facts[0].fact_type == "Nonnumeric"
    assert envelope.facts[0].value is None
    assert "FIFO" in (envelope.facts[0].text_value or "")
    rendering = envelope.view.rendering
    assert rendering is not None
    assert len(rendering.rows) == 1
    assert rendering.rows[0].text_value is not None
    assert rendering.rows[0].values == []
    assert rendering.periods[0].end == date(2026, 12, 31)

  def test_returns_none_when_no_facts_and_no_arcs(self) -> None:
    """A bare registry row (no bindings, no arcs) is not renderable."""
    structure = _structure()
    session = MagicMock()

    with patch.object(
      text_block_mod,
      "load_base_envelope_atoms",
      return_value=_atoms(structure, with_fact_set=False),
    ):
      envelope = text_block_mod.build_text_block_envelope(session, "struct_policy")

    assert envelope is None

  def test_returns_none_when_structure_missing(self) -> None:
    session = MagicMock()
    with patch.object(text_block_mod, "load_base_envelope_atoms", return_value=None):
      assert text_block_mod.build_text_block_envelope(session, "struct_missing") is None


class TestDisclosureDispatch:
  def test_text_block_cap_routes_to_text_builder(self) -> None:
    session = MagicMock()
    session.get.return_value = _structure(cap="text_block")

    sentinel = MagicMock()
    with patch.object(
      disclosure_handlers, "build_text_block_envelope", return_value=sentinel
    ) as text_builder:
      result = disclosure_handlers.build_envelope(session, "struct_policy", "fs_pin")

    text_builder.assert_called_once_with(session, "struct_policy", "fs_pin")
    assert result is sentinel

  def test_level1_textblock_cap_routes_to_text_builder(self) -> None:
    session = MagicMock()
    session.get.return_value = _structure(cap="level1_textblock")

    with patch.object(
      disclosure_handlers, "build_text_block_envelope", return_value=MagicMock()
    ) as text_builder:
      disclosure_handlers.build_envelope(session, "struct_policy")

    text_builder.assert_called_once()

  def test_roll_up_cap_keeps_statement_path(self) -> None:
    session = MagicMock()
    session.get.return_value = _structure(cap="roll_up")

    with (
      patch.object(disclosure_handlers, "build_text_block_envelope") as text_builder,
      patch.object(
        disclosure_handlers, "_build_disclosure_envelope", return_value=None
      ) as stmt_builder,
    ):
      result = disclosure_handlers.build_envelope(session, "struct_note")

    text_builder.assert_not_called()
    stmt_builder.assert_called_once()
    assert result is None
