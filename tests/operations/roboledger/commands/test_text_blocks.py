"""Tests for the Document → text-block-fact bind command."""

from __future__ import annotations

import hashlib
from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from robosystems.models.api.extensions.text_blocks import BindTextBlockRequest
from robosystems.models.extensions import Fact as FactModel
from robosystems.operations.roboledger.commands import text_blocks as tb

_DOC_CONTENT = """---
title: Inventory Policy
---

# Inventory and COGS Policy

Green coffee is valued FIFO at the lower of cost or NRV, with freight and
import duties capitalized into landed cost at receipt for every lot we
purchase during the fiscal year.

## Costing Method

Roasting labor and packaging are absorbed into finished goods using a
standard absorption rate reviewed quarterly by operations and finance, with
variances recognized in cost of goods sold in the period they arise.

## Reserves

Obsolete inventory is written down when identified through the quarterly
cycle count program, and any lot older than twelve months is reviewed for
net realizable value impairment before period close each quarter end.
"""


def _doc(content: str = _DOC_CONTENT) -> SimpleNamespace:
  return SimpleNamespace(id="doc_1", title="Inventory Policy", content=content)


def _request(**overrides) -> BindTextBlockRequest:
  base = {
    "document_id": "doc_1",
    "structure_id": "struct_policy",
    "element_id": "elem_policy",
    "period_start": date(2026, 1, 1),
    "period_end": date(2026, 12, 31),
    "entity_id": "ent_1",
  }
  base.update(overrides)
  return BindTextBlockRequest.model_validate(base)


def _structure(cap: str = "text_block") -> MagicMock:
  s = MagicMock()
  s.id = "struct_policy"
  s.block_type = "regulatory_disclosure"
  s.concept_arrangement = cap
  return s


def _element(*, is_abstract: bool = False, item_type: str | None = None) -> MagicMock:
  e = MagicMock()
  e.id = "elem_policy"
  e.qname = "driftline:InventoryPolicyTextBlock"
  e.is_abstract = is_abstract
  e.item_type = item_type
  return e


def _session(
  structure: MagicMock | None,
  element: MagicMock | None,
  standing=None,
) -> MagicMock:
  """Extensions-session mock: session.get returns structure then element;
  the standing-FactSet lookup returns ``standing``."""
  session = MagicMock()
  session.get.side_effect = [structure, element]
  standing_result = MagicMock()
  standing_result.scalar_one_or_none.return_value = standing
  session.execute.return_value = standing_result
  return session


class TestRequestModel:
  def test_requires_exactly_one_element_ref(self) -> None:
    with pytest.raises(ValueError):
      _request(element_id=None, element_qname=None)
    with pytest.raises(ValueError):
      _request(element_qname="driftline:X")  # both set


class TestBindTextBlock:
  def test_creates_standing_fact_set_and_nonnumeric_fact(self) -> None:
    session = _session(_structure(), _element())
    platform_db = MagicMock()

    with patch.object(tb.Document, "get_by_id_and_graph", return_value=_doc()):
      resp = tb.bind_text_block(session, platform_db, "kg_x", _request(), "usr_1")

    assert resp.replaced is False
    assert resp.document_id == "doc_1"
    assert resp.element_id == "elem_policy"
    assert len(resp.content_hash) == 64  # full sha256 hex, not the [:12] helper
    facts = [
      c.args[0] for c in session.add.call_args_list if isinstance(c.args[0], FactModel)
    ]
    assert len(facts) == 1
    fact = facts[0]
    assert fact.fact_type == "Nonnumeric"
    assert fact.value is None
    assert fact.value_type == "inline"
    assert fact.content_type == "text/markdown"
    assert fact.period_type == "duration"
    assert "Green coffee is valued FIFO" in fact.string_value
    assert resp.characters == len(fact.string_value)

  def test_whole_document_strips_frontmatter(self) -> None:
    session = _session(_structure(), _element())
    with patch.object(tb.Document, "get_by_id_and_graph", return_value=_doc()):
      resp = tb.bind_text_block(session, MagicMock(), "kg_x", _request(), "usr_1")
    facts = [
      c.args[0] for c in session.add.call_args_list if isinstance(c.args[0], FactModel)
    ]
    assert "title: Inventory Policy" not in facts[0].string_value
    assert resp.section_id is None

  def test_section_bind_extracts_by_slug(self) -> None:
    session = _session(_structure(), _element())
    with patch.object(tb.Document, "get_by_id_and_graph", return_value=_doc()):
      resp = tb.bind_text_block(
        session,
        MagicMock(),
        "kg_x",
        _request(section_id="costing-method"),
        "usr_1",
      )
    facts = [
      c.args[0] for c in session.add.call_args_list if isinstance(c.args[0], FactModel)
    ]
    assert "absorbed into finished goods" in facts[0].string_value
    assert "written down when identified" not in facts[0].string_value
    assert resp.section_id == "costing-method"

  def test_unknown_section_lists_available_slugs(self) -> None:
    session = _session(_structure(), _element())
    with patch.object(tb.Document, "get_by_id_and_graph", return_value=_doc()):
      with pytest.raises(tb.SectionNotFoundError) as exc:
        tb.bind_text_block(
          session, MagicMock(), "kg_x", _request(section_id="nope"), "usr_1"
        )
    assert "costing-method" in str(exc.value)

  def test_cross_graph_document_is_a_plain_miss(self) -> None:
    session = _session(_structure(), _element())
    with patch.object(tb.Document, "get_by_id_and_graph", return_value=None):
      with pytest.raises(tb.DocumentNotFoundError):
        tb.bind_text_block(session, MagicMock(), "kg_other", _request(), "usr_1")

  def test_rejects_non_text_block_structure(self) -> None:
    session = _session(_structure(cap="roll_up"), _element())
    with patch.object(tb.Document, "get_by_id_and_graph", return_value=_doc()):
      with pytest.raises(tb.TextBlockStructureError):
        tb.bind_text_block(session, MagicMock(), "kg_x", _request(), "usr_1")

  def test_rejects_abstract_element(self) -> None:
    session = _session(_structure(), _element(is_abstract=True))
    with patch.object(tb.Document, "get_by_id_and_graph", return_value=_doc()):
      with pytest.raises(tb.TextBlockElementError):
        tb.bind_text_block(session, MagicMock(), "kg_x", _request(), "usr_1")

  def test_stamps_element_item_type_when_null(self) -> None:
    element = _element(item_type=None)
    session = _session(_structure(), element)
    with patch.object(tb.Document, "get_by_id_and_graph", return_value=_doc()):
      tb.bind_text_block(session, MagicMock(), "kg_x", _request(), "usr_1")
    assert element.item_type == "text_block"

  def test_preserves_existing_item_type(self) -> None:
    element = _element(item_type="string")
    session = _session(_structure(), element)
    with patch.object(tb.Document, "get_by_id_and_graph", return_value=_doc()):
      tb.bind_text_block(session, MagicMock(), "kg_x", _request(), "usr_1")
    assert element.item_type == "string"

  def test_rebind_replaces_fact_and_refreshes_provenance(self) -> None:
    standing = MagicMock()
    standing.id = "fs_standing"
    old_fact = MagicMock(spec=FactModel)

    session = MagicMock()
    session.get.side_effect = [_structure(), _element()]
    standing_result = MagicMock()
    standing_result.scalar_one_or_none.return_value = standing
    existing_result = MagicMock()
    existing_result.scalars.return_value.all.return_value = [old_fact]
    session.execute.side_effect = [standing_result, existing_result]

    with patch.object(tb.Document, "get_by_id_and_graph", return_value=_doc()):
      resp = tb.bind_text_block(session, MagicMock(), "kg_x", _request(), "usr_1")

    assert resp.replaced is True
    assert resp.fact_set_id == "fs_standing"
    session.delete.assert_called_once_with(old_fact)
    # Standing FactSet's provenance refreshed to the new bind.
    assert standing.provenance["origin"] == "document"
    assert standing.provenance["content_hash"] == resp.content_hash

  def test_content_hash_is_full_sha256_of_bound_text(self) -> None:
    session = _session(_structure(), _element())
    with patch.object(tb.Document, "get_by_id_and_graph", return_value=_doc()):
      resp = tb.bind_text_block(session, MagicMock(), "kg_x", _request(), "usr_1")
    facts = [
      c.args[0] for c in session.add.call_args_list if isinstance(c.args[0], FactModel)
    ]
    expected = hashlib.sha256(facts[0].string_value.encode("utf-8")).hexdigest()
    assert resp.content_hash == expected
