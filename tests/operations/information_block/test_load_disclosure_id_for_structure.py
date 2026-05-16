"""Unit tests for :func:`load_disclosure_id_for_structure`.

Three branches covered:

1. Target structure IS a disclosure-namespace structure -> derive qname
   from its own role_uri.
2. Target is a renderable presentation -> look up the
   disclosure-namespace sibling by block_type.
3. No match (validation_rules, taxonomy_mapping, schedules) -> ``None``.

Also confirms the module-level type cache short-circuits subsequent
calls for the same block_type.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from robosystems.operations.information_block.envelope import (
  _DISCLOSURE_QNAME_BY_TYPE,
  _DISCLOSURE_ROLE_PREFIX,
  load_disclosure_id_for_structure,
)


@pytest.fixture(autouse=True)
def _clear_cache() -> Any:
  """Reset the module-level type cache between tests."""
  _DISCLOSURE_QNAME_BY_TYPE.clear()
  yield
  _DISCLOSURE_QNAME_BY_TYPE.clear()


def _make_structure(*, role_uri: str | None, block_type: str | None) -> Any:
  """Build a mock Structure ORM row.

  ``role_uri`` lives inside the ``metadata_`` JSONB blob (see
  ``_structure_role_uri`` in the helper module), not as a top-level
  column — mirror that in the mock so attribute access matches what
  the real ORM exposes.
  """
  s = MagicMock()
  s.metadata_ = {"role_uri": role_uri} if role_uri is not None else {}
  s.block_type = block_type
  return s


class TestDirectDisclosureRoleUri:
  def test_balance_sheet_disclosure_returns_own_qname(self) -> None:
    session = MagicMock()
    session.get.return_value = _make_structure(
      role_uri=f"{_DISCLOSURE_ROLE_PREFIX}BalanceSheet",
      block_type="balance_sheet",
    )
    assert (
      load_disclosure_id_for_structure(session, "struct_bs_disclosure")
      == "disclosures:BalanceSheet"
    )
    # Should never hit the fallback query for direct hits.
    session.execute.assert_not_called()

  def test_note_disclosure_returns_qname(self) -> None:
    session = MagicMock()
    session.get.return_value = _make_structure(
      role_uri=f"{_DISCLOSURE_ROLE_PREFIX}LeasesOfLesseeDisclosure",
      block_type="regulatory_disclosure",
    )
    assert (
      load_disclosure_id_for_structure(session, "struct_leases")
      == "disclosures:LeasesOfLesseeDisclosure"
    )


class TestStructureTypeFallback:
  def test_presentation_finds_matching_disclosure(self) -> None:
    """A BS-classified presentation should resolve to disclosures:BalanceSheet
    via the block_type lookup."""
    session = MagicMock()
    session.get.return_value = _make_structure(
      role_uri="https://robosystems.ai/seattle/cm-roles/roles/rs-gaap-presentation/BS-classified",
      block_type="balance_sheet",
    )
    # The fallback query selects metadata->>'role_uri' (a string), not the row
    session.execute.return_value.scalar.return_value = (
      f"{_DISCLOSURE_ROLE_PREFIX}BalanceSheet"
    )

    assert (
      load_disclosure_id_for_structure(session, "struct_bs_classified")
      == "disclosures:BalanceSheet"
    )
    assert session.execute.call_count == 1

  def test_cache_short_circuits_second_call_with_same_type(self) -> None:
    """The N+1 fix: 2 envelopes of the same block_type should issue
    only 1 fallback query thanks to the module-level cache."""
    session = MagicMock()
    session.get.side_effect = [
      _make_structure(
        role_uri="https://.../IS-multistep",
        block_type="income_statement",
      ),
      _make_structure(
        role_uri="https://.../IS-singlestep",
        block_type="income_statement",
      ),
    ]
    session.execute.return_value.scalar.return_value = (
      f"{_DISCLOSURE_ROLE_PREFIX}IncomeStatement"
    )

    a = load_disclosure_id_for_structure(session, "struct_is_multistep")
    b = load_disclosure_id_for_structure(session, "struct_is_singlestep")

    assert a == "disclosures:IncomeStatement"
    assert b == "disclosures:IncomeStatement"
    # Second call hits the type cache; only one fallback query fires.
    assert session.execute.call_count == 1


class TestNoMatch:
  def test_target_not_found_returns_none(self) -> None:
    session = MagicMock()
    session.get.return_value = None
    assert load_disclosure_id_for_structure(session, "missing") is None

  def test_target_without_role_uri_returns_none(self) -> None:
    session = MagicMock()
    session.get.return_value = _make_structure(
      role_uri=None, block_type="balance_sheet"
    )
    assert load_disclosure_id_for_structure(session, "no_role") is None

  def test_unknown_block_type_returns_none(
    self, monkeypatch: pytest.MonkeyPatch
  ) -> None:
    """validation_rules and taxonomy_mapping should resolve to None
    because no disclosure-namespace sibling exists with that type."""
    session = MagicMock()
    session.get.return_value = _make_structure(
      role_uri="https://.../rs-gaap-calculations/IS-Rev",
      block_type="validation_rules",
    )
    session.execute.return_value.scalar.return_value = None

    assert load_disclosure_id_for_structure(session, "struct_calc") is None
    # The None result is cached as well — second call doesn't requery.
    session2 = MagicMock()
    session2.get.return_value = _make_structure(
      role_uri="https://.../rs-gaap-calculations/IS-COR",
      block_type="validation_rules",
    )
    assert load_disclosure_id_for_structure(session2, "struct_calc2") is None
    session2.execute.assert_not_called()
