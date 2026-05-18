"""Parity tests for the QB tx_type → canonical action verb mapping.

The mapping lives in three places and they must stay in sync:

1. `robosystems/adapters/quickbooks/pipeline/event_action_mapping.py` —
   Python dict (the canonical QB-side projection).
2. `robosystems/adapters/quickbooks/dbt/models/ledger/transactions.sql` —
   the dbt CASE block that populates `event_action` in the staging mart.
3. `robosystems/models/extensions/roboledger/event.py:EVENT_ACTIONS` —
   the canonical 19-verb frozenset (also the DB CHECK constraint).

A typo in any one place silently drifts. These tests catch the common
drift modes at test time.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from robosystems.adapters.quickbooks.pipeline.event_action_mapping import (
  QB_TXTYPE_TO_EVENT_ACTION,
  map_qb_txtype_to_event_action,
)
from robosystems.models.extensions.roboledger.event import EVENT_ACTIONS

_DBT_TRANSACTIONS_SQL = (
  Path(__file__).resolve().parents[4]
  / "robosystems"
  / "adapters"
  / "quickbooks"
  / "dbt"
  / "models"
  / "ledger"
  / "transactions.sql"
)


def _extract_dbt_event_action_branches() -> dict[str, str]:
  """Parse the `as event_action` CASE block out of transactions.sql.

  Returns {tx_type: action_verb} from each `when e.tx_type = 'X' then 'Y'`
  branch in the event_action CASE. Captures only the event_action block;
  the file has other CASE blocks (event_type, event_category).
  """
  sql = _DBT_TRANSACTIONS_SQL.read_text()

  # The event_action CASE block runs from a `case` statement preceding
  # `as event_action` and ends at that label.
  match = re.search(
    r"case\s+(?:when[^\n]*\n\s+)*?\s*else\s+null\s+end\s+as\s+event_action",
    sql,
    re.IGNORECASE,
  )
  assert match is not None, "could not locate event_action CASE block in dbt SQL"

  block = match.group(0)
  branches: dict[str, str] = {}
  for tx, action in re.findall(
    r"when\s+e\.tx_type\s*=\s*'([^']+)'\s+then\s+'([^']+)'",
    block,
    re.IGNORECASE,
  ):
    branches[tx] = action
  return branches


class TestQbToEventActionMappingParity:
  def test_all_python_dict_values_are_canonical_verbs(self):
    """Defended by the module-level guard, but assert here for visibility."""
    invalid = {v for v in QB_TXTYPE_TO_EVENT_ACTION.values() if v not in EVENT_ACTIONS}
    assert not invalid, f"QB mapping has non-canonical values: {sorted(invalid)}"

  def test_dbt_branches_match_python_dict(self):
    """Every dbt event_action branch must appear in QB_TXTYPE_TO_EVENT_ACTION
    with the same verb (drift detector)."""
    dbt_branches = _extract_dbt_event_action_branches()
    assert dbt_branches, "dbt SQL produced zero event_action branches — regex broken?"

    drift: list[str] = []
    for tx, dbt_action in dbt_branches.items():
      py_action = QB_TXTYPE_TO_EVENT_ACTION.get(tx)
      if py_action is None:
        drift.append(f"  dbt has '{tx}' → '{dbt_action}', Python dict is missing it")
      elif py_action != dbt_action:
        drift.append(f"  '{tx}': dbt='{dbt_action}' vs Python='{py_action}'")

    assert not drift, (
      "QB action-verb mapping drift between dbt SQL and Python dict:\n"
      + "\n".join(drift)
      + "\n\n  Fix: update both event_action_mapping.py and transactions.sql together."
    )

  def test_python_dict_branches_match_dbt(self):
    """Every Python-dict key must also appear in the dbt CASE block."""
    dbt_branches = _extract_dbt_event_action_branches()
    missing = sorted(set(QB_TXTYPE_TO_EVENT_ACTION) - set(dbt_branches))
    assert not missing, (
      f"Python QB mapping has tx_types not present in dbt SQL: {missing}\n"
      "  Fix: add CASE branches in transactions.sql for these tx_types."
    )

  def test_dbt_falls_back_to_null(self):
    """The else clause must be NULL — DB CHECK accepts NULL, so unknown
    tx_types should fall through harmlessly rather than violate the CHECK."""
    sql = _DBT_TRANSACTIONS_SQL.read_text()
    assert re.search(r"else\s+null\s+end\s+as\s+event_action", sql, re.IGNORECASE), (
      "event_action CASE must end with `else null`"
    )


class TestMapQbTxtypeToEventAction:
  @pytest.mark.parametrize(
    ("tx_type", "expected"),
    [
      ("Invoice", "transferAllRights"),
      ("Bill", "accept"),
      ("Payment", "transfer"),
      ("BillPayment", "transfer"),
      ("Bill Payment", "transfer"),
      ("JournalEntry", "modify"),
      ("Inventory Qty Adjust", "modify"),
    ],
  )
  def test_known_types_return_canonical_verb(self, tx_type: str, expected: str):
    assert map_qb_txtype_to_event_action(tx_type) == expected

  def test_unknown_returns_none(self):
    assert map_qb_txtype_to_event_action("SomeUnknownType") is None

  def test_none_input_returns_none(self):
    assert map_qb_txtype_to_event_action(None) is None

  def test_empty_string_returns_none(self):
    assert map_qb_txtype_to_event_action("") is None
