"""Account rollups read operation.

Shows how CoA accounts roll up to reporting line items — the mapping
taxonomy rendered with current trial balance balances. This is what
accountants call a "lead schedule."
"""

from __future__ import annotations

from datetime import date

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from robosystems.models.api.extensions import cents_to_dollars
from robosystems.models.api.extensions.account_rollups import (
  AccountRollupGroup,
  AccountRollupRow,
  AccountRollupsResponse,
)
from robosystems.models.extensions.roboledger import COA_SOURCES, Structure


class MappingNotFoundError(LookupError):
  """Raised when a user-supplied mapping_id does not exist."""


def _natural_sign(net_balance: float, balance_type: str) -> float:
  """Convert net balance (debits - credits) to natural sign for display."""
  if balance_type == "credit":
    return -net_balance
  return net_balance


_CLASSIFICATION_ORDER = {
  "asset": 0,
  "liability": 1,
  "equity": 2,
  "revenue": 3,
  "expense": 4,
}


_ROLLUP_SQL = text("""
  SELECT
    target.id AS reporting_element_id,
    target.name AS reporting_name,
    target.qname AS reporting_qname,
    tcls.identifier AS classification,
    target.balance_type,
    source.id AS coa_element_id,
    source.name AS coa_name,
    source.code AS coa_code,
    COALESCE(SUM(li.debit_amount), 0) AS total_debits,
    COALESCE(SUM(li.credit_amount), 0) AS total_credits
  FROM associations mapping
  JOIN elements source ON source.id = mapping.from_element_id
  JOIN elements target ON target.id = mapping.to_element_id
  LEFT JOIN element_classifications tec
    ON tec.element_id = target.id AND tec.is_primary = TRUE
  LEFT JOIN classifications tcls
    ON tcls.id = tec.classification_id
    AND tcls.category = 'elementsOfFinancialStatements'
  LEFT JOIN line_items li ON li.element_id = source.id
  LEFT JOIN entries e ON e.id = li.entry_id AND e.status = 'posted'
    AND (e.posting_date >= :start_date OR :start_date IS NULL)
    AND (e.posting_date <= :end_date OR :end_date IS NULL)
  WHERE mapping.structure_id = :mapping_id
    AND mapping.association_type = 'mapping'
  GROUP BY target.id, target.name, target.qname, tcls.identifier,
           target.balance_type, source.id, source.name, source.code
  ORDER BY tcls.identifier, target.name, source.code
""")


_UNMAPPED_SQL = text("""
  SELECT COUNT(*) AS cnt
  FROM elements e
  WHERE e.source = ANY(:sources)
    AND e.is_active = true
    AND e.is_abstract = false
    AND NOT EXISTS (
      SELECT 1 FROM associations a
      WHERE a.from_element_id = e.id
        AND a.association_type = 'mapping'
        AND a.structure_id = :mapping_id
    )
""")


def get_account_rollups(
  session: Session,
  *,
  mapping_id: str | None = None,
  start_date: date | None = None,
  end_date: date | None = None,
) -> AccountRollupsResponse:
  """Return CoA accounts grouped by reporting element with balances.

  When `mapping_id` is `None`, auto-discovers the first active
  `coa_mapping` structure. Returns an empty response if no mapping
  exists at all. Raises `MappingNotFoundError` if a caller-supplied
  `mapping_id` does not resolve to a structure.
  """
  # Auto-discover mapping if not provided
  if not mapping_id:
    mapping = session.execute(
      select(Structure)
      .where(
        Structure.structure_type == "coa_mapping",
        Structure.is_active.is_(True),
      )
      .limit(1)
    ).scalar_one_or_none()

    if not mapping:
      return AccountRollupsResponse(
        mapping_id="",
        mapping_name="No mapping found",
        groups=[],
        total_mapped=0,
        total_unmapped=0,
      )
    mapping_id = str(mapping.id)
    mapping_name = str(mapping.name)
  else:
    mapping = session.get(Structure, mapping_id)
    if not mapping:
      raise MappingNotFoundError("Mapping not found")
    mapping_name = str(mapping.name)

  # Single query: mapping associations + trial balance balances
  result = session.execute(
    _ROLLUP_SQL,
    {
      "mapping_id": mapping_id,
      "start_date": start_date,
      "end_date": end_date,
    },
  )

  # Group rows by reporting element
  groups_dict: dict[str, AccountRollupGroup] = {}
  for row in result:
    debits = cents_to_dollars(row.total_debits)
    credits = cents_to_dollars(row.total_credits)
    net = debits - credits
    natural = _natural_sign(net, row.balance_type or "debit")

    key = row.reporting_element_id
    if key not in groups_dict:
      groups_dict[key] = AccountRollupGroup(
        reporting_element_id=row.reporting_element_id,
        reporting_name=row.reporting_name,
        reporting_qname=row.reporting_qname or "",
        classification=row.classification or "",
        balance_type=row.balance_type or "debit",
        total=0.0,
        accounts=[],
      )

    groups_dict[key].accounts.append(
      AccountRollupRow(
        element_id=row.coa_element_id,
        account_name=row.coa_name,
        account_code=row.coa_code,
        total_debits=debits,
        total_credits=credits,
        net_balance=natural,
      )
    )
    groups_dict[key].total += natural

  # Sort groups by classification order
  groups = sorted(
    groups_dict.values(),
    key=lambda g: (
      _CLASSIFICATION_ORDER.get(g.classification, 99),
      g.reporting_name,
    ),
  )

  # Count unmapped CoA elements
  unmapped_result = session.execute(
    _UNMAPPED_SQL,
    {"mapping_id": mapping_id, "sources": list(COA_SOURCES)},
  )
  unmapped_row = unmapped_result.fetchone()
  total_unmapped = unmapped_row.cnt if unmapped_row else 0

  total_mapped = sum(len(g.accounts) for g in groups)

  return AccountRollupsResponse(
    mapping_id=mapping_id,
    mapping_name=mapping_name,
    groups=groups,
    total_mapped=total_mapped,
    total_unmapped=total_unmapped,
  )
