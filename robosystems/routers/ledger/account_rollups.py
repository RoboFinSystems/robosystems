"""Account rollups endpoint.

Shows how CoA accounts roll up to reporting line items — the mapping
taxonomy rendered with current trial balance balances. This is what
accountants call a "lead schedule."
"""

from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from sqlalchemy import select, text
from sqlalchemy.exc import ProgrammingError

from robosystems.db.extensions import extensions_session
from robosystems.logger import logger
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.models.api.extensions import cents_to_dollars
from robosystems.models.api.extensions.account_rollups import (
  AccountRollupGroup,
  AccountRollupRow,
  AccountRollupsResponse,
)
from robosystems.models.core import User
from robosystems.models.extensions.roboledger import Structure

router = APIRouter()


def _ledger_404():
  return HTTPException(
    status_code=404,
    detail="Ledger not initialized. Connect a data source first.",
  )


def _natural_sign(net_balance: float, balance_type: str) -> float:
  """Convert net balance (debits - credits) to natural sign for display."""
  if balance_type == "credit":
    return -net_balance
  return net_balance


# Classification sort order for grouping
_CLASSIFICATION_ORDER = {
  "asset": 0,
  "liability": 1,
  "equity": 2,
  "revenue": 3,
  "expense": 4,
}


@router.get(
  "/account-rollups",
  response_model=AccountRollupsResponse,
  operation_id="getAccountRollups",
  summary="Account Rollups",
  tags=["Ledger"],
)
async def get_account_rollups(
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  mapping_id: str | None = Query(
    None, description="Mapping structure ID (auto-discovers if omitted)"
  ),
  start_date: date | None = Query(None, description="Start date (inclusive)"),
  end_date: date | None = Query(None, description="End date (inclusive)"),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
):
  """Account rollups — CoA accounts grouped by reporting element with balances.

  Shows how company-specific accounts roll up to standardized reporting
  line items. Auto-discovers the mapping structure if mapping_id is not provided.
  """
  try:
    with extensions_session(graph_id) as session:
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
          raise HTTPException(status_code=404, detail="Mapping not found")
        mapping_name = str(mapping.name)

      # Single query: mapping associations + trial balance balances
      result = session.execute(
        text("""
          SELECT
            target.id AS reporting_element_id,
            target.name AS reporting_name,
            target.qname AS reporting_qname,
            target.classification,
            target.balance_type,
            source.id AS coa_element_id,
            source.name AS coa_name,
            source.code AS coa_code,
            COALESCE(SUM(li.debit_amount), 0) AS total_debits,
            COALESCE(SUM(li.credit_amount), 0) AS total_credits
          FROM associations mapping
          JOIN elements source ON source.id = mapping.from_element_id
          JOIN elements target ON target.id = mapping.to_element_id
          LEFT JOIN line_items li ON li.element_id = source.id
          LEFT JOIN entries e ON e.id = li.entry_id AND e.status = 'posted'
            AND (e.posting_date >= :start_date OR :start_date IS NULL)
            AND (e.posting_date <= :end_date OR :end_date IS NULL)
          WHERE mapping.structure_id = :mapping_id
            AND mapping.association_type = 'mapping'
          GROUP BY target.id, target.name, target.qname, target.classification,
                   target.balance_type, source.id, source.name, source.code
          ORDER BY target.classification, target.name, source.code
        """),
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

      # Count unmapped
      from robosystems.models.extensions.roboledger import COA_SOURCES

      source_list = ", ".join(f"'{s}'" for s in COA_SOURCES)
      unmapped_result = session.execute(
        text(f"""
          SELECT COUNT(*) AS cnt
          FROM elements e
          WHERE e.source IN ({source_list})
            AND e.is_active = true
            AND e.is_abstract = false
            AND NOT EXISTS (
              SELECT 1 FROM associations a
              WHERE a.from_element_id = e.id
                AND a.association_type = 'mapping'
                AND a.structure_id = :mapping_id
            )
        """),
        {"mapping_id": mapping_id},
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

  except ValueError:
    raise _ledger_404()
  except ProgrammingError as e:
    if "does not exist" in str(e) and ("schema" in str(e) or "relation" in str(e)):
      raise _ledger_404()
    logger.error(f"Account rollups failed: {e}")
    raise
