import pandas as pd
from robosystems.middleware.graph import get_graph_repository


async def aggregate_trial_balance(
  graph_id: str,
  period_start: str,
  period_end: str,
  entity_id: str | None = None,
  requested_dimensions: list[str] | None = None,
) -> pd.DataFrame:
  """
  Aggregate transactions to trial balance (Mode 1: Transaction Aggregation).

  This is the core of the financial reporting system - generating financial
  reports on-demand from raw transaction data without pre-computed facts.

  Args:
      graph_id: Graph containing transactions
      period_start: Start date (YYYY-MM-DD)
      period_end: End date (YYYY-MM-DD)
      entity_id: Optional entity filter
      requested_dimensions: Dimension axes (not typically used for transactions)

  Returns:
      DataFrame with columns:
      - element_id: Element identifier
      - element_name: Element name
      - element_classification: asset, liability, equity, revenue, expense
      - element_balance: debit or credit
      - total_debits: Sum of debit amounts
      - total_credits: Sum of credit amounts
      - net_balance: Calculated balance
  """
  query = """
    MATCH (e:Entity)-[:ENTITY_HAS_TRANSACTION]->(t:Transaction)
          -[:TRANSACTION_HAS_LINE_ITEM]->(li:LineItem)
          -[:LINE_ITEM_RELATES_TO_ELEMENT]->(elem:Element)
    WHERE t.date >= $period_start
      AND t.date <= $period_end

    WITH elem,
         sum(li.debit_amount) AS total_debits,
         sum(li.credit_amount) AS total_credits

    RETURN elem.identifier AS element_id,
           elem.uri AS element_uri,
           elem.name AS element_name,
           elem.classification AS element_classification,
           elem.balance AS element_balance,
           elem.period_type AS element_period_type,
           total_debits,
           total_credits,
           total_debits - total_credits AS net_balance
    ORDER BY elem.name
    """

  params = {
    "period_start": period_start,
    "period_end": period_end,
  }

  if entity_id:
    query = query.replace("WHERE t.date", "WHERE e.identifier = $entity_id AND t.date")
    params["entity_id"] = entity_id

  repository = await get_graph_repository(graph_id)
  results = await repository.execute_query(query, params)

  if not results:
    return pd.DataFrame(
      columns=[
        "element_id",
        "element_uri",
        "element_name",
        "element_classification",
        "element_balance",
        "element_period_type",
        "total_debits",
        "total_credits",
        "net_balance",
      ]
    )

  return pd.DataFrame(results)
