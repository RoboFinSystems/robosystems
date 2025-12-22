import uuid
from datetime import datetime
from typing import Any

from robosystems.middleware.graph import get_universal_repository
from robosystems.models.api.views.save_view import (
  FactDetail,
  SaveViewRequest,
  SaveViewResponse,
  StructureDetail,
)


def generate_report_id(entity_id: str, period_end: str, report_type: str) -> str:
  period_slug = period_end.replace("-", "")
  type_slug = report_type.lower().replace(" ", "-")
  entity_slug = entity_id[:8] if len(entity_id) >= 8 else entity_id
  return f"{entity_slug}-{type_slug}-{period_slug}"


async def get_entity_info(graph_id: str) -> dict[str, Any]:
  query = """
    MATCH (e:Entity)
    WHERE e.is_parent = true
    RETURN e.identifier as entity_id, e.name as entity_name
    LIMIT 1
    """

  repository = await get_universal_repository(graph_id)
  results = await repository.execute_query(query, {})

  if not results or len(results) == 0:
    return {"entity_id": "unknown-entity", "entity_name": "Unknown Entity"}

  return {
    "entity_id": results[0]["entity_id"],
    "entity_name": results[0]["entity_name"],
  }


async def query_view_facts(graph_id: str) -> list[dict[str, Any]]:
  income_query = """
    MATCH (usGaap:Element)--(a:Association)--(coaElement:Element)
    WHERE usGaap.uri CONTAINS 'us-gaap'
      AND usGaap.name IN ['RevenueFromContractWithCustomer', 'OperatingExpenses']
    MATCH (li:LineItem)-[:LINE_ITEM_RELATES_TO_ELEMENT]->(coaElement)
    WITH
        usGaap.uri as element_uri,
        usGaap.name as element_name,
        CASE
            WHEN usGaap.name = 'RevenueFromContractWithCustomer'
            THEN sum(li.credit_amount) - sum(li.debit_amount)
            WHEN usGaap.name = 'OperatingExpenses'
            THEN sum(li.debit_amount) - sum(li.credit_amount)
        END as amount
    WHERE amount <> 0
    RETURN element_uri, element_name, amount, 'income_statement' as statement_type
    ORDER BY element_name
    """

  balance_query = """
    MATCH (usGaap:Element)--(a:Association)--(coaElement:Element)
    WHERE usGaap.uri CONTAINS 'us-gaap'
      AND usGaap.name = 'CashAndCashEquivalents'
    MATCH (li:LineItem)-[:LINE_ITEM_RELATES_TO_ELEMENT]->(coaElement)
    WITH
        usGaap.uri as element_uri,
        usGaap.name as element_name,
        sum(li.debit_amount) - sum(li.credit_amount) as amount
    WHERE amount <> 0
    RETURN element_uri, element_name, amount, 'balance_sheet' as statement_type
    """

  repository = await get_universal_repository(graph_id)
  income_results = await repository.execute_query(income_query, {})
  balance_results = await repository.execute_query(balance_query, {})

  facts = []

  if income_results:
    for row in income_results:
      facts.append(
        {
          "element_uri": row["element_uri"],
          "element_name": row["element_name"],
          "numeric_value": float(row["amount"]),
          "fact_type": "monetary",
          "statement_type": row["statement_type"],
        }
      )

  if balance_results:
    for row in balance_results:
      facts.append(
        {
          "element_uri": row["element_uri"],
          "element_name": row["element_name"],
          "numeric_value": float(row["amount"]),
          "fact_type": "monetary",
          "statement_type": row["statement_type"],
        }
      )

  return facts


async def check_report_exists(graph_id: str, report_id: str) -> bool:
  """Check if a report with the given ID already exists"""
  query = """
    MATCH (r:Report {identifier: $report_id})
    RETURN r.identifier as report_id
    LIMIT 1
    """

  repository = await get_universal_repository(graph_id)
  params = {"report_id": report_id}
  results = await repository.execute_query(query, params)
  return results is not None and len(results) > 0


async def delete_report_data(graph_id: str, report_id: str) -> dict[str, int]:
  """Delete all facts, structures, and relationships associated with a report"""
  delete_facts_query = """
    MATCH (r:Report {identifier: $report_id})-[:REPORT_HAS_FACT]->(f:Fact)
    WITH count(f) as fact_count
    MATCH (r:Report {identifier: $report_id})-[:REPORT_HAS_FACT]->(f:Fact)
    DETACH DELETE f
    RETURN fact_count
    """

  repository = await get_universal_repository(graph_id)
  params = {"report_id": report_id}
  facts_results = await repository.execute_query(delete_facts_query, params)

  return {
    "facts_deleted": facts_results[0]["fact_count"]
    if facts_results and len(facts_results) > 0
    else 0,
    "presentations_deleted": 0,
    "calculations_deleted": 0,
  }


async def create_report_node(
  graph_id: str,
  report_id: str,
  entity_id: str,
  entity_name: str,
  period_start: str,
  period_end: str,
  report_type: str,
) -> bool:
  query = """
    CREATE (r:Report {
        identifier: $report_id,
        name: $name,
        uri: $uri,
        report_date: $report_date,
        period_end_date: $period_end_date,
        updated_at: $updated_at,
        processed: true,
        failed: false
    })
    RETURN r.identifier as report_id
    """

  repository = await get_universal_repository(graph_id)
  params = {
    "report_id": report_id,
    "name": f"{report_type} - {entity_name}",
    "uri": f"internal:{report_id}",
    "report_date": period_end,
    "period_end_date": period_end,
    "updated_at": datetime.utcnow().isoformat(),
  }
  results = await repository.execute_query(query, params)
  return results is not None and len(results) > 0


async def update_report_metadata(
  graph_id: str,
  report_id: str,
  entity_id: str,
  entity_name: str,
  period_start: str,
  period_end: str,
  report_type: str,
) -> bool:
  """Update existing report metadata"""
  query = """
    MATCH (r:Report {identifier: $report_id})
    SET r.name = $name,
        r.report_date = $report_date,
        r.period_end_date = $period_end_date,
        r.updated_at = $updated_at
    RETURN r.identifier as report_id
    """

  repository = await get_universal_repository(graph_id)
  params = {
    "report_id": report_id,
    "name": f"{report_type} - {entity_name}",
    "report_date": period_end,
    "period_end_date": period_end,
    "updated_at": datetime.utcnow().isoformat(),
  }
  results = await repository.execute_query(query, params)
  return results is not None and len(results) > 0


async def create_fact_nodes(
  graph_id: str,
  report_id: str,
  facts: list[dict[str, Any]],
  entity_id: str,
  period_start: str,
  period_end: str,
  unit: str = "USD",
) -> list[FactDetail]:
  created_facts = []
  repository = await get_universal_repository(graph_id)

  for fact_data in facts:
    fact_id = str(uuid.uuid4())

    query = """
        MATCH (r:Report {identifier: $report_id})
        MATCH (e:Element {uri: $element_uri})
        MATCH (ent:Entity {identifier: $entity_id})
        CREATE (f:Fact {
            identifier: $fact_id,
            uri: $uri,
            value: $value,
            numeric_value: $numeric_value,
            fact_type: $fact_type,
            decimals: '2',
            value_type: 'numeric',
            content_type: 'monetary'
        })
        CREATE (r)-[:REPORT_HAS_FACT]->(f)
        CREATE (f)-[:FACT_HAS_ELEMENT]->(e)
        CREATE (f)-[:FACT_HAS_ENTITY]->(ent)
        RETURN f.identifier as fact_id
        """

    params = {
      "report_id": report_id,
      "element_uri": fact_data["element_uri"],
      "entity_id": entity_id,
      "fact_id": fact_id,
      "uri": f"{fact_data['element_uri']}#{fact_id}",
      "value": str(fact_data["numeric_value"]),
      "numeric_value": fact_data["numeric_value"],
      "fact_type": fact_data["fact_type"],
    }
    results = await repository.execute_query(query, params)

    if results and len(results) > 0:
      created_facts.append(
        FactDetail(
          fact_id=fact_id,
          element_uri=fact_data["element_uri"],
          element_name=fact_data["element_name"],
          numeric_value=fact_data["numeric_value"],
          unit=unit,
          period_start=period_start,
          period_end=period_end,
        )
      )

  return created_facts


async def create_presentation_structure(
  graph_id: str,
  report_id: str,
  structure_name: str,
  role_uri: str,
  facts: list[dict[str, Any]],
) -> StructureDetail | None:
  structure_id = str(uuid.uuid4())
  repository = await get_universal_repository(graph_id)

  query = """
    CREATE (s:Structure {
        identifier: $identifier,
        uri: $uri,
        network_uri: $network_uri,
        type: 'presentation',
        name: $name,
        definition: $definition
    })
    RETURN s.identifier as structure_id
    """

  params = {
    "identifier": structure_id,
    "uri": role_uri,
    "network_uri": role_uri,
    "name": structure_name,
    "definition": f"Presentation structure for {structure_name}",
  }
  results = await repository.execute_query(query, params)

  if not results or len(results) == 0:
    return None

  return StructureDetail(
    structure_id=structure_id,
    structure_type="presentation",
    name=structure_name,
    element_count=len(facts),
  )


async def create_calculation_structure(
  graph_id: str,
  report_id: str,
  structure_name: str,
  parent_element: str,
  children: list[dict[str, Any]],
) -> StructureDetail | None:
  structure_id = str(uuid.uuid4())
  repository = await get_universal_repository(graph_id)

  query = """
    CREATE (s:Structure {
        identifier: $identifier,
        uri: $uri,
        network_uri: $network_uri,
        type: 'calculation',
        name: $name,
        definition: $definition
    })
    RETURN s.identifier as structure_id
    """

  params = {
    "identifier": structure_id,
    "uri": parent_element,
    "network_uri": parent_element,
    "name": structure_name,
    "definition": f"Calculation structure for {parent_element}",
  }
  results = await repository.execute_query(query, params)

  if not results or len(results) == 0:
    return None

  return StructureDetail(
    structure_id=structure_id,
    structure_type="calculation",
    name=structure_name,
    element_count=len(children),
  )


async def save_view_as_report(
  graph_id: str, request: SaveViewRequest
) -> SaveViewResponse:
  entity_info = await get_entity_info(graph_id)

  if request.entity_id:
    entity_info["entity_id"] = request.entity_id

  if request.report_id:
    report_id = request.report_id
    is_update = await check_report_exists(graph_id, report_id)
  else:
    report_id = generate_report_id(
      entity_info["entity_id"], request.period_end, request.report_type
    )
    is_update = False

  facts = await query_view_facts(graph_id)

  if is_update:
    await delete_report_data(graph_id, report_id)

    await update_report_metadata(
      graph_id,
      report_id,
      entity_info["entity_id"],
      entity_info["entity_name"],
      request.period_start,
      request.period_end,
      request.report_type,
    )
  else:
    await create_report_node(
      graph_id,
      report_id,
      entity_info["entity_id"],
      entity_info["entity_name"],
      request.period_start,
      request.period_end,
      request.report_type,
    )

  created_facts = await create_fact_nodes(
    graph_id,
    report_id,
    facts,
    entity_info["entity_id"],
    request.period_start,
    request.period_end,
  )

  structures = []

  if request.include_presentation:
    income_facts = [f for f in facts if f.get("statement_type") == "income_statement"]
    balance_facts = [f for f in facts if f.get("statement_type") == "balance_sheet"]

    if income_facts:
      income_structure = await create_presentation_structure(
        graph_id,
        report_id,
        "Income Statement",
        "http://example.com/role/IncomeStatement",
        income_facts,
      )
      if income_structure:
        structures.append(income_structure)

    if balance_facts:
      balance_structure = await create_presentation_structure(
        graph_id,
        report_id,
        "Balance Sheet",
        "http://example.com/role/BalanceSheet",
        balance_facts,
      )
      if balance_structure:
        structures.append(balance_structure)

  if request.include_calculation:
    revenue_fact = next((f for f in facts if "Revenue" in f["element_name"]), None)
    expense_fact = next((f for f in facts if "Expense" in f["element_name"]), None)

    if revenue_fact and expense_fact:
      calc_structure = await create_calculation_structure(
        graph_id,
        report_id,
        "Net Income Calculation",
        "us-gaap:NetIncome",
        [
          {
            "element_uri": revenue_fact["element_uri"],
            "element_name": revenue_fact["element_name"],
            "weight": 1.0,
          },
          {
            "element_uri": expense_fact["element_uri"],
            "element_name": expense_fact["element_name"],
            "weight": -1.0,
          },
        ],
      )
      if calc_structure:
        structures.append(calc_structure)

  presentation_count = len(
    [s for s in structures if s.structure_type == "presentation"]
  )
  calculation_count = len([s for s in structures if s.structure_type == "calculation"])

  return SaveViewResponse(
    report_id=report_id,
    report_type=request.report_type,
    entity_id=entity_info["entity_id"],
    entity_name=entity_info["entity_name"],
    period_start=request.period_start,
    period_end=request.period_end,
    fact_count=len(created_facts),
    presentation_count=presentation_count,
    calculation_count=calculation_count,
    facts=created_facts,
    structures=structures,
    created_at=datetime.utcnow().isoformat(),
    parquet_export_prefix=report_id,
  )
