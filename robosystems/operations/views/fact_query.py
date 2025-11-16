import pandas as pd
from robosystems.middleware.graph import get_graph_repository


async def query_facts_with_aspects(
  graph_id: str,
  fact_set_id: str | None = None,
  period_start: str | None = None,
  period_end: str | None = None,
  entity_id: str | None = None,
) -> pd.DataFrame:
  """
  Query existing facts with all aspects (Mode 2: Existing Facts).

  This mode works with pre-existing Fact nodes (like SEC filings data)
  and returns them with all their aspects for multidimensional analysis.

  Args:
      graph_id: Graph containing facts
      fact_set_id: Optional FactSet filter
      period_start: Optional period start filter
      period_end: Optional period end filter
      entity_id: Optional entity filter

  Returns:
      DataFrame with columns:
      - fact_id: Fact identifier
      - numeric_value: Fact value
      - element_id: Element identifier
      - element_name: Element name
      - element_classification: Element type
      - period_id: Period identifier
      - period_start: Period start date
      - period_end: Period end date
      - unit_value: Unit (e.g., USD, shares)
      - entity_id: Entity identifier
      - dimension_axis: Dimension axis (if any)
      - dimension_member: Dimension member (if any)
  """
  query = """
    MATCH (f:Fact)-[:FACT_HAS_ELEMENT]->(e:Element)
    MATCH (f)-[:FACT_HAS_PERIOD]->(p:Period)
    MATCH (f)-[:FACT_HAS_UNIT]->(u:Unit)
    MATCH (f)-[:FACT_HAS_ENTITY]->(ent:Entity)
    OPTIONAL MATCH (f)-[:FACT_HAS_DIMENSION]->(fd:FactDimension)
                  -[:FACT_DIMENSION_AXIS_ELEMENT]->(axis:Element)
    OPTIONAL MATCH (fd)-[:FACT_DIMENSION_MEMBER_ELEMENT]->(member:Element)
    """

  where_clauses = []
  params = {}

  if fact_set_id:
    query += "\nMATCH (fs:FactSet)-[:FACT_SET_CONTAINS_FACT]->(f)"
    where_clauses.append("fs.identifier = $fact_set_id")
    params["fact_set_id"] = fact_set_id

  if period_start:
    where_clauses.append("p.start_date >= $period_start OR p.end_date >= $period_start")
    params["period_start"] = period_start

  if period_end:
    where_clauses.append("p.end_date <= $period_end OR p.start_date <= $period_end")
    params["period_end"] = period_end

  if entity_id:
    where_clauses.append("ent.identifier = $entity_id")
    params["entity_id"] = entity_id

  if where_clauses:
    query += "\nWHERE " + " AND ".join(where_clauses)

  query += """
    RETURN f.identifier AS fact_id,
           f.numeric_value AS numeric_value,
           e.identifier AS element_id,
           e.name AS element_name,
           e.classification AS element_classification,
           e.period_type AS element_period_type,
           p.identifier AS period_id,
           p.start_date AS period_start,
           p.end_date AS period_end,
           p.fiscal_year AS fiscal_year,
           u.value AS unit_value,
           ent.identifier AS entity_id,
           axis.name AS dimension_axis,
           member.name AS dimension_member
    ORDER BY e.name, p.start_date
    """

  repository = await get_graph_repository(graph_id)
  results = await repository.execute_query(query, params)

  if not results:
    return pd.DataFrame(
      columns=[
        "fact_id",
        "numeric_value",
        "element_id",
        "element_name",
        "element_classification",
        "element_period_type",
        "period_id",
        "period_start",
        "period_end",
        "fiscal_year",
        "unit_value",
        "entity_id",
        "dimension_axis",
        "dimension_member",
      ]
    )

  return pd.DataFrame(results)
