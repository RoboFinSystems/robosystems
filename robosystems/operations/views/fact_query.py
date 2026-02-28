from typing import Any

import pandas as pd

from robosystems.middleware.graph import get_graph_repository


async def query_fact_grid(
  graph_id: str,
  elements: list[str] | None = None,
  canonical_concepts: list[str] | None = None,
  periods: list[str] | None = None,
  entity: str | None = None,
  entities: list[str] | None = None,
  form: str | None = None,
  fiscal_year: int | None = None,
  fiscal_period: str | None = None,
  period_type: str | None = None,
) -> pd.DataFrame:
  """Query facts for a fact grid, aligned with the build-fact-grid MCP tool.

  Builds a parameterized Cypher query with optional filters for elements
  (by qname or canonical concept), periods, entities, report context,
  and period type. Returns deduplicated consolidated facts.

  Args:
      graph_id: Graph containing facts.
      elements: Element qnames (e.g., ['us-gaap:Assets']).
      canonical_concepts: Canonical concept names (e.g., ['revenue']).
      periods: Period end dates (YYYY-MM-DD).
      entity: Single entity ticker, CIK, or name.
      entities: Multiple entity tickers.
      form: SEC filing form type (e.g., '10-K').
      fiscal_year: Fiscal year filter.
      fiscal_period: Fiscal period filter (e.g., 'FY', 'Q1').
      period_type: 'annual', 'quarterly', or 'instant'.

  Returns:
      DataFrame with columns: element_id, element_name, period_end, value, unit,
      and optionally entity_ticker, entity_name when entity filters are used.
  """
  match_clauses = [
    "MATCH (f:Fact)-[:FACT_HAS_ELEMENT]->(el:Element)",
    "MATCH (f)-[:FACT_HAS_PERIOD]->(p:Period)",
    "MATCH (f)-[:FACT_HAS_UNIT]->(u:Unit)",
  ]
  where_clauses = ["f.has_dimensions = false"]
  parameters: dict[str, Any] = {}

  # Element filter: qnames, canonical concepts, or both
  if elements and canonical_concepts:
    where_clauses.append(
      "(el.qname IN $elements OR el.canonical_concept IN $canonical_concepts)"
    )
    parameters["elements"] = elements
    parameters["canonical_concepts"] = canonical_concepts
  elif elements:
    where_clauses.append("el.qname IN $elements")
    parameters["elements"] = elements
  elif canonical_concepts:
    where_clauses.append("el.canonical_concept IN $canonical_concepts")
    parameters["canonical_concepts"] = canonical_concepts

  if periods:
    where_clauses.append("p.end_date IN $periods")
    parameters["periods"] = periods

  if period_type:
    if period_type == "instant":
      where_clauses.append("p.period_type = 'instant'")
    elif period_type == "annual":
      where_clauses.append("p.duration_type = 'annual'")
    elif period_type == "quarterly":
      where_clauses.append("p.duration_type = 'quarterly'")

  # Normalize entity filter: single 'entity' or multi 'entities'
  entity_list = entities if entities else ([entity] if entity else None)

  if entity_list:
    match_clauses.append("MATCH (f)-[:FACT_HAS_ENTITY]->(ent:Entity)")
    where_clauses.append(
      "(ent.ticker IN $entities OR ent.cik IN $entities OR ent.name IN $entities)"
    )
    parameters["entities"] = entity_list

  if form or fiscal_year is not None or fiscal_period:
    match_clauses.append("MATCH (r:Report)-[:REPORT_HAS_FACT]->(f)")
    if form:
      where_clauses.append("r.form = $form")
      parameters["form"] = form
    if fiscal_year is not None:
      where_clauses.append("r.fiscal_year_focus = $fiscal_year")
      parameters["fiscal_year"] = fiscal_year
    if fiscal_period:
      where_clauses.append("r.fiscal_period_focus = $fiscal_period")
      parameters["fiscal_period"] = fiscal_period

  if entity_list:
    return_clause = """
    RETURN DISTINCT
      el.qname as element_id,
      el.name as element_name,
      p.end_date as period_end,
      f.numeric_value as value,
      u.value as unit,
      ent.ticker as entity_ticker,
      ent.name as entity_name
    """
  else:
    return_clause = """
    RETURN DISTINCT
      el.qname as element_id,
      el.name as element_name,
      p.end_date as period_end,
      f.numeric_value as value,
      u.value as unit
    """

  query = "\n".join(match_clauses)
  query += "\nWHERE " + "\n  AND ".join(where_clauses)
  query += return_clause

  repository = await get_graph_repository(graph_id)
  results = await repository.execute_query(query, parameters)

  base_columns = ["element_id", "element_name", "period_end", "value", "unit"]
  if entity_list:
    base_columns.extend(["entity_ticker", "entity_name"])

  if not results:
    return pd.DataFrame(columns=base_columns)

  return pd.DataFrame(results)


async def query_facts_with_aspects(
  graph_id: str,
  fact_set_id: str | None = None,
  period_start: str | None = None,
  period_end: str | None = None,
  entity_id: str | None = None,
  requested_dimensions: list[str] | None = None,
) -> pd.DataFrame:
  """Query existing facts with all aspects (legacy, used by save_view).

  This function is retained for backward compatibility with the save_view
  endpoint which uses the FactSet-based query pattern.
  """
  query = """
    MATCH (f:Fact)-[:FACT_HAS_ELEMENT]->(e:Element)
    MATCH (f)-[:FACT_HAS_PERIOD]->(p:Period)
    MATCH (f)-[:FACT_HAS_UNIT]->(u:Unit)
    MATCH (f)-[:FACT_HAS_ENTITY]->(ent:Entity)
    OPTIONAL MATCH (f)-[:FACT_HAS_DIMENSION]->(d:Dimension)
                  -[:DIMENSION_HAS_AXIS_ELEMENT]->(axis:Element)
    OPTIONAL MATCH (d)-[:DIMENSION_HAS_MEMBER_ELEMENT]->(member:Element)
    """

  where_clauses: list[str] = []
  params: dict[str, Any] = {}

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

  if requested_dimensions is None or len(requested_dimensions) == 0:
    where_clauses.append("d IS NULL")
  else:
    where_clauses.append("axis.name IN $requested_dimensions")
    params["requested_dimensions"] = requested_dimensions

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
           p.calendar_year AS calendar_year,
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
        "calendar_year",
        "unit_value",
        "entity_id",
        "dimension_axis",
        "dimension_member",
      ]
    )

  df = pd.DataFrame(results)

  dedup_cols = ["element_id", "period_end", "entity_id", "numeric_value"]
  if requested_dimensions and len(requested_dimensions) > 0:
    dedup_cols.extend(["dimension_axis", "dimension_member"])
  df = df.drop_duplicates(subset=dedup_cols, keep="first")

  return df
