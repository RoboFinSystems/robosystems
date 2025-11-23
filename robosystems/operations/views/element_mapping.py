"""
Element Mapping Operations (Minimal Server-Side Support)

This file provides minimal server-side support for backward compatibility
when the /views endpoint receives a mapping_structure_id parameter.

For new implementations, use client-side extensions:
- robosystems-python-client/robosystems_client/extensions/element_mapping_client.py
- robosystems-python-client/robosystems_client/extensions/subgraph_workspace_client.py
"""

from typing import List, Optional
import pandas as pd

from robosystems.middleware.graph import get_universal_repository
from robosystems.models.api.views import (
  AggregationMethod,
  MappingResponse,
  MappingStructure,
  ElementAssociation,
)
from robosystems.models.iam.graph import GraphTier


async def get_mapping_structure(
  graph_id: str,
  structure_id: str,
  tier: GraphTier = GraphTier.LADYBUG_STANDARD,
) -> Optional[MappingResponse]:
  """
  Get a mapping structure from the main graph (backward compatibility).

  New implementations should use ElementMappingClient to read from subgraphs.
  """
  repository = await get_universal_repository(graph_id, "read", tier)

  query = """
    MATCH (s:Structure)
    WHERE s.identifier = $structure_id AND s.type = 'ElementMapping'
    OPTIONAL MATCH (s)-[:STRUCTURE_HAS_ASSOCIATION]->(a:Association)
    OPTIONAL MATCH (a)-[:ASSOCIATION_HAS_FROM_ELEMENT]->(from_el:Element)
    OPTIONAL MATCH (a)-[:ASSOCIATION_HAS_TO_ELEMENT]->(to_el:Element)
    RETURN s,
           collect({
             identifier: a.identifier,
             source_element: from_el.uri,
             target_element: to_el.uri,
             aggregation_method: a.preferred_label,
             weight: a.weight,
             order_value: a.order_value
           }) as associations
    """

  result = await repository.execute_query(query, {"structure_id": structure_id})

  if not result:
    return None

  row = result[0]
  structure_data = row["s"]

  associations = []
  for assoc in row["associations"]:
    if assoc["identifier"]:
      associations.append(
        ElementAssociation(
          identifier=assoc["identifier"],
          source_element=assoc["source_element"] or "",
          target_element=assoc["target_element"] or "",
          aggregation_method=AggregationMethod(assoc["aggregation_method"] or "sum"),
          weight=assoc["weight"] or 1.0,
          formula=None,
          order_value=assoc["order_value"] or 1.0,
        )
      )

  structure = MappingStructure(
    identifier=structure_data["identifier"],
    name=structure_data["name"],
    description=structure_data.get("definition"),
    taxonomy_uri=structure_data.get("uri"),
    target_taxonomy_uri=structure_data.get("network_uri"),
    associations=associations,
  )

  return MappingResponse(
    structure=structure,
    association_count=len(associations),
  )


def apply_element_mapping(
  fact_data: pd.DataFrame,
  mapping_structure: MappingStructure,
) -> pd.DataFrame:
  """
  Apply element mapping to aggregate source elements into target elements.

  This pandas operation is shared between server and client.
  Consider using ElementMappingClient.apply_element_mapping() for new code.
  """
  if fact_data.empty or not mapping_structure.associations:
    return fact_data

  df = fact_data.copy()
  aggregated_rows = []

  # Handle both numeric_value (from facts) and net_balance (from trial balance)
  value_col = "numeric_value" if "numeric_value" in df.columns else "net_balance"

  # Group associations by target element
  target_groups = {}
  for assoc in mapping_structure.associations:
    if assoc.target_element not in target_groups:
      target_groups[assoc.target_element] = []
    target_groups[assoc.target_element].append(assoc)

  # Build URI to ID mapping
  uri_to_id_map = {}
  if "element_uri" in df.columns and "element_id" in df.columns:
    for _, row in df[["element_uri", "element_id"]].drop_duplicates().iterrows():
      uri_to_id_map[row["element_uri"]] = row["element_id"]

  # Determine groupby columns
  groupby_columns = []
  for col in [
    "period_end",
    "period_start",
    "entity_id",
    "dimension_axis",
    "dimension_member",
  ]:
    if col in df.columns:
      groupby_columns.append(col)

  # Aggregate for each target element
  for target_element, associations in target_groups.items():
    source_element_uris = [assoc.source_element for assoc in associations]
    source_element_ids = [uri_to_id_map.get(uri, uri) for uri in source_element_uris]

    source_facts = df[df["element_id"].isin(source_element_ids)].copy()

    if source_facts.empty:
      continue

    aggregation_method = associations[0].aggregation_method

    if groupby_columns:
      for group_keys, group_df in source_facts.groupby(groupby_columns):
        aggregated_value = _aggregate_values(
          group_df, associations, aggregation_method, value_col
        )

        aggregated_row = group_df.iloc[0].copy()
        aggregated_row["element_id"] = target_element
        aggregated_row["element_name"] = target_element.split(":")[-1]
        aggregated_row[value_col] = aggregated_value

        if "element_label" in aggregated_row:
          aggregated_row["element_label"] = target_element.split(":")[-1]

        aggregated_rows.append(aggregated_row)
    else:
      aggregated_value = _aggregate_values(
        source_facts, associations, aggregation_method, value_col
      )

      aggregated_row = source_facts.iloc[0].copy()
      aggregated_row["element_id"] = target_element
      aggregated_row["element_name"] = target_element.split(":")[-1]
      aggregated_row[value_col] = aggregated_value

      if "element_label" in aggregated_row:
        aggregated_row["element_label"] = target_element.split(":")[-1]

      aggregated_rows.append(aggregated_row)

  if not aggregated_rows:
    return df

  return pd.DataFrame(aggregated_rows)


def _aggregate_values(
  facts: pd.DataFrame,
  associations: List[ElementAssociation],
  method: AggregationMethod,
  value_col: str,
) -> float:
  """Helper function to aggregate values based on method."""
  if method == AggregationMethod.SUM:
    return facts[value_col].sum()

  elif method == AggregationMethod.AVERAGE:
    return facts[value_col].mean()

  elif method == AggregationMethod.WEIGHTED_AVERAGE:
    weights_map = {assoc.source_element: assoc.weight for assoc in associations}
    facts_with_weights = facts.copy()
    facts_with_weights["weight"] = facts_with_weights["element_id"].map(weights_map)
    facts_with_weights["weighted_value"] = (
      facts_with_weights[value_col] * facts_with_weights["weight"]
    )
    total_weight = facts_with_weights["weight"].sum()
    if total_weight == 0:
      return 0.0
    return facts_with_weights["weighted_value"].sum() / total_weight

  elif method == AggregationMethod.FIRST:
    return facts[value_col].iloc[0]

  elif method == AggregationMethod.LAST:
    return facts[value_col].iloc[-1]

  elif method == AggregationMethod.CALCULATED:
    return facts[value_col].sum()

  return facts[value_col].sum()
