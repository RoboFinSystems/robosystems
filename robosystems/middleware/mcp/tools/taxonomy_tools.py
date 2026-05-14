"""Taxonomy mapping read tools for CoA → GAAP mapping workflows.

Four read-side tools; writes (`create-mapping-association`,
`delete-mapping-association`, `create-associations`, etc.) are
registrar-generated from the roboledger OperationSpec declarations.

1. list-mapping-structures: List `coa_mapping` structures with their OLTP ids
2. get-unmapped-elements: List CoA elements not yet mapped to reporting taxonomy
3. suggest-mapping: Query reporting taxonomy for matching concepts by classification
4. get-mapping-summary: Get mapping coverage stats

All four route through `operations/roboledger/reads/taxonomies.py` so
MCP, GraphQL, and the REST read surface share one source of truth.
"""

from typing import Any

from robosystems.db.extensions import extensions_session
from robosystems.logger import logger
from robosystems.models.api.extensions.taxonomies import (
  CreateMappingAssociationOperation,
)
from robosystems.operations.roboledger.commands.taxonomies import (
  create_mapping_association,
)
from robosystems.operations.roboledger.reads.taxonomies import (
  count_coa_elements,
  expand_to_rs_gaap_candidates,
  get_element,
  get_mapping_coverage,
  list_mappings,
  list_unmapped_elements,
  suggest_mapping_candidates,
)


class ListMappingStructuresTool:
  """List the `coa_mapping` Structure rows on the tenant graph.

  Solves the discoverability gap surfaced during the 2026-05-12 demo
  walk: the OLTP `coa_mapping` Structure isn't materialized to the
  Cypher graph, so an MCP-only agent had no way to find its id without
  dropping to direct psql access. With this tool, the standard mapping
  workflow (`get-mapping-summary`, `get-unmapped-elements`,
  `suggest-mapping`, `create-mapping-association`) can be driven
  entirely through MCP.
  """

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "list-mapping-structures",
      "description": """List the `coa_mapping` Structures on the tenant graph.

**WHEN TO USE:**
- At the start of any mapping workflow, to discover the active mapping
  structure id without dropping out of the typed MCP surface
- Before calling `get-mapping-summary`, `get-unmapped-elements`, or
  `create-mapping-association` — each of those takes a `mapping_id`
  that you get from here

**RETURNS:**
List of mapping structures with id, name, structure_type, taxonomy_id.
Most tenants have exactly one `coa_mapping` structure ("CoA to US GAAP
Mapping"); deployments that target multiple taxonomies (fac + rs-gaap)
will have several.""",
      "inputSchema": {
        "type": "object",
        "properties": {},
        "required": [],
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    graph_id = self.client.graph_id

    try:
      with extensions_session(graph_id) as session:
        result = list_mappings(session)
        return {
          "structures": [
            {
              "id": s.id,
              "name": s.name,
              "structure_type": s.structure_type,
              "taxonomy_id": s.taxonomy_id,
            }
            for s in result.structures
          ],
          "count": len(result.structures),
        }
    except Exception as exc:
      logger.warning(f"list-mapping-structures failed: {exc}")
      return {"error": str(exc)}


class GetUnmappedElementsTool:
  """List CoA elements not yet mapped to the reporting taxonomy."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "get-unmapped-elements",
      "description": """List Chart of Accounts elements that are not yet mapped to US GAAP reporting concepts.

**WHEN TO USE:**
- At the start of a CoA → GAAP mapping workflow
- To check which accounts still need mapping
- To track progress during an iterative mapping session

**RETURNS:**
- List of unmapped CoA elements with id, code, name, classification, balance_type
- EFS trait (asset/liability/equity/revenue/expense) helps narrow the target reporting concept

**WORKFLOW:**
1. Call this tool to see unmapped elements
2. For each element, use suggest-mapping to find matching reporting concepts
3. Use create-mapping-association to write confirmed mappings
4. Call get-mapping-summary to check overall coverage""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "mapping_id": {
            "type": "string",
            "description": "Optional: specific mapping structure ID to check against. If omitted, checks all mapping structures.",
          },
        },
        "required": [],
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    graph_id = self.client.graph_id
    mapping_id = arguments.get("mapping_id")

    try:
      with extensions_session(graph_id) as session:
        unmapped = list_unmapped_elements(session, mapping_id=mapping_id)
        total_coa = count_coa_elements(session)
        return {
          "unmapped_count": len(unmapped),
          "total_coa_elements": total_coa,
          "elements": [e.model_dump(mode="json") for e in unmapped],
        }
    except Exception as exc:
      logger.warning(f"get-unmapped-elements failed: {exc}")
      return {"error": str(exc)}


class SuggestMappingTool:
  """Query reporting taxonomy for matching concepts by classification."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "suggest-mapping",
      "description": """Find matching rs-gaap reporting concepts for a CoA element.

**WHEN TO USE:**
- When deciding which reporting concept a CoA account should map to
- Narrows by classification (asset→Assets subtree, revenue→Revenues subtree, etc.)

**HOW IT WORKS:**
- Queries rs-gaap elements filtered by the EFS trait (FASB
  elementsOfFinancialStatements) matching the source element's
  classification.
- Restricts to concepts that appear in at least one rs-gaap-presentation
  Network — any picked target is guaranteed to render on the standard
  BS / IS / CF / SE reports under the active Reporting Style.
- Excludes statement-level subtotals whose value comes from rendering
  (e.g., rs-gaap:Assets, rs-gaap:LiabilitiesCurrent) — those are
  computed by the calc DAG, not by a leaf fact.

**TIPS:**
- The classification filter (asset / liability / equity / revenue /
  expense / gain / loss) narrows candidates by ~80%.
- Depth 1 = high-level line items; depth 2+ = detail items.
- rs-gaap is the canonical render target (per §3.2 Reporting Style);
  no longer a target_taxonomy parameter — only one taxonomy renders.""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "element_id": {
            "type": "string",
            "description": "The CoA element ID to find mapping candidates for",
          },
          "classification": {
            "type": "string",
            "description": "Override classification filter (asset, liability, equity, revenue, expense). Defaults to the element's own classification.",
          },
        },
        "required": ["element_id"],
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    graph_id = self.client.graph_id
    element_id = arguments["element_id"]
    classification_override = arguments.get("classification")

    try:
      with extensions_session(graph_id) as session:
        source = get_element(session, element_id)
        if source is None:
          return {"error": f"Element {element_id} not found"}

        classification = classification_override or source.trait
        if not classification:
          return {
            "source_element": {
              "id": source.id,
              "code": source.code,
              "name": source.name,
              "trait": source.trait,
            },
            "candidates": [],
          }

        candidates = suggest_mapping_candidates(
          session, trait=classification, element_id=element_id
        )

        return {
          "source_element": {
            "id": source.id,
            "code": source.code,
            "name": source.name,
            "trait": source.trait,
          },
          "candidates": [
            {
              "id": c.id,
              "qname": c.qname,
              "name": c.name,
              "trait": c.trait,
              "is_abstract": c.is_abstract,
              "depth": c.depth,
              "source": c.source,
            }
            for c in candidates
          ],
        }
    except Exception as exc:
      logger.warning(f"suggest-mapping failed: {exc}")
      return {"error": str(exc)}


class GetMappingSummaryTool:
  """Get mapping coverage statistics."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "get-mapping-summary",
      "description": """Get coverage statistics for account rollups (CoA → GAAP mapping).

**WHEN TO USE:**
- To check progress during a mapping session
- To verify account rollups are complete before closing the books
- To see how many accounts are mapped vs unmapped
- To review confidence distribution of AI-suggested mappings

**RETURNS:**
- Total CoA elements, mapped count, unmapped count, coverage percentage
- Confidence distribution: high (>0.90), medium (0.70-0.90), low (<0.70)""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "mapping_id": {
            "type": "string",
            "description": "The mapping structure ID to check coverage for",
          },
        },
        "required": ["mapping_id"],
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    graph_id = self.client.graph_id
    mapping_id = arguments["mapping_id"]

    try:
      with extensions_session(graph_id) as session:
        coverage = get_mapping_coverage(session, mapping_id)

        return {
          "mapping_id": coverage.mapping_id,
          "total_coa_elements": coverage.total_coa_elements,
          "mapped_count": coverage.mapped_count,
          "unmapped_count": coverage.unmapped_count,
          "coverage_percent": round(coverage.coverage_percent, 1),
          "confidence_distribution": {
            "high": coverage.high_confidence,
            "medium": coverage.medium_confidence,
            "low": coverage.low_confidence,
            "manual": (
              coverage.mapped_count
              - coverage.high_confidence
              - coverage.medium_confidence
              - coverage.low_confidence
            ),
          },
          "unreachable_count": coverage.unreachable_count,
          "unreachable": [
            {
              "coa_code": u.coa_code,
              "coa_name": u.coa_name,
              "target_qname": u.target_qname,
            }
            for u in coverage.unreachable
          ],
        }
    except Exception as exc:
      logger.warning(f"get-mapping-summary failed: {exc}")
      return {"error": str(exc)}


class ExpandToRsGaapCandidatesTool:
  """Follow FAC → rs-gaap equivalence + type-subtype to get specific filing candidates."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "expand-to-rs-gaap-candidates",
      "description": """Follow a FAC element's fac-to-rs-gaap equivalence arc to its
rs-gaap parent, then return type-subtype children as specific filing-level candidates.

**WHEN TO USE:**
- After confirming a CoA → FAC mapping (second pass)
- To get the most specific rs-gaap tag for a CoA account

**RETURNS:**
- rs_gaap_parent: the direct rs-gaap equivalent of the FAC concept
- candidates: type-subtype children (more specific filing variants)""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "fac_element_id": {
            "type": "string",
            "description": "The FAC element ID from the first mapping pass",
          },
        },
        "required": ["fac_element_id"],
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    graph_id = self.client.graph_id
    fac_element_id = arguments["fac_element_id"]
    try:
      with extensions_session(graph_id) as session:
        result = expand_to_rs_gaap_candidates(session, fac_element_id)
        if result is None:
          return {"error": f"No fac-to-rs-gaap equivalence found for {fac_element_id}"}
        return result
    except Exception as exc:
      logger.warning(f"expand-to-rs-gaap-candidates failed: {exc}")
      return {"error": str(exc)}


class CreateMappingAssociationTool:
  """Write a CoA → FAC mapping association."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "create-mapping-association",
      "description": """Write a confirmed CoA → target mapping association.

**WHEN TO USE:**
- After choosing the best FAC candidate for a CoA element (primary pass; `association_type='mapping'`)
- Or after refining a FAC-classified account to a specific rs-gaap tag (`association_type='equivalence'`)
- Confidence ≥ 0.70 is required; skip below that threshold

**INPUTS:**
- mapping_id: The coa_mapping structure ID (from get-unmapped-elements)
- from_element_id: CoA element ID (source)
- to_element_id: Target element ID (FAC for primary, rs-gaap for equivalence)
- confidence: Float 0.0–1.0 (AI confidence in the match)
- association_type: 'mapping' (primary FAC rollup) or 'equivalence' (rs-gaap refinement). Defaults to 'mapping'.""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "mapping_id": {
            "type": "string",
            "description": "The coa_mapping structure ID",
          },
          "from_element_id": {
            "type": "string",
            "description": "CoA element ID (source)",
          },
          "to_element_id": {
            "type": "string",
            "description": "Target element ID (FAC for primary, rs-gaap for equivalence)",
          },
          "confidence": {
            "type": "number",
            "description": "Confidence score 0.0–1.0",
          },
          "association_type": {
            "type": "string",
            "enum": ["mapping", "equivalence"],
            "description": "'mapping' for primary FAC rollup, 'equivalence' for rs-gaap refinement. Defaults to 'mapping'.",
          },
        },
        "required": ["mapping_id", "from_element_id", "to_element_id", "confidence"],
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    graph_id = self.client.graph_id
    try:
      body = CreateMappingAssociationOperation(
        mapping_id=arguments["mapping_id"],
        from_element_id=arguments["from_element_id"],
        to_element_id=arguments["to_element_id"],
        confidence=float(arguments["confidence"]),
        association_type=arguments.get("association_type", "mapping"),
        suggested_by="mapping-agent",
      )
      with extensions_session(graph_id) as session:
        result = create_mapping_association(session, body, created_by="mapping-agent")
        return {
          "association_id": result.id,
          "from_element_id": result.from_element_id,
          "to_element_id": result.to_element_id,
          "confidence": result.confidence,
        }
    except Exception as exc:
      logger.warning(f"create-mapping-association failed: {exc}")
      return {"error": str(exc)}
