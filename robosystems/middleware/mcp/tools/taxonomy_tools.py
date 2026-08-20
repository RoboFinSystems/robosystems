"""Taxonomy mapping read tools for CoA → GAAP mapping workflows.

Four read-side tools, all registered in `manager.py`:

1. list-mapping-structures: List `coa_mapping` structures with their OLTP ids
2. get-unmapped-elements: List CoA elements not yet mapped to reporting taxonomy
3. suggest-mapping: Query reporting taxonomy for matching concepts by classification
4. get-mapping-summary: Get mapping coverage stats

All four route through `operations/roboledger/reads/taxonomies.py` so
MCP, GraphQL, and the REST read surface share one source of truth.

Mapping **writes** are registrar-generated from the roboledger
`OperationSpec` declarations (`create-mapping-association`,
`delete-mapping-association`, …) — one registration mounts both the REST
route and the MCP tool.

`CreateMappingAssociationTool` below shares a name with one of those
operations anyway, and that is load-bearing rather than duplication.
**There are two dispatchers, and only one of them knows about the
registrar.** `GraphMCPTools.call_tool` resolves registrar tools first
(Layer 0), so a remote MCP client never reaches the class. But
`DirectToolAccess` — the worker path, used by `MappingOperator` and hence
by `auto-map-elements` — instantiates tool classes in process and calls
`.execute()` on them directly, never consulting the registrar at all.

So "the registrar publishes this name" does **not** imply "this class is
dead". Check `operations/operators/tool_access.py` before deleting a tool
class on that reasoning; deleting this one broke the worker mapping path.
"""

from typing import Any

from sqlalchemy.exc import SQLAlchemyError

from robosystems.db.extensions import extensions_session
from robosystems.logger import logger
from robosystems.middleware.operations import run_off_loop
from robosystems.models.api.extensions.taxonomies import (
  CreateMappingAssociationOperation,
)
from robosystems.operations.extensions.staleness import mark_graph_stale
from robosystems.operations.roboledger.commands.taxonomies import (
  create_mapping_association,
)
from robosystems.operations.roboledger.reads.taxonomies import (
  count_coa_elements,
  get_element,
  get_mapping_coverage,
  list_mappings,
  list_unmapped_elements,
  suggest_mapping_candidates,
)

from ._errors import database_failure


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
List of mapping structures with id, name, block_type, taxonomy_id.
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
    return await run_off_loop(self._execute_sync, arguments)

  def _execute_sync(self, arguments: dict[str, Any]) -> Any:
    graph_id = self.client.graph_id

    try:
      with extensions_session(graph_id) as session:
        result = list_mappings(session)
        return {
          "structures": [
            {
              "id": s.id,
              "name": s.name,
              "block_type": s.block_type,
              "taxonomy_id": s.taxonomy_id,
            }
            for s in result.structures
          ],
          "count": len(result.structures),
        }
    except SQLAlchemyError as exc:
      return database_failure("list-mapping-structures", exc)
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
- liquidity (current/noncurrent, assets/liabilities only) narrows further to the right BS section

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
    return await run_off_loop(self._execute_sync, arguments)

  def _execute_sync(self, arguments: dict[str, Any]) -> Any:
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
    except SQLAlchemyError as exc:
      return database_failure("get-unmapped-elements", exc)
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
- rs-gaap is the canonical render target (per the active Reporting
  Style); there is no target_taxonomy parameter — only one taxonomy
  renders.""",
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
          "liquidity": {
            "type": "string",
            "enum": ["current", "noncurrent"],
            "description": "Optional liquidity narrowing (assets/liabilities). Drops candidates of the opposite liquidity. Defaults to the element's own liquidity trait.",
          },
        },
        "required": ["element_id"],
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    return await run_off_loop(self._execute_sync, arguments)

  def _execute_sync(self, arguments: dict[str, Any]) -> Any:
    from robosystems.operations.roboledger.reports.network_picker import (
      load_primary_reporting_style,
    )

    graph_id = self.client.graph_id
    element_id = arguments["element_id"]
    classification_override = arguments.get("classification")

    try:
      with extensions_session(graph_id) as session:
        # Resolve the entity's active Reporting Style so candidate filtering
        # matches what the renderer walks (not the wider rs-gaap-presentation
        # taxonomy). Resolved from this same session; soft-fail to the wider
        # filter when the tenant has no entity yet.
        try:
          reporting_style_id = load_primary_reporting_style(session)
        except LookupError:
          reporting_style_id = None

        source = get_element(session, element_id)
        if source is None:
          return {"error": f"Element {element_id} not found"}

        classification = classification_override or source.trait
        # Liquidity: explicit override (the auto-map agent passes the CoA
        # element's liquidity) else derive from the element's own trait so
        # manual MCP calls narrow too. None → EFS-only (backward compatible).
        liquidity = arguments.get("liquidity")
        if liquidity is None:
          from robosystems.operations.library.reads import liquidity_by_element

          liquidity = liquidity_by_element(session, [element_id]).get(element_id)
        if not classification:
          return {
            "source_element": {
              "id": source.id,
              "code": source.code,
              "name": source.name,
              "trait": source.trait,
              "liquidity": liquidity,
            },
            "candidates": [],
          }

        candidates = suggest_mapping_candidates(
          session,
          trait=classification,
          element_id=element_id,
          reporting_style_id=reporting_style_id,
          liquidity=liquidity,
        )

        return {
          "source_element": {
            "id": source.id,
            "code": source.code,
            "name": source.name,
            "trait": source.trait,
            "liquidity": liquidity,
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
    except SQLAlchemyError as exc:
      return database_failure("suggest-mapping", exc)
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
    return await run_off_loop(self._execute_sync, arguments)

  def _execute_sync(self, arguments: dict[str, Any]) -> Any:
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
    except SQLAlchemyError as exc:
      return database_failure("get-mapping-summary", exc)
    except Exception as exc:
      logger.warning(f"get-mapping-summary failed: {exc}")
      return {"error": str(exc)}


class CreateMappingAssociationTool:
  """Write a CoA → rs-gaap mapping association.

  Shares its name with the ``create-mapping-association`` registrar
  operation, and that is deliberate rather than duplication: the two serve
  different dispatchers. On the MCP wire, ``GraphMCPTools.call_tool``
  resolves registrar tools at Layer 0, so *this* class is not what a remote
  client reaches. On the worker path, ``DirectToolAccess`` instantiates tool
  classes in process and executes them directly — it never consults the
  registrar — so this class is the live write path for ``auto-map-elements``
  and every MappingOperator run.

  Consequence worth keeping: the ``mark_graph_stale`` call below is not
  redundant with the spec's ``mark_stale_reason``. That only fires on the
  registrar path; without the call here a worker mapping run would never
  reach LadybugDB.
  """

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "create-mapping-association",
      "description": """Write a confirmed CoA → rs-gaap mapping association.

**WHEN TO USE:**
- After choosing the best rs-gaap candidate for a CoA element (`association_type='mapping'` — the canonical arc the renderer follows)
- Confidence ≥ 0.70 is required; skip below that threshold

**INPUTS:**
- mapping_id: The coa_mapping structure ID (from get-unmapped-elements)
- from_element_id: CoA element ID (source)
- to_element_id: rs-gaap element ID (the reporting concept)
- confidence: Float 0.0–1.0 (AI confidence in the match)
- association_type: 'mapping' (CoA→rs-gaap, the primary arc the renderer follows) or 'equivalence' (alternate cross-taxonomy arc). Defaults to 'mapping'.""",
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
            "description": "rs-gaap element ID (the reporting concept)",
          },
          "confidence": {
            "type": "number",
            "description": "Confidence score 0.0–1.0",
          },
          "association_type": {
            "type": "string",
            "enum": ["mapping", "equivalence"],
            "description": "'mapping' = CoA→rs-gaap (primary, what the renderer follows); 'equivalence' = alternate cross-taxonomy arc. Defaults to 'mapping'.",
          },
        },
        "required": ["mapping_id", "from_element_id", "to_element_id", "confidence"],
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    return await run_off_loop(self._execute_sync, arguments)

  def _execute_sync(self, arguments: dict[str, Any]) -> Any:
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
      # `suggested_by` names the suggester (the mapping operator); `created_by`
      # is the accountable user — the caller when one is threaded through,
      # otherwise the operator's own tag.
      created_by = str(getattr(self.client, "user_id", None) or "mapping-agent")
      with extensions_session(graph_id) as session:
        result = create_mapping_association(session, body, created_by=created_by)
      # The registrar-published tools get this from `OperationSpec`; this one
      # is hand-written and reaches the command directly, so it has to mark
      # the graph itself. Associations are materialized, and `auto-map-elements`
      # writes its whole mapping run through this tool — without the mark the
      # operator's work never reaches LadybugDB.
      mark_graph_stale(graph_id, "mapping_association_created")
      return {
        "association_id": result.id,
        "from_element_id": result.from_element_id,
        "to_element_id": result.to_element_id,
        "confidence": result.confidence,
      }
    except SQLAlchemyError as exc:
      return database_failure("create-mapping-association", exc)
    except Exception as exc:
      logger.warning(f"create-mapping-association failed: {exc}")
      return {"error": str(exc)}
