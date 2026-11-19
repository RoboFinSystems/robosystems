"""CoA → GAAP mapping tools.

The reads are hand-written; mapping writes are registrar-generated, except
`CreateMappingAssociationTool` (see its docstring).
"""

from typing import Any

from sqlalchemy.exc import SQLAlchemyError

from robosystems.db.extensions import LIBRARY_GRAPH_ID, extensions_session
from robosystems.logger import logger
from robosystems.middleware.operations import run_off_loop
from robosystems.models.api.extensions.taxonomies import (
  CreateMappingAssociationOperation,
)
from robosystems.operations.extensions.staleness import mark_graph_stale
from robosystems.operations.library import reads as library_reads
from robosystems.operations.library.reads.elements import (
  DEFAULT_ELEMENT_LIMIT,
  MAX_ELEMENT_LIMIT,
)
from robosystems.operations.roboledger.commands.taxonomies import (
  create_mapping_association,
)
from robosystems.operations.roboledger.entity_scope import EntityNotInGraphError
from robosystems.operations.roboledger.reads.taxonomies import (
  count_coa_elements,
  get_element,
  get_mapping_coverage,
  list_mappings,
  list_unmapped_elements,
  suggest_mapping_candidates,
)
from robosystems.operations.taxonomy_block.coa_mappings import mapping_owner_id
from robosystems.operations.taxonomy_block.immutability import ProtectedFactsError

from ._arguments import bounded_int, invalid, unexpected
from ._errors import database_failure
from .constants import ENTITY_ID_ARGUMENT


class ListMappingStructuresTool:
  """List `coa_mapping` Structures, which are not materialized to the graph."""

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
List of mapping structures with id, name, block_type, taxonomy_id and
framework — the reporting framework each maps the chart into. A chart
holds one mapping per framework; the book mapping (`rs-gaap`) is listed
first. On a group, the mappings of one entity's chart: `entity_id` names
it, default the group parent.""",
      "inputSchema": {
        "type": "object",
        "properties": {"entity_id": ENTITY_ID_ARGUMENT},
        "required": [],
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    return await run_off_loop(self._execute_sync, arguments)

  def _execute_sync(self, arguments: dict[str, Any]) -> Any:
    graph_id = self.client.graph_id

    try:
      with extensions_session(graph_id) as session:
        result = list_mappings(session, arguments.get("entity_id"))
        return {
          "structures": [
            {
              "id": s.id,
              "name": s.name,
              "block_type": s.block_type,
              "taxonomy_id": s.taxonomy_id,
              "framework": s.framework,
            }
            for s in result.structures
          ],
          "count": len(result.structures),
        }
    except EntityNotInGraphError as exc:
      return {"error": "entity_not_found", "message": str(exc)}
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
- On a group, one entity's chart: `entity_id` names it (or `mapping_id` does, through the entity whose chart it maps); default the group parent's
- liquidity (current/noncurrent, assets/liabilities only) narrows further to the right BS section

**WORKFLOW:**
1. Call this tool to see unmapped elements
2. For each element, use suggest-mapping to find matching reporting concepts
3. Use create-mapping-association to write confirmed mappings
4. Call get-mapping-summary to check overall coverage
5. To move an account that is already mapped, create-mapping-association with replaces_association_id (its current arc): one write, judged as a reclassification when closed months are involved""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "mapping_id": {
            "type": "string",
            "description": (
              "Optional: the mapping structure to check against. It names the "
              "entity whose chart is read, over `entity_id`; omit it to read "
              "`entity_id`'s chart."
            ),
          },
          "entity_id": ENTITY_ID_ARGUMENT,
        },
        "required": [],
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    return await run_off_loop(self._execute_sync, arguments)

  def _execute_sync(self, arguments: dict[str, Any]) -> Any:
    graph_id = self.client.graph_id
    mapping_id = arguments.get("mapping_id")
    entity_id = arguments.get("entity_id")

    try:
      with extensions_session(graph_id) as session:
        # A mapping names the entity whose chart it maps, and that is the
        # chart the list reads; the count follows it, whatever entity the
        # caller also named, so the two numbers describe one chart.
        if mapping_id:
          entity_id = mapping_owner_id(session, mapping_id)
        unmapped = list_unmapped_elements(
          session, mapping_id=mapping_id, entity_id=entity_id
        )
        total_coa = count_coa_elements(session, entity_id=entity_id)
        return {
          "unmapped_count": len(unmapped),
          "total_coa_elements": total_coa,
          "elements": [e.model_dump(mode="json") for e in unmapped],
        }
    except EntityNotInGraphError as exc:
      return {"error": "entity_not_found", "message": str(exc)}
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

**RETURNS:** Candidate rs-gaap concepts with element id, qname, label and
depth. Every candidate appears in at least one rs-gaap-presentation Network,
so any target picked from here is guaranteed to render on the standard
BS / IS / CF / SE reports under the active Reporting Style.

**NOTES:**
- Candidates are filtered by the EFS trait (FASB
  elementsOfFinancialStatements) matching the source element's
  classification, which narrows by roughly 80%.
- Statement-level subtotals are excluded (e.g. rs-gaap:Assets,
  rs-gaap:LiabilitiesCurrent) — the calc DAG computes those, not a leaf fact.
- Depth 1 is high-level line items; depth 2+ is detail.
- rs-gaap is the only render target, so there is no target_taxonomy
  parameter.""",
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
    from robosystems.adapters.bank_feed.accounts import account_entities
    from robosystems.operations.roboledger.entity_scope import resolve_entity_id
    from robosystems.operations.roboledger.reports.network_picker import (
      load_entity_reporting_style,
    )

    graph_id = self.client.graph_id
    element_id = arguments["element_id"]
    classification_override = arguments.get("classification")

    try:
      with extensions_session(graph_id) as session:
        source = get_element(session, element_id)
        if source is None:
          return {"error": f"Element {element_id} not found"}

        # Filter candidates by the Reporting Style the account's own entity
        # renders under, as the renderer walks; the wider taxonomy when the
        # tenant has no entity yet.
        owner = account_entities(session, [element_id], parent_id=None).get(element_id)
        try:
          reporting_style_id = load_entity_reporting_style(
            session, resolve_entity_id(session, owner)
          )
        except LookupError:
          reporting_style_id = None

        classification = classification_override or source.trait
        # Default to the element's own liquidity; None narrows by EFS only.
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
            "manual": coverage.manual_confidence,
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


class ListElementArcsTool:
  """The mapping arcs an element is an end of, each with its association id."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "list-element-arcs",
      "description": """The mapping arcs an element is an end of: an account's arc to its reporting concept, and the cross-taxonomy arcs of a concept, each with its association id.

**WHEN TO USE:**
- Before `delete-mapping-association`, or `create-mapping-association` with `replaces_association_id`: the id of the account's current arc
- "What does this account map to?" or "which accounts map to this concept?"

**PARAMETERS:**
- element_id (required): the account or concept (element id)

**RETURNS:**
`arcs`: each with `id` (the association id), `direction` (`outgoing` when this element is the source, `incoming` when it is the target), `association_type`, `arcrole`, the structure and taxonomy it belongs to, and `peer`, the element at the other end with its `qname`, `name` and `trait`.

**RELATED TOOLS:**
- create-mapping-association / delete-mapping-association — move or remove an arc
- suggest-mapping — candidate concepts for an account""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "element_id": {
            "type": "string",
            "description": "The account or concept (element id).",
          },
        },
        "required": ["element_id"],
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    return await run_off_loop(self._execute_sync, arguments)

  def _execute_sync(self, arguments: dict[str, Any]) -> Any:
    element_id = arguments.get("element_id")
    if not isinstance(element_id, str) or not element_id:
      return {"error": "invalid_arguments", "message": "element_id is required"}
    try:
      with extensions_session(self.client.graph_id) as session:
        arcs = library_reads.get_element_arcs(session, element_id=element_id)
        return {
          "element_id": element_id,
          "arcs": [arc.model_dump(mode="json") for arc in arcs],
        }
    except SQLAlchemyError as exc:
      return database_failure("list-element-arcs", exc)
    except Exception:
      return unexpected("list-element-arcs")


def _brief(element: Any) -> dict[str, Any]:
  """A concept in one short row: enough to choose it, not to cite it."""
  label = next(
    (row.text for row in element.labels if row.role == "standard"), element.name
  )
  return {
    "id": element.id,
    "qname": element.qname,
    "label": label,
    "balance_type": element.balance_type,
    "period_type": element.period_type,
    "trait": element.trait,
    "is_abstract": element.is_abstract,
  }


class SearchLibraryElementsTool:
  """Reporting concepts and accounts found by a phrase or a qname."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "search-library-elements",
      "description": """Find taxonomy elements by a phrase or a qname: reporting concepts (rs-gaap and the other standards) and this graph's own accounts, matched against qname, name and label.

**WHEN TO USE:**
- Turning a qname (`rs-gaap:ProceedsFromIssuanceOfLongTermDebt`) or a phrase ("deferred revenue") into an element id for a mapping, a flow or a schedule
- Checking which reporting concept an idea is called before mapping to it

**PARAMETERS:**
- query (required): a substring of the qname, name or label text
- source (optional): one standard only, e.g. `rs-gaap`
- whole_library (optional, default false): search every concept the reporting library has, not only the ones this graph already holds. Use it to name a line of a report being received with `ingest-report`; a mapping or a schedule needs a concept this graph holds
- brief (optional, default false): return one short row per concept (`id`, `qname`, `label`, `balance_type`, `period_type`, `trait`, `is_abstract`) without documentation and references. Use it when choosing among many candidates
- limit (optional, default 50, 1 to 500)

**RETURNS:**
`elements`, by qname: each with `id`, `qname`, `namespace`, `name`, `trait`, `balance_type`, `period_type`, `is_abstract`, `is_monetary`, `element_type`, `source`, `taxonomy_id`, `parent_id`, `labels` and `references`; with `brief`, the short row.

**RELATED TOOLS:**
- suggest-mapping — ranked concepts for one account
- list-element-arcs — what an element maps to""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "query": {
            "type": "string",
            "description": "Substring of the qname, name or label text.",
          },
          "source": {"type": "string", "description": "One standard, e.g. rs-gaap."},
          "whole_library": {
            "type": "boolean",
            "default": False,
            "description": (
              "Search the whole reporting library rather than this graph's own "
              "working set."
            ),
          },
          "brief": {
            "type": "boolean",
            "default": False,
            "description": "One short row per concept, without documentation and references.",
          },
          "limit": {
            "type": "integer",
            "minimum": 1,
            "maximum": MAX_ELEMENT_LIMIT,
            "default": DEFAULT_ELEMENT_LIMIT,
          },
        },
        "required": ["query"],
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    return await run_off_loop(self._execute_sync, arguments)

  def _execute_sync(self, arguments: dict[str, Any]) -> Any:
    query = arguments.get("query")
    if not isinstance(query, str) or not query.strip():
      return {"error": "invalid_arguments", "message": "query is required"}
    try:
      limit = bounded_int(
        arguments, "limit", default=DEFAULT_ELEMENT_LIMIT, low=1, high=MAX_ELEMENT_LIMIT
      )
    except ValueError as exc:
      return invalid(str(exc))
    whole_library = arguments.get("whole_library", False)
    if not isinstance(whole_library, bool):
      return invalid("whole_library must be true or false")
    brief = arguments.get("brief", False)
    if not isinstance(brief, bool):
      return invalid("brief must be true or false")
    scope = LIBRARY_GRAPH_ID if whole_library else self.client.graph_id
    try:
      with extensions_session(scope) as session:
        elements = library_reads.search_elements(
          session,
          query_text=query,
          limit=limit,
          source=arguments.get("source") or None,
        )
        if brief:
          return {"elements": [_brief(e) for e in elements]}
        return {"elements": [e.model_dump(mode="json") for e in elements]}
    except SQLAlchemyError as exc:
      return database_failure("search-library-elements", exc)
    except Exception:
      return unexpected("search-library-elements")


class CreateMappingAssociationTool:
  """Write a CoA → rs-gaap mapping association.

  Shadowed on the MCP wire by the registrar op of the same name, but not
  dead: ``DirectToolAccess`` executes tool classes directly, and
  ``HttpToolAccess`` instantiates it to read the name. Check
  ``operations/operators/tool_access.py`` before deleting.
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

**PARAMETERS:**
- mapping_id: The coa_mapping structure ID (from get-unmapped-elements)
- from_element_id: CoA element ID (source)
- to_element_id: rs-gaap element ID (the reporting concept)
- confidence: Float 0.0–1.0 (AI confidence in the match)
- association_type: 'mapping' (CoA→rs-gaap, the primary arc the renderer follows) or 'equivalence' (alternate cross-taxonomy arc). Defaults to 'mapping'.
- replaces_association_id: the account's current arc, removed in the same write. An account with landed history in a closed month moves only this way, and only within its primary statement (a reclassification); across one it is refused.
- reason: why the account is reclassified, kept on the new arc.

**RETURNS:** The written association's id, the arc it replaced, and the closed months it reclassified.""",
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
          "replaces_association_id": {
            "type": "string",
            "description": "The account's current mapping arc in this structure, removed in the same write.",
          },
          "reason": {
            "type": "string",
            "description": "Why the account is reclassified; recorded on the new arc.",
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
        replaces_association_id=arguments.get("replaces_association_id"),
        reason=arguments.get("reason"),
      )
      # `created_by` is the accountable user, else the operator's own tag.
      created_by = str(getattr(self.client, "user_id", None) or "mapping-agent")
      with extensions_session(graph_id) as session:
        result = create_mapping_association(session, body, created_by=created_by)
      # The registrar path marks stale via `OperationSpec`; this one must
      # do it itself or the association never reaches LadybugDB.
      mark_graph_stale(graph_id, "mapping_association_created")
      return {
        "association_id": result.id,
        "from_element_id": result.from_element_id,
        "to_element_id": result.to_element_id,
        "confidence": result.confidence,
        "replaced_association_id": result.replaced_association_id,
        "reclassified_periods": result.reclassified_periods,
      }
    except ProtectedFactsError as exc:
      # The arc would restate a closed month. Typed so the mapping operator
      # can count it apart from an ordinary rejection.
      return {
        "error": "protected_history",
        "message": str(exc),
        "closed_periods": list(exc.closed_period_names),
      }
    except SQLAlchemyError as exc:
      return database_failure("create-mapping-association", exc)
    except Exception as exc:
      logger.warning(f"create-mapping-association failed: {exc}")
      return {"error": str(exc)}
