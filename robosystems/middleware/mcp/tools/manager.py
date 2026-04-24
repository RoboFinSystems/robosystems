"""
Graph MCP Tools - MCP tools implementation using Graph API.

This module contains the GraphMCPTools class which provides all the MCP tool
functionality for interacting with graph databases.

Tool availability is schema-driven:
- Core tools (cypher, schema) are always available
- Extension tools (financial statements, etc.) require matching schema_extensions
- Infrastructure tools (workspace, memory) are gated by feature flags

**Registrar-generated tools:** Extensions with an `OperationRegistrar`
(roboledger, roboinvestor) contribute their `OperationSpec`s as
auto-generated MCP tools via `registrar.build_tools_for_extension`. These
are looked up in `_registrar_dispatch` at call time BEFORE the hand-written
if/elif ladder. This lets the hand-written layer be pruned incrementally
as tools migrate, without a big-bang refactor.
"""

import json
from typing import TYPE_CHECKING, Any

from robosystems.config import env
from robosystems.logger import logger
from robosystems.middleware.mcp.query_validator import GraphQueryValidator

from ..exceptions import (
  GraphAPIError,
  GraphQueryComplexityError,
  GraphQueryTimeoutError,
  GraphValidationError,
)
from .cypher_tool import CypherTool
from .example_queries_tool import ExampleQueriesTool
from .graph_tools import (
  CreateBackupTool,
  CreateSubgraphTool,
  DeleteSubgraphTool,
  GetGraphSyncStatusTool,
  ListSubgraphsTool,
  MaterializeTool,
  SwitchWorkspaceTool,
)
from .memory_tools import AddNodeTableTool, AddRelationshipTableTool, WriteCypherTool
from .schema_tool import SchemaTool

if TYPE_CHECKING:
  from robosystems.middleware.extensions import GraphExtensionContext

  from .registrar import _RegistrarMCPTool


def resolve_schema_extensions(graph_id: str) -> list[str]:
  """Resolve schema extensions for a graph.

  For shared repositories: reads from the adapter manifest (no DB query needed).
  For user graphs: queries the PostgreSQL graphs table.

  Returns:
      List of extension names (e.g., ["roboledger"]), or empty list.
  """
  # Shared repos: resolve from manifest (in-memory, no DB needed)
  try:
    from robosystems.config.shared_repositories import (
      get_manifest,
      is_shared_repository_or_subgraph,
      resolve_shared_repository_parent,
    )

    if is_shared_repository_or_subgraph(graph_id):
      parent_id = resolve_shared_repository_parent(graph_id)
      manifest = get_manifest(parent_id)
      if manifest and manifest.schema_extensions:
        return list(manifest.schema_extensions)
      return []
  except Exception:
    logger.warning(f"Manifest lookup failed for {graph_id}, trying PostgreSQL")

  # User graph: query PostgreSQL
  try:
    from robosystems.database import get_db_session
    from robosystems.models.core import Graph

    db_gen = get_db_session()
    db = next(db_gen)
    try:
      graph = Graph.get_by_id(graph_id, db)
      if graph and graph.schema_extensions:
        return list(graph.schema_extensions)
    finally:
      try:
        next(db_gen)
      except StopIteration:
        pass
  except Exception:
    logger.warning(f"Could not resolve schema extensions for {graph_id}")

  return []


class GraphMCPTools:
  """
  MCP tools implementation using Graph API.

  Tool availability is layered:
  - Layer 1 (Core): cypher, schema — always available
  - Layer 2 (Schema): financial tools — only when schema_extensions includes "roboledger"
  - Layer 3 (Infrastructure): workspace, memory, data — gated by feature flags
  """

  def __init__(
    self,
    graph_client,
    schema_extensions: list[str] | tuple[str, ...] = (),
    read_only: bool = False,
  ):
    # Import here to avoid circular import
    from ..client import GraphMCPClient

    self.client: GraphMCPClient = graph_client
    self.schema_extensions: tuple[str, ...] = tuple(schema_extensions)
    self.read_only: bool = read_only

    # Initialize query validator
    self.validator = GraphQueryValidator()

    # Layer 1: Core tools (always available for any graph)
    self.cypher_tool = CypherTool(graph_client)
    self.schema_tool = SchemaTool(graph_client)

    # Layer 2: Schema extension tools (gated by schema_extensions)
    self.example_queries_tool = None
    self.live_financial_statement_tool = None
    self.financial_statement_analysis_tool = None
    self.resolve_element_tool = None
    self.resolve_structure_tool = None

    if self._has_extension("roboledger"):
      self.example_queries_tool = ExampleQueriesTool(graph_client)

      from .financial_statement_tools import (
        FinancialStatementAnalysisTool,
        LiveFinancialStatementTool,
      )

      # Graph-backed analytical view — works on shared-repo + materialized
      # tenant graphs alike. Always registered on roboledger graphs.
      self.financial_statement_analysis_tool = FinancialStatementAnalysisTool(
        graph_client
      )
      # OLTP-backed live statement — tenant entity graphs only. Skipped
      # on shared repos (no OLTP tenant schema) and on read-only graphs.
      if not self._is_shared_repository() and not read_only:
        self.live_financial_statement_tool = LiveFinancialStatementTool(graph_client)

      # Semantic enrichment tools (roboledger + manifest flag)
      if self._should_include_semantic_tools():
        from .resolve_element_tool import ResolveElementTool

        self.resolve_element_tool = ResolveElementTool(graph_client)

    # Layer 3: Graph lifecycle tools — subgraph navigation + lifecycle ops
    # mirroring a subset of the REST `/v1/graphs/{g}/operations/*` surface.
    # Writes are gated by `read_only`; `switch-workspace` is a client-side
    # sentinel so it stays always-available when the workspace feature
    # flag is set.
    #
    # Deliberately **not exposed on MCP** — both still live on REST for
    # humans:
    # - `change-tier`: 3-5 minute destructive EBS migration with billing
    #   implications and fail-on-downgrade semantics.
    # - `restore-backup`: overwrites live graph data with a historical
    #   snapshot; recovery from a mis-fired restore is very expensive.
    self.create_subgraph_tool = None
    self.delete_subgraph_tool = None
    self.list_subgraphs_tool = None
    self.switch_workspace_tool = None
    self.create_backup_tool = None
    if env.MCP_WORKSPACE_ENABLED:
      # Navigation tools (list / switch) — always available.
      self.list_subgraphs_tool = ListSubgraphsTool(graph_client)
      self.switch_workspace_tool = SwitchWorkspaceTool(graph_client)
      # Write tools — blocked on shared-repo or read-only graphs.
      if not read_only:
        self.create_subgraph_tool = CreateSubgraphTool(graph_client)
        self.delete_subgraph_tool = DeleteSubgraphTool(graph_client)
        self.create_backup_tool = CreateBackupTool(graph_client)

    self.write_cypher_tool = None
    self.add_node_table_tool = None
    self.add_relationship_table_tool = None
    if env.MCP_MEMORY_ENABLED and not read_only:
      self.write_cypher_tool = WriteCypherTool(graph_client)
      self.add_node_table_tool = AddNodeTableTool(graph_client)
      self.add_relationship_table_tool = AddRelationshipTableTool(graph_client)

    self.build_fact_grid_tool = None
    if env.FACT_GRID_ENABLED:
      from .fact_grid_tool import BuildFactGridTool

      self.build_fact_grid_tool = BuildFactGridTool(graph_client)

    # Layer 2: Materialization — `materialize` + `get-graph-sync-status`.
    # User entity graphs only (shared repos use their own pipeline and
    # don't track staleness). `materialize` replaces the legacy
    # `materialize-graph` tool and mirrors the REST body shape.
    self.get_graph_sync_status_tool = None
    self.materialize_tool = None
    if (
      self._has_extension("roboledger")
      and env.ROBOLEDGER_ENABLED
      and not self._is_shared_repository()
      and not read_only
    ):
      self.get_graph_sync_status_tool = GetGraphSyncStatusTool(graph_client)
      self.materialize_tool = MaterializeTool(graph_client)

    # Layer 2: Period-workflow read tools (gated by roboledger extension
    # + ROBOLEDGER_ENABLED). Schedule-specific reads were retired in
    # favour of the generic information_block tools (see below). Writes
    # are registrar-generated.
    self.get_period_close_status_tool = None
    self.list_period_drafts_tool = None
    # Fiscal calendar tools (same gate as schedule tools)
    self.get_fiscal_calendar_tool = None
    self.close_period_tool = None
    self.reopen_period_tool = None
    # Information Block read tools — the eventual unified replacement for
    # list-schedule-structures + get-schedule-facts. Both ship in parallel
    # until agent workflows migrate.
    self.get_information_block_tool = None
    self.list_information_blocks_tool = None
    if self._has_extension("roboledger") and env.ROBOLEDGER_ENABLED and not read_only:
      from .fiscal_calendar_tools import (
        ClosePeriodTool,
        GetFiscalCalendarTool,
        ReopenPeriodTool,
      )
      from .schedule_tools import (
        GetPeriodCloseStatusTool,
        ListPeriodDraftsTool,
      )

      # Period-workflow read tools (span multiple blocks; not
      # Information Block tools). Writes (create-schedule,
      # create-closing-entry, create-manual-closing-entry, truncate-schedule,
      # update-schedule, delete-schedule) are registrar-generated from the
      # roboledger OperationSpec declarations.
      #
      # Schedule-specific read tools (list-schedule-structures,
      # get-schedule-facts) were retired in favour of the generic
      # information_block_tools: `list-information-blocks` with
      # ``blockType="schedule"`` and `get-information-block`.
      self.get_period_close_status_tool = GetPeriodCloseStatusTool(graph_client)
      self.list_period_drafts_tool = ListPeriodDraftsTool(graph_client)
      self.get_fiscal_calendar_tool = GetFiscalCalendarTool(graph_client)
      self.close_period_tool = ClosePeriodTool(graph_client)
      self.reopen_period_tool = ReopenPeriodTool(graph_client)

    # Information Block reads are pure reads and must stay available on
    # read-only graphs. Same gate pattern as document_tools below:
    # extension + flag only, no read_only guard.
    self.get_information_block_tool = None
    self.list_information_blocks_tool = None
    if self._has_extension("roboledger") and env.ROBOLEDGER_ENABLED:
      from .information_block_tools import (
        GetInformationBlockTool,
        ListInformationBlocksTool,
      )

      self.get_information_block_tool = GetInformationBlockTool(graph_client)
      self.list_information_blocks_tool = ListInformationBlocksTool(graph_client)

    # Event Block reads (get-event-block, list-event-blocks) — same gate as
    # information block reads: available on read-only graphs.
    self.get_event_block_tool = None
    self.list_event_blocks_tool = None
    if self._has_extension("roboledger") and env.ROBOLEDGER_ENABLED:
      from .event_block_tools import GetEventBlockTool, ListEventBlocksTool

      self.get_event_block_tool = GetEventBlockTool(graph_client)
      self.list_event_blocks_tool = ListEventBlocksTool(graph_client)

    # Layer 2: Taxonomy mapping read tools (gated by roboledger extension +
    # ROBOLEDGER_ENABLED). Writes — create-mapping-association,
    # create-associations, update/delete-association — are
    # registrar-generated.
    self.get_unmapped_elements_tool = None
    self.suggest_mapping_tool = None
    self.get_mapping_summary_tool = None
    if self._has_extension("roboledger") and env.ROBOLEDGER_ENABLED and not read_only:
      from .taxonomy_tools import (
        GetMappingSummaryTool,
        GetUnmappedElementsTool,
        SuggestMappingTool,
      )

      self.get_unmapped_elements_tool = GetUnmappedElementsTool(graph_client)
      self.suggest_mapping_tool = SuggestMappingTool(graph_client)
      self.get_mapping_summary_tool = GetMappingSummaryTool(graph_client)

    # Layer 3: Text search tools (gated by SEMANTIC_SEARCH_ENABLED)
    self.search_documents_tool = None
    self.get_document_section_tool = None
    if env.SEMANTIC_SEARCH_ENABLED:
      from .search_tools import GetDocumentSectionTool, SearchDocumentsTool

      self.search_documents_tool = SearchDocumentsTool(graph_client)
      self.get_document_section_tool = GetDocumentSectionTool(graph_client)

    # Layer 3: Document management tools (user graphs only, not shared repos)
    # Shared repos (SEC) use OpenSearch directly — no PG document rows.
    # Read tools (list/get) are available on read-only user graphs.
    # Write tools (create/update) require writable user graphs.
    self.create_document_tool = None
    self.update_document_tool = None
    self.get_document_tool = None
    self.list_documents_tool = None
    if env.SEMANTIC_SEARCH_ENABLED and not self._is_shared_repository():
      from .document_tools import (
        CreateDocumentTool,
        GetDocumentTool,
        ListDocumentsTool,
        UpdateDocumentTool,
      )

      # Read tools — available on all user graphs (including read-only)
      self.get_document_tool = GetDocumentTool(graph_client)
      self.list_documents_tool = ListDocumentsTool(graph_client)

      # Write tools — only on writable graphs
      if not read_only:
        self.create_document_tool = CreateDocumentTool(graph_client)
        self.update_document_tool = UpdateDocumentTool(graph_client)

    # Cache statistics (inherited from schema tool)
    self._cache_hits = 0
    self._cache_misses = 0

    # ── Registrar-generated tools ──────────────────────────────────────
    # Auto-generate MCP tools from every OperationSpec declared for the
    # graph's enabled extensions. Keyed by tool name — checked before the
    # hand-written if/elif ladder in `call_tool` so migrations can drop
    # hand-written classes without touching the dispatch code.
    self._cached_meta: GraphExtensionContext | None = None
    self._registrar_dispatch: dict[str, _RegistrarMCPTool] = {}
    if not read_only:
      # Skip registrar wiring for shared repos; they never accept writes.
      from .registrar import build_tools_for_extension

      for ext in self.schema_extensions:
        self._registrar_dispatch.update(
          build_tools_for_extension(
            extension=ext,
            client=self.client,
            meta_getter=self._get_cached_meta,
          )
        )

    logger.info(
      f"Initialized Graph MCP tools (extensions={list(self.schema_extensions)}, "
      f"read_only={self.read_only}, registrar_tools={len(self._registrar_dispatch)})"
    )

  def _get_cached_meta(self) -> "GraphExtensionContext | None":
    """Lazy accessor for platform DB graph metadata.

    First call opens a short-lived session, caches the result on the
    handler instance. Subsequent registrar-tool calls within the same
    handler lifetime hit the cache. On any load failure, returns None so
    the tool's own gate path handles it.
    """
    if self._cached_meta is not None:
      return self._cached_meta
    try:
      from robosystems.database import get_db_session
      from robosystems.middleware.extensions import load_graph_metadata

      db_gen = get_db_session()
      session = next(db_gen)
      try:
        self._cached_meta = load_graph_metadata(self.client.graph_id, session)
      finally:
        try:
          next(db_gen)
        except StopIteration:
          pass
    except Exception:
      logger.debug(
        "Graph metadata preload failed for %s; registrar tools will fall back to per-call load",
        getattr(self.client, "graph_id", "unknown"),
      )
    return self._cached_meta

  def _has_extension(self, extension: str) -> bool:
    """Check if the graph has a specific schema extension."""
    return extension in self.schema_extensions

  def _is_shared_repository(self) -> bool:
    """Check if the graph is a shared repository (e.g., SEC)."""
    try:
      from robosystems.config.shared_repositories import (
        is_shared_repository_or_subgraph,
      )

      return is_shared_repository_or_subgraph(self.client.graph_id)
    except Exception:
      return False

  def _should_include_semantic_tools(self) -> bool:
    """Check if semantic enrichment tools should be included.

    Returns true if the manifest declares has_semantic_enrichment=True.
    Resolves subgraph parent so that subgraphs inherit the parent manifest.
    """
    try:
      from robosystems.config.shared_repositories import (
        get_manifest,
        is_shared_repository_or_subgraph,
        resolve_shared_repository_parent,
      )

      graph_id = self.client.graph_id
      if is_shared_repository_or_subgraph(graph_id):
        graph_id = resolve_shared_repository_parent(graph_id)
      manifest = get_manifest(graph_id)
      if manifest and manifest.has_semantic_enrichment:
        return True
    except Exception as exc:
      graph_id = getattr(self.client, "graph_id", "unknown")
      logger.debug(f"Semantic enrichment check failed for {graph_id}: {exc}")
    return False

  def _get_semantic_tool_definitions(self) -> list[dict[str, Any]]:
    """Get semantic enrichment tool definitions.

    Returns:
        List of tool definitions (empty if semantic enrichment not enabled)
    """
    if self.resolve_element_tool is None:
      return []
    return [
      self.resolve_element_tool.get_tool_definition(),
    ]

  def _get_workspace_tool_definitions(self) -> list[dict[str, Any]]:
    """
    Get graph-lifecycle tool definitions (navigation + write ops).

    Returns:
        List of lifecycle tool definitions (empty if MCP_WORKSPACE_ENABLED is false).
        For read-only graphs, only navigation tools (switch / list-subgraphs)
        are included.
    """
    tools = []
    if self.switch_workspace_tool is not None:
      tools.append(self.switch_workspace_tool.get_tool_definition())
    if self.list_subgraphs_tool is not None:
      tools.append(self.list_subgraphs_tool.get_tool_definition())
    if self.create_subgraph_tool is not None:
      tools.append(self.create_subgraph_tool.get_tool_definition())
    if self.delete_subgraph_tool is not None:
      tools.append(self.delete_subgraph_tool.get_tool_definition())
    if self.create_backup_tool is not None:
      tools.append(self.create_backup_tool.get_tool_definition())
    return tools

  def _get_memory_tool_definitions(self) -> list[dict[str, Any]]:
    """
    Get memory management tool definitions.

    Returns:
        List of memory tool definitions (empty if MCP_MEMORY_ENABLED is false)
    """
    if self.write_cypher_tool is None:
      return []
    return [
      self.write_cypher_tool.get_tool_definition(),
      self.add_node_table_tool.get_tool_definition(),
      self.add_relationship_table_tool.get_tool_definition(),
    ]

  def _get_fact_grid_tool_definitions(self) -> list[dict[str, Any]]:
    """
    Get fact-grid tool definitions.

    Returns:
        List of fact-grid tool definitions (empty if FACT_GRID_ENABLED is false)
    """
    tools = []
    if self.build_fact_grid_tool is not None:
      tools.append(self.build_fact_grid_tool.get_tool_definition())
    return tools

  def _get_materialization_tool_definitions(self) -> list[dict[str, Any]]:
    """Get materialization awareness tool definitions (sync status + trigger)."""
    tools = []
    if self.get_graph_sync_status_tool is not None:
      tools.append(self.get_graph_sync_status_tool.get_tool_definition())
    if self.materialize_tool is not None:
      tools.append(self.materialize_tool.get_tool_definition())
    return tools

  def _get_schedule_tool_definitions(self) -> list[dict[str, Any]]:
    """Get period-workflow tool definitions + fiscal calendar tools.

    Schedule-specific read tools were retired; schedule envelopes now
    surface through the generic information-block read tools.
    """
    tools = []
    if self.get_period_close_status_tool is not None:
      tools.append(self.get_period_close_status_tool.get_tool_definition())
    if self.list_period_drafts_tool is not None:
      tools.append(self.list_period_drafts_tool.get_tool_definition())
    if self.get_fiscal_calendar_tool is not None:
      tools.append(self.get_fiscal_calendar_tool.get_tool_definition())
    if self.close_period_tool is not None:
      tools.append(self.close_period_tool.get_tool_definition())
    if self.reopen_period_tool is not None:
      tools.append(self.reopen_period_tool.get_tool_definition())
    return tools

  def _get_taxonomy_tool_definitions(self) -> list[dict[str, Any]]:
    """Get taxonomy read tool definitions (CoA → GAAP workflow)."""
    tools = []
    if self.get_unmapped_elements_tool is not None:
      tools.append(self.get_unmapped_elements_tool.get_tool_definition())
    if self.suggest_mapping_tool is not None:
      tools.append(self.suggest_mapping_tool.get_tool_definition())
    if self.get_mapping_summary_tool is not None:
      tools.append(self.get_mapping_summary_tool.get_tool_definition())
    return tools

  def _get_information_block_tool_definitions(self) -> list[dict[str, Any]]:
    """Get Information Block read tool definitions (cross-block-type reads)."""
    tools = []
    if self.get_information_block_tool is not None:
      tools.append(self.get_information_block_tool.get_tool_definition())
    if self.list_information_blocks_tool is not None:
      tools.append(self.list_information_blocks_tool.get_tool_definition())
    return tools

  def _get_event_block_tool_definitions(self) -> list[dict[str, Any]]:
    tools = []
    if self.get_event_block_tool is not None:
      tools.append(self.get_event_block_tool.get_tool_definition())
    if self.list_event_blocks_tool is not None:
      tools.append(self.list_event_blocks_tool.get_tool_definition())
    return tools

  def _get_search_tool_definitions(self) -> list[dict[str, Any]]:
    """
    Get text search tool definitions.

    Returns:
        List of search tool definitions (empty if SEMANTIC_SEARCH_ENABLED is false)
    """
    tools = []
    if self.search_documents_tool is not None:
      tools.append(self.search_documents_tool.get_tool_definition())
    if self.get_document_section_tool is not None:
      tools.append(self.get_document_section_tool.get_tool_definition())
    return tools

  def _get_curated_tool_definitions(self) -> list[dict[str, Any]]:
    """Get curated financial statement tool definitions.

    - ``financial-statement-analysis`` — graph-backed (SEC + materialized tenants)
    - ``live-financial-statement`` — OLTP-backed (tenant entity graphs only)
    """
    tools: list[dict[str, Any]] = []
    if self.financial_statement_analysis_tool is not None:
      tools.append(self.financial_statement_analysis_tool.get_tool_definition())
    if self.live_financial_statement_tool is not None:
      tools.append(self.live_financial_statement_tool.get_tool_definition())
    return tools

  def _tool_unavailable_reason(self, tool_name: str, feature_flag: str) -> str:
    """Return a context-aware error message for unavailable tools."""
    if self.read_only:
      return f"{tool_name} is not available on this read-only graph."
    return f"{tool_name} tool is not available. Set {feature_flag}=true to enable this feature."

  def get_tool_definitions_as_dict(self) -> list[dict[str, Any]]:
    """
    Get MCP tool definitions for graph databases, using compatible naming.

    Tool availability is schema-driven:
    - Core tools are always included (2 tools)
    - RoboLedger extension tools require "roboledger" in schema_extensions
    - Infrastructure tools are gated by feature flags

    Returns:
        List of tool definition dictionaries
    """
    # Layer 1: Core tools (always available)
    tools = [
      self.cypher_tool.get_tool_definition(),
      self.schema_tool.get_tool_definition(),
    ]

    # Layer 2: Schema extension tools (roboledger)
    if self._has_extension("roboledger"):
      tools.append(self.example_queries_tool.get_tool_definition())

      # Semantic enrichment tools (preferred path for concept resolution)
      tools.extend(self._get_semantic_tool_definitions())

      # Curated financial tools (FactSet-powered)
      tools.extend(self._get_curated_tool_definitions())

      # Fact grid tool (custom element/period/entity queries)
      tools.extend(self._get_fact_grid_tool_definitions())

      # Materialization tools (sync status + trigger)
      tools.extend(self._get_materialization_tool_definitions())

      # Schedule tools (close workflow)
      tools.extend(self._get_schedule_tool_definitions())

      # Taxonomy mapping tools (CoA → GAAP workflow)
      tools.extend(self._get_taxonomy_tool_definitions())

      # Information Block read tools (cross-block-type reads)
      tools.extend(self._get_information_block_tool_definitions())

      # Event Block read tools (get-event-block, list-event-blocks)
      tools.extend(self._get_event_block_tool_definitions())

    # Layer 3: Infrastructure tools (feature-flag gated)
    tools.extend(self._get_workspace_tool_definitions())
    tools.extend(self._get_memory_tool_definitions())
    tools.extend(self._get_search_tool_definitions())
    tools.extend(self._get_document_tool_definitions())

    # Layer 0: Registrar-generated tools (per enabled extension).
    # Appended last so hand-written tools retain their historical
    # ordering; clients rely on get_tool_definitions_as_dict to
    # enumerate by name, not index.
    for tool in self._registrar_dispatch.values():
      tools.append(tool.get_tool_definition())

    return tools

  def _get_document_tool_definitions(self) -> list[dict[str, Any]]:
    """Get document management tool definitions (create, update, get, list)."""
    tools = []
    if self.create_document_tool is not None:
      tools.append(self.create_document_tool.get_tool_definition())
    if self.update_document_tool is not None:
      tools.append(self.update_document_tool.get_tool_definition())
    if self.get_document_tool is not None:
      tools.append(self.get_document_tool.get_tool_definition())
    if self.list_documents_tool is not None:
      tools.append(self.list_documents_tool.get_tool_definition())
    return tools

  async def call_tool(
    self, name: str, arguments: dict[str, Any], return_raw: bool = False
  ) -> Any:
    """
    Call a specific MCP tool by name.

    Args:
        name: Tool name
        arguments: Tool arguments
        return_raw: Whether to return raw result or formatted string

    Returns:
        Tool execution result
    """
    try:
      # Layer 0: Registrar-generated tools (auto-derived from OperationSpec)
      # Checked first so hand-written if/elif branches can be pruned as
      # tools migrate. These tools embed the extension gate internally.
      registrar_tool = self._registrar_dispatch.get(name)
      if registrar_tool is not None:
        result = await registrar_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      # Layer 1: Core tools (always available)
      if name == "read-graph-cypher":
        result = await self.cypher_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      elif name == "get-graph-schema":
        # For schema tool, we need to handle caching differently
        result = await self.schema_tool.execute(arguments)

        # Update our cache stats from schema tool
        schema_stats = self.schema_tool.get_cache_stats()
        self._cache_hits = schema_stats["cache_hits"]
        self._cache_misses = schema_stats["cache_misses"]

        if return_raw:
          return result
        else:
          cache_info = {
            "_cache_metadata": {
              "cached": schema_stats["is_cached"],
              "cache_age_seconds": schema_stats.get("cache_age_seconds"),
              "cache_hit_rate": f"{schema_stats['hit_rate_percent']:.1f}%",
            },
            "schema": result,
          }
          return json.dumps(cache_info, indent=2)

      # Layer 2: Schema extension tools (roboledger-gated)
      elif name == "get-example-queries":
        if self.example_queries_tool is None:
          raise ValueError(
            "get-example-queries tool is not available. "
            "This graph does not have the roboledger schema extension."
          )
        result = await self.example_queries_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      elif name == "resolve-element":
        if self.resolve_element_tool is None:
          raise ValueError(
            "resolve-element tool is not available. "
            "This graph does not have semantic enrichment enabled."
          )
        result = await self.resolve_element_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      elif name == "financial-statement-analysis":
        if self.financial_statement_analysis_tool is None:
          raise ValueError(
            "financial-statement-analysis tool is not available. "
            "This graph does not have the roboledger schema extension."
          )
        result = await self.financial_statement_analysis_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      elif name == "live-financial-statement":
        if self.live_financial_statement_tool is None:
          raise ValueError(
            "live-financial-statement tool is not available. "
            "This graph is either a shared repository, read-only, or missing "
            "the roboledger schema extension."
          )
        result = await self.live_financial_statement_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      # Layer 3: Graph lifecycle tools (MCP_WORKSPACE_ENABLED, read_only gates)
      elif name == "create-subgraph":
        if self.create_subgraph_tool is None:
          raise ValueError(
            self._tool_unavailable_reason("create-subgraph", "MCP_WORKSPACE_ENABLED")
          )
        result = await self.create_subgraph_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      elif name == "delete-subgraph":
        if self.delete_subgraph_tool is None:
          raise ValueError(
            self._tool_unavailable_reason("delete-subgraph", "MCP_WORKSPACE_ENABLED")
          )
        result = await self.delete_subgraph_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      elif name == "list-subgraphs":
        if self.list_subgraphs_tool is None:
          raise ValueError(
            "list-subgraphs tool is not available. "
            "Set MCP_WORKSPACE_ENABLED=true to enable this feature."
          )
        result = await self.list_subgraphs_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      elif name == "switch-workspace":
        # Client-side sentinel — the MCP client should intercept locally.
        if self.switch_workspace_tool is None:
          raise ValueError(
            "switch-workspace tool is not available. "
            "Set MCP_WORKSPACE_ENABLED=true to enable this feature."
          )
        result = await self.switch_workspace_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      elif name == "create-backup":
        if self.create_backup_tool is None:
          raise ValueError(
            self._tool_unavailable_reason("create-backup", "MCP_WORKSPACE_ENABLED")
          )
        result = await self.create_backup_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      elif name == "write-graph-cypher":
        if self.write_cypher_tool is None:
          raise ValueError(
            self._tool_unavailable_reason("write-graph-cypher", "MCP_MEMORY_ENABLED")
          )
        result = await self.write_cypher_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      elif name == "add-node-table":
        if self.add_node_table_tool is None:
          raise ValueError(
            self._tool_unavailable_reason("add-node-table", "MCP_MEMORY_ENABLED")
          )
        result = await self.add_node_table_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      elif name == "add-relationship-table":
        if self.add_relationship_table_tool is None:
          raise ValueError(
            self._tool_unavailable_reason(
              "add-relationship-table", "MCP_MEMORY_ENABLED"
            )
          )
        result = await self.add_relationship_table_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      elif name == "build-fact-grid":
        if self.build_fact_grid_tool is None:
          raise ValueError(
            "build-fact-grid tool is not available. "
            "Set FACT_GRID_ENABLED=true to enable this feature."
          )
        result = await self.build_fact_grid_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      # Materialization tools
      elif name == "get-graph-sync-status":
        if self.get_graph_sync_status_tool is None:
          raise ValueError(
            "get-graph-sync-status tool is not available. "
            "Requires roboledger extension and ROBOLEDGER_ENABLED=true."
          )
        result = await self.get_graph_sync_status_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      elif name == "materialize":
        if self.materialize_tool is None:
          raise ValueError(
            self._tool_unavailable_reason("materialize", "ROBOLEDGER_ENABLED")
          )
        result = await self.materialize_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      # Period-workflow read tools (writes are registrar-generated, Layer 0).
      # Schedule-specific reads retired — use list-information-blocks
      # with block_type="schedule" or get-information-block.
      elif name == "get-period-close-status":
        if self.get_period_close_status_tool is None:
          raise ValueError(
            "get-period-close-status tool is not available. "
            "Requires roboledger extension and ROBOLEDGER_ENABLED=true."
          )
        result = await self.get_period_close_status_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      elif name == "list-period-drafts":
        if self.list_period_drafts_tool is None:
          raise ValueError(
            "list-period-drafts tool is not available. "
            "Requires roboledger extension and ROBOLEDGER_ENABLED=true."
          )
        result = await self.list_period_drafts_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      # Information Block read tools (writes are registrar-generated, handled at Layer 0)
      elif name == "get-information-block":
        if self.get_information_block_tool is None:
          raise ValueError(
            "get-information-block tool is not available. "
            "Requires roboledger extension and ROBOLEDGER_ENABLED=true."
          )
        result = await self.get_information_block_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      elif name == "list-information-blocks":
        if self.list_information_blocks_tool is None:
          raise ValueError(
            "list-information-blocks tool is not available. "
            "Requires roboledger extension and ROBOLEDGER_ENABLED=true."
          )
        result = await self.list_information_blocks_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      # Event Block read tools (writes are registrar-generated, handled at Layer 0)
      elif name == "get-event-block":
        if self.get_event_block_tool is None:
          raise ValueError(
            "get-event-block tool is not available. "
            "Requires roboledger extension and ROBOLEDGER_ENABLED=true."
          )
        result = await self.get_event_block_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      elif name == "list-event-blocks":
        if self.list_event_blocks_tool is None:
          raise ValueError(
            "list-event-blocks tool is not available. "
            "Requires roboledger extension and ROBOLEDGER_ENABLED=true."
          )
        result = await self.list_event_blocks_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      # Fiscal calendar tools
      elif name == "get-fiscal-calendar":
        if self.get_fiscal_calendar_tool is None:
          raise ValueError(
            "get-fiscal-calendar tool is not available. "
            "Requires roboledger extension and ROBOLEDGER_ENABLED=true."
          )
        result = await self.get_fiscal_calendar_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      elif name == "close-period":
        if self.close_period_tool is None:
          raise ValueError(
            "close-period tool is not available. "
            "Requires roboledger extension and ROBOLEDGER_ENABLED=true."
          )
        result = await self.close_period_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      elif name == "reopen-period":
        if self.reopen_period_tool is None:
          raise ValueError(
            "reopen-period tool is not available. "
            "Requires roboledger extension and ROBOLEDGER_ENABLED=true."
          )
        result = await self.reopen_period_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      # Taxonomy mapping tools
      elif name == "get-unmapped-elements":
        if self.get_unmapped_elements_tool is None:
          raise ValueError(
            "get-unmapped-elements tool is not available. "
            "Requires roboledger extension and ROBOLEDGER_ENABLED=true."
          )
        result = await self.get_unmapped_elements_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      elif name == "suggest-mapping":
        if self.suggest_mapping_tool is None:
          raise ValueError(
            "suggest-mapping tool is not available. "
            "Requires roboledger extension and ROBOLEDGER_ENABLED=true."
          )
        result = await self.suggest_mapping_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      elif name == "get-mapping-summary":
        if self.get_mapping_summary_tool is None:
          raise ValueError(
            "get-mapping-summary tool is not available. "
            "Requires roboledger extension and ROBOLEDGER_ENABLED=true."
          )
        result = await self.get_mapping_summary_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      elif name == "search-documents":
        if self.search_documents_tool is None:
          raise ValueError(
            self._tool_unavailable_reason("search-documents", "SEMANTIC_SEARCH_ENABLED")
          )
        result = await self.search_documents_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      elif name == "get-document-section":
        if self.get_document_section_tool is None:
          raise ValueError(
            self._tool_unavailable_reason(
              "get-document-section", "SEMANTIC_SEARCH_ENABLED"
            )
          )
        result = await self.get_document_section_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      # Document management tools
      elif name == "create-document":
        if self.create_document_tool is None:
          raise ValueError(
            self._tool_unavailable_reason("create-document", "SEMANTIC_SEARCH_ENABLED")
          )
        result = await self.create_document_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      elif name == "update-document":
        if self.update_document_tool is None:
          raise ValueError(
            self._tool_unavailable_reason("update-document", "SEMANTIC_SEARCH_ENABLED")
          )
        result = await self.update_document_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      elif name == "get-document":
        if self.get_document_tool is None:
          raise ValueError(
            self._tool_unavailable_reason("get-document", "SEMANTIC_SEARCH_ENABLED")
          )
        result = await self.get_document_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      elif name == "list-documents":
        if self.list_documents_tool is None:
          raise ValueError(
            self._tool_unavailable_reason("list-documents", "SEMANTIC_SEARCH_ENABLED")
          )
        result = await self.list_documents_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      else:
        raise ValueError(f"Unknown tool: {name}")

    except GraphQueryTimeoutError as e:
      # Enhanced timeout error handling
      error_context = self._build_error_context(name, arguments, e)
      error_msg = str(e)

      # Add timeout-specific suggestions
      if name == "read-graph-cypher" and "query" in arguments:
        query = arguments["query"]
        if len(query) > 1000:
          error_msg += (
            "\n💡 Large query detected. Consider breaking into smaller parts."
          )
        if "LIMIT" not in query.upper():
          error_msg += "\n💡 Add LIMIT clause to reduce result size."

      logger.error(
        f"Query timeout in tool '{name}': {error_msg}",
        extra={"error_context": error_context},
      )
      if return_raw:
        raise  # Re-raise for raw mode
      return f"Timeout: {error_msg}"

    except GraphQueryComplexityError as e:
      # Enhanced complexity error handling
      error_context = self._build_error_context(name, arguments, e)
      error_msg = str(e)

      # Add complexity-specific suggestions
      if hasattr(e, "details") and "complexity_score" in e.details:
        score = e.details["complexity_score"]
        error_msg += f"\n💡 Complexity score: {score}. Consider simplifying the query."

      logger.error(
        f"Query complexity error in tool '{name}': {error_msg}",
        extra={"error_context": error_context},
      )
      if return_raw:
        raise  # Re-raise for raw mode
      return f"Complexity Error: {error_msg}"

    except GraphAPIError as e:
      # Enhanced error handling with more context
      error_msg = str(e)
      error_context = self._build_error_context(name, arguments, e)
      logger.error(
        f"Graph API error in tool '{name}': {error_msg}",
        extra={"error_context": error_context},
      )

      # Add helpful context based on error type and tool
      enhanced_msg = self._enhance_error_message(error_msg, name, arguments)

      if return_raw:
        # Preserve original exception with enhanced message
        e.args = (enhanced_msg, *e.args[1:]) if len(e.args) > 1 else (enhanced_msg,)
        if hasattr(e, "details"):
          e.details = {**e.details, **error_context}
        raise
      return f"Error: {enhanced_msg}"

    except ValueError as e:
      # Handle argument validation errors with specific context
      error_msg = str(e)
      if "Query parameter" in error_msg or "argument" in error_msg.lower():
        error_msg = f"Invalid argument in tool '{name}': {error_msg}"
        if arguments:
          error_msg += f"\nProvided arguments: {list(arguments.keys())}"

      logger.error(f"Argument validation error in tool '{name}': {error_msg}")
      if return_raw:
        raise GraphValidationError(error_msg, validation_errors=[error_msg])
      return f"Validation Error: {error_msg}"

    except Exception as e:
      # Handle other errors with enhanced sanitization and context
      error_context = self._build_error_context(name, arguments, e)
      error_msg = self._sanitize_error_message(str(e))

      logger.error(
        f"Tool execution failed for '{name}': {error_msg}",
        extra={"error_context": error_context, "exception_type": type(e).__name__},
      )

      if return_raw:
        raise GraphAPIError(f"Tool execution failed: {error_msg}")
      return f"Error: {error_msg}"

  async def execute_cypher_tool(
    self, query: str, parameters: dict[str, Any] | None = None
  ) -> list[dict[str, Any]]:
    """
    Execute Cypher tool directly.

    Args:
        query: Cypher query
        parameters: Optional query parameters

    Returns:
        Query result
    """
    arguments: dict[str, Any] = {"query": query}
    if parameters:
      arguments["parameters"] = parameters

    return await self.call_tool("read-graph-cypher", arguments, return_raw=True)

  async def execute_schema_tool(self) -> list[dict[str, Any]]:
    """
    Execute schema retrieval tool.

    Returns:
        Schema information list
    """
    return await self.call_tool("get-graph-schema", {}, return_raw=True)

  def _build_error_context(
    self, tool_name: str, arguments: dict[str, Any], exception: Exception
  ) -> dict[str, Any]:
    """Build comprehensive error context for logging and debugging."""
    context: dict[str, Any] = {
      "tool_name": tool_name,
      "graph_id": self.client.graph_id,
      "exception_type": type(exception).__name__,
    }

    # Add argument context (sanitized)
    if arguments:
      # Don't log full query content for security, just metadata
      arg_context: dict[str, Any] = {}
      for key, value in arguments.items():
        if key == "query" and isinstance(value, str):
          arg_context[key] = {
            "length": len(value),
            "has_limit": "LIMIT" in value.upper(),
            "has_where": "WHERE" in value.upper(),
            "has_match": "MATCH" in value.upper(),
          }
        elif key == "parameters":
          arg_context[key] = {"param_count": len(value) if value else 0}
        else:
          arg_context[key] = type(value).__name__

      context["arguments"] = arg_context

    # Add exception-specific context
    if hasattr(exception, "error_code"):
      context["error_code"] = exception.error_code
    if hasattr(exception, "details"):
      context["exception_details"] = exception.details

    return context

  def _enhance_error_message(
    self, error_msg: str, tool_name: str, arguments: dict[str, Any]
  ) -> str:
    """Enhance error messages with tool-specific context and suggestions."""
    enhanced_msg = error_msg

    # Add tool-specific suggestions
    if tool_name == "read-graph-cypher":
      if "Parser exception" in error_msg:
        enhanced_msg += "\n\n🔧 Query Syntax Help:"
        enhanced_msg += "\n- Check node labels exist: Use get-graph-schema first"
        enhanced_msg += (
          "\n- Property access: n.property_name (use keys(n) to discover properties)"
        )
        enhanced_msg += "\n- Ensure proper Cypher syntax for graph database"

      elif "property" in error_msg.lower() and "not found" in error_msg.lower():
        enhanced_msg += "\n\n🔧 Property Help:"
        enhanced_msg += "\n- Use keys(node) to list available properties"
        enhanced_msg += "\n- Common properties: identifier, name, value, uri"
        enhanced_msg += "\n- Properties vary by node type - check schema first"

      elif "connection" in error_msg.lower():
        enhanced_msg += "\n\n🔧 Connection Help:"
        enhanced_msg += "\n- Check if Graph API service is running"
        enhanced_msg += "\n- Verify network connectivity and firewall settings"
        enhanced_msg += "\n- Ensure correct API endpoint configuration"

    elif tool_name == "get-graph-schema" and "timeout" in error_msg.lower():
      enhanced_msg += "\n\n💡 Large schema detected. Consider using read-graph-cypher with CALL SHOW_TABLES() for specific node types."

    # Add general suggestions based on error patterns
    if "unauthorized" in error_msg.lower() or "forbidden" in error_msg.lower():
      enhanced_msg += "\n\n🔐 Check API permissions and authentication credentials."

    elif "rate limit" in error_msg.lower():
      enhanced_msg += (
        "\n\n⏱️ API rate limit exceeded. Wait before retrying or reduce query frequency."
      )

    return enhanced_msg

  def _sanitize_error_message(self, error_msg: str) -> str:
    """
    Sanitize error messages to remove sensitive information.

    Args:
        error_msg: Raw error message

    Returns:
        Sanitized error message
    """
    # Remove file paths and sensitive details
    sensitive_patterns = [
      r"/[^\s]+\.db",  # Database file paths
      r"password[=:][^\s]+",  # Password patterns
      r"token[=:][^\s]+",  # Token patterns
      r"key[=:][^\s]+",  # Key patterns
    ]

    sanitized = error_msg
    for pattern in sensitive_patterns:
      import re

      sanitized = re.sub(pattern, "[REDACTED]", sanitized, flags=re.IGNORECASE)

    # Map common errors to user-friendly messages
    error_mappings = {
      "connection": "Database connection failed",
      "timeout": "Query execution timed out",
      "syntax": "Query syntax error",
      "permission": "Insufficient permissions",
    }

    for key, friendly_msg in error_mappings.items():
      if key.lower() in sanitized.lower():
        return friendly_msg

    return sanitized

  def clear_schema_cache(self):
    """Clear the schema cache to force refresh on next call."""
    self.schema_tool.clear_schema_cache()
    logger.debug("Schema cache cleared")

  def get_cache_stats(self) -> dict[str, Any]:
    """Get cache performance statistics."""
    return self.schema_tool.get_cache_stats()

  async def close(self):
    """Close MCP tools and log final statistics."""
    # Log final cache statistics
    stats = self.get_cache_stats()
    logger.info(
      f"MCP Tools cache stats - Hits: {stats['cache_hits']}, "
      f"Misses: {stats['cache_misses']}, Hit Rate: {stats['hit_rate_percent']:.1f}%"
    )
