"""`GraphMCPTools` — the per-graph MCP tool surface.

Core tools are always present, extension tools follow `schema_extensions`,
and infrastructure tools are feature-flagged. Registrar-generated tools are
dispatched before the hand-written ladder, so they win on a name clash.
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
  SetWritePolicyTool,
  SyncConnectionTool,
)
from .graphql_tool import GraphqlQueryTool, GraphqlSchemaTool
from .schema_tool import SchemaTool
from .subgraph_write_tools import (
  AddNodeTableTool,
  AddRelationshipTableTool,
  WriteCypherTool,
)

if TYPE_CHECKING:
  from robosystems.middleware.extensions import GraphExtensionContext

  from .registrar import _RegistrarMCPTool


# The tool surface a tenant subgraph advertises. A subgraph inherits its
# parent's `schema_extensions` but has no extensions OLTP schema, no
# connections and no materialization, so every other tool would fail or
# answer about machinery that isn't there. It is schema plus Cypher.
SUBGRAPH_TOOL_PROFILE = frozenset(
  {
    # Schema DDL — the point of a subgraph
    "add-node-table",
    "add-relationship-table",
    "get-graph-schema",
    # Registered outside this manager (routers/graphs/mcp); listed so the
    # call_tool guard allows it.
    "get-graph-info",
    # Cypher, both directions
    "read-graph-cypher",
    "write-graph-cypher",
    "get-example-queries",
    # Per-graph semantic memory (LanceDB, genuinely per-graph)
    "recall",
    "remember",
    "forget",
    "update-memory",
    # Navigation back out
    "list-subgraphs",
  }
)


# Tools the `/v1/mcp/roboledger` route withholds: it serves directory listings
# that freeze one tool list per URL, so it drops what a chat client cannot use
# or should not drive. An exclusion list, so new ledger tools appear by
# default. Applied in `routers/graphs/mcp/remote.py`.
ROBOLEDGER_ROUTE_TOOL_EXCLUSIONS = frozenset(
  {
    # Subgraph-only; a chat user cannot add a subgraph connector mid-conversation.
    "write-graph-cypher",
    "add-node-table",
    "add-relationship-table",
    # Workspace administration — a new subgraph needs its own connector.
    "create-subgraph",
    "delete-subgraph",
    "list-subgraphs",
    # `materialize` already runs on staleness, can clear the graph
    # (`rebuild=true`), and answers with a stream URL a chat client can't follow.
    "create-backup",
    "materialize",
    # QuickBooks write-back is an owner decision in the app, not a chat action.
    "set-write-policy",
    # Maintenance and onboarding repair, not the close.
    "rebuild-schedule",
    "backfill-plan-history",
    # The most destructive tool on the surface (`cascade_facts=true`).
    "delete-taxonomy-block",
  }
)


def resolve_schema_extensions(graph_id: str) -> list[str]:
  """Shared repositories read the manifest; user graphs read the graphs table."""
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

  try:
    from robosystems.database import SessionFactory
    from robosystems.models.core import Graph

    # Independent session (see platform_session docs).
    db = SessionFactory()
    try:
      graph = Graph.get_by_id(graph_id, db)
      if graph and graph.schema_extensions:
        return list(graph.schema_extensions)
    finally:
      db.close()
  except Exception:
    logger.warning(f"Could not resolve schema extensions for {graph_id}")

  return []


class GraphMCPTools:
  """Per-graph MCP tools.

  Layer 1 (core) is always present, layer 2 needs the "roboledger" schema
  extension, layer 3 (infrastructure) is feature-flagged.
  """

  def __init__(
    self,
    graph_client,
    schema_extensions: list[str] | tuple[str, ...] = (),
    read_only: bool = False,
  ):
    from ..client import GraphMCPClient

    self.client: GraphMCPClient = graph_client
    self.schema_extensions: tuple[str, ...] = tuple(schema_extensions)
    self.read_only: bool = read_only

    self.validator = GraphQueryValidator()

    # Layer 1: core
    self.cypher_tool = CypherTool(
      graph_client, schema_extensions=self.schema_extensions
    )
    self.schema_tool = SchemaTool(graph_client)

    # Layer 1: GraphQL. MCP_GRAPHQL_ENABLED is a runtime kill switch checked
    # at dispatch. Not on shared repos: they have no extensions OLTP schema,
    # so introspection answers but every real query errors.
    self.graphql_schema_tool: GraphqlSchemaTool | None = None
    self.graphql_query_tool: GraphqlQueryTool | None = None
    if env.EXTENSIONS_GRAPHQL_ENABLED and not self._is_shared_repository():
      self.graphql_schema_tool = GraphqlSchemaTool(graph_client)
      self.graphql_query_tool = GraphqlQueryTool(
        graph_client, schema_extensions=self.schema_extensions
      )

    # Layer 2: schema extension
    self.example_queries_tool = None
    self.live_financial_statement_tool = None
    self.financial_statement_analysis_tool = None
    self.disclosures_tool = None
    self.information_block_tool = None
    self.resolve_element_tool = None
    self.resolve_structure_tool = None

    if self._has_extension("roboledger"):
      self.example_queries_tool = ExampleQueriesTool(graph_client)

      from .financial_statement_tools import (
        FinancialStatementAnalysisTool,
        LiveFinancialStatementTool,
      )

      # Graph-backed: shared repos and materialized tenants alike.
      self.financial_statement_analysis_tool = FinancialStatementAnalysisTool(
        graph_client
      )
      # xbrlkit over the report held whole (the published filing on a shared
      # repo, the ledger's report on a tenant); the graph is not in the path.
      from .disclosure_tools import DisclosuresTool, InformationBlockTool

      self.disclosures_tool = DisclosuresTool(graph_client)
      self.information_block_tool = InformationBlockTool(graph_client)
      # OLTP-backed, so tenant graphs only.
      if not self._is_shared_repository():
        self.live_financial_statement_tool = LiveFinancialStatementTool(graph_client)

      if self._should_include_semantic_tools():
        from .resolve_element_tool import ResolveElementTool

        self.resolve_element_tool = ResolveElementTool(graph_client)

    # Layer 3: graph lifecycle, a subset of `/v1/graphs/{g}/operations/*`.
    # `change-tier` stays REST-only: a destructive, billed EBS migration.
    # Restore has no customer surface at all; it is operator-run.
    self.create_subgraph_tool = None
    self.delete_subgraph_tool = None
    self.list_subgraphs_tool = None
    self.create_backup_tool = None
    if env.MCP_WORKSPACE_ENABLED:
      self.list_subgraphs_tool = ListSubgraphsTool(graph_client)
      if not read_only:
        self.create_subgraph_tool = CreateSubgraphTool(graph_client)
        self.delete_subgraph_tool = DeleteSubgraphTool(graph_client)
        self.create_backup_tool = CreateBackupTool(graph_client)

    self.write_cypher_tool = None
    self.add_node_table_tool = None
    self.add_relationship_table_tool = None
    if env.MCP_SUBGRAPH_OPS_ENABLED and not read_only:
      self.write_cypher_tool = WriteCypherTool(graph_client)
      self.add_node_table_tool = AddNodeTableTool(graph_client)
      self.add_relationship_table_tool = AddRelationshipTableTool(graph_client)

    self.build_fact_grid_tool = None
    if env.FACT_GRID_ENABLED:
      from .fact_grid_tool import BuildFactGridTool

      self.build_fact_grid_tool = BuildFactGridTool(graph_client)

    # Layer 2: materialization. User entity graphs only: shared repos use
    # their own pipeline and don't track staleness.
    self.get_graph_sync_status_tool = None
    self.materialize_tool = None
    if (
      self._has_extension("roboledger")
      and env.ROBOLEDGER_ENABLED
      and not self._is_shared_repository()
      and not self._is_subgraph()
    ):
      self.get_graph_sync_status_tool = GetGraphSyncStatusTool(graph_client)
      if not read_only:
        self.materialize_tool = MaterializeTool(graph_client)

    self.set_write_policy_tool = None
    if not read_only and not self._is_shared_repository():
      self.set_write_policy_tool = SetWritePolicyTool(graph_client)

    # Not roboledger-gated, matching the REST sync endpoint. Not on
    # subgraphs: connections belong to the parent.
    self.sync_connection_tool = None
    if not read_only and not self._is_shared_repository() and not self._is_subgraph():
      self.sync_connection_tool = SyncConnectionTool(graph_client)

    # Layer 2: period workflow and fiscal calendar
    self.get_period_close_status_tool = None
    self.list_period_drafts_tool = None
    self.get_fiscal_calendar_tool = None
    self.close_period_tool = None
    self.reopen_period_tool = None
    self.backfill_plan_history_tool = None
    self.get_information_block_tool = None
    self.list_information_blocks_tool = None
    if (
      self._has_extension("roboledger")
      and env.ROBOLEDGER_ENABLED
      and not self._is_shared_repository()
    ):
      from .fiscal_calendar_tools import (
        BackfillPlanHistoryTool,
        ClosePeriodTool,
        GetFiscalCalendarTool,
        ReopenPeriodTool,
      )
      from .schedule_tools import (
        GetPeriodCloseStatusTool,
        ListPeriodDraftsTool,
      )

      self.get_period_close_status_tool = GetPeriodCloseStatusTool(graph_client)
      self.list_period_drafts_tool = ListPeriodDraftsTool(graph_client)
      self.get_fiscal_calendar_tool = GetFiscalCalendarTool(graph_client)
      if not read_only:
        self.close_period_tool = ClosePeriodTool(graph_client)
        self.reopen_period_tool = ReopenPeriodTool(graph_client)
        self.backfill_plan_history_tool = BackfillPlanHistoryTool(graph_client)

    # The extensions OLTP reads below all skip shared repos, which have no
    # per-graph OLTP schema.
    self.get_information_block_tool = None
    self.list_information_blocks_tool = None
    if (
      self._has_extension("roboledger")
      and env.ROBOLEDGER_ENABLED
      and not self._is_shared_repository()
    ):
      from .information_block_tools import (
        GetInformationBlockTool,
        ListInformationBlocksTool,
      )

      self.get_information_block_tool = GetInformationBlockTool(graph_client)
      self.list_information_blocks_tool = ListInformationBlocksTool(graph_client)

    # No DB access; skipped on shared repos, where a close means nothing.
    self.get_close_playbook_tool = None
    if (
      self._has_extension("roboledger")
      and env.ROBOLEDGER_ENABLED
      and not self._is_shared_repository()
    ):
      from .playbook_tools import GetClosePlaybookTool

      self.get_close_playbook_tool = GetClosePlaybookTool(graph_client)

    self.get_agent_tool = None
    self.list_agents_tool = None
    self.agent_activity_tool = None
    if (
      self._has_extension("roboledger")
      and env.ROBOLEDGER_ENABLED
      and not self._is_shared_repository()
    ):
      from .agent_tools import AgentActivityTool, GetAgentTool, ListAgentsTool

      self.get_agent_tool = GetAgentTool(graph_client)
      self.list_agents_tool = ListAgentsTool(graph_client)
      self.agent_activity_tool = AgentActivityTool(graph_client)

    self.get_event_handler_tool = None
    self.list_event_handlers_tool = None
    if (
      self._has_extension("roboledger")
      and env.ROBOLEDGER_ENABLED
      and not self._is_shared_repository()
    ):
      from .event_handler_tools import GetEventHandlerTool, ListEventHandlersTool

      self.get_event_handler_tool = GetEventHandlerTool(graph_client)
      self.list_event_handlers_tool = ListEventHandlersTool(graph_client)

    self.get_event_block_tool = None
    self.list_event_blocks_tool = None
    if (
      self._has_extension("roboledger")
      and env.ROBOLEDGER_ENABLED
      and not self._is_shared_repository()
    ):
      from .event_block_tools import GetEventBlockTool, ListEventBlocksTool

      self.get_event_block_tool = GetEventBlockTool(graph_client)
      self.list_event_blocks_tool = ListEventBlocksTool(graph_client)

    # Layer 2: taxonomy mapping reads; the writes are registrar-generated.
    self.list_mapping_structures_tool = None
    self.get_unmapped_elements_tool = None
    self.suggest_mapping_tool = None
    self.get_mapping_summary_tool = None
    if (
      self._has_extension("roboledger")
      and env.ROBOLEDGER_ENABLED
      and not self._is_shared_repository()
    ):
      from .taxonomy_tools import (
        GetMappingSummaryTool,
        GetUnmappedElementsTool,
        ListMappingStructuresTool,
        SuggestMappingTool,
      )

      # suggest-mapping is heuristic: no writes, no AI.
      self.list_mapping_structures_tool = ListMappingStructuresTool(graph_client)
      self.get_unmapped_elements_tool = GetUnmappedElementsTool(graph_client)
      self.suggest_mapping_tool = SuggestMappingTool(graph_client)
      self.get_mapping_summary_tool = GetMappingSummaryTool(graph_client)

    self.search_documents_tool = None
    self.get_document_section_tool = None
    if env.SEMANTIC_SEARCH_ENABLED:
      from .search_tools import GetDocumentSectionTool, SearchDocumentsTool

      self.search_documents_tool = SearchDocumentsTool(graph_client)
      self.get_document_section_tool = GetDocumentSectionTool(graph_client)

    # Semantic memory (LanceDB)
    self.semantic_remember_tool = None
    self.semantic_recall_tool = None
    self.semantic_update_memory_tool = None
    self.semantic_forget_tool = None
    if (
      env.SEMANTIC_MEMORY_ENABLED
      and env.MCP_SEMANTIC_MEMORY_ENABLED
      and not self._is_shared_repository()
    ):
      from .semantic_memory_tools import (
        SemanticForgetTool,
        SemanticRecallTool,
        SemanticRememberTool,
        SemanticUpdateMemoryTool,
      )

      self.semantic_recall_tool = SemanticRecallTool(graph_client)
      if not read_only:
        # A subgraph gets no memory store, so the tools that would create
        # one are withheld; forget stays so older entries remain removable.
        if not self._is_subgraph():
          self.semantic_remember_tool = SemanticRememberTool(graph_client)
          self.semantic_update_memory_tool = SemanticUpdateMemoryTool(graph_client)
        self.semantic_forget_tool = SemanticForgetTool(graph_client)

    # Documents: shared repos use OpenSearch directly and have no PG rows.
    self.create_document_tool = None
    self.update_document_tool = None
    self.delete_document_tool = None
    self.get_document_tool = None
    self.list_documents_tool = None
    if env.SEMANTIC_SEARCH_ENABLED and not self._is_shared_repository():
      from .document_tools import (
        CreateDocumentTool,
        DeleteDocumentTool,
        GetDocumentTool,
        ListDocumentsTool,
        UpdateDocumentTool,
      )

      self.get_document_tool = GetDocumentTool(graph_client)
      self.list_documents_tool = ListDocumentsTool(graph_client)

      if not read_only:
        self.create_document_tool = CreateDocumentTool(graph_client)
        self.update_document_tool = UpdateDocumentTool(graph_client)
        self.delete_document_tool = DeleteDocumentTool(graph_client)

    # Hand-written: needs both the platform and the tenant extensions session,
    # which the registrar runner doesn't pass.
    self.bind_text_block_tool = None
    if (
      not read_only
      and not self._is_shared_repository()
      and "roboledger" in self.schema_extensions
    ):
      from .text_block_tools import BindTextBlockTool

      self.bind_text_block_tool = BindTextBlockTool(graph_client)

    # Hand-written: removes the report's published artifacts after the rows
    # commit, which the registrar runner doesn't do.
    self.delete_report_tool = None
    if (
      not read_only
      and not self._is_shared_repository()
      and "roboledger" in self.schema_extensions
    ):
      from .report_tools import DeleteReportTool

      self.delete_report_tool = DeleteReportTool(graph_client)

    self._cache_hits = 0
    self._cache_misses = 0

    # ── Registrar-generated tools ──────────────────────────────────────
    self._cached_meta: GraphExtensionContext | None = None
    self._registrar_dispatch: dict[str, _RegistrarMCPTool] = {}
    if not read_only:
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
    """Graph metadata, loaded once per handler; None on failure (the gate reloads)."""
    if self._cached_meta is not None:
      return self._cached_meta
    try:
      # Independent session (see platform_session docs): the scoped session
      # would resolve to the request's own and closing it here would close it.
      from robosystems.database import SessionFactory
      from robosystems.middleware.extensions import load_graph_metadata

      session = SessionFactory()
      try:
        self._cached_meta = load_graph_metadata(self.client.graph_id, session)
      finally:
        session.close()
    except Exception:
      logger.debug(
        "Graph metadata preload failed for %s; registrar tools will fall back to per-call load",
        getattr(self.client, "graph_id", "unknown"),
      )
    return self._cached_meta

  def _has_extension(self, extension: str) -> bool:
    return extension in self.schema_extensions

  def _is_shared_repository(self) -> bool:
    try:
      from robosystems.config.shared_repositories import (
        is_shared_repository_or_subgraph,
      )

      return is_shared_repository_or_subgraph(self.client.graph_id)
    except Exception:
      return False

  def _is_subgraph(self) -> bool:
    """Subgraph writes are exclusive with materialization, which would discard
    them. MCP bypasses FastAPI DI, so the REST gate is mirrored here."""
    try:
      from robosystems.middleware.graph.utils import is_subgraph

      return is_subgraph(self.client.graph_id)
    except Exception:
      return False

  def _is_tenant_subgraph(self) -> bool:
    """Excludes shared-repo subgraphs, which take the `_is_shared_repository` cut."""
    if self._is_shared_repository():
      return False
    try:
      from robosystems.middleware.graph.utils import is_subgraph

      return is_subgraph(self.client.graph_id)
    except Exception:
      return False

  def _should_include_semantic_tools(self) -> bool:
    """True when the (parent) manifest declares has_semantic_enrichment."""
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
    if self.resolve_element_tool is None:
      return []
    return [
      self.resolve_element_tool.get_tool_definition(),
    ]

  def _get_navigation_tool_definitions(self) -> list[dict[str, Any]]:
    tools = []
    if self.list_subgraphs_tool is not None:
      tools.append(self.list_subgraphs_tool.get_tool_definition())
    if self.create_subgraph_tool is not None:
      tools.append(self.create_subgraph_tool.get_tool_definition())
    if self.delete_subgraph_tool is not None:
      tools.append(self.delete_subgraph_tool.get_tool_definition())
    if self.create_backup_tool is not None:
      tools.append(self.create_backup_tool.get_tool_definition())
    return tools

  def _get_subgraph_write_tool_definitions(self) -> list[dict[str, Any]]:
    if self.write_cypher_tool is None:
      return []
    return [
      self.write_cypher_tool.get_tool_definition(),
      self.add_node_table_tool.get_tool_definition(),
      self.add_relationship_table_tool.get_tool_definition(),
    ]

  def _get_semantic_memory_tool_definitions(self) -> list[dict[str, Any]]:
    tools = []
    if self.semantic_recall_tool is not None:
      tools.append(self.semantic_recall_tool.get_tool_definition())
    if self.semantic_remember_tool is not None:
      tools.append(self.semantic_remember_tool.get_tool_definition())
    if self.semantic_update_memory_tool is not None:
      tools.append(self.semantic_update_memory_tool.get_tool_definition())
    if self.semantic_forget_tool is not None:
      tools.append(self.semantic_forget_tool.get_tool_definition())
    return tools

  def _get_fact_grid_tool_definitions(self) -> list[dict[str, Any]]:
    tools = []
    if self.build_fact_grid_tool is not None:
      tools.append(self.build_fact_grid_tool.get_tool_definition())
    return tools

  def _get_materialization_tool_definitions(self) -> list[dict[str, Any]]:
    tools = []
    if self.get_graph_sync_status_tool is not None:
      tools.append(self.get_graph_sync_status_tool.get_tool_definition())
    if self.materialize_tool is not None:
      tools.append(self.materialize_tool.get_tool_definition())
    return tools

  def _get_schedule_tool_definitions(self) -> list[dict[str, Any]]:
    tools = []
    # Playbook first: it tells the agent how the rest compose.
    if self.get_close_playbook_tool is not None:
      tools.append(self.get_close_playbook_tool.get_tool_definition())
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
    if self.backfill_plan_history_tool is not None:
      tools.append(self.backfill_plan_history_tool.get_tool_definition())
    return tools

  def _get_taxonomy_tool_definitions(self) -> list[dict[str, Any]]:
    tools = []
    if self.list_mapping_structures_tool is not None:
      tools.append(self.list_mapping_structures_tool.get_tool_definition())
    if self.get_unmapped_elements_tool is not None:
      tools.append(self.get_unmapped_elements_tool.get_tool_definition())
    if self.suggest_mapping_tool is not None:
      tools.append(self.suggest_mapping_tool.get_tool_definition())
    if self.get_mapping_summary_tool is not None:
      tools.append(self.get_mapping_summary_tool.get_tool_definition())
    return tools

  def _get_information_block_tool_definitions(self) -> list[dict[str, Any]]:
    tools = []
    if self.get_information_block_tool is not None:
      tools.append(self.get_information_block_tool.get_tool_definition())
    if self.list_information_blocks_tool is not None:
      tools.append(self.list_information_blocks_tool.get_tool_definition())
    return tools

  def _get_agent_tool_definitions(self) -> list[dict[str, Any]]:
    tools = []
    if self.get_agent_tool is not None:
      tools.append(self.get_agent_tool.get_tool_definition())
    if self.list_agents_tool is not None:
      tools.append(self.list_agents_tool.get_tool_definition())
    if self.agent_activity_tool is not None:
      tools.append(self.agent_activity_tool.get_tool_definition())
    return tools

  def _get_event_handler_tool_definitions(self) -> list[dict[str, Any]]:
    tools = []
    if self.get_event_handler_tool is not None:
      tools.append(self.get_event_handler_tool.get_tool_definition())
    if self.list_event_handlers_tool is not None:
      tools.append(self.list_event_handlers_tool.get_tool_definition())
    return tools

  def _get_event_block_tool_definitions(self) -> list[dict[str, Any]]:
    tools = []
    if self.get_event_block_tool is not None:
      tools.append(self.get_event_block_tool.get_tool_definition())
    if self.list_event_blocks_tool is not None:
      tools.append(self.list_event_blocks_tool.get_tool_definition())
    return tools

  def _get_search_tool_definitions(self) -> list[dict[str, Any]]:
    tools = []
    if self.search_documents_tool is not None:
      tools.append(self.search_documents_tool.get_tool_definition())
    if self.get_document_section_tool is not None:
      tools.append(self.get_document_section_tool.get_tool_definition())
    return tools

  def _get_curated_tool_definitions(self) -> list[dict[str, Any]]:
    tools: list[dict[str, Any]] = []
    if self.financial_statement_analysis_tool is not None:
      tools.append(self.financial_statement_analysis_tool.get_tool_definition())
    if self.live_financial_statement_tool is not None:
      tools.append(self.live_financial_statement_tool.get_tool_definition())
    if self.disclosures_tool is not None:
      tools.append(self.disclosures_tool.get_tool_definition())
    if self.information_block_tool is not None:
      tools.append(self.information_block_tool.get_tool_definition())
    return tools

  def _tool_unavailable_reason(self, tool_name: str, feature_flag: str) -> str:
    if self.read_only:
      return f"{tool_name} is not available on this read-only graph."
    return f"{tool_name} tool is not available. Set {feature_flag}=true to enable this feature."

  def get_tool_definitions_as_dict(self) -> list[dict[str, Any]]:
    tools = [
      self.cypher_tool.get_tool_definition(),
      self.schema_tool.get_tool_definition(),
    ]

    if self.graphql_schema_tool is not None and env.MCP_GRAPHQL_ENABLED:
      tools.append(self.graphql_schema_tool.get_tool_definition())
    if self.graphql_query_tool is not None and env.MCP_GRAPHQL_ENABLED:
      tools.append(self.graphql_query_tool.get_tool_definition())

    if self._has_extension("roboledger"):
      tools.append(self.example_queries_tool.get_tool_definition())
      tools.extend(self._get_semantic_tool_definitions())
      tools.extend(self._get_curated_tool_definitions())
      tools.extend(self._get_fact_grid_tool_definitions())
      tools.extend(self._get_materialization_tool_definitions())
      tools.extend(self._get_schedule_tool_definitions())
      tools.extend(self._get_taxonomy_tool_definitions())
      tools.extend(self._get_information_block_tool_definitions())
      tools.extend(self._get_agent_tool_definitions())
      tools.extend(self._get_event_handler_tool_definitions())
      tools.extend(self._get_event_block_tool_definitions())

    tools.extend(self._get_navigation_tool_definitions())
    if self.set_write_policy_tool is not None:
      tools.append(self.set_write_policy_tool.get_tool_definition())
    if self.sync_connection_tool is not None:
      tools.append(self.sync_connection_tool.get_tool_definition())
    tools.extend(self._get_subgraph_write_tool_definitions())
    tools.extend(self._get_semantic_memory_tool_definitions())
    tools.extend(self._get_search_tool_definitions())
    tools.extend(self._get_document_tool_definitions())

    for tool in self._registrar_dispatch.values():
      tools.append(tool.get_tool_definition())

    # Filtered over the assembled list so a tool added later is excluded
    # by default.
    if self._is_tenant_subgraph():
      tools = [t for t in tools if t.get("name") in SUBGRAPH_TOOL_PROFILE]

    return tools

  def _get_document_tool_definitions(self) -> list[dict[str, Any]]:
    tools = []
    if self.create_document_tool is not None:
      tools.append(self.create_document_tool.get_tool_definition())
    if self.update_document_tool is not None:
      tools.append(self.update_document_tool.get_tool_definition())
    if self.delete_document_tool is not None:
      tools.append(self.delete_document_tool.get_tool_definition())
    if self.get_document_tool is not None:
      tools.append(self.get_document_tool.get_tool_definition())
    if self.list_documents_tool is not None:
      tools.append(self.list_documents_tool.get_tool_definition())
    if self.bind_text_block_tool is not None:
      tools.append(self.bind_text_block_tool.get_tool_definition())
    if self.delete_report_tool is not None:
      tools.append(self.delete_report_tool.get_tool_definition())
    return tools

  async def call_tool(
    self, name: str, arguments: dict[str, Any], return_raw: bool = False
  ) -> Any:
    """Dispatch by name; `return_raw` returns the native result, not JSON."""
    # A client with a stale tool list can still call a tool this graph no
    # longer advertises; answer it instead of failing in the OLTP validator.
    if self._is_tenant_subgraph() and name not in SUBGRAPH_TOOL_PROFILE:
      result = {
        "error": "not_applicable_on_subgraph",
        "message": (
          f"'{name}' is not available on a subgraph. Subgraphs are graph "
          f"databases — schema and Cypher — with no ledger tables, "
          f"connections, or materialization of their own. Run this against "
          f"the parent graph."
        ),
        "graph_id": self.client.graph_id,
        "available_tools": sorted(SUBGRAPH_TOOL_PROFILE),
      }
      return result if return_raw else json.dumps(result, indent=2)

    try:
      # Registrar tools first; they carry their own extension gate.
      registrar_tool = self._registrar_dispatch.get(name)
      if registrar_tool is not None:
        result = await registrar_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      if name == "read-graph-cypher":
        result = await self.cypher_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      elif name == "get-graph-schema":
        result = await self.schema_tool.execute(arguments)

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

      elif name == "get-graphql-schema":
        if self.graphql_schema_tool is None:
          raise ValueError(
            "get-graphql-schema is not available. "
            "Set EXTENSIONS_GRAPHQL_ENABLED=true to enable this feature."
          )
        if not env.MCP_GRAPHQL_ENABLED:
          raise ValueError(
            "get-graphql-schema is temporarily disabled. "
            "Set MCP_GRAPHQL_ENABLED=true to re-enable."
          )
        result = await self.graphql_schema_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      elif name == "query-graphql":
        if self.graphql_query_tool is None:
          raise ValueError(
            "query-graphql is not available. "
            "Set EXTENSIONS_GRAPHQL_ENABLED=true to enable this feature."
          )
        if not env.MCP_GRAPHQL_ENABLED:
          raise ValueError(
            "query-graphql is temporarily disabled. "
            "Set MCP_GRAPHQL_ENABLED=true to re-enable."
          )
        result = await self.graphql_query_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

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

      elif name == "disclosures":
        if self.disclosures_tool is None:
          raise ValueError(
            "disclosures tool is not available. "
            "This graph does not have the roboledger schema extension."
          )
        result = await self.disclosures_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      elif name == "information-block":
        if self.information_block_tool is None:
          raise ValueError(
            "information-block tool is not available. "
            "This graph does not have the roboledger schema extension."
          )
        result = await self.information_block_tool.execute(arguments)
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

      elif name == "create-backup":
        if self.create_backup_tool is None:
          raise ValueError(
            self._tool_unavailable_reason("create-backup", "MCP_WORKSPACE_ENABLED")
          )
        result = await self.create_backup_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      elif name == "set-write-policy":
        if self.set_write_policy_tool is None:
          raise ValueError(
            "set-write-policy tool is not available on this read-only or "
            "shared-repository graph."
          )
        result = await self.set_write_policy_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      elif name == "sync-connection":
        if self.sync_connection_tool is None:
          raise ValueError(
            "sync-connection tool is not available on this read-only or "
            "shared-repository graph."
          )
        result = await self.sync_connection_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      elif name == "write-graph-cypher":
        if self.write_cypher_tool is None:
          raise ValueError(
            self._tool_unavailable_reason(
              "write-graph-cypher", "MCP_SUBGRAPH_OPS_ENABLED"
            )
          )
        result = await self.write_cypher_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      elif name == "add-node-table":
        if self.add_node_table_tool is None:
          raise ValueError(
            self._tool_unavailable_reason("add-node-table", "MCP_SUBGRAPH_OPS_ENABLED")
          )
        result = await self.add_node_table_tool.execute(arguments)
        if isinstance(result, dict) and result.get("success"):
          self.clear_schema_cache()
        return result if return_raw else json.dumps(result, indent=2)

      elif name == "add-relationship-table":
        if self.add_relationship_table_tool is None:
          raise ValueError(
            self._tool_unavailable_reason(
              "add-relationship-table", "MCP_SUBGRAPH_OPS_ENABLED"
            )
          )
        result = await self.add_relationship_table_tool.execute(arguments)
        if isinstance(result, dict) and result.get("success"):
          self.clear_schema_cache()
        return result if return_raw else json.dumps(result, indent=2)

      elif name == "remember":
        if self.semantic_remember_tool is None:
          raise ValueError(
            self._tool_unavailable_reason("remember", "MCP_SEMANTIC_MEMORY_ENABLED")
          )
        result = await self.semantic_remember_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      elif name == "recall":
        if self.semantic_recall_tool is None:
          raise ValueError(
            self._tool_unavailable_reason("recall", "MCP_SEMANTIC_MEMORY_ENABLED")
          )
        result = await self.semantic_recall_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      elif name == "update-memory":
        if self.semantic_update_memory_tool is None:
          raise ValueError(
            self._tool_unavailable_reason(
              "update-memory", "MCP_SEMANTIC_MEMORY_ENABLED"
            )
          )
        result = await self.semantic_update_memory_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      elif name == "forget":
        if self.semantic_forget_tool is None:
          raise ValueError(
            self._tool_unavailable_reason("forget", "MCP_SEMANTIC_MEMORY_ENABLED")
          )
        result = await self.semantic_forget_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      elif name == "build-fact-grid":
        if self.build_fact_grid_tool is None:
          raise ValueError(
            "build-fact-grid tool is not available. "
            "Set FACT_GRID_ENABLED=true to enable this feature."
          )
        result = await self.build_fact_grid_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

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

      elif name == "get-close-playbook":
        if self.get_close_playbook_tool is None:
          raise ValueError(
            "get-close-playbook tool is not available. "
            "Requires roboledger extension and ROBOLEDGER_ENABLED=true."
          )
        result = await self.get_close_playbook_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

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

      elif name == "get-agent":
        if self.get_agent_tool is None:
          raise ValueError(
            "get-agent tool is not available. "
            "Requires roboledger extension and ROBOLEDGER_ENABLED=true."
          )
        result = await self.get_agent_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      elif name == "list-agents":
        if self.list_agents_tool is None:
          raise ValueError(
            "list-agents tool is not available. "
            "Requires roboledger extension and ROBOLEDGER_ENABLED=true."
          )
        result = await self.list_agents_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      elif name == "agent-activity":
        if self.agent_activity_tool is None:
          raise ValueError(
            "agent-activity tool is not available. "
            "Requires roboledger extension and ROBOLEDGER_ENABLED=true."
          )
        result = await self.agent_activity_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      elif name == "get-event-handler":
        if self.get_event_handler_tool is None:
          raise ValueError(
            "get-event-handler tool is not available. "
            "Requires roboledger extension and ROBOLEDGER_ENABLED=true."
          )
        result = await self.get_event_handler_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      elif name == "list-event-handlers":
        if self.list_event_handlers_tool is None:
          raise ValueError(
            "list-event-handlers tool is not available. "
            "Requires roboledger extension and ROBOLEDGER_ENABLED=true."
          )
        result = await self.list_event_handlers_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

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

      elif name == "backfill-plan-history":
        if self.backfill_plan_history_tool is None:
          raise ValueError(
            "backfill-plan-history tool is not available. "
            "Requires roboledger extension and ROBOLEDGER_ENABLED=true."
          )
        result = await self.backfill_plan_history_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

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

      elif name == "list-mapping-structures":
        if self.list_mapping_structures_tool is None:
          raise ValueError(
            "list-mapping-structures tool is not available. "
            "Requires roboledger extension and ROBOLEDGER_ENABLED=true."
          )
        result = await self.list_mapping_structures_tool.execute(arguments)
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

      elif name == "bind-text-block":
        if self.bind_text_block_tool is None:
          raise ValueError(
            "bind-text-block requires a writable roboledger graph "
            "(not available on shared repositories or read-only access)"
          )
        result = await self.bind_text_block_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

      elif name == "delete-report":
        if self.delete_report_tool is None:
          raise ValueError(
            "delete-report requires a writable roboledger graph "
            "(not available on shared repositories or read-only access)"
          )
        result = await self.delete_report_tool.execute(arguments)
        return result if return_raw else json.dumps(result, indent=2)

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

      elif name == "delete-document":
        if self.delete_document_tool is None:
          raise ValueError(
            self._tool_unavailable_reason("delete-document", "SEMANTIC_SEARCH_ENABLED")
          )
        result = await self.delete_document_tool.execute(arguments)
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
      error_context = self._build_error_context(name, arguments, e)
      error_msg = str(e)

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
        raise
      return f"Timeout: {error_msg}"

    except GraphQueryComplexityError as e:
      error_context = self._build_error_context(name, arguments, e)
      error_msg = str(e)

      if hasattr(e, "details") and "complexity_score" in e.details:
        score = e.details["complexity_score"]
        error_msg += f"\n💡 Complexity score: {score}. Consider simplifying the query."

      logger.error(
        f"Query complexity error in tool '{name}': {error_msg}",
        extra={"error_context": error_context},
      )
      if return_raw:
        raise
      return f"Complexity Error: {error_msg}"

    except GraphAPIError as e:
      error_msg = str(e)
      error_context = self._build_error_context(name, arguments, e)
      logger.error(
        f"Graph API error in tool '{name}': {error_msg}",
        extra={"error_context": error_context},
      )

      enhanced_msg = self._enhance_error_message(error_msg, name, arguments)

      if return_raw:
        e.args = (enhanced_msg, *e.args[1:]) if len(e.args) > 1 else (enhanced_msg,)
        if hasattr(e, "details"):
          e.details = {**e.details, **error_context}
        raise
      return f"Error: {enhanced_msg}"

    except ValueError as e:
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
      error_context = self._build_error_context(name, arguments, e)
      error_msg = self._sanitize_error_message(str(e))

      logger.error(
        f"Tool execution failed for '{name}': {error_msg}",
        extra={"error_context": error_context, "exception_type": type(e).__name__},
      )

      if return_raw:
        raise GraphAPIError(f"Tool execution failed: {error_msg}")
      return f"Error: {error_msg}"

  def _build_error_context(
    self, tool_name: str, arguments: dict[str, Any], exception: Exception
  ) -> dict[str, Any]:
    context: dict[str, Any] = {
      "tool_name": tool_name,
      "graph_id": self.client.graph_id,
      "exception_type": type(exception).__name__,
    }

    if arguments:
      # Query metadata only, never its content.
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

    if hasattr(exception, "error_code"):
      context["error_code"] = exception.error_code
    if hasattr(exception, "details"):
      context["exception_details"] = exception.details

    return context

  def _enhance_error_message(
    self, error_msg: str, tool_name: str, arguments: dict[str, Any]
  ) -> str:
    enhanced_msg = error_msg

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

    if "unauthorized" in error_msg.lower() or "forbidden" in error_msg.lower():
      enhanced_msg += "\n\n🔐 Check API permissions and authentication credentials."

    elif "rate limit" in error_msg.lower():
      enhanced_msg += (
        "\n\n⏱️ API rate limit exceeded. Wait before retrying or reduce query frequency."
      )

    return enhanced_msg

  def _sanitize_error_message(self, error_msg: str) -> str:
    """Strip file paths and credentials out of an error message."""
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
    """Log final cache statistics."""
    stats = self.get_cache_stats()
    logger.info(
      f"MCP Tools cache stats - Hits: {stats['cache_hits']}, "
      f"Misses: {stats['cache_misses']}, Hit Rate: {stats['hit_rate_percent']:.1f}%"
    )
