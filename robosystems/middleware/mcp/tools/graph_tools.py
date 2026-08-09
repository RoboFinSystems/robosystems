"""Graph lifecycle MCP tools.

Five tools that mirror a subset of the REST graph lifecycle surface at
`POST /v1/graphs/{graph_id}/operations/*`:

1. `create-subgraph` — isolated workspace within the parent graph
2. `delete-subgraph` — hard-delete a subgraph (admin on parent)
3. `list-subgraphs` — enumerate subgraphs for the current parent graph
4. `materialize` — rebuild LadybugDB from OLTP (extensions) or staging
5. `create-backup` — enqueue a full-dump backup (admin)

Plus two platform-DB connection tools that share the same hand-written
rationale (platform DB, graph-scoped auth, no extensions registrar):

6. `set-write-policy` — opt a connection into / out of outbound write-back
7. `sync-connection` — trigger a provider resync (the write half of the
   sync-freshness pair; `get-fiscal-calendar` / `get-graph-sync-status`
   are the read half)

**Deliberately NOT exposed on MCP:**

- `change-tier` — destructive 3-5 minute EBS migration with billing
  implications and fail-on-downgrade semantics. Humans execute it on the
  REST surface.

Restore is not on MCP either, but for a different reason: it is not exposed
anywhere customer-facing. Backups are a download capability — every graph type
with an upstream rebuilds from that upstream, and the classes without one are
recovered by downloading the payload and rebuilding, or by an operator-run
restore job.

Tools stay hand-written rather than registrar-generated because:
- Each targets the **platform DB** (not extensions), different session
- Auth rules differ per op: admin-on-parent, entity-graph checks
- Several dispatch async Dagster jobs and return an operation_id that
  the agent can poll via `/v1/operations/{operation_id}/stream`

Tool descriptions frame each operation for an agent — what a subgraph *is*,
when a backup is the right move — rather than restating the operation name.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any

from robosystems.logger import logger

# ══════════════════════════════════════════════════════════════════════════
# Shared helpers
# ══════════════════════════════════════════════════════════════════════════


def _open_platform_session():
  """Open a short-lived platform-DB session.

  MCP tools don't receive a FastAPI-injected session, so each helper
  opens its own generator-backed session and closes it on exit.
  """
  from robosystems.database import get_db_session

  gen = get_db_session()
  session = next(gen)

  def close():
    try:
      next(gen)
    except StopIteration:
      pass

  return session, close


def _require_user(client) -> Any | None:
  """Fetch `client.user` or return an auth-missing error dict."""
  user = getattr(client, "user", None)
  if user is None:
    return None
  return user


def _user_missing_err() -> dict[str, Any]:
  return {
    "error": "authentication_required",
    "message": "User context required for this operation.",
  }


def _block_shared_repo(graph_id: str) -> dict[str, Any] | None:
  """Return an error envelope if the target is a shared repository."""
  from robosystems.config.shared_repositories import is_shared_repository_or_subgraph

  if is_shared_repository_or_subgraph(graph_id):
    return {
      "error": "not_allowed_on_shared_repo",
      "message": (
        f"This operation is not available on shared repository graphs ({graph_id})."
      ),
    }
  return None


def _verify_admin_on_graph(user, graph_id: str, session) -> dict[str, Any] | None:
  """Check that `user` has an `admin` role on `graph_id`."""
  from robosystems.models.core.graph.graph_user import GraphUser

  if not GraphUser.user_has_admin_access(user.id, graph_id, session):
    return {
      "error": "insufficient_permissions",
      "message": f"Admin access to graph '{graph_id}' required for this operation.",
    }
  return None


# ══════════════════════════════════════════════════════════════════════════
# create-subgraph
# ══════════════════════════════════════════════════════════════════════════


class CreateSubgraphTool:
  """Create an isolated subgraph (workspace) under the current parent graph."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "create-subgraph",
      "description": (
        "Create an isolated subgraph under this parent graph — think of it "
        "as a sandbox or workspace that inherits the schema but keeps data "
        "separate. Use for experiments, scratch work, staging data, or "
        "agent knowledge/scratch contexts without affecting the main graph.\n\n"
        "**WHEN TO USE:**\n"
        "- Creating an experimentation sandbox that won't pollute the main graph\n"
        "- Staging data before promoting to the primary graph\n"
        "- Isolating agent scratch/knowledge work in a separate namespace\n\n"
        "**PARAMETERS:**\n"
        "- name: Alphanumeric only, 1-20 chars (no hyphens or underscores)\n"
        "- fork_parent: Copy all parent data into the new subgraph\n"
        "- subgraph_type: `static` (default), `knowledge` (auto-includes the knowledge schema), or `empty` (bare database, no schema — define your own)\n\n"
        "**RETURNS:** `subgraph_id` (format: `{parent_graph_id}_{name}`) and "
        "`connector_url`. A subgraph is a separate MCP endpoint, not a mode of "
        "this one: this connector is anchored to its own graph by URL and "
        "cannot reach the new subgraph. To work in it, add an MCP connector "
        "for `connector_url` — the API key this connector already uses covers "
        "its own subgraphs, so reuse it as-is rather than generating a new one."
      ),
      "inputSchema": {
        "type": "object",
        "properties": {
          "name": {
            "type": "string",
            "description": "Subgraph name (alphanumeric, 1-20 chars).",
          },
          "description": {
            "type": "string",
            "description": "Optional description.",
          },
          "fork_parent": {
            "type": "boolean",
            "description": "Copy data from the parent graph (default false).",
            "default": False,
          },
          "subgraph_type": {
            "type": "string",
            "enum": ["static", "knowledge"],
            "default": "static",
          },
        },
        "required": ["name"],
        "additionalProperties": False,
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> dict[str, Any]:
    from robosystems.models.core.graph import Graph
    from robosystems.operations.graph.subgraph_service import SubgraphService

    name = arguments.get("name") or ""
    description = arguments.get("description") or f"MCP subgraph: {name}"
    fork_parent = bool(arguments.get("fork_parent", False))
    subgraph_type = arguments.get("subgraph_type", "static")

    from robosystems.middleware.graph.utils.subgraph import validate_subgraph_name

    # Use the canonical ASCII validator (^[a-zA-Z0-9]{1,20}$) rather than
    # Unicode-permissive str.isalnum(), which admits names like "café" that the
    # downstream construct_subgraph_id then rejects with a generic error.
    if not validate_subgraph_name(name):
      return {
        "error": "invalid_name",
        "message": (
          "Subgraph name must be alphanumeric only, 1-20 characters "
          "(no hyphens or underscores)."
        ),
        "valid_examples": ["dev", "staging", "test1"],
      }

    user = _require_user(self.client)
    if user is None:
      return _user_missing_err()

    parent_graph_id = self.client.graph_id
    repo_err = _block_shared_repo(parent_graph_id)
    if repo_err:
      return repo_err

    session, close = _open_platform_session()
    try:
      parent = session.query(Graph).filter(Graph.graph_id == parent_graph_id).first()
      if not parent:
        return {
          "error": "parent_not_found",
          "message": f"Parent graph {parent_graph_id} not found.",
        }

      service = SubgraphService()
      try:
        result = await service.create_subgraph(
          parent_graph=parent,
          user=user,
          name=name,
          description=description,
          subgraph_type=subgraph_type,
          metadata={},
          fork_parent=fork_parent,
          fork_options=None,
        )
      except Exception as exc:
        logger.error("create-subgraph failed for %s: %s", parent_graph_id, exc)
        return {"error": "create_failed", "message": str(exc)}

      # An id with no way to reach it is the whole friction here: under the
      # stdio bridge you created then switched in-session, but a remote
      # connector is URL-anchored, so the caller needs the address and the
      # (already-satisfied) credential answer handed to them, not inferred.
      from robosystems.config import env

      subgraph_id = result.get("graph_id")
      return {
        "subgraph_id": subgraph_id,
        "name": name,
        "parent_graph_id": parent_graph_id,
        "description": description,
        "forked_from_parent": fork_parent,
        "subgraph_type": subgraph_type,
        "operation_id": result.get("operation_id"),
        "connector_url": f"{env.ROBOSYSTEMS_API_URL}/v1/graphs/{subgraph_id}/mcp",
        "credential": (
          "Reuse this connector's API key — a key scoped to "
          f"'{parent_graph_id}' covers its subgraphs. No new key needed."
        ),
        "next_step": (
          "Add an MCP connector for connector_url. This connector is anchored "
          "to its own graph by URL and cannot switch to the new subgraph."
        ),
      }
    finally:
      close()


# ══════════════════════════════════════════════════════════════════════════
# delete-subgraph
# ══════════════════════════════════════════════════════════════════════════


class DeleteSubgraphTool:
  """Delete a subgraph (admin on parent)."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "delete-subgraph",
      "description": (
        "Hard-delete a subgraph and all its data. Cannot target the parent "
        "graph. Requires admin role on the parent. Use `backup_first=true` "
        "to create a safety snapshot before deletion.\n\n"
        "**PARAMETERS:**\n"
        "- subgraph_id: Full id like `{parent_id}_{name}`\n"
        "- force: Delete even if the subgraph contains data (default false)\n"
        "- backup_first: Create a backup before deletion (default true)"
      ),
      "inputSchema": {
        "type": "object",
        "properties": {
          "subgraph_id": {
            "type": "string",
            "description": "Subgraph id, e.g. `kg123_dev`.",
          },
          "force": {"type": "boolean", "default": False},
          "backup_first": {"type": "boolean", "default": True},
        },
        "required": ["subgraph_id"],
        "additionalProperties": False,
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> dict[str, Any]:
    from robosystems.middleware.graph.utils import parse_subgraph_id
    from robosystems.models.core.graph import Graph
    from robosystems.operations.graph.subgraph_service import SubgraphService

    subgraph_id = arguments.get("subgraph_id") or ""
    force = bool(arguments.get("force", False))
    backup_first = bool(arguments.get("backup_first", True))

    info = parse_subgraph_id(subgraph_id)
    if info is None:
      return {
        "error": "invalid_subgraph_id",
        "message": f"{subgraph_id!r} is not a valid subgraph identifier.",
      }

    # The client's current graph must be the subgraph's parent — prevents
    # cross-tenant delete via a mismatched `graph_id`/`subgraph_id`.
    current_parent = self.client.graph_id
    if info.parent_graph_id != current_parent:
      return {
        "error": "authorization_failed",
        "message": (
          f"Subgraph {subgraph_id} does not belong to graph {current_parent}."
        ),
      }

    # Shared-repo subgraphs (e.g., `sec_historical`) are platform-managed.
    # The manager normally skips write-tool registration for shared repos
    # (read_only=True), so this is defense-in-depth against a mistake in
    # that gating — and makes the error message specific.
    repo_err = _block_shared_repo(current_parent)
    if repo_err:
      return repo_err

    user = _require_user(self.client)
    if user is None:
      return _user_missing_err()

    session, close = _open_platform_session()
    try:
      subgraph = session.query(Graph).filter(Graph.graph_id == subgraph_id).first()
      if subgraph is None or not subgraph.is_subgraph:
        return {
          "error": "subgraph_not_found",
          "message": f"Subgraph {subgraph_id} not found.",
        }

      admin_err = _verify_admin_on_graph(user, info.parent_graph_id, session)
      if admin_err:
        return admin_err

      service = SubgraphService()
      try:
        await service.delete_subgraph_database(
          subgraph_id=subgraph_id,
          force=force,
          create_backup=backup_first,
        )
      except Exception as exc:
        logger.error("delete-subgraph failed for %s: %s", subgraph_id, exc)
        hint = None
        if "contains data" in str(exc).lower():
          hint = "Subgraph contains data. Set force=true to delete anyway."
        return {
          "error": "deletion_failed",
          "message": str(exc),
          "hint": hint,
        }

      session.delete(subgraph)
      session.commit()
      return {
        "deleted": True,
        "subgraph_id": subgraph_id,
        "backup_created": backup_first,
      }
    finally:
      close()


# ══════════════════════════════════════════════════════════════════════════
# list-subgraphs
# ══════════════════════════════════════════════════════════════════════════


class ListSubgraphsTool:
  """List subgraphs attached to the current parent graph."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "list-subgraphs",
      "description": (
        "List every subgraph under this parent graph, plus the parent itself "
        "as the `primary` entry. Each row carries `connector_url` — the MCP "
        "endpoint that serves that graph.\n\n"
        "A subgraph is a separate endpoint, not a mode of this one: this "
        "connector is anchored to its own graph by URL and cannot be "
        "retargeted. To work in a subgraph, add its `connector_url` as its "
        "own MCP connector.\n\n"
        "**Credential:** a key scoped to a parent graph also covers that "
        "parent's subgraphs, so a connector on the parent can reuse its own "
        "key on any subgraph listed here. Going the other way — from a "
        "subgraph to its parent or a sibling — a subgraph-scoped key does "
        "not reach, and a key for the target is generated from the app's MCP "
        "page (/connect)."
      ),
      "inputSchema": {
        "type": "object",
        "properties": {},
        "additionalProperties": False,
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> dict[str, Any]:
    from robosystems.config import env
    from robosystems.models.core.graph import Graph

    parent_id = self.client.graph_id

    def connector_url(gid: str) -> str:
      return f"{env.ROBOSYSTEMS_API_URL}/v1/graphs/{gid}/mcp"

    session, close = _open_platform_session()
    try:
      subgraphs = (
        session.query(Graph)
        .filter(Graph.parent_graph_id == parent_id)
        .order_by(Graph.created_at.desc())
        .all()
      )

      out: list[dict[str, Any]] = []
      parent = session.query(Graph).filter(Graph.graph_id == parent_id).first()
      out.append(
        {
          "subgraph_id": parent_id,
          "name": "main",
          "description": parent.graph_name if parent else "Primary graph",
          "type": "primary",
          "parent_graph_id": None,
          "connector_url": connector_url(parent_id),
        }
      )
      for sg in subgraphs:
        out.append(
          {
            "subgraph_id": sg.graph_id,
            "name": sg.subgraph_name or sg.graph_name,
            "description": sg.graph_name,
            "type": "subgraph",
            "parent_graph_id": parent_id,
            "created_at": sg.created_at.isoformat() if sg.created_at else None,
            "connector_url": connector_url(sg.graph_id),
          }
        )

      return {
        "primary_graph_id": parent_id,
        "total_subgraphs": len(out) - 1,
        "subgraphs": out,
      }
    finally:
      close()


# ══════════════════════════════════════════════════════════════════════════
# materialize
# ══════════════════════════════════════════════════════════════════════════


class MaterializeTool:
  """Rebuild the graph database (LadybugDB) from OLTP or staging tables."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "materialize",
      "description": (
        "Rebuild LadybugDB for this graph from its source data — either "
        "staged DuckDB tables (for file uploads) or the extensions OLTP "
        "database (roboledger / roboinvestor writes).\n\n"
        "**WHEN TO USE:**\n"
        "- After significant OLTP writes, to refresh the queryable graph\n"
        "- After uploading and staging new files\n"
        "- When `get-graph-sync-status` reports `stale`\n"
        "- When the user asks to rebuild or refresh the graph\n\n"
        "**PARAMETERS:**\n"
        "- source: `extensions` (OLTP) or `staged` (DuckDB). Auto-detected from\n"
        "  graph_type if omitted.\n"
        "- dry_run: Validate without writing (returns a preview synchronously).\n"
        "- rebuild: Drop existing data and rebuild from scratch.\n"
        "- force: Materialize even if the graph is already up-to-date.\n"
        "- materialize_embeddings: Generate vector embeddings during rebuild.\n\n"
        "**RETURNS:** `operation_id` — subscribe to "
        "`/v1/operations/{operation_id}/stream` for progress events. For "
        "`dry_run`, returns a synchronous preview."
      ),
      "inputSchema": {
        "type": "object",
        "properties": {
          "source": {"type": "string", "enum": ["staged", "extensions"]},
          "dry_run": {"type": "boolean", "default": False},
          "rebuild": {"type": "boolean", "default": False},
          "force": {"type": "boolean", "default": False},
          "materialize_embeddings": {"type": "boolean", "default": False},
        },
        "additionalProperties": False,
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> dict[str, Any]:
    from robosystems.models.api.graphs.operations import MaterializeOp
    from robosystems.operations.graph.commands.materialize import materialize_cmd

    user = _require_user(self.client)
    if user is None:
      return _user_missing_err()

    graph_id = self.client.graph_id
    repo_err = _block_shared_repo(graph_id)
    if repo_err:
      return repo_err

    try:
      body = MaterializeOp.model_validate(arguments)
    except Exception as exc:
      return {"error": "invalid_arguments", "message": str(exc)}

    session, close = _open_platform_session()
    try:
      try:
        result = await materialize_cmd(graph_id, body, user, session)
      except Exception as exc:
        # Re-raise HTTPException details through the envelope; other
        # failures become a generic command_failed.
        detail = getattr(exc, "detail", None) or str(exc)
        status_code = getattr(exc, "status_code", None)
        if status_code:
          return {
            "error": "materialize_rejected",
            "status": status_code,
            "message": str(detail),
          }
        logger.error("materialize failed for %s: %s", graph_id, exc)
        return {"error": "command_failed", "message": str(detail)}

      if isinstance(result, dict):
        return result
      return result.model_dump(mode="json")
    finally:
      close()


# ══════════════════════════════════════════════════════════════════════════
# create-backup
# ══════════════════════════════════════════════════════════════════════════


class CreateBackupTool:
  """Enqueue a full-dump backup of the graph (admin)."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "create-backup",
      "description": (
        "Enqueue a full-dump backup of this graph. Async — returns an "
        "`operation_id`; subscribe to the SSE endpoint for progress.\n\n"
        "**WHEN TO USE:**\n"
        "- Before a destructive operation (tier change, restore, schema wipe)\n"
        "- On a scheduled basis for disaster-recovery hygiene\n"
        "- Before running large agent-driven workflows\n\n"
        "**PARAMETERS:**\n"
        "- retention_days: How long to keep the backup (default 30, capped at tier max)."
      ),
      "inputSchema": {
        "type": "object",
        "properties": {
          "retention_days": {
            "type": "integer",
            "minimum": 1,
            "maximum": 90,
            "default": 30,
          },
        },
        "additionalProperties": False,
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> dict[str, Any]:
    from robosystems.config import env
    from robosystems.config.graph_tier import GraphTierConfig
    from robosystems.middleware.sse import build_graph_job_config
    from robosystems.models.core import Graph
    from robosystems.worker.client import enqueue_task

    user = _require_user(self.client)
    if user is None:
      return _user_missing_err()

    graph_id = self.client.graph_id
    repo_err = _block_shared_repo(graph_id)
    if repo_err:
      return repo_err

    if not env.BACKUP_CREATION_ENABLED:
      return {
        "error": "backup_disabled",
        "message": "Backup creation is currently disabled.",
      }

    retention_days = int(arguments.get("retention_days", 30))

    session, close = _open_platform_session()
    try:
      admin_err = _verify_admin_on_graph(user, graph_id, session)
      if admin_err:
        return admin_err

      # Cap retention to the tier max. Unconditional, mirroring the REST
      # route: a missing graph row or tier falls back to the smallest tier's
      # cap rather than skipping the clamp. Skipping it would let an uncapped
      # value reach `expires_at` while the 90-day S3 lifecycle rule still
      # deletes the object, leaving a completed record pointing at nothing.
      graph_record = Graph.get_by_id(graph_id, session)
      backup_tier = (
        str(graph_record.graph_tier)
        if graph_record and graph_record.graph_tier
        else "ladybug-standard"
      )
      backup_limits = GraphTierConfig.get_backup_limits(backup_tier)
      tier_max = backup_limits.get("backup_retention_days", 7)
      retention_days = min(retention_days, tier_max)

      # Daily limit, mirroring the REST route for the same reason the clamp
      # above mirrors it: this path enqueues the same job, so a check that
      # lives on only one of them is not a limit. A negative value means
      # unlimited; an unresolvable tier gets the smallest allowance.
      max_per_day = backup_limits.get("max_backups_per_day", 2)
      if max_per_day is not None and max_per_day >= 0:
        from robosystems.models.core import GraphBackup

        taken_today = GraphBackup.count_user_initiated_today(graph_id, session)
        if taken_today >= max_per_day:
          return {
            "error": "daily_backup_limit_reached",
            "message": (
              f"Daily backup limit reached for this graph "
              f"({taken_today}/{max_per_day} on the {backup_tier} tier). "
              f"Scheduled backups do not count against this limit."
            ),
          }

      run_config = build_graph_job_config(
        "backup_graph_job",
        graph_id=graph_id,
        user_id=str(user.id),
        backup_type="full",
        backup_format="full_dump",
        retention_days=retention_days,
        compression=True,
        initiated_by="user",
      )
      response = await enqueue_task(
        task_type="dagster_job_monitor",
        graph_id=graph_id,
        user_id=str(user.id),
        params={"job_name": "backup_graph_job", "run_config": run_config},
      )
      operation_id = response["operation_id"]
      return {
        "status": "accepted",
        "operation_id": operation_id,
        "retention_days": retention_days,
        "message": (
          f"Backup started. Monitor via /v1/operations/{operation_id}/stream"
        ),
      }
    finally:
      close()


# ══════════════════════════════════════════════════════════════════════════
# get-graph-sync-status (moved from materialization_tools.py)
# ══════════════════════════════════════════════════════════════════════════


class GetGraphSyncStatusTool:
  """Check whether the LadybugDB graph is stale vs its OLTP source."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "get-graph-sync-status",
      "description": (
        "Check data freshness on BOTH sync edges: source system → OLTP "
        "(per-connection health + last sync outcome) and OLTP → LadybugDB "
        "graph (staleness / materialization).\n\n"
        "**WHEN TO USE:**\n"
        "- Before querying financial data, to verify the graph is current\n"
        "- After OLTP writes, to check if rematerialization is needed\n"
        "- After sync-connection, to verify the sync finished and see what "
        "it did (captured / updated / reconciling_items / dispatch_failed "
        "counts)\n"
        "- When the user asks about data freshness or a connection's health\n\n"
        "**RETURNS:** sync_status (`fresh`/`stale`), stale_since, "
        "stale_reason, stale_duration_minutes, last_materialized_at, "
        "hours_since_materialization, materialization_count, and "
        "`connections`: per source connection — provider, status "
        "(`connected` / `needs_reauth` = operator must re-OAuth / `error`), "
        "last_sync_at, and last_sync_result (the most recent attempt's "
        "outcome summary: status, window, counts, errors — `failed` status "
        "means the last attempt raised and last_sync_at did NOT advance)."
      ),
      "inputSchema": {
        "type": "object",
        "properties": {},
        "additionalProperties": False,
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> dict[str, Any]:
    from robosystems.models.core import Graph
    from robosystems.models.core.connection.connection import Connection

    graph_id = self.client.graph_id
    session, close = _open_platform_session()
    try:
      graph = Graph.get_by_id(graph_id, session)
      if graph is None:
        return {"error": "graph_not_found", "message": f"Graph {graph_id} not found."}

      # Source → OLTP edge: per-connection health + the last sync
      # attempt's outcome.
      connections = [
        {
          "connection_id": conn.id,
          "provider": conn.provider,
          "status": conn.status,
          "last_sync_at": conn.last_sync.isoformat() if conn.last_sync else None,
          "last_sync_result": conn.last_sync_result,
        }
        for conn in session.query(Connection)
        .filter(
          Connection.graph_id == graph_id,
          Connection.deleted_at.is_(None),
        )
        .all()
      ]

      is_stale = bool(graph.graph_stale)
      stale_reason = graph.graph_stale_reason
      stale_at = graph.graph_stale_at

      metadata = graph.graph_metadata or {}
      last_materialized_at = metadata.get("last_materialized_at")
      materialization_count = metadata.get("materialization_count", 0)

      stale_duration_minutes = None
      if is_stale and stale_at:
        stale_dt = stale_at
        if stale_dt.tzinfo is None:
          stale_dt = stale_dt.replace(tzinfo=UTC)
        stale_duration_minutes = round(
          (datetime.now(UTC) - stale_dt).total_seconds() / 60, 1
        )

      hours_since_materialization = None
      if last_materialized_at:
        try:
          from dateutil import parser as date_parser

          last_mat = date_parser.isoparse(last_materialized_at)
          hours_since_materialization = round(
            (datetime.now(UTC) - last_mat).total_seconds() / 3600, 1
          )
        except Exception as exc:
          logger.warning(
            "Could not parse last_materialized_at %r: %s", last_materialized_at, exc
          )

      return {
        "sync_status": "stale" if is_stale else "fresh",
        "stale_since": stale_at.isoformat() if stale_at else None,
        "stale_duration_minutes": stale_duration_minutes,
        "stale_reason": stale_reason,
        "last_materialized_at": last_materialized_at,
        "hours_since_materialization": hours_since_materialization,
        "materialization_count": materialization_count,
        "connections": connections,
      }
    finally:
      close()


# ══════════════════════════════════════════════════════════════════════════
# sync-connection
# ══════════════════════════════════════════════════════════════════════════


class SyncConnectionTool:
  """Trigger a provider resync for this graph's connection.

  The write half of the sync-freshness pair (`get-fiscal-calendar` /
  `get-graph-sync-status` are the read half). Platform-DB, hand-written
  for the same reason as `set-write-policy`: `Connection` lives in the
  platform DB and the op is graph-scoped via the service rather than the
  extensions registrar. Both this tool and the REST sync endpoint call
  the same `dispatch_connection_sync` kernel — same lock, same dispatch.
  """

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "sync-connection",
      "description": (
        "Pull fresh data from this graph's connected source system "
        "(e.g. QuickBooks) into the ledger. Async — dispatches the "
        "provider's sync pipeline and returns immediately.\n\n"
        "**WHEN TO USE:**\n"
        "- Before a period close when `get-fiscal-calendar` reports the "
        "`sync_stale` blocker (source sync older than period end) — sync "
        "first instead of passing `allow_stale_sync=true`\n"
        "- When the user says the source system has new transactions\n"
        "- After a code-level chart-of-accounts / mapping change that must "
        "back-propagate: set `full_rebuild=true`\n\n"
        "**PARAMETERS:**\n"
        "- connection_id: Omit to auto-resolve the graph's single sync "
        "connection; needed only when the graph has several (the error "
        "lists them).\n"
        "- provider: Optional auto-resolution filter (e.g. 'quickbooks').\n"
        "- full_rebuild: EXPENSIVE — re-ingests the entire source history "
        "and resets captured/classified state. Use only after a code-level "
        "CoA/mapping change that must back-propagate; the default "
        "incremental refresh is the common case.\n"
        "- since_date: Bounded incremental window (YYYY-MM-DD); ignored "
        "when full_rebuild=true.\n\n"
        "**COMPLETION:** returns `task_id` once dispatched; the sync runs "
        "in the background. Poll `get-fiscal-calendar` until "
        "`last_sync_at` advances past the dispatch time (and any "
        "`sync_stale` blocker clears) before proceeding with a close. A "
        "`sync_in_progress` error means a sync is already running — wait "
        "and poll rather than retrying."
      ),
      "inputSchema": {
        "type": "object",
        "properties": {
          "connection_id": {"type": "string"},
          "provider": {"type": "string"},
          "full_rebuild": {"type": "boolean", "default": False},
          "since_date": {"type": "string", "format": "date"},
        },
        "additionalProperties": False,
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> dict[str, Any]:
    from robosystems.operations.connection_service import (
      AmbiguousSyncConnectionError,
      ConnectionNotFoundError,
      NoSyncConnectionError,
      ProviderUnavailableError,
      SyncInProgressError,
      dispatch_connection_sync,
      resolve_sync_connection,
    )

    user = _require_user(self.client)
    if user is None:
      return _user_missing_err()

    graph_id = self.client.graph_id
    repo_err = _block_shared_repo(graph_id)
    if repo_err:
      return repo_err

    connection_id = arguments.get("connection_id") or None
    provider = arguments.get("provider") or None
    full_rebuild = bool(arguments.get("full_rebuild", False))
    since_date = None
    since_date_raw = arguments.get("since_date") or None
    if since_date_raw:
      try:
        since_date = date.fromisoformat(since_date_raw)
      except ValueError:
        return {
          "error": "invalid_arguments",
          "message": f"since_date must be YYYY-MM-DD, got {since_date_raw!r}.",
        }

    try:
      if connection_id is None:
        connection = await resolve_sync_connection(
          graph_id, str(user.id), provider=provider
        )
        connection_id = connection["connection_id"]
      result = await dispatch_connection_sync(
        graph_id=graph_id,
        connection_id=connection_id,
        user_id=str(user.id),
        full_rebuild=full_rebuild,
        since_date=since_date,
        dispatch_timeout=120,
      )
    except NoSyncConnectionError as exc:
      return {"error": "no_sync_connection", "message": str(exc)}
    except AmbiguousSyncConnectionError as exc:
      return {
        "error": "ambiguous_sync_connection",
        "message": str(exc),
        "candidates": exc.candidates,
      }
    except ConnectionNotFoundError as exc:
      return {"error": "connection_not_found", "message": str(exc)}
    except SyncInProgressError as exc:
      return {
        "error": "sync_in_progress",
        "message": str(exc),
        "holder_id": exc.holder_id,
        "ttl_remaining_seconds": exc.ttl_remaining,
      }
    except ProviderUnavailableError as exc:
      return {"error": "provider_unavailable", "message": str(exc)}
    except TimeoutError:
      return {
        "error": "dispatch_timeout",
        "message": (
          "Sync dispatch timed out; the job may not have started. "
          "Check get-fiscal-calendar before retrying."
        ),
      }
    except Exception as exc:
      logger.error("sync-connection failed for %s: %s", graph_id, exc, exc_info=True)
      return {"error": "command_failed", "message": str(exc)}

    return {
      "status": "accepted",
      **result,
      "message": (
        "Sync dispatched. Poll get-fiscal-calendar until last_sync_at "
        "advances past this dispatch (and any sync_stale blocker clears) "
        "before closing."
      ),
    }


# ══════════════════════════════════════════════════════════════════════════
# set-write-policy
# ══════════════════════════════════════════════════════════════════════════


class SetWritePolicyTool:
  """Set a connection's source-of-truth write policy (the write-back opt-in).

  Platform-DB, hand-written (same rationale as the lifecycle tools above):
  `Connection` lives in the platform DB and the op is graph-scoped via the
  service rather than the extensions registrar (which carries no graph_id).
  """

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "set-write-policy",
      "description": (
        "Set whether a connection writes back to its source system — the "
        "explicit opt-in for outbound write-back.\n\n"
        "**WHAT IT CONTROLS:**\n"
        "- `native`: RoboSystems is the source of truth. No write-back; "
        "RoboSystems-originated entries (manual JEs, schedule drafts) post "
        "locally only.\n"
        "- `qb_authoritative`: QuickBooks is the source of truth. "
        "RoboSystems-originated entries publish to QuickBooks when executed "
        "(`execute-event-block`) or at period close, then post locally.\n\n"
        "**WHEN TO USE:** Flip a QuickBooks connection to `qb_authoritative` "
        "before relying on write-back so drafts round-trip into QB; flip "
        "back to `native` to disable write-back. (`hybrid` is not yet "
        "supported.)\n\n"
        "**RETURNS:** the updated connection (including `write_policy`)."
      ),
      "inputSchema": {
        "type": "object",
        "properties": {
          "connection_id": {
            "type": "string",
            "description": "Connection to update (must belong to this graph).",
          },
          "write_policy": {
            "type": "string",
            "enum": ["native", "qb_authoritative"],
            "description": "New write policy.",
          },
        },
        "required": ["connection_id", "write_policy"],
        "additionalProperties": False,
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> dict[str, Any]:
    from robosystems.operations.connection_service import ConnectionService

    user = _require_user(self.client)
    if user is None:
      return _user_missing_err()

    graph_id = self.client.graph_id
    repo_err = _block_shared_repo(graph_id)
    if repo_err:
      return repo_err

    connection_id = arguments.get("connection_id") or ""
    write_policy = arguments.get("write_policy") or ""
    if not connection_id:
      return {"error": "invalid_arguments", "message": "connection_id is required."}
    if write_policy not in ("native", "qb_authoritative"):
      return {
        "error": "invalid_arguments",
        "message": "write_policy must be 'native' or 'qb_authoritative'.",
      }

    try:
      connection = await ConnectionService.set_write_policy(
        connection_id=connection_id,
        write_policy=write_policy,
        user_id=str(user.id),
        graph_id=graph_id,
      )
    except Exception as exc:
      logger.error("set-write-policy failed for %s: %s", connection_id, exc)
      return {"error": "command_failed", "message": str(exc)}

    if connection is None:
      return {
        "error": "connection_not_found",
        "message": (
          f"Connection {connection_id} not found in graph {graph_id} "
          "(or not accessible)."
        ),
      }

    return {
      "connection_id": connection["connection_id"],
      "provider": connection["provider"],
      "status": connection["status"],
      "write_policy": connection.get("write_policy"),
    }
