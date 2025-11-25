"""
Workspace management MCP tools.

Provides tools for creating, switching, and managing isolated workspace environments.

These tools use SubgraphService just like the API routers do, maintaining parallel systems
at the service layer while the client handles context switching.
"""

from typing import Any, Dict

from robosystems.logger import logger
from robosystems.operations.graph.subgraph_service import SubgraphService


class CreateWorkspaceTool:
  """Create an isolated workspace (subgraph)."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> Dict[str, Any]:
    return {
      "name": "create-workspace",
      "description": "Create an isolated workspace (subgraph) for experimentation. Data and queries are isolated from the main graph. Returns workspace_id for the client to track.",
      "inputSchema": {
        "type": "object",
        "properties": {
          "name": {
            "type": "string",
            "description": "Workspace name (alphanumeric only, 1-20 characters). No hyphens, underscores, or special characters.",
          },
          "description": {
            "type": "string",
            "description": "Optional workspace description for documentation purposes",
          },
          "fork_parent": {
            "type": "boolean",
            "description": "If true, copies all data from the parent graph to the workspace. If false, creates an empty workspace.",
            "default": False,
          },
        },
        "required": ["name"],
      },
    }

  async def execute(self, arguments: Dict[str, Any]) -> Dict[str, Any]:
    """
    Execute workspace creation using SubgraphService.

    Args:
        arguments: Tool arguments with name, description, fork_parent

    Returns:
        Dict with workspace_id, name, and success status
    """
    name = arguments.get("name")
    description = arguments.get("description", f"MCP workspace: {name}")
    fork_parent = arguments.get("fork_parent", False)

    # Validate name (alphanumeric only, 1-20 chars)
    if not name or not name.isalnum() or len(name) < 1 or len(name) > 20:
      return {
        "error": "invalid_name",
        "message": "Workspace name must be alphanumeric only, 1-20 characters (no hyphens, underscores, or special chars)",
        "valid_examples": ["dev", "staging", "prod1", "test123"],
      }

    try:
      # Get parent graph and user from context
      parent_graph_id = self.client.graph_id
      user = getattr(self.client, "user", None)

      if not user:
        return {
          "error": "authentication_required",
          "message": "User context required for workspace creation",
        }

      # Get parent graph from database
      from robosystems.database import get_db_session
      from robosystems.models.iam.graph import Graph

      db_gen = get_db_session()
      db = next(db_gen)
      try:
        parent_graph = db.query(Graph).filter(Graph.graph_id == parent_graph_id).first()
        if not parent_graph:
          return {
            "error": "parent_not_found",
            "message": f"Parent graph {parent_graph_id} not found",
          }

        # Create subgraph using SubgraphService (same as API routers)
        service = SubgraphService()
        subgraph_result = await service.create_subgraph(
          parent_graph=parent_graph,
          user=user,
          name=name,
          description=description,
          subgraph_type="static",
          metadata={},
          fork_parent=fork_parent,
          fork_options=None,
        )

        workspace_id = subgraph_result["graph_id"]

        logger.info(
          f"Created workspace {workspace_id} from parent {parent_graph_id} (fork={fork_parent})"
        )

        return {
          "success": True,
          "workspace_id": workspace_id,
          "name": name,
          "parent_graph_id": parent_graph_id,
          "description": description,
          "forked_from_parent": fork_parent,
          "message": f"Created workspace '{name}'. Use switch-workspace to activate it.",
        }

      finally:
        try:
          next(db_gen)
        except StopIteration:
          pass

    except Exception as e:
      logger.error(f"Failed to create workspace: {e}")
      import traceback

      logger.error(traceback.format_exc())
      return {
        "error": "creation_failed",
        "message": str(e),
        "workspace_name": name,
      }


class DeleteWorkspaceTool:
  """Delete a workspace."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> Dict[str, Any]:
    return {
      "name": "delete-workspace",
      "description": "Delete a workspace and all its data. Cannot delete the primary graph.",
      "inputSchema": {
        "type": "object",
        "properties": {
          "workspace_id": {
            "type": "string",
            "description": "Workspace ID to delete (e.g., 'kg123_dev')",
          },
          "force": {
            "type": "boolean",
            "description": "Force deletion even if workspace contains data. Use with caution.",
            "default": False,
          },
        },
        "required": ["workspace_id"],
      },
    }

  async def execute(self, arguments: Dict[str, Any]) -> Dict[str, Any]:
    """
    Execute workspace deletion using SubgraphService.

    Args:
        arguments: Tool arguments with workspace_id, force

    Returns:
        Dict with success status
    """
    workspace_id = arguments.get("workspace_id")
    force = arguments.get("force", False)

    # Validate workspace_id format and extract parent_graph_id
    if not workspace_id or "_" not in workspace_id:
      return {
        "error": "invalid_workspace_id",
        "message": f"Invalid workspace ID format: {workspace_id}. Expected format: parent_name",
      }

    # Parse workspace_id to extract parent_graph_id
    from robosystems.middleware.graph.utils import parse_subgraph_id

    subgraph_info = parse_subgraph_id(workspace_id)
    if not subgraph_info:
      return {
        "error": "invalid_workspace_id",
        "message": f"{workspace_id} is not a valid subgraph identifier",
      }

    # Verify the workspace belongs to the current graph (prevent cross-tenant access)
    current_graph_id = self.client.graph_id
    if subgraph_info.parent_graph_id != current_graph_id:
      return {
        "error": "authorization_failed",
        "message": f"Workspace {workspace_id} does not belong to graph {current_graph_id}",
        "hint": "You can only delete workspaces that belong to your current graph",
      }

    # Get user from context
    user = getattr(self.client, "user", None)
    if not user:
      return {
        "error": "authentication_required",
        "message": "User context required for workspace deletion",
      }

    # Verify user has admin access to parent graph
    from robosystems.database import get_db_session
    from robosystems.models.iam.graph import Graph
    from robosystems.models.iam.graph_user import GraphUser

    db_gen = get_db_session()
    db = next(db_gen)
    try:
      # Verify workspace exists and belongs to parent
      workspace = db.query(Graph).filter(Graph.graph_id == workspace_id).first()
      if not workspace or not workspace.is_subgraph:
        return {
          "error": "workspace_not_found",
          "message": f"Workspace {workspace_id} not found",
        }

      # Verify user has admin access to parent graph
      user_graph = (
        db.query(GraphUser)
        .filter(
          GraphUser.user_id == user.id,
          GraphUser.graph_id == subgraph_info.parent_graph_id,
        )
        .first()
      )

      if not user_graph or user_graph.role != "admin":
        return {
          "error": "insufficient_permissions",
          "message": "Admin access to parent graph required to delete workspaces",
          "hint": "Only users with admin role can delete workspaces",
        }
    finally:
      try:
        next(db_gen)
      except StopIteration:
        pass

    try:
      # Delete subgraph using SubgraphService (same as API routers)
      service = SubgraphService()
      await service.delete_subgraph_database(
        subgraph_id=workspace_id, force=force, create_backup=False
      )

      # Also delete from PostgreSQL
      db_gen2 = get_db_session()
      db2 = next(db_gen2)
      try:
        subgraph = db2.query(Graph).filter(Graph.graph_id == workspace_id).first()
        if subgraph:
          db2.delete(subgraph)
          db2.commit()
      finally:
        try:
          next(db_gen2)
        except StopIteration:
          pass

      logger.info(f"Deleted workspace {workspace_id} (force={force})")

      return {
        "success": True,
        "deleted": workspace_id,
        "message": f"Deleted workspace '{workspace_id}'.",
      }

    except Exception as e:
      logger.error(f"Failed to delete workspace: {e}")
      hint = None
      if "400" in str(e) or "contains data" in str(e).lower():
        hint = "Workspace contains data. Use force=true to delete anyway."

      return {
        "error": "deletion_failed",
        "message": str(e),
        "workspace_id": workspace_id,
        "hint": hint,
      }


class ListWorkspacesTool:
  """List all workspaces."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> Dict[str, Any]:
    return {
      "name": "list-workspaces",
      "description": "List all workspaces for the current graph. Shows workspace IDs that can be used with switch-workspace.",
      "inputSchema": {"type": "object", "properties": {}},
    }

  async def execute(self, arguments: Dict[str, Any]) -> Dict[str, Any]:
    """
    Execute workspace listing from PostgreSQL.

    Returns:
        Dict with list of workspaces
    """
    try:
      from robosystems.database import get_db_session
      from robosystems.models.iam.graph import Graph

      parent_graph_id = self.client.graph_id

      db_gen = get_db_session()
      db = next(db_gen)
      try:
        # Get all subgraphs for the parent graph
        subgraphs = (
          db.query(Graph)
          .filter(Graph.parent_graph_id == parent_graph_id)
          .order_by(Graph.created_at.desc())
          .all()
        )

        workspaces = []

        # Add primary graph first
        parent_graph = db.query(Graph).filter(Graph.graph_id == parent_graph_id).first()
        workspaces.append(
          {
            "workspace_id": parent_graph_id,
            "name": "main",
            "description": parent_graph.graph_name if parent_graph else "Primary graph",
            "type": "primary",
            "parent_graph_id": None,
          }
        )

        # Add subgraphs
        for sg in subgraphs:
          workspaces.append(
            {
              "workspace_id": sg.graph_id,
              "name": sg.subgraph_name or sg.graph_name,
              "description": sg.graph_name,
              "type": "workspace",
              "parent_graph_id": parent_graph_id,
              "created_at": sg.created_at.isoformat() if sg.created_at else None,
            }
          )

        return {
          "primary_graph_id": parent_graph_id,
          "total_workspaces": len(workspaces),
          "workspaces": workspaces,
        }

      finally:
        try:
          next(db_gen)
        except StopIteration:
          pass

    except Exception as e:
      logger.error(f"Failed to list workspaces: {e}")
      return {
        "error": "listing_failed",
        "message": str(e),
      }


class SwitchWorkspaceTool:
  """
  Switch workspace tool (client-side only).

  This tool is implemented entirely in the MCP client.
  The server provides the definition for discoverability.
  """

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> Dict[str, Any]:
    return {
      "name": "switch-workspace",
      "description": "Switch to a different workspace. This is a client-side operation - the client manages which workspace is active.",
      "inputSchema": {
        "type": "object",
        "properties": {
          "workspace_id": {
            "type": "string",
            "description": "Workspace ID to switch to (e.g., 'kg123_dev'), or use 'primary' to switch back to the main graph",
          }
        },
        "required": ["workspace_id"],
      },
    }

  async def execute(self, arguments: Dict[str, Any]) -> Dict[str, Any]:
    """
    This should never be called server-side.
    The client intercepts switch-workspace before sending to server.
    """
    return {
      "error": "client_side_tool",
      "message": "switch-workspace is a client-side tool. The MCP client should intercept and handle this locally.",
    }
