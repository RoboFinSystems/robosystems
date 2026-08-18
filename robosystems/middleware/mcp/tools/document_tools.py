"""Document management MCP tools — create, read, update, list documents.

Free-form notes live here too, as documents with ``folder="memory"``.

Five tools:
1. create-document: Create a markdown document (PG + OpenSearch)
2. update-document: Edit an existing document's content or metadata
3. delete-document: Remove a document from PG + OpenSearch by ID
4. get-document: Retrieve full document content by ID
5. list-documents: Browse documents in the graph (filter by folder/source_type)

Discovery (search) is handled by the existing search-documents tool.
"""

from typing import Any

from sqlalchemy.exc import SQLAlchemyError

from robosystems.logger import logger
from robosystems.middleware.operations import run_off_loop

from ._errors import database_failure


def _get_platform_session():
  """Get a platform database session."""
  from robosystems.database import SessionFactory

  return SessionFactory()


def _resolve_tier(graph_id: str) -> str | None:
  """Resolve subscription tier for a graph."""
  try:
    from robosystems.database import SessionFactory
    from robosystems.middleware.graph.utils import parse_subgraph_id
    from robosystems.models.core.graph import Graph

    sub_info = parse_subgraph_id(graph_id)
    parent_id = sub_info.parent_graph_id if sub_info else graph_id

    session = SessionFactory()
    try:
      graph = session.query(Graph).filter(Graph.graph_id == parent_id).first()
      return graph.graph_tier if graph else None
    finally:
      session.close()
  except Exception:
    return None


def _resolve_graph_owner(graph_id: str) -> str | None:
  """Resolve a fallback actor for a graph: its earliest admin, else its
  earliest member. Only for callers that carry no authenticated user."""
  try:
    from sqlalchemy import case

    from robosystems.database import SessionFactory
    from robosystems.models.core.graph.graph_user import GraphUser

    session = SessionFactory()
    try:
      graph_user = (
        session.query(GraphUser)
        .filter(GraphUser.graph_id == graph_id)
        .order_by(
          case((GraphUser.role == "admin", 0), else_=1),
          GraphUser.created_at.asc(),
        )
        .first()
      )
      return graph_user.user_id if graph_user else None
    finally:
      session.close()
  except Exception:
    return None


def _resolve_acting_user(client: Any, graph_id: str) -> str | None:
  """Who a write should be stamped with.

  The MCP handler attaches the authenticated user's id to the client
  (``client.user_id``); that is the actor. Only when no user is threaded
  through — an in-process tool driven by a background operator — does this
  fall back to the graph's admin, so the write is at least attributed to an
  accountable owner rather than to whichever member happened to join first.
  """
  user_id = getattr(client, "user_id", None)
  if user_id:
    return str(user_id)
  return _resolve_graph_owner(graph_id)


def _block_shared_repository(graph_id: str) -> dict | None:
  """Return error dict if graph is a shared repository."""
  try:
    from robosystems.config.shared_repositories import is_shared_repository_or_subgraph

    if is_shared_repository_or_subgraph(graph_id):
      return {
        "error": "not_allowed",
        "message": "Document management is not available on shared repository graphs.",
      }
  except Exception:
    pass
  return None


def _check_graph_access(graph_id: str, require_write: bool = False) -> dict | None:
  """Check graph lifecycle and subscription status. Returns error dict or None."""
  try:
    from robosystems.database import SessionFactory
    from robosystems.middleware.billing.enforcement import require_graph_access

    session = SessionFactory()
    try:
      require_graph_access(graph_id, session, require_write=require_write)
    finally:
      session.close()
  except Exception as e:
    detail = getattr(e, "detail", str(e))
    return {"error": "access_denied", "message": detail}
  return None


class CreateDocumentTool:
  """Create a markdown document in the graph."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "create-document",
      "description": """Create a markdown document in the graph's document store.

**WHEN TO USE:**
- To save accounting policies, procedures, or reference documents
- To record observations, analysis notes, or research findings (use folder="memory")
- To create any persistent document that should be searchable via search-documents

**DOCUMENT TYPES:**
- **Policy documents**: Accounting policies, close procedures, depreciation methods (folder="policies")
- **Memory/notes**: Analysis, observations, key findings (folder="memory")
- **General**: Any markdown document

**FEATURES:**
- Content is stored in PostgreSQL and automatically indexed in OpenSearch
- Documents are searchable via the search-documents tool after creation
- Supports markdown formatting with heading-based section splitting
- Tags and folders help organize and filter documents

**TIPS:**
- Use markdown headings (## Section) to create searchable sections
- Use YAML frontmatter for metadata (title, tags, folder)
- Documents with folder="memory" replace the old remember-text pattern""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "title": {
            "type": "string",
            "description": "Document title",
          },
          "content": {
            "type": "string",
            "description": "Markdown content (max 500,000 characters)",
          },
          "folder": {
            "type": "string",
            "description": "Folder for organization (e.g., 'policies', 'memory', 'procedures')",
          },
          "tags": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Tags for filtering (e.g., ['depreciation', 'fixed-assets'])",
          },
        },
        "required": ["title", "content"],
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    return await run_off_loop(self._execute_sync, arguments)

  def _execute_sync(self, arguments: dict[str, Any]) -> Any:
    from robosystems.models.api.search import DocumentUploadRequest
    from robosystems.operations.document_service import DocumentService

    graph_id = self.client.graph_id

    blocked = _block_shared_repository(graph_id)
    if blocked:
      return blocked

    access_error = _check_graph_access(graph_id, require_write=True)
    if access_error:
      return access_error

    title = arguments["title"]
    content = arguments["content"]
    folder = arguments.get("folder")
    tags = arguments.get("tags")

    if not content or len(content) < 1:
      return {"error": "invalid_input", "message": "Content must not be empty"}
    if len(content) > 500_000:
      return {
        "error": "invalid_input",
        "message": f"Content exceeds 500,000 character limit ({len(content)} chars)",
      }

    session = _get_platform_session()
    try:
      tier = _resolve_tier(graph_id)
      service = DocumentService(session)
      request = DocumentUploadRequest(
        title=title,
        content=content,
        tags=tags,
        folder=folder,
      )
      owner_id = _resolve_acting_user(self.client, graph_id)
      if not owner_id:
        return {
          "success": False,
          "error": f"No user found with access to graph {graph_id}",
        }

      doc, response = service.create_document(
        graph_id=graph_id,
        user_id=owner_id,
        request=request,
        tier=tier,
      )
      session.commit()

      return {
        "success": True,
        "document_id": doc.id,
        "title": doc.title,
        "folder": doc.folder,
        "sections_indexed": response.sections_indexed,
        "message": f"Document created ({len(content)} chars, {response.sections_indexed} sections indexed)",
      }
    except (ValueError, KeyError) as e:
      return {"error": "create_failed", "message": str(e)}
    except SQLAlchemyError as e:
      return database_failure("create-document", e, not_initialized_message=None)
    except Exception as e:
      logger.error(f"create-document failed for graph_id={graph_id}: {e}")
      return {"error": "create_failed", "message": str(e)}
    finally:
      session.close()


class UpdateDocumentTool:
  """Edit an existing document's content or metadata."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "update-document",
      "description": """Update an existing document's content, title, tags, or folder.

**WHEN TO USE:**
- To edit a policy document after reviewing it
- To append notes to an existing memory document
- To update tags or folder for organization

Only provided fields are updated — omit fields you don't want to change.
Content changes are automatically re-indexed in OpenSearch.""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "document_id": {
            "type": "string",
            "description": "Document ID to update",
          },
          "title": {
            "type": "string",
            "description": "New title (omit to keep current)",
          },
          "content": {
            "type": "string",
            "description": "New content (omit to keep current)",
          },
          "folder": {
            "type": "string",
            "description": "New folder (omit to keep current)",
          },
          "tags": {
            "type": "array",
            "items": {"type": "string"},
            "description": "New tags (omit to keep current)",
          },
        },
        "required": ["document_id"],
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    return await run_off_loop(self._execute_sync, arguments)

  def _execute_sync(self, arguments: dict[str, Any]) -> Any:
    from robosystems.operations.document_service import DocumentService

    graph_id = self.client.graph_id

    blocked = _block_shared_repository(graph_id)
    if blocked:
      return blocked

    access_error = _check_graph_access(graph_id, require_write=True)
    if access_error:
      return access_error

    document_id = arguments["document_id"]

    session = _get_platform_session()
    try:
      service = DocumentService(session)

      kwargs: dict[str, Any] = {}
      if "title" in arguments:
        kwargs["title"] = arguments["title"]
      if "content" in arguments:
        kwargs["content"] = arguments["content"]
      if "folder" in arguments:
        kwargs["folder"] = arguments["folder"]
      if "tags" in arguments:
        kwargs["tags"] = arguments["tags"]

      if not kwargs:
        return {"error": "invalid_input", "message": "No fields to update"}

      doc, response = service.update_document(
        graph_id=graph_id,
        document_id=document_id,
        **kwargs,
      )
      session.commit()

      return {
        "success": True,
        "document_id": doc.id,
        "title": doc.title,
        "sections_indexed": response.sections_indexed,
        "message": "Document updated and re-indexed",
      }
    except KeyError as e:
      return {"error": "not_found", "message": str(e)}
    except SQLAlchemyError as e:
      return database_failure("update-document", e, not_initialized_message=None)
    except Exception as e:
      logger.error(f"update-document failed for graph_id={graph_id}: {e}")
      return {"error": "update_failed", "message": str(e)}
    finally:
      session.close()


class GetDocumentTool:
  """Retrieve full document content by ID."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "get-document",
      "description": """Get the full content of a document by ID.

**WHEN TO USE:**
- To read a complete policy document or procedure
- To review a document before updating it
- When you know the document ID (from list-documents or a previous interaction)

**HOW IT DIFFERS FROM get-document-section:**
- get-document returns the FULL document from PostgreSQL (the source of truth)
- get-document-section returns a single search-indexed section from OpenSearch
- Use get-document for reading/editing, use get-document-section for search drill-in""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "document_id": {
            "type": "string",
            "description": "Document ID",
          },
        },
        "required": ["document_id"],
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    return await run_off_loop(self._execute_sync, arguments)

  def _execute_sync(self, arguments: dict[str, Any]) -> Any:
    from robosystems.operations.document_service import DocumentService

    graph_id = self.client.graph_id
    document_id = arguments["document_id"]

    blocked = _block_shared_repository(graph_id)
    if blocked:
      return blocked

    access_error = _check_graph_access(graph_id)
    if access_error:
      return access_error

    session = _get_platform_session()
    try:
      service = DocumentService(session)
      doc = service.get_document(graph_id, document_id)

      if doc is None:
        return {"error": "not_found", "message": f"Document '{document_id}' not found"}

      return {
        "document_id": doc.id,
        "title": doc.title,
        "content": doc.content,
        "folder": doc.folder,
        "tags": doc.tags,
        "source_type": doc.source_type,
        "sections_indexed": doc.sections_indexed,
        "created_at": str(doc.created_at),
        "updated_at": str(doc.updated_at),
      }
    except SQLAlchemyError as e:
      return database_failure("get-document", e, not_initialized_message=None)
    except Exception as e:
      logger.error(f"get-document failed for graph_id={graph_id}: {e}")
      return {"error": "retrieval_failed", "message": str(e)}
    finally:
      session.close()


class DeleteDocumentTool:
  """Delete a document by ID from PostgreSQL and OpenSearch."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "delete-document",
      "description": """Delete a document by ID from the graph's document store.

**WHEN TO USE:**
- To remove an outdated or incorrect policy/procedure document
- To clean up a memory note that is no longer relevant

Removes the document from PostgreSQL (source of truth) and its indexed sections
from OpenSearch. This is permanent — use get-document to review before deleting.""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "document_id": {
            "type": "string",
            "description": "Document ID to delete",
          },
        },
        "required": ["document_id"],
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    return await run_off_loop(self._execute_sync, arguments)

  def _execute_sync(self, arguments: dict[str, Any]) -> Any:
    from robosystems.operations.document_service import DocumentService

    graph_id = self.client.graph_id

    blocked = _block_shared_repository(graph_id)
    if blocked:
      return blocked

    access_error = _check_graph_access(graph_id, require_write=True)
    if access_error:
      return access_error

    document_id = arguments["document_id"]

    session = _get_platform_session()
    try:
      service = DocumentService(session)
      deleted = service.delete_document(graph_id, document_id)
      if not deleted:
        return {"error": "not_found", "message": f"Document '{document_id}' not found"}
      return {
        "success": True,
        "document_id": document_id,
        "deleted": True,
        "message": "Document deleted from PostgreSQL and OpenSearch",
      }
    except SQLAlchemyError as e:
      return database_failure("delete-document", e, not_initialized_message=None)
    except Exception as e:
      logger.error(f"delete-document failed for graph_id={graph_id}: {e}")
      return {"error": "delete_failed", "message": str(e)}
    finally:
      session.close()


class ListDocumentsTool:
  """List documents in the graph."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "list-documents",
      "description": """List documents in the graph, optionally filtered by folder or source type.

**WHEN TO USE:**
- To browse what documents exist before searching
- To find a specific document by title when you don't have the ID
- To see all policy documents (folder="policies") or memory notes (folder="memory")

**HOW IT DIFFERS FROM search-documents:**
- list-documents is a direct browse of PostgreSQL (no search ranking)
- search-documents uses hybrid search (BM25 + KNN) for discovery by content
- list-documents shows all docs including empty ones; search-documents only returns matches""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "folder": {
            "type": "string",
            "description": "Filter by folder (e.g., 'policies', 'memory', 'procedures')",
          },
          "source_type": {
            "type": "string",
            "description": "Filter by source type (e.g., 'uploaded_doc', 'connection_doc')",
          },
        },
        "required": [],
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    return await run_off_loop(self._execute_sync, arguments)

  def _execute_sync(self, arguments: dict[str, Any]) -> Any:
    from robosystems.operations.document_service import DocumentService

    graph_id = self.client.graph_id

    blocked = _block_shared_repository(graph_id)
    if blocked:
      return blocked

    access_error = _check_graph_access(graph_id)
    if access_error:
      return access_error

    folder = arguments.get("folder")
    source_type = arguments.get("source_type")

    session = _get_platform_session()
    try:
      service = DocumentService(session)
      docs = service.list_documents(graph_id, source_type, folder)

      return {
        "total": len(docs),
        "documents": [
          {
            "document_id": d.id,
            "title": d.title,
            "folder": d.folder,
            "tags": d.tags,
            "source_type": d.source_type,
            "sections_indexed": d.sections_indexed,
            "created_at": str(d.created_at),
            "updated_at": str(d.updated_at),
          }
          for d in docs
        ],
      }
    except SQLAlchemyError as e:
      return database_failure("list-documents", e, not_initialized_message=None)
    except Exception as e:
      logger.error(f"list-documents failed for graph_id={graph_id}: {e}")
      return {"error": "list_failed", "message": str(e)}
    finally:
      session.close()
