"""Document CRUD tools (PostgreSQL rows plus the OpenSearch index).

Free-form notes are documents with ``folder="memory"``; search is
search-documents.
"""

from typing import Any, cast

from sqlalchemy.exc import SQLAlchemyError

from robosystems.logger import logger
from robosystems.middleware.operations import run_off_loop
from robosystems.security.error_handling import safe_error_message

from ._errors import database_failure


def _get_platform_session():
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
  """``client.user_id``, else (a background operator) the graph's owner."""
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


def _file_summary(doc) -> dict | None:
  """A stored file's identity, for a document that is one. Its text is read
  with read-document-file; its bytes are downloaded in the app."""
  if not doc.is_file:
    return None
  return {
    "file_name": doc.file_name,
    "content_type": doc.file_content_type,
    "size_bytes": doc.file_size_bytes,
    "sha256": doc.file_sha256,
  }


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

**PARAMETERS:**
- `folder` groups the document: "policies" for accounting policies, close
  procedures and depreciation methods; "memory" for analysis, observations and
  findings; omit it for general documents
- `content` is markdown. Use `## Section` headings — sections are split on them
  and become individually searchable
- YAML frontmatter sets title, tags and folder inline

**RETURNS:** The created document's id and metadata.

**NOTES:**
- Stored in PostgreSQL and indexed in OpenSearch, so it is reachable via
  search-documents once created
- Tags and folders are the filters available to list-documents""",
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
      logger.error(
        f"create-document failed for graph_id={graph_id}: {e}", exc_info=True
      )
      return {
        "error": "create_failed",
        "message": safe_error_message(e)
        or "create-document failed on a backend error; see server logs",
      }
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

**RETURNS:** The updated document's metadata.

**NOTES:** Only provided fields are updated — omit fields you don't want to
change. Content changes are re-indexed in OpenSearch.""",
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
    from robosystems.operations.document_service import (
      DocumentFileError,
      DocumentService,
    )

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
    except DocumentFileError as e:
      return {"error": "invalid_input", "message": str(e)}
    except SQLAlchemyError as e:
      return database_failure("update-document", e, not_initialized_message=None)
    except Exception as e:
      logger.error(
        f"update-document failed for graph_id={graph_id}: {e}", exc_info=True
      )
      return {
        "error": "update_failed",
        "message": safe_error_message(e)
        or "update-document failed on a backend error; see server logs",
      }
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

**RETURNS:** The full document — content, title, tags, folder and metadata.

**RELATED TOOLS:**
- get-document-section returns one search-indexed section from OpenSearch; use
  it to drill into a search hit. This tool reads the whole document from
  PostgreSQL, which is the source of truth, so use it for reading and editing""",
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
        "file": _file_summary(doc),
        "created_at": str(doc.created_at),
        "updated_at": str(doc.updated_at),
      }
    except SQLAlchemyError as e:
      return database_failure("get-document", e, not_initialized_message=None)
    except Exception as e:
      logger.error(f"get-document failed for graph_id={graph_id}: {e}", exc_info=True)
      return {
        "error": "retrieval_failed",
        "message": safe_error_message(e)
        or "get-document failed on a backend error; see server logs",
      }
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

**RETURNS:** Confirmation of the deletion.

**NOTES:** Removes the document from PostgreSQL and its indexed sections from
OpenSearch. Permanent — read it with get-document first.""",
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
    from robosystems.operations.document_service import (
      DocumentInUseError,
      DocumentService,
    )

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
        "message": "Document deleted",
      }
    except DocumentInUseError as e:
      return {"error": "in_use", "message": str(e)}
    except SQLAlchemyError as e:
      return database_failure("delete-document", e, not_initialized_message=None)
    except Exception as e:
      logger.error(
        f"delete-document failed for graph_id={graph_id}: {e}", exc_info=True
      )
      return {
        "error": "delete_failed",
        "message": safe_error_message(e)
        or "delete-document failed on a backend error; see server logs",
      }
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

**RETURNS:** Document ids, titles, folders and tags — metadata, not content.

**RELATED TOOLS:**
- search-documents ranks by content using hybrid search (BM25 + KNN) and
  returns only matches. This tool browses PostgreSQL directly with no ranking
  and lists every document, including empty ones""",
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
            "file": _file_summary(d),
            "created_at": str(d.created_at),
            "updated_at": str(d.updated_at),
          }
          for d in docs
        ],
      }
    except SQLAlchemyError as e:
      return database_failure("list-documents", e, not_initialized_message=None)
    except Exception as e:
      logger.error(f"list-documents failed for graph_id={graph_id}: {e}", exc_info=True)
      return {
        "error": "list_failed",
        "message": safe_error_message(e)
        or "list-documents failed on a backend error; see server logs",
      }
    finally:
      session.close()


# ── Document files ─────────────────────────────────────────────────────────
# The bytes never pass through the model: an agent with a shell uploads them
# with the command create-document-upload returns. A client without one (a
# chat) cannot, and sends the user to the RoboLedger app to upload instead.

_PATH_PLACEHOLDER = "<path-to-file>"


class CreateDocumentUploadTool:
  """Presign the upload of a document file, such as a statement PDF."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "create-document-upload",
      "description": """Start uploading a file to keep as a document: a bank statement, an invoice or a receipt.

**WHEN TO USE:** You can run shell commands and the file is on the local disk
(Claude Code, an agent). Without a shell — a chat with the PDF attached — you
cannot send its bytes: ask the user to upload it in the RoboLedger app, then
find it with list-documents.

**WORKFLOW:**
1. Call this with the file's name (and its local path, to get a ready command).
2. Run the returned `upload_command`. It PUTs the file; nothing passes through you.
3. Call complete-document-upload with the `upload_id` and a title.

**RETURNS:** `upload_id`, `upload_url`, `expires_in` and `upload_command`.

**NOTES:** A PDF, PNG or JPEG, at most 25 MB; the type is taken from the
file's extension. Nothing is recorded until the upload completes, and an
upload never completed expires. Stored files are not indexed for search.""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "file_name": {
            "type": "string",
            "description": (
              "The file's name, ending in .pdf, .png, .jpg or .jpeg "
              "(e.g. 'checking-2026-09.pdf')"
            ),
          },
          "file_path": {
            "type": "string",
            "description": (
              "The file's local path, used only to write upload_command; the "
              "server never reads it"
            ),
          },
        },
        "required": ["file_name"],
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    return await run_off_loop(self._execute_sync, arguments)

  def _execute_sync(self, arguments: dict[str, Any]) -> Any:
    import shlex

    from robosystems.config.constants import PRESIGNED_URL_EXPIRY_SECONDS
    from robosystems.models.api.graphs.operations import (
      CreateDocumentUploadOp,
      DocumentFileContentType,
    )
    from robosystems.operations.document_service import (
      DocumentFileError,
      DocumentService,
      document_content_type,
    )

    graph_id = self.client.graph_id
    blocked = _block_shared_repository(graph_id)
    if blocked:
      return blocked
    access_error = _check_graph_access(graph_id, require_write=True)
    if access_error:
      return access_error

    session = _get_platform_session()
    try:
      file_name = arguments["file_name"]
      request = CreateDocumentUploadOp(
        file_name=file_name,
        content_type=cast(DocumentFileContentType, document_content_type(file_name)),
      )
      upload_id, url = DocumentService(session).begin_file_upload(graph_id, request)
    except (DocumentFileError, ValueError) as e:
      return {"error": "invalid_input", "message": str(e)}
    finally:
      session.close()

    path = arguments.get("file_path")
    target = shlex.quote(path) if path else _PATH_PLACEHOLDER
    return {
      "upload_id": upload_id,
      "upload_url": url,
      "expires_in": PRESIGNED_URL_EXPIRY_SECONDS,
      "upload_command": (
        f"curl -sSf -X PUT -H 'Content-Type: {request.content_type}' "
        f"--data-binary @{target} {shlex.quote(url)}"
      ),
      "next": "Run upload_command, then call complete-document-upload with upload_id.",
    }


class CompleteDocumentUploadTool:
  """Store an uploaded file as a document."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "complete-document-upload",
      "description": """Store a file uploaded with create-document-upload as a document.

**WHEN TO USE:** After the upload command succeeded.

**RETURNS:** The new document's id, title, folder and its stored file (name,
size, SHA-256). Pass `document_id` to record-statement-balance as the
statement's evidence; read the file's text with read-document-file.

**NOTES:** The file is checked (a real PDF, within the size cap) and hashed; a
file that fails is discarded. Completing the same upload twice returns the
same document. A stored file is not searchable with search-documents.""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "upload_id": {
            "type": "string",
            "description": "The upload_id create-document-upload returned",
          },
          "title": {
            "type": "string",
            "description": "Document title (e.g. 'Operating Checking statement, Sept 2026')",
          },
          "folder": {
            "type": "string",
            "description": "Folder for organization (e.g. 'statements')",
          },
          "tags": {
            "type": "array",
            "items": {"type": "string"},
            "description": "Tags for filtering",
          },
        },
        "required": ["upload_id", "title"],
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    return await run_off_loop(self._execute_sync, arguments)

  def _execute_sync(self, arguments: dict[str, Any]) -> Any:
    from pydantic import ValidationError

    from robosystems.models.api.graphs.operations import CompleteDocumentUploadOp
    from robosystems.operations.document_service import (
      DocumentFileError,
      DocumentFileNotUploadedError,
      DocumentService,
    )

    graph_id = self.client.graph_id
    blocked = _block_shared_repository(graph_id)
    if blocked:
      return blocked
    access_error = _check_graph_access(graph_id, require_write=True)
    if access_error:
      return access_error
    owner_id = _resolve_acting_user(self.client, graph_id)
    if not owner_id:
      return {"error": "access_denied", "message": "No user to record the file as."}

    session = _get_platform_session()
    try:
      request = CompleteDocumentUploadOp(
        upload_id=arguments["upload_id"],
        title=arguments["title"],
        folder=arguments.get("folder"),
        tags=arguments.get("tags"),
      )
      doc = DocumentService(session).complete_file_upload(graph_id, owner_id, request)
      return {
        "success": True,
        "document_id": doc.id,
        "title": doc.title,
        "folder": doc.folder,
        "file": _file_summary(doc),
      }
    except ValidationError as e:
      return {"error": "invalid_input", "message": str(e)}
    except DocumentFileNotUploadedError as e:
      return {"error": "not_uploaded", "message": str(e)}
    except DocumentFileError as e:
      return {"error": "invalid_file", "message": str(e)}
    except SQLAlchemyError as e:
      return database_failure(
        "complete-document-upload", e, not_initialized_message=None
      )
    finally:
      session.close()


# One call returns at most this much text, so a long statement is read in pages.
_MAX_PAGES_PER_READ = 10


class ReadDocumentFileTool:
  """Read the text of a stored document file."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "read-document-file",
      "description": f"""Read the text of a stored document file, such as a bank statement PDF, page by page.

**WHEN TO USE:** To read a statement's ending balance, closing date and lines
before recording it with record-statement-balance. Find file documents with
list-documents (they carry a `file` object).

**RETURNS:** `page_count` and the requested `pages` as text, at most
{_MAX_PAGES_PER_READ} per call; read further with `first_page`.

**NOTES:** Reads a PDF's text layer. A statement a bank generates has one; a
scanned PDF does not, and its pages come back empty, and a photo (PNG, JPEG)
has none to read — ask the user for the figures then. For a text document use
get-document.""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "document_id": {
            "type": "string",
            "description": "The file document's id",
          },
          "first_page": {
            "type": "integer",
            "minimum": 1,
            "default": 1,
            "description": "First page to read (1-based)",
          },
          "max_pages": {
            "type": "integer",
            "minimum": 1,
            "maximum": _MAX_PAGES_PER_READ,
            "default": 3,
            "description": f"Pages to read, at most {_MAX_PAGES_PER_READ}",
          },
        },
        "required": ["document_id"],
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    return await run_off_loop(self._execute_sync, arguments)

  def _execute_sync(self, arguments: dict[str, Any]) -> Any:
    from robosystems.operations.document_service import (
      DocumentFileError,
      DocumentService,
    )

    graph_id = self.client.graph_id
    blocked = _block_shared_repository(graph_id)
    if blocked:
      return blocked
    access_error = _check_graph_access(graph_id)
    if access_error:
      return access_error

    document_id = arguments["document_id"]
    first_page = max(1, int(arguments.get("first_page") or 1))
    max_pages = min(_MAX_PAGES_PER_READ, max(1, int(arguments.get("max_pages") or 3)))

    session = _get_platform_session()
    try:
      text = DocumentService(session).read_file_text(
        graph_id, document_id, first_page=first_page, max_pages=max_pages
      )
      last = text.pages[-1][0] if text.pages else first_page - 1
      return {
        "document_id": document_id,
        "title": text.document.title,
        "file_name": text.document.file_name,
        "page_count": text.page_count,
        "pages": [{"page": n, "text": body} for n, body in text.pages],
        "more_pages": last < text.page_count,
      }
    except KeyError:
      return {"error": "not_found", "message": f"Document '{document_id}' not found"}
    except DocumentFileError as e:
      return {"error": "not_a_file", "message": str(e)}
    except SQLAlchemyError as e:
      return database_failure("read-document-file", e, not_initialized_message=None)
    finally:
      session.close()
