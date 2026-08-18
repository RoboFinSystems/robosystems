"""Text-block binding MCP tool — bind a Document to a disclosure element.

Hand-written (not registrar-generated) because the command needs BOTH the
platform database session (Documents live there) and the tenant
extensions session — the ``OperationSpec`` runner passes neither the
platform session nor the trusted-path ``graph_id``.
"""

from typing import Any

from sqlalchemy.exc import SQLAlchemyError

from robosystems.db.extensions import extensions_session
from robosystems.logger import logger
from robosystems.middleware.operations import run_off_loop

from ._errors import database_failure
from .document_tools import (
  _block_shared_repository,
  _check_graph_access,
  _get_platform_session,
  _resolve_graph_owner,
)


class BindTextBlockTool:
  """Bind a platform Document (or section) as a Nonnumeric text-block fact."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "bind-text-block",
      "description": """Bind a platform Document (markdown) — or one of its sections — to a disclosure element as a text-block fact.

**WHEN TO USE:**
- To attach an accounting-policy or footnote narrative (stored as a document via create-document) to a disclosure structure so it renders in reports
- After authoring a text-block disclosure structure via create-taxonomy-block (block_type='regulatory_disclosure', concept_arrangement='text_block')

**HOW IT WORKS:**
- The document stays the editable source of truth; the bind snapshots its text into a standing 'disclosure' FactSet with document provenance (document_id + section + content_hash)
- Reports generated afterward snapshot the standing set, so filed reports stay immutable even if the document is later edited
- Re-binding the same element + period replaces the fact and refreshes the content hash (the drift signal)

**TIPS:**
- Use section_id (the slugified heading ids search-documents returns) to bind one section of a larger document; omit it to bind the whole document
- The element must be a concrete (non-abstract) concept on the disclosure structure; provide element_qname or element_id
- period_start/period_end should span the reporting period the narrative covers""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "document_id": {
            "type": "string",
            "description": "Platform document ID (see list-documents)",
          },
          "section_id": {
            "type": "string",
            "description": "Optional slugified heading id of one section to bind; omit for the whole document",
          },
          "structure_id": {
            "type": "string",
            "description": "Disclosure structure (block_type='regulatory_disclosure', text-block concept_arrangement)",
          },
          "element_id": {
            "type": "string",
            "description": "Disclosure element id (provide exactly one of element_id / element_qname)",
          },
          "element_qname": {
            "type": "string",
            "description": "Disclosure element qname, e.g. 'acme:SignificantAccountingPoliciesTextBlock'",
          },
          "period_start": {
            "type": "string",
            "description": "Reporting period start (YYYY-MM-DD)",
          },
          "period_end": {
            "type": "string",
            "description": "Reporting period end (YYYY-MM-DD)",
          },
          "entity_id": {
            "type": "string",
            "description": "Optional entity id; defaults to the primary entity",
          },
        },
        "required": ["document_id", "structure_id", "period_start", "period_end"],
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    return await run_off_loop(self._execute_sync, arguments)

  def _execute_sync(self, arguments: dict[str, Any]) -> Any:
    from pydantic import ValidationError

    from robosystems.models.api.extensions.text_blocks import BindTextBlockRequest
    from robosystems.operations.extensions.staleness import mark_graph_stale
    from robosystems.operations.roboledger.commands.text_blocks import (
      DocumentNotFoundError,
      bind_text_block,
    )

    graph_id = self.client.graph_id

    blocked = _block_shared_repository(graph_id)
    if blocked:
      return blocked

    access_error = _check_graph_access(graph_id, require_write=True)
    if access_error:
      return access_error

    try:
      body = BindTextBlockRequest.model_validate(arguments)
    except ValidationError as e:
      return {"error": "invalid_input", "message": str(e)}

    owner_id = _resolve_graph_owner(graph_id)
    if not owner_id:
      return {
        "error": "access_denied",
        "message": f"No user found with access to graph {graph_id}",
      }

    platform_db = _get_platform_session()
    try:
      with extensions_session(graph_id) as session:
        response = bind_text_block(
          session,
          platform_db,
          graph_id,
          body,
          created_by=owner_id,
        )
      mark_graph_stale(graph_id, "text_block_bound")
      return response.model_dump(mode="json")
    except DocumentNotFoundError as e:
      return {"error": "not_found", "message": str(e)}
    except ValueError as e:
      return {"error": "invalid_input", "message": str(e)}
    except SQLAlchemyError as e:
      return database_failure("bind-text-block", e)
    except Exception as e:
      logger.error(f"bind-text-block failed for graph_id={graph_id}: {e}")
      return {"error": "command_failed", "message": str(e)}
    finally:
      platform_db.close()
