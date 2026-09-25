"""delete-report: remove a report that was never filed.

Hand-written: after the rows commit it removes the report's published
artifacts from the object store, which the registrar runner cannot do (it
only marks the graph stale).
"""

from typing import Any

from sqlalchemy.exc import SQLAlchemyError

from robosystems.db.extensions import extensions_session
from robosystems.logger import logger
from robosystems.middleware.auth.dependencies import user_is_graph_admin
from robosystems.middleware.operations import run_off_loop
from robosystems.operations.extensions.staleness import mark_graph_stale
from robosystems.operations.locking import RowLockedError
from robosystems.operations.roboledger.commands.reports import (
  NotAuthorizedError,
  ReportHasActiveSharesError,
  ReportNotFiledError,
  delete_report,
  delete_report_artifacts,
)

from ._errors import database_failure
from .document_tools import _check_graph_access, _resolve_acting_user


class DeleteReportTool:
  """Delete a draft or under-review report and its generated facts."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "delete-report",
      "description": """Delete a report that has not been filed, with its generated facts.

**WHEN TO USE:**
- To remove a draft or under-review report that is wrong or no longer needed
- To clear an abandoned draft before creating a new report for the same period

**NOTES:**
- Refused for a filed or archived report: filing makes it a record, and a record is archived, not deleted. Archiving and unarchiving are done by a person
- A generated draft can be deleted; generation does not lock a report, filing does
- To refresh a draft against newer ledger data, use regenerate-report instead
- Refused while the report is shared to other graphs; revoke the shares first
- Only the report's author can delete it

**RETURNS:** `deleted: true` and the report id.
""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "report_id": {
            "type": "string",
            "description": "The report to delete",
          },
        },
        "required": ["report_id"],
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    return await run_off_loop(self._execute_sync, arguments)

  def _execute_sync(self, arguments: dict[str, Any]) -> Any:
    graph_id = self.client.graph_id

    report_id = arguments.get("report_id")
    if not isinstance(report_id, str) or not report_id:
      return {"error": "invalid_input", "message": "report_id is required"}

    access_error = _check_graph_access(graph_id, require_write=True)
    if access_error:
      return access_error

    user_id = _resolve_acting_user(self.client, graph_id)
    if not user_id:
      return {
        "error": "access_denied",
        "message": f"No user found with access to graph {graph_id}",
      }

    try:
      with extensions_session(graph_id) as session:
        deleted = delete_report(
          session,
          report_id,
          user_id,
          acting_user_is_graph_admin=user_is_graph_admin(user_id, graph_id),
        )
    except ReportNotFiledError as e:
      return {"error": "not_allowed", "message": str(e)}
    except NotAuthorizedError:
      return {
        "error": "access_denied",
        "message": "Not authorized to delete this report.",
      }
    except (ReportHasActiveSharesError, RowLockedError) as e:
      return {"error": "conflict", "message": str(e)}
    except SQLAlchemyError as e:
      return database_failure("delete-report", e)
    except Exception as e:
      logger.error(f"delete-report failed for graph_id={graph_id}: {e}")
      return {"error": "command_failed", "message": "delete-report failed"}

    if not deleted:
      return {"error": "not_found", "message": f"Report '{report_id}' not found."}

    # The rows have committed; artifacts go after, never before.
    mark_graph_stale(graph_id, "report_deleted")
    delete_report_artifacts(graph_id, [report_id])
    return {"deleted": True, "report_id": report_id}
