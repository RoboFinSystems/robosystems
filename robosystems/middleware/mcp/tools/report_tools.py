"""Report tools: delete-report and get-report-bundle.

delete-report is hand-written because after the rows commit it removes the
report's published artifacts from the object store, which the registrar
runner cannot do (it only marks the graph stale). get-report-bundle is a
read the registrar does not generate: a presigned link to a published
report's serialization.
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
from robosystems.operations.roboledger.reads import reports as reads_reports
from robosystems.operations.serialization.flavors import RdfFlavor, XbrlFlavor

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


_BUNDLE_FORMATS = (
  XbrlFlavor.TAVI.value,
  RdfFlavor.HOLON_JSONLD.value,
  XbrlFlavor.XBRL_2_1.value,
)


class GetReportBundleTool:
  """Presigned link to a published report's serialization bundle."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "get-report-bundle",
      "description": """Get a short-lived download link for a published report, as a file another tool can open.

**WHEN TO USE:**
- To open a report in a local XBRL reader or report viewer that loads a file by URL
- To hand a report to a tool outside this graph as a standards-form document

**PARAMETERS:**
- `report_id`: the report. List them with query-graphql: `{ reports { reports { id name generationStatus } } }`
- `format`: `tavi` (default) is the Project Tavi compiled model, the report in the form XBRL tools read; `holon-jsonld` the JSON-LD holon; `xbrl-2.1` the XBRL 2.1 package

**RETURNS:** `download_url`, `expires_at`, `content_type`, `format`, and `omitted_content` — what the report carries that this format does not.

**NOTES:**
- The link carries its own credential and expires in five minutes. Pass it straight to a tool that fetches it on this machine, and fetch a new one when it lapses
- Never place the link inside another URL, a browser address, or a message: anyone holding it can download the report until it expires
- Only a report whose generation has finished has a bundle; one still generating, or whose generation failed, is refused — regenerate-report produces one
""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "report_id": {
            "type": "string",
            "description": "The published report to link",
          },
          "format": {
            "type": "string",
            "enum": list(_BUNDLE_FORMATS),
            "description": "Serialization to link. Defaults to tavi.",
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
    flavor = arguments.get("format") or XbrlFlavor.TAVI.value
    if flavor not in _BUNDLE_FORMATS:
      return {
        "error": "invalid_input",
        "message": f"format must be one of {', '.join(_BUNDLE_FORMATS)}",
      }

    access_error = _check_graph_access(graph_id)
    if access_error:
      return access_error

    try:
      with extensions_session(graph_id) as session:
        response = reads_reports.get_report_download_url(
          session, graph_id, report_id, flavor=flavor
        )
    except reads_reports.ReportBundleNotAvailableError as e:
      return {"error": "not_available", "message": str(e)}
    except reads_reports.BundleSigningError:
      return {
        "error": "command_failed",
        "message": "The report bundle could not be signed; try again.",
      }
    except SQLAlchemyError as e:
      return database_failure("get-report-bundle", e)
    except Exception as e:
      logger.error(f"get-report-bundle failed for graph_id={graph_id}: {e}")
      return {"error": "command_failed", "message": "get-report-bundle failed"}

    if response is None:
      return {"error": "not_found", "message": f"Report '{report_id}' not found."}
    return {"report_id": report_id, **response.model_dump(mode="json")}
