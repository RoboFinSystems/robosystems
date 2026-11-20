"""Report tools: list-reports, delete-report and get-report-bundle.

delete-report is hand-written because after the rows commit it removes the
report's published artifacts from the object store, which the registrar
runner cannot do (it only marks the graph stale). list-reports and
get-report-bundle are reads the registrar does not generate: the report
list, and a presigned link to a published report's serialization.
"""

from typing import Any

from sqlalchemy.exc import SQLAlchemyError

from robosystems.db.extensions import extensions_session
from robosystems.logger import logger
from robosystems.middleware.auth.dependencies import user_is_graph_admin
from robosystems.middleware.operations import run_off_loop
from robosystems.models.api.extensions.reports import ReportLifecycle
from robosystems.operations.extensions.staleness import mark_graph_stale
from robosystems.operations.locking import RowLockedError
from robosystems.operations.roboledger.commands.reports import (
  NotAuthorizedError,
  ReportHasActiveSharesError,
  ReportNotFiledError,
  delete_report,
  delete_report_artifacts,
)
from robosystems.operations.roboledger.entity_scope import EntityNotInGraphError
from robosystems.operations.roboledger.reads import reports as reads_reports
from robosystems.operations.serialization.flavors import RdfFlavor, XbrlFlavor

from ._arguments import unexpected
from ._errors import database_failure
from .document_tools import _check_graph_access, _resolve_acting_user

_LIFECYCLES = tuple(lifecycle.value for lifecycle in ReportLifecycle)


class ListReportsTool:
  """One entity's report definitions, newest first."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "list-reports",
      "description": """List one entity's reports, newest first: each report's id, name, periods and where it stands.

**WHEN TO USE:**
- "What reports do we have?", or finding a report's id before reading, regenerating or deleting it
- Checking whether a report finished generating, or has been filed

**PARAMETERS:**
- lifecycle (optional, default `current`): `current` leaves out archived reports, `archived` returns only those, `all` returns every report
- entity_id (optional): omit for the group parent, whose list also carries reports shared in from other graphs

**RETURNS:**
`reports`: each with `id`, `name`, `taxonomy_id`, `generation_status` (`generating`, `published`, `failed`), `filing_status` (`draft`, `under_review`, `filed`, `archived`), `period_type`, `period_start`, `period_end`, `periods`, `entity_id` and `entity_name`, and when it was created and last generated. The renderable structures each report's taxonomy offers are left out.

**RELATED TOOLS:**
- create-report / regenerate-report — generate a report, or re-run one against the latest ledger
- disclosures — a report's sections, read by its id""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "lifecycle": {
            "type": "string",
            "enum": list(_LIFECYCLES),
            "default": ReportLifecycle.CURRENT.value,
          },
          "entity_id": {
            "type": "string",
            "description": "The entity whose reports to list. Omit for the group parent.",
          },
        },
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    return await run_off_loop(self._execute_sync, arguments)

  def _execute_sync(self, arguments: dict[str, Any]) -> Any:
    lifecycle = arguments.get("lifecycle") or ReportLifecycle.CURRENT.value
    if lifecycle not in _LIFECYCLES:
      return {
        "error": "invalid_input",
        "message": f"lifecycle must be one of {', '.join(_LIFECYCLES)}",
      }
    try:
      with extensions_session(self.client.graph_id) as session:
        response = reads_reports.list_reports(
          session,
          lifecycle=ReportLifecycle(lifecycle),
          entity_id=arguments.get("entity_id") or None,
        )
    except EntityNotInGraphError as e:
      return {"error": "entity_not_found", "message": str(e)}
    except SQLAlchemyError as e:
      return database_failure("list-reports", e)
    except Exception:
      return unexpected("list-reports")
    # The taxonomy's structures repeat on every report of it; a report's
    # sections are read through `disclosures`.
    return response.model_dump(
      mode="json", exclude={"reports": {"__all__": {"structures"}}}
    )


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
- A report's open columns follow the ledger on their own; to change its periods, use regenerate-report instead
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
  RdfFlavor.HOLON_JSONLD.value,
  RdfFlavor.HOLON_JSONLD_LINEAGE.value,
  XbrlFlavor.TAVI.value,
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
- `report_id`: the report. `list-reports` lists them with their `generation_status`
- `format`: `holon-jsonld` (default) is the JSON-LD holon; `holon-jsonld-lineage` the same holon with its `#lineage` graph — the ledger lines, entries and events behind every frozen figure, for this graph's own members, never shared to another graph; `tavi` the Project Tavi compiled model; `xbrl-2.1` the XBRL 2.1 package

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
            "description": "Serialization to link. Defaults to holon-jsonld.",
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
    flavor = arguments.get("format") or RdfFlavor.HOLON_JSONLD.value
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
          session,
          graph_id,
          report_id,
          flavor=flavor,
          actor_id=_resolve_acting_user(self.client, graph_id),
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
