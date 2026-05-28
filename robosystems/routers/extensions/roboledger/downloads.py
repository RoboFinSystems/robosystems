"""RoboLedger serialization bundle downloads.

REST GET endpoints that return short-lived presigned URLs to S3-stamped
serialization artifacts. Sibling to ``operations.py`` (command writes),
``reads.py`` (OLTP analytical reads), and ``views.py`` (graph-backed
reads). Downloads are simple lookups — they don't carry the
``OperationEnvelope`` shape, don't need idempotency keys, and don't
mutate state. They live here rather than under ``/operations/`` so the
URL semantics stay aligned with the REST resource (``/reports/{id}``)
rather than dressing up as an operation.

Currently exposes one route, with the format-family enum scaffolded so
the XBRL flavor (Phase 1b) drops in by extending the dispatch arm:

* ``GET /extensions/roboledger/{graph_id}/reports/{report_id}/download``
  — returns a presigned URL to the stamped JSON-LD bundle, gated by
  the same per-graph access dependency as the rest of the extension.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from pydantic import BaseModel, Field
from sqlalchemy.exc import ProgrammingError

from robosystems.db.extensions import extensions_session
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.extensions import (
  GraphExtensionContext,
  require_graph_extension,
)
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
from robosystems.middleware.otel.metrics import endpoint_metrics_decorator
from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency
from robosystems.models.core import User
from robosystems.models.extensions.roboledger.report import Report
from robosystems.operations.aws.s3 import S3Client
from robosystems.operations.serialization import RdfFlavor
from robosystems.routers.extensions.roboledger.operations import _ledger_404

router = APIRouter()

_OP_TAG = "Extensions: RoboLedger"
_RATE_LIMIT = Depends(subscription_aware_rate_limit_dependency)
_require_roboledger = require_graph_extension("roboledger")

# Maximum lifetime of a presigned URL. Short window — clients are
# expected to follow the redirect immediately; long-lived URLs are a
# share path, not a download path (the share path is Phase 2).
_PRESIGN_DEFAULT_SECONDS = 300
_PRESIGN_MAX_SECONDS = 3600


class ReportBundleDownloadResponse(BaseModel):
  """Presigned-URL response for a Report bundle download.

  Mirrors :class:`BackupDownloadUrlResponse` in shape — the frontend
  treats both the same way (fetch, follow URL, GET the artifact).
  """

  download_url: str = Field(
    ..., description="Presigned URL that streams the bundle directly from S3."
  )
  expires_at: datetime = Field(
    ..., description="UTC timestamp at which the presigned URL stops working."
  )
  content_type: str = Field(
    ..., description="MIME type of the artifact behind the URL."
  )
  format: str = Field(
    ...,
    description=(
      "Serialization flavor delivered by this URL — matches the "
      "``format`` query parameter."
    ),
  )
  generation_count: int = Field(
    ..., description="Bundle generation number stamped on the Report."
  )


@router.get(
  "/reports/{report_id}/download",
  response_model=ReportBundleDownloadResponse,
  operation_id="getReportBundleDownloadUrl",
  summary="Download Report bundle",
  description=(
    "Return a short-lived presigned URL to the published Report's "
    "serialization bundle. JSON-LD (default) is stored at publish; "
    "additional flavors slot in as Phase 1b/Phase 4 work. 404 when "
    "the Report exists but has no bundle (published before the "
    "serialization feature shipped)."
  ),
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
)
@endpoint_metrics_decorator(
  "/extensions/roboledger/{graph_id}/reports/{report_id}/download",
  business_event_type="report_bundle_download_url_generated",
)
async def get_report_bundle_download_url(
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  report_id: str = Path(..., description="Report identifier (rpt_-prefixed ULID)."),
  format: str = Query(
    RdfFlavor.JSONLD.value,
    description=(
      "Serialization flavor. ``jsonld`` returns the stored JSON-LD "
      "artifact. XBRL flavors arrive in Phase 1b."
    ),
  ),
  expires_in: int = Query(
    _PRESIGN_DEFAULT_SECONDS,
    ge=60,
    le=_PRESIGN_MAX_SECONDS,
    description=(
      f"Presigned URL lifetime in seconds (min 60, max {_PRESIGN_MAX_SECONDS})."
    ),
  ),
  _user: User = Depends(get_current_user_with_graph),
  _ext: GraphExtensionContext = Depends(_require_roboledger),
) -> ReportBundleDownloadResponse:
  # Phase 1a only the JSON-LD flavor is downloadable; other flavors
  # are reserved enum values until their producers land. Surface a
  # specific 400 rather than a generic 404 so the client knows the
  # report exists but the flavor isn't supported yet.
  try:
    flavor = RdfFlavor(format)
  except ValueError:
    raise HTTPException(
      status_code=400,
      detail=(
        f"Unsupported format '{format}'. Supported flavors: "
        f"{', '.join(f.value for f in RdfFlavor)}."
      ),
    )
  if flavor is not RdfFlavor.JSONLD:
    raise HTTPException(
      status_code=400,
      detail=(f"Format '{flavor.value}' is reserved for future implementation."),
    )

  try:
    with extensions_session(graph_id) as session:
      report = session.get(Report, report_id)
      if report is None:
        raise HTTPException(status_code=404, detail=f"Report '{report_id}' not found.")
      if not report.bundle_url:
        raise HTTPException(
          status_code=404,
          detail=(
            f"Report '{report_id}' has no stamped bundle — it was "
            f"published before the serialization feature shipped. "
            f"Regenerate the report to produce a bundle."
          ),
        )
      # Project the ORM-loaded values into locals before the session
      # closes — Report becomes detached on context exit, and any
      # lazy attribute access after that raises DetachedInstanceError.
      bundle_uri = str(report.bundle_url)
      generation_count = int(report.generation_count or 0)
  except ProgrammingError:
    raise _ledger_404()

  bucket, key = _parse_s3_uri(bundle_uri)
  if bucket is None or key is None:
    # Bundle URL stamped but malformed — internal data integrity issue,
    # not a client error. 500 with a generic message; logs carry the
    # specific URL.
    raise HTTPException(
      status_code=500,
      detail="Bundle URL is malformed; cannot generate download link.",
    )

  download_url = S3Client().generate_presigned_url(
    bucket=bucket,
    key=key,
    expires_in=expires_in,
    response_content_type="application/ld+json",
    response_content_disposition=(
      f'attachment; filename="{report_id}-g{generation_count}.jsonld"'
    ),
  )
  if download_url is None:
    raise HTTPException(
      status_code=502,
      detail="Failed to sign download URL for the bundle.",
    )

  return ReportBundleDownloadResponse(
    download_url=download_url,
    expires_at=datetime.now(UTC) + timedelta(seconds=expires_in),
    content_type="application/ld+json",
    format=flavor.value,
    generation_count=generation_count,
  )


def _parse_s3_uri(uri: str) -> tuple[str | None, str | None]:
  """Split an ``s3://bucket/key`` URI into ``(bucket, key)``.

  Returns ``(None, None)`` on any malformed input so the caller can
  produce a single error path instead of branching on the failure
  shape. The ``env.USER_DATA_BUCKET`` cross-check is intentionally
  *not* applied here — bundles stamped under a legacy bucket name
  still resolve, and a misconfigured ``USER_DATA_BUCKET`` shouldn't
  silently 404 customer downloads.
  """
  if not uri.startswith("s3://"):
    return None, None
  remainder = uri[len("s3://") :]
  if "/" not in remainder:
    return None, None
  bucket, _, key = remainder.partition("/")
  if not bucket or not key:
    return None, None
  return bucket, key
