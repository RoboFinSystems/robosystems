"""RoboLedger serialization bundle downloads.

REST GET endpoints that return serialization artifacts. Sibling to
``operations.py`` (command writes), ``reads.py`` (OLTP analytical
reads), and ``views.py`` (graph-backed reads). Downloads are simple
lookups — they don't carry the ``OperationEnvelope`` shape, don't
need idempotency keys, and don't mutate state. They live here rather
than under ``/operations/`` so the URL semantics stay aligned with
the REST resource (``/reports/{id}``) rather than dressing up as an
operation.

* ``GET .../reports/{id}/download?format=jsonld`` — returns a JSON
  envelope with a short-lived presigned URL to the stamped JSON-LD
  bundle in S3. The bundle is generated at publish time and reused
  across downloads.
* ``GET .../reports/{id}/download?format=xbrl-2.1`` — rebuilds the
  bundle on-demand and streams an XBRL 2.1 zip directly. Not stored;
  the cost of regenerating per download is acceptable, with an async
  pre-generate path available if profiling reveals latency issues at
  scale.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from fastapi.responses import Response
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
from robosystems.operations.serialization import (
  RdfFlavor,
  XbrlFlavor,
  build_report_bundle,
  serialize_to_xbrl,
)
from robosystems.routers.extensions.roboledger.operations import _ledger_404

router = APIRouter()

_OP_TAG = "Extensions: RoboLedger"
_RATE_LIMIT = Depends(subscription_aware_rate_limit_dependency)
_require_roboledger = require_graph_extension("roboledger")

# Maximum lifetime of a presigned URL. Short window — clients are
# expected to follow the redirect immediately; long-lived URLs are a
# share path, not a download path.
_PRESIGN_DEFAULT_SECONDS = 300
_PRESIGN_MAX_SECONDS = 3600

# All format values the endpoint understands. Combines both encoder
# families so the dispatch logic has a single source of truth.
_ALL_FORMATS: tuple[str, ...] = tuple(
  [f.value for f in RdfFlavor] + [f.value for f in XbrlFlavor]
)


class ReportBundleDownloadResponse(BaseModel):
  """Presigned-URL response for a Report bundle download.

  Mirrors :class:`BackupDownloadUrlResponse` in shape — the frontend
  treats both the same way (fetch, follow URL, GET the artifact).

  Only returned for RDF-family flavors (JSON-LD) where the artifact
  is stored in S3. XBRL flavors stream the binary content directly
  in the response body (no JSON wrapper).
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
  # Response shape varies by format query param (JSON envelope for RDF
  # flavors, binary zip for XBRL flavors). FastAPI can't auto-derive a
  # single response model from a Union[JSON, binary]; opt out of
  # auto-derivation and document the two responses explicitly below.
  response_model=None,
  responses={
    200: {
      "description": (
        "JSON envelope (RDF flavors) or zip stream (XBRL flavors); see "
        "``format`` query parameter."
      ),
      "content": {
        "application/json": {
          "schema": ReportBundleDownloadResponse.model_json_schema()
        },
        "application/zip": {"schema": {"type": "string", "format": "binary"}},
      },
    },
  },
  operation_id="getReportBundleDownloadUrl",
  summary="Download Report bundle",
  description=(
    "Return the published Report's serialization bundle. ``format=jsonld`` "
    "(default) returns a JSON envelope containing a short-lived presigned "
    "URL to the stamped JSON-LD bundle in S3. ``format=xbrl-2.1`` rebuilds "
    "the bundle on-demand and streams an XBRL 2.1 zip directly. 404 when "
    "the Report has no stamped bundle (published before the serialization "
    "feature shipped — JSON-LD only)."
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
      "Serialization flavor. ``jsonld`` returns a presigned URL to the "
      "stored JSON-LD bundle; ``xbrl-2.1`` streams a freshly-emitted "
      "XBRL zip directly. Other RDF / XBRL flavors slot in as their "
      "producers ship."
    ),
  ),
  expires_in: int = Query(
    _PRESIGN_DEFAULT_SECONDS,
    ge=60,
    le=_PRESIGN_MAX_SECONDS,
    description=(
      f"Presigned URL lifetime in seconds (min 60, max {_PRESIGN_MAX_SECONDS}). "
      f"Ignored for XBRL flavors (streamed directly, no URL)."
    ),
  ),
  _user: User = Depends(get_current_user_with_graph),
  _ext: GraphExtensionContext = Depends(_require_roboledger),
) -> ReportBundleDownloadResponse | Response:
  if format in {f.value for f in RdfFlavor}:
    return _download_rdf(
      graph_id=graph_id,
      report_id=report_id,
      flavor=RdfFlavor(format),
      expires_in=expires_in,
    )
  if format in {f.value for f in XbrlFlavor}:
    return _download_xbrl(
      graph_id=graph_id,
      report_id=report_id,
      flavor=XbrlFlavor(format),
    )
  raise HTTPException(
    status_code=400,
    detail=(
      f"Unsupported format '{format}'. Supported flavors: {', '.join(_ALL_FORMATS)}."
    ),
  )


# ── RDF-family download (presigned URL response) ─────────────────────────


def _download_rdf(
  graph_id: str,
  report_id: str,
  flavor: RdfFlavor,
  expires_in: int,
) -> ReportBundleDownloadResponse:
  """JSON-LD path — return a presigned URL to the stored S3 bundle."""
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
      bundle_uri = str(report.bundle_url)
      generation_count = int(report.generation_count or 0)
  except ProgrammingError:
    raise _ledger_404()

  bucket, key = _parse_s3_uri(bundle_uri)
  if bucket is None or key is None:
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


# ── XBRL-family download (binary stream response) ────────────────────────


def _download_xbrl(
  graph_id: str,
  report_id: str,
  flavor: XbrlFlavor,
) -> Response:
  """XBRL path — rebuild bundle, emit zip, stream directly.

  XBRL is an on-demand emit, not stored. The cost of regenerating on
  each download is acceptable at current scale; an async pre-generate +
  S3-stamp path can be added if profiling reveals latency issues.
  """
  try:
    with extensions_session(graph_id) as session:
      report = session.get(Report, report_id)
      if report is None:
        raise HTTPException(status_code=404, detail=f"Report '{report_id}' not found.")
      generation_count = int(report.generation_count or 0)
      bundle = build_report_bundle(session, graph_id, report_id)
  except ProgrammingError:
    raise _ledger_404()
  except LookupError as exc:
    # build_report_bundle raises LookupError for missing report / graph;
    # the latter shouldn't be possible since we just resolved Report, but
    # surface explicitly so tests catch any drift.
    raise HTTPException(status_code=404, detail=str(exc))

  zip_bytes = serialize_to_xbrl(bundle, flavor)
  filename = f"{report_id}-g{generation_count}.zip"
  return Response(
    content=zip_bytes,
    media_type="application/zip",
    headers={
      "Content-Disposition": f'attachment; filename="{filename}"',
      "X-Bundle-Format": flavor.value,
      "X-Bundle-Generation": str(generation_count),
    },
  )


# ── Helpers ──────────────────────────────────────────────────────────────


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
