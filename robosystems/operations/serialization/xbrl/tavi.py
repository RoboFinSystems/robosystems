"""The Tavi flavor: a ``StatementBundle`` as a Project Tavi compiled model,
emitted by the same xbrlkit writer the SEC pipeline uses.

Bundle content the model has no home for is listed in
:data:`TAVI_OMITTED_CONTENT` and surfaced on the download response.
"""

from __future__ import annotations

import json

from xbrlkit.serialize import to_tavi_report

from robosystems.logger import logger
from robosystems.operations.serialization.bundle import StatementBundle
from robosystems.operations.serialization.model import (
  bundle_to_xbrl_model,
  report_identifier,
)

TAVI_MEDIA_TYPE = "application/json"

# The holon keeps all of it. The reporting style and the fact-set partition
# ride the Tavi as `rs:` properties since xbrlkit 0.18.1.
TAVI_OMITTED_CONTENT: tuple[str, ...] = (
  "ib_envelopes",
  "definition_links",
  "framework_pins",
  "filing_lifecycle",
)


def serialize_to_tavi(bundle: StatementBundle) -> bytes:
  """Emit the bundle as compact Tavi JSON — the SEC writer's serialization."""
  report_id = report_identifier(bundle)
  document, gaps = to_tavi_report(
    bundle_to_xbrl_model(bundle),
    report_id=report_id,
    description=tavi_description(bundle),
  )
  logger.debug("Tavi gap report for report %s: %s", report_id, gaps.to_dict())
  return json.dumps(document, separators=(",", ":"), default=str).encode("utf-8")


def tavi_description(bundle: StatementBundle) -> str:
  """The ``documentInfo`` sentence: which report, which generation, whose."""
  meta = bundle.report_meta
  if meta is None:
    subject = "RoboLedger live snapshot"
  else:
    subject = f"RoboLedger report {meta.report_id} g{meta.generation_count}"
  return f"{subject} ({bundle.entity.name}) projected from its StatementBundle by robosystems"
