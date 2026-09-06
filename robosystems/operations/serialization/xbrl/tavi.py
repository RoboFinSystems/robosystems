"""The Tavi flavor: a ``StatementBundle`` as a Project Tavi compiled model.

Tavi (PWD-2026-09-01) is the OIM-family compiled model the SEC pipeline already
writes beside each filing's holon. RoboLedger reports get the same file through
the same emitter — xbrlkit's ``to_tavi_report`` — fed by the bundle → model
bridge in ``operations/serialization/model.py``, so a report and a filing are
byte-comparable and both track the draft through one implementation. The
report-components adapter that renders the SEC files renders these unchanged,
with no RDF step.

Derived on first download and cached per generation, like the holon; carried
across a share beside it. What the bundle holds that the model has no home for
is listed in :data:`TAVI_OMITTED_CONTENT` and surfaced on the download
response, never dropped silently.
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

# Bundle content the Tavi carries nothing for, by the names the response
# documents. The holon and the flat JSON-LD keep all of it.
TAVI_OMITTED_CONTENT: tuple[str, ...] = (
  "ib_envelopes",
  "definition_links",
  "reporting_style",
  "framework_pins",
  "fact_sets",
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
