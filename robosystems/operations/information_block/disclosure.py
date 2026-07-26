"""Handler for the ``regulatory_disclosure`` Information Block type.

Disclosure notes are the first render targets beyond the statement
family — an inventory-by-category note, a PP&E-by-class breakdown, a
debt-maturity schedule. Like statements they exercise the
**compositional** construction mode: the Structure (with its
presentation + calculation arcs) exists before any report runs — either
library-seeded or tenant-authored through the TaxonomyBlock envelope —
and per-tenant facts land when ``create-report`` picks the structure
because its concepts received mapped facts (fact-driven picking in
``commands/reports.py``; disclosures are NOT composed by the Reporting
Style, which pins statement layouts only).

The envelope builder is the statement family's, parameterised on the
block_type — ``_build_rows`` and the hierarchy walker are CAP-agnostic,
so a ``roll_up`` note renders rows + footed subtotals through the same
machinery. Two disclosure-specific behaviours layer on top:

- **Arc-less structures return no envelope.** The rs-gaap-disclosures
  package seeds one identity Structure per disclosure
  (``disclosures:BalanceSheet`` … — envelope rows with no arcs). Those
  are disclosure *registry* entries, not renderable blocks; surfacing
  them would flood ``list-information-blocks`` with empty envelopes.
- **Display name = the structure's own name** (a note is named by its
  author/taxonomy, not its type) — handled by the statement builder's
  fallback.

Creation does not flow through ``create-information-block`` — disclosure
structures are vocabulary, authored via ``create-taxonomy-block``
(reporting_extension); the registry installs not-implemented stubs for
the write slots.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from robosystems.models.extensions.structure import TEXT_BLOCK_CAPS, Structure
from robosystems.operations.information_block.envelope import DISCLOSURE_BLOCK_TYPE
from robosystems.operations.information_block.statement import make_statement_handlers
from robosystems.operations.information_block.text_block import (
  build_text_block_envelope,
)

if TYPE_CHECKING:
  from sqlalchemy.orm import Session

  from robosystems.models.api.information_block import InformationBlockEnvelope

DISCLOSURE_DISPLAY_NAME = "Disclosure"
DISCLOSURE_CATEGORY = "Reporting"

# The statement family's envelope builder, bound to this block_type — the
# public factory, so disclosure.py doesn't reach for statement internals.
_build_disclosure_envelope = make_statement_handlers(DISCLOSURE_BLOCK_TYPE)


def build_envelope(
  session: Session,
  structure_id: str,
  fact_set_id: str | None = None,
  scenario_id: str | None = None,
  series: bool = False,
  series_history: int | None = None,
  series_forecast: int | None = None,
) -> InformationBlockEnvelope | None:
  """Pack the envelope for a disclosure-note structure.

  Dispatches by CAP: text-block CAPs render narrative rows through
  :func:`build_text_block_envelope`; every other CAP (roll_up, ...)
  renders the numeric grid through the statement family's builder.

  Returns ``None`` when the structure doesn't exist, isn't a
  ``regulatory_disclosure``, or carries neither arcs nor content (the
  library's disclosure-identity envelopes — not renderable blocks).

  ``scenario_id`` is accepted for dispatch-signature parity and ignored
  — disclosures bind standing document/report content, not scenario
  slices (the forecast engine never emits disclosure sets).
  """
  structure = session.get(Structure, structure_id)
  if structure is None or structure.block_type != DISCLOSURE_BLOCK_TYPE:
    return None
  if (structure.concept_arrangement or "") in TEXT_BLOCK_CAPS:
    return build_text_block_envelope(session, structure_id, fact_set_id)
  envelope = _build_disclosure_envelope(session, structure_id, fact_set_id)
  if envelope is None or not envelope.connections:
    return None
  return envelope


__all__ = [
  "DISCLOSURE_BLOCK_TYPE",
  "DISCLOSURE_CATEGORY",
  "DISCLOSURE_DISPLAY_NAME",
  "build_envelope",
]
