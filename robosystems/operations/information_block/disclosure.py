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

from robosystems.operations.information_block.statement import (
  _build_statement_envelope,  # type: ignore[reportPrivateUsage]
)

if TYPE_CHECKING:
  from sqlalchemy.orm import Session

  from robosystems.models.api.information_block import InformationBlockEnvelope

DISCLOSURE_BLOCK_TYPE = "regulatory_disclosure"
DISCLOSURE_DISPLAY_NAME = "Disclosure"
DISCLOSURE_CATEGORY = "Reporting"


def build_envelope(
  session: Session,
  structure_id: str,
  fact_set_id: str | None = None,
) -> InformationBlockEnvelope | None:
  """Pack the envelope for a disclosure-note structure.

  Returns ``None`` when the structure doesn't exist, isn't a
  ``regulatory_disclosure``, or carries no arcs (the library's
  disclosure-identity envelopes — not renderable blocks).
  """
  envelope = _build_statement_envelope(
    session,
    structure_id,
    fact_set_id,
    block_type=DISCLOSURE_BLOCK_TYPE,
  )
  if envelope is None or not envelope.connections:
    return None
  return envelope


__all__ = [
  "DISCLOSURE_BLOCK_TYPE",
  "DISCLOSURE_CATEGORY",
  "DISCLOSURE_DISPLAY_NAME",
  "build_envelope",
]
