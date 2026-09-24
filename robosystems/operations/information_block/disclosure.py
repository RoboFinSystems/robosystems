"""Handler for the ``regulatory_disclosure`` Information Block type.

Disclosure notes render through the statement family's builder (text-block
CAPs through the text-block builder). Facts land when ``create-report`` picks
the structure because its concepts received mapped facts; the Reporting Style
doesn't compose them. Arc-less structures (the library's disclosure identity
rows) return no envelope. Structures are authored via
``create-taxonomy-block``, not ``create-information-block``.
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

  ``None`` when the structure is missing, isn't a ``regulatory_disclosure``,
  or has no arcs. ``scenario_id`` is ignored.
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
