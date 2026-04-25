"""Handlers for the statement family of Information Block types.

Four block types — ``balance_sheet``, ``income_statement``,
``cash_flow_statement``, ``equity_statement`` — share one envelope
builder parameterised on the block_type string. Each corresponds to a
library-seeded Structure in ``public.structures``; seeds live in
:mod:`robosystems.taxonomy.seed` (``seed_reporting_taxonomy``).

Statements exercise the **compositional** construction mode: the
Structure exists before any tenant action; per-tenant facts come from
``create-report`` calls; the envelope materialises on GET by pulling
the library atoms together with the tenant's most-recent report facts.

Statements aren't created via ``create-information-block``; the
``dispatch_create``/``update``/``delete`` handlers in the registry
entry are the not-implemented stubs built by
``make_not_implemented_handler``.
"""

from __future__ import annotations

from collections.abc import Callable
from functools import partial
from typing import TYPE_CHECKING

from sqlalchemy import select

from robosystems.models.api.information_block import (
  ArtifactResponse,
  InformationBlockEnvelope,
  InformationModelResponse,
  StatementMechanics,
)
from robosystems.models.extensions.roboledger import Fact, Report
from robosystems.operations.information_block.envelope import (
  association_to_connection,
  element_to_lite,
  fact_to_lite,
  load_base_envelope_atoms,
)

if TYPE_CHECKING:
  from sqlalchemy.orm import Session


# block_type → (display_name, display_plural)
STATEMENT_DISPLAY: dict[str, tuple[str, str]] = {
  "balance_sheet": ("Balance Sheet", "Balance Sheets"),
  "income_statement": ("Income Statement", "Income Statements"),
  "cash_flow_statement": ("Cash Flow Statement", "Cash Flow Statements"),
  "equity_statement": ("Equity Statement", "Equity Statements"),
}

# All four statement block types share the same sidebar category.
STATEMENT_CATEGORY = "Reporting"


def _build_statement_envelope(
  session: Session,
  structure_id: str,
  *,
  block_type: str,
) -> InformationBlockEnvelope | None:
  """Pack the Information Block envelope for a statement-family block.

  Returns ``None`` when the structure doesn't exist or is not the
  expected block_type — lets :func:`get_information_block` cleanly
  return nothing to the caller.

  Surfaces facts from the **most recent** Report that has at least one
  fact for this structure's elements. Scoping by element membership —
  rather than taking the latest Report of any type — avoids an empty
  envelope when a tenant's most recent report is for a different
  statement (e.g. asking for the BS envelope when the newest report is
  an IS). On the library sentinel the search_path is ``public`` and the
  Report table is empty, so ``facts`` comes back empty, which is the
  correct behaviour for the sentinel. A future revision of this
  behaviour will replace the heuristic with explicit ``fact_set_id``
  selection once write paths stamp FactSet rows on every report.
  """
  atoms = load_base_envelope_atoms(
    session, structure_id, expected_block_type=block_type
  )
  if atoms is None:
    return None

  structure = atoms.structure
  element_ids = atoms.element_ids

  facts: list[Fact] = []
  if element_ids:
    latest_report_id = session.execute(
      select(Report.id)
      .join(Fact, Fact.report_id == Report.id)
      .where(Fact.element_id.in_(element_ids))
      .order_by(Report.created_at.desc())
      .limit(1)
    ).scalar()

    if latest_report_id is not None:
      facts = list(
        session.execute(
          select(Fact).where(
            Fact.report_id == latest_report_id,
            Fact.element_id.in_(element_ids),
          )
        )
        .scalars()
        .all()
      )

  # Mechanics are read from the typed ``artifact_mechanics`` column when
  # populated; library-seeded rows that haven't been enriched fall back
  # to an empty tagged body so the discriminated union still validates.
  if structure.artifact_mechanics:
    mechanics = StatementMechanics.model_validate(structure.artifact_mechanics)
  else:
    mechanics = StatementMechanics(kind="statement_renderer")

  display_name, _display_plural = STATEMENT_DISPLAY[block_type]
  return InformationBlockEnvelope(
    id=structure.id,
    block_type=block_type,
    name=structure.name,
    display_name=display_name,
    category=STATEMENT_CATEGORY,
    taxonomy_id=structure.taxonomy_id,
    taxonomy_name=atoms.taxonomy_name,
    information_model=InformationModelResponse(
      concept_arrangement=structure.concept_arrangement or "roll_up",
      member_arrangement=structure.member_arrangement or "aggregation",
    ),
    artifact=ArtifactResponse(
      topic=structure.description,
      parenthetical_note=structure.parenthetical_note,
      template=None,
      mechanics=mechanics,
    ),
    elements=[element_to_lite(e) for e in atoms.elements],
    connections=[
      association_to_connection(a, atoms.classifications_by_assoc.get(a.id, []))
      for a in atoms.associations
    ],
    facts=[fact_to_lite(f) for f in facts],
    rules=atoms.rules,
    fact_set=atoms.fact_set,
    verification_results=atoms.verification_results,
  )


def make_statement_handlers(
  block_type: str,
) -> Callable[[Session, str], InformationBlockEnvelope | None]:
  """Build the envelope handler for one statement type.

  ``functools.partial`` binds the ``block_type`` keyword on the envelope
  builder so the registry entry holds a two-argument callable matching
  :class:`BlockTypeRegistryEntry.dispatch_build_envelope`'s signature
  ``(session, structure_id) -> envelope | None``.

  The create / update / delete handlers for statement block types are
  not built here — the registry installs not-implemented stubs via
  ``make_not_implemented_handler`` for those slots.
  """
  return partial(_build_statement_envelope, block_type=block_type)


__all__ = [
  "STATEMENT_CATEGORY",
  "STATEMENT_DISPLAY",
  "make_statement_handlers",
]
