"""Handlers for the statement family of Information Block types.

Four block types — ``balance_sheet``, ``income_statement``,
``cash_flow_statement``, ``equity_statement`` — share one handler body
parameterised on the block_type string. Each corresponds to a
library-seeded Structure in ``public.structures``; seeds live in
:mod:`robosystems.taxonomy.seed` (``seed_reporting_taxonomy``).

Statements exercise the **compositional** construction mode: the
Structure exists before any tenant action; per-tenant facts come from
``create-report`` calls; the envelope materialises on GET by pulling
the library atoms together with the tenant's most-recent report facts.

Because statements aren't created via ``create-information-block``,
``dispatch_create`` for every statement block type raises
:class:`NotImplementedError`, which the registrar's error map
translates to an HTTP 501 with a pointer to ``create-report``.
"""

from __future__ import annotations

from collections.abc import Callable
from functools import partial
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel
from sqlalchemy import select

from robosystems.models.api.information_block import (
  ArtifactResponse,
  InformationBlockEnvelope,
  InformationModelResponse,
  StatementMechanics,
)
from robosystems.models.extensions import Association, Element, Structure
from robosystems.models.extensions.roboledger import Fact, Report
from robosystems.operations.information_block.envelope import (
  association_to_connection,
  element_to_lite,
  fact_to_lite,
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


def _create_not_implemented(
  session: Session, payload: BaseModel, created_by: str
) -> str:
  """``dispatch_create`` for every statement block type.

  Statements are generated through ``create-report``, not through
  ``create-information-block``. Raising here propagates to the router's
  error map and becomes HTTP 501.
  """
  raise NotImplementedError(
    "Statements are generated via create-report, not "
    "create-information-block. Create a report with the relevant "
    "taxonomy; the statement envelope becomes queryable via "
    "informationBlocks(blockType=...) after the report is published."
  )


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

  Phase b surfaces facts from the **most recent** Report for the tenant
  in the current session context. On the library sentinel the
  search_path is ``public`` and the Report table is empty, so
  ``facts`` comes back empty — which is the correct behaviour for the
  sentinel. Phase z replaces the "most recent report" heuristic with
  explicit ``fact_set_id`` selection.
  """
  structure = session.get(Structure, structure_id)
  if structure is None or structure.structure_type != block_type:
    return None

  associations = (
    session.execute(select(Association).where(Association.structure_id == structure_id))
    .scalars()
    .all()
  )

  element_ids = {a.from_element_id for a in associations} | {
    a.to_element_id for a in associations
  }
  elements = (
    session.execute(select(Element).where(Element.id.in_(element_ids))).scalars().all()
    if element_ids
    else []
  )

  latest_report_id = session.execute(
    select(Report.id).order_by(Report.created_at.desc()).limit(1)
  ).scalar()

  facts: list[Fact] = []
  if latest_report_id is not None and element_ids:
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

  display_name, _display_plural = STATEMENT_DISPLAY[block_type]
  return InformationBlockEnvelope(
    id=structure.id,
    block_type=block_type,
    name=structure.name,
    display_name=display_name,
    category=STATEMENT_CATEGORY,
    information_model=InformationModelResponse(
      concept_arrangement="roll_up",
      member_arrangement="aggregation",
    ),
    artifact=ArtifactResponse(
      topic=structure.description,
      parenthetical_note=None,
      template=None,
      mechanics=StatementMechanics(kind="statement_renderer"),
    ),
    elements=[element_to_lite(e) for e in elements],
    connections=[association_to_connection(a) for a in associations],
    facts=[fact_to_lite(f) for f in facts],
  )


def make_statement_handlers(block_type: str) -> dict[str, Callable[..., Any]]:
  """Build the ``(create, build_envelope)`` pair for one statement type.

  ``functools.partial`` binds the ``block_type`` keyword on the
  envelope builder so the registry entry holds a two-argument callable
  matching :class:`BlockTypeRegistryEntry.dispatch_build_envelope`'s
  signature ``(session, structure_id) -> envelope | None``.
  """
  return {
    "create": _create_not_implemented,
    "build_envelope": partial(_build_statement_envelope, block_type=block_type),
  }


__all__ = [
  "STATEMENT_CATEGORY",
  "STATEMENT_DISPLAY",
  "make_statement_handlers",
]
