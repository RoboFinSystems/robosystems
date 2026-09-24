"""Reporting Style → Network picker.

Given a Reporting Style and a statement type, return the Network Structure
the renderer walks, from ``reporting_style_networks`` (one row per
``(reporting_style_id, statement_type)``). A missing row raises
``NoNetworkForStatementTypeError``.
"""

from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.orm import Session

from robosystems.config.constants import ReportingStyleConstants

DEFAULT_STYLE_ID = ReportingStyleConstants.DEFAULT_STYLE_ID


class NoNetworkForStatementTypeError(LookupError):
  """Raised when a Reporting Style has no Network composed for this statement."""

  def __init__(self, reporting_style_id: str, statement_type: str) -> None:
    super().__init__(
      f"Reporting Style {reporting_style_id} has no Network composed "
      f"for statement_type={statement_type!r}. Seed the composition in "
      f"reporting_style_networks."
    )
    self.reporting_style_id = reporting_style_id
    self.statement_type = statement_type


@dataclass(frozen=True)
class RenderNetwork:
  """The Network resolved for one (Reporting Style, statement_type) pair."""

  structure_id: str
  name: str
  concept_arrangement: str | None


def get_render_network(
  session: Session,
  reporting_style_id: str,
  statement_type: str,
) -> RenderNetwork:
  """Resolve the Network this Reporting Style composes for a statement type.

  ``session`` must have the tenant schema active. Raises
  ``NoNetworkForStatementTypeError`` when the composition row is missing or
  the Network is inactive (never render against a stale Network).
  """
  row = session.execute(
    text(
      """
      SELECT s.id, s.name, s.concept_arrangement
      FROM reporting_style_networks rsn
      JOIN structures s ON s.id = rsn.network_id
      WHERE rsn.reporting_style_id = :style_id
        AND rsn.statement_type = :stmt_type
        AND s.is_active = true
      """
    ),
    {"style_id": reporting_style_id, "stmt_type": statement_type},
  ).fetchone()

  if row is None:
    raise NoNetworkForStatementTypeError(reporting_style_id, statement_type)

  return RenderNetwork(
    structure_id=row.id,
    name=row.name,
    concept_arrangement=row.concept_arrangement,
  )


def load_entity_reporting_style(session: Session, entity_id: str) -> str:
  """The Reporting Style pinned on an entity. Raises ``LookupError`` if absent."""
  row = session.execute(
    text("SELECT reporting_style_id FROM entities WHERE id = :eid"),
    {"eid": entity_id},
  ).fetchone()
  if row is None:
    raise LookupError(f"Entity {entity_id!r} not found in tenant schema.")
  # NOT NULL in the model, but a null falls back to the corporate Default
  # rather than handing a null id to the picker.
  return str(row.reporting_style_id) if row.reporting_style_id else DEFAULT_STYLE_ID


def load_primary_reporting_style(session: Session) -> str:
  """The Reporting Style of the graph's primary (earliest-created) entity.

  Must pick the same entity as ``statement_sets._get_entity_id``. Prefer
  ``load_entity_reporting_style`` when the entity is known. Raises
  ``LookupError`` when the tenant has no entity yet.
  """
  row = session.execute(
    text("SELECT reporting_style_id FROM entities ORDER BY created_at ASC LIMIT 1")
  ).fetchone()
  if row is None:
    raise LookupError("No entity found in tenant schema. Import data first.")
  return str(row.reporting_style_id) if row.reporting_style_id else DEFAULT_STYLE_ID


# Corporate default; other entity-form Styles override it in their metadata.
DEFAULT_CLOSE_TARGET_CONCEPT = "rs-gaap:RetainedEarningsAccumulatedDeficit"


def load_close_target_concept(session: Session, reporting_style_id: str) -> str:
  """The equity concept derived cumulative earnings close to for this Style.

  Read from ``structures.metadata.retained_earnings_concept``
  (CORP→RetainedEarnings, PART→PartnersCapital, LLC→MembersEquity); an
  unstamped Style falls back to ``DEFAULT_CLOSE_TARGET_CONCEPT``.
  """
  # Tenant schema, not ``public``: customer-authored Styles live only there.
  row = session.execute(
    text(
      """
      SELECT metadata ->> 'retained_earnings_concept' AS close_target
      FROM structures
      WHERE id = :sid
      """
    ),
    {"sid": reporting_style_id},
  ).fetchone()
  if row is None or not row.close_target:
    return DEFAULT_CLOSE_TARGET_CONCEPT
  return str(row.close_target)
