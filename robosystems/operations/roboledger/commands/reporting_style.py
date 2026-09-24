"""Change-reporting-style command (entity-scoped, tenant-only).

Sets ``entities.reporting_style_id`` after validating the target is a
renderable Style with a Network for every required statement type. Existing
Reports are unaffected: their FactSets pin ``structure_id`` at creation.
"""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from robosystems.models.api.extensions.entity import (
  ChangeReportingStyleRequest,
  ChangeReportingStyleResponse,
)
from robosystems.models.extensions import Entity

# Every Style needs a Network for each; ``comprehensive_income`` is optional.
_REQUIRED_STATEMENT_TYPES: tuple[str, ...] = (
  "balance_sheet",
  "income_statement",
  "cash_flow_statement",
  "equity_statement",
)


class EntityNotFoundError(LookupError):
  """The target (or primary) entity doesn't exist in the graph."""


class ReportingStyleInvalidError(ValueError):
  """The target Style is missing, inactive, wrong-typed, or incomplete."""


def _resolve_entity(session: Session, entity_id: str | None) -> Entity:
  """The given entity, else the primary (earliest-created, as the renderer's
  ``_get_entity_id`` resolves it)."""
  if entity_id:
    entity = session.get(Entity, entity_id)
    if entity is None:
      raise EntityNotFoundError(f"Entity {entity_id!r} not found in this graph.")
    return entity
  entity = session.execute(
    select(Entity).order_by(Entity.created_at.asc()).limit(1)
  ).scalar_one_or_none()
  if entity is None:
    raise EntityNotFoundError("No entity found. Import data before setting a Style.")
  return entity


def change_reporting_style(
  session: Session, body: ChangeReportingStyleRequest
) -> ChangeReportingStyleResponse:
  """Switch a reporting entity's Reporting Style (default: the primary entity).

  Raises `EntityNotFoundError` or `ReportingStyleInvalidError`.
  """
  entity = _resolve_entity(session, body.entity_id)

  previous_style_id = entity.reporting_style_id or None

  if previous_style_id == body.reporting_style_id:
    return ChangeReportingStyleResponse(
      entity_id=entity.id,
      previous_reporting_style_id=previous_style_id,
      reporting_style_id=body.reporting_style_id,
      reporting_style_code=None,
      changed=False,
    )

  row = session.execute(
    text(
      """
      SELECT id, block_type, is_active, metadata
      FROM structures
      WHERE id = :sid
      """
    ),
    {"sid": body.reporting_style_id},
  ).fetchone()
  if row is None:
    raise ReportingStyleInvalidError(
      f"Reporting Style {body.reporting_style_id!r} not found in tenant schema."
    )
  # Legacy ``custom`` Style rows stay selectable; the picker accepts them too.
  if row.block_type not in ("reporting_style", "custom"):
    raise ReportingStyleInvalidError(
      f"Structure {body.reporting_style_id!r} has block_type={row.block_type!r}; "
      f"expected 'reporting_style' (or 'custom' for legacy tenants)."
    )
  if not row.is_active:
    raise ReportingStyleInvalidError(
      f"Reporting Style {body.reporting_style_id!r} is inactive."
    )

  composed = {
    r.statement_type
    for r in session.execute(
      text(
        """
        SELECT statement_type FROM reporting_style_networks
        WHERE reporting_style_id = :sid
        """
      ),
      {"sid": body.reporting_style_id},
    ).fetchall()
  }
  missing = [st for st in _REQUIRED_STATEMENT_TYPES if st not in composed]
  if missing:
    raise ReportingStyleInvalidError(
      f"Reporting Style {body.reporting_style_id!r} has an incomplete "
      f"composition — missing Network(s) for: {', '.join(missing)}. Author the "
      f"missing reporting_style_networks rows before switching."
    )

  # e.g. BSC-CORP-IS02-CF1, stamped at seed time; None if unstamped.
  metadata = row.metadata if isinstance(row.metadata, dict) else {}
  reporting_style_code = metadata.get("reporting_style_code")

  entity.reporting_style_id = body.reporting_style_id
  entity.updated_at = datetime.now(UTC)
  session.commit()

  return ChangeReportingStyleResponse(
    entity_id=entity.id,
    previous_reporting_style_id=previous_style_id,
    reporting_style_id=body.reporting_style_id,
    reporting_style_code=reporting_style_code,
    changed=True,
  )
