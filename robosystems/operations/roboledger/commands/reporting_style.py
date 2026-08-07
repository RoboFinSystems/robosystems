"""Change-reporting-style command (entity-scoped, tenant-only).

Flips ``entities.reporting_style_id`` after validating the target Style is a
renderable Structure (``block_type='reporting_style'`` — or legacy
``'custom'``) with a complete composition (one Network per required
statement_type) in the graph's tenant schema.

The Reporting Style lives on the entity, co-located with the ``structures``
/ ``reporting_style_networks`` it points at, so this is a pure extensions-DB
write — no platform round-trip. Entities in a multi-entity hierarchy can each
carry their own Style while resolving to the same canonical calc-DAG
subtotals.

Filed Reports are unaffected — each ``Report``'s FactSet rows pin their
``structure_id`` at create-time, so historical packages keep rendering against
the Networks they were authored with. New reports use the new Style.
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

# The picker iterates over these four statement types at render time; every
# Reporting Style must compose a Network for each. ``comprehensive_income``
# stays optional (only Styles that split OCI from the regular IS use it).
_REQUIRED_STATEMENT_TYPES: tuple[str, ...] = (
  "balance_sheet",
  "income_statement",
  "cash_flow_statement",
  "equity_statement",
)


class EntityNotFoundError(LookupError):
  """Raised when the target (or primary) entity doesn't exist in the graph."""


class ReportingStyleInvalidError(ValueError):
  """Raised when the target Style is missing, inactive, wrong-typed, or has
  an incomplete Network composition in the tenant schema."""


def _resolve_entity(session: Session, entity_id: str | None) -> Entity:
  """Resolve the target entity: the given id, else the primary entity.

  Primary = earliest-created, matching ``_get_entity_id`` in the report path
  so the Style set here is the one the renderer resolves for that entity.
  """
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
  """Switch a reporting entity's Reporting Style.

  Args:
      session: Extensions session with the tenant schema active.
      body: Target Style id and optional entity id (defaults to primary).

  Raises:
      EntityNotFoundError: target/primary entity doesn't exist.
      ReportingStyleInvalidError: target Style not found, inactive, has the
          wrong block_type, or has an incomplete composition.
  """
  entity = _resolve_entity(session, body.entity_id)

  previous_style_id = entity.reporting_style_id or None

  # Same-target is an idempotent no-op so a retry doesn't churn the row or
  # surface a spurious change downstream.
  if previous_style_id == body.reporting_style_id:
    return ChangeReportingStyleResponse(
      entity_id=entity.id,
      previous_reporting_style_id=previous_style_id,
      reporting_style_id=body.reporting_style_id,
      reporting_style_code=None,
      changed=False,
    )

  # Validate the target Style in the tenant's ``structures`` (search_path).
  # Style rows — seeded or customer-authored — live in the tenant schema, so
  # this is the only place that can confirm the row exists and is renderable.
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
  # ``custom`` is accepted so a Style row that was never promoted to
  # ``block_type='reporting_style'`` stays selectable. The picker doesn't
  # filter on block_type either.
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

  # 4-segment Reporting Style code (e.g. BSC-CORP-IS02-CF1), stamped into the
  # Style Structure's metadata at seed time. None on an unstamped Style.
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
