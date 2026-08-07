"""Reporting Style → Network picker.

The render-target resolver: given the graph's Reporting Style and a
statement type, return the Network Structure the renderer should walk.
Single source of truth for both the saved-report path
(``commands/reports.py``) and the live-statement path
(``reports/fact_grid.py``).

Composition lives in ``reporting_style_networks`` (one row per
``(reporting_style_id, statement_type)``); the Default Style ships
seeded with rs-gaap Classified BS / Multi-step IS / Indirect CF /
Equity Roll Forward. Other Styles are placeholders until library or
customer authoring fills them in.

The picker runs strict: a missing composition row raises
``NoNetworkForStatementTypeError``. Tenants pick up the composition by
re-provisioning.
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
  """Resolve which Network this Reporting Style composes for the given
  statement type.

  Single join against ``reporting_style_networks ⋈ structures``. Filters
  on ``structures.is_active`` so a deactivated Network composition row
  reads as missing (forces the caller to author a replacement rather
  than silently rendering against a stale Network).

  ``session`` must have the tenant schema active via ``SET search_path``;
  everything resolves in it with no fan-out to the platform DB.
  ``reporting_style_id`` comes from ``entities.reporting_style_id`` (see
  :func:`load_entity_reporting_style` / :func:`load_primary_reporting_style`).
  ``statement_type`` is one of ``balance_sheet`` / ``income_statement`` /
  ``cash_flow_statement`` / ``equity_statement`` / ``comprehensive_income``.

  Raises ``NoNetworkForStatementTypeError`` when the composition row is
  missing or the target Network is inactive.
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
  """The Reporting Style pinned on a specific entity (tenant schema).

  The style lives on the entity, co-located with the ``structures`` /
  ``reporting_style_networks`` it points at — no cross-DB hop. Used by the
  render path once it has resolved which entity a report belongs to.

  Raises ``LookupError`` when the entity doesn't exist in the tenant schema.
  """
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

  Matches how ``_get_entity_id`` (commands/reports.py) selects the primary
  entity for single-entity graphs, so the style the renderer resolves lines
  up with the entity whose facts it walks. Callers that already know the
  target entity should prefer ``load_entity_reporting_style``.

  Raises ``LookupError`` when the tenant has no entity yet.
  """
  row = session.execute(
    text("SELECT reporting_style_id FROM entities ORDER BY created_at ASC LIMIT 1")
  ).fetchone()
  if row is None:
    raise LookupError("No entity found in tenant schema. Import data first.")
  return str(row.reporting_style_id) if row.reporting_style_id else DEFAULT_STYLE_ID


# Corporate default — the form whose accumulated earnings get their own
# named line. Partnership/LLC/etc. Styles override this in their metadata,
# stamped from the package's ``retainedEarningsConcept``.
DEFAULT_CLOSE_TARGET_CONCEPT = "rs-gaap:RetainedEarningsAccumulatedDeficit"


def load_close_target_concept(session: Session, reporting_style_id: str) -> str:
  """The equity concept derived cumulative earnings close to for this Style.

  Each Reporting Style declares exactly one earnings-home concept
  (``retainedEarningsConcept`` in the package, stamped into
  ``structures.metadata.retained_earnings_concept``): CORP→RetainedEarnings,
  PART→PartnersCapital, LLC→MembersEquity. A single authoritative value per
  Style — never a runtime scan. An unstamped Style falls back to
  ``DEFAULT_CLOSE_TARGET_CONCEPT``.

  ``session`` must have the tenant schema active.
  """
  # Read from the tenant's ``structures`` (search_path), NOT ``public`` —
  # the Style row is mirrored into each tenant with its stamped metadata by
  # ``copy_library_into_tenant``, and customer-authored Styles live only in
  # the tenant. This is consistent with how ``get_render_network`` and the
  # rest of the renderer read; an explicit ``public.`` prefix would miss
  # tenant-only Styles.
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
