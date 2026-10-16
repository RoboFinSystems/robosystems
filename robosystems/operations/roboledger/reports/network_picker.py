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
from robosystems.operations.roboledger.entity_scope import resolve_entity_id
from robosystems.operations.roboledger.reports.calc_dag import EXTENDS_NETWORK_KEY

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


def network_structure_ids(session: Session, network_id: str) -> list[str]:
  """The network and every active extension layered onto it, the network
  first. Read a statement's arcs from all of them."""
  extensions = session.execute(
    text(
      """
      SELECT id FROM structures
      WHERE metadata ->> :key = :nid AND is_active = true
      ORDER BY created_at, id
      """
    ),
    {"key": EXTENDS_NETWORK_KEY, "nid": network_id},
  ).scalars()
  return [network_id, *(str(sid) for sid in extensions)]


def style_structures_cte(style_param: str = "rsid") -> str:
  """A ``style_structures (structure_id, statement_type)`` CTE body: every
  network a Reporting Style composes, plus the extensions layered onto them.
  Callers bind ``:<style_param>``."""
  return f"""
    style_structures AS (
      SELECT rsn.network_id AS structure_id, rsn.statement_type
      FROM reporting_style_networks rsn
      WHERE rsn.reporting_style_id = :{style_param}
      UNION ALL
      SELECT s.id AS structure_id, rsn.statement_type
      FROM reporting_style_networks rsn
      JOIN structures s
        ON s.metadata ->> '{EXTENDS_NETWORK_KEY}' = rsn.network_id
        AND s.is_active = true
      WHERE rsn.reporting_style_id = :{style_param}
    )
  """


def taggable_flow_qnames(session: Session, reporting_style_id: str) -> set[str]:
  """The investing and financing flows a Reporting Style's cash flow
  statement presents as leaf rows that also foot a subtotal.

  These are the flows a line may be tagged with and the ones a default may
  land on: a flow outside the set computes a fact no row shows. Rows a
  tenant extension layers onto the network count. Empty when the Style
  composes no cash flow network.
  """
  try:
    network = get_render_network(session, reporting_style_id, "cash_flow_statement")
  except NoNetworkForStatementTypeError:
    return set()
  rows = session.execute(
    text(
      """
      SELECT e.qname
      FROM associations p
      JOIN elements e ON e.id = p.to_element_id
      WHERE p.structure_id = ANY(:sids)
        AND p.association_type = 'presentation'
        AND NOT EXISTS (
          SELECT 1 FROM associations c
          WHERE c.structure_id = ANY(:sids)
            AND c.association_type = 'presentation'
            AND c.from_element_id = p.to_element_id
        )
        AND EXISTS (
          SELECT 1 FROM associations k
          JOIN structures ks ON ks.id = k.structure_id
          WHERE k.association_type = 'calculation'
            AND ks.block_type = 'cash_flow_statement'
            AND k.to_element_id = p.to_element_id
        )
        AND EXISTS (
          SELECT 1 FROM element_traits et
          JOIN traits t ON t.id = et.trait_id
          WHERE et.element_id = e.id
            AND t.category = 'activityType'
            AND t.identifier IN ('investingActivity', 'financingActivity')
        )
      """
    ),
    {"sids": network_structure_ids(session, network.structure_id)},
  ).fetchall()
  return {str(r.qname) for r in rows}


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
  """The group parent's Reporting Style. Prefer ``load_entity_reporting_style``
  when the entity is known. Raises ``LookupError`` before any entity exists."""
  return load_entity_reporting_style(session, resolve_entity_id(session))


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
