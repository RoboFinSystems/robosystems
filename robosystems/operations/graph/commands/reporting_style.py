"""Change-reporting-style command (Phase 2 of roadmap §3.2).

Synchronous platform-DB update. Validates the target Style is a
library- or customer-authored Structure with
``structure_type='reporting_style'`` and has a complete composition
(one Network per required statement_type) in the graph's tenant
schema. Then flips ``graphs.reporting_style_id``.

Filed Reports are unaffected — each ``Report``'s FactSet rows already
pin their ``structure_id`` at create-time, so historical packages
keep rendering against the Networks they were authored with. New
reports use the new Style.
"""

from __future__ import annotations

from typing import Any

from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session

from robosystems.models.core import User
from robosystems.models.core.graph import Graph

# The picker iterates over these four statement types at render time;
# every Reporting Style must compose a Network for each of them.
# ``comprehensive_income`` stays optional (only used by Styles that
# split OCI from the regular IS).
_REQUIRED_STATEMENT_TYPES: tuple[str, ...] = (
  "balance_sheet",
  "income_statement",
  "cash_flow_statement",
  "equity_statement",
)


async def change_reporting_style_cmd(
  graph_id: str,
  new_reporting_style_id: str,
  current_user: User,
  db: Session,
) -> dict[str, Any]:
  """Switch the graph's Reporting Style.

  Args:
      graph_id: Target graph (path parameter).
      new_reporting_style_id: Structure id of the target Style — must
          exist in the graph's tenant extensions schema with
          ``structure_type='reporting_style'`` and have a complete
          composition.
      current_user: Authenticated caller. Must be an organization
          owner — the Reporting Style governs every render the org
          produces going forward, so this matches the authorization
          bar on ``change-tier`` (the other graph-wide settings op).
      db: Platform-DB session.

  Returns:
      Summary dict with ``graph_id``, ``previous_reporting_style_id``,
      ``reporting_style_id``, and ``changed`` (False when same target).

  Raises:
      HTTPException 403: caller is not an org member, or not an owner.
      HTTPException 404: graph not found.
      HTTPException 422: target Style not found in tenant, has wrong
          structure_type, or has an incomplete composition.
  """
  from robosystems.db.extensions import extensions_session
  from robosystems.models.core import OrgRole, OrgUser

  # Authorization: org owners only. Reporting Style is a graph-wide
  # setting that affects every render the org sees; matches the
  # change-tier auth bar (operations/graph/commands/tier.py).
  user_orgs = OrgUser.get_user_orgs(current_user.id, db)
  if not user_orgs:
    raise HTTPException(
      status_code=403, detail="You are not a member of any organization"
    )
  if user_orgs[0].role != OrgRole.OWNER:
    raise HTTPException(
      status_code=403,
      detail="Only organization owners can change the Reporting Style",
    )

  graph = Graph.get_by_id(graph_id, db)
  if not graph:
    raise HTTPException(status_code=404, detail=f"Graph {graph_id!r} not found")

  previous_style_id = str(graph.reporting_style_id)

  # Same-target is an idempotent no-op so a retry on transient network
  # error doesn't churn the platform row or surface a spurious change
  # event downstream.
  if previous_style_id == new_reporting_style_id:
    return {
      "graph_id": graph_id,
      "previous_reporting_style_id": previous_style_id,
      "reporting_style_id": new_reporting_style_id,
      "changed": False,
    }

  # Validate the target Style in the tenant's extensions schema.
  # Structure id is library-deterministic for the 3 seeded Styles and
  # customer-deterministic for taxonomy-block-authored ones, so the
  # tenant lookup is the only place that can confirm the row exists
  # and is renderable here.
  with extensions_session(graph_id) as ext:
    row = ext.execute(
      text(
        """
        SELECT id, name, structure_type, is_active
        FROM structures
        WHERE id = :sid
        """
      ),
      {"sid": new_reporting_style_id},
    ).fetchone()
    if row is None:
      raise HTTPException(
        status_code=422,
        detail=(
          f"Reporting Style {new_reporting_style_id!r} not found in tenant "
          f"schema for graph {graph_id!r}."
        ),
      )
    # Accept legacy 'custom' rows so existing tenants whose Style rows
    # weren't promoted by migration 0008 (immutability trigger leaves
    # tenant copies alone) can still be selected. The picker doesn't
    # filter on structure_type either.
    if row.structure_type not in ("reporting_style", "custom"):
      raise HTTPException(
        status_code=422,
        detail=(
          f"Structure {new_reporting_style_id!r} has structure_type="
          f"{row.structure_type!r}; expected 'reporting_style' "
          f"(or 'custom' for legacy tenants whose Style rows weren't "
          f"promoted by migration 0008)."
        ),
      )
    if not row.is_active:
      raise HTTPException(
        status_code=422,
        detail=f"Reporting Style {new_reporting_style_id!r} is inactive.",
      )

    composed = {
      r.statement_type
      for r in ext.execute(
        text(
          """
          SELECT statement_type FROM reporting_style_networks
          WHERE reporting_style_id = :sid
          """
        ),
        {"sid": new_reporting_style_id},
      ).fetchall()
    }
    missing = [st for st in _REQUIRED_STATEMENT_TYPES if st not in composed]
    if missing:
      raise HTTPException(
        status_code=422,
        detail=(
          f"Reporting Style {new_reporting_style_id!r} has an incomplete "
          f"composition — missing Network(s) for: {', '.join(missing)}. "
          f"Author the missing reporting_style_networks rows before "
          f"switching."
        ),
      )

  # All validation passed — flip the platform-DB column.
  graph.reporting_style_id = new_reporting_style_id
  db.commit()
  db.refresh(graph)

  return {
    "graph_id": graph_id,
    "previous_reporting_style_id": previous_style_id,
    "reporting_style_id": new_reporting_style_id,
    "changed": True,
  }
