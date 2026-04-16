"""Demo-only reset logic for close_demo.

This is NOT a production operation. It selectively wipes demo-generated
state while preserving graph infrastructure (entity, shared taxonomies,
US GAAP reporting elements).

For production use cases:
- Entries are corrected via reverse-journal-entry, not deleted
- Schedules are truncated or individually deleted
- Fiscal calendar state is managed via reopen-period
"""

from __future__ import annotations

from sqlalchemy import select, text

from robosystems.models.extensions import Element, EntityTaxonomy
from robosystems.models.extensions.roboledger import (
  Association,
  Fact,
  Structure,
  Taxonomy,
)
from robosystems.models.extensions.roboledger.fiscal_calendar import (
  FiscalCalendar,
  FiscalCalendarEvent,
)
from robosystems.models.extensions.roboledger.fiscal_period import FiscalPeriod

# The shared reporting taxonomy ID — must survive reset
_REPORTING_TAX_ID = "tax_usgaap_reporting"


def reset_demo_state(graph_id: str) -> None:
  """Wipe demo-generated state, preserving entity + shared taxonomy data.

  After this, the graph is clean with only:
  - The entity row (graph identity)
  - The shared US GAAP reporting taxonomy + elements + structures

  All demo-created data (CoA taxonomy, elements, entries, transactions,
  associations, facts, schedules, fiscal calendar) is removed.
  """
  from robosystems.db.extensions import extensions_session

  with extensions_session(graph_id) as session:
    # 1-3. All entries + line items + transactions
    session.execute(text("DELETE FROM line_items"))
    session.execute(text("DELETE FROM entries"))
    session.execute(text("DELETE FROM transactions"))

    # 4. Associations (only those referencing demo-source elements)
    demo_elem_ids = select(Element.id).where(
      Element.source.notin_(["us-gaap", "sfac6", "system"])
    )
    session.query(Association).filter(
      Association.from_element_id.in_(demo_elem_ids)
    ).delete(synchronize_session=False)

    # 5. Facts
    session.query(Fact).delete(synchronize_session=False)

    # 6. Elements (preserve shared GAAP/SFAC/system elements)
    session.query(Element).filter(
      Element.source.notin_(["us-gaap", "sfac6", "system"])
    ).delete(synchronize_session=False)

    # 7. Structures in non-shared taxonomies
    non_shared_tax_ids = select(Taxonomy.id).where(
      Taxonomy.id != _REPORTING_TAX_ID
    )
    session.query(Structure).filter(
      Structure.taxonomy_id.in_(non_shared_tax_ids)
    ).delete(synchronize_session=False)

    # 8. Entity taxonomy linkages (will be re-created by demo setup)
    session.query(EntityTaxonomy).delete(synchronize_session=False)

    # 9. Non-reporting taxonomies
    session.query(Taxonomy).filter(
      Taxonomy.id != _REPORTING_TAX_ID,
    ).delete(synchronize_session=False)

    # 10. Fiscal calendar state
    session.query(FiscalCalendarEvent).delete(synchronize_session=False)
    session.query(FiscalCalendar).delete(synchronize_session=False)
    session.query(FiscalPeriod).delete(synchronize_session=False)

    session.flush()

  # Force re-seed the shared reporting taxonomy elements if they're
  # missing (may have been wiped by a prior broader reset).
  _ensure_reporting_taxonomy_seeded(graph_id)


def _ensure_reporting_taxonomy_seeded(graph_id: str) -> None:
  """Re-seed US GAAP reporting elements if they were wiped."""
  from robosystems.db.extensions import _get_engine, _sanitize_schema

  schema = _sanitize_schema(graph_id)
  engine = _get_engine()

  with engine.connect() as conn:
    # Check if reporting elements exist in the tenant schema
    result = conn.execute(
      text(f"SELECT COUNT(*) FROM {schema}.elements WHERE source = 'sfac6'"),
    )
    count = result.scalar()
    if count and count > 0:
      conn.commit()
      return

    # Re-seed from public schema
    from robosystems.config.taxonomy.seed import seed_tenant_reporting_taxonomy

    # First ensure the reporting taxonomy row exists
    result = conn.execute(
      text(f"SELECT id FROM {schema}.taxonomies WHERE id = :id"),
      {"id": _REPORTING_TAX_ID},
    )
    if not result.fetchone():
      # Copy from public
      conn.execute(
        text(f"""
          INSERT INTO {schema}.taxonomies
            SELECT * FROM public.taxonomies WHERE id = :id
        """),
        {"id": _REPORTING_TAX_ID},
      )

    seed_tenant_reporting_taxonomy(conn, schema)
    conn.commit()
