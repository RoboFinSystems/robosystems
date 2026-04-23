"""Demo-only reset logic for close_demo.

This is NOT a production operation. It selectively wipes demo-generated
state while preserving graph infrastructure (entity, library-seeded
taxonomies + elements + structures + associations).

For production use cases:
- Entries are corrected via reverse-journal-entry, not deleted
- Schedules are truncated or individually deleted
- Fiscal calendar state is managed via reopen-period

Library-origin rows (``created_by = 'library-seeder'``) are protected by
per-tenant PL/pgSQL triggers installed at provision time. This reset
script uses the same audit literal to determine what it can delete:
everything that is NOT library-seeded belongs to the demo run and is
fair game.
"""

from __future__ import annotations

from sqlalchemy import text

from robosystems.models.extensions import EntityTaxonomy
from robosystems.models.extensions.roboledger.fiscal_calendar import (
  FiscalCalendar,
  FiscalCalendarEvent,
)
from robosystems.models.extensions.roboledger.fiscal_period import FiscalPeriod

_LIBRARY_SEEDER = "library-seeder"


def reset_demo_state(graph_id: str) -> None:
  """Wipe demo-generated state, preserving entity + library data.

  After this, the graph is clean with only:
  - The entity row (graph identity)
  - The full library (taxonomies, elements, labels, references,
    structures, associations — everything with
    ``created_by='library-seeder'``)

  All demo-created data (CoA taxonomy, CoA elements, entries,
  transactions, tenant associations, facts, schedules, fiscal calendar)
  is removed.
  """
  from robosystems.db.extensions import extensions_session

  with extensions_session(graph_id) as session:
    # 1-3. All entries + line items + transactions (tenant-generated;
    # the library itself never authors these).
    session.execute(text("DELETE FROM line_items"))
    session.execute(text("DELETE FROM entries"))
    session.execute(text("DELETE FROM transactions"))

    # 4. Tenant-origin associations. The library's immutability triggers
    # raise on any UPDATE/DELETE of rows with created_by='library-seeder',
    # so we filter those out explicitly. CoA mapping arcs created by the
    # demo (created_by='coa-classifier' or the demo user) are fair game.
    session.execute(
      text("DELETE FROM associations WHERE created_by != :seeder"),
      {"seeder": _LIBRARY_SEEDER},
    )

    # 5. Facts
    session.execute(text("DELETE FROM facts"))

    # 6. Tenant-origin element classifications (child of elements; must
    # be deleted before elements due to FK constraint).
    session.execute(
      text(
        "DELETE FROM element_classifications WHERE element_id IN "
        "(SELECT id FROM elements WHERE created_by != :seeder)"
      ),
      {"seeder": _LIBRARY_SEEDER},
    )

    # 6b. Tenant-origin elements. Library elements (fac, rs-gaap,
    # type-subtype, plus the native mapping/presentation overlays all
    # carrying 'library-seeder') stay put.
    session.execute(
      text("DELETE FROM elements WHERE created_by != :seeder"),
      {"seeder": _LIBRARY_SEEDER},
    )

    # 6c. Verification results, rules, and fact_sets all FK structures —
    # delete them before structures to avoid constraint violations.
    session.execute(text("DELETE FROM verification_results"))
    session.execute(
      text("DELETE FROM rules WHERE target_structure_id IN "
           "(SELECT id FROM structures WHERE created_by != :seeder)"),
      {"seeder": _LIBRARY_SEEDER},
    )
    session.execute(
      text("DELETE FROM fact_sets WHERE structure_id IN "
           "(SELECT id FROM structures WHERE created_by != :seeder)"),
      {"seeder": _LIBRARY_SEEDER},
    )

    # 7. Tenant-origin structures (any the demo created — anchor
    # structure, schedules, report layouts).
    session.execute(
      text("DELETE FROM structures WHERE created_by != :seeder"),
      {"seeder": _LIBRARY_SEEDER},
    )

    # 8. Entity taxonomy linkages (will be re-created by demo setup).
    session.query(EntityTaxonomy).delete(synchronize_session=False)

    # 9. Tenant-origin taxonomies (the demo's CoA taxonomy).
    session.execute(
      text("DELETE FROM taxonomies WHERE created_by != :seeder"),
      {"seeder": _LIBRARY_SEEDER},
    )

    # 10. Fiscal calendar state
    session.query(FiscalCalendarEvent).delete(synchronize_session=False)
    session.query(FiscalCalendar).delete(synchronize_session=False)
    session.query(FiscalPeriod).delete(synchronize_session=False)

    session.flush()
