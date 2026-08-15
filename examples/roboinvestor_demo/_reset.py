"""Demo-only reset logic for roboinvestor_demo.

Not a production operation, and the only place in this demo that touches
PostgreSQL directly instead of going through the HTTP API. Wiping a tenant
back to a clean slate is a demo convenience, not something a customer
should be able to do, so it deliberately has no API surface.

The reset spans **two** tenant schemas, because the demo's subject spans
two graphs:

- the investor's schema, where portfolios / securities / positions live
  along with anything the handshake delivered (the shared report copy, its
  facts, and the linked ``Entity``);
- the issuer's schema, where the publish list and the outbound share record
  live.

Real withdrawals never take this path — a sender revokes with
``revoke-report-share``, which deletes the recipient's copy and stamps the
share record. The demo exercises that operation too; this module exists
only so a re-run starts from a known-empty state rather than accumulating
duplicate portfolios.
"""

from __future__ import annotations

from sqlalchemy import text


def reset_investor_state(graph_id: str) -> None:
  """Wipe the investor graph's demo state, preserving the fund entity.

  Removes, in FK order: positions, portfolios, securities, every report
  that arrived from another graph (with its fact sets and facts), and the
  linked entities the handshake created. The graph's own parent entity and
  its library-seeded taxonomy survive, so the graph never needs
  re-provisioning between runs.
  """
  from examples._common.local_db import assert_local_extensions_db
  from robosystems.db.extensions import extensions_session

  assert_local_extensions_db()

  with extensions_session(graph_id) as session:
    # Positions FK both portfolios and securities, so they go first.
    session.execute(text("DELETE FROM positions"))
    session.execute(text("DELETE FROM portfolios"))
    session.execute(text("DELETE FROM securities"))

    # Shared-in reports and their facts. `source_graph_id IS NOT NULL` is
    # exactly the set that arrived from elsewhere — a report the investor
    # authored has a null source graph. Facts hang off fact_sets, which
    # hang off the report.
    session.execute(
      text("""
        DELETE FROM facts WHERE fact_set_id IN (
          SELECT fs.id FROM fact_sets fs
          JOIN reports r ON r.id = fs.report_id
          WHERE r.source_graph_id IS NOT NULL
        )
      """)
    )
    session.execute(
      text("""
        DELETE FROM fact_sets WHERE report_id IN (
          SELECT id FROM reports WHERE source_graph_id IS NOT NULL
        )
      """)
    )
    session.execute(text("DELETE FROM reports WHERE source_graph_id IS NOT NULL"))

    # The concepts the share carried in. These have to go for the re-run to
    # exercise the copy at all — left in place, `_ensure_shared_elements`
    # finds them present and returns, and the run silently stops testing
    # the path it exists to test. Elements before taxonomies (FK), and both
    # only after the facts citing them are gone (above).
    session.execute(text("DELETE FROM elements WHERE source = 'linked'"))
    session.execute(
      text("DELETE FROM taxonomies WHERE metadata->>'source_graph_id' IS NOT NULL")
    )

    # Linked entities are the handshake's other product. Deleting them is
    # safe only because the securities that pointed at them are already
    # gone (above). The parent entity has source != 'linked' and stays.
    session.execute(text("DELETE FROM entities WHERE source = 'linked'"))

    # A blocked sender would make the share fail with a per-target error
    # rather than a hard failure, which reads as a mysterious demo bug. A
    # previous run's block-source-graph experiment must not leak forward.
    session.execute(text("DELETE FROM blocked_source_graphs"))

    session.flush()


def reset_issuer_share_state(graph_id: str) -> None:
  """Drop the issuer graph's publish lists and outbound share records.

  Scoped to the sender's own bookkeeping — the recipient's copy is removed
  by :func:`reset_investor_state`. Deleting every list rather than only
  the demo's is deliberate: the scenario demos that build this graph never
  create publish lists, so anything here came from a prior run of *this*
  demo.
  """
  from examples._common.local_db import assert_local_extensions_db
  from robosystems.db.extensions import extensions_session

  assert_local_extensions_db()

  with extensions_session(graph_id) as session:
    session.execute(text("DELETE FROM report_shares"))
    session.execute(text("DELETE FROM publish_list_members"))
    session.execute(text("DELETE FROM publish_lists"))
    session.flush()
