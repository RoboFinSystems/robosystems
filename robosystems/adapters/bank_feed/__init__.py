"""The bank-feed lane — what every bank feed shares, whatever the source.

A bank feed is native accounting (``ref/adapters.md`` §2.9): it captures
posted bank activity into the inbox of books the tenant keeps natively,
against a chart of accounts that already exists. It authors no chart, no
elements and no GL rows; posting is a classification made once.

Hoisted out of the Mercury adapter when the second feed (Plaid) arrived —
the second instance is what decides the seam:

- ``hints.py`` — ``AccountHint``, the canonical hint vocabulary, trait
  inference for an account known only by name.
- ``chart.py`` — pure: ``BankAccount`` and ``ChartIndex``, which resolves a
  hint against the tenant's chart by name then by code.
- ``accounts.py`` — link-or-create: one chart account per bank account the
  feed exposes, linked on ``Element.metadata.bank_feed``; and
  ``build_chart_index`` from the graph.
- ``load.py`` — counterparties and captured events through the event-block
  kernel, and the hint refresh on a still-captured event.
- ``sync.py`` — the sync-result discipline every feed's Dagster asset keeps:
  the failure record, the lock release, ``last_sync``, the fiscal-calendar
  bootstrap and the staleness mark.

Each source keeps its client, its category tables and its transform.
"""
