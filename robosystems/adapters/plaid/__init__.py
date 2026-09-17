"""Plaid adapter — the aggregator bank feed.

The second feed on the bank-feed contract (``adapters/bank_feed/``): one
connection per Plaid Item (one institution login for one customer),
connected through Plaid Link inside the app rather than a redirect. Like
every bank feed it authors no chart and no GL rows; it captures posted
activity into the inbox with a Tier-0 suggestion from Plaid's
personal-finance category.

- ``client/api.py`` — ``PlaidClient``: Link tokens, the public-token
  exchange, accounts, the ``/transactions/sync`` cursor, Item removal.
- ``pipeline/`` — ``tier0.py`` (category → hint), ``transform.py``
  (transactions → captured events, transfer legs paired), ``load.py``
  (added / modified / removed against what the inbox already holds),
  ``assets.py`` (the one Dagster asset ``plaid_feed``, job ``plaid_sync``).

Design: ``local/RoboSystems/specs/adapters/plaid-bank-feed.md``.
"""

from robosystems.adapters.plaid.client import PlaidClient

__all__ = ["PlaidClient"]
