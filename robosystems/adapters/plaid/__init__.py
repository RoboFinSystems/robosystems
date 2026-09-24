"""Plaid adapter — the aggregator bank feed on the ``bank_feed`` contract.

One connection per Plaid Item (one institution login), connected through
Plaid Link in the app. It captures posted activity into the inbox with a
Tier-0 suggestion from Plaid's personal-finance category; it authors no GL rows.
"""

from robosystems.adapters.plaid.client import PlaidClient

__all__ = ["PlaidClient"]
