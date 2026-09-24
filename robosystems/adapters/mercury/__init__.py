"""Mercury adapter — a bank feed on the ``bank_feed`` contract.

Captures every posted bank transaction into the inbox, against the tenant's
existing chart. It authors no chart and no GL rows.
"""

from robosystems.adapters.mercury.client import MercuryClient

__all__ = ["MercuryClient"]
