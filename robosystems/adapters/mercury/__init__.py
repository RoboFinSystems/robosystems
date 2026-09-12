"""Mercury adapter — the bank as a first-class source.

A bank feed is native accounting: it captures every posted bank transaction
into the inbox of books the tenant keeps natively, against a chart of
accounts that already exists. It authors no chart, no elements and no GL
rows; posting is a classification a person (or Claude over MCP) makes once.

- ``client/api.py`` — ``MercuryClient`` over a ``TokenSource`` (the partner
  OAuth client's rotating tokens, or a personal read-only key on self-hosted
  deployments).
- ``pipeline/`` — ``transform.py`` (transactions → captured events with a
  Tier-0 suggestion), ``accounts.py`` (link or create one chart account per
  bank account), ``load.py`` (through the event-block kernel), ``assets.py``
  (the one Dagster asset ``mercury_feed``, job ``mercury_sync``).

Design: ``local/RoboSystems/specs/adapters/mercury-adapter.md`` §3;
doctrine in ``ref/adapters.md`` §2.9.
"""

from robosystems.adapters.mercury.client import MercuryClient

__all__ = ["MercuryClient"]
