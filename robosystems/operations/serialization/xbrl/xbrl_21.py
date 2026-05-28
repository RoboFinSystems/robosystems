"""XBRL 2.1 instance.xml emitter (Phase 1b).

Implementation lands in task 1b.2 — this module is a typed stub so the
``serialize_to_xbrl`` dispatch import resolves while the JSON-LD half
of the round-trip lands first.
"""

from __future__ import annotations

from robosystems.operations.serialization.bundle import StatementBundle


def serialize_to_xbrl_21(bundle: StatementBundle) -> bytes:
  del bundle  # placeholder — 1b.2 supplies the implementation
  raise NotImplementedError(
    "XBRL 2.1 emitter is wired in task 1b.2 — placeholder for 1a.2 scaffolding"
  )
