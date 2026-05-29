"""XBRL-family encoder dispatch.

Phase 1 ships XBRL 2.1; OIM flavors (xBRL-CSV, xBRL-JSON) slot in by
adding a module and an ``elif`` arm here. The public
``serialize_to_xbrl`` entry point keeps a stable signature regardless
of which flavor is requested.
"""

from __future__ import annotations

from robosystems.operations.serialization.bundle import StatementBundle
from robosystems.operations.serialization.flavors import XbrlFlavor


def serialize_to_xbrl(
  bundle: StatementBundle,
  flavor: XbrlFlavor = XbrlFlavor.XBRL_2_1,
) -> bytes:
  """Serialize a ``StatementBundle`` to an XBRL-family format.

  Returns bytes — XBRL 2.1 emits a ``report.zip`` (Phase 2 packaging)
  or a single ``instance.xml`` zipped for download (Phase 1b interim).
  OIM flavors return their canonical content (xBRL-JSON bytes,
  xBRL-CSV multi-file zip) when added later.
  """
  if flavor is XbrlFlavor.XBRL_2_1:
    from robosystems.operations.serialization.xbrl.xbrl_21 import (
      serialize_to_xbrl_21,
    )

    return serialize_to_xbrl_21(bundle)
  raise ValueError(f"Unsupported XBRL flavor: {flavor!r}")
