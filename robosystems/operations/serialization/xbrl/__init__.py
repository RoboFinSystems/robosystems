"""XBRL-family encoder dispatch."""

from __future__ import annotations

from robosystems.operations.serialization.bundle import StatementBundle
from robosystems.operations.serialization.flavors import XbrlFlavor


def serialize_to_xbrl(
  bundle: StatementBundle,
  flavor: XbrlFlavor = XbrlFlavor.XBRL_2_1,
) -> bytes:
  """XBRL 2.1 returns a zip of instance + schema + linkbases; Tavi returns JSON."""
  if flavor is XbrlFlavor.XBRL_2_1:
    from robosystems.operations.serialization.xbrl.xbrl_21 import (
      serialize_to_xbrl_21,
    )

    return serialize_to_xbrl_21(bundle)
  if flavor is XbrlFlavor.TAVI:
    from robosystems.operations.serialization.xbrl.tavi import serialize_to_tavi

    return serialize_to_tavi(bundle)
  raise ValueError(f"Unsupported XBRL flavor: {flavor!r}")
