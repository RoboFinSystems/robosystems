"""RDF-family encoder dispatch."""

from __future__ import annotations

from robosystems.operations.serialization.bundle import StatementBundle
from robosystems.operations.serialization.flavors import RdfFlavor


def serialize_to_rdf(
  bundle: StatementBundle,
  flavor: RdfFlavor = RdfFlavor.HOLON_JSONLD,
) -> str:
  if flavor is RdfFlavor.HOLON_JSONLD:
    from robosystems.operations.serialization.rdf.holon import (
      serialize_to_holon_jsonld,
    )

    return serialize_to_holon_jsonld(bundle)
  if flavor is RdfFlavor.HOLON_JSONLD_LINEAGE:
    raise ValueError(
      "The lineage holon reads the ledger; use "
      "serialize_to_lineage_holon_jsonld(session, bundle)."
    )
  raise ValueError(f"Unsupported RDF flavor: {flavor!r}")
