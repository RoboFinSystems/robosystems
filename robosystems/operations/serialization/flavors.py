"""Encoder-family flavor enums; API models import them from here."""

from __future__ import annotations

from enum import StrEnum


class RdfFlavor(StrEnum):
  """RDF-family serialization flavor."""

  # Dataset-form JSON-LD: the report holon (``rdf/holon.py``); stamped at publish.
  HOLON_JSONLD = "holon-jsonld"
  # The same holon with its fourth named graph, ``#lineage`` — the ledger
  # behind the frozen facts (``rdf/lineage.py``); derived on first download,
  # for the graph's own members.
  HOLON_JSONLD_LINEAGE = "holon-jsonld-lineage"


class XbrlFlavor(StrEnum):
  """XBRL-family serialization flavor."""

  XBRL_2_1 = "xbrl-2.1"
  # Project Tavi compiled model (``xbrl/tavi.py``).
  TAVI = "tavi"
