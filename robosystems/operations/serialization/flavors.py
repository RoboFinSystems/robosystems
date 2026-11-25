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
  # The holon as a DataBook (``rdf/databook.py``): Markdown a person reads,
  # each value linked to its fact; derived from the stored holon on first
  # download.
  DATABOOK = "databook"
  # The holon as an Excel workbook (``rdf/workbook.py``): a sheet per section,
  # every number the reported fact; derived from the stored holon on first
  # download.
  WORKBOOK = "workbook"


class XbrlFlavor(StrEnum):
  """XBRL-family serialization flavor."""

  XBRL_2_1 = "xbrl-2.1"
  # Project Tavi compiled model (``xbrl/tavi.py``).
  TAVI = "tavi"
