"""Encoder-family flavor enums; API models import them from here."""

from __future__ import annotations

from enum import StrEnum


class RdfFlavor(StrEnum):
  """RDF-family serialization flavor."""

  JSONLD = "jsonld"
  # Dataset-form JSON-LD: the report holon (``rdf/holon.py``).
  HOLON_JSONLD = "holon-jsonld"


class XbrlFlavor(StrEnum):
  """XBRL-family serialization flavor."""

  XBRL_2_1 = "xbrl-2.1"
  # Project Tavi compiled model (``xbrl/tavi.py``).
  TAVI = "tavi"
