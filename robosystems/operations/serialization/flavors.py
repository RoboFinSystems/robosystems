"""Encoder-family flavor enums.

Each format family has a default flavor + room to grow.
``RdfFlavor.JSONLD`` / ``HOLON_JSONLD`` and ``XbrlFlavor.XBRL_2_1`` /
``TAVI`` are implemented; additional flavors slot in as enum values
without changing the public API.

Placement note: enums live here in the operations layer (single source
of truth). API-models that need a flavor parameter (e.g. download
endpoint query strings) import directly from here — no duplicate
definition in ``models/api/``.
"""

from __future__ import annotations

from enum import StrEnum


class RdfFlavor(StrEnum):
  """RDF-family serialization flavor."""

  JSONLD = "jsonld"
  # Dataset-form JSON-LD carrying the scene/boundary/projection named graphs
  # (the report holon) — API-native, materialized on demand off the same
  # bundle. See ``rdf/holon.py``.
  HOLON_JSONLD = "holon-jsonld"
  # Future flavors:
  # TURTLE = "turtle"
  # NQUADS = "nquads"
  # RDFXML = "rdfxml"


class XbrlFlavor(StrEnum):
  """XBRL-family serialization flavor."""

  XBRL_2_1 = "xbrl-2.1"
  # The Project Tavi compiled model (OIM family) — the wire value, the SEC
  # catalog's ``kind`` and the file name ``g{n}.tavi.json`` all say ``tavi``.
  # Materialized on demand off the same bundle through xbrlkit's emitter.
  # See ``xbrl/tavi.py``.
  TAVI = "tavi"
  # Future OIM flavors:
  # XBRL_CSV = "xbrl-csv"
  # XBRL_JSON = "xbrl-json"
