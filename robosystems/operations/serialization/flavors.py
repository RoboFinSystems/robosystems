"""Encoder-family flavor enums.

Each format family has a default flavor + room to grow. Phase 1 ships
``RdfFlavor.JSONLD`` and ``XbrlFlavor.XBRL_2_1``; additional flavors
slot in as enum values without changing the public API.

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
  # Future flavors (per spec §7 Phase 4):
  # TURTLE = "turtle"
  # NQUADS = "nquads"
  # RDFXML = "rdfxml"


class XbrlFlavor(StrEnum):
  """XBRL-family serialization flavor."""

  XBRL_2_1 = "xbrl-2.1"
  # Future OIM flavors (per spec §7 Phase 4):
  # XBRL_CSV = "xbrl-csv"
  # XBRL_JSON = "xbrl-json"
