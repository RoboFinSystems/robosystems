"""Taxonomy-neutral XBRL taxonomy → JSON-LD pipeline for the library seeds:
`extract_taxonomy(model_xbrl)` builds an rdflib graph, `serialize_jsonld`
renders it with the canonical `@context`. The Arelle loader itself lives at
`robosystems.adapters.sec.client.arelle`.
"""

from __future__ import annotations

from robosystems.arelle.context import CANONICAL_CONTEXT, context_document
from robosystems.arelle.extractor import extract_taxonomy
from robosystems.arelle.serializer import serialize_jsonld

__all__ = [
  "CANONICAL_CONTEXT",
  "context_document",
  "extract_taxonomy",
  "serialize_jsonld",
]
