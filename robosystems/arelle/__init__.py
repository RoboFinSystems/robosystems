"""Platform-scoped Arelle integration.

This module provides a neutral interface for ingesting XBRL taxonomies
and serializing them to JSON-LD via rdflib. Callers include the library
seed pipeline (Charlie Hoffman's FAC + SFAC 6 artifacts) and — in the
future — the SEC filing processor.

The underlying Arelle client still lives at
`robosystems.adapters.sec.client.arelle` during the POC phase; full
promotion of the client to this module is Phase 1 work.

Entry points:

- `extract_taxonomy(model_xbrl) → rdflib.Graph` — walk Arelle's model
  and produce an RDF graph that captures concepts, labels, references,
  arcs, and extended link roles.
- `serialize_jsonld(graph, standard, version) → str` — render the graph
  as canonical JSON-LD using the RoboSystems `@context`.
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
