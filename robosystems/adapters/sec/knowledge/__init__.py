"""SEC graph knowledge for XBRL financial data.

Provides statement classification using graph analytics algorithms
from icebug (networkit), a DuckDB analytics framework for running
queries on SEC staging data, and knowledge artifact builders for
graph-based confidence refinement.

**Re-exports are lazy.** `artifact` and `graphs` import `networkit`, and
importing networkit registers the ``file`` URI scheme in the Arrow registry
that `pyarrow` also registers — after which *every* pyarrow local-file
operation in that process raises ``ArrowKeyError: Attempted to register
factory for scheme 'file'``, including ``pq.write_table`` and
``pq.read_metadata``. Eager re-exports here meant that importing any module
in this package — `framework`, which only needs duckdb, or `classifiers`,
for a constant dict — pulled networkit in and poisoned pyarrow for the rest
of the process. Resolving names on demand keeps networkit confined to the
callers that actually run graph analytics.
"""

_LAZY_IMPORTS = {
  "ArcExtractor": "robosystems.adapters.sec.knowledge.extractors",
  "DisclosureProfileBuilder": "robosystems.adapters.sec.knowledge.artifact",
  "DuckDBAnalyticsContext": "robosystems.adapters.sec.knowledge.framework",
  "ElementKnowledgeBuilder": "robosystems.adapters.sec.knowledge.artifact",
  "StatementClassifier": "robosystems.adapters.sec.knowledge.classifiers",
  "StructureKnowledgeBuilder": "robosystems.adapters.sec.knowledge.artifact",
}


def __getattr__(name: str):
  """Lazy import knowledge classes on first access."""
  if name in _LAZY_IMPORTS:
    import importlib

    module = importlib.import_module(_LAZY_IMPORTS[name])
    return getattr(module, name)
  raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
