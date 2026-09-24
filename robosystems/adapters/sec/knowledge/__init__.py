"""Graph analytics over SEC staging data: statement classification and the
knowledge artifacts used for confidence refinement.

Re-exports are lazy because importing networkit (``artifact``, ``graphs``)
registers the ``file`` URI scheme in Arrow's registry, after which every
pyarrow local-file operation by path raises ``ArrowKeyError``. Pass pyarrow
file handles, not paths, anywhere networkit may be loaded.
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
  if name in _LAZY_IMPORTS:
    import importlib

    module = importlib.import_module(_LAZY_IMPORTS[name])
    return getattr(module, name)
  raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
