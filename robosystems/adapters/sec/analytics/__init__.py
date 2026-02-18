"""SEC graph analytics for XBRL financial data.

Provides statement classification and element normalization using
graph analytics algorithms from icebug (networkit), plus a DuckDB
analytics framework for running queries on SEC staging data.
"""

from robosystems.adapters.sec.analytics.classifiers import StatementClassifier
from robosystems.adapters.sec.analytics.extractors import ArcExtractor
from robosystems.adapters.sec.analytics.framework import DuckDBAnalyticsContext
from robosystems.adapters.sec.analytics.normalizers import ElementNormalizer

__all__ = [
  "ArcExtractor",
  "DuckDBAnalyticsContext",
  "ElementNormalizer",
  "StatementClassifier",
]
