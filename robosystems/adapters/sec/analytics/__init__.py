"""SEC graph analytics for XBRL financial data.

Provides statement classification and element normalization using
graph analytics algorithms from icebug (networkit).
"""

from robosystems.adapters.sec.analytics.classifiers import StatementClassifier
from robosystems.adapters.sec.analytics.extractors import ArcExtractor
from robosystems.adapters.sec.analytics.normalizers import ElementNormalizer

__all__ = [
  "ArcExtractor",
  "ElementNormalizer",
  "StatementClassifier",
]
