"""RoboSystems Dagster orchestration.

This module provides Dagster-based data orchestration for:
- Billing and infrastructure scheduled tasks
- SEC EDGAR data pipeline
- User graph operations

The Dagster setup replaces background tasks for all orchestration needs, providing:
- Better observability through the Dagster UI
- Asset-based data lineage tracking
- Declarative scheduling and sensors
- Unified monitoring for all pipeline activity

Note: ``defs`` is loaded lazily to avoid a circular import
(dagster → definitions → sec.pipeline → backup → dagster.resources → dagster).
``__dir__`` exposes ``defs`` so Dagster's ``inspect.getmembers()`` autodiscovery
triggers ``__getattr__``, which loads and caches the value in the module dict.
"""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING

if TYPE_CHECKING:
  from dagster import Definitions

  defs: Definitions

__all__ = ["defs"]


def __dir__():
  return __all__ + [k for k in globals() if not k.startswith("_")]


def __getattr__(name: str):
  if name == "defs":
    from robosystems.dagster.definitions import defs

    # Cache in module dict so subsequent access is direct
    sys.modules[__name__].defs = defs
    return defs
  raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
