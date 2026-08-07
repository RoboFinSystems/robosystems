"""RoboSystems Dagster orchestration.

Covers billing and infrastructure schedules, the SEC EDGAR data pipeline, and
user graph operations. See ``robosystems/dagster/README.md`` for the layout of
assets, jobs, schedules, sensors, and resources.

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
