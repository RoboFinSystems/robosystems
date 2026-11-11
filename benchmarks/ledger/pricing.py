"""Dollars from token usage, costed per turn so a long prompt takes its own rate."""

from __future__ import annotations

import json
from functools import cache
from pathlib import Path
from typing import Any

_PRICES = Path(__file__).with_name("prices.json")


@cache
def _table() -> dict[str, dict[str, Any]]:
  return {k: v for k, v in json.loads(_PRICES.read_text()).items() if k[0] != "_"}


def turn_cost(model: str, usage: dict[str, Any]) -> float | None:
  row = _table().get(model)
  if row is None:
    return None
  prompt = (
    usage["input_tokens"] + usage["cache_read_tokens"] + usage["cache_write_tokens"]
  )
  threshold = row.get("long_threshold")
  prefix = "long_" if threshold is not None and prompt > threshold else ""
  per_million = (
    usage["input_tokens"] * row[f"{prefix}input"]
    + usage["output_tokens"] * row[f"{prefix}output"]
    + usage["cache_write_tokens"] * row[f"{prefix}cache_write"]
    + usage["cache_read_tokens"] * row[f"{prefix}cache_read"]
  )
  return per_million / 1_000_000


def trial_cost(model: str, turn_usage: list[dict[str, Any]]) -> float | None:
  costs = [turn_cost(model, u) for u in turn_usage]
  if any(c is None for c in costs):
    return None
  return round(sum(c for c in costs if c is not None), 6)
