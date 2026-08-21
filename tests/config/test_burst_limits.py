"""The burst-limit table and its call sites must name the same things.

These are constants, not tunables, so the risk is not that someone sets one and
it does not take — it is that the table and the code that reads it drift apart.
A key read but not defined is a ``KeyError`` on a rate-limited path; a key
defined but never read is a number that looks authoritative and governs nothing.
Both directions are asserted.

The predecessor of this table resolved through ``getattr(env, name, default)``,
which answered with the default for any name — so a value that governed nothing
was indistinguishable from one that did. Reading the table by key means a wrong
name fails loudly instead.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from robosystems.config.rate_limits import BURST_LIMITS

RATE_LIMITING = (
  Path(__file__).resolve().parents[2]
  / "robosystems/middleware/rate_limits/rate_limiting.py"
)


def _keys_read() -> set[str]:
  """Every ``BURST_LIMITS["..."]`` subscript in the rate-limiting middleware."""
  tree = ast.parse(RATE_LIMITING.read_text(encoding="utf-8"))
  keys: set[str] = set()
  for node in ast.walk(tree):
    if (
      isinstance(node, ast.Subscript)
      and isinstance(node.value, ast.Name)
      and node.value.id == "BURST_LIMITS"
      and isinstance(node.slice, ast.Constant)
      and isinstance(node.slice.value, str)
    ):
      keys.add(node.slice.value)
  return keys


@pytest.mark.unit
def test_the_call_sites_are_discoverable():
  """A parse failure would make both assertions below vacuously pass."""
  assert len(_keys_read()) >= 20


@pytest.mark.unit
def test_every_key_read_is_defined():
  missing = sorted(_keys_read() - set(BURST_LIMITS))
  assert not missing, (
    f"rate_limiting.py reads BURST_LIMITS keys that are not defined: {missing}"
  )


@pytest.mark.unit
def test_every_key_defined_is_read():
  unused = sorted(set(BURST_LIMITS) - _keys_read())
  assert not unused, (
    f"BURST_LIMITS defines keys nothing reads: {unused}. Remove them, or find "
    "the call site that was supposed to use them."
  )


@pytest.mark.unit
def test_limits_are_positive_integers():
  bad = {k: v for k, v in BURST_LIMITS.items() if not isinstance(v, int) or v <= 0}
  assert not bad, f"non-positive or non-integer burst limits: {bad}"
