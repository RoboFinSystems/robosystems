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
def test_rate_limits_do_not_reappear_in_constants():
  """``constants.py`` is not a second home for rate limits.

  It held five of them until the pair split became visible at the call site: a
  login attempt *count* in one module and its *window* in the other, changed
  independently. Two more windows sat there dead, duplicating values that were
  already here. One home per concept is what keeps a count and its window in
  step, so this fails if a rate limit is added back to the other one.
  """
  source = (
    Path(__file__).resolve().parents[2] / "robosystems/config/constants.py"
  ).read_text(encoding="utf-8")
  offenders = [
    node.target.id
    for node in ast.parse(source).body
    if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name)
    if "RATE_LIMIT" in node.target.id
  ] + [
    target.id
    for node in ast.parse(source).body
    if isinstance(node, ast.Assign)
    for target in node.targets
    if isinstance(target, ast.Name) and "RATE_LIMIT" in target.id
  ]
  assert not offenders, (
    f"config/constants.py defines rate limits {sorted(offenders)}. They belong "
    "in BURST_LIMITS, next to the windows they pair with."
  )


@pytest.mark.unit
def test_limits_are_positive_integers():
  bad = {k: v for k, v in BURST_LIMITS.items() if not isinstance(v, int) or v <= 0}
  assert not bad, f"non-positive or non-integer burst limits: {bad}"


# The full table, pinned. Every other test here compares key *sets*, which
# cannot see a wrong number — a transposed pair (`login_attempts: 300,
# login_window: 5`) satisfies all of them. Rate limits are security controls
# whose values no functional test exercises, so the only thing that makes a
# change to one deliberate is having to edit it here too.
EXPECTED_BURST_LIMITS = {
  "analytics": 100,
  "anonymous": 10,
  "api_key": 1000,
  "auth_attempts": 10,
  "auth_status": 600,
  "auth_window": 300,
  "backup_ops": 10,
  "billing": 60,
  "connection_mgmt": 30,
  "general_api": 200,
  "jwt": 500,
  "jwt_refresh": 20,
  "login_attempts": 5,
  "login_window": 300,
  "logout": 300,
  "mfa": 120,
  "oauth_authorize": 120,
  "oauth_consent": 120,
  "oauth_register": 50,
  "oauth_token": 300,
  "oidc": 120,
  "passkey_management": 60,
  "public_api": 600,
  "register_attempts": 3,
  "register_window": 3600,
  "scim": 120,
  "scim_ip": 300,
  "sensitive_auth": 60,
  "sse_connections": 10,
  "sse_connections_window": 60,
  "sso": 100,
  "sync_ops": 50,
  "tasks": 200,
  "user_management": 600,
  "webhook": 1200,
}


@pytest.mark.unit
def test_the_values_are_what_we_think_they_are():
  assert BURST_LIMITS == EXPECTED_BURST_LIMITS, (
    "a burst limit changed. If that was deliberate, update "
    "EXPECTED_BURST_LIMITS in the same commit and say why in the message; "
    "if it was not, this is a security control that moved by accident."
  )


@pytest.mark.unit
def test_each_attempt_count_is_smaller_than_its_window():
  """Catches a transposed pair even if both numbers are updated here.

  Every count/window pair is an attempt budget over a span of seconds, and in
  every real pairing the budget is the smaller number. Swapping them turns a
  5-attempts-per-300s login throttle into 300-attempts-per-5s.
  """
  for name in ("login", "register", "auth"):
    count = BURST_LIMITS[f"{name}_attempts"]
    window = BURST_LIMITS[f"{name}_window"]
    assert count < window, (
      f"{name}: attempts={count} is not smaller than window={window}s — "
      "the pair looks transposed"
    )
