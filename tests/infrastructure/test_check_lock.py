"""uv.lock downgrades fail unless deliberate, and spent exemptions are reported."""

from __future__ import annotations

import importlib.util
import sys
import tomllib
from datetime import UTC, datetime
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

SCRIPT = Path(__file__).resolve().parents[2] / "bin" / "tools" / "check_lock.py"
_spec = importlib.util.spec_from_file_location("check_lock", SCRIPT)
assert _spec and _spec.loader
guard = importlib.util.module_from_spec(_spec)
sys.modules["check_lock"] = guard
_spec.loader.exec_module(guard)

NOW = datetime(2026, 11, 10, tzinfo=UTC)


def _lock(*packages, constraints=(), overrides=(), exempt=(), span="P7D"):
  root = {
    "name": "robosystems",
    "metadata": {"requires-dist": [dict(c) for c in constraints]},
  }
  return {
    "options": {
      "exclude-newer-span": span,
      "exclude-newer-package": dict.fromkeys(exempt, False),
    },
    "manifest": {"overrides": [dict(o) for o in overrides]},
    "package": [root, *packages],
  }


def _package(name, version, uploaded="2026-10-01T00:00:00Z"):
  return {
    "name": name,
    "version": version,
    "wheels": [{"url": "u", "upload-time": uploaded}],
  }


def _names(downgrades):
  return [name for name, _, _ in downgrades]


def test_a_version_that_falls_is_a_downgrade():
  base = _lock(_package("pypdf", "6.21.4"))
  head = _lock(_package("pypdf", "6.21.3"))

  assert _names(guard.find_downgrades(base, head)) == ["pypdf"]


def test_versions_compare_numerically_not_as_text():
  base = _lock(_package("pkg", "1.9.0"))
  head = _lock(_package("pkg", "1.10.0"))

  assert guard.find_downgrades(base, head) == []


def test_a_changed_constraint_makes_a_downgrade_deliberate():
  base = _lock(
    _package("moto", "5.2.5"), constraints=[{"name": "moto", "specifier": ">=5"}]
  )
  head = _lock(
    _package("moto", "5.2.3"), constraints=[{"name": "moto", "specifier": "<5.2.4"}]
  )

  assert guard.find_downgrades(base, head) == []


def test_a_changed_override_makes_a_transitive_downgrade_deliberate():
  base = _lock(_package("pillow", "13.1.0"))
  head = _lock(
    _package("pillow", "13.0.0"),
    overrides=[{"name": "pillow", "specifier": "==13.0.0"}],
  )

  assert guard.find_downgrades(base, head) == []


def test_an_unrelated_constraint_change_does_not_excuse_a_downgrade():
  base = _lock(_package("pypdf", "6.21.4"))
  head = _lock(
    _package("pypdf", "6.21.3"), constraints=[{"name": "duckdb", "specifier": "==1"}]
  )

  assert _names(guard.find_downgrades(base, head)) == ["pypdf"]


def test_added_removed_and_raised_packages_pass():
  base = _lock(_package("gone", "1.0"), _package("raised", "1.0"))
  head = _lock(_package("new", "1.0"), _package("raised", "2.0"))

  assert guard.find_downgrades(base, head) == []


def test_forks_compare_on_the_highest_locked_version():
  base = _lock(_package("pkg", "1.0"), _package("pkg", "2.0"))
  head = _lock(_package("pkg", "2.0"))

  assert guard.find_downgrades(base, head) == []


def test_an_exemption_past_the_cooldown_is_spent():
  lock = _lock(_package("pypdf", "6.21.4"), exempt=["pypdf"])

  assert guard.spent_exemptions(lock, NOW) == ["pypdf"]


def test_an_exemption_inside_the_cooldown_is_still_needed():
  lock = _lock(
    _package("pypdf", "6.21.4", uploaded="2026-11-05T00:00:00Z"), exempt=["pypdf"]
  )

  assert guard.spent_exemptions(lock, NOW) == []


def test_our_own_packages_are_exempt_for_good():
  lock = _lock(_package("ladybug", "0.22.1"), exempt=["ladybug"])

  assert guard.spent_exemptions(lock, NOW) == []


def test_the_repository_lock_parses():
  lock = tomllib.loads((SCRIPT.parents[2] / "uv.lock").read_text())

  assert guard.find_downgrades(lock, lock) == []
  assert guard.declared_constraints(lock)["duckdb"]
  assert guard.locked_versions(lock)["duckdb"]
