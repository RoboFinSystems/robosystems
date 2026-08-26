"""The per-worker database isolation that makes ``pytest -n`` safe.

Two kinds of test: the pure rewrite logic, and — only when actually running
under xdist — the live assertion that this worker really is on its own
database, both in the environment the fixtures read and in the engine the
app bound at import.
"""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest

from tests.xdist_workers import (
  PER_WORKER_URL_KEYS,
  WORKER_ENV,
  database_name,
  isolate_worker_databases,
  per_worker_url,
)

BASE = "postgresql://postgres:postgres@localhost:5432/robosystems_test"


class TestPerWorkerUrl:
  def test_suffixes_the_database_name_only(self):
    assert per_worker_url(BASE, "gw3") == (
      "postgresql://postgres:postgres@localhost:5432/robosystems_test_gw3"
    )

  def test_keeps_credentials_host_and_query(self):
    url = "postgresql://u:p@db.internal:6543/robosystems_test?sslmode=require"
    out = per_worker_url(url, "gw0")
    assert out.startswith("postgresql://u:p@db.internal:6543/")
    assert database_name(out) == "robosystems_test_gw0"
    assert out.endswith("?sslmode=require")


class TestIsolateWorkerDatabases:
  def test_does_nothing_outside_xdist(self):
    env = dict.fromkeys(PER_WORKER_URL_KEYS, BASE)
    with patch("tests.xdist_workers.ensure_database") as ensure:
      assert isolate_worker_databases(env) is None
    ensure.assert_not_called()
    assert all(env[key] == BASE for key in PER_WORKER_URL_KEYS)

  def test_moves_both_platform_urls_and_creates_the_database_once(self):
    """`TEST_DATABASE_URL` (fixtures) and `DATABASE_URL` (the app engine) name
    the same database, so they must move together — a fixture inspecting a
    database the app is not writing to would pass every test vacuously."""
    env = {WORKER_ENV: "gw2", **dict.fromkeys(PER_WORKER_URL_KEYS, BASE)}
    with patch("tests.xdist_workers.ensure_database") as ensure:
      assert isolate_worker_databases(env) == "gw2"
    ensure.assert_called_once_with(per_worker_url(BASE, "gw2"))
    for key in PER_WORKER_URL_KEYS:
      assert database_name(env[key]) == "robosystems_test_gw2"

  def test_leaves_the_extensions_database_shared(self):
    env = {
      WORKER_ENV: "gw1",
      "TEST_DATABASE_URL": BASE,
      "EXTENSIONS_DATABASE_URL": "postgresql://postgres:postgres@localhost:5432/extensions",
    }
    with patch("tests.xdist_workers.ensure_database"):
      isolate_worker_databases(env)
    assert database_name(env["EXTENSIONS_DATABASE_URL"]) == "extensions"


@pytest.mark.skipif(
  not os.environ.get(WORKER_ENV), reason="only meaningful under pytest -n"
)
def test_this_worker_runs_on_its_own_platform_database():
  """Under xdist, the environment the fixtures read and the engine the app
  bound at import must both point at this worker's copy. If the rewrite ran
  too late — after `robosystems` was imported — the engine would still be on
  the shared database while the fixtures truncate a private one."""
  from robosystems.db.platform import engine

  worker = os.environ[WORKER_ENV]
  assert database_name(os.environ["TEST_DATABASE_URL"]).endswith(f"_{worker}")
  assert engine.url.database is not None
  assert engine.url.database.endswith(f"_{worker}")
