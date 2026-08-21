"""Unit tests for the oracle — proves the harness can actually catch a leak.

A live run where every attacker is denied (403) reports all-PASS without ever
exercising the LEAK path, so a green run against a correct system is weak
evidence that the classifier works. These tests feed the oracle synthetic
responses — including real leaks — and assert it flags them. Plain unit tests:
no TARGET_API_URL, they run in the normal suite.
"""

from __future__ import annotations

import httpx
import pytest

from ._oracle import (
  Verdict,
  classify_graphql,
  classify_read,
  classify_write,
)

pytestmark = pytest.mark.unit


def _resp(status: int, json=None, text: str | None = None) -> httpx.Response:
  if json is not None:
    return httpx.Response(status, json=json)
  return httpx.Response(status, text=text or "")


# ── reads ──────────────────────────────────────────────────────────────────


def test_read_denied_is_pass():
  victim = _resp(200, json={"c": 5})
  attacker = _resp(403, json={"detail": "forbidden"})
  assert classify_read(victim, attacker)[0] is Verdict.PASS


def test_read_identical_data_is_leak():
  """Attacker sees exactly the owner's data — the definitive leak."""
  data = {"columns": ["c"], "rows": [[5]]}
  victim = _resp(200, json=data)
  attacker = _resp(200, json=data)
  verdict, note = classify_read(victim, attacker)
  assert verdict is Verdict.LEAK, note


def test_read_any_data_for_non_owned_graph_is_leak():
  """Even non-identical data for a graph you don't own is a leak."""
  victim = _resp(200, json={"members": [{"id": "u_owner"}]})
  attacker = _resp(200, json={"members": [{"id": "u_someone"}]})
  assert classify_read(victim, attacker)[0] is Verdict.LEAK


def test_read_leak_shape_org_name():
  """A 200 carrying another tenant's org_name is the known real leak shape."""
  victim = _resp(200, json={"org_name": "Victim Corp", "lists": []})
  attacker = _resp(200, json={"org_name": "Victim Corp", "lists": []})
  assert classify_read(victim, attacker)[0] is Verdict.LEAK


def test_read_owner_denied_is_invalid():
  """Positive control failed — harness misconfigured, must not read as PASS."""
  victim = _resp(403, json={"detail": "forbidden"})
  attacker = _resp(403, json={"detail": "forbidden"})
  assert classify_read(victim, attacker)[0] is Verdict.INVALID


def test_read_owner_empty_is_valid_positive_control():
  """A fresh graph's empty /backups is a valid positive control, not INVALID."""
  victim = _resp(200, json={"backups": []})
  attacker = _resp(403, json={"detail": "forbidden"})
  assert classify_read(victim, attacker)[0] is Verdict.PASS


def test_read_attacker_200_empty_is_inconclusive():
  """200 for a non-owned graph, even empty, means authz did not deny."""
  victim = _resp(200, json={"backups": []})
  attacker = _resp(200, json={"backups": []})
  assert classify_read(victim, attacker)[0] is Verdict.INCONCLUSIVE


# ── writes ─────────────────────────────────────────────────────────────────


def test_write_accepted_is_leak():
  for sc in (200, 201, 202):
    assert classify_write(_resp(sc, json={"ok": True}))[0] is Verdict.LEAK


def test_write_denied_is_pass():
  for sc in (401, 403, 404):
    assert classify_write(_resp(sc, json={"detail": "no"}))[0] is Verdict.PASS


def test_write_validation_error_is_inconclusive():
  assert classify_write(_resp(422, json={"detail": "bad"}))[0] is Verdict.INCONCLUSIVE


def test_write_server_error_is_inconclusive():
  """A 5xx on a cross-tenant write is an anomaly, not a clean deny — must flag."""
  for sc in (500, 503):
    assert classify_write(_resp(sc, json={"detail": "boom"}))[0] is Verdict.INCONCLUSIVE


# ── graphql ────────────────────────────────────────────────────────────────


def test_graphql_http_denied_is_pass():
  victim = _resp(200, json={"data": {"hello": "hello, owner"}})
  attacker = _resp(403, json={"detail": "forbidden"})
  assert classify_graphql(victim, attacker)[0] is Verdict.PASS


def test_graphql_resolver_error_is_pass():
  victim = _resp(200, json={"data": {"hello": "hello, owner"}})
  attacker = _resp(
    200, json={"data": None, "errors": [{"extensions": {"code": "FORBIDDEN"}}]}
  )
  assert classify_graphql(victim, attacker)[0] is Verdict.PASS


def test_graphql_200_with_data_is_inconclusive():
  """`hello` echoes the caller, so a cross-tenant 200 needs a data field to judge."""
  victim = _resp(200, json={"data": {"hello": "hello, owner"}})
  attacker = _resp(200, json={"data": {"hello": "hello, attacker"}})
  assert classify_graphql(victim, attacker)[0] is Verdict.INCONCLUSIVE


def test_graphql_data_field_leak():
  """A cross-tenant `{ entity }` payload is real graph data — a leak."""
  data = {"data": {"entity": {"name": "Victim Corp"}}}
  victim = _resp(200, json=data)
  attacker = _resp(200, json=data)
  assert classify_graphql(victim, attacker, data_field=True)[0] is Verdict.LEAK


def test_graphql_data_field_denied_is_pass():
  victim = _resp(200, json={"data": {"entity": {"name": "Victim Corp"}}})
  attacker = _resp(
    200, json={"data": None, "errors": [{"extensions": {"code": "FORBIDDEN"}}]}
  )
  assert classify_graphql(victim, attacker, data_field=True)[0] is Verdict.PASS


def test_graphql_data_field_owner_empty_is_invalid():
  """A data-field probe whose owner query returns no data can't detect a leak."""
  victim = _resp(200, json={"data": {"entity": None}})
  attacker = _resp(200, json={"data": None})
  assert classify_graphql(victim, attacker, data_field=True)[0] is Verdict.INVALID
