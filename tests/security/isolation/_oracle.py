"""The oracle — classify a cross-tenant response as PASS / LEAK / … .

The load-bearing idea (spec §4): a status code alone cannot tell a leak from
a correctly-scoped empty result, and it passes the known real leak shape (a
200 carrying another tenant's `org_name`). So the verdict is computed by
comparing the attacker's response against what the *owner* legitimately sees
— the positive control is the truth source. If the owner's own request is
denied, the run is INVALID and no negative result may be trusted.

Verdicts are derived from API responses only — never from the database.
"""

from __future__ import annotations

import hashlib
import json
from enum import Enum

import httpx


class Verdict(str, Enum):
  PASS = "PASS"  # denied, or scoped-empty — no cross-tenant access
  LEAK = "LEAK"  # attacker reached the victim's resource / write was accepted
  INVALID = "INVALID"  # positive control failed — harness misconfigured, abort
  INCONCLUSIVE = "INCONCLUSIVE"  # 4xx that isn't a clean deny — worth a human look
  ERROR = "ERROR"  # transport error


_VOLATILE_KEYS = {
  "timestamp",
  "at",
  "created_at",
  "createdAt",
  "request_id",
  "requestId",
  "operationId",
  "operation_id",
  "trace_id",
  "expires_in",
}


def _strip_volatile(obj):
  if isinstance(obj, dict):
    return {k: _strip_volatile(v) for k, v in obj.items() if k not in _VOLATILE_KEYS}
  if isinstance(obj, list):
    return [_strip_volatile(v) for v in obj]
  return obj


# Structural / metadata keys that don't count as tenant content. Anything
# NOT in this set — e.g. `org_name` — is content, so `{"org_name": "...",
# "lists": []}` (the publish_lists.py F5 leak shape) is data, not empty.
_ENVELOPE_KEYS = {
  "status",
  "message",
  "success",
  "code",
  "detail",
  "ok",
  "count",
  "total",
  "page",
  "page_size",
  "limit",
  "offset",
  "has_more",
  "error",
  "errors",
  "warnings",
}


def _is_empty_data(obj) -> bool:
  """True when a body carries no tenant content (envelope/metadata only).

  A non-empty scalar under a non-envelope key counts as content — a leaked
  `org_name` with empty collections must not read as empty.
  """
  if obj is None:
    return True
  if isinstance(obj, str):
    return len(obj.strip()) == 0
  if isinstance(obj, (int, float)):  # a present numeric value is content
    return False
  if isinstance(obj, list):
    return len(obj) == 0
  if isinstance(obj, dict):
    for k, v in obj.items():
      if k in _ENVELOPE_KEYS or k in _VOLATILE_KEYS:
        continue
      if not _is_empty_data(v):
        return False
    return True
  return False


def body_fingerprint(resp: httpx.Response) -> str:
  """Stable digest of a response's data content (volatile fields removed)."""
  try:
    data = _strip_volatile(resp.json())
    canonical = json.dumps(data, sort_keys=True, default=str)
  except Exception:
    canonical = resp.text
  return hashlib.sha256(canonical.encode("utf-8", "replace")).hexdigest()[:16]


def positive_ok(resp: httpx.Response) -> bool:
  """The victim's own request must not be denied on its own graph.

  A 2xx (even an empty collection — a fresh graph has no backups/subgraphs)
  means the endpoint works and the owner has access. A denial *here* means
  the harness is misconfigured (wrong key or graph_id), so no negative
  result may be trusted — that is INVALID, not PASS.
  """
  return resp.status_code // 100 == 2


def has_data(resp: httpx.Response) -> bool:
  try:
    return not _is_empty_data(resp.json())
  except Exception:
    return bool(resp.text.strip())


def classify_read(
  victim: httpx.Response, attacker: httpx.Response
) -> tuple[Verdict, str]:
  """Classify a cross-tenant READ (GET, or a read-only cypher/MCP call).

  The primary assertion is denial: a non-owner must be turned away. When the
  owner legitimately has data, an identical fingerprint is the definitive
  leak signal; any data at all for a non-owned graph is a leak; a 2xx with
  no data still means authz did not deny, which is a finding worth a look.
  """
  if not positive_ok(victim):
    return (
      Verdict.INVALID,
      f"positive control failed: owner got {victim.status_code} on own graph",
    )
  sc = attacker.status_code
  if sc in (401, 403, 404):
    return Verdict.PASS, f"denied ({sc})"
  if sc // 100 == 2:
    if has_data(attacker):
      if has_data(victim) and body_fingerprint(attacker) == body_fingerprint(victim):
        return Verdict.LEAK, f"identical to owner data ({sc}) — cross-tenant read"
      return Verdict.LEAK, f"returned data for a non-owned graph ({sc})"
    return (
      Verdict.INCONCLUSIVE,
      f"200 on a non-owned graph, empty ({sc}) — authz did not deny",
    )
  return Verdict.INCONCLUSIVE, f"unexpected status {sc}"


def classify_graphql(
  victim: httpx.Response, attacker: httpx.Response, *, data_field: bool = False
) -> tuple[Verdict, str]:
  """Classify a cross-tenant GraphQL probe.

  GraphQL is graph-scoped by URL; `check_graph_access` should run before any
  resolver, and denials arrive as HTTP 401/403 or as an HTTP-200 body with
  `data: null` and an errors array. `data_field=True` marks a query over
  *graph data* (e.g. `{ entity { name } }`): a non-empty `data` payload for a
  non-owned graph is then a LEAK. `data_field=False` marks a caller-echo probe
  (`{ hello }`), where a 200 with data only means authz did not gate the field
  — INCONCLUSIVE, since the payload is the caller's own identity, not graph data.
  """
  if not positive_ok(victim):
    return Verdict.INVALID, f"positive control failed: owner got {victim.status_code}"
  if data_field:
    try:
      owner_data = victim.json().get("data")
    except Exception:
      owner_data = None
    if _is_empty_data(owner_data):
      return (
        Verdict.INVALID,
        "owner's data-field query returned no data — probe cannot detect a leak",
      )
  sc = attacker.status_code
  if sc in (401, 403):
    return Verdict.PASS, f"denied at HTTP ({sc})"
  if sc // 100 == 2:
    try:
      data = attacker.json().get("data")
    except Exception:
      return Verdict.INCONCLUSIVE, f"200 non-JSON ({sc})"
    if _is_empty_data(data):
      return Verdict.PASS, f"no data ({sc}) — denied at resolver"
    if data_field:
      return Verdict.LEAK, f"graph data for a non-owned graph ({sc})"
    return (
      Verdict.INCONCLUSIVE,
      f"200 with data ({sc}) — caller-echo field; judge with a graph-data field",
    )
  return Verdict.INCONCLUSIVE, f"unexpected status {sc}"


def classify_write(attacker: httpx.Response) -> tuple[Verdict, str]:
  """Classify a cross-tenant WRITE — any acceptance is a leak."""
  sc = attacker.status_code
  if sc // 100 == 2:
    return Verdict.LEAK, f"write accepted cross-tenant ({sc})"
  if sc in (401, 403, 404):
    return Verdict.PASS, f"denied ({sc})"
  if sc in (400, 422):
    # rejected, but before or after authz? worth confirming authz fires first.
    return Verdict.INCONCLUSIVE, f"rejected ({sc}) — confirm authz precedes validation"
  if sc == 429:
    return Verdict.INCONCLUSIVE, "rate-limited (429) — retry pacing"
  # Anything else (5xx, 405, …) is not a clean deny. A cross-tenant write that
  # crashes the endpoint must be flagged for a look, not folded into PASS —
  # the harness's whole promise is that only a clean deny reads as safe.
  return Verdict.INCONCLUSIVE, f"unexpected status {sc} — not a clean deny"
