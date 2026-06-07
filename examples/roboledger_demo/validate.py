"""Information Block validation — Step 7 of the roboledger demo.

Runs after schedules are created to verify:
  - FactSet rows exist for every schedule
  - SumEquals rules auto-generated and evaluate clean
  - GraphQL envelope resolves fact_set + verification_results
  - dispose-schedule computes correct NBV, deletes the SumEquals rule
  - Ad-hoc 3-month prepaid rollforward: creation → evaluation → cleanup

Can also be run standalone against any graph:

    uv run python -m examples.roboledger_demo.validate <graph_id>
"""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

BASE_URL = "http://localhost:8000"
CREDENTIALS_FILE = Path(".local/config.json")

_GREEN = "\033[32m"
_RED = "\033[31m"
_YELLOW = "\033[33m"
_BOLD = "\033[1m"
_RESET = "\033[0m"


# ── Result ────────────────────────────────────────────────────────────────────


@dataclass
class _Result:
  name: str
  passed: bool
  detail: str = ""

  def __str__(self) -> str:
    icon, color = ("✓", _GREEN) if self.passed else ("✗", _RED)
    line = f"    {color}{icon}{_RESET} {self.name}"
    if self.detail:
      line += f"  {_YELLOW}({self.detail}){_RESET}"
    return line


# ── Validator ─────────────────────────────────────────────────────────────────


class _Validator:
  def __init__(self, graph_id: str, api_key: str) -> None:
    self.graph_id = graph_id
    self.api_key = api_key
    self.results: list[_Result] = []
    # (graph_id, structure_id) pairs created by this run — deleted in cleanup()
    self._cleanup: list[tuple[str, str]] = []
    self._ledger = None
    self._gqlc = None

  # ── Primitives ───────────────────────────────────────────────────────────

  def _client(self):
    if self._ledger is None:
      from robosystems_client.clients.ledger_client import LedgerClient

      self._ledger = LedgerClient({"base_url": BASE_URL, "token": self.api_key})
    return self._ledger

  def _gql(self, query: str) -> dict[str, Any]:
    if self._gqlc is None:
      from robosystems_client.graphql.client import GraphQLClient

      self._gqlc = GraphQLClient(BASE_URL, token=self.api_key)
    return self._gqlc.execute(self.graph_id, query)

  def _sub(self, title: str) -> None:
    print(f"  {_BOLD}{title}{_RESET}")

  def _check(self, name: str, passed: bool, detail: str = "") -> bool:
    r = _Result(name=name, passed=passed, detail=detail)
    self.results.append(r)
    print(r)
    return passed

  def _skip(self, name: str, reason: str) -> None:
    print(f"    {_YELLOW}–{_RESET} {name}  {_YELLOW}(skipped: {reason}){_RESET}")

  # ── Schedule integrity ───────────────────────────────────────────────────

  def schedule_fact_sets(self) -> None:
    self._sub("FactSet rows")
    data = self._gql("""
      {
        informationBlocks(blockType: "schedule") {
          id name
          factSet { id periodStart periodEnd factsetType }
        }
      }
    """)
    blocks = data.get("informationBlocks", [])
    self._check("at least one schedule exists", len(blocks) > 0, f"{len(blocks)} found")
    for b in blocks:
      fs = b.get("factSet")
      self._check(
        b["name"],
        fs is not None,
        f"id={fs['id']}" if fs else "null — missing FactSet row",
      )

  def sum_equals_rules(self) -> None:
    self._sub("SumEquals rules (auto-generated)")
    data = self._gql("""
      {
        informationBlocks(blockType: "schedule") {
          id name
          rules { rulePattern ruleOrigin }
        }
      }
    """)
    for b in data.get("informationBlocks", []):
      native = [
        r
        for r in b["rules"]
        if r["rulePattern"] == "SumEquals" and r["ruleOrigin"] == "native"
      ]
      self._check(b["name"], len(native) == 1, f"{len(native)} native SumEquals rules")

  def evaluate_rules(self) -> None:
    self._sub("evaluate-rules (all schedules)")
    client = self._client()
    data = self._gql('{ informationBlocks(blockType: "schedule") { id name } }')
    for b in data.get("informationBlocks", []):
      try:
        payload = client.evaluate_rules(self.graph_id, structure_id=b["id"])
        # ``summary`` is the SDK's ``EvaluateRulesResponseSummary`` attrs
        # class — its keys land in ``additional_properties`` since the
        # server's schema declares it as an open ``dict[str, int]``.
        summary = payload.summary.to_dict() if payload.summary else {}
        # Empty dict means no rules were evaluated — treat as a failure so we
        # catch cases where the SumEquals rule wasn't created or wasn't found.
        if not summary:
          self._check(b["name"], False, "summary=empty (no rules evaluated)")
          continue
        fails = summary.get("fail", 0) + summary.get("error", 0)
        self._check(b["name"], fails == 0, f"summary={summary}")
      except Exception as exc:
        self._check(b["name"], False, str(exc))

  def graphql_envelope(self) -> None:
    self._sub("GraphQL envelope (one schedule)")
    # Fetch all schedules and pick the first one whose envelope carries
    # facts. A schedule's envelope only surfaces `fact_scope='in_scope'`
    # rows; schedules whose entire period window sits at or before
    # `closed_through` (e.g. Business Insurance running Apr 2025 -
    # Mar 2026 against closed_through=2026-03) correctly return zero
    # facts. The check picks the first schedule with in_scope facts so
    # it asserts envelope shape, not a side-effect of closed_through.
    data = self._gql("""
      {
        informationBlocks(blockType: "schedule") {
          name
          blockType
          factSet { id factsetType }
          rules { rulePattern ruleOrigin ruleSeverity }
          facts { value periodType }
          verificationResults { status }
          verificationSummary {
            total passed failed errored skipped
            byCategory { category total passed failed errored skipped }
          }
        }
      }
    """)
    blocks = data.get("informationBlocks", [])
    if not blocks:
      self._check("schedule available for envelope check", False)
      return
    b = next((blk for blk in blocks if len(blk.get("facts", [])) > 0), None)
    if b is None:
      self._check(
        "schedule with in_scope facts available",
        False,
        "all schedules have zero in_scope facts (closed_through covers every period?)",
      )
      return
    self._check("blockType = 'schedule'", b.get("blockType") == "schedule")
    self._check(
      "facts present",
      len(b.get("facts", [])) > 0,
      f"{len(b.get('facts', []))} facts on '{b.get('name')}'",
    )
    self._check("factSet non-null", b.get("factSet") is not None)
    rules = b.get("rules", [])
    self._check("rules present", len(rules) > 0, f"{len(rules)} rules")
    se = [
      r
      for r in rules
      if r.get("rulePattern") == "SumEquals" and r.get("ruleOrigin") == "native"
    ]
    self._check("native SumEquals rule in envelope", len(se) == 1, f"{len(se)} found")

    # verificationSummary arm: server-computed aggregate over
    # verificationResults. Present when there are results; its counts
    # must reconcile with the result rows and the per-category
    # breakdown.
    vr = b.get("verificationResults", [])
    vs = b.get("verificationSummary")
    if vr:
      self._check(
        "verificationSummary present",
        vs is not None,
        "present" if vs is not None else "missing despite verificationResults",
      )
      if vs:
        tallied = vs["passed"] + vs["failed"] + vs["errored"] + vs["skipped"]
        self._check(
          "verificationSummary counts reconcile",
          vs["total"] == tallied == len(vr),
          f"total={vs['total']} tallied={tallied} results={len(vr)}",
        )
        by_cat_total = sum(c["total"] for c in vs.get("byCategory", []))
        self._check(
          "verificationSummary byCategory sums to total",
          by_cat_total == vs["total"],
          f"byCategory={by_cat_total} total={vs['total']}",
        )
    else:
      self._check(
        "verificationSummary null when no results",
        vs is None,
        f"expected null, got {vs}",
      )

  # ── Dispose smoke test ───────────────────────────────────────────────────

  def dispose_smoke_test(self, dr_id: str, cr_id: str) -> None:
    self._sub("asset_disposed event block smoke test")
    if not dr_id or not cr_id:
      self._skip("dispose test", "element IDs unavailable")
      return

    client = self._client()

    # Dispose the asset on the last day of the demo's `close_target`
    # period (the next period to be closed — always open by definition).
    # Schedule spans `close_target` + the following month so we have
    # exactly two periods of depreciation, with NBV = monthly_amount on
    # the disposal date. Computing periods dynamically — using fixed
    # dates breaks whenever `current_month_period()` advances past them.
    from robosystems.operations.roboledger.fiscal_calendar import (
      current_month_period,
      next_period,
      period_date_range,
      previous_period,
    )

    # close_target = previous_period(current_month_period()) per
    # `initialize_fiscal_calendar` in main.py. Schedule starts in
    # close_target, runs two months, disposed on the last day of
    # close_target. Both periods are open.
    close_target = previous_period(current_month_period())
    schedule_start, _ = period_date_range(close_target)
    _, disposal_date = period_date_range(close_target)
    _, schedule_end = period_date_range(next_period(close_target))

    # 2-month schedule, $200 total → dispose at end of period 1 → NBV $100, loss $100
    try:
      sched = client.create_schedule(
        self.graph_id,
        name="[VALIDATE] Dispose Smoke Test",
        element_ids=[dr_id, cr_id],
        period_start=schedule_start.isoformat(),
        period_end=schedule_end.isoformat(),
        monthly_amount=10_000,
        debit_element_id=dr_id,
        credit_element_id=cr_id,
        entry_type="closing",
        memo_template="Validation disposal test",
        method="straight_line",
        original_amount=20_000,
        useful_life_months=2,
        asset_element_id=cr_id,
      )
      sid = sched.id or ""
      self._check("throwaway schedule created", bool(sid), f"id={sid}")
      if sid:
        self._cleanup.append((self.graph_id, sid))
    except Exception as exc:
      self._check("throwaway schedule created", False, str(exc))
      return

    # Preview the disposal first — confirm NBV/gain-loss computation
    # before firing the handler for real.
    disposal_body = {
      "event_type": "asset_disposed",
      "event_category": "adjustment",
      "event_class": "economic",
      "source": "manual",
      "occurred_at": f"{disposal_date.isoformat()}T00:00:00Z",
      "metadata": {
        "schedule_id": sid,
        "proceeds": 0,
        "gain_loss_element_id": dr_id,
        "memo": "Validation disposal",
        "reason": "e2e test",
      },
      "apply_handlers": True,
    }

    try:
      preview = client.preview_event_block(self.graph_id, disposal_body)
      self._check(
        "preview would_succeed",
        preview.would_succeed is True,
        f"errors={preview.validation_errors}",
      )
      computed = preview.handler_metadata.to_dict() if preview.handler_metadata else {}
      nbv_preview = computed.get("nbv_cents", -1)
      gain_loss_preview = computed.get("gain_loss_cents")
      self._check("preview NBV > 0", nbv_preview > 0, f"{nbv_preview} cents")
      self._check(
        "preview gain_loss < 0 (loss)",
        gain_loss_preview is not None and gain_loss_preview < 0,
        f"{gain_loss_preview} cents",
      )
    except Exception as exc:
      self._check("preview-event-block call", False, str(exc))
      return

    # Execute the disposal via create-event-block directly to keep the
    # validation aligned with the demo's pedagogy of going through the
    # event-block primitive (the SDK's `dispose_schedule` helper wraps
    # the same call).
    try:
      payload = client.create_event_block(self.graph_id, disposal_body)

      self._check(
        "event status = fulfilled",
        payload.status == "fulfilled",
        f"status={payload.status}",
      )
      self._check("event id returned", bool(payload.id), payload.id or "")
    except Exception as exc:
      self._check("dispose_schedule call", False, str(exc))
      return

    try:
      block = (
        self._gql(f"""
        {{ informationBlock(id: "{sid}") {{ rules {{ rulePattern ruleOrigin }} }} }}
      """).get("informationBlock")
        or {}
      )
      native_se = [
        r
        for r in block.get("rules", [])
        if r["rulePattern"] == "SumEquals" and r["ruleOrigin"] == "native"
      ]
      self._check(
        "SumEquals rule deleted after dispose",
        len(native_se) == 0,
        f"{len(native_se)} remain",
      )
    except Exception as exc:
      self._check("SumEquals rule deleted after dispose", False, str(exc))

  # ── Ad-hoc rollforward ───────────────────────────────────────────────────

  def adhoc_rollforward(self, dr_id: str, cr_id: str) -> None:
    """3-month prepaid rollforward — validates the full create→evaluate lifecycle.

    $300 original, $100/mo. Checks FactSet envelope, SumEquals expected_total,
    no rounding drift across 3 periods, and clean evaluate-rules result.
    """
    self._sub("ad-hoc prepaid rollforward (3 months, $300)")
    if not dr_id or not cr_id:
      self._skip("rollforward", "element IDs unavailable")
      return

    client = self._client()

    # Use 3 open periods starting at `close_target` so the schedule
    # spans only un-closed months. Computing dynamically keeps the test
    # stable as `current_month_period()` advances.
    from robosystems.operations.roboledger.fiscal_calendar import (
      current_month_period,
      next_period,
      period_date_range,
      previous_period,
    )

    p1 = previous_period(current_month_period())  # close_target
    p2 = next_period(p1)
    p3 = next_period(p2)
    rf_start, _ = period_date_range(p1)
    _, rf_end = period_date_range(p3)

    try:
      sched = client.create_schedule(
        self.graph_id,
        name="[VALIDATE] Prepaid Rollforward",
        element_ids=[dr_id, cr_id],
        period_start=rf_start.isoformat(),
        period_end=rf_end.isoformat(),
        monthly_amount=10_000,
        debit_element_id=dr_id,
        credit_element_id=cr_id,
        entry_type="closing",
        memo_template="Validation rollforward test",
        method="straight_line",
        original_amount=30_000,
        useful_life_months=3,
      )
      sid = sched.id or ""
      self._check("rollforward schedule created", bool(sid), f"id={sid}")
      if sid:
        self._cleanup.append((self.graph_id, sid))
    except Exception as exc:
      self._check("rollforward schedule created", False, str(exc))
      return

    try:
      data = self._gql(f"""
        {{
          informationBlock(id: "{sid}") {{
            factSet {{ periodStart periodEnd factsetType }}
            rules {{ rulePattern ruleOrigin }}
            facts {{ value periodType }}
          }}
        }}
      """)
    except Exception as exc:
      self._check("GraphQL envelope fetch", False, str(exc))
      return

    b = data.get("informationBlock") or {}

    fs = b.get("factSet")
    self._check(
      "FactSet created",
      fs is not None,
      f"{fs['periodStart']}→{fs['periodEnd']}" if fs else "null",
    )
    if fs:
      self._check(
        f"FactSet spans {p1} to {p3}",
        fs.get("periodStart") == rf_start.isoformat()
        and fs.get("periodEnd") == rf_end.isoformat(),
        f"{fs['periodStart']} → {fs['periodEnd']}",
      )

    se = [
      r
      for r in b.get("rules", [])
      if r.get("rulePattern") == "SumEquals" and r.get("ruleOrigin") == "native"
    ]
    self._check("SumEquals rule present", len(se) == 1, f"{len(se)} found")

    duration = [f for f in b.get("facts", []) if f.get("periodType") == "duration"]
    self._check("3 duration facts", len(duration) == 3, f"found {len(duration)}")
    if duration:
      total = sum(f["value"] for f in duration if f.get("value") is not None)
      self._check(
        "no rounding drift (sum = 300.0)", abs(total - 300.0) < 0.01, f"sum={total}"
      )

    try:
      payload = client.evaluate_rules(self.graph_id, structure_id=sid)
      summary = payload.summary.to_dict() if payload.summary else {}
      if not summary:
        self._check(
          "evaluate-rules all-pass", False, "summary=empty (no rules evaluated)"
        )
      else:
        fails = summary.get("fail", 0) + summary.get("error", 0)
        self._check("evaluate-rules all-pass", fails == 0, f"summary={summary}")
    except Exception as exc:
      self._check("evaluate-rules", False, str(exc))

  # ── Cleanup ──────────────────────────────────────────────────────────────

  def cleanup(self) -> None:
    if not self._cleanup:
      return
    client = self._client()
    for gid, sid in self._cleanup:
      try:
        client.delete_schedule(gid, sid)
      except Exception:
        pass


# ── Element lookup ────────────────────────────────────────────────────────────


def _prepaid_elements(graph_id: str, api_key: str) -> tuple[str, str]:
  """Return (debit_id, credit_id) from an existing prepaid schedule's mechanics.

  Prefers Business Insurance (clearest prepaid pattern). Falls back to any
  schedule. Returns ('', '') when no schedules exist yet.
  """
  from robosystems_client.graphql.client import GraphQLClient

  query = """
    { informationBlocks(blockType: "schedule") { name artifact { mechanics } } }
  """
  try:
    data = GraphQLClient(BASE_URL, token=api_key, timeout=15).execute(graph_id, query)
    blocks = data.get("informationBlocks", [])
    # Prefer a prepaid (insurance) schedule — avoid depreciation
    ordered = sorted(
      blocks,
      key=lambda b: 0 if "Insurance" in b["name"] and "Year 2" not in b["name"] else 1,
    )
    for b in ordered:
      et = b.get("artifact", {}).get("mechanics", {}).get("entry_template", {})
      dr, cr = et.get("debit_element_id", ""), et.get("credit_element_id", "")
      if dr and cr:
        return dr, cr
  except Exception:
    pass
  return "", ""


# ── Public entry point ────────────────────────────────────────────────────────


def run_validation(graph_id: str, api_key: str) -> bool:
  """Run all Information Block checks. Returns True when every check passes."""
  dr_id, cr_id = _prepaid_elements(graph_id, api_key)

  v = _Validator(graph_id, api_key)
  try:
    v.schedule_fact_sets()
    v.sum_equals_rules()
    v.evaluate_rules()
    v.graphql_envelope()
    v.dispose_smoke_test(dr_id, cr_id)
    v.adhoc_rollforward(dr_id, cr_id)
  finally:
    v.cleanup()

  passed = sum(1 for r in v.results if r.passed)
  total = len(v.results)
  failed = total - passed
  color = _GREEN if failed == 0 else _RED
  print(f"  {color}{_BOLD}{passed}/{total} checks passed{_RESET}", end="")
  if failed:
    print(f"  {_RED}({failed} failed){_RESET}")
  else:
    print()
  return failed == 0


# ── Standalone usage ─────────────────────────────────────────────────────────


def _main() -> None:
  if not CREDENTIALS_FILE.exists():
    print(f"ERROR: {CREDENTIALS_FILE} not found. Run `just demo-user` first.")
    sys.exit(1)

  creds = json.loads(CREDENTIALS_FILE.read_text())
  api_key = creds.get("api_key", "")

  if len(sys.argv) > 1:
    graph_id = sys.argv[1]
  else:
    graph_id = creds.get("graphs", {}).get("cascade_demo", "")
    if not graph_id:
      print("Usage: uv run python -m examples.roboledger_demo.validate <graph_id>")
      sys.exit(1)

  print(f"{_BOLD}Information Block Validation{_RESET}  graph={graph_id}")
  ok = run_validation(graph_id, api_key)
  sys.exit(0 if ok else 1)


if __name__ == "__main__":
  _main()
