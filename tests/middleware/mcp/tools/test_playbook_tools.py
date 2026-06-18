"""Tests for the workflow-playbook MCP tools.

The playbook is guidance-only (no DB, no side effects), so these tests
focus on (a) the tool definition shape and (b) that each mode returns the
sections + the load-bearing rules a codebase-blind operator needs. The
specific phrases asserted here are the ones the 2026-06-17 close-workflow
legibility gate found missing from the tool surface — keep them.
"""

from __future__ import annotations

import asyncio
import json

from robosystems.middleware.mcp.tools.playbook_tools import GetClosePlaybookTool


class _StubClient:
  graph_id = "kgTEST"
  user_id = "user_test"


def _tool() -> GetClosePlaybookTool:
  return GetClosePlaybookTool(_StubClient())


def _run(mode: str | None = None) -> dict:
  args = {} if mode is None else {"mode": mode}
  return asyncio.run(_tool().execute(args))


class TestGetClosePlaybookDefinition:
  def test_definition_shape(self) -> None:
    d = _tool().get_tool_definition()
    assert d["name"] == "get-close-playbook"
    schema = d["inputSchema"]
    assert schema["required"] == []
    assert schema["properties"]["mode"]["enum"] == [
      "overview",
      "initiate",
      "recurring",
    ]
    # The description points the agent here as the FIRST call.
    assert "get-close-playbook" not in d["description"]  # don't self-reference
    assert "Call this FIRST".lower() in d["description"].lower()


class TestGetClosePlaybookModes:
  def test_overview_returns_both_tracks(self) -> None:
    payload = _run()
    assert payload["mode"] == "overview"
    assert "recurring_close_sequence" in payload
    assert "initiate_first_close" in payload
    assert "schedule_authoring" in payload

  def test_recurring_excludes_setup(self) -> None:
    payload = _run("recurring")
    assert payload["mode"] == "recurring"
    assert "recurring_close_sequence" in payload
    assert "initiate_first_close" not in payload

  def test_initiate_includes_setup_and_authoring(self) -> None:
    payload = _run("initiate")
    assert payload["mode"] == "initiate"
    assert "initiate_first_close" in payload
    assert "schedule_authoring" in payload
    assert "recurring_close_sequence" not in payload

  def test_invalid_mode_falls_back_to_overview(self) -> None:
    assert _run("garbage")["mode"] == "overview"


class TestGetClosePlaybookContent:
  def test_carries_the_gate_residual_rules(self) -> None:
    """The legibility-gate residuals must be teachable from the playbook."""
    blob = json.dumps(_run())
    # One schedule = one debit/credit pair (multi-line ⇒ multiple schedules).
    assert "one per asset/element pair" in blob
    # The real draft-producing path (no phantom create-closing-entry).
    assert "promote-obligations" in blob
    assert "create-closing-entry" in blob  # explicitly named as nonexistent
    # Element discovery instead of inventing qnames.
    assert "get-graph-schema" in blob
    # Amounts in cents.
    assert "cents" in blob
    # Period-identifier convention friction.
    assert "YYYY-MM" in blob
    # block_type enum overstates capability (statements 501).
    assert "501" in blob
    # Per-tenant procedures doc convention.
    assert "search-documents" in blob
    # Onboarding watermark rule + its failure mode (blocked first close).
    assert "closed_through" in blob
    assert "pending_obligations" in blob

  def test_initiate_teaches_orient_then_watermark(self) -> None:
    """The initiate track must tell the agent to read the calendar's
    closed_through first and stamp the matching watermark on every schedule
    — the rule that prevents both a blocked first close and duplicate
    entries for already-handled months.
    """
    payload = _run("initiate")
    init_blob = " || ".join(payload["initiate_first_close"])
    authoring_blob = " || ".join(payload["schedule_authoring"])
    # Orient on the calendar before authoring.
    assert "get-fiscal-calendar" in init_blob
    assert "closed_through" in init_blob
    # Authoring carries the watermark-match rule + the blocked-close warning.
    assert "closed_through" in authoring_blob
    assert "pending" in authoring_blob.lower()

  def test_recurring_sequence_is_ordered_and_complete(self) -> None:
    seq = " || ".join(_run("recurring")["recurring_close_sequence"])
    # The canonical chain appears in order.
    for earlier, later in [
      ("get-fiscal-calendar", "promote-obligations"),
      ("promote-obligations", "list-period-drafts"),
      ("list-period-drafts", "close-period"),
    ]:
      assert seq.index(earlier) < seq.index(later), (earlier, later)
