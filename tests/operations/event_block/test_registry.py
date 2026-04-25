"""Tests for DSL event-handler matching."""

from __future__ import annotations

from robosystems.operations.event_block.registry import _matches_metadata_expression


def test_matches_metadata_expression_accepts_metadata_prefixed_paths() -> None:
  assert _matches_metadata_expression(
    {"metadata.category": "payroll"},
    {"category": "payroll"},
  )


def test_matches_metadata_expression_accepts_root_relative_paths() -> None:
  assert _matches_metadata_expression(
    {"details.kind": "bonus"},
    {"details": {"kind": "bonus"}},
  )
