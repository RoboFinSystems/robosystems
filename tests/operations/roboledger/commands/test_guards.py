"""Unit tests for the closed-period write guard."""

from __future__ import annotations

from unittest.mock import MagicMock


def _row(graph_id="kgabc", name="2026-01", status="open"):
  row = MagicMock()
  row.graph_id = graph_id
  row.name = name
  row.status = status
  return row
