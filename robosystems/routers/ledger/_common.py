"""Shared helpers for ledger router endpoints."""

from __future__ import annotations

from fastapi import HTTPException


def ledger_404() -> HTTPException:
  """Standard 404 for requests against an uninitialized ledger.

  Raised when the tenant schema or its tables don't exist — the user
  hasn't connected a data source yet. Kept as a single helper so the
  message stays consistent across every ledger router.
  """
  return HTTPException(
    status_code=404,
    detail="Ledger not initialized. Connect a data source first.",
  )
