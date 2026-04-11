"""Shared error helpers for ledger routers."""

from fastapi import HTTPException


def ledger_not_found() -> HTTPException:
  """Raise when the extensions schema doesn't exist for a graph."""
  return HTTPException(
    status_code=404,
    detail="Ledger not initialized. Connect a data source first.",
  )
