"""Report-share response models: the sender's record of where a report went.

A `ReportShare` row lives in the source graph's tenant schema and is written
by `share-report`. Re-sharing to a recipient refreshes the active row; a
revoke stamps it `revoked_at` and leaves it as history. A recipient may hold
several rows over time (shared, revoked, shared again), so a list is read with
`revoked_at` in hand rather than pre-filtered.
"""

from datetime import datetime

from pydantic import BaseModel, Field

from robosystems.models.api.common import PaginationInfo


class ReportShareResponse(BaseModel):
  """One delivery of a report to a recipient graph."""

  id: str = Field(..., description="Share row identifier (ULID).")
  report_id: str = Field(..., description="The shared report's ID in this graph.")
  target_graph_id: str = Field(..., description="The recipient's graph ID.")
  target_graph_name: str | None = Field(
    None, description="Display name of the recipient graph (if known)."
  )
  shared_by: str = Field(..., description="User ID that shared the report.")
  shared_at: datetime = Field(
    ..., description="When the report was shared, or last re-shared."
  )
  revoked_at: datetime | None = Field(
    None,
    description=(
      "When the share was revoked. Null while the recipient holds the copy; "
      "a re-share after a revoke is a new row, so a revoked row stays revoked."
    ),
  )
  fact_count: int = Field(
    ..., description="Number of facts delivered to the recipient on this share."
  )


class ReportShareListResponse(BaseModel):
  """Paginated list of a report's shares, newest first."""

  report_shares: list[ReportShareResponse] = Field(
    default_factory=list, description="Shares, newest first."
  )
  pagination: PaginationInfo = Field(..., description="Pagination metadata.")
