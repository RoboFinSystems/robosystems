"""Blocked-source-graph request and response models.

Cross-graph report sharing is authorized capability-style: a sender who holds
your ``graph_id`` can copy a published report into your tenant schema. A block
is the recipient's refusal — ``share-report`` consults the list before copying
anything in, and a blocked sender receives an explicit per-target error rather
than a silent drop.

Blocking and purging are offered as one action because a recipient who blocks a
sender almost always wants both: stop the next one, and remove the ones already
landed.
"""

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from robosystems.models.api.common import PaginationInfo

# ── Requests ───────────────────────────────────────────────────────────────


class BlockSourceGraphRequest(BaseModel):
  """Block a source graph from sharing reports into this graph.

  Blocking is idempotent: re-blocking an already-blocked source succeeds and
  leaves the original ``blocked_at`` in place, so a retry can't be used to
  rewrite the record.
  """

  source_graph_id: str = Field(
    ...,
    max_length=64,
    description=(
      "Graph ID to block. Read it off the `source_graph_id` provenance field "
      "of a report that was shared to you."
    ),
  )
  reason: str | None = Field(
    None,
    max_length=500,
    description=("Free-form note for your own records. Never disclosed to the sender."),
  )
  purge: bool = Field(
    False,
    description=(
      "Also delete every report already shared in from this source, with "
      "their fact sets and facts. Reports you authored are never touched."
    ),
  )

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {"source_graph_id": "kg1a2b3c4d5e6f7a8b9c"},
        {
          "source_graph_id": "kg1a2b3c4d5e6f7a8b9c",
          "reason": "No longer a shareholder.",
          "purge": True,
        },
      ]
    }
  )


class UnblockSourceGraphRequest(BaseModel):
  """Remove a source graph from the block list, allowing shares again."""

  source_graph_id: str = Field(..., max_length=64, description="Graph ID to unblock.")

  model_config = ConfigDict(
    json_schema_extra={"examples": [{"source_graph_id": "kg1a2b3c4d5e6f7a8b9c"}]}
  )


# ── Responses ──────────────────────────────────────────────────────────────


class BlockedSourceGraphResponse(BaseModel):
  """One blocked source graph."""

  id: str = Field(..., description="Block row identifier (ULID).")
  source_graph_id: str = Field(..., description="The blocked sender's graph ID.")
  source_graph_name: str | None = Field(
    None, description="Display name of the blocked graph (if known)."
  )
  blocked_by: str = Field(..., description="User ID that created the block.")
  blocked_at: datetime = Field(..., description="When the block was created.")
  reason: str | None = Field(None, description="Recipient's own note, if given.")


class BlockSourceGraphResult(BaseModel):
  """Outcome of a block, including anything the purge removed."""

  block: BlockedSourceGraphResponse = Field(..., description="The block record.")
  already_blocked: bool = Field(
    False,
    description=(
      "True when the source was already blocked and this call was a no-op "
      "apart from any purge."
    ),
  )
  purged_report_count: int = Field(
    0,
    description=(
      "Number of previously-shared reports deleted from this graph. Zero "
      "unless `purge` was set."
    ),
  )
  # Also consumed server-side: the router's after-success hook uses these ids
  # to delete each purged copy's stored publication once the row deletion has
  # committed. That is an implementation detail and stays out of the
  # description, which is published verbatim into both client SDKs.
  purged_report_ids: list[str] = Field(
    default_factory=list,
    description=(
      "Ids of the previously-shared reports deleted from this graph. Empty "
      "unless `purge` was set."
    ),
  )


class BlockedSourceGraphListResponse(BaseModel):
  """Paginated list of blocked source graphs."""

  blocked_source_graphs: list[BlockedSourceGraphResponse] = Field(
    default_factory=list, description="Blocked source graphs."
  )
  pagination: PaginationInfo = Field(..., description="Pagination metadata.")
