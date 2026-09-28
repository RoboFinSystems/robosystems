"""Graph mutation audit API models."""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


class MutationAuditEntry(BaseModel):
  """One mutating call on the graph."""

  id: str = Field(..., description="Audit entry identifier")
  occurred_at: datetime = Field(..., description="When the call finished")
  surface: Literal["api", "mcp", "operator"] = Field(
    ...,
    description=(
      "Where the call came from: 'api' for a REST operation, 'mcp' for an "
      "external MCP client, 'operator' for an in-app AI operator run such as "
      "the console's /do"
    ),
  )
  operation_name: str = Field(..., description="The operation or MCP tool that ran")
  status: Literal["completed", "failed"] = Field(
    ...,
    description="Whether the call succeeded; a failed call changed nothing it reports",
  )
  error_code: str | None = Field(None, description="Why a failed call failed")
  duration_ms: float = Field(..., description="How long the call took")
  user_id: str | None = Field(None, description="The user the call ran as")
  auth_method: str | None = Field(
    None, description="How the caller authenticated, for example 'api_key' or 'oauth'"
  )
  api_key_prefix: str | None = Field(
    None, description="The first characters of the API key used, when one was"
  )
  request_id: str | None = Field(
    None, description="The HTTP request that made the call"
  )
  operation_id: str | None = Field(
    None,
    description=(
      "The REST operation's envelope id, or the operator run's operation id: "
      "every write an operator run makes shares it"
    ),
  )
  operator_type: str | None = Field(
    None, description="The operator that made the call, for surface 'operator'"
  )
  arguments_fingerprint: str | None = Field(
    None,
    description=(
      "SHA-256 of the call's arguments. The arguments themselves are not stored"
    ),
  )
  object_ids: list[str] = Field(
    default_factory=list, description="Identifiers of the objects the call touched"
  )


class MutationAuditListResponse(BaseModel):
  """A page of the graph's mutation audit, newest first."""

  graph_id: str
  entries: list[MutationAuditEntry]
  next_cursor: str | None = Field(
    None, description="Pass as `cursor` for the next, older page; null on the last page"
  )
