"""Graph mutation audit and operator run API models."""

from datetime import datetime
from typing import Any, Literal

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
  status: Literal["completed", "failed", "pending"] = Field(
    ...,
    description=(
      "Whether the call succeeded; a failed call changed nothing it reports. "
      "'pending' means the call started background work; follow its "
      "operation_id for the outcome"
    ),
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


class OperatorRunSummary(BaseModel):
  """One in-app AI operator run: what it was asked and what it answered."""

  operation_id: str = Field(
    ...,
    description=(
      "The run's operation id; every mutation audit entry the run made "
      "carries it as `operation_id`"
    ),
  )
  thread_id: str | None = Field(
    None,
    description=(
      "The conversation the run belongs to, when the caller threaded it; "
      "null for a standalone API call"
    ),
  )
  user_id: str = Field(..., description="The user the run ran as")
  operator_type: str = Field(..., description="The operator that ran")
  mode: str = Field(..., description="The execution mode the run used")
  status: Literal["completed", "failed", "cancelled"] = Field(
    ..., description="How the run ended"
  )
  question: str = Field(..., description="The message the run was given")
  answer: str | None = Field(
    None, description="The operator's answer; null when the run did not reach one"
  )
  error: str | None = Field(None, description="Why a failed run failed")
  credits_consumed: float = Field(0.0, description="Credits the run's model calls cost")
  call_count: int = Field(0, description="Model calls the run made")
  tools_called: list[str] = Field(
    default_factory=list, description="The tools the run called, each named once"
  )
  write_count: int = Field(0, description="Writes the run made to the graph")
  started_at: datetime = Field(..., description="When the run started on the worker")
  finished_at: datetime = Field(..., description="When the run ended")
  duration_ms: float = Field(..., description="How long the run took")


class OperatorRunDetail(OperatorRunSummary):
  """A run with its full paper trail: the history it was given, every model
  call, and the response envelope's metadata."""

  history: list[dict[str, Any]] = Field(
    default_factory=list,
    description="The prior turns the run was given, as role/content pairs",
  )
  transcript: list[dict[str, Any]] = Field(
    default_factory=list,
    description=(
      "One entry per model call, in order: the input added since the previous "
      "call (the question, then each turn's tool results), the model's output "
      "blocks (text and tool calls), stop reason, token usage and credits. The "
      "first entry also carries the system prompt's SHA-256 and size and the "
      "tool names offered; later entries repeat them only when they change"
    ),
  )
  result: dict[str, Any] = Field(
    default_factory=dict,
    description=(
      "The response envelope's metadata without its result rows: tokens, "
      "credits, the Cypher run, the writes made, confidence and loop flags"
    ),
  )


class OperatorRunListResponse(BaseModel):
  """A page of the graph's operator runs, newest first."""

  graph_id: str
  runs: list[OperatorRunSummary]
  next_cursor: str | None = Field(
    None, description="Pass as `cursor` for the next, older page; null on the last page"
  )


class OperatorThreadSummary(BaseModel):
  """One console conversation: the runs that share a thread id."""

  thread_id: str = Field(..., description="The thread id the runs were made under")
  user_id: str = Field(..., description="The user whose conversation it is")
  title: str = Field(..., description="The conversation's first question, clipped")
  run_count: int = Field(..., description="Runs in the thread")
  first_run_at: datetime = Field(..., description="When the first run started")
  last_run_at: datetime = Field(..., description="When the latest run started")
  last_operator_type: str = Field(..., description="The operator the latest run used")
  last_status: Literal["completed", "failed", "cancelled"] = Field(
    ..., description="How the latest run ended"
  )


class OperatorThreadListResponse(BaseModel):
  """A page of the graph's operator threads, most recently active first."""

  graph_id: str
  threads: list[OperatorThreadSummary]
  next_cursor: str | None = Field(
    None, description="Pass as `cursor` for the next, older page; null on the last page"
  )
