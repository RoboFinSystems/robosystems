"""Request models for the unified operations endpoints."""

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class OperationResumeRequest(BaseModel):
  """Answer for an operation paused at a checkpoint (`awaiting_input`)."""

  input: dict[str, Any] = Field(
    default_factory=dict,
    description=(
      "The decision the paused run asked for, in whatever shape its prompt "
      "described. Delivered to the task as `params['resume']['input']`."
    ),
  )

  model_config = ConfigDict(
    json_schema_extra={"examples": [{"input": {"approved": True}}]},
  )
