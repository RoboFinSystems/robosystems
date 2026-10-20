"""Management dimensions: department, class, location and project on ledger lines.

A dimension type is one axis and a `Dimension` row one member of it. A line
carries at most one member per axis; an untagged line is in the axis's
*Unallocated* member, so the members plus Unallocated always sum to the
undimensioned total.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field

# The axes offered by name. The set is open: any lowercase type is one axis.
COMMON_DIMENSION_TYPES = ("department", "class", "location", "project")

# A statement or trial balance read of the lines no member of the axis tags.
UNALLOCATED = "unallocated"


class DimensionResponse(BaseModel):
  """One member of a management dimension."""

  id: str = Field(..., description="The member's id, as lines carry it.")
  dimension_type: str = Field(
    ..., description="The axis: department, class, location, project, or another."
  )
  name: str = Field(..., description="The member's label.")
  value: str = Field(
    ...,
    description=(
      "The member's stable key within its axis: the source's id for a synced "
      "member, a slug of the name for one created here."
    ),
  )
  source: str = Field(
    "native", description="Where the member came from: `native` or `quickbooks`."
  )
  is_active: bool = Field(
    ...,
    description=(
      "False once retired: the member stays on the lines that carry it and "
      "can no longer be put on new ones."
    ),
  )
  line_count: int = Field(0, description="How many ledger lines carry the member.")
  created_at: datetime | None = Field(None, description="When it was created.")


class DimensionListResponse(BaseModel):
  """The graph's members of its management dimensions, by axis then name."""

  dimensions: list[DimensionResponse] = Field(
    ..., description="The members, ordered by axis and then name."
  )
  types: list[str] = Field(
    ..., description="The axes the graph has members on, in order."
  )


class CreateDimensionRequest(BaseModel):
  """Add a member to a management dimension."""

  dimension_type: str = Field(
    ...,
    pattern=r"^[a-z][a-z0-9_]{0,39}$",
    description=(
      "The axis: `department`, `class`, `location` or `project`, or any other "
      "lowercase name, which starts an axis of its own. `scenario` is "
      "reserved for forecasts."
    ),
  )
  name: str = Field(
    ..., min_length=1, max_length=200, description="The member's label."
  )
  value: str | None = Field(
    None,
    min_length=1,
    max_length=200,
    description=(
      "The member's stable key within the axis; omitted, a slug of the name. "
      "Refused (409) when the axis already has it."
    ),
  )


class UpdateDimensionRequest(BaseModel):
  """Rename or retire a member. Members are never deleted: lines reference
  them."""

  dimension_id: str = Field(..., description="The member to change.")
  name: str | None = Field(
    None,
    min_length=1,
    max_length=200,
    description=(
      "The new label. Refused on a synced member, whose name follows its source."
    ),
  )
  is_active: bool | None = Field(
    None,
    description=(
      "False retires the member: lines that carry it keep it, and it can no "
      "longer be put on new ones. True brings it back."
    ),
  )


class LineDimension(BaseModel):
  """A member a ledger line carries."""

  id: str = Field(..., description="The member's id.")
  dimension_type: str = Field(..., description="The member's axis.")
  name: str = Field(..., description="The member's label.")


class StatementDimension(BaseModel):
  """A management slice of a statement or trial balance."""

  type: str = Field(
    ..., description="The axis to slice by: `department`, `class`, and so on."
  )
  member_id: str | None = Field(
    None,
    description=(
      "One member's lines only, by id, or `unallocated` for the lines no "
      "member of the axis tags. Omitted, a statement returns one column per "
      "member, then Unallocated, then Total, for the current window."
    ),
  )


class StatementDimensionColumn(BaseModel):
  """One column of a statement read by dimension."""

  member_id: str | None = Field(
    None,
    description=(
      "The member the column reads, `unallocated` for the untagged lines, "
      "null for the Total."
    ),
  )
  name: str = Field(..., description="The column's label.")


class StatementDimensionResponse(BaseModel):
  """How a statement was sliced, and whether the slice ties out."""

  type: str = Field(..., description="The axis the statement was sliced by.")
  member_id: str | None = Field(
    None,
    description=(
      "The one member read, when the statement was filtered to one; null "
      "when it was read by column."
    ),
  )
  columns: list[StatementDimensionColumn] = Field(
    default_factory=list,
    description=(
      "Read by column: one per member, then Unallocated, then Total, aligned "
      "with each row's `values` and with `periods`. Empty when filtered to "
      "one member."
    ),
  )
  ties_out: bool | None = Field(
    None,
    description=(
      "Read by column: whether every row's members plus Unallocated equal "
      "its Total. Null when filtered to one member."
    ),
  )
  differences: list[str] = Field(
    default_factory=list,
    description="The rows whose members plus Unallocated miss the Total, by qname.",
  )
