"""Text-block binding request and response models.

The ``bind-text-block`` operation binds a platform Document (markdown) —
or one of its sections — to a disclosure Element as a ``Nonnumeric``
text-block Fact in a standing ``factset_type='disclosure'`` FactSet.
Report builds snapshot that standing set into the report's own FactSet,
so a filed report stays immutable even if the document is later edited.
"""

from __future__ import annotations

from datetime import date

from pydantic import BaseModel, Field, model_validator


class BindTextBlockRequest(BaseModel):
  document_id: str = Field(
    ...,
    description="Platform Document to bind (see list-documents).",
  )
  section_id: str | None = Field(
    None,
    description=(
      "Slugified heading id of one section to bind (the section ids "
      "search-documents returns); omit to bind the whole document."
    ),
  )
  structure_id: str = Field(
    ...,
    description=(
      "Disclosure Structure the text block belongs to — must be "
      "block_type='regulatory_disclosure' with a text-block "
      "concept_arrangement (text_block / levelN_textblock)."
    ),
  )
  element_id: str | None = Field(
    None,
    description="Disclosure element to tag the text to (id form).",
  )
  element_qname: str | None = Field(
    None,
    description=(
      "Disclosure element qname (e.g. 'acme:SignificantAccounting"
      "PoliciesTextBlock') — exactly one of element_id / element_qname."
    ),
  )
  period_start: date = Field(
    ...,
    description="Reporting period start the narrative covers (duration fact).",
  )
  period_end: date = Field(..., description="Reporting period end.")
  entity_id: str | None = Field(
    None,
    description="Entity the fact belongs to; defaults to the primary entity.",
  )

  @model_validator(mode="after")
  def _exactly_one_element_ref(self) -> BindTextBlockRequest:
    if bool(self.element_id) == bool(self.element_qname):
      raise ValueError("provide exactly one of element_id / element_qname")
    return self


class BindTextBlockResponse(BaseModel):
  fact_id: str = Field(..., description="The Nonnumeric Fact created.")
  fact_set_id: str = Field(
    ..., description="Standing 'disclosure' FactSet holding the fact."
  )
  structure_id: str
  element_id: str
  document_id: str
  section_id: str | None = None
  content_hash: str = Field(
    ..., description="Full sha256 hex of the bound text (drift signal)."
  )
  characters: int = Field(..., description="Length of the bound text.")
  period_start: date
  period_end: date
  replaced: bool = Field(
    ...,
    description=(
      "True when a re-bind replaced this element's existing fact in the "
      "standing FactSet (content and provenance refreshed)."
    ),
  )
