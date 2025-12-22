from enum import Enum

from pydantic import BaseModel, Field


class AggregationMethod(str, Enum):
  SUM = "sum"
  AVERAGE = "average"
  WEIGHTED_AVERAGE = "weighted_average"
  CALCULATED = "calculated"
  FIRST = "first"
  LAST = "last"


class ElementAssociation(BaseModel):
  identifier: str = Field(..., description="Association identifier")
  source_element: str = Field(..., description="Source element URI (e.g., CoA element)")
  target_element: str = Field(..., description="Target element URI (e.g., US-GAAP)")
  aggregation_method: AggregationMethod = Field(
    AggregationMethod.SUM, description="How to aggregate values"
  )
  weight: float = Field(
    1.0, description="Weight for aggregation (used in weighted averages)"
  )
  formula: str | None = Field(None, description="Formula for calculated aggregations")
  order_value: float = Field(1.0, description="Order within mapping structure")


class MappingStructure(BaseModel):
  identifier: str = Field(..., description="Structure identifier")
  name: str = Field(..., description="Mapping structure name")
  description: str | None = Field(None, description="Description of this mapping")
  taxonomy_uri: str | None = Field(
    None, description="Source taxonomy URI (e.g., QuickBooks taxonomy)"
  )
  target_taxonomy_uri: str | None = Field(
    None, description="Target taxonomy URI (e.g., US-GAAP)"
  )
  associations: list[ElementAssociation] = Field(
    default_factory=list, description="Element associations in this mapping"
  )


class CreateMappingRequest(BaseModel):
  name: str = Field(..., description="Mapping name")
  description: str | None = Field(None, description="Mapping description")
  taxonomy_uri: str | None = Field(None, description="Source taxonomy URI")
  target_taxonomy_uri: str | None = Field(
    None, description="Target taxonomy URI (e.g., US-GAAP)"
  )


class CreateAssociationRequest(BaseModel):
  source_element: str = Field(..., description="Source element URI")
  target_element: str = Field(..., description="Target element URI")
  aggregation_method: AggregationMethod = Field(
    AggregationMethod.SUM, description="Aggregation method"
  )
  weight: float = Field(1.0, description="Weight for aggregation")
  formula: str | None = Field(None, description="Formula for calculated aggregations")
  order_value: float = Field(1.0, description="Order within structure")


class UpdateAssociationRequest(BaseModel):
  aggregation_method: AggregationMethod | None = Field(
    None, description="Aggregation method"
  )
  weight: float | None = Field(None, description="Weight for aggregation")
  formula: str | None = Field(None, description="Formula for calculated aggregations")
  order_value: float | None = Field(None, description="Order within structure")


class MappingResponse(BaseModel):
  structure: MappingStructure
  association_count: int
