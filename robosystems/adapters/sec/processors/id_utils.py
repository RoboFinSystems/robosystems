"""
XBRL ID Generation Utilities

Deterministic UUID generation for XBRL graph entities using UUIDv7
for optimal database performance and cross-pipeline consistency.
"""

from robosystems.utils.uuid import generate_deterministic_uuid7


def create_element_id(uri: str) -> str:
  """Create an Element identifier from URI."""
  return generate_deterministic_uuid7(uri, namespace="element")


def create_label_id(value: str, label_type: str, language: str) -> str:
  """Create a Label identifier from content."""
  content = f"{value}#{label_type}#{language}"
  return generate_deterministic_uuid7(content, namespace="label")


def create_taxonomy_id(uri: str) -> str:
  """Create a Taxonomy identifier from URI."""
  return generate_deterministic_uuid7(uri, namespace="taxonomy")


def create_reference_id(value: str, ref_type: str) -> str:
  """Create a Reference identifier."""
  content = f"{value}#{ref_type}"
  return generate_deterministic_uuid7(content, namespace="reference")


def create_report_id(uri: str) -> str:
  """
  Create a Report identifier from URI.

  Reports must have deterministic IDs based on URI for consistency
  across pipeline runs.
  """
  return generate_deterministic_uuid7(uri, namespace="report")


def create_fact_id(fact_uri: str) -> str:
  """
  Create a Fact identifier.

  Facts must have deterministic IDs based on URI for consistency
  across pipeline runs.
  """
  return generate_deterministic_uuid7(fact_uri, namespace="fact")


def create_entity_id(entity_uri: str) -> str:
  """Create an Entity identifier."""
  return generate_deterministic_uuid7(entity_uri, namespace="entity")


def create_period_id(period_uri: str) -> str:
  """Create a Period identifier."""
  return generate_deterministic_uuid7(period_uri, namespace="period")


def create_unit_id(unit_uri: str) -> str:
  """Create a Unit identifier."""
  return generate_deterministic_uuid7(unit_uri, namespace="unit")


def create_factset_id(factset_uri: str) -> str:
  """Create a FactSet identifier."""
  return generate_deterministic_uuid7(factset_uri, namespace="factset")


def create_dimension_id(dimension_uri: str) -> str:
  """Create a FactDimension identifier."""
  return generate_deterministic_uuid7(dimension_uri, namespace="dimension")


def create_structure_id(structure_uri: str) -> str:
  """Create a Structure identifier."""
  return generate_deterministic_uuid7(structure_uri, namespace="structure")
