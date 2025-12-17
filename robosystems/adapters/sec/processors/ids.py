"""
XBRL ID and Naming Utilities

Deterministic UUID generation for XBRL graph entities using UUIDv7
for optimal database performance and cross-pipeline consistency.
Also includes string conversion and naming convention helpers.
"""

import re

import pandas as pd

from robosystems.utils.uuid import generate_deterministic_uuid7


# =============================================================================
# ID Generation Functions
# =============================================================================


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


# =============================================================================
# Naming Utilities
# =============================================================================


def camel_to_snake(name: str) -> str:
  """
  Convert PascalCase to snake_case.

  Examples:
    EntityReport -> entity_report
    FactDimension -> fact_dimension
    HTTPSConnection -> https_connection
  """
  # Insert underscore before uppercase letters (except first)
  s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
  return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def make_plural(word: str) -> str:
  """
  Convert word to plural form following simple English rules.

  Examples:
    entity -> entities
    fact -> facts
    taxonomy -> taxonomies
    box -> boxes
  """
  if word.endswith("y"):
    return word[:-1] + "ies"
  elif word.endswith(("s", "x", "z", "ch", "sh")):
    return word + "es"
  else:
    return word + "s"


def convert_schema_name_to_filename(schema_name: str) -> str:
  """
  Convert schema name to appropriate filename using exact table names.

  Uses exact schema names for file naming - no conversion to snake_case.
  This ensures directories and files match exact table names like Entity,
  FactDimension, etc.

  Args:
    schema_name: Schema name like "Entity", "FACT_HAS_DIMENSION"

  Returns:
    Filename like "Entity.parquet", "FACT_HAS_DIMENSION.parquet"
  """
  return f"{schema_name}.parquet"


def safe_concat(existing_df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
  """
  Safely concatenate DataFrames, handling empty DataFrames and dtype mismatches.

  Fixes pandas FutureWarning about dtype inconsistencies by explicitly
  handling dtypes when concatenating.

  Args:
    existing_df: Existing DataFrame
    new_df: New DataFrame to append

  Returns:
    Concatenated DataFrame
  """
  if new_df.empty:
    return existing_df
  if existing_df.empty:
    return new_df.copy()

  # Ensure consistent dtypes between DataFrames before concatenation
  for col in new_df.columns:
    if col in existing_df.columns:
      # Convert to common dtype if they differ
      if existing_df[col].dtype != new_df[col].dtype:
        # Use object dtype as fallback for mixed types
        common_dtype = (
          "object"
          if existing_df[col].dtype == "object" or new_df[col].dtype == "object"
          else existing_df[col].dtype
        )
        existing_df[col] = existing_df[col].astype(common_dtype)
        new_df[col] = new_df[col].astype(common_dtype)

  return pd.concat([existing_df, new_df], ignore_index=True, sort=False)
