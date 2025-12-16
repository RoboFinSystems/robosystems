"""
XBRL Naming Utilities

String conversion and naming convention helpers for XBRL processing.
"""

import re
import pandas as pd


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
