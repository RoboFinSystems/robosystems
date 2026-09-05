"""
XBRL Naming Utilities

String conversion and naming helpers for the processor's DataFrames and
parquet files. Graph identifiers are minted by xbrlkit's projection
(``xbrlkit.serialize.lpg.graph_id``), which shares the platform's UUID5
namespace, so nothing here derives an id.
"""

import re

import pandas as pd


def camel_to_snake(name: str) -> str:
  """
  Convert PascalCase to snake_case.

  Examples:
    EntityReport -> entity_report
    LineItem -> line_item
    HTTPSConnection -> https_connection
  """
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
  """Name a table's parquet file after its exact schema name.

  No snake_case conversion — directories and files match the table names the
  graph uses ("Entity.parquet", "FACT_HAS_DIMENSION.parquet").
  """
  return f"{schema_name}.parquet"


def safe_concat(existing_df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
  """Concatenate two DataFrames, tolerating empties and dtype mismatches.

  Columns whose dtypes differ are widened to a common dtype first, which avoids
  the pandas FutureWarning about concatenating inconsistent dtypes.
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
