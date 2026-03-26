"""SEC Entity Sync Pipeline Configuration."""

from dagster import Config
from pydantic import Field


class SECEntitySyncConfig(Config):
  """Configuration for SEC entity sync pipeline.

  All three assets (extract, transform, load) receive the same config
  at runtime, following the QuickBooks pipeline pattern.
  """

  graph_id: str
  connection_id: str
  user_id: str
  cik: str  # 10-digit CIK number (zero-padded)
  form_types: list[str] = Field(
    default=["10-K", "10-Q", "20-F", "40-F"],
    description="Form types to include. Defaults to core financial statements.",
  )
  skip_enrichment: bool = Field(
    default=True,
    description="Skip semantic enrichment (embeddings, canonical concepts). "
    "Reduces processing time significantly. Enrichment is mainly "
    "useful for cross-company analysis in the shared repository.",
  )
