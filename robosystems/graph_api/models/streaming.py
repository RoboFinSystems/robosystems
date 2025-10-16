"""
Streaming response models for large result sets.
"""

from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field


class StreamingQueryChunk(BaseModel):
  """A chunk of query results for streaming responses."""

  chunk_index: int = Field(..., description="Index of this chunk in the stream")
  data: List[Dict[str, Any]] = Field(..., description="Query result rows in this chunk")
  columns: List[str] = Field(..., description="Column names (sent in first chunk)")
  is_last_chunk: bool = Field(..., description="Whether this is the last chunk")
  row_count: int = Field(..., description="Number of rows in this chunk")
  total_rows_sent: int = Field(..., description="Total rows sent so far")
  execution_time_ms: Optional[float] = Field(
    None, description="Query execution time (sent in last chunk)"
  )
