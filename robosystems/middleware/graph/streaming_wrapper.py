"""Streaming query support for Graph API clients.

Lets callers process large result sets in chunks instead of materializing
every row.
"""

import time
from collections.abc import AsyncIterator
from typing import Any

from robosystems.logger import logger


class StreamingRepositoryWrapper:
  """Wrapper that adds streaming support to Graph API client repositories.

  This wrapper checks if the underlying client supports streaming and
  provides the execute_query_streaming method for the repository layer.
  """

  def __init__(self, client):
    """Initialize the streaming wrapper."""
    self.client = client

  async def execute_query_streaming(
    self, cypher: str, params: dict[str, Any] | None = None, chunk_size: int = 1000
  ) -> AsyncIterator[dict[str, Any]]:
    """Execute a query and stream results in chunks."""
    start_time = time.time()

    if hasattr(self.client, "query"):
      try:
        graph_id = getattr(self.client, "graph_id", "unknown")

        # Call the query method with streaming=True
        # This now returns a true async generator from the graph database instance
        stream_generator = await self.client.query(
          cypher=cypher, graph_id=graph_id, parameters=params, streaming=True
        )

        # Pass through chunks from graph database instance without buffering
        if hasattr(stream_generator, "__aiter__"):
          chunk_count = 0
          total_rows = 0

          async for chunk in stream_generator:
            chunk_count += 1

            # The graph database instance already provides properly formatted chunks
            # Just pass them through with minimal processing
            if isinstance(chunk, dict):
              total_rows = chunk.get("total_rows_sent", total_rows)

              if "chunk_index" not in chunk:
                chunk["chunk_index"] = chunk_count - 1

              if chunk.get("is_last_chunk"):
                if "execution_time_ms" not in chunk:
                  chunk["execution_time_ms"] = (time.time() - start_time) * 1000
                logger.info(
                  f"Completed streaming {total_rows} rows in {chunk_count} chunks "
                  f"from graph database instance for graph {graph_id}"
                )

            # Yield chunk immediately without buffering
            yield chunk

            # Periodic progress logging
            if chunk_count % 10 == 0:
              logger.debug(
                f"Streaming progress: {total_rows} rows in {chunk_count} chunks"
              )
        else:
          # Fallback for non-streaming responses
          logger.debug("Client returned non-streaming result, converting to chunks")
          async for chunk in self._convert_to_chunks(
            stream_generator, chunk_size, start_time
          ):
            yield chunk

      except Exception as e:
        logger.error(f"Streaming query failed: {e}")
        # Yield error chunk
        yield {
          "error": str(e),
          "error_type": type(e).__name__,
          "chunk_index": 0,
          "is_last_chunk": True,
          "row_count": 0,
          "total_rows_sent": 0,
          "execution_time_ms": (time.time() - start_time) * 1000,
        }
    else:
      # Client doesn't support query method, try execute_query
      logger.debug("Client doesn't support streaming, falling back to chunked response")

      if hasattr(self.client, "execute_query"):
        # Execute normally and convert to chunks
        if callable(self.client.execute_query):
          result = await self.client.execute_query(cypher, params)
          async for chunk in self._convert_to_chunks(result, chunk_size, start_time):
            yield chunk
      else:
        raise AttributeError("Client has no query or execute_query method")

  async def _convert_to_chunks(
    self, result: Any, chunk_size: int, start_time: float
  ) -> AsyncIterator[dict[str, Any]]:
    """Convert a regular query result to streaming chunks."""
    # Handle different result formats
    if isinstance(result, dict) and "data" in result:
      # Result is a response object
      data = result.get("data", [])
      columns = result.get("columns", [])
    elif isinstance(result, list):
      # Result is raw data
      data = result
      columns = list(data[0].keys()) if data else []
    else:
      # Unknown format
      logger.warning(f"Unknown result format: {type(result)}")
      data = []
      columns = []

    total_rows = len(data)

    # Yield chunks
    for i in range(0, total_rows, chunk_size):
      chunk_data = data[i : i + chunk_size]
      is_last = i + chunk_size >= total_rows

      chunk = {
        "chunk_index": i // chunk_size,
        "data": chunk_data,
        "columns": columns if i == 0 else [],  # Only send columns in first chunk
        "is_last_chunk": is_last,
        "row_count": len(chunk_data),
        "total_rows_sent": min(i + chunk_size, total_rows),
      }

      # Add execution time to last chunk
      if is_last:
        chunk["execution_time_ms"] = (time.time() - start_time) * 1000

      yield chunk


def add_streaming_support(client):
  """Add streaming support to a Graph API client.

  This function wraps the client with streaming capabilities if it doesn't
  already have them.
  """
  if hasattr(client, "execute_query_streaming"):
    return client

  wrapper = StreamingRepositoryWrapper(client)

  client.execute_query_streaming = wrapper.execute_query_streaming

  return client
