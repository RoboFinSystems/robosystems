"""
MCP streaming implementations with transparent aggregation for AI agents.

This module provides streaming capabilities that are transparently handled
by the Node.js MCP client, presenting a unified interface to AI agents.
"""

import asyncio
import json
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from typing import Any

from robosystems.logger import logger


async def stream_mcp_tool_execution(
  handler: Any,
  tool_name: str,
  arguments: dict[str, Any],
  strategy: str,
  chunk_size: int = 1000,
) -> AsyncGenerator[dict[str, Any]]:
  """
  Stream MCP tool execution with progress updates.

  This generator produces events that can be:
  - Sent as SSE to clients that support it
  - Aggregated by the Node.js client for AI agents
  - Converted to NDJSON for efficient transfer

  Args:
      handler: MCP handler instance
      tool_name: Name of the tool to execute
      arguments: Tool arguments
      strategy: Execution strategy being used
      chunk_size: Rows per chunk for query streaming

  Yields:
      Event dictionaries with type and data
  """
  start_time = datetime.now(UTC)

  try:
    # Send start event
    yield {
      "event": "start",
      "data": {
        "tool": tool_name,
        "strategy": strategy,
        "timestamp": start_time.isoformat(),
        "message": f"Starting {tool_name} execution",
      },
    }

    # Tool-specific streaming logic
    if tool_name in ["read-graph-cypher", "read-neo4j-cypher", "read-ladybug-cypher"]:
      # Stream query results
      async for event in stream_cypher_query(handler, arguments, chunk_size):
        yield event

    elif tool_name in ["get-graph-schema", "get-neo4j-schema", "get-ladybug-schema"]:
      # Stream schema in parts
      async for event in stream_schema_retrieval(handler, tool_name, arguments):
        yield event

    elif tool_name == "describe-graph-structure":
      # Stream description progressively
      async for event in stream_graph_description(handler, arguments):
        yield event

    else:
      # Generic tool execution with single result
      yield {
        "event": "progress",
        "data": {
          "tool": tool_name,
          "message": f"Executing {tool_name}...",
          "progress": 50,
        },
      }

      result = await handler.call_tool(tool_name, arguments)

      yield {
        "event": "result",
        "data": {
          "tool": tool_name,
          "result": result,
        },
      }

    # Send completion event
    execution_time = (datetime.now(UTC) - start_time).total_seconds()
    yield {
      "event": "complete",
      "data": {
        "tool": tool_name,
        "execution_time_seconds": execution_time,
        "timestamp": datetime.now(UTC).isoformat(),
      },
    }

  except TimeoutError:
    yield {
      "event": "error",
      "data": {
        "tool": tool_name,
        "error": "Execution timeout",
        "message": f"Tool {tool_name} execution timed out",
      },
    }
  except Exception as e:
    logger.error(f"Error streaming MCP tool {tool_name}: {e}")
    yield {
      "event": "error",
      "data": {
        "tool": tool_name,
        "error": str(e),
        "error_type": type(e).__name__,
      },
    }


async def stream_cypher_query(
  handler: Any,
  arguments: dict[str, Any],
  chunk_size: int = 1000,
) -> AsyncGenerator[dict[str, Any]]:
  """
  Stream Cypher query results in chunks.

  For AI agents, the Node.js client will aggregate these chunks
  into a complete result transparently.
  """
  query = arguments.get("query", "")
  parameters = arguments.get("parameters", {})

  # Check if handler supports streaming
  if hasattr(handler, "execute_query_streaming"):
    total_rows = 0
    chunk_count = 0
    all_columns = None

    async for chunk in handler.execute_query_streaming(
      query, parameters, chunk_size=chunk_size
    ):
      chunk_count += 1
      rows_in_chunk = len(chunk.get("data", []))
      total_rows += rows_in_chunk

      # Capture columns from first chunk
      if all_columns is None and "columns" in chunk:
        all_columns = chunk["columns"]

      # Send progress event
      yield {
        "event": "query_chunk",
        "data": {
          "chunk_number": chunk_count,
          "rows_in_chunk": rows_in_chunk,
          "total_rows_so_far": total_rows,
          "columns": all_columns,
          "data": chunk.get("data", []),
        },
      }

      # Send progress update every 5 chunks
      if chunk_count % 5 == 0:
        yield {
          "event": "progress",
          "data": {
            "message": f"Processed {total_rows} rows",
            "rows_processed": total_rows,
          },
        }

    # Send final summary
    yield {
      "event": "query_complete",
      "data": {
        "total_rows": total_rows,
        "total_chunks": chunk_count,
        "columns": all_columns,
      },
    }
  else:
    # Non-streaming execution
    yield {
      "event": "progress",
      "data": {
        "message": "Executing query...",
        "progress": 50,
      },
    }

    result = await handler.call_tool("read-graph-cypher", arguments)

    # Parse result if it's in the expected format
    if isinstance(result, dict) and "text" in result:
      try:
        parsed = json.loads(result["text"])
        yield {
          "event": "query_result",
          "data": {
            "result": parsed,
            "row_count": len(parsed) if isinstance(parsed, list) else 1,
          },
        }
      except json.JSONDecodeError:
        yield {
          "event": "query_result",
          "data": {
            "result": result["text"],
            "row_count": 0,
          },
        }
    else:
      yield {
        "event": "query_result",
        "data": {
          "result": result,
          "row_count": 0,
        },
      }


async def stream_schema_retrieval(
  handler: Any,
  tool_name: str,
  arguments: dict[str, Any],
) -> AsyncGenerator[dict[str, Any]]:
  """
  Stream schema information in digestible parts.

  This helps AI agents process large schemas incrementally.
  """
  # Indicate schema retrieval start
  yield {
    "event": "progress",
    "data": {
      "message": "Retrieving graph schema...",
      "progress": 10,
    },
  }

  # Get full schema
  schema_result = await handler.call_tool(tool_name, arguments)

  # Parse schema result
  if isinstance(schema_result, dict) and "text" in schema_result:
    try:
      schema = json.loads(schema_result["text"])
    except json.JSONDecodeError:
      schema = {"raw": schema_result["text"]}
  else:
    schema = schema_result

  # Stream node types
  if isinstance(schema, list):
    # Schema is a list of tables
    node_tables = [t for t in schema if t.get("category") == "Node Tables"]
    rel_tables = [t for t in schema if t.get("category") == "Relationship Tables"]

    if node_tables:
      yield {
        "event": "schema_nodes",
        "data": {
          "node_count": len(node_tables),
          "node_tables": node_tables,
        },
      }

    yield {
      "event": "progress",
      "data": {
        "message": "Processing relationships...",
        "progress": 50,
      },
    }

    if rel_tables:
      yield {
        "event": "schema_relationships",
        "data": {
          "relationship_count": len(rel_tables),
          "relationship_tables": rel_tables,
        },
      }
  else:
    # Schema in different format
    yield {
      "event": "schema_complete",
      "data": {
        "schema": schema,
      },
    }

  yield {
    "event": "progress",
    "data": {
      "message": "Schema retrieval complete",
      "progress": 100,
    },
  }


async def stream_graph_description(
  handler: Any,
  arguments: dict[str, Any],
) -> AsyncGenerator[dict[str, Any]]:
  """
  Stream graph description in parts for better AI agent comprehension.
  """
  yield {
    "event": "progress",
    "data": {
      "message": "Analyzing graph structure...",
      "progress": 25,
    },
  }

  # Get description
  result = await handler.call_tool("describe-graph-structure", arguments)

  # Parse result
  if isinstance(result, dict) and "text" in result:
    description = result["text"]
  else:
    description = str(result)

  # Split into sections if possible
  sections = description.split("\n\n")

  for i, section in enumerate(sections):
    if section.strip():
      progress = 25 + (50 * (i + 1) / len(sections))
      yield {
        "event": "description_section",
        "data": {
          "section_number": i + 1,
          "content": section,
          "progress": min(progress, 75),
        },
      }
      # Small delay for readability in real-time streams
      await asyncio.sleep(0.05)

  yield {
    "event": "progress",
    "data": {
      "message": "Description complete",
      "progress": 100,
    },
  }


def aggregate_streamed_results(events: list[dict[str, Any]]) -> dict[str, Any]:
  """
  Aggregate streamed events into a unified result for AI agents.

  This is used by the Node.js client to present a simple interface
  to AI agents while benefiting from streaming performance.

  Args:
      events: List of streaming events

  Returns:
      Aggregated result suitable for AI agent consumption
  """
  # Find the tool name
  tool_name = None
  for event in events:
    if event.get("event") == "start":
      tool_name = event["data"].get("tool")
      break

  # Check for errors
  for event in events:
    if event.get("event") == "error":
      return {
        "success": False,
        "error": event["data"].get("error", "Unknown error"),
        "tool": tool_name,
      }

  # Aggregate based on event types
  if any(e.get("event") == "query_chunk" for e in events):
    # Aggregate query chunks
    all_rows = []
    columns = None

    for event in events:
      if event.get("event") == "query_chunk":
        data = event["data"]
        if columns is None and "columns" in data:
          columns = data["columns"]
        all_rows.extend(data.get("data", []))

    return {
      "success": True,
      "tool": tool_name,
      "result": {
        "columns": columns,
        "data": all_rows,
        "row_count": len(all_rows),
      },
    }

  elif any(e.get("event") == "schema_nodes" for e in events):
    # Aggregate schema parts
    schema = {
      "node_tables": [],
      "relationship_tables": [],
    }

    for event in events:
      if event.get("event") == "schema_nodes":
        schema["node_tables"] = event["data"].get("node_tables", [])
      elif event.get("event") == "schema_relationships":
        schema["relationship_tables"] = event["data"].get("relationship_tables", [])

    return {
      "success": True,
      "tool": tool_name,
      "result": schema,
    }

  elif any(e.get("event") == "description_section" for e in events):
    # Aggregate description sections
    sections = []

    for event in events:
      if event.get("event") == "description_section":
        sections.append(event["data"]["content"])

    return {
      "success": True,
      "tool": tool_name,
      "result": "\n\n".join(sections),
    }

  else:
    # Look for a simple result event
    for event in events:
      if event.get("event") == "result":
        return {
          "success": True,
          "tool": tool_name,
          "result": event["data"].get("result"),
        }

    # No recognizable pattern
    return {
      "success": False,
      "error": "Unable to aggregate results",
      "tool": tool_name,
    }
