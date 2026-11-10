"""A minimal MCP client for the graph-scoped endpoint: JSON-RPC over HTTP.

The harness uses one instance to plant and score, and the model's tool calls go
through another call path on the same client, so both see the public surface.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

import httpx

_LOCAL_HOSTS = frozenset({"localhost", "127.0.0.1", "host.docker.internal"})
_PROTOCOL_VERSION = "2025-06-18"


class McpError(RuntimeError):
  pass


@dataclass(frozen=True)
class ToolDef:
  name: str
  description: str
  input_schema: dict[str, Any]

  @classmethod
  def from_mcp(cls, tool: dict[str, Any]) -> ToolDef:
    """A tool with no usable schema is a harness defect, never a default."""
    schema = tool.get("inputSchema")
    if not isinstance(schema, dict) or "properties" not in schema:
      raise McpError(f"tool {tool.get('name')!r} has no inputSchema.properties")
    return cls(tool["name"], tool.get("description", ""), schema)

  @property
  def chars(self) -> int:
    return len(json.dumps(self.as_dict()))

  def as_dict(self) -> dict[str, Any]:
    return {
      "name": self.name,
      "description": self.description,
      "input_schema": self.input_schema,
    }


class McpClient:
  def __init__(self, base_url: str, graph_id: str, api_key: str) -> None:
    host = urlparse(base_url).hostname or ""
    if host not in _LOCAL_HOSTS:
      raise McpError(f"{base_url} is not a local stack; the benchmark writes to it")
    self.graph_id = graph_id
    self.base_url = base_url.rstrip("/")
    self._url = f"{self.base_url}/v1/graphs/{graph_id}/mcp"
    self._http = httpx.Client(
      timeout=300,
      headers={
        "X-API-Key": api_key,
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
      },
    )
    self._next_id = 0

  def _rpc(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
    self._next_id += 1
    body = {"jsonrpc": "2.0", "id": self._next_id, "method": method, "params": params}
    response = self._http.post(self._url, json=body)
    response.raise_for_status()
    message = _last_message(response.text)
    if "error" in message:
      raise McpError(f"{method}: {json.dumps(message['error'])}")
    return message["result"]

  def instructions(self) -> str:
    result = self._rpc(
      "initialize",
      {
        "protocolVersion": _PROTOCOL_VERSION,
        "capabilities": {},
        "clientInfo": {"name": "ledger-benchmark", "version": "0"},
      },
    )
    return result.get("instructions") or ""

  def list_tools(self) -> list[ToolDef]:
    tools = self._rpc("tools/list", {})["tools"]
    return sorted((ToolDef.from_mcp(t) for t in tools), key=lambda t: t.name)

  def call(self, name: str, args: dict[str, Any]) -> tuple[str, bool]:
    """Returns the tool's text and whether the server marked it an error."""
    try:
      result = self._rpc("tools/call", {"name": name, "arguments": args})
    except McpError as exc:
      return json.dumps({"error": str(exc)}), True
    text = "\n".join(c.get("text", "") for c in result.get("content", []))
    return text, bool(result.get("isError"))

  def read(self, name: str, args: dict[str, Any]) -> Any:
    """A harness read or write that must succeed, parsed as JSON."""
    text, is_error = self.call(name, args)
    if is_error:
      raise McpError(f"{name} failed: {text[:400]}")
    return json.loads(text)

  def graphql(self, query: str) -> dict[str, Any]:
    result = self.read("query-graphql", {"query": query})
    if result.get("errors"):
      raise McpError(f"graphql: {json.dumps(result['errors'])[:400]}")
    return result["data"]


def _last_message(body: str) -> dict[str, Any]:
  """The endpoint answers plain JSON, or an event stream whose last data line is it."""
  if body.lstrip().startswith("{"):
    return json.loads(body)
  message: dict[str, Any] | None = None
  for line in body.splitlines():
    if line.startswith("data:"):
      candidate = json.loads(line[5:])
      if "result" in candidate or "error" in candidate:
        message = candidate
  if message is None:
    raise McpError(f"no JSON-RPC result in response: {body[:200]}")
  return message
