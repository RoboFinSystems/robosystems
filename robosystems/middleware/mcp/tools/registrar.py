"""MCP tool auto-generation from `OperationSpec` declarations.

The REST registrar (`middleware/extensions.py:OperationRegistrar`) mounts
write operations as FastAPI routes. This module mirrors that pipeline for
MCP: a `_RegistrarMCPTool` reads the same `OperationSpec` and exposes it
through the MCP wire format.

A tool generated this way:

  1. Derives its `name`/`description`/`inputSchema` from the spec's
     `name`, `summary`/`description`, and `request_model`.
  2. On execute:
     a. Fires the call-time extension gate (`require_graph_extension_mcp`)
        so writes are rejected on repository graphs and on graphs missing
        the extension.
     b. Builds the request model via `model_validate(arguments)` — Pydantic
        handles type coercion, nested models, date parsing, and field
        validation at the boundary.
     c. Calls the spec's command in an `extensions_session(graph_id)`,
        passing `created_by=<mcp user id>` when `requires_created_by=True`.
     d. Translates domain exceptions through the spec's `error_map` into
        MCP-native `{"error": code, "message": ...}` envelopes.

The result is a tool that's behaviorally identical to the REST handler
built from the same spec, without the FastAPI transport.

Tools generated here plug into `GraphMCPTools` via a dispatch dict so the
manager's `call_tool` can look them up by name without knowing about
registrar internals.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ValidationError

from robosystems.logger import logger
from robosystems.middleware.extensions import (
  ErrorMap,
  OperationRegistrar,
  OperationSpec,
)

from ._gate import MCPExtensionGateError, require_graph_extension_mcp
from .base_tool import BaseTool

if TYPE_CHECKING:
  from ..client import GraphMCPClient


# ── Input schema derivation ─────────────────────────────────────────────────


def derive_input_schema(request_model: type[BaseModel]) -> dict[str, Any]:
  """Convert a Pydantic request model to MCP `inputSchema` shape.

  Pydantic's `model_json_schema()` returns OpenAPI-flavored JSON Schema with
  `$defs` for nested models. MCP clients read plain JSON Schema, so the
  `$ref` pointers are inlined and Pydantic-specific metadata (`title` on
  leaf fields) is dropped for readability.
  """
  schema = request_model.model_json_schema(mode="serialization")
  defs = schema.pop("$defs", {}) or schema.pop("definitions", {}) or {}

  # Recursively resolve any `$ref` references against the popped `$defs`
  # block. After this walk the tree is self-contained.
  resolved = _inline_refs(schema, defs)

  # Drop top-level "title" (Pydantic adds the class name; MCP clients get
  # the tool name from the surrounding envelope).
  resolved.pop("title", None)
  resolved.setdefault("type", "object")
  # MCP tools are strict — reject arguments not in the schema. This matches
  # the REST handler's FastAPI behavior where unknown body fields are
  # silently discarded by Pydantic's `extra="ignore"` default but still
  # don't mutate the command's view of the world.
  resolved.setdefault("additionalProperties", False)
  return resolved


def _inline_refs(node: Any, defs: dict[str, Any]) -> Any:
  """Walk a JSON Schema tree, replacing `$ref` nodes with their definition."""
  if isinstance(node, dict):
    ref = node.get("$ref")
    if isinstance(ref, str) and ref.startswith("#/$defs/"):
      key = ref.rsplit("/", 1)[-1]
      target = defs.get(key)
      if target is not None:
        # Recurse into the referenced subtree too.
        inlined = _inline_refs(target, defs)
        # Merge any sibling keys on the $ref node (e.g. overrides) —
        # Pydantic sometimes emits `{"$ref": "...", "description": "..."}`.
        merged = dict(inlined) if isinstance(inlined, dict) else inlined
        for k, v in node.items():
          if k != "$ref":
            merged[k] = v
        return merged
      # Unknown ref — fall through and return the node as-is so the tree
      # still serializes (the schema will be imperfect but present).
    return {k: _inline_refs(v, defs) for k, v in node.items()}
  if isinstance(node, list):
    return [_inline_refs(item, defs) for item in node]
  return node


# ── Error translation ──────────────────────────────────────────────────────


def translate_error(exc: Exception, error_map: ErrorMap) -> dict[str, Any]:
  """Mirror `OperationRegistrar._raise_mapped` but return an MCP error dict.

  MCP tools surface errors as `{"error": code, "message": ...}` rather than
  raising `HTTPException`. The REST registrar's `error_map` maps domain
  exception classes to `(status_code, detail_factory)` tuples or a bare
  status code; the MCP translation ignores the HTTP status and uses the
  exception class name as the stable error code.
  """
  for exc_type, mapping in error_map.items():
    if isinstance(exc, exc_type):
      code = _exception_code(exc_type)
      if isinstance(mapping, int):
        return {"error": code, "message": str(exc)}
      _status_code, detail_factory = mapping
      return {"error": code, "message": detail_factory(exc)}
  # No mapping — surface the raw message under a generic code.
  return {"error": "command_failed", "message": str(exc)}


def _exception_code(exc_type: type[Exception]) -> str:
  """Derive a kebab-case error code from an exception class name."""
  name = exc_type.__name__
  if name.endswith("Error"):
    name = name[:-5]
  # Convert CamelCase → kebab-case.
  out: list[str] = []
  for i, ch in enumerate(name):
    if ch.isupper() and i > 0 and not name[i - 1].isupper():
      out.append("_")
    out.append(ch.lower())
  return "".join(out) or "error"


# ── _RegistrarMCPTool ──────────────────────────────────────────────────────


class _RegistrarMCPTool(BaseTool):
  """MCP tool auto-generated from an OperationSpec.

  Not exported directly; instantiated by `build_tools_for_extension` when
  GraphMCPTools wires up the dispatch dict for a given graph's enabled
  extensions. The tool owns a reference to its originating spec and the
  registrar (for session_factory and schema_missing_404) so it can invoke
  the same command the REST handler calls.
  """

  def __init__(
    self,
    client: GraphMCPClient,
    spec: OperationSpec,
    registrar: OperationRegistrar,
    meta_getter: Any | None = None,
  ) -> None:
    super().__init__(client)
    self.spec = spec
    self.registrar = registrar
    self.extension = registrar.extension
    # Optional callback that the manager uses to supply pre-loaded graph
    # metadata so tools don't each do their own DB lookup. Accepts no
    # arguments and returns `GraphExtensionContext | None`.
    self._meta_getter = meta_getter

  def get_tool_definition(self) -> dict[str, Any]:
    """Build the MCP tool definition.

    Name comes straight from the spec. Description prefers the explicit
    spec.description, falls back to the command's docstring (most commands
    already have terse one-line docstrings), then spec.summary.
    """
    description = (
      self.spec.description
      or (self.spec.command.__doc__ or "").strip()
      or self.spec.summary
    )
    return {
      "name": self.spec.name,
      "description": description,
      "inputSchema": derive_input_schema(self.spec.request_model),
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    """Run the spec's command end-to-end, with gate + validation + error map.

    Returns a plain dict (`.model_dump(mode="json")` of the command's
    response on success; `{"error": code, "message": ...}` on gate
    rejection or mapped domain failure). The manager's `call_tool` wraps
    the dict with `json.dumps(..., indent=2)` when `return_raw=False`.
    """
    self._log_tool_execution(self.spec.name, arguments)
    graph_id = self.client.graph_id

    # ── 1. Feature gate ─────────────────────────────────────────────────
    try:
      pre_loaded = self._meta_getter() if self._meta_getter else None
      require_graph_extension_mcp(self.extension, graph_id, meta=pre_loaded)
    except MCPExtensionGateError as exc:
      return {"error": exc.code, "message": exc.message}

    # ── 2. Parse request ────────────────────────────────────────────────
    try:
      body = self.spec.request_model.model_validate(arguments)
    except ValidationError as exc:
      return {
        "error": "invalid_arguments",
        "message": f"Arguments failed validation: {exc.errors()}",
      }

    # ── 3. Optional pre_validate hook ───────────────────────────────────
    if self.spec.pre_validate is not None:
      try:
        self.spec.pre_validate(body)
      except Exception as exc:
        # pre_validate raises HTTPException(400) on format errors (e.g. bad
        # period). Translate to MCP error without swallowing the message.
        detail = getattr(exc, "detail", None) or str(exc)
        return {"error": "invalid_arguments", "message": str(detail)}

    # ── 4. Resolve command, call it inside the session ─────────────────
    session_factory = self.registrar.session_factory
    command = self.spec.command
    created_by = self._resolve_created_by(graph_id)

    try:
      with session_factory(graph_id) as session:
        if self.spec.requires_created_by:
          result = command(session, body, created_by=created_by)
        else:
          result = command(session, body)
    except tuple(self.spec.error_map.keys()) as exc:
      return translate_error(exc, self.spec.error_map)
    except Exception as exc:
      # Last-resort: the ops layer raises ValueError / ProgrammingError on
      # schema-missing. Reuse the REST `schema_missing_404` helper's
      # message so MCP and REST agree on the surface string.
      logger.warning(
        "MCP tool %s failed unexpectedly: %s", self.spec.name, exc, exc_info=True
      )
      return {"error": "command_failed", "message": str(exc)}

    # ── 5. Normalize response ───────────────────────────────────────────
    return _dump_response(result)

  def _resolve_created_by(self, graph_id: str) -> str:
    """MCP clients attach the authenticated user's ID to the client where
    possible. Fall back to `mcp:{graph_id}` when the user context isn't
    threaded through — matches the hand-written tool convention.
    """
    user_id = getattr(self.client, "user_id", None)
    if user_id:
      return str(user_id)
    return f"mcp:{graph_id}"


def _dump_response(result: Any) -> Any:
  """Normalize a command response into a JSON-safe dict.

  Commands return Pydantic response models. `model_dump(mode="json")`
  handles date/datetime serialization, nested models, and enums. Non-model
  responses (rare; usually `list[Model]` or `dict`) are returned as-is.
  """
  if isinstance(result, BaseModel):
    return result.model_dump(mode="json")
  if isinstance(result, list) and result and isinstance(result[0], BaseModel):
    return [item.model_dump(mode="json") for item in result]
  return result


# ── Public factory ─────────────────────────────────────────────────────────


def build_tools_for_extension(
  extension: str,
  client: GraphMCPClient,
  meta_getter: Any | None = None,
) -> dict[str, _RegistrarMCPTool]:
  """Return `{tool_name: tool_instance}` for every OperationSpec on the
  given extension.

  Called from `GraphMCPTools.__init__` after the core tool layer is wired
  up. The caller merges the returned dict into its dispatch table.
  """
  tools: dict[str, _RegistrarMCPTool] = {}
  for registrar, spec in OperationRegistrar.specs_for_extension(extension):
    tool = _RegistrarMCPTool(
      client=client, spec=spec, registrar=registrar, meta_getter=meta_getter
    )
    tools[spec.name] = tool
  return tools


__all__ = [
  "_RegistrarMCPTool",
  "build_tools_for_extension",
  "derive_input_schema",
  "translate_error",
]
