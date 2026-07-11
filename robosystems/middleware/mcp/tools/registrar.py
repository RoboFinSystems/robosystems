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
from robosystems.operations.extensions.staleness import mark_graph_stale

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

  Discriminated-union ``RootModel`` request bodies (e.g.
  ``CreateInformationBlockRequest``) emit a schema with top-level
  ``oneOf`` / ``anyOf``. Anthropic's tool-call API rejects top-level
  unions in tool ``input_schema``; we flatten those into an object
  envelope here — ``{<discriminator>: enum, payload: …}``. When the
  union's arms carry **typed** payloads (the schedule arm of
  ``CreateInformationBlockRequest`` does), the flattener preserves each
  arm's payload schema under a nested ``payload.anyOf`` and hoists the
  arms' ``examples`` to the envelope, so a codebase-blind MCP client can
  see the real field names instead of an opaque blob. Nested unions are
  safe here — Pydantic ``Optional[...]`` fields already emit nested
  ``anyOf`` that flow through unchanged; only the *top level* is
  forbidden. Strict validation still happens at dispatch time (the
  registrar re-parses the payload through the original
  ``request_model``), so the wire contract is unchanged. See
  ``_flatten_top_level_union`` below for the envelope shape.
  """
  schema = request_model.model_json_schema(mode="serialization")
  defs = schema.pop("$defs", {}) or schema.pop("definitions", {}) or {}

  # Recursively resolve any `$ref` references against the popped `$defs`
  # block. After this walk the tree is self-contained.
  resolved = _inline_refs(schema, defs)

  # Drop top-level "title" (Pydantic adds the class name; MCP clients get
  # the tool name from the surrounding envelope).
  resolved.pop("title", None)

  # Anthropic's tool-call API forbids ``oneOf`` / ``anyOf`` / ``allOf`` at
  # the top level of a tool's ``input_schema``. Pydantic discriminated-union
  # ``RootModel`` bodies emit exactly that shape, so flatten before
  # returning. The fallback envelope keeps the discriminator visible while
  # leaving the payload open — dispatch-time Pydantic validation enforces
  # the real contract.
  if any(key in resolved for key in ("oneOf", "anyOf", "allOf")):
    return _flatten_top_level_union(resolved)

  resolved.setdefault("type", "object")
  # MCP tools are strict — reject arguments not in the schema. This matches
  # the REST handler's FastAPI behavior where unknown body fields are
  # silently discarded by Pydantic's `extra="ignore"` default but still
  # don't mutate the command's view of the world.
  resolved.setdefault("additionalProperties", False)
  return resolved


def _flatten_top_level_union(schema: dict[str, Any]) -> dict[str, Any]:
  """Collapse a top-level ``oneOf`` / ``anyOf`` / ``allOf`` schema into
  an object envelope that Anthropic's tool API accepts.

  The output shape:

  - **Typed arms** (at least one arm exposes a structured ``payload``):
    ``{type: object, properties: {<discriminator>: {type: string,
    enum: [...]}, payload: {anyOf: [<per-arm payload schema>, …]}},
    required: [<discriminator>, payload], examples: [...],
    additionalProperties: True}``. The per-arm payload schemas carry the
    real field names so a codebase-blind MCP client can construct a valid
    call; ``examples`` holds the arms' copy-pasteable request bodies.

  - **Untyped arms** (every arm's payload is a freeform ``dict``):
    ``{type: object, properties: {<discriminator>: {type: string,
    enum: [...]}, payload: {type: object, additionalProperties: True}},
    required: [<discriminator>], additionalProperties: True}`` — the
    permissive shape, since there is nothing structured to advertise.

  - When no discriminator is declared, or the arms don't follow the
    standard ``{<discriminator>, payload}`` shape: a permissive object
    envelope (``additionalProperties: True``) with the original
    ``description`` preserved.

  Dispatch-time Pydantic validation still enforces the per-arm shape,
  so callers passing an invalid payload get a clean ValidationError at
  the boundary — same behavior as a non-union request model.
  """
  description = schema.get("description")
  flattened: dict[str, Any] = {
    "type": "object",
    "additionalProperties": True,
  }
  if description:
    flattened["description"] = description

  discriminator = schema.get("discriminator")
  if not isinstance(discriminator, dict):
    return flattened
  prop_name = discriminator.get("propertyName")
  if not prop_name:
    return flattened

  arms = schema.get("oneOf") or schema.get("anyOf") or []
  parsed = _parse_union_arms(arms, prop_name)

  if parsed is None:
    # Arms don't follow the standard ``{<discriminator>, payload}`` shape.
    # Fall back to a discriminator-only envelope using the mapping keys.
    mapping = discriminator.get("mapping") or {}
    enum_values = sorted(mapping.keys()) if mapping else None
    flattened["properties"] = {
      prop_name: _discriminator_schema(enum_values),
    }
    if enum_values:
      flattened["required"] = [prop_name]
    return flattened

  enum_values, payload_options, examples = parsed
  flattened["properties"] = {prop_name: _discriminator_schema(enum_values)}

  has_typed_payload = any(_is_typed_object(opt[1]) for opt in payload_options)
  if has_typed_payload:
    flattened["properties"]["payload"] = _payload_anyof_schema(
      payload_options, prop_name
    )
    flattened["required"] = [prop_name, "payload"]
    if examples:
      flattened["examples"] = examples
  else:
    # Nothing structured to advertise — keep the permissive payload.
    flattened["properties"]["payload"] = {
      "type": "object",
      "description": (
        "Variant payload. Shape depends on the discriminator value; "
        "validated against the corresponding Pydantic arm at dispatch time."
      ),
      "additionalProperties": True,
    }
    flattened["required"] = [prop_name]
  return flattened


def _discriminator_schema(enum_values: list[str] | None) -> dict[str, Any]:
  """Build the JSON-schema fragment for the discriminator property."""
  out: dict[str, Any] = {
    "type": "string",
    "description": (
      f"Discriminator — selects the variant. Allowed values: {', '.join(enum_values)}."
      if enum_values
      else "Discriminator value selecting the request variant."
    ),
  }
  if enum_values:
    out["enum"] = enum_values
  return out


def _parse_union_arms(
  arms: list[Any], prop_name: str
) -> tuple[list[str], list[tuple[list[str], dict[str, Any]]], list[Any]] | None:
  """Correlate each union arm to its discriminator value(s) + payload schema.

  Returns ``(enum_values, payload_options, examples)`` where
  ``payload_options`` is ``[(discriminator_values, payload_schema), …]``,
  or ``None`` when any arm doesn't follow the standard
  ``{<discriminator>, payload}`` object shape (signalling the caller to
  fall back to a discriminator-only envelope).
  """
  if not arms:
    return None
  enum_values: list[str] = []
  payload_options: list[tuple[list[str], dict[str, Any]]] = []
  examples: list[Any] = []
  for arm in arms:
    if not isinstance(arm, dict):
      return None
    props = arm.get("properties")
    if not isinstance(props, dict):
      return None
    values = _discriminator_values(props.get(prop_name))
    payload_schema = props.get("payload")
    if not values or not isinstance(payload_schema, dict):
      return None
    enum_values.extend(values)
    payload_options.append((values, payload_schema))
    for example in arm.get("examples") or []:
      # Pydantic's ``json_schema_extra`` examples are OpenAPI-style
      # ``{summary, description, value}`` objects; surface the raw value
      # so the example is a copy-pasteable request body.
      if isinstance(example, dict) and "value" in example:
        examples.append(example["value"])
      else:
        examples.append(example)
  # Dedupe + stabilise the enum for deterministic schemas.
  enum_values = sorted(dict.fromkeys(enum_values))
  return enum_values, payload_options, examples


def _discriminator_values(disc_prop: Any) -> list[str]:
  """Extract the discriminator value(s) an arm matches from its property
  schema — Pydantic emits ``{"const": "x"}`` for a single ``Literal`` and
  ``{"enum": [...]}`` for a multi-value one.
  """
  if not isinstance(disc_prop, dict):
    return []
  if "const" in disc_prop:
    return [str(disc_prop["const"])]
  enum = disc_prop.get("enum")
  if isinstance(enum, list):
    return [str(v) for v in enum]
  return []


def _is_typed_object(payload_schema: dict[str, Any]) -> bool:
  """True when the payload advertises structured fields worth exposing
  (an untyped ``dict`` payload renders with no ``properties``)."""
  return bool(payload_schema.get("properties"))


def _payload_anyof_schema(
  payload_options: list[tuple[list[str], dict[str, Any]]], prop_name: str
) -> dict[str, Any]:
  """Build the ``payload`` schema as an ``anyOf`` over each arm's payload,
  each option annotated with the discriminator value(s) it applies to.
  """
  options: list[dict[str, Any]] = []
  for values, payload_schema in payload_options:
    option = dict(payload_schema)
    quoted = ", ".join(f"'{v}'" for v in values)
    option["title"] = f"payload when {prop_name} is {quoted}"
    options.append(option)
  return {
    "description": (
      f"Variant payload — its shape depends on `{prop_name}`. Use the "
      f"anyOf option whose title matches your `{prop_name}`. The example "
      "request bodies show complete, valid payloads."
    ),
    "anyOf": options,
  }


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
  """Derive a snake_case error code from an exception class name."""
  name = exc_type.__name__
  if name.endswith("Error"):
    name = name[:-5]
  # Convert CamelCase → snake_case.
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

    # Write-role authorization (member/admin, fail-closed) is enforced upstream
    # at the MCP dispatch boundary in `execute.py`'s `validate_mcp_access`:
    # every registrar op is a write, so it is absent from `READ_ONLY_MCP_TOOLS`
    # and classified as a write there. This layer has no `current_user`, so the
    # check cannot be repeated here — it is a single enforcement point by design.

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

    if self.spec.mark_stale_reason is not None:
      mark_graph_stale(graph_id, self.spec.mark_stale_reason)

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
