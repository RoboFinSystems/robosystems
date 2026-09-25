"""MCP tools generated from `OperationSpec` declarations.

Mirrors the REST `OperationRegistrar`: the same spec drives the extension
gate, request validation, the command call and the `error_map`, so a tool
behaves like the REST handler built from that spec, without FastAPI.
"""

from __future__ import annotations

import importlib
import time
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ValidationError
from sqlalchemy.exc import DBAPIError, ProgrammingError

from robosystems.db.extensions import is_statement_timeout
from robosystems.logger import logger
from robosystems.middleware.extensions import (
  ErrorMap,
  OperationRegistrar,
  OperationSpec,
  is_schema_missing,
)
from robosystems.middleware.mcp.tools._errors import statement_timeout_answer
from robosystems.middleware.operations import (
  fingerprint_body,
  generate_operation_id,
  get_idempotency_cache,
  log_operation_audit,
  run_off_loop,
)
from robosystems.operations.extensions.staleness import mark_graph_stale

from ._gate import MCPExtensionGateError, require_graph_extension_mcp
from .base_tool import BaseTool

if TYPE_CHECKING:
  from ..client import GraphMCPClient


# ── Input schema derivation ─────────────────────────────────────────────────


# MCP has no Idempotency-Key, so a call reserves `<prefix>:<argument
# fingerprint>` per (user, graph, tool) in the idempotency cache instead.
_MCP_INFLIGHT_KEY = "mcp-inflight"


def derive_input_schema(request_model: type[BaseModel]) -> dict[str, Any]:
  """Convert a Pydantic request model to a self-contained MCP `inputSchema`.

  Anthropic's tool API rejects a top-level ``oneOf`` / ``anyOf`` /
  ``allOf``, which discriminated-union ``RootModel`` bodies emit, so those
  are flattened (``_flatten_top_level_union``). Nested unions pass through.
  Dispatch re-validates against ``request_model``, so flattening never
  loosens the wire contract.
  """
  schema = request_model.model_json_schema(mode="serialization")
  defs = schema.pop("$defs", {}) or schema.pop("definitions", {}) or {}

  resolved = _inline_refs(schema, defs)

  # Pydantic's class name; the tool name comes from the envelope.
  resolved.pop("title", None)

  if any(key in resolved for key in ("oneOf", "anyOf", "allOf")):
    return _flatten_top_level_union(resolved)

  resolved.setdefault("type", "object")
  resolved.setdefault("additionalProperties", False)
  return resolved


def _flatten_top_level_union(schema: dict[str, Any]) -> dict[str, Any]:
  """Collapse a top-level union into an object envelope the tool API accepts.

  - Some arm has a typed ``payload``: ``{<discriminator>: enum, payload:
    anyOf[<per-arm payload>]}``, both required, plus the arms' examples.
  - Every payload is a freeform dict: a permissive ``payload`` object and
    only the discriminator required.
  - No discriminator, or arms not shaped ``{<discriminator>, payload}``: a
    permissive envelope (a discriminator enum when a mapping exists).
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
  """Return ``(enum_values, [(discriminator_values, payload_schema)], examples)``,
  or ``None`` when any arm isn't shaped ``{<discriminator>, payload}``."""
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
      # OpenAPI-style ``{summary, description, value}``: keep the raw body.
      if isinstance(example, dict) and "value" in example:
        examples.append(example["value"])
      else:
        examples.append(example)
  enum_values = sorted(dict.fromkeys(enum_values))
  return enum_values, payload_options, examples


def _discriminator_values(disc_prop: Any) -> list[str]:
  """Pydantic emits ``const`` for a single ``Literal``, ``enum`` for several."""
  if not isinstance(disc_prop, dict):
    return []
  if "const" in disc_prop:
    return [str(disc_prop["const"])]
  enum = disc_prop.get("enum")
  if isinstance(enum, list):
    return [str(v) for v in enum]
  return []


def _is_typed_object(payload_schema: dict[str, Any]) -> bool:
  return bool(payload_schema.get("properties"))


def _payload_anyof_schema(
  payload_options: list[tuple[list[str], dict[str, Any]]], prop_name: str
) -> dict[str, Any]:
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
  if isinstance(node, dict):
    ref = node.get("$ref")
    if isinstance(ref, str) and ref.startswith("#/$defs/"):
      key = ref.rsplit("/", 1)[-1]
      target = defs.get(key)
      if target is not None:
        inlined = _inline_refs(target, defs)
        # Sibling keys override: `{"$ref": "...", "description": "..."}`.
        merged = dict(inlined) if isinstance(inlined, dict) else inlined
        for k, v in node.items():
          if k != "$ref":
            merged[k] = v
        return merged
      # Unknown ref: leave it; the schema is imperfect but still serializes.
    return {k: _inline_refs(v, defs) for k, v in node.items()}
  if isinstance(node, list):
    return [_inline_refs(item, defs) for item in node]
  return node


# ── Error translation ──────────────────────────────────────────────────────


def translate_error(exc: Exception, error_map: ErrorMap) -> dict[str, Any]:
  """Mirror `OperationRegistrar._raise_mapped` as an MCP error dict.

  The HTTP status is ignored; the exception class name becomes the code.
  """
  for exc_type, mapping in error_map.items():
    if isinstance(exc, exc_type):
      code = _exception_code(exc_type)
      if isinstance(mapping, int):
        return {"error": code, "message": str(exc)}
      _status_code, detail_factory = mapping
      return {"error": code, "message": detail_factory(exc)}
  return {"error": "command_failed", "message": str(exc)}


def _exception_code(exc_type: type[Exception]) -> str:
  name = exc_type.__name__
  if name.endswith("Error"):
    name = name[:-5]
  out: list[str] = []
  for i, ch in enumerate(name):
    if ch.isupper() and i > 0 and not name[i - 1].isupper():
      out.append("_")
    out.append(ch.lower())
  return "".join(out) or "error"


# ── _RegistrarMCPTool ──────────────────────────────────────────────────────


class _RegistrarMCPTool(BaseTool):
  """MCP tool generated from an OperationSpec by `build_tools_for_extension`."""

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
    # Zero-arg callback returning pre-loaded `GraphExtensionContext | None`,
    # so each tool skips its own metadata lookup.
    self._meta_getter = meta_getter

  def get_tool_definition(self) -> dict[str, Any]:
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
    """Return the dumped response, or `{"error": code, "message": ...}`."""
    self._log_tool_execution(self.spec.name, arguments)
    graph_id = self.client.graph_id

    # Write-role authorization (fail-closed) is enforced once, upstream, in
    # `validate_mcp_access`: registrar ops are writes, absent from
    # `READ_ONLY_MCP_TOOLS`. This layer has no `current_user` to recheck.

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
        # pre_validate raises HTTPException(400); keep its detail.
        detail = getattr(exc, "detail", None) or str(exc)
        return {"error": "invalid_arguments", "message": str(detail)}

    # ── 4. Resolve command, call it inside the session ─────────────────
    # Synchronous DB work runs off the event loop. With no Idempotency-Key
    # there is no replay; an identical call arriving while one is still
    # running is refused, and the claim is released when the run ends.
    created_by = self._resolve_created_by(graph_id)
    fingerprint = fingerprint_body(body)
    inflight_key = f"{_MCP_INFLIGHT_KEY}:{fingerprint}"
    cache = get_idempotency_cache()
    claimed = await cache.reserve(
      created_by, graph_id, self.spec.name, inflight_key, fingerprint
    )
    if not claimed:
      log_operation_audit(
        operation_name=self.spec.name,
        operation_id=generate_operation_id(),
        user_id=created_by,
        graph_id=graph_id,
        duration_ms=0.0,
        status="failed",
        error="duplicate call while an identical one is in progress",
        surface="mcp",
      )
      return {
        "error": "in_progress",
        "message": (
          f"An identical {self.spec.name} call is still in progress; wait for "
          "its result instead of repeating it."
        ),
      }

    start = time.monotonic()
    try:
      result = await run_off_loop(self._run_command, graph_id, body, created_by)
    finally:
      await cache.release(created_by, graph_id, self.spec.name, inflight_key)
    duration_ms = (time.monotonic() - start) * 1000

    # Same audit shape and stream as the REST operation routes.
    failed = isinstance(result, dict) and "error" in result
    log_operation_audit(
      operation_name=self.spec.name,
      operation_id=generate_operation_id(),
      user_id=created_by,
      graph_id=graph_id,
      duration_ms=duration_ms,
      status="failed" if failed else "completed",
      error=str(result.get("error")) if failed else None,
      surface="mcp",
    )
    return result

  def _run_command(self, graph_id: str, body: BaseModel, created_by: str) -> Any:
    session_factory = self.registrar.session_factory
    command = self.spec.command

    session_bound = False
    try:
      with session_factory(graph_id) as session:
        session_bound = True
        kwargs = {}
        if self.spec.requires_created_by:
          kwargs["created_by"] = created_by
        if self.spec.requires_graph_id:
          kwargs["graph_id"] = graph_id
        result = command(session, body, **kwargs)
    except tuple(self.spec.error_map.keys()) as exc:
      return translate_error(exc, self.spec.error_map)
    except ProgrammingError as exc:
      # Missing schema = not initialized, as on REST. Anything else gets a
      # fixed message: DBAPI error text carries SQL and bound parameters.
      if is_schema_missing(exc):
        return {
          "error": "not_initialized",
          "message": self.registrar.schema_missing_404().detail,
        }
      logger.warning(
        "MCP tool %s hit a database programming error", self.spec.name, exc_info=True
      )
      return {
        "error": "command_failed",
        "message": f"{self.spec.name} failed on a database error; see server logs",
      }
    except DBAPIError as exc:
      if is_statement_timeout(exc):
        return statement_timeout_answer(self.spec.name)
      raise
    except ValueError as exc:
      if not session_bound:
        # The session factory rejected the graph id — not a tenant-schema id.
        return {
          "error": "not_initialized",
          "message": self.registrar.schema_missing_404().detail,
        }
      # A domain refusal the error_map did not name (REST answers 422 with
      # the same message).
      logger.warning(
        "MCP tool %s: unmapped %s: %s", self.spec.name, type(exc).__name__, exc
      )
      return {"error": "invalid_request", "message": str(exc)}
    except Exception as exc:
      # Raw exception text can carry SQL, parameters and internal paths.
      logger.warning(
        "MCP tool %s failed unexpectedly: %s", self.spec.name, exc, exc_info=True
      )
      return {
        "error": "command_failed",
        "message": f"{self.spec.name} failed unexpectedly; see server logs",
      }

    if self.spec.mark_stale_reason is not None:
      mark_graph_stale(graph_id, self.spec.mark_stale_reason)

    # ── 5. Normalize response ───────────────────────────────────────────
    return _dump_response(result)

  def _resolve_created_by(self, graph_id: str) -> str:
    """The client's user ID, else `mcp:{graph_id}` as the hand-written tools do."""
    user_id = getattr(self.client, "user_id", None)
    if user_id:
      return str(user_id)
    return f"mcp:{graph_id}"


def _dump_response(result: Any) -> Any:
  if isinstance(result, BaseModel):
    return result.model_dump(mode="json")
  if isinstance(result, list) and result and isinstance(result[0], BaseModel):
    return [item.model_dump(mode="json") for item in result]
  return result


# ── Public factory ─────────────────────────────────────────────────────────


_OPERATION_MODULES = {
  "roboledger": "robosystems.routers.extensions.roboledger.operations",
  "roboinvestor": "robosystems.routers.extensions.roboinvestor.operations",
}


def _ensure_specs_registered(extension: str) -> None:
  # Specs register when their router module is imported. The API imports them
  # at startup; the worker and Dagster never do, so import them here.
  module = _OPERATION_MODULES.get(extension)
  if module:
    importlib.import_module(module)


def build_tools_for_extension(
  extension: str,
  client: GraphMCPClient,
  meta_getter: Any | None = None,
) -> dict[str, _RegistrarMCPTool]:
  _ensure_specs_registered(extension)
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
