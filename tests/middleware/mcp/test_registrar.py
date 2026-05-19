"""Tests for the MCP registrar infrastructure.

Covers the three primitives every registrar-generated tool depends on:

1. `derive_input_schema` — Pydantic `model_json_schema()` → MCP `inputSchema`
   (with `$ref` inlining, title stripping, strict `additionalProperties`).
2. `translate_error` (+ `_exception_code`) — `error_map` → MCP error envelope
   shape `{"error": <kebab-code>, "message": ...}`.
3. `_RegistrarMCPTool.execute` — the full call pipeline: gate → validation
   → `pre_validate` → `command(session, body[, created_by])` → model_dump
   response OR mapped error.

These run on every write tool generated via `build_tools_for_extension`
(18+ across roboledger + roboinvestor), so a regression here affects the
entire registrar surface.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from pydantic import BaseModel, Field

from robosystems.middleware.extensions import ErrorMap, OperationSpec
from robosystems.middleware.mcp.tools._gate import MCPExtensionGateError
from robosystems.middleware.mcp.tools.registrar import (
  _exception_code,
  _RegistrarMCPTool,
  build_tools_for_extension,
  derive_input_schema,
  translate_error,
)

# ── Schema derivation ─────────────────────────────────────────────────────


class _Leaf(BaseModel):
  code: str
  weight: float | None = None


class _Outer(BaseModel):
  """Docstring is used as MCP tool description fallback."""

  name: str = Field(..., description="Item name")
  leaf: _Leaf


class TestDeriveInputSchema:
  def test_inlines_refs_and_strips_title(self) -> None:
    schema = derive_input_schema(_Outer)

    # Top-level plumbing
    assert schema["type"] == "object"
    assert schema["additionalProperties"] is False
    assert "title" not in schema

    # $ref to _Leaf was inlined — no `$defs` block, nested object carries
    # its own properties directly.
    assert "$defs" not in schema
    leaf_schema = schema["properties"]["leaf"]
    assert leaf_schema["type"] == "object"
    assert "code" in leaf_schema["properties"]

  def test_required_list_preserved(self) -> None:
    schema = derive_input_schema(_Outer)
    assert set(schema["required"]) == {"name", "leaf"}

  def test_additional_properties_not_overridden(self) -> None:
    """If Pydantic emits additionalProperties=true for a `dict` field,
    don't stomp on it — only set the top-level default to False."""

    class WithDict(BaseModel):
      meta: dict[str, Any] = Field(default_factory=dict)

    schema = derive_input_schema(WithDict)
    assert schema["additionalProperties"] is False

  def test_top_level_discriminated_union_is_flattened(self) -> None:
    """Pydantic discriminated-union ``RootModel`` request bodies (e.g.
    ``CreateInformationBlockRequest``) emit a schema with top-level
    ``oneOf``. Anthropic's tool API rejects top-level oneOf/anyOf/allOf,
    so the registrar must flatten the schema into a permissive object
    envelope. Regression test for the 2026-05-19 MCP-tool registration
    failure.
    """
    from typing import Annotated, Literal

    from pydantic import Discriminator, RootModel

    class _AArm(BaseModel):
      kind: Literal["a"]
      payload: dict[str, Any] = Field(default_factory=dict)

    class _BArm(BaseModel):
      kind: Literal["b"]
      payload: dict[str, Any] = Field(default_factory=dict)

    _Arms = Annotated[_AArm | _BArm, Discriminator("kind")]

    class _DiscriminatedRequest(RootModel[_Arms]):
      """A discriminated-union RootModel."""

    schema = derive_input_schema(_DiscriminatedRequest)

    # No top-level union — Anthropic API constraint.
    assert "oneOf" not in schema
    assert "anyOf" not in schema
    assert "allOf" not in schema

    # Flattened envelope: object with discriminator as enum + payload.
    assert schema["type"] == "object"
    assert "kind" in schema["properties"]
    assert schema["properties"]["kind"]["type"] == "string"
    assert set(schema["properties"]["kind"]["enum"]) == {"a", "b"}
    assert schema["properties"]["payload"]["type"] == "object"
    assert schema["required"] == ["kind"]
    # Permissive payload — strict validation happens at dispatch time.
    assert schema["additionalProperties"] is True

  def test_create_information_block_schema_has_no_top_level_union(self) -> None:
    """Regression check for the production case that triggered the
    Anthropic API rejection — ``CreateInformationBlockRequest`` is a
    3-arm discriminated union after the rollforward block-type landed.
    """
    from robosystems.models.api.information_block import (
      CreateInformationBlockRequest,
    )

    schema = derive_input_schema(CreateInformationBlockRequest)
    assert "oneOf" not in schema
    assert "anyOf" not in schema
    assert "allOf" not in schema
    assert schema["type"] == "object"
    # block_type enum covers every registered block type
    assert "rollforward" in schema["properties"]["block_type"]["enum"]
    assert "schedule" in schema["properties"]["block_type"]["enum"]


# ── Error translation ─────────────────────────────────────────────────────


class DemoNotFoundError(Exception):
  pass


class DemoBusyError(Exception):
  def __init__(self, count: int) -> None:
    super().__init__(f"{count} in-flight")
    self.count = count


class DemoUnmappedError(Exception):
  pass


class TestTranslateError:
  def test_bare_int_mapping_uses_str_exc(self) -> None:
    error_map: ErrorMap = {DemoNotFoundError: 404}
    result = translate_error(DemoNotFoundError("element x not found"), error_map)
    # Class name `DemoNotFoundError` → strip "Error" → `DemoNotFound`
    # → kebab-case → `demo_not_found`
    assert result == {"error": "demo_not_found", "message": "element x not found"}

  def test_tuple_mapping_uses_detail_factory(self) -> None:
    error_map: ErrorMap = {
      DemoBusyError: (
        409,
        lambda e: f"{e.count} active — dispose first",  # type: ignore[attr-defined]
      ),
    }
    result = translate_error(DemoBusyError(3), error_map)
    assert result["error"] == "demo_busy"
    assert result["message"] == "3 active — dispose first"

  def test_unmapped_exception_falls_back(self) -> None:
    result = translate_error(DemoUnmappedError("oops"), {})
    assert result == {"error": "command_failed", "message": "oops"}

  def test_subclass_match_via_isinstance(self) -> None:
    """Subclasses of mapped errors should resolve to the parent's entry."""

    class _ChildNotFoundError(DemoNotFoundError):
      pass

    error_map: ErrorMap = {DemoNotFoundError: 404}
    result = translate_error(_ChildNotFoundError("missing"), error_map)
    # Resolves to the parent's entry → `demo_not_found` (from DemoNotFoundError).
    assert result["error"] == "demo_not_found"


class TestExceptionCode:
  @pytest.mark.parametrize(
    ("cls_name", "expected"),
    [
      ("NotFoundError", "not_found"),
      ("ElementNotFoundError", "element_not_found"),
      ("IOError", "io"),  # acronym run collapses to one token
      ("BusyError", "busy"),
      ("JSONParseError", "jsonparse"),  # acronym+word collapses; OK in practice
    ],
  )
  def test_kebab_conversion(self, cls_name: str, expected: str) -> None:
    cls = type(cls_name, (Exception,), {})
    assert _exception_code(cls) == expected


# ── _RegistrarMCPTool — full pipeline ─────────────────────────────────────


class _Request(BaseModel):
  """Demo request body used by the test spec below."""

  id: str = Field(..., description="Identifier")
  value: int = Field(0, description="Count")


class _Response(BaseModel):
  id: str
  echoed: int


def _noop_command(session, body: _Request, created_by: str) -> _Response:  # type: ignore[no-untyped-def]
  return _Response(id=body.id, echoed=body.value)


def _spec(**overrides) -> OperationSpec:
  defaults: dict[str, Any] = {
    "name": "demo-op",
    "summary": "Demo Op",
    "command": _noop_command,
    "request_model": _Request,
    "error_map": {DemoNotFoundError: 404},
  }
  defaults.update(overrides)
  return OperationSpec(**defaults)


def _registrar_stub(extension: str = "roboledger") -> MagicMock:
  """Minimal registrar with the fields `_RegistrarMCPTool` reads."""
  reg = MagicMock()
  reg.extension = extension
  reg.session_factory = lambda gid: _session_cm()
  return reg


class _Session:
  def __enter__(self):
    return self

  def __exit__(self, *_exc):
    return False


def _session_cm():
  return _Session()


def _client(graph_id: str = "kg01ABC", user_id: str | None = None) -> MagicMock:
  c = MagicMock()
  c.graph_id = graph_id
  if user_id is None:
    # Explicitly drop the attribute so `getattr(..., "user_id", None)` → None
    del c.user_id
  else:
    c.user_id = user_id
  return c


class TestRegistrarToolDefinition:
  def test_definition_uses_spec_description_when_set(self) -> None:
    tool = _RegistrarMCPTool(
      client=_client(),
      spec=_spec(description="Explicit."),
      registrar=_registrar_stub(),
    )
    defn = tool.get_tool_definition()
    assert defn["name"] == "demo-op"
    assert defn["description"] == "Explicit."
    assert defn["inputSchema"]["type"] == "object"
    assert defn["inputSchema"]["additionalProperties"] is False

  def test_definition_falls_back_to_command_docstring(self) -> None:
    tool = _RegistrarMCPTool(
      client=_client(),
      spec=_spec(description=None),
      registrar=_registrar_stub(),
    )
    defn = tool.get_tool_definition()
    # `_noop_command` has no docstring; `_spec` sets summary="Demo Op".
    assert defn["description"] == "Demo Op"


class TestRegistrarToolExecute:
  @pytest.mark.asyncio
  async def test_happy_path_returns_model_dump(self) -> None:
    tool = _RegistrarMCPTool(
      client=_client(user_id="usr_42"),
      spec=_spec(),
      registrar=_registrar_stub(),
    )
    # Gate passes; session factory yields a fake session.
    with patch(
      "robosystems.middleware.mcp.tools.registrar.require_graph_extension_mcp",
      return_value=MagicMock(),
    ):
      result = await tool.execute({"id": "x", "value": 3})

    assert result == {"id": "x", "echoed": 3}

  @pytest.mark.asyncio
  async def test_mark_stale_reason_marks_graph_after_success(self) -> None:
    tool = _RegistrarMCPTool(
      client=_client(graph_id="kg_stale", user_id="usr_42"),
      spec=_spec(mark_stale_reason="demo_changed"),
      registrar=_registrar_stub(),
    )
    with (
      patch(
        "robosystems.middleware.mcp.tools.registrar.require_graph_extension_mcp",
        return_value=MagicMock(),
      ),
      patch("robosystems.middleware.mcp.tools.registrar.mark_graph_stale") as stale,
    ):
      result = await tool.execute({"id": "x", "value": 3})

    assert result == {"id": "x", "echoed": 3}
    stale.assert_called_once_with("kg_stale", "demo_changed")

  @pytest.mark.asyncio
  async def test_gate_rejection_short_circuits_before_validation(self) -> None:
    """A gate error must return the code/message envelope and skip
    Pydantic validation (so an otherwise invalid body still produces the
    gate error, not a validation error)."""
    tool = _RegistrarMCPTool(
      client=_client(), spec=_spec(), registrar=_registrar_stub()
    )
    with patch(
      "robosystems.middleware.mcp.tools.registrar.require_graph_extension_mcp",
      side_effect=MCPExtensionGateError("repository_write_forbidden", "nope"),
    ):
      result = await tool.execute({"totally": "invalid"})

    assert result == {
      "error": "repository_write_forbidden",
      "message": "nope",
    }

  @pytest.mark.asyncio
  async def test_validation_error_returns_invalid_arguments(self) -> None:
    tool = _RegistrarMCPTool(
      client=_client(), spec=_spec(), registrar=_registrar_stub()
    )
    with patch(
      "robosystems.middleware.mcp.tools.registrar.require_graph_extension_mcp",
      return_value=MagicMock(),
    ):
      # `id` is required; omit it
      result = await tool.execute({"value": 5})

    assert result["error"] == "invalid_arguments"
    assert "validation" in result["message"].lower()

  @pytest.mark.asyncio
  async def test_pre_validate_hook_translates_http_exception(self) -> None:
    from fastapi import HTTPException

    def bad_period(_body: _Request) -> None:
      raise HTTPException(status_code=422, detail="period required")

    tool = _RegistrarMCPTool(
      client=_client(),
      spec=_spec(pre_validate=bad_period),
      registrar=_registrar_stub(),
    )
    with patch(
      "robosystems.middleware.mcp.tools.registrar.require_graph_extension_mcp",
      return_value=MagicMock(),
    ):
      result = await tool.execute({"id": "x", "value": 1})

    assert result["error"] == "invalid_arguments"
    assert "period required" in result["message"]

  @pytest.mark.asyncio
  async def test_requires_created_by_false_omits_kwarg(self) -> None:
    """Specs with `requires_created_by=False` invoke the command without
    the `created_by` kwarg (e.g. `update-*` ops)."""
    received: dict[str, Any] = {}

    def capture_cmd(session, body):  # no created_by
      received["session"] = session
      received["body"] = body
      return _Response(id=body.id, echoed=body.value)

    spec = _spec(command=capture_cmd, requires_created_by=False)
    tool = _RegistrarMCPTool(
      client=_client(user_id="usr_1"), spec=spec, registrar=_registrar_stub()
    )
    with patch(
      "robosystems.middleware.mcp.tools.registrar.require_graph_extension_mcp",
      return_value=MagicMock(),
    ):
      result = await tool.execute({"id": "y", "value": 7})

    assert result == {"id": "y", "echoed": 7}
    assert "created_by" not in received

  @pytest.mark.asyncio
  async def test_error_map_domain_exception_translated(self) -> None:
    def raising_cmd(session, body, created_by: str):
      raise DemoNotFoundError(f"no such id {body.id}")

    tool = _RegistrarMCPTool(
      client=_client(user_id="usr_1"),
      spec=_spec(command=raising_cmd),
      registrar=_registrar_stub(),
    )
    with patch(
      "robosystems.middleware.mcp.tools.registrar.require_graph_extension_mcp",
      return_value=MagicMock(),
    ):
      result = await tool.execute({"id": "x", "value": 0})

    assert result["error"] == "demo_not_found"
    assert "no such id x" in result["message"]

  @pytest.mark.asyncio
  async def test_unmapped_exception_falls_back_to_command_failed(self) -> None:
    def raising_cmd(session, body, created_by: str):
      raise RuntimeError("boom")

    tool = _RegistrarMCPTool(
      client=_client(user_id="usr_1"),
      spec=_spec(command=raising_cmd),
      registrar=_registrar_stub(),
    )
    with patch(
      "robosystems.middleware.mcp.tools.registrar.require_graph_extension_mcp",
      return_value=MagicMock(),
    ):
      result = await tool.execute({"id": "x", "value": 0})

    assert result["error"] == "command_failed"
    assert "boom" in result["message"]

  @pytest.mark.asyncio
  async def test_list_response_is_dumped_per_item(self) -> None:
    def list_cmd(session, body, created_by: str):
      return [_Response(id="a", echoed=1), _Response(id="b", echoed=2)]

    tool = _RegistrarMCPTool(
      client=_client(user_id="usr_1"),
      spec=_spec(command=list_cmd),
      registrar=_registrar_stub(),
    )
    with patch(
      "robosystems.middleware.mcp.tools.registrar.require_graph_extension_mcp",
      return_value=MagicMock(),
    ):
      result = await tool.execute({"id": "x", "value": 0})

    assert result == [{"id": "a", "echoed": 1}, {"id": "b", "echoed": 2}]

  @pytest.mark.asyncio
  async def test_created_by_falls_back_when_client_has_no_user_id(self) -> None:
    captured: dict[str, Any] = {}

    def capture_cmd(session, body, created_by: str):
      captured["created_by"] = created_by
      return _Response(id=body.id, echoed=body.value)

    tool = _RegistrarMCPTool(
      # user_id=None → sentinel fallback `mcp:{graph_id}`
      client=_client(graph_id="kg_ABC", user_id=None),
      spec=_spec(command=capture_cmd),
      registrar=_registrar_stub(),
    )
    with patch(
      "robosystems.middleware.mcp.tools.registrar.require_graph_extension_mcp",
      return_value=MagicMock(),
    ):
      await tool.execute({"id": "x", "value": 0})

    assert captured["created_by"] == "mcp:kg_ABC"


# ── build_tools_for_extension — factory ───────────────────────────────────


class TestBuildToolsForExtension:
  def test_returns_dict_keyed_by_spec_name(self) -> None:
    """Assemble a stub Registrar with two specs and verify the factory
    returns a `{name: tool}` dispatch dict."""
    from robosystems.middleware.extensions import OperationRegistrar

    spec_a = _spec(name="op-a")
    spec_b = _spec(name="op-b")
    fake_reg = MagicMock()
    fake_reg.extension = "demo-ext"
    fake_reg._registered = [spec_a, spec_b]
    fake_reg.session_factory = lambda gid: _session_cm()

    with patch.object(
      OperationRegistrar,
      "specs_for_extension",
      classmethod(lambda _cls, ext: [(fake_reg, spec_a), (fake_reg, spec_b)]),
    ):
      tools = build_tools_for_extension(
        extension="demo-ext", client=_client(), meta_getter=None
      )

    assert set(tools.keys()) == {"op-a", "op-b"}
    assert all(isinstance(t, _RegistrarMCPTool) for t in tools.values())
