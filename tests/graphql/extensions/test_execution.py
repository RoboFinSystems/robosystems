"""The execution-time guards: sync resolvers leave the event loop, and only
deliberate errors reach ``errors[]`` verbatim."""

from __future__ import annotations

import threading
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import strawberry
from sqlalchemy.exc import IntegrityError, OperationalError
from strawberry.exceptions import StrawberryGraphQLError

from robosystems.graphql.execution import (
  INTERNAL_ERROR_CODE,
  INTERNAL_ERROR_MESSAGE,
  STATEMENT_TIMEOUT_CODE,
  MaskUnexpectedErrors,
  OffloadSyncResolvers,
)

_LEAKY_SQL = 'INSERT INTO "kg00000000000000aa".entities (id) VALUES (%(id)s)'


@strawberry.type
class Leaf:
  label: str


@strawberry.type
class Query:
  @strawberry.field
  def thread_ident(self) -> str:
    return str(threading.get_ident())

  @strawberry.field
  def leaf(self) -> Leaf:
    return Leaf(label="x")

  @strawberry.field
  def integrity(self) -> str:
    raise IntegrityError(_LEAKY_SQL, {"id": "secret"}, Exception("duplicate key"))

  @strawberry.field
  def timeout(self) -> str:
    raise OperationalError(_LEAKY_SQL, {}, MagicMock(pgcode="57014"))

  @strawberry.field
  def deliberate(self) -> str:
    raise StrawberryGraphQLError(
      message="Ledger not initialized", extensions={"code": "LEDGER_NOT_INITIALIZED"}
    )

  @strawberry.field
  def plain_python(self) -> str:
    raise KeyError("column_that_names_the_schema")


schema = strawberry.Schema(
  query=Query, extensions=[OffloadSyncResolvers, MaskUnexpectedErrors]
)


def _ctx(request_id: str | None = "req_abc") -> dict:
  request = SimpleNamespace(state=SimpleNamespace(request_id=request_id))
  return {"request": request}


class TestOffloadSyncResolvers:
  @pytest.mark.asyncio
  async def test_sync_resolver_runs_off_the_loop_thread(self) -> None:
    result = await schema.execute("{ threadIdent }", context_value=_ctx())
    assert not result.errors
    assert result.data["threadIdent"] != str(threading.get_ident())

  @pytest.mark.asyncio
  async def test_default_field_resolvers_stay_inline(self) -> None:
    """Attribute getters on returned objects are not worth a thread hop; the
    offload only wraps resolvers someone wrote."""
    result = await schema.execute("{ leaf { label } }", context_value=_ctx())
    assert not result.errors
    assert result.data == {"leaf": {"label": "x"}}

  def test_no_running_loop_runs_inline(self) -> None:
    """`execute_sync` from a worker thread has no loop to protect and cannot
    await — the resolver runs where it is called."""
    result = schema.execute_sync("{ threadIdent }", context_value=_ctx())
    assert not result.errors
    assert result.data["threadIdent"] == str(threading.get_ident())

  def test_introspection_is_untouched(self) -> None:
    result = schema.execute_sync("{ __typename }", context_value=_ctx())
    assert result.data == {"__typename": "Query"}


class TestMaskUnexpectedErrors:
  def test_database_fault_is_masked_and_carries_the_request_id(self) -> None:
    result = schema.execute_sync("{ integrity }", context_value=_ctx("req_42"))
    assert result.errors is not None and len(result.errors) == 1
    error = result.errors[0]
    assert error.message == INTERNAL_ERROR_MESSAGE
    assert error.extensions == {"code": INTERNAL_ERROR_CODE, "requestId": "req_42"}
    assert "kg00000000000000aa" not in str(error)
    assert "secret" not in str(error)
    assert error.path == ["integrity"]

  def test_plain_python_error_is_masked(self) -> None:
    result = schema.execute_sync("{ plainPython }", context_value=_ctx())
    assert result.errors[0].message == INTERNAL_ERROR_MESSAGE
    assert "column_that_names_the_schema" not in result.errors[0].message

  def test_statement_timeout_gets_its_own_code(self) -> None:
    result = schema.execute_sync("{ timeout }", context_value=_ctx())
    error = result.errors[0]
    assert error.extensions["code"] == STATEMENT_TIMEOUT_CODE
    assert "INSERT" not in error.message
    assert "time limit" in error.message

  def test_deliberate_graphql_error_passes_through(self) -> None:
    result = schema.execute_sync("{ deliberate }", context_value=_ctx())
    error = result.errors[0]
    assert error.message == "Ledger not initialized"
    assert error.extensions == {"code": "LEDGER_NOT_INITIALIZED"}

  def test_validation_error_passes_through(self) -> None:
    result = schema.execute_sync("{ noSuchField }", context_value=_ctx())
    assert result.errors[0].message.startswith("Cannot query field")

  def test_missing_request_id_omits_the_extension_key(self) -> None:
    result = schema.execute_sync("{ integrity }", context_value=_ctx(None))
    assert result.errors[0].extensions == {"code": INTERNAL_ERROR_CODE}

  @pytest.mark.asyncio
  async def test_masking_applies_on_the_async_path_too(self) -> None:
    result = await schema.execute("{ integrity }", context_value=_ctx())
    assert result.errors[0].message == INTERNAL_ERROR_MESSAGE
