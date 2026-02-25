"""Unit tests for common API models."""

from datetime import UTC, datetime

import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from robosystems.models.api.common import (
  CreditCostInfo,
  ErrorCode,
  ErrorResponse,
  HealthStatus,
  PaginationInfo,
  SuccessResponse,
  create_error_response,
  create_pagination_info,
)


@pytest.mark.unit
class TestErrorResponse:
  def test_required_detail_field(self):
    model = ErrorResponse(detail="Something went wrong")
    assert model.detail == "Something went wrong"
    assert model.code is None
    assert model.request_id is None
    assert model.timestamp is None

  def test_all_fields(self):
    ts = datetime(2024, 1, 1, tzinfo=UTC)
    model = ErrorResponse(
      detail="Not found",
      code="NOT_FOUND",
      request_id="req_abc123",
      timestamp=ts,
    )
    assert model.detail == "Not found"
    assert model.code == "NOT_FOUND"
    assert model.request_id == "req_abc123"
    assert model.timestamp == ts

  def test_detail_required(self):
    with pytest.raises(ValidationError) as exc_info:
      ErrorResponse()  # type: ignore[call-arg]
    errors = exc_info.value.errors()
    assert any(e["loc"] == ("detail",) for e in errors)

  def test_model_dump_excludes_none(self):
    model = ErrorResponse(detail="error")
    dumped = model.model_dump(exclude_none=True)
    assert dumped == {"detail": "error"}

  def test_model_dump_includes_all(self):
    ts = datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC)
    model = ErrorResponse(detail="error", code="ERR", request_id="req_1", timestamp=ts)
    dumped = model.model_dump()
    assert dumped["detail"] == "error"
    assert dumped["code"] == "ERR"
    assert dumped["request_id"] == "req_1"
    assert dumped["timestamp"] == ts

  def test_json_serialization(self):
    model = ErrorResponse(detail="test error")
    json_str = model.model_dump_json()
    assert '"detail":"test error"' in json_str

  def test_json_deserialization(self):
    json_data = '{"detail": "parsed error", "code": "PARSE_ERR"}'
    model = ErrorResponse.model_validate_json(json_data)
    assert model.detail == "parsed error"
    assert model.code == "PARSE_ERR"


@pytest.mark.unit
class TestSuccessResponse:
  def test_defaults(self):
    model = SuccessResponse(message="Done")
    assert model.success is True
    assert model.message == "Done"
    assert model.data is None

  def test_with_data(self):
    model = SuccessResponse(message="Created", data={"id": "abc"})
    assert model.data == {"id": "abc"}

  def test_success_can_be_overridden(self):
    model = SuccessResponse(success=False, message="Partial")
    assert model.success is False

  def test_message_required(self):
    with pytest.raises(ValidationError) as exc_info:
      SuccessResponse()  # type: ignore[call-arg]
    errors = exc_info.value.errors()
    assert any(e["loc"] == ("message",) for e in errors)

  def test_model_dump(self):
    model = SuccessResponse(message="OK")
    dumped = model.model_dump()
    assert dumped == {"success": True, "message": "OK", "data": None}


@pytest.mark.unit
class TestPaginationInfo:
  def test_all_required_fields(self):
    model = PaginationInfo(total=100, limit=20, offset=0, has_more=True)
    assert model.total == 100
    assert model.limit == 20
    assert model.offset == 0
    assert model.has_more is True

  def test_missing_fields_raises(self):
    with pytest.raises(ValidationError):
      PaginationInfo(total=100)  # type: ignore[call-arg]

  def test_model_dump(self):
    model = PaginationInfo(total=50, limit=10, offset=20, has_more=True)
    dumped = model.model_dump()
    assert dumped == {"total": 50, "limit": 10, "offset": 20, "has_more": True}


@pytest.mark.unit
class TestHealthStatus:
  def test_valid_healthy_status(self):
    ts = datetime(2024, 1, 1, tzinfo=UTC)
    model = HealthStatus(status="healthy", timestamp=ts)
    assert model.status == "healthy"

  def test_valid_degraded_status(self):
    ts = datetime(2024, 1, 1, tzinfo=UTC)
    model = HealthStatus(status="degraded", timestamp=ts)
    assert model.status == "degraded"

  def test_valid_unhealthy_status(self):
    ts = datetime(2024, 1, 1, tzinfo=UTC)
    model = HealthStatus(status="unhealthy", timestamp=ts)
    assert model.status == "unhealthy"

  def test_invalid_status_rejected(self):
    ts = datetime(2024, 1, 1, tzinfo=UTC)
    with pytest.raises(ValidationError) as exc_info:
      HealthStatus(status="broken", timestamp=ts)
    errors = exc_info.value.errors()
    assert any("pattern" in e["type"] or "string_pattern" in e["type"] for e in errors)

  def test_details_optional(self):
    ts = datetime(2024, 1, 1, tzinfo=UTC)
    model = HealthStatus(status="healthy", timestamp=ts, details={"db": "up"})
    assert model.details == {"db": "up"}


@pytest.mark.unit
class TestCreditCostInfo:
  def test_required_fields(self):
    model = CreditCostInfo(base_cost=1.0, multiplier=1.5, total_cost=1.5)
    assert model.base_cost == 1.0
    assert model.multiplier == 1.5
    assert model.total_cost == 1.5
    assert model.cached is False

  def test_cached_default_false(self):
    model = CreditCostInfo(base_cost=0.0, multiplier=1.0, total_cost=0.0)
    assert model.cached is False

  def test_cached_true(self):
    model = CreditCostInfo(base_cost=0.0, multiplier=1.0, total_cost=0.0, cached=True)
    assert model.cached is True


@pytest.mark.unit
class TestCreateErrorResponse:
  def test_returns_http_exception(self):
    result = create_error_response(404, "Not found")
    assert isinstance(result, HTTPException)
    assert result.status_code == 404

  def test_error_detail_contains_message(self):
    result = create_error_response(400, "Bad request", code="BAD_REQUEST")
    assert result.detail["detail"] == "Bad request"
    assert result.detail["code"] == "BAD_REQUEST"

  def test_error_detail_contains_timestamp(self):
    result = create_error_response(500, "Server error")
    assert "timestamp" in result.detail

  def test_error_with_request_id(self):
    result = create_error_response(
      403, "Forbidden", code="FORBIDDEN", request_id="req_test123"
    )
    assert result.detail["request_id"] == "req_test123"

  def test_excludes_none_values(self):
    result = create_error_response(404, "Missing")
    # code and request_id are None, should not appear in detail
    assert "code" not in result.detail
    assert "request_id" not in result.detail


@pytest.mark.unit
class TestCreatePaginationInfo:
  def test_has_more_true(self):
    info = create_pagination_info(total=100, limit=20, offset=0)
    assert info.has_more is True

  def test_has_more_false_at_end(self):
    info = create_pagination_info(total=100, limit=20, offset=80)
    assert info.has_more is False

  def test_has_more_false_exact_page(self):
    info = create_pagination_info(total=20, limit=20, offset=0)
    assert info.has_more is False

  def test_has_more_false_beyond_total(self):
    info = create_pagination_info(total=10, limit=20, offset=0)
    assert info.has_more is False

  def test_values_preserved(self):
    info = create_pagination_info(total=50, limit=10, offset=30)
    assert info.total == 50
    assert info.limit == 10
    assert info.offset == 30


@pytest.mark.unit
class TestErrorCode:
  def test_error_codes_are_strings(self):
    assert ErrorCode.UNAUTHORIZED == "UNAUTHORIZED"
    assert ErrorCode.FORBIDDEN == "FORBIDDEN"
    assert ErrorCode.NOT_FOUND == "NOT_FOUND"
    assert ErrorCode.INSUFFICIENT_CREDITS == "INSUFFICIENT_CREDITS"
    assert ErrorCode.RATE_LIMIT_EXCEEDED == "RATE_LIMIT_EXCEEDED"
    assert ErrorCode.INTERNAL_ERROR == "INTERNAL_ERROR"
    assert ErrorCode.ALREADY_EXISTS == "ALREADY_EXISTS"
    assert ErrorCode.INVALID_INPUT == "INVALID_INPUT"
    assert ErrorCode.DATABASE_ERROR == "DATABASE_ERROR"
