import pytest
from pydantic import ValidationError

from robosystems.models.api.views.save_view import SaveViewRequest


@pytest.mark.security
@pytest.mark.unit
class TestSaveViewRequestValidation:
  def test_report_id_with_sql_injection_attempt(self):
    with pytest.raises(ValidationError) as exc_info:
      SaveViewRequest(
        report_id="test' OR 1=1--",
        report_type="Annual Report",
        period_start="2024-01-01",
        period_end="2024-12-31",
      )
    assert "String should match pattern" in str(exc_info.value)

  def test_report_id_with_single_quote(self):
    with pytest.raises(ValidationError) as exc_info:
      SaveViewRequest(
        report_id="test'report",
        report_type="Annual Report",
        period_start="2024-01-01",
        period_end="2024-12-31",
      )
    assert "String should match pattern" in str(exc_info.value)

  def test_report_id_with_semicolon(self):
    with pytest.raises(ValidationError) as exc_info:
      SaveViewRequest(
        report_id="test;DROP TABLE",
        report_type="Annual Report",
        period_start="2024-01-01",
        period_end="2024-12-31",
      )
    assert "String should match pattern" in str(exc_info.value)

  def test_report_id_too_long(self):
    with pytest.raises(ValidationError) as exc_info:
      SaveViewRequest(
        report_id="a" * 101,
        report_type="Annual Report",
        period_start="2024-01-01",
        period_end="2024-12-31",
      )
    assert "String should have at most 100 characters" in str(exc_info.value)

  def test_valid_report_id_with_alphanumeric_and_hyphens(self):
    request = SaveViewRequest(
      report_id="entity123-annual-2024",
      report_type="Annual Report",
      period_start="2024-01-01",
      period_end="2024-12-31",
    )
    assert request.report_id == "entity123-annual-2024"

  def test_entity_id_with_injection_attempt(self):
    with pytest.raises(ValidationError) as exc_info:
      SaveViewRequest(
        entity_id="entity' OR '1'='1",
        report_type="Annual Report",
        period_start="2024-01-01",
        period_end="2024-12-31",
      )
    assert "String should match pattern" in str(exc_info.value)

  def test_report_type_with_newline_characters(self):
    with pytest.raises(ValidationError) as exc_info:
      SaveViewRequest(
        report_type="Annual\nReport",
        period_start="2024-01-01",
        period_end="2024-12-31",
      )
    assert "Report type cannot contain newline characters" in str(exc_info.value)

  def test_report_type_with_carriage_return(self):
    with pytest.raises(ValidationError) as exc_info:
      SaveViewRequest(
        report_type="Annual\rReport",
        period_start="2024-01-01",
        period_end="2024-12-31",
      )
    assert "Report type cannot contain newline characters" in str(exc_info.value)

  def test_report_type_too_long(self):
    with pytest.raises(ValidationError) as exc_info:
      SaveViewRequest(
        report_type="A" * 201,
        period_start="2024-01-01",
        period_end="2024-12-31",
      )
    assert "String should have at most 200 characters" in str(exc_info.value)

  def test_report_type_empty_or_whitespace(self):
    with pytest.raises(ValidationError) as exc_info:
      SaveViewRequest(
        report_type="   ",
        period_start="2024-01-01",
        period_end="2024-12-31",
      )
    assert "Report type cannot be empty or whitespace only" in str(exc_info.value)

  def test_report_type_strips_whitespace(self):
    request = SaveViewRequest(
      report_type="  Annual Report  ",
      period_start="2024-01-01",
      period_end="2024-12-31",
    )
    assert request.report_type == "Annual Report"

  def test_invalid_date_format_period_start(self):
    with pytest.raises(ValidationError) as exc_info:
      SaveViewRequest(
        report_type="Annual Report",
        period_start="2024/01/01",
        period_end="2024-12-31",
      )
    assert "String should match pattern" in str(exc_info.value)

  def test_invalid_date_format_period_end(self):
    with pytest.raises(ValidationError) as exc_info:
      SaveViewRequest(
        report_type="Annual Report",
        period_start="2024-01-01",
        period_end="12-31-2024",
      )
    assert "String should match pattern" in str(exc_info.value)

  def test_invalid_date_values(self):
    with pytest.raises(ValidationError) as exc_info:
      SaveViewRequest(
        report_type="Annual Report",
        period_start="2024-13-32",
        period_end="2024-12-31",
      )
    assert "Date must be in YYYY-MM-DD format" in str(exc_info.value)

  def test_date_with_sql_injection(self):
    with pytest.raises(ValidationError) as exc_info:
      SaveViewRequest(
        report_type="Annual Report",
        period_start="2024-01-01' OR '1'='1",
        period_end="2024-12-31",
      )
    assert "String should match pattern" in str(exc_info.value)

  def test_valid_request_minimal(self):
    request = SaveViewRequest(
      report_type="Annual Report",
      period_start="2024-01-01",
      period_end="2024-12-31",
    )
    assert request.report_type == "Annual Report"
    assert request.period_start == "2024-01-01"
    assert request.period_end == "2024-12-31"
    assert request.report_id is None
    assert request.entity_id is None
    assert request.include_presentation is True
    assert request.include_calculation is True

  def test_valid_request_complete(self):
    request = SaveViewRequest(
      report_id="entity123-annual-2024",
      entity_id="entity_123",
      report_type="Annual Report",
      period_start="2024-01-01",
      period_end="2024-12-31",
      include_presentation=False,
      include_calculation=False,
    )
    assert request.report_id == "entity123-annual-2024"
    assert request.entity_id == "entity_123"
    assert request.report_type == "Annual Report"
    assert request.period_start == "2024-01-01"
    assert request.period_end == "2024-12-31"
    assert request.include_presentation is False
    assert request.include_calculation is False
