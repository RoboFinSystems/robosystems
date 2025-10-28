import json
import logging
import sys

import pytest

from robosystems.config.env import EnvConfig
from robosystems.config.logging import (
  StructuredFormatter,
  TieredLogFilter,
  get_logger,
  get_logging_config,
  log_api_request,
  log_database_query,
  log_error,
  log_performance_metric,
  log_security_event,
  setup_logging,
)


def _make_record(level: int) -> logging.LogRecord:
  return logging.LogRecord(
    name="test",
    level=level,
    pathname=__file__,
    lineno=0,
    msg="message",
    args=(),
    exc_info=None,
  )


def test_structured_formatter_includes_optional_fields():
  formatter = StructuredFormatter()
  record = logging.LogRecord(
    name="robosystems.test",
    level=logging.INFO,
    pathname=__file__,
    lineno=10,
    msg="Test %s",
    args=("message",),
    exc_info=None,
  )
  record.component = "api"
  record.action = "request_completed"
  record.user_id = "user-1"
  record.entity_id = "entity-99"
  record.database = "db"
  record.duration_ms = 42.5
  record.status_code = 200
  record.metadata = {"key": "value"}
  record.request_id = "req-123"

  payload = json.loads(formatter.format(record))

  assert payload["message"] == "Test message"
  assert payload["component"] == "api"
  assert payload["action"] == "request_completed"
  assert payload["user_id"] == "user-1"
  assert payload["entity_id"] == "entity-99"
  assert payload["database"] == "db"
  assert payload["duration_ms"] == 42.5
  assert payload["status_code"] == 200
  assert payload["metadata"] == {"key": "value"}
  assert payload["request_id"] == "req-123"
  assert payload["timestamp"].endswith("Z")


def test_structured_formatter_includes_error_details():
  formatter = StructuredFormatter()

  try:
    raise ValueError("boom")
  except ValueError:
    record = logging.LogRecord(
      name="robosystems.test",
      level=logging.ERROR,
      pathname=__file__,
      lineno=50,
      msg="Failure occurred",
      args=(),
      exc_info=None,
    )
    record.exc_info = sys.exc_info()
    record.error_category = "validation"

  payload = json.loads(formatter.format(record))

  assert payload["level"] == "ERROR"
  assert payload["error_category"] == "validation"
  assert payload["error"]["type"] == "ValueError"
  assert payload["error"]["message"] == "boom"
  assert isinstance(payload["error"]["traceback"], list)
  assert any("ValueError: boom" in line for line in payload["error"]["traceback"])


@pytest.mark.parametrize(
  "tier,level,should_pass",
  [
    ("critical", logging.ERROR, True),
    ("critical", logging.INFO, False),
    ("operational", logging.INFO, True),
    ("operational", logging.ERROR, False),
    ("debug", logging.DEBUG, True),
    ("debug", logging.INFO, False),
  ],
)
def test_tiered_log_filter(tier, level, should_pass):
  log_filter = TieredLogFilter(tier)
  record = _make_record(level)

  assert log_filter.filter(record) is should_pass


def test_get_logging_config_prod_has_expected_handlers(monkeypatch):
  monkeypatch.setattr(EnvConfig, "LOG_LEVEL", "INFO")
  config = get_logging_config("prod")

  assert config["handlers"]["console"]["formatter"] == "structured"
  assert "debug" not in config["handlers"]
  assert config["loggers"]["robosystems"]["handlers"] == ["critical", "operational"]


def test_get_logging_config_staging_adds_debug_handler(monkeypatch):
  monkeypatch.setattr(EnvConfig, "LOG_LEVEL", "INFO")
  config = get_logging_config("staging")

  assert "debug" in config["handlers"]
  assert "debug" in config["loggers"]["robosystems"]["handlers"]
  assert config["handlers"]["debug"]["level"] == "DEBUG"


def test_get_logging_config_dev_respects_log_level_override(monkeypatch):
  monkeypatch.setattr(EnvConfig, "LOG_LEVEL", "DEBUG")
  config = get_logging_config("dev")

  assert config["handlers"]["console"]["level"] == "DEBUG"
  assert config["handlers"]["console"]["formatter"] == "simple"
  assert "debug" in config["handlers"]
  assert config["loggers"]["robosystems"]["handlers"] == ["console"]


def test_setup_logging_invokes_dict_config(monkeypatch):
  captured = {}

  def fake_dict_config(value):
    captured["config"] = value

  monkeypatch.setattr(logging.config, "dictConfig", fake_dict_config)
  setup_logging("test")

  assert captured["config"]["root"]["level"] == "WARNING"


def test_log_api_request_captures_structured_fields(caplog):
  logger = get_logger("tests.config.logging.api")

  with caplog.at_level(logging.INFO, logger=logger.name):
    log_api_request(
      logger,
      method="GET",
      path="/v1/items",
      status_code=200,
      duration_ms=12.3,
      user_id="user-1",
      entity_id="entity-2",
      request_id="req-9",
    )

  record = caplog.records[-1]
  assert record.component == "api"
  assert record.action == "request_completed"
  assert record.status_code == 200
  assert record.duration_ms == 12.3
  assert record.user_id == "user-1"
  assert record.entity_id == "entity-2"
  assert record.request_id == "req-9"
  assert record.message == "GET /v1/items - 200 (12.30ms)"


def test_log_database_query_warns_on_slow_query(caplog):
  logger = get_logger("tests.config.logging.database")
  caplog.set_level(logging.WARNING, logger=logger.name)

  log_database_query(
    logger,
    database="postgres",
    query_type="SELECT",
    duration_ms=1500,
    row_count=5,
    user_id="user",
    entity_id="entity",
  )

  record = caplog.records[-1]
  assert record.levelno == logging.WARNING
  assert record.row_count == 5
  assert record.database == "postgres"
  assert record.duration_ms == 1500


def test_log_database_query_info_for_fast_query(caplog):
  logger = get_logger("tests.config.logging.database.fast")

  with caplog.at_level(logging.INFO, logger=logger.name):
    log_database_query(
      logger,
      database="postgres",
      query_type="INSERT",
      duration_ms=100,
    )

  record = caplog.records[-1]
  assert record.levelno == logging.INFO
  assert record.database == "postgres"
  assert not hasattr(record, "row_count")


def test_log_error_includes_metadata(caplog):
  logger = get_logger("tests.config.logging.error")

  with caplog.at_level(logging.ERROR, logger=logger.name):
    try:
      raise RuntimeError("Failure")
    except RuntimeError as exc:
      log_error(
        logger,
        error=exc,
        component="service",
        action="process",
        error_category="runtime",
        user_id="user-x",
        entity_id="entity-y",
        metadata={"foo": "bar"},
      )

  record = caplog.records[-1]
  assert record.component == "service"
  assert record.action == "process"
  assert record.error_category == "runtime"
  assert record.user_id == "user-x"
  assert record.entity_id == "entity-y"
  assert record.metadata == {"foo": "bar"}
  assert record.exc_info is not None


def test_log_security_event_sets_level_based_on_success(caplog):
  logger = get_logger("tests.config.logging.security")

  with caplog.at_level(logging.INFO, logger=logger.name):
    log_security_event(
      logger,
      event_type="login",
      user_id="user-1",
      ip_address="127.0.0.1",
      success=True,
    )
  record_success = caplog.records[-1]
  assert record_success.levelno == logging.INFO
  assert record_success.success is True

  caplog.clear()
  caplog.set_level(logging.WARNING, logger=logger.name)

  log_security_event(
    logger,
    event_type="login",
    user_id="user-1",
    ip_address="127.0.0.1",
    success=False,
    metadata={"attempt": 2},
  )
  record_failure = caplog.records[-1]
  assert record_failure.levelno == logging.WARNING
  assert record_failure.success is False
  assert record_failure.metadata == {"attempt": 2}


def test_log_performance_metric_logs_expected_fields(caplog):
  logger = get_logger("tests.config.logging.performance")

  with caplog.at_level(logging.INFO, logger=logger.name):
    log_performance_metric(
      logger,
      metric_name="requests_per_minute",
      value=42,
      unit="rpm",
      component="worker",
      metadata={"node": "a"},
    )

  record = caplog.records[-1]
  assert record.component == "performance"
  assert record.action == "metric_recorded"
  assert record.metric_name == "requests_per_minute"
  assert record.metric_value == 42
  assert record.unit == "rpm"
  assert record.source_component == "worker"
  assert record.metadata == {"node": "a"}
