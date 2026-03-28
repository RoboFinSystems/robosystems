"""Tests for connection cleanup between worker tasks."""

from unittest.mock import MagicMock, patch

from robosystems.worker.cleanup import cleanup_connections


@patch("robosystems.worker.cleanup.logger")
def test_cleanup_disposes_both_engines(mock_logger):
  mock_platform_engine = MagicMock()
  mock_extensions_engine = MagicMock()

  with (
    patch(
      "robosystems.db.platform.engine",
      mock_platform_engine,
    ),
    patch(
      "robosystems.db.extensions._engine",
      mock_extensions_engine,
    ),
  ):
    cleanup_connections()

  mock_platform_engine.dispose.assert_called_once()
  mock_extensions_engine.dispose.assert_called_once()


@patch("robosystems.worker.cleanup.logger")
def test_cleanup_handles_extensions_engine_none(mock_logger):
  mock_platform_engine = MagicMock()

  with (
    patch(
      "robosystems.db.platform.engine",
      mock_platform_engine,
    ),
    patch(
      "robosystems.db.extensions._engine",
      None,
    ),
  ):
    cleanup_connections()

  mock_platform_engine.dispose.assert_called_once()


@patch("robosystems.worker.cleanup.logger")
def test_cleanup_catches_platform_exception(mock_logger):
  mock_platform_engine = MagicMock()
  mock_platform_engine.dispose.side_effect = RuntimeError("connection lost")

  with (
    patch(
      "robosystems.db.platform.engine",
      mock_platform_engine,
    ),
    patch(
      "robosystems.db.extensions._engine",
      None,
    ),
  ):
    # Should not raise
    cleanup_connections()

  mock_logger.warning.assert_called()


@patch("robosystems.worker.cleanup.logger")
def test_cleanup_catches_extensions_exception(mock_logger):
  mock_platform_engine = MagicMock()
  mock_extensions_engine = MagicMock()
  mock_extensions_engine.dispose.side_effect = RuntimeError("connection lost")

  with (
    patch(
      "robosystems.db.platform.engine",
      mock_platform_engine,
    ),
    patch(
      "robosystems.db.extensions._engine",
      mock_extensions_engine,
    ),
  ):
    # Should not raise
    cleanup_connections()

  mock_platform_engine.dispose.assert_called_once()
  mock_logger.warning.assert_called()
