"""
Comprehensive tests for SEC provider operations.

Tests the critical SEC data provider that handles CIK validation,
filing counts, and SEC connection management.
"""

import pytest
from unittest.mock import Mock, patch, AsyncMock
import httpx
from fastapi import HTTPException
from sqlalchemy.orm import Session

from robosystems.operations.providers.sec_provider import (
  validate_cik_with_sec_api,
  get_sec_filing_count,
  create_sec_connection,
  sync_sec_connection,
  cleanup_sec_connection,
)
from robosystems.models.api.graphs.connections import SECConnectionConfig


class TestValidateCIKWithSECAPI:
  """Tests for CIK validation with SEC API."""

  @pytest.mark.asyncio
  async def test_validate_cik_found_in_tickers(self):
    """Test successful CIK validation when found in entity tickers."""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
      "0": {"cik_str": 1018724, "ticker": "AMZN", "title": "AMAZON COM INC"},
      "1": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."},
    }

    with patch("httpx.AsyncClient") as mock_client_class:
      mock_client = AsyncMock()
      mock_client.__aenter__.return_value = mock_client
      mock_client.get.return_value = mock_response
      mock_client_class.return_value = mock_client

      result = await validate_cik_with_sec_api("0000320193")

      assert result["is_valid"] is True
      assert result["cik"] == "0000320193"
      assert result["entity_name"] == "Apple Inc."
      assert result["ticker"] == "AAPL"

      # Verify the correct URL was called
      mock_client.get.assert_called_once()
      call_args = mock_client.get.call_args
      assert "entity_tickers.json" in call_args[0][0]

  @pytest.mark.asyncio
  async def test_validate_cik_found_in_submissions(self):
    """Test CIK validation when not in tickers but found in submissions API."""
    # First call to tickers returns data without our CIK
    mock_tickers_response = Mock()
    mock_tickers_response.status_code = 200
    mock_tickers_response.json.return_value = {
      "0": {"cik_str": 1018724, "ticker": "AMZN", "title": "AMAZON COM INC"}
    }

    # Second call to submissions API returns our CIK data
    mock_submissions_response = Mock()
    mock_submissions_response.status_code = 200
    mock_submissions_response.json.return_value = {
      "cik": "0001234567",
      "name": "Test Company Inc.",
      "tickers": ["TEST"],
      "sic": "3571",
      "sicDescription": "Electronic Computers",
    }

    with patch("httpx.AsyncClient") as mock_client_class:
      mock_client = AsyncMock()
      mock_client.__aenter__.return_value = mock_client
      mock_client.get.side_effect = [mock_tickers_response, mock_submissions_response]
      mock_client_class.return_value = mock_client

      result = await validate_cik_with_sec_api("0001234567")

      assert result["is_valid"] is True
      assert result["cik"] == "0001234567"
      assert result["entity_name"] == "Test Company Inc."
      assert result["ticker"] == "TEST"
      assert result["sic"] == "3571"
      assert result["sic_description"] == "Electronic Computers"

      # Verify both APIs were called
      assert mock_client.get.call_count == 2

  @pytest.mark.asyncio
  async def test_validate_cik_not_found(self):
    """Test CIK validation when CIK is not found."""
    # Tickers API returns data without our CIK
    mock_tickers_response = Mock()
    mock_tickers_response.status_code = 200
    mock_tickers_response.json.return_value = {"0": {"cik_str": 1018724}}

    # Submissions API returns 404
    mock_submissions_response = Mock()
    mock_submissions_response.status_code = 404

    with patch("httpx.AsyncClient") as mock_client_class:
      mock_client = AsyncMock()
      mock_client.__aenter__.return_value = mock_client
      mock_client.get.side_effect = [mock_tickers_response, mock_submissions_response]
      mock_client_class.return_value = mock_client

      result = await validate_cik_with_sec_api("9999999999")

      assert result["is_valid"] is False
      assert result["cik"] == "9999999999"
      assert "not found" in result["error"].lower()

  @pytest.mark.asyncio
  async def test_validate_cik_api_error(self):
    """Test CIK validation when SEC API is unavailable."""
    with patch("httpx.AsyncClient") as mock_client_class:
      mock_client = AsyncMock()
      mock_client.__aenter__.return_value = mock_client
      mock_client.get.side_effect = httpx.HTTPError("Connection failed")
      mock_client_class.return_value = mock_client

      with pytest.raises(Exception) as exc_info:
        await validate_cik_with_sec_api("0000320193")

      assert "SEC API unavailable" in str(exc_info.value)

  @pytest.mark.asyncio
  async def test_validate_cik_with_empty_tickers(self):
    """Test CIK validation when entity has no ticker symbols."""
    mock_tickers_response = Mock()
    mock_tickers_response.status_code = 200
    mock_tickers_response.json.return_value = {}

    mock_submissions_response = Mock()
    mock_submissions_response.status_code = 200
    mock_submissions_response.json.return_value = {
      "cik": "0001234567",
      "name": "Private Company LLC",
      "tickers": [],  # Empty tickers list
      "sic": "1234",
      "sicDescription": "Test Industry",
    }

    with patch("httpx.AsyncClient") as mock_client_class:
      mock_client = AsyncMock()
      mock_client.__aenter__.return_value = mock_client
      mock_client.get.side_effect = [mock_tickers_response, mock_submissions_response]
      mock_client_class.return_value = mock_client

      result = await validate_cik_with_sec_api("0001234567")

      assert result["is_valid"] is True
      assert result["ticker"] is None


class TestGetSECFilingCount:
  """Tests for SEC filing count retrieval."""

  @pytest.mark.asyncio
  async def test_get_filing_count_success(self):
    """Test successful retrieval of filing count."""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
      "filings": {
        "recent": {
          "form": ["10-K", "10-Q", "8-K", "10-K", "10-Q"],
          "filingDate": [
            "2024-01-01",
            "2023-10-01",
            "2023-09-15",
            "2023-01-01",
            "2022-10-01",
          ],
        },
        "files": [
          {"name": "CIK0001234567-filings-001.json"},
          {"name": "CIK0001234567-filings-002.json"},
        ],
      }
    }

    with patch("httpx.AsyncClient") as mock_client_class:
      mock_client = AsyncMock()
      mock_client.__aenter__.return_value = mock_client
      mock_client.get.return_value = mock_response
      mock_client_class.return_value = mock_client

      count = await get_sec_filing_count("0001234567")

      assert count == 205  # 5 recent + (2 * 100) estimated
      mock_client.get.assert_called_once()

  @pytest.mark.asyncio
  async def test_get_filing_count_no_files(self):
    """Test filing count when only recent filings exist."""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
      "filings": {"recent": {"form": ["10-K", "10-Q", "8-K"]}, "files": []}
    }

    with patch("httpx.AsyncClient") as mock_client_class:
      mock_client = AsyncMock()
      mock_client.__aenter__.return_value = mock_client
      mock_client.get.return_value = mock_response
      mock_client_class.return_value = mock_client

      count = await get_sec_filing_count("0001234567")

      assert count == 3  # Only recent filings

  @pytest.mark.asyncio
  async def test_get_filing_count_api_error(self):
    """Test filing count when API fails."""
    with patch("httpx.AsyncClient") as mock_client_class:
      mock_client = AsyncMock()
      mock_client.__aenter__.return_value = mock_client
      mock_client.get.side_effect = httpx.HTTPError("Connection failed")
      mock_client_class.return_value = mock_client

      count = await get_sec_filing_count("0001234567")

      assert count == 0  # Returns 0 on error

  @pytest.mark.asyncio
  async def test_get_filing_count_not_found(self):
    """Test filing count when CIK not found."""
    mock_response = Mock()
    mock_response.status_code = 404

    with patch("httpx.AsyncClient") as mock_client_class:
      mock_client = AsyncMock()
      mock_client.__aenter__.return_value = mock_client
      mock_client.get.return_value = mock_response
      mock_client_class.return_value = mock_client

      count = await get_sec_filing_count("9999999999")

      assert count == 0

  @pytest.mark.asyncio
  async def test_get_filing_count_empty_filings(self):
    """Test filing count with empty filings structure."""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"filings": {}}

    with patch("httpx.AsyncClient") as mock_client_class:
      mock_client = AsyncMock()
      mock_client.__aenter__.return_value = mock_client
      mock_client.get.return_value = mock_response
      mock_client_class.return_value = mock_client

      count = await get_sec_filing_count("0001234567")

      assert count == 0


class TestCreateSECConnection:
  """Tests for SEC connection creation."""

  @pytest.fixture
  def mock_db(self):
    """Mock database session."""
    return Mock(spec=Session)

  @pytest.fixture
  def sec_config(self):
    """Sample SEC connection configuration."""
    return SECConnectionConfig(cik="0001234567", entity_name="Test Company Inc.")

  @pytest.mark.asyncio
  async def test_create_connection_success(self, mock_db, sec_config):
    """Test successful SEC connection creation."""
    # Mock repository
    mock_repository = Mock()
    mock_repository.execute_single.side_effect = [
      {"identifier": "entity123", "name": "Test Entity"},  # Entity exists
      {"identifier": "entity123"},  # Update successful
    ]

    # Mock connection service
    mock_connection_data = {
      "connection_id": "conn123",
      "entity_id": "entity123",
      "provider": "SEC",
    }

    with patch(
      "robosystems.operations.providers.sec_provider.get_graph_repository"
    ) as mock_get_repo:
      mock_get_repo.return_value = mock_repository

      with patch(
        "robosystems.operations.providers.sec_provider.ConnectionService.create_connection"
      ) as mock_create:
        mock_create.return_value = mock_connection_data

        with patch(
          "robosystems.operations.providers.sec_provider.env.SEC_VALIDATE_CIK", False
        ):
          connection_id = await create_sec_connection(
            entity_id="entity123",
            config=sec_config,
            user_id="user123",
            graph_id="graph123",
            db=mock_db,
          )

          assert connection_id == "conn123"

          # Verify entity was queried
          assert mock_repository.execute_single.call_count == 2

          # Verify connection was created
          mock_create.assert_called_once()
          call_kwargs = mock_create.call_args[1]
          assert call_kwargs["entity_id"] == "entity123"
          assert call_kwargs["provider"] == "SEC"
          assert call_kwargs["credentials"]["cik"] == "0001234567"

  @pytest.mark.asyncio
  async def test_create_connection_entity_not_found(self, mock_db, sec_config):
    """Test connection creation when entity doesn't exist."""
    mock_repository = Mock()
    mock_repository.execute_single.return_value = None  # Entity not found

    with patch(
      "robosystems.operations.providers.sec_provider.get_graph_repository"
    ) as mock_get_repo:
      mock_get_repo.return_value = mock_repository

      with pytest.raises(HTTPException) as exc_info:
        await create_sec_connection(
          entity_id="nonexistent",
          config=sec_config,
          user_id="user123",
          graph_id="graph123",
          db=mock_db,
        )

      assert exc_info.value.status_code == 404
      assert "Entity not found" in str(exc_info.value.detail)

  @pytest.mark.asyncio
  async def test_create_connection_with_cik_validation(self, mock_db, sec_config):
    """Test connection creation with CIK validation enabled."""
    mock_repository = Mock()
    mock_repository.execute_single.side_effect = [
      {"identifier": "entity123", "name": "Test Entity"},
      {"identifier": "entity123"},
    ]

    mock_cik_info = {
      "is_valid": True,
      "cik": "0001234567",
      "entity_name": "Validated Company Name",
      "ticker": "TEST",
    }

    with patch(
      "robosystems.operations.providers.sec_provider.get_graph_repository"
    ) as mock_get_repo:
      mock_get_repo.return_value = mock_repository

      with patch(
        "robosystems.operations.providers.sec_provider.validate_cik_with_sec_api"
      ) as mock_validate:
        mock_validate.return_value = mock_cik_info

        with patch(
          "robosystems.operations.providers.sec_provider.ConnectionService.create_connection"
        ) as mock_create:
          mock_create.return_value = {"connection_id": "conn123"}

          with patch(
            "robosystems.operations.providers.sec_provider.env.SEC_VALIDATE_CIK", True
          ):
            # Config without entity_name to test it gets set from validation
            config_no_name = SECConnectionConfig(cik="0001234567", entity_name=None)

            connection_id = await create_sec_connection(
              entity_id="entity123",
              config=config_no_name,
              user_id="user123",
              graph_id="graph123",
              db=mock_db,
            )

            assert connection_id == "conn123"
            mock_validate.assert_called_once_with("0001234567")

            # Check entity name was updated from validation
            call_kwargs = mock_create.call_args[1]
            assert call_kwargs["metadata"]["entity_name"] == "Validated Company Name"

  @pytest.mark.asyncio
  async def test_create_connection_invalid_cik(self, mock_db):
    """Test connection creation with invalid CIK."""
    mock_repository = Mock()
    mock_repository.execute_single.side_effect = [
      {"identifier": "entity123", "name": "Test Entity"},
      {"identifier": "entity123"},
    ]

    mock_cik_info = {"is_valid": False, "cik": "9999999999", "error": "CIK not found"}

    with patch(
      "robosystems.operations.providers.sec_provider.get_graph_repository"
    ) as mock_get_repo:
      mock_get_repo.return_value = mock_repository

      with patch(
        "robosystems.operations.providers.sec_provider.validate_cik_with_sec_api"
      ) as mock_validate:
        mock_validate.return_value = mock_cik_info

        with patch(
          "robosystems.operations.providers.sec_provider.ConnectionService.create_connection"
        ) as mock_create:
          mock_create.return_value = {"connection_id": "conn123"}

          with patch(
            "robosystems.operations.providers.sec_provider.env.SEC_VALIDATE_CIK", True
          ):
            config = SECConnectionConfig(cik="9999999999", entity_name=None)

            # Should still create connection but log warning
            connection_id = await create_sec_connection(
              entity_id="entity123",
              config=config,
              user_id="user123",
              graph_id="graph123",
              db=mock_db,
            )

            assert connection_id == "conn123"
            mock_validate.assert_called_once()


class TestSyncSECConnection:
  """Tests for SEC connection sync."""

  @pytest.mark.asyncio
  async def test_sync_not_implemented(self):
    """Test that sync raises NotImplementedError."""
    connection = {
      "connection_id": "conn123",
      "entity_id": "entity123",
      "credentials": {"cik": "0001234567"},
    }

    with pytest.raises(NotImplementedError) as exc_info:
      await sync_sec_connection(
        connection=connection, sync_options=None, graph_id="graph123"
      )

    assert "temporarily disabled" in str(exc_info.value)


class TestCleanupSECConnection:
  """Tests for SEC connection cleanup."""

  @pytest.mark.asyncio
  async def test_cleanup_connection(self):
    """Test cleanup removes CIK from entity."""
    mock_repository = Mock()
    mock_repository.execute_single.return_value = {"identifier": "entity123"}

    connection = {
      "connection_id": "conn123",
      "entity_id": "entity123",
      "credentials": {"cik": "0001234567"},
    }

    with patch(
      "robosystems.operations.providers.sec_provider.get_graph_repository"
    ) as mock_get_repo:
      mock_get_repo.return_value = mock_repository

      await cleanup_sec_connection(connection, "graph123")

      # Verify CIK was removed from entity
      mock_repository.execute_single.assert_called_once()
      call_args = mock_repository.execute_single.call_args
      assert "SET c.cik = null" in call_args[0][0]
      assert call_args[0][1]["entity_id"] == "entity123"

  @pytest.mark.asyncio
  async def test_cleanup_connection_entity_not_found(self):
    """Test cleanup when entity doesn't exist."""
    mock_repository = Mock()
    mock_repository.execute_single.return_value = None

    connection = {
      "connection_id": "conn123",
      "entity_id": "nonexistent",
      "credentials": {"cik": "0001234567"},
    }

    with patch(
      "robosystems.operations.providers.sec_provider.get_graph_repository"
    ) as mock_get_repo:
      mock_get_repo.return_value = mock_repository

      # Should not raise error, just return
      await cleanup_sec_connection(connection, "graph123")

      mock_repository.execute_single.assert_called_once()


class TestIntegration:
  """Integration tests for SEC provider."""

  @pytest.mark.asyncio
  @pytest.mark.integration
  async def test_full_connection_lifecycle(self):
    """Test full connection create, validate, and cleanup lifecycle."""
    # This would be an integration test with actual SEC API calls
    # Skipped in unit tests but useful for integration testing
    pass
