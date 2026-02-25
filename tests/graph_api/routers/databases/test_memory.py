"""Tests for memory management router endpoints."""

from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from robosystems.graph_api.routers.databases import memory as memory_module

MODULE = "robosystems.graph_api.routers.databases.memory"


@pytest.fixture
def memory_client():
  """Create TestClient with memory router."""
  app = FastAPI()
  app.include_router(memory_module.router)
  return TestClient(app)


class TestBoostMemory:
  """Test cases for POST /databases/{graph_id}/memory/boost endpoint."""

  @pytest.mark.unit
  def test_boost_both_success(self, memory_client):
    """Test boosting both DuckDB and LadybugDB memory."""
    with (
      patch(f"{MODULE}.ensure_duckdb_memory_boosted", return_value="55GB") as mock_duck,
      patch(f"{MODULE}.ensure_ladybug_memory_boosted", return_value=8192) as mock_lbug,
    ):
      response = memory_client.post(
        "/databases/testgraph/memory/boost",
        json={"target": "both"},
      )

      assert response.status_code == 200
      data = response.json()
      assert data["graph_id"] == "testgraph"
      assert data["target"] == "both"
      assert data["duckdb_boosted"] is True
      assert data["duckdb_boost_mb"] == 55 * 1024  # 55GB -> MB
      assert data["ladybug_boosted"] is True
      assert data["ladybug_boost_mb"] == 8192
      assert "DuckDB" in data["message"]
      assert "LadybugDB" in data["message"]

      mock_duck.assert_called_once_with("testgraph")
      mock_lbug.assert_called_once_with("testgraph")

  @pytest.mark.unit
  def test_boost_duckdb_only(self, memory_client):
    """Test boosting only DuckDB memory."""
    with (
      patch(
        f"{MODULE}.ensure_duckdb_memory_boosted", return_value="4096MB"
      ) as mock_duck,
      patch(f"{MODULE}.ensure_ladybug_memory_boosted") as mock_lbug,
    ):
      response = memory_client.post(
        "/databases/testgraph/memory/boost",
        json={"target": "duckdb"},
      )

      assert response.status_code == 200
      data = response.json()
      assert data["duckdb_boosted"] is True
      assert data["duckdb_boost_mb"] == 4096
      assert data["ladybug_boosted"] is False
      assert data["ladybug_boost_mb"] is None

      mock_duck.assert_called_once_with("testgraph")
      mock_lbug.assert_not_called()

  @pytest.mark.unit
  def test_boost_ladybug_only(self, memory_client):
    """Test boosting only LadybugDB memory."""
    with (
      patch(f"{MODULE}.ensure_duckdb_memory_boosted") as mock_duck,
      patch(f"{MODULE}.ensure_ladybug_memory_boosted", return_value=16384) as mock_lbug,
    ):
      response = memory_client.post(
        "/databases/testgraph/memory/boost",
        json={"target": "ladybug"},
      )

      assert response.status_code == 200
      data = response.json()
      assert data["duckdb_boosted"] is False
      assert data["duckdb_boost_mb"] is None
      assert data["ladybug_boosted"] is True
      assert data["ladybug_boost_mb"] == 16384

      mock_duck.assert_not_called()
      mock_lbug.assert_called_once_with("testgraph")

  @pytest.mark.unit
  def test_boost_already_boosted(self, memory_client):
    """Test boost when memory is already boosted returns correct status."""
    with (
      patch(f"{MODULE}.ensure_duckdb_memory_boosted", return_value=None),
      patch(f"{MODULE}.is_duckdb_memory_boosted", return_value=True),
      patch(f"{MODULE}.ensure_ladybug_memory_boosted", return_value=None),
      patch(f"{MODULE}.is_ladybug_memory_boosted", return_value=True),
    ):
      response = memory_client.post(
        "/databases/testgraph/memory/boost",
        json={"target": "both"},
      )

      assert response.status_code == 200
      data = response.json()
      # Already boosted - return True but no MB values
      assert data["duckdb_boosted"] is True
      assert data["ladybug_boosted"] is True
      assert data["duckdb_boost_mb"] is None
      assert data["ladybug_boost_mb"] is None

  @pytest.mark.unit
  def test_boost_no_boost_configured(self, memory_client):
    """Test boost when no boost is configured for the tier."""
    with (
      patch(f"{MODULE}.ensure_duckdb_memory_boosted", return_value=None),
      patch(f"{MODULE}.is_duckdb_memory_boosted", return_value=False),
      patch(f"{MODULE}.ensure_ladybug_memory_boosted", return_value=None),
      patch(f"{MODULE}.is_ladybug_memory_boosted", return_value=False),
    ):
      response = memory_client.post(
        "/databases/testgraph/memory/boost",
        json={"target": "both"},
      )

      assert response.status_code == 200
      data = response.json()
      assert data["duckdb_boosted"] is False
      assert data["ladybug_boosted"] is False
      assert "No boost applied" in data["message"]

  @pytest.mark.unit
  def test_boost_default_target_is_both(self, memory_client):
    """Test that the default boost target is 'both'."""
    with (
      patch(f"{MODULE}.ensure_duckdb_memory_boosted", return_value="10GB"),
      patch(f"{MODULE}.ensure_ladybug_memory_boosted", return_value=4096),
    ):
      # Send empty body -- default should be "both"
      response = memory_client.post(
        "/databases/testgraph/memory/boost",
      )

      assert response.status_code == 200
      data = response.json()
      assert data["target"] == "both"

  @pytest.mark.unit
  def test_boost_duckdb_unparseable_value(self, memory_client):
    """Test that an unparseable DuckDB boost value is handled gracefully."""
    with (
      patch(f"{MODULE}.ensure_duckdb_memory_boosted", return_value="unknown_format"),
      patch(f"{MODULE}.ensure_ladybug_memory_boosted", return_value=None),
      patch(f"{MODULE}.is_ladybug_memory_boosted", return_value=False),
    ):
      response = memory_client.post(
        "/databases/testgraph/memory/boost",
        json={"target": "both"},
      )

      assert response.status_code == 200
      data = response.json()
      assert data["duckdb_boosted"] is True
      assert data["duckdb_boost_mb"] is None  # Could not parse

  @pytest.mark.unit
  def test_boost_invalid_target(self, memory_client):
    """Test that an invalid target value returns a validation error."""
    response = memory_client.post(
      "/databases/testgraph/memory/boost",
      json={"target": "invalid"},
    )

    assert response.status_code == 422


class TestRestoreMemory:
  """Test cases for POST /databases/{graph_id}/memory/restore endpoint."""

  @pytest.mark.unit
  def test_restore_both_restored(self, memory_client):
    """Test restoring memory when both were boosted."""
    with (
      patch(f"{MODULE}.restore_duckdb_memory", return_value=True) as mock_duck,
      patch(f"{MODULE}.restore_ladybug_memory", return_value=True) as mock_lbug,
    ):
      response = memory_client.post("/databases/testgraph/memory/restore")

      assert response.status_code == 200
      data = response.json()
      assert data["graph_id"] == "testgraph"
      assert data["duckdb_restored"] is True
      assert data["ladybug_restored"] is True
      assert "DuckDB" in data["message"]
      assert "LadybugDB" in data["message"]

      mock_duck.assert_called_once_with("testgraph")
      mock_lbug.assert_called_once_with("testgraph")

  @pytest.mark.unit
  def test_restore_duckdb_only(self, memory_client):
    """Test restoring when only DuckDB was boosted."""
    with (
      patch(f"{MODULE}.restore_duckdb_memory", return_value=True),
      patch(f"{MODULE}.restore_ladybug_memory", return_value=False),
    ):
      response = memory_client.post("/databases/testgraph/memory/restore")

      assert response.status_code == 200
      data = response.json()
      assert data["duckdb_restored"] is True
      assert data["ladybug_restored"] is False
      assert "DuckDB" in data["message"]

  @pytest.mark.unit
  def test_restore_nothing_to_restore(self, memory_client):
    """Test restoring when nothing was boosted."""
    with (
      patch(f"{MODULE}.restore_duckdb_memory", return_value=False),
      patch(f"{MODULE}.restore_ladybug_memory", return_value=False),
    ):
      response = memory_client.post("/databases/testgraph/memory/restore")

      assert response.status_code == 200
      data = response.json()
      assert data["duckdb_restored"] is False
      assert data["ladybug_restored"] is False
      assert "No memory boost was active" in data["message"]


class TestReleaseMemory:
  """Test cases for POST /databases/{graph_id}/memory/release endpoint."""

  @pytest.mark.unit
  def test_release_both_success(self, memory_client):
    """Test releasing both DuckDB and LadybugDB memory."""
    with (
      patch(
        f"{MODULE}.release_duckdb_memory",
        return_value={"connections_closed": 3, "success": True},
      ) as mock_duck,
      patch(
        f"{MODULE}.release_ladybug_memory",
        return_value={"success": True},
      ) as mock_lbug,
    ):
      response = memory_client.post(
        "/databases/testgraph/memory/release",
        json={"target": "both", "aggressive": True},
      )

      assert response.status_code == 200
      data = response.json()
      assert data["graph_id"] == "testgraph"
      assert data["target"] == "both"
      assert data["duckdb_connections_closed"] == 3
      assert data["duckdb_released"] is True
      assert data["ladybug_released"] is True
      assert "DuckDB (3 connections)" in data["message"]
      assert "LadybugDB" in data["message"]

      mock_duck.assert_called_once_with("testgraph")
      mock_lbug.assert_called_once_with("testgraph", aggressive=True)

  @pytest.mark.unit
  def test_release_duckdb_only(self, memory_client):
    """Test releasing only DuckDB memory."""
    with (
      patch(
        f"{MODULE}.release_duckdb_memory",
        return_value={"connections_closed": 2, "success": True},
      ),
      patch(f"{MODULE}.release_ladybug_memory") as mock_lbug,
    ):
      response = memory_client.post(
        "/databases/testgraph/memory/release",
        json={"target": "duckdb"},
      )

      assert response.status_code == 200
      data = response.json()
      assert data["duckdb_released"] is True
      assert data["duckdb_connections_closed"] == 2
      assert data["ladybug_released"] is False

      mock_lbug.assert_not_called()

  @pytest.mark.unit
  def test_release_ladybug_only(self, memory_client):
    """Test releasing only LadybugDB memory."""
    with (
      patch(f"{MODULE}.release_duckdb_memory") as mock_duck,
      patch(
        f"{MODULE}.release_ladybug_memory",
        return_value={"success": True},
      ),
    ):
      response = memory_client.post(
        "/databases/testgraph/memory/release",
        json={"target": "ladybug", "aggressive": False},
      )

      assert response.status_code == 200
      data = response.json()
      assert data["duckdb_released"] is False
      assert data["ladybug_released"] is True

      mock_duck.assert_not_called()

  @pytest.mark.unit
  def test_release_no_connections(self, memory_client):
    """Test releasing when no connections exist."""
    with (
      patch(
        f"{MODULE}.release_duckdb_memory",
        return_value={"connections_closed": 0, "success": False},
      ),
      patch(
        f"{MODULE}.release_ladybug_memory",
        return_value={"success": False},
      ),
    ):
      response = memory_client.post(
        "/databases/testgraph/memory/release",
        json={"target": "both"},
      )

      assert response.status_code == 200
      data = response.json()
      assert data["duckdb_released"] is False
      assert data["ladybug_released"] is False
      assert "No memory released" in data["message"]

  @pytest.mark.unit
  def test_release_default_is_both_aggressive(self, memory_client):
    """Test that release defaults to target=both and aggressive=True."""
    with (
      patch(
        f"{MODULE}.release_duckdb_memory",
        return_value={"connections_closed": 0, "success": True},
      ),
      patch(
        f"{MODULE}.release_ladybug_memory",
        return_value={"success": True},
      ) as mock_lbug,
    ):
      response = memory_client.post(
        "/databases/testgraph/memory/release",
      )

      assert response.status_code == 200
      data = response.json()
      assert data["target"] == "both"

      # Verify aggressive=True was passed (default)
      mock_lbug.assert_called_once_with("testgraph", aggressive=True)

  @pytest.mark.unit
  def test_release_non_aggressive(self, memory_client):
    """Test release with aggressive=False."""
    with (
      patch(
        f"{MODULE}.release_duckdb_memory",
        return_value={"connections_closed": 0, "success": False},
      ),
      patch(
        f"{MODULE}.release_ladybug_memory",
        return_value={"success": True},
      ) as mock_lbug,
    ):
      response = memory_client.post(
        "/databases/testgraph/memory/release",
        json={"target": "ladybug", "aggressive": False},
      )

      assert response.status_code == 200
      mock_lbug.assert_called_once_with("testgraph", aggressive=False)


class TestMemoryStatus:
  """Test cases for GET /databases/{graph_id}/memory/status endpoint."""

  @pytest.mark.unit
  def test_status_not_boosted(self, memory_client):
    """Test status when no memory boost is active."""
    with (
      patch(f"{MODULE}.is_duckdb_memory_boosted", return_value=False),
      patch(f"{MODULE}.is_ladybug_memory_boosted", return_value=False),
    ):
      response = memory_client.get("/databases/testgraph/memory/status")

      assert response.status_code == 200
      data = response.json()
      assert data["graph_id"] == "testgraph"
      assert data["duckdb_boosted"] is False
      assert data["ladybug_boosted"] is False

  @pytest.mark.unit
  def test_status_both_boosted(self, memory_client):
    """Test status when both systems are boosted."""
    with (
      patch(f"{MODULE}.is_duckdb_memory_boosted", return_value=True),
      patch(f"{MODULE}.is_ladybug_memory_boosted", return_value=True),
    ):
      response = memory_client.get("/databases/testgraph/memory/status")

      assert response.status_code == 200
      data = response.json()
      assert data["duckdb_boosted"] is True
      assert data["ladybug_boosted"] is True

  @pytest.mark.unit
  def test_status_duckdb_only_boosted(self, memory_client):
    """Test status when only DuckDB is boosted."""
    with (
      patch(f"{MODULE}.is_duckdb_memory_boosted", return_value=True),
      patch(f"{MODULE}.is_ladybug_memory_boosted", return_value=False),
    ):
      response = memory_client.get("/databases/testgraph/memory/status")

      assert response.status_code == 200
      data = response.json()
      assert data["duckdb_boosted"] is True
      assert data["ladybug_boosted"] is False

  @pytest.mark.unit
  def test_status_different_graph_ids(self, memory_client):
    """Test that graph_id is correctly returned in response."""
    with (
      patch(f"{MODULE}.is_duckdb_memory_boosted", return_value=False),
      patch(f"{MODULE}.is_ladybug_memory_boosted", return_value=False),
    ):
      response = memory_client.get("/databases/sec/memory/status")

      assert response.status_code == 200
      data = response.json()
      assert data["graph_id"] == "sec"


class TestMemoryModels:
  """Tests for Pydantic request/response models."""

  @pytest.mark.unit
  def test_memory_boost_request_default(self):
    """Test MemoryBoostRequest default target."""
    req = memory_module.MemoryBoostRequest()
    assert req.target == "both"

  @pytest.mark.unit
  def test_memory_boost_request_valid_targets(self):
    """Test all valid MemoryBoostRequest targets."""
    for target in ["duckdb", "ladybug", "both"]:
      req = memory_module.MemoryBoostRequest(target=target)
      assert req.target == target

  @pytest.mark.unit
  def test_memory_release_request_defaults(self):
    """Test MemoryReleaseRequest defaults."""
    req = memory_module.MemoryReleaseRequest()
    assert req.target == "both"
    assert req.aggressive is True

  @pytest.mark.unit
  def test_memory_target_enum_values(self):
    """Test MemoryTarget enum has expected values."""
    assert memory_module.MemoryTarget.DUCKDB == "duckdb"
    assert memory_module.MemoryTarget.LADYBUG == "ladybug"
    assert memory_module.MemoryTarget.BOTH == "both"
