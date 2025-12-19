import pytest

from robosystems.graph_api.backends import LadybugBackend, Neo4jBackend, get_backend


def test_backend_factory_lbug(monkeypatch, tmp_path):
  from unittest.mock import MagicMock

  monkeypatch.setenv("GRAPH_BACKEND_TYPE", "ladybug")
  monkeypatch.setattr("robosystems.graph_api.backends._backend_instance", None)

  # Mock the global ConnectionPool
  mock_pool = MagicMock()
  monkeypatch.setattr(
    "robosystems.graph_api.backends.lbug.get_connection_pool", lambda: mock_pool
  )

  backend = get_backend()

  assert isinstance(backend, LadybugBackend)
  assert backend.connection_pool is mock_pool


def test_backend_factory_neo4j_community(monkeypatch):
  monkeypatch.setenv("GRAPH_BACKEND_TYPE", "neo4j_community")
  monkeypatch.setenv("NEO4J_URI", "bolt://localhost:7687")
  monkeypatch.setenv("NEO4J_USERNAME", "neo4j")
  monkeypatch.setenv("NEO4J_PASSWORD", "password")
  monkeypatch.setattr("robosystems.config.env.GRAPH_BACKEND_TYPE", "neo4j_community")
  monkeypatch.setattr("robosystems.config.env.NEO4J_URI", "bolt://localhost:7687")
  monkeypatch.setattr("robosystems.config.env.NEO4J_USERNAME", "neo4j")
  monkeypatch.setattr("robosystems.config.env.NEO4J_PASSWORD", "password")
  monkeypatch.setattr("robosystems.graph_api.backends._backend_instance", None)

  backend = get_backend()

  assert isinstance(backend, Neo4jBackend)
  assert backend.enterprise is False


def test_backend_factory_neo4j_enterprise(monkeypatch):
  monkeypatch.setenv("GRAPH_BACKEND_TYPE", "neo4j_enterprise")
  monkeypatch.setenv("NEO4J_URI", "bolt://localhost:7687")
  monkeypatch.setenv("NEO4J_USERNAME", "neo4j")
  monkeypatch.setenv("NEO4J_PASSWORD", "password")
  monkeypatch.setattr("robosystems.config.env.GRAPH_BACKEND_TYPE", "neo4j_enterprise")
  monkeypatch.setattr("robosystems.config.env.NEO4J_URI", "bolt://localhost:7687")
  monkeypatch.setattr("robosystems.config.env.NEO4J_USERNAME", "neo4j")
  monkeypatch.setattr("robosystems.config.env.NEO4J_PASSWORD", "password")
  monkeypatch.setattr("robosystems.graph_api.backends._backend_instance", None)

  backend = get_backend()

  assert isinstance(backend, Neo4jBackend)
  assert backend.enterprise is True


def test_backend_factory_invalid_type(monkeypatch):
  monkeypatch.setenv("GRAPH_BACKEND_TYPE", "invalid_backend")
  monkeypatch.setattr("robosystems.config.env.GRAPH_BACKEND_TYPE", "invalid_backend")
  monkeypatch.setattr("robosystems.graph_api.backends._backend_instance", None)

  with pytest.raises(ValueError, match="Unknown GRAPH_BACKEND_TYPE"):
    get_backend()


def test_backend_factory_singleton(monkeypatch, tmp_path):
  from unittest.mock import MagicMock

  monkeypatch.setenv("GRAPH_BACKEND_TYPE", "ladybug")
  monkeypatch.setattr("robosystems.graph_api.backends._backend_instance", None)

  # Mock the global ConnectionPool
  mock_pool = MagicMock()
  monkeypatch.setattr(
    "robosystems.graph_api.backends.lbug.get_connection_pool", lambda: mock_pool
  )

  backend1 = get_backend()
  backend2 = get_backend()

  assert backend1 is backend2
