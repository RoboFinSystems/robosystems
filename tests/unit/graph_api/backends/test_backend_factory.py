import pytest
from robosystems.graph_api.backends import get_backend, KuzuBackend, Neo4jBackend


def test_backend_factory_kuzu(monkeypatch, tmp_path):
  monkeypatch.setenv("BACKEND_TYPE", "kuzu")
  monkeypatch.setattr("robosystems.graph_api.backends._backend_instance", None)
  monkeypatch.setattr(
    "robosystems.graph_api.backends.kuzu.KuzuBackend.__init__",
    lambda self: setattr(self, "data_path", str(tmp_path))
    or setattr(self, "_engines", {}),
  )

  backend = get_backend()

  assert isinstance(backend, KuzuBackend)


def test_backend_factory_neo4j_community(monkeypatch):
  monkeypatch.setenv("BACKEND_TYPE", "neo4j_community")
  monkeypatch.setattr("robosystems.graph_api.backends._backend_instance", None)

  backend = get_backend()

  assert isinstance(backend, Neo4jBackend)
  assert backend.enterprise is False


def test_backend_factory_neo4j_enterprise(monkeypatch):
  monkeypatch.setenv("BACKEND_TYPE", "neo4j_enterprise")
  monkeypatch.setattr("robosystems.graph_api.backends._backend_instance", None)

  backend = get_backend()

  assert isinstance(backend, Neo4jBackend)
  assert backend.enterprise is True


def test_backend_factory_invalid_type(monkeypatch):
  monkeypatch.setenv("BACKEND_TYPE", "invalid_backend")
  monkeypatch.setattr("robosystems.graph_api.backends._backend_instance", None)

  with pytest.raises(ValueError, match="Unknown BACKEND_TYPE"):
    get_backend()


def test_backend_factory_singleton(monkeypatch, tmp_path):
  monkeypatch.setenv("BACKEND_TYPE", "kuzu")
  monkeypatch.setattr("robosystems.graph_api.backends._backend_instance", None)
  monkeypatch.setattr(
    "robosystems.graph_api.backends.kuzu.KuzuBackend.__init__",
    lambda self: setattr(self, "data_path", str(tmp_path))
    or setattr(self, "_engines", {}),
  )

  backend1 = get_backend()
  backend2 = get_backend()

  assert backend1 is backend2
