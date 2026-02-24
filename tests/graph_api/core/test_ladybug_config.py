"""LadybugDB configuration tests."""

from unittest.mock import patch

import pytest

from robosystems.graph_api.core.ladybug.config import (
    _memory_overrides,
    get_database_memory_config,
    get_ladybug_memory_override,
    set_ladybug_memory_override,
)


@pytest.fixture(autouse=True)
def clear_overrides():
    """Clear memory overrides between tests."""
    _memory_overrides.clear()
    yield
    _memory_overrides.clear()


class TestSetLadybugMemoryOverride:
    def test_set_override_for_graph(self):
        old = set_ladybug_memory_override(50000, graph_id="sec")
        assert old is None
        assert _memory_overrides["sec"] == 50000

    def test_returns_previous_value(self):
        set_ladybug_memory_override(10000, graph_id="sec")
        old = set_ladybug_memory_override(50000, graph_id="sec")
        assert old == 10000

    def test_clear_override_for_graph(self):
        set_ladybug_memory_override(50000, graph_id="sec")
        old = set_ladybug_memory_override(None, graph_id="sec")
        assert old == 50000
        assert "sec" not in _memory_overrides

    def test_clear_all_overrides(self):
        set_ladybug_memory_override(10000, graph_id="sec")
        set_ladybug_memory_override(20000, graph_id="kg123")
        set_ladybug_memory_override(None, graph_id=None)
        assert len(_memory_overrides) == 0

    def test_set_without_graph_id_raises(self):
        with pytest.raises(ValueError, match="graph_id is required"):
            set_ladybug_memory_override(50000, graph_id=None)


class TestGetLadybugMemoryOverride:
    def test_no_override(self):
        assert get_ladybug_memory_override("sec") is None

    def test_with_override(self):
        set_ladybug_memory_override(50000, graph_id="sec")
        assert get_ladybug_memory_override("sec") == 50000

    def test_different_graph(self):
        set_ladybug_memory_override(50000, graph_id="sec")
        assert get_ladybug_memory_override("kg123") is None

    def test_legacy_no_graph_id(self):
        assert get_ladybug_memory_override() is None

    def test_legacy_returns_any_override(self):
        set_ladybug_memory_override(50000, graph_id="sec")
        assert get_ladybug_memory_override() == 50000


class TestGetDatabaseMemoryConfig:
    def test_uses_override_when_set(self):
        set_ladybug_memory_override(99999, graph_id="sec")
        result = get_database_memory_config("sec")
        assert result == 99999

    def test_no_override_uses_tier_config(self):
        result = get_database_memory_config("sec")
        assert isinstance(result, int)
        assert result > 0

    def test_subgraph_detection(self):
        # Subgraphs have underscores: sec_historical, kg123_dev
        result = get_database_memory_config("sec_historical")
        assert isinstance(result, int)
        assert result > 0

    def test_parent_graph(self):
        result = get_database_memory_config("sec")
        assert isinstance(result, int)

    def test_none_database_name(self):
        result = get_database_memory_config(None)
        assert isinstance(result, int)
        assert result > 0
