"""A tier's subgraph memory budget is shared by the subgraphs that exist."""

from unittest.mock import MagicMock, patch

import pytest

from robosystems.config.graph_tier import GraphTierConfig
from robosystems.graph_api.core.ladybug import config
from robosystems.graph_api.core.ladybug.config import (
  count_subgraph_databases,
  get_database_memory_config,
  subgraph_share_mb,
)
from robosystems.graph_api.core.memory_manager import reopen_sibling_subgraphs

STANDARD = {
  "memory_per_db_mb": 1024,
  "subgraph_memory_budget_mb": 1024,
  "subgraph_memory_min_mb": 256,
}
TIERS = ("ladybug-standard", "ladybug-large", "ladybug-xlarge")


class TestShare:
  @pytest.mark.unit
  @pytest.mark.parametrize("count,expected", [(1, 1024), (2, 512), (3, 341)])
  def test_the_budget_is_divided_by_the_subgraphs_that_exist(self, count, expected):
    assert subgraph_share_mb(STANDARD, count) == expected

  @pytest.mark.unit
  def test_a_share_never_exceeds_the_parents_pool(self):
    tier = {**STANDARD, "subgraph_memory_budget_mb": 8192}
    assert subgraph_share_mb(tier, 1) == 1024

  @pytest.mark.unit
  def test_a_share_never_falls_under_the_minimum(self):
    assert subgraph_share_mb(STANDARD, 40) == 256

  @pytest.mark.unit
  def test_a_tier_without_a_budget_has_no_share(self):
    assert subgraph_share_mb({"memory_per_subgraph_mb": 2048}, 1) is None


class TestCount:
  @pytest.mark.unit
  def test_counts_subgraph_files_and_the_one_being_created(self, tmp_path):
    for name in (
      "kg1.lbdb",
      "kg1_a.lbdb",
      "kg1_b.lbdb",
      "kg1_b.lbdb.wal",
      "kg1_c-wip.lbdb",
    ):
      (tmp_path / name).write_text("")
    with patch.object(config.env, "LBUG_DATABASE_PATH", str(tmp_path)):
      assert count_subgraph_databases("kg1_new") == 3
      assert count_subgraph_databases("kg1_a") == 2

  @pytest.mark.unit
  def test_a_missing_directory_counts_only_the_one_asked_about(self, tmp_path):
    with patch.object(config.env, "LBUG_DATABASE_PATH", str(tmp_path / "absent")):
      assert count_subgraph_databases("kg1_new") == 1


class TestMemoryConfig:
  @pytest.mark.unit
  def test_a_subgraph_opens_at_its_share(self):
    with (
      patch.object(config.env, "get_lbug_tier_config", return_value=STANDARD),
      patch.object(config, "count_subgraph_databases", return_value=2),
    ):
      assert get_database_memory_config("kg1_a") == 512

  @pytest.mark.unit
  def test_a_parent_is_untouched(self):
    with patch.object(config.env, "get_lbug_tier_config", return_value=STANDARD):
      assert get_database_memory_config("kg1") == 1024

  @pytest.mark.unit
  def test_the_fixed_figure_still_serves_a_tier_without_a_budget(self):
    tier = {"memory_per_db_mb": 9216, "memory_per_subgraph_mb": 2048}
    with patch.object(config.env, "get_lbug_tier_config", return_value=tier):
      assert get_database_memory_config("sec_historical") == 2048


class TestReopen:
  @pytest.mark.unit
  def test_idle_siblings_are_closed_and_busy_ones_left(self):
    pool = MagicMock()
    pool.list_databases.return_value = ["kg1", "kg1_a", "kg1_b", "kg1_new"]
    busy = {"kg1_b"}
    closed: set[str] = set()
    pool.has_active_connections.side_effect = lambda n: n in busy and n not in closed
    pool.close_database_connections.side_effect = closed.add

    assert reopen_sibling_subgraphs(pool, "kg1_new") == ["kg1_a"]

  @pytest.mark.unit
  def test_a_parent_graph_changes_nothing(self):
    pool = MagicMock()
    assert reopen_sibling_subgraphs(pool, "kg1") == []
    pool.list_databases.assert_not_called()


class TestTierBudgets:
  """The shipped numbers have to add up on each instance."""

  @pytest.mark.unit
  @pytest.mark.parametrize("tier", TIERS)
  def test_parent_plus_budget_plus_staging_fits_the_engine_budget(self, tier):
    instance = GraphTierConfig.get_instance_config(tier)
    staging_mb = int(str(instance["duckdb_memory_limit"]).rstrip("GB")) * 1024
    used = (
      instance["memory_per_db_mb"] + instance["subgraph_memory_budget_mb"] + staging_mb
    )
    assert used <= instance["max_memory_mb"]

  @pytest.mark.unit
  @pytest.mark.parametrize("tier", TIERS)
  def test_a_lone_subgraph_gets_more_than_the_minimum(self, tier):
    instance = GraphTierConfig.get_instance_config(tier)
    assert subgraph_share_mb(instance, 1) > instance["subgraph_memory_min_mb"]
