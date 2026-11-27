"""The checkpoint threshold has to stay inside the buffer pool it protects."""

import pytest

from robosystems.graph_api.core.ladybug.pool import (
  DEFAULT_CHECKPOINT_THRESHOLD,
  MIN_CHECKPOINT_THRESHOLD,
  SEC_CHECKPOINT_THRESHOLD,
  checkpoint_threshold_for,
)

MB = 1024 * 1024


@pytest.mark.unit
@pytest.mark.parametrize("pool_mb", [128, 384, 1024, 2048, 8192])
def test_the_threshold_is_always_below_the_pool(pool_mb):
  assert checkpoint_threshold_for("kg123_scratch", pool_mb * MB) < pool_mb * MB


@pytest.mark.unit
def test_a_subgraph_pool_checkpoints_at_a_quarter_of_it():
  assert checkpoint_threshold_for("kg123_scratch", 384 * MB) == 96 * MB


@pytest.mark.unit
def test_a_large_pool_keeps_the_default():
  assert checkpoint_threshold_for("kg123", 8192 * MB) == DEFAULT_CHECKPOINT_THRESHOLD


@pytest.mark.unit
def test_sec_never_exceeds_its_own_figure():
  assert checkpoint_threshold_for("sec", 32768 * MB) == SEC_CHECKPOINT_THRESHOLD


@pytest.mark.unit
def test_a_tiny_pool_still_gets_a_workable_threshold():
  assert checkpoint_threshold_for("kg123_scratch", 64 * MB) == MIN_CHECKPOINT_THRESHOLD
