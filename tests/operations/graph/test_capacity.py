"""Tests for the shared refuse-the-sale capacity rule."""

from unittest.mock import AsyncMock, patch

import pytest

_MANAGER = "robosystems.middleware.graph.allocation_manager.LadybugAllocationManager"


@pytest.mark.unit
@pytest.mark.asyncio
class TestTierCapacityStatus:
  async def _status(self, tier="ladybug-large", *, value=None, raises=None):
    from robosystems.operations.graph.capacity import tier_capacity_status

    with patch(_MANAGER) as manager_cls:
      manager_cls.return_value.check_tier_capacity = AsyncMock(
        side_effect=raises, return_value=value
      )
      return await tier_capacity_status(tier)

  async def test_ready_passes_through(self):
    assert await self._status(value="ready") == "ready"

  async def test_scalable_is_not_ready(self):
    # Nothing on a sale path raises desired capacity; headroom is not a slot.
    assert await self._status(value="scalable") == "at_capacity"

  async def test_at_capacity_passes_through(self):
    assert await self._status(value="at_capacity") == "at_capacity"

  async def test_lookup_failure_reads_as_no_capacity(self):
    assert await self._status(raises=RuntimeError("dynamodb down")) == "at_capacity"

  async def test_unknown_tier_reads_as_no_capacity(self):
    assert await self._status(tier="not-a-tier", value="ready") == "at_capacity"
