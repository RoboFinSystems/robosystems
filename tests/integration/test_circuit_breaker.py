"""
Integration tests for circuit breaker functionality in Kuzu client factory.
"""

import asyncio
import pytest

from robosystems.kuzu_api.client.factory import CircuitBreaker


class TestCircuitBreaker:
  """Test circuit breaker behavior under various conditions."""

  @pytest.mark.asyncio
  async def test_circuit_breaker_opens_after_failures(self):
    """Test that circuit breaker opens after threshold failures."""
    circuit_breaker = CircuitBreaker(failure_threshold=3, timeout=1)

    # Simulate failures
    for _ in range(3):
      await circuit_breaker.record_failure()

    # Circuit should now be open
    assert circuit_breaker.is_open is True
    assert await circuit_breaker.should_attempt() is False

  @pytest.mark.asyncio
  async def test_circuit_breaker_recovers_after_timeout(self):
    """Test that circuit breaker recovers after timeout period."""
    circuit_breaker = CircuitBreaker(failure_threshold=2, timeout=0.1)

    # Open the circuit
    for _ in range(2):
      await circuit_breaker.record_failure()

    assert circuit_breaker.is_open is True

    # Wait for timeout
    await asyncio.sleep(0.15)

    # Should attempt again after timeout
    assert await circuit_breaker.should_attempt() is True
    # Circuit should be closed after attempting
    assert circuit_breaker.is_open is False

  @pytest.mark.asyncio
  async def test_circuit_breaker_success_resets_failure_count(self):
    """Test that successful calls reset the failure count."""
    circuit_breaker = CircuitBreaker(failure_threshold=3, timeout=1)

    # Record some failures but not enough to open
    await circuit_breaker.record_failure()
    await circuit_breaker.record_failure()
    assert circuit_breaker.failure_count == 2
    assert circuit_breaker.is_open is False

    # Success should reset
    await circuit_breaker.record_success()
    assert circuit_breaker.failure_count == 0
    assert circuit_breaker.is_open is False

  @pytest.mark.asyncio
  async def test_circuit_breaker_async_safety(self):
    """Test that circuit breaker is async-safe."""
    circuit_breaker = CircuitBreaker(failure_threshold=10, timeout=1)

    async def record_failures():
      for _ in range(5):
        await circuit_breaker.record_failure()

    # Start multiple async tasks recording failures
    tasks = [asyncio.create_task(record_failures()) for _ in range(3)]
    await asyncio.gather(*tasks)

    # Should have recorded 15 failures total
    assert circuit_breaker.failure_count == 15
    assert circuit_breaker.is_open is True

  @pytest.mark.asyncio
  async def test_circuit_breaker_timeout_reset(self):
    """Test that circuit breaker resets properly after timeout."""
    circuit_breaker = CircuitBreaker(failure_threshold=2, timeout=0.05)

    # Open circuit
    await circuit_breaker.record_failure()
    await circuit_breaker.record_failure()
    assert circuit_breaker.is_open is True

    # Wait for timeout
    await asyncio.sleep(0.1)

    # Should reset on next attempt
    assert await circuit_breaker.should_attempt() is True
    assert circuit_breaker.failure_count == 0

  @pytest.mark.asyncio
  async def test_circuit_breaker_immediate_recovery(self):
    """Test immediate recovery after success."""
    circuit_breaker = CircuitBreaker(failure_threshold=3, timeout=10)

    # Almost open the circuit
    await circuit_breaker.record_failure()
    await circuit_breaker.record_failure()
    assert circuit_breaker.is_open is False

    # Success should reset
    await circuit_breaker.record_success()
    assert circuit_breaker.failure_count == 0

    # More failures but should start from 0
    await circuit_breaker.record_failure()
    assert circuit_breaker.failure_count == 1
    assert circuit_breaker.is_open is False
