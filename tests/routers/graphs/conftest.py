import asyncio
import pytest


@pytest.fixture(scope="function", autouse=True)
def event_loop_reset():
  """
  Reset event loop and global state for each test to prevent pollution.

  This fixture ensures that each test gets a fresh event loop and
  resets global singletons that might cache event loop state,
  preventing issues where one test closes the loop and breaks
  subsequent tests that need async functionality.
  """
  yield

  # Reset global SSE event storage to prevent reusing stale async clients
  import robosystems.middleware.sse.event_storage as event_storage_module

  event_storage_module._event_storage = None

  # After test completes, ensure we have a clean event loop for the next test
  try:
    loop = asyncio.get_event_loop()
    if loop.is_closed():
      # Create a new loop if the current one is closed
      loop = asyncio.new_event_loop()
      asyncio.set_event_loop(loop)
  except RuntimeError:
    # No event loop exists, create one
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
