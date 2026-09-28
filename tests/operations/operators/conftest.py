import pytest

from robosystems.operations.operators import operator_registry


@pytest.fixture(autouse=True)
def _restore_operator_registry():
  """Tests that call `clear_registry()` would otherwise leave the import-time
  registrations gone for every later test on the same xdist worker."""
  operators = dict(operator_registry._OPERATORS)
  adapters_loaded = list(operator_registry._adapter_operators_loaded)
  yield
  operator_registry._OPERATORS.clear()
  operator_registry._OPERATORS.update(operators)
  operator_registry._adapter_operators_loaded[:] = adapters_loaded
