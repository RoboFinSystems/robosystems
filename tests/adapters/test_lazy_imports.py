"""Every lazily exported adapter name resolves on the package."""

import pytest

import robosystems.adapters as adapters


@pytest.mark.parametrize("name", sorted(adapters._LAZY_IMPORTS))
def test_lazy_import_resolves(name: str) -> None:
  assert getattr(adapters, name) is not None
