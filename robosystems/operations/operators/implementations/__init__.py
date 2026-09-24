"""Operator implementations; importing this module registers them."""

from robosystems.operations.operators.implementations import analyst
from robosystems.operations.operators.implementations.mapping import operator as mapping

__all__ = ["analyst", "mapping"]
