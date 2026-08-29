"""Operator implementations.

Importing this module registers all operators in the operator_registry.
"""

from robosystems.operations.operators.implementations import analyst
from robosystems.operations.operators.implementations.mapping import operator as mapping

__all__ = ["analyst", "mapping"]
