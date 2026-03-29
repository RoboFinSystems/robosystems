"""Report generation operations — FactGridBuilder, guard rails, report lifecycle."""

from .fact_grid import FactGrid, FactRow, build_fact_grid
from .guard_rails import ValidationResult, validate_report

__all__ = [
  "FactGrid",
  "FactRow",
  "ValidationResult",
  "build_fact_grid",
  "validate_report",
]
