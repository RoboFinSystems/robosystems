"""Report generation operations — fact generation, structure rendering, guard rails."""

from .fact_grid import (
  FactGrid,
  FactRow,
  PeriodSpec,
  ReportFact,
  ReportFacts,
  generate_report_facts,
  render_structure_view,
)
from .guard_rails import ValidationResult, validate_report

__all__ = [
  "FactGrid",
  "FactRow",
  "PeriodSpec",
  "ReportFact",
  "ReportFacts",
  "ValidationResult",
  "generate_report_facts",
  "render_structure_view",
  "validate_report",
]
