"""Shape tests for Phase θ canonical-concept columns folded into 0002.

Static checks — verifies the public-schema column additions, the
tenant-schema helper exists, the tenant copy column list carries the
new fields, and the downgrade path drops them in the reverse order.
"""

from __future__ import annotations

import importlib.util as _util
from pathlib import Path

from sqlalchemy import CheckConstraint

from robosystems.db import extensions as extensions_db
from robosystems.models.extensions import Structure
from robosystems.taxonomy.writer import _ELEMENT_COLS

_MIGRATION_PATH = (
  Path(__file__).resolve().parents[2]
  / "migrations"
  / "extensions"
  / "versions"
  / "0002_taxonomy_library.py"
)

_spec = _util.spec_from_file_location("mig_0002_theta", _MIGRATION_PATH)
assert _spec is not None and _spec.loader is not None
mig_0002 = _util.module_from_spec(_spec)
_spec.loader.exec_module(mig_0002)


class TestTenantHelpers:
  def test_add_helper_exists(self) -> None:
    assert callable(mig_0002._add_phase_theta_columns_to_tenant)

  def test_drop_helper_exists(self) -> None:
    assert callable(mig_0002._drop_phase_theta_columns_from_tenant)


class TestTenantWriterCols:
  def test_element_cols_include_theta_fields(self) -> None:
    """Library → tenant copy must carry the new canonical-concept fields;
    otherwise tenant graphs end up with NULL agent_id / empty aliases
    even when the library seed has populated them."""
    for col in ("agent_id", "aliases", "embedding"):
      assert col in _ELEMENT_COLS, f"_ELEMENT_COLS missing {col!r}"


class TestMetricTypeInStructureCheck:
  def test_metric_is_allowed_block_type(self) -> None:
    """The Phase η metric block type must be admissible in the widened
    block_type CHECK so tenant Structures can be created with it."""
    assert "'metric'" in mig_0002._WIDENED_BLOCK_TYPE_CHECK

  def test_metric_is_allowed_by_runtime_structure_model(self) -> None:
    """Fresh tenant schemas are created from SQLAlchemy metadata, so the
    runtime model CHECK must match the migration vocabulary."""
    checks = [
      c
      for c in Structure.__table__.constraints
      if isinstance(c, CheckConstraint) and c.name == "check_block_type"
    ]
    assert len(checks) == 1
    assert "'metric'" in str(checks[0].sqltext)

  def test_fresh_tenant_check_widener_mentions_metric(self) -> None:
    """Provisioning widens checks after metadata create_all; it must not
    narrow the freshly-created block_type vocabulary."""
    statements: list[str] = []

    class FakeConn:
      def execute(self, statement):
        statements.append(str(statement))

    extensions_db._widen_library_checks(FakeConn(), "kg0123456789abcdef")
    assert any("structures" in stmt and "'metric'" in stmt for stmt in statements)

  def test_fresh_tenant_check_widener_admits_cm_and_has_part(self) -> None:
    """The cm framework introduces source='cm' (cm:Debit / cm:Credit) and
    schedule posting arcs use association_type='has-part'. Both must be in
    the fresh-provision widener, or tenant-copy of the cm elements and
    schedule creation fail with a CheckViolation (the graph-creation failure
    this regression guards against)."""
    statements: list[str] = []

    class FakeConn:
      def execute(self, statement):
        statements.append(str(statement))

    extensions_db._widen_library_checks(FakeConn(), "kg0123456789abcdef")
    assert any("elements" in s and "'cm'" in s for s in statements), (
      "check_element_source widener must admit source='cm'"
    )
    assert any("associations" in s and "'has-part'" in s for s in statements), (
      "check_association_type widener must admit association_type='has-part'"
    )
