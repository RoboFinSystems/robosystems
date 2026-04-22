"""Shape tests for Phase iota verification_results + structure_templates.

Static checks — no DB round-trip. Locks down the 0004 migration's
constants, tenant helpers, and revision chain.
"""

from __future__ import annotations

import importlib.util as _util
from pathlib import Path

_MIGRATION_PATH = (
  Path(__file__).resolve().parents[2]
  / "migrations"
  / "extensions"
  / "versions"
  / "0004_verification_results_and_templates.py"
)

_spec = _util.spec_from_file_location("mig_0004", _MIGRATION_PATH)
assert _spec is not None and _spec.loader is not None
mig_0004 = _util.module_from_spec(_spec)
_spec.loader.exec_module(mig_0004)


class TestMigrationChain:
  def test_revision_and_down_revision(self) -> None:
    assert mig_0004.revision == "0004"
    assert mig_0004.down_revision == "0003"


class TestVerificationStatusCheck:
  def test_enum_values_present(self) -> None:
    check = mig_0004._VERIFICATION_STATUS_CHECK
    for value in ("'pass'", "'fail'", "'error'", "'skipped'"):
      assert value in check, f"status CHECK missing {value}"


class TestTemplateTypeCheck:
  def test_enum_values_present(self) -> None:
    check = mig_0004._TEMPLATE_TYPE_CHECK
    for value in ("'renderer'", "'schedule_variant'", "'metric_preset'"):
      assert value in check, f"template_type CHECK missing {value}"


class TestTenantHelpers:
  def test_create_helper_exists(self) -> None:
    assert callable(mig_0004._create_in_tenant)

  def test_drop_helper_exists(self) -> None:
    assert callable(mig_0004._drop_in_tenant)
