"""Shape tests for Phase δ.2 rule plumbing across the migration surface.

Static checks — no DB round-trip. The extensions-DB migration harness
lives in the dev loop (``just migrate-up extensions`` + CI dry runs);
these tests lock down the pieces that a typo in the migration file
could otherwise drift out of alignment:

- Immutability trigger includes ``rules`` so library-seeded rows stay
  read-only in tenant schemas.
- ``_WIDENED_TAXONOMY_TYPE_CHECK`` allows ``'rules'`` so the fac-rules
  seed row can be inserted into ``public.taxonomies``.
- The fac-rules seed path is registered in ``SEED_FILES``.
- The tenant-copy column list matches the seed-writer column list
  (shape symmetry for the public → tenant INSERT...SELECT).
- ``DEFAULT_TAXONOMY_PIN`` includes ``fac-rules`` so tenant copies
  actually fetch rule rows.
"""

from __future__ import annotations

# Import migration 0002 via path-based loader because the file name
# starts with a digit. This is the same trick Alembic uses internally.
import importlib.util as _util
from pathlib import Path

from robosystems.taxonomy.loaders.jsonld_loader import (
  RULE_CATEGORY_VALUES,
  RULE_PATTERN_VALUES,
)
from robosystems.taxonomy.pins import DEFAULT_TAXONOMY_PIN

_MIGRATION_PATH = (
  Path(__file__).resolve().parents[2]
  / "migrations"
  / "extensions"
  / "versions"
  / "0002_taxonomy_library.py"
)

_spec = _util.spec_from_file_location("mig_0002", _MIGRATION_PATH)
assert _spec is not None and _spec.loader is not None
mig_0002 = _util.module_from_spec(_spec)
_spec.loader.exec_module(mig_0002)


class TestImmutableTables:
  def test_rules_is_immutable_in_tenant_schemas(self) -> None:
    """Library-seeded rules copied into a tenant schema must be
    trigger-protected against UPDATE/DELETE — same contract as
    structures/associations/elements."""
    assert "rules" in mig_0002._IMMUTABLE_TABLES


class TestTaxonomyTypeCheck:
  def test_rules_type_is_allowed(self) -> None:
    """The widened CHECK constraint on ``public.taxonomies.taxonomy_type``
    must include ``'rules'`` so the fac-rules seed row can be written."""
    assert "'rules'" in mig_0002._WIDENED_TAXONOMY_TYPE_CHECK


class TestSeedFiles:
  def test_fac_rules_seed_is_registered(self) -> None:
    """The fac-rules/v1 JSON-LD seed must be in the SEED_FILES tuple —
    missing it silently skips the rules seeder in every migration run."""
    seeded_names = {path.name for path in mig_0002.SEED_FILES}
    # Every seed is named ``taxonomy.jsonld`` — distinguish by parent dir.
    seeded_paths = {
      (path.parent.parent.name, path.parent.name) for path in mig_0002.SEED_FILES
    }
    assert "taxonomy.jsonld" in seeded_names
    assert ("fac-rules", "v1") in seeded_paths


class TestRuleCheckConstraints:
  def test_category_check_lists_every_enum_value(self) -> None:
    """Drift between the loader's enum and the DB CHECK would admit seeds
    the writer validates but the DB rejects (or vice versa)."""
    for category in RULE_CATEGORY_VALUES:
      assert f"'{category}'" in mig_0002._RULE_CATEGORY_CHECK, (
        f"category {category!r} missing from _RULE_CATEGORY_CHECK"
      )

  def test_pattern_check_lists_every_enum_value(self) -> None:
    for pattern in RULE_PATTERN_VALUES:
      assert f"'{pattern}'" in mig_0002._RULE_PATTERN_CHECK, (
        f"pattern {pattern!r} missing from _RULE_PATTERN_CHECK"
      )

  def test_severity_check_enumerates_info_warning_error(self) -> None:
    for severity in ("info", "warning", "error"):
      assert f"'{severity}'" in mig_0002._RULE_SEVERITY_CHECK

  def test_origin_check_enumerates_forked_and_native(self) -> None:
    for origin in ("forked", "native"):
      assert f"'{origin}'" in mig_0002._RULE_ORIGIN_CHECK

  def test_polymorphism_check_names_all_three_target_fks(self) -> None:
    check = mig_0002._RULE_TARGET_POLYMORPHISM_CHECK
    assert "target_kind" in check
    assert "target_structure_id" in check
    assert "target_element_id" in check
    assert "target_association_id" in check


class TestTenantPin:
  def test_default_pin_includes_fac_rules(self) -> None:
    """Without this pin entry, ``copy_library_into_tenant`` would filter
    ``public.rules`` by the pinned (standard, version) pairs and exclude
    the fac-rules seed rows — tenant schemas would ship empty."""
    assert DEFAULT_TAXONOMY_PIN.get("fac-rules") == "v1"


class TestTenantWriterRuleCols:
  def test_rule_cols_include_polymorphic_target_fks(self) -> None:
    """The public→tenant INSERT...SELECT must name the polymorphic FK
    columns so the CHECK constraint on the tenant side sees a valid
    combination on insert."""
    from robosystems.taxonomy.writers.tenant_writer import _RULE_COLS

    for col in (
      "id",
      "taxonomy_id",
      "rule_category",
      "rule_pattern",
      "rule_expression",
      "rule_severity",
      "rule_origin",
      "target_kind",
      "target_structure_id",
      "target_element_id",
      "target_association_id",
      "rule_variables",
    ):
      assert col in _RULE_COLS, f"_RULE_COLS missing {col!r}"
