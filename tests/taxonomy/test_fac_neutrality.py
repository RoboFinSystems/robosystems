"""Invariant: ``fac`` is the framework-neutral accounting substrate.

The architecture splits responsibilities cleanly (see
``frameworks/fac/packages/README.md`` and [[framework-scope-fac-vs-rsgaap]]):

- ``fac`` holds high-level concepts + the universal trait vocabulary that
  persist regardless of regulatory regime (us-gaap, ifrs, irs, …).
- Each ``rs-*`` framework is a *codification bridge* into one regime and is
  the ONLY place regime-specific content lives: codification references
  (FASB ASC, IAS/IFRS, IRC) and validation rules that target that
  framework's own concepts.

These tests fail loudly if framework-specific content leaks back into fac —
which would make a downstream framework inheriting ``fac`` (rs-ifrs,
rs-call-report, …) drag another regime's citations/rules along.
"""

from __future__ import annotations

import pytest

from robosystems.taxonomy import load_framework_manifest, load_taxonomy_package
from robosystems.taxonomy.discovery import framework_root

_FAC_PKGS = framework_root("fac") / "packages"

# Reference types that denote a regulatory codification — these belong in an
# rs-* bridge, never in the neutral fac layer.
_CODIFICATION_REF_TYPES = {"ASC", "IAS", "IFRS", "IRC", "SEC", "SFAS", "FERC"}


@pytest.mark.unit
class TestFacReferenceNeutrality:
  def test_fac_concepts_carry_no_codification_references(self) -> None:
    """fac concept references must be conceptual, never codification.

    rs-gaap-references carries the ~3,964 FASB ASC citations; fac stays
    codification-free so it remains a true cross-framework spine.
    """
    pkg = load_taxonomy_package(_FAC_PKGS / "fac" / "v1" / "taxonomy.jsonld")
    offenders = [
      (element.qname, ref.ref_type, ref.citation)
      for element in pkg.elements
      for ref in element.references
      if (ref.ref_type or "").upper() in _CODIFICATION_REF_TYPES
    ]
    assert offenders == [], (
      "fac must stay codification-free (move ASC/IFRS/IRC citations to the "
      f"rs-* bridge): {offenders[:5]}"
    )


@pytest.mark.unit
class TestFacFrameworkCarriesNoRules:
  def test_fac_manifest_has_no_validation_rule_package(self) -> None:
    """Framework-specific validation rules live with the rs-* framework
    that owns the targeted concepts. The cross-tree consistency rules were
    moved to ``rs-gaap-rules``; fac's manifest must declare no rules pack.
    """
    manifest = load_framework_manifest("fac", "v1")
    standards = {pkg["standard"] for pkg in manifest["packages"]}
    rule_packages = {s for s in standards if s.endswith("-rules")}
    assert rule_packages == set(), (
      f"fac must declare no validation-rule package; found {rule_packages}"
    )

  def test_rs_gaap_owns_the_moved_rules(self) -> None:
    """Positive control: the moved rules now live in the rs-gaap framework."""
    manifest = load_framework_manifest("rs-gaap", "v1")
    standards = {pkg["standard"] for pkg in manifest["packages"]}
    assert "rs-gaap-rules" in standards
