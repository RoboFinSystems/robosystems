"""Opt-in SHACL validation at report-publish time + structured capture.

Covers ``shacl_report`` (the non-raising structured result) and the publish
hook's ``_record_bundle_validation`` — which records the outcome onto
``Report.metadata['bundle_validation']`` and, in ``strict`` mode, blocks the
publish on non-conformance. Default mode is ``off`` (publish path unchanged).
"""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace

import pytest
import rdflib
from rdflib import Literal

from robosystems.operations.serialization.bundle import (
  BundleElement,
  BundleFact,
  BundleLinkbases,
  BundlePeriod,
  BundleUnit,
  EntityMeta,
  FrameworkPin,
  PeriodMeta,
  ReportMeta,
  StatementBundle,
)
from robosystems.operations.serialization.rdf import jsonld
from robosystems.operations.serialization.rdf.jsonld import (
  RS,
  BundleValidationError,
  ShaclResult,
  build_graph,
  shacl_report,
)

XBRLI = rdflib.Namespace("http://www.xbrl.org/2003/instance#")


def _bundle() -> StatementBundle:
  return StatementBundle(
    entity=EntityMeta(id="ent_01", name="Test Co"),
    periods=[PeriodMeta(start=date(2024, 1, 1), end=date(2024, 12, 31), label="FY24")],
    reporting_style="BSC-CORP-IS02-CF1",
    framework_pins=[FrameworkPin(framework="rs-gaap", version="v1")],
    schema_concepts=[
      BundleElement(
        id="Assets",
        qname="rs-gaap:Assets",
        name="Assets",
        period_type="instant",
        is_monetary=True,
        balance_type="debit",
        source="rs-gaap",
      )
    ],
    linkbases=BundleLinkbases(),
    period_nodes=[
      BundlePeriod(id="p_1", period_end=date(2024, 12, 31), period_type="instant")
    ],
    units=[BundleUnit(id="u_USD", measure="iso4217:USD")],
    facts=[
      BundleFact(
        id="fact_01",
        element_id="Assets",
        element_qname="rs-gaap:Assets",
        value=100.0,
        period_ref="p_1",
        unit_ref="u_USD",
        entity_ref="ent_01",
      )
    ],
    ib_envelopes=[],
    mode="report",
    report_meta=ReportMeta(
      report_id="rpt_test", generation_count=1, filing_status="filed"
    ),
  )


# ── shacl_report (structured, non-raising) ───────────────────────────────


class TestShaclReport:
  def test_conforming_bundle(self) -> None:
    r = shacl_report(build_graph(_bundle()))
    assert r.ran is True
    assert r.conforms is True
    assert r.violations == 0
    assert r.shapes_checked > 0
    assert r.report == ""

  def test_violation_is_reported_not_raised(self) -> None:
    g = build_graph(_bundle())
    fact = next(g.subjects(rdflib.RDF.type, RS.Fact))
    g.add((fact, XBRLI.contextRef, Literal("ctx_1")))  # banned dialect
    r = shacl_report(g)  # does NOT raise
    assert r.conforms is False
    assert r.violations >= 1
    assert r.report  # non-empty pyshacl text

  def test_as_dict_is_bounded_and_jsonable(self) -> None:
    r = ShaclResult(
      ran=True, conforms=False, violations=2, shapes_checked=8, report="x" * 9000
    )
    d = r.as_dict()
    assert d["conforms"] is False
    assert d["violations"] == 2
    assert len(d["report_excerpt"]) == 4000  # truncated
    assert d["shapes_version"].endswith("shapes.ttl")


# ── _record_bundle_validation (publish-hook capture) ─────────────────────


def _fake_report():
  return SimpleNamespace(id="rpt_test", metadata_={})


class TestRecordBundleValidation:
  def test_off_is_noop(self, monkeypatch: pytest.MonkeyPatch) -> None:
    from robosystems.config import env
    from robosystems.operations.roboledger.commands import reports

    monkeypatch.setattr(env, "REPORT_BUNDLE_SHACL_VALIDATION", "off")
    rpt = _fake_report()
    reports._record_bundle_validation(_bundle(), rpt)
    assert "bundle_validation" not in rpt.metadata_

  def test_warn_captures_conforming_result(
    self, monkeypatch: pytest.MonkeyPatch
  ) -> None:
    from robosystems.config import env
    from robosystems.operations.roboledger.commands import reports

    monkeypatch.setattr(env, "REPORT_BUNDLE_SHACL_VALIDATION", "warn")
    rpt = _fake_report()
    reports._record_bundle_validation(_bundle(), rpt)  # real bundle conforms
    bv = rpt.metadata_["bundle_validation"]
    assert bv["ran"] is True and bv["conforms"] is True and bv["violations"] == 0
    assert "validated_at" in bv

  def test_warn_records_violation_without_raising(
    self, monkeypatch: pytest.MonkeyPatch
  ) -> None:
    from robosystems.config import env
    from robosystems.operations.roboledger.commands import reports

    monkeypatch.setattr(env, "REPORT_BUNDLE_SHACL_VALIDATION", "warn")
    monkeypatch.setattr(
      jsonld,
      "shacl_report",
      lambda g: ShaclResult(
        ran=True, conforms=False, violations=3, shapes_checked=8, report="bad"
      ),
    )
    rpt = _fake_report()
    reports._record_bundle_validation(_bundle(), rpt)  # no raise in warn
    assert rpt.metadata_["bundle_validation"]["conforms"] is False

  def test_strict_raises_on_violation(self, monkeypatch: pytest.MonkeyPatch) -> None:
    from robosystems.config import env
    from robosystems.operations.roboledger.commands import reports

    monkeypatch.setattr(env, "REPORT_BUNDLE_SHACL_VALIDATION", "strict")
    monkeypatch.setattr(
      jsonld,
      "shacl_report",
      lambda g: ShaclResult(
        ran=True, conforms=False, violations=3, shapes_checked=8, report="bad"
      ),
    )
    rpt = _fake_report()
    with pytest.raises(BundleValidationError):
      reports._record_bundle_validation(_bundle(), rpt)
    # outcome still recorded before raising
    assert rpt.metadata_["bundle_validation"]["violations"] == 3

  def test_strict_conforming_does_not_raise(
    self, monkeypatch: pytest.MonkeyPatch
  ) -> None:
    from robosystems.config import env
    from robosystems.operations.roboledger.commands import reports

    monkeypatch.setattr(env, "REPORT_BUNDLE_SHACL_VALIDATION", "strict")
    rpt = _fake_report()
    reports._record_bundle_validation(_bundle(), rpt)  # real bundle conforms
    assert rpt.metadata_["bundle_validation"]["conforms"] is True

  def test_warn_swallows_validation_exception(
    self, monkeypatch: pytest.MonkeyPatch
  ) -> None:
    """A pyshacl/build_graph failure must NOT break a warn-mode publish."""
    from robosystems.config import env
    from robosystems.operations.roboledger.commands import reports

    def _boom(_bundle):
      raise RuntimeError("pyshacl exploded")

    monkeypatch.setattr(env, "REPORT_BUNDLE_SHACL_VALIDATION", "warn")
    monkeypatch.setattr(jsonld, "build_graph", _boom)
    rpt = _fake_report()
    reports._record_bundle_validation(_bundle(), rpt)  # does not raise
    assert "bundle_validation" not in rpt.metadata_

  def test_strict_reraises_validation_exception(
    self, monkeypatch: pytest.MonkeyPatch
  ) -> None:
    from robosystems.config import env
    from robosystems.operations.roboledger.commands import reports

    def _boom(_bundle):
      raise RuntimeError("pyshacl exploded")

    monkeypatch.setattr(env, "REPORT_BUNDLE_SHACL_VALIDATION", "strict")
    monkeypatch.setattr(jsonld, "build_graph", _boom)
    rpt = _fake_report()
    with pytest.raises(RuntimeError):
      reports._record_bundle_validation(_bundle(), rpt)
