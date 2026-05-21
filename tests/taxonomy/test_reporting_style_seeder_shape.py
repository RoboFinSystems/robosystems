"""Shape tests for the data-driven Reporting Style seeder in migration 0008.

Static checks — no DB round-trip. Migration 0008 reads each Style's
``reportingStyleNetworks`` from the ``rs-gaap-reporting-styles`` package
and seeds ``reporting_style_networks`` + stamps ``reportingStyleCode``.
"Adding a preset is pure package content" is now load-bearing, so these
lock the contract a typo in the package or the seeder could break:

- ``_declared_styles`` parses the package into the two composed presets
  (Default = multi-step IS, SmallPrivate = single-step IS), each with a
  complete 4-statement composition.
- Composition-less placeholders (Banking) are skipped but still promoted.
- ``_stamp_style_codes`` uses ``CAST(:code AS text)`` — guards against
  the ``:bind::cast`` form that silently fails to bind in SQLAlchemy.
"""

from __future__ import annotations

# Import migration 0008 via path-based loader because the file name
# starts with a digit (same trick Alembic uses internally).
import importlib.util as _util
from pathlib import Path

import pytest

_MIGRATION_PATH = (
  Path(__file__).resolve().parents[2]
  / "migrations"
  / "extensions"
  / "versions"
  / "0008_reporting_style.py"
)

_spec = _util.spec_from_file_location("mig_0008", _MIGRATION_PATH)
assert _spec is not None and _spec.loader is not None
mig_0008 = _util.module_from_spec(_spec)
_spec.loader.exec_module(mig_0008)

_REQUIRED = {
  "balance_sheet",
  "income_statement",
  "cash_flow_statement",
  "equity_statement",
}


class _Conn:
  """Captures (sql, params) for every execute — no DB round-trip."""

  def __init__(self) -> None:
    self.calls: list[tuple[str, dict]] = []

  def execute(self, stmt, params=None) -> None:
    self.calls.append((str(stmt), params or {}))


class TestDeclaredStyles:
  def test_exactly_the_four_composed_presets(self) -> None:
    codes = {s["code"] for s in mig_0008._declared_styles()}
    assert codes == {
      "BSC-CORP-IS02-CF1",
      "BSC-CORP-IS01-CF1",
      "BSC-PART-IS02-CF1",
      "BSC-LLC-IS02-CF1",
    }

  def test_every_declared_style_has_complete_composition(self) -> None:
    """The change-reporting-style validator rejects an incomplete
    composition, so a seeded preset must compose all four statements."""
    for style in mig_0008._declared_styles():
      stmt_types = {st for st, _net in style["networks"]}
      assert stmt_types == _REQUIRED, style["code"]

  def test_is_axis_distinguishes_the_single_vs_multistep_presets(self) -> None:
    """IS02 → multi-step network, IS01 → single-step network. Everything
    else is shared between the two corporate presets."""
    by_code = {s["code"]: dict(s["networks"]) for s in mig_0008._declared_styles()}
    assert by_code["BSC-CORP-IS02-CF1"]["income_statement"].endswith("IS-multistep")
    assert by_code["BSC-CORP-IS01-CF1"]["income_statement"].endswith("IS-singlestep")
    # BS / CF / SE are identical across the two corporate presets.
    for stmt in ("balance_sheet", "cash_flow_statement", "equity_statement"):
      assert by_code["BSC-CORP-IS02-CF1"][stmt] == by_code["BSC-CORP-IS01-CF1"][stmt], (
        stmt
      )

  def test_equity_form_axis_is_the_only_difference_corp_part_llc(self) -> None:
    """CORP/PART/LLC share IS-multistep + CashFlow-indirect; only the
    balance_sheet equity section and the equity_statement rollforward vary
    by form (the equity-form axis)."""
    by_code = {s["code"]: dict(s["networks"]) for s in mig_0008._declared_styles()}
    corp = by_code["BSC-CORP-IS02-CF1"]
    part = by_code["BSC-PART-IS02-CF1"]
    llc = by_code["BSC-LLC-IS02-CF1"]

    # IS + CF are held constant across the three forms.
    for stmt in ("income_statement", "cash_flow_statement"):
      assert corp[stmt] == part[stmt] == llc[stmt], stmt

    # Equity-form axis: BS + equity_statement networks are form-specific.
    assert corp["balance_sheet"].endswith("BS-classified")
    assert part["balance_sheet"].endswith("BS-classified-PART")
    assert llc["balance_sheet"].endswith("BS-classified-LLC")
    assert corp["equity_statement"].endswith("Equity-rollforward")
    assert part["equity_statement"].endswith("Equity-rollforward-PART")
    assert llc["equity_statement"].endswith("Equity-rollforward-LLC")

  def test_placeholder_skipped_but_still_promoted(self) -> None:
    """Banking declares no composition — skipped by the seeder (stays
    non-selectable) but still listed for the block_type promotion."""
    declared = {s["role_uri"] for s in mig_0008._declared_styles()}
    all_styles = set(mig_0008._all_style_role_uris())
    banking = mig_0008._STYLE_ROLE_PREFIX + "Banking"
    assert banking in all_styles
    assert banking not in declared


class TestSeedComposition:
  def test_seeds_one_row_per_statement_per_style(self) -> None:
    conn = _Conn()
    mig_0008._seed_style_compositions(conn, "public")
    # 4 composed presets, 4 statement types each.
    assert len(conn.calls) == 16
    for sql, params in conn.calls:
      assert "public.reporting_style_networks" in sql
      assert set(params) >= {"style", "stmt", "network"}

  def test_tenant_schema_qualifies_table(self) -> None:
    conn = _Conn()
    mig_0008._seed_style_compositions(conn, "kgtest")
    assert all('"kgtest".reporting_style_networks' in sql for sql, _ in conn.calls)


class TestStampStyleCodes:
  def test_uses_cast_not_bind_then_double_colon(self) -> None:
    """Regression guard: ``to_jsonb(:code::text)`` silently fails to bind
    in SQLAlchemy text() (the ``::`` defeats the parser). Must use
    ``CAST(:code AS text)``."""
    conn = _Conn()
    mig_0008._stamp_style_codes(conn)
    assert conn.calls, "expected at least one UPDATE"
    for sql, params in conn.calls:
      assert "CAST(:code AS text)" in sql
      assert ":code::text" not in sql
      assert set(params) == {"sid", "code"}

  def test_stamps_every_declared_code(self) -> None:
    conn = _Conn()
    mig_0008._stamp_style_codes(conn)
    stamped = {params["code"] for _sql, params in conn.calls}
    assert stamped == {
      "BSC-CORP-IS02-CF1",
      "BSC-CORP-IS01-CF1",
      "BSC-PART-IS02-CF1",
      "BSC-LLC-IS02-CF1",
    }


class _Result:
  def __init__(self, row) -> None:
    self._row = row

  def fetchone(self):
    return self._row


class _ValidatingConn:
  """Fake conn for _stamp_close_targets: answers the present-in-BS SELECT
  with a configurable hit/miss, captures the UPDATEs."""

  def __init__(self, present: bool) -> None:
    self.present = present
    self.updates: list[tuple[str, dict]] = []

  def execute(self, stmt, params=None):
    sql = str(stmt)
    if "associations" in sql:  # the present-in-BS validation SELECT
      return _Result((1,) if self.present else None)
    self.updates.append((sql, params or {}))
    return _Result(None)


class TestCloseTargets:
  def test_declared_styles_carry_form_close_target(self) -> None:
    by_code = {s["code"]: s["close_target"] for s in mig_0008._declared_styles()}
    assert by_code["BSC-CORP-IS02-CF1"] == "rs-gaap:RetainedEarningsAccumulatedDeficit"
    assert by_code["BSC-CORP-IS01-CF1"] == "rs-gaap:RetainedEarningsAccumulatedDeficit"
    assert by_code["BSC-PART-IS02-CF1"] == "rs-gaap:PartnersCapital"
    assert by_code["BSC-LLC-IS02-CF1"] == "rs-gaap:MembersEquity"

  def test_stamps_every_close_target_when_present_in_bs(self) -> None:
    conn = _ValidatingConn(present=True)
    mig_0008._stamp_close_targets(conn)
    stamped = {params["target"] for _sql, params in conn.updates}
    assert stamped == {
      "rs-gaap:RetainedEarningsAccumulatedDeficit",
      "rs-gaap:PartnersCapital",
      "rs-gaap:MembersEquity",
    }
    for sql, params in conn.updates:
      assert "retained_earnings_concept" in sql
      assert set(params) == {"sid", "target"}

  def test_raises_when_close_target_absent_from_bs(self) -> None:
    """The determinism guard: a declared earnings concept the BS doesn't
    present would silently drop earnings — must fail the migration."""
    conn = _ValidatingConn(present=False)
    with pytest.raises(RuntimeError, match="does not"):
      mig_0008._stamp_close_targets(conn)
