"""Tests for QuickBooks dbt transformation project.

These tests verify the dbt project builds correctly and produces
valid RoboLedger schema output from sample QuickBooks data.
"""

import subprocess
from pathlib import Path

import pytest

DBT_PROJECT_DIR = (
  Path(__file__).resolve().parents[4]
  / "robosystems"
  / "adapters"
  / "quickbooks"
  / "dbt"
)


@pytest.mark.unit
class TestDbtProject:
  """Test that the dbt project is correctly configured."""

  def test_dbt_project_exists(self):
    """Verify dbt project directory and key files exist."""
    assert DBT_PROJECT_DIR.exists(), f"dbt project not found at {DBT_PROJECT_DIR}"
    assert (DBT_PROJECT_DIR / "dbt_project.yml").exists()
    assert (DBT_PROJECT_DIR / "profiles.yml").exists()

  def test_seed_files_exist(self):
    """Verify seed CSV files exist for sample data."""
    seeds_dir = DBT_PROJECT_DIR / "seeds"
    assert (seeds_dir / "raw_accounts.csv").exists()
    assert (seeds_dir / "raw_journal_entries.csv").exists()
    assert (seeds_dir / "raw_journal_lines.csv").exists()
    assert (seeds_dir / "raw_company_info.csv").exists()

  def test_staging_models_exist(self):
    """Verify staging model SQL files exist."""
    staging_dir = DBT_PROJECT_DIR / "models" / "staging"
    assert (staging_dir / "stg_qb_accounts.sql").exists()
    assert (staging_dir / "stg_qb_journal_entries.sql").exists()
    assert (staging_dir / "stg_qb_journal_lines.sql").exists()
    assert (staging_dir / "stg_qb_company_info.sql").exists()

  def test_oltp_models_exist(self):
    """Verify OLTP output model SQL files exist."""
    oltp_dir = DBT_PROJECT_DIR / "models" / "ledger"
    assert (oltp_dir / "elements.sql").exists()
    assert (oltp_dir / "transactions.sql").exists()
    assert (oltp_dir / "entries.sql").exists()
    assert (oltp_dir / "line_items.sql").exists()
    assert (oltp_dir / "dimensions.sql").exists()

  def test_macros_exist(self):
    """Verify macro SQL files exist."""
    macros_dir = DBT_PROJECT_DIR / "macros"
    assert (macros_dir / "normal_balance.sql").exists()
    assert (macros_dir / "classification.sql").exists()

  def test_custom_tests_exist(self):
    """Verify custom test SQL files exist."""
    tests_dir = DBT_PROJECT_DIR / "tests"
    assert (tests_dir / "assert_debits_equal_credits.sql").exists()
    assert (tests_dir / "assert_accounting_equation.sql").exists()
    assert (tests_dir / "assert_unique_identifiers.sql").exists()


class TestDbtBuild:
  """Test that dbt build succeeds with seed data.

  These tests require dbt-core and dbt-duckdb to be installed.
  They run the actual dbt build process and verify outputs.
  """

  def test_dbt_build_succeeds(self, tmp_path):
    """Run dbt build and verify it completes without errors."""
    result = subprocess.run(
      [
        "uv",
        "run",
        "dbt",
        "build",
        "--profiles-dir",
        str(DBT_PROJECT_DIR),
        "--project-dir",
        str(DBT_PROJECT_DIR),
        "--target-path",
        str(tmp_path / "target"),
        "--vars",
        '{"use_seeds": true}',
      ],
      capture_output=True,
      text=True,
      cwd=str(DBT_PROJECT_DIR),
      env={
        **__import__("os").environ,
        "DBT_DUCKDB_PATH": str(tmp_path / "quickbooks.duckdb"),
      },
    )
    assert result.returncode == 0, (
      f"dbt build failed:\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
    )

  def test_dbt_output_tables(self, tmp_path):
    """Run dbt build and verify DuckDB contains expected tables."""
    import duckdb

    duckdb_path = tmp_path / "quickbooks.duckdb"

    subprocess.run(
      [
        "uv",
        "run",
        "dbt",
        "build",
        "--profiles-dir",
        str(DBT_PROJECT_DIR),
        "--project-dir",
        str(DBT_PROJECT_DIR),
        "--target-path",
        str(tmp_path / "target"),
        "--vars",
        '{"use_seeds": true}',
      ],
      capture_output=True,
      text=True,
      cwd=str(DBT_PROJECT_DIR),
      env={
        **__import__("os").environ,
        "DBT_DUCKDB_PATH": str(duckdb_path),
      },
      check=True,
    )

    con = duckdb.connect(str(duckdb_path), read_only=True)
    try:
      tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}

      # Verify OLTP output tables exist
      assert "elements" in tables
      assert "transactions" in tables
      assert "entries" in tables
      assert "line_items" in tables
      assert "dimensions" in tables

      # Verify row counts
      acct_count = con.execute("SELECT count(*) FROM elements").fetchone()[0]
      assert acct_count == 16, f"Expected 16 elements, got {acct_count}"

      tx_count = con.execute("SELECT count(*) FROM transactions").fetchone()[0]
      assert tx_count == 15, f"Expected 15 transactions, got {tx_count}"

      li_count = con.execute("SELECT count(*) FROM line_items").fetchone()[0]
      assert li_count == 30, f"Expected 30 line items, got {li_count}"

      # Verify debits = credits (amounts in cents)
      result = con.execute("""
        SELECT sum(debit_amount) as debits, sum(credit_amount) as credits
        FROM line_items
      """).fetchone()
      assert abs(result[0] - result[1]) <= 1, (
        f"Debits ({result[0]}) != Credits ({result[1]})"
      )
    finally:
      con.close()


def test_a_negative_line_keeps_its_entry_balanced(tmp_path):
  """A payout deposited net of its fee: QuickBooks shows the fee as a
  negative credit. Dropped, it unbalanced the window and failed the build."""
  import os

  import duckdb

  from robosystems.adapters.quickbooks.pipeline.utils import (
    parse_journal_report,
    write_extract_parquet,
  )

  def row(date, typ, tid, acct, aid, debit, credit):
    return {
      "ColData": [
        {"value": date},
        {"value": typ, "id": tid},
        {"value": ""},
        {"value": ""},
        {"value": ""},
        {"value": acct, "id": aid},
        {"value": debit},
        {"value": credit},
      ]
    }

  def summary(debit, credit):
    return {
      "Summary": {
        "ColData": [{"value": ""}] * 6 + [{"value": debit}, {"value": credit}]
      }
    }

  report = {
    "Rows": {
      "Row": [
        row("2026-09-03", "Deposit", "201", "Checking", "35", "970.00", ""),
        row("", "", "", "Undeposited Funds", "4", "", "1000.00"),
        row("", "", "", "Merchant Fees", "60", "", "-30.00"),
        summary("970.00", "970.00"),
      ]
    }
  }
  accounts = [
    {
      "Id": aid,
      "Name": name,
      "FullyQualifiedName": name,
      "AccountType": kind,
      "Classification": cls,
      "ParentRef": None,
      "Active": True,
      "CurrentBalance": 0.0,
      "AccountSubType": kind,
      "AcctNum": None,
      "SubAccount": False,
    }
    for aid, name, kind, cls in [
      ("35", "Checking", "Bank", "Asset"),
      ("4", "Undeposited Funds", "Other Current Asset", "Asset"),
      ("60", "Merchant Fees", "Expense", "Expense"),
    ]
  ]
  company = [
    {
      "Id": "1",
      "CompanyName": "Fictional Widgets LLC",
      "LegalName": "Fictional Widgets LLC",
      "CompanyAddr_Line1": "",
      "CompanyAddr_City": "",
      "CompanyAddr_CountrySubDivisionCode": "",
      "CompanyAddr_PostalCode": "",
      "CompanyAddr_Country": "US",
      "PrimaryPhone_FreeFormNumber": "",
      "WebAddr_URI": "",
      "FiscalYearStartMonth": "January",
    }
  ]
  extract = tmp_path / "extract"
  entries, lines = parse_journal_report(report)
  write_extract_parquet(extract, accounts, entries, lines, company)
  duckdb_path = tmp_path / "quickbooks.duckdb"

  result = subprocess.run(
    [
      "uv",
      "run",
      "dbt",
      "build",
      "--profiles-dir",
      str(DBT_PROJECT_DIR),
      "--project-dir",
      str(DBT_PROJECT_DIR),
      "--target-path",
      str(tmp_path / "target"),
      "--vars",
      f'{{"realm_id": "1", "qb_extract_path": "{extract}", "use_seeds": false}}',
    ],
    capture_output=True,
    text=True,
    cwd=str(DBT_PROJECT_DIR),
    env={**os.environ, "DBT_DUCKDB_PATH": str(duckdb_path)},
  )

  assert result.returncode == 0, result.stdout[-2000:]
  con = duckdb.connect(str(duckdb_path), read_only=True)
  try:
    sides = con.execute(
      "SELECT element_external_id, debit_amount, credit_amount FROM line_items "
      "ORDER BY element_external_id"
    ).fetchall()
  finally:
    con.close()
  assert sides == [("35", 97000, 0), ("4", 0, 100000), ("60", 3000, 0)]
