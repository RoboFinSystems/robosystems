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


@pytest.mark.slow
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
