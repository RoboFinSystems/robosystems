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

  def test_graph_node_models_exist(self):
    """Verify graph node model SQL files exist."""
    nodes_dir = DBT_PROJECT_DIR / "models" / "graph" / "nodes"
    assert (nodes_dir / "entity.sql").exists()
    assert (nodes_dir / "element.sql").exists()
    assert (nodes_dir / "transaction.sql").exists()
    assert (nodes_dir / "line_item.sql").exists()
    assert (nodes_dir / "dimension.sql").exists()

  def test_graph_relationship_models_exist(self):
    """Verify graph relationship model SQL files exist."""
    rels_dir = DBT_PROJECT_DIR / "models" / "graph" / "relationships"
    assert (rels_dir / "entity_has_transaction.sql").exists()
    assert (rels_dir / "transaction_has_line_item.sql").exists()
    assert (rels_dir / "line_item_relates_to_element.sql").exists()
    assert (rels_dir / "line_item_has_dimension.sql").exists()

  def test_macros_exist(self):
    """Verify macro SQL files exist."""
    macros_dir = DBT_PROJECT_DIR / "macros"
    assert (macros_dir / "normal_balance.sql").exists()
    assert (macros_dir / "classification.sql").exists()
    assert (macros_dir / "uri_generators.sql").exists()
    assert (macros_dir / "generate_identifier.sql").exists()

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
        "QB_DUCKDB_PATH": str(tmp_path / "quickbooks.duckdb"),
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
        "QB_DUCKDB_PATH": str(duckdb_path),
      },
      check=True,
    )

    con = duckdb.connect(str(duckdb_path), read_only=True)
    try:
      tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}

      # Verify node tables exist
      assert "entity" in tables
      assert "element" in tables
      assert "transaction" in tables
      assert "line_item" in tables
      assert "dimension" in tables

      # Verify relationship tables exist
      assert "entity_has_transaction" in tables
      assert "transaction_has_line_item" in tables
      assert "line_item_relates_to_element" in tables
      assert "line_item_has_dimension" in tables

      # Verify row counts
      entity_count = con.execute("SELECT count(*) FROM entity").fetchone()[0]
      assert entity_count == 1, f"Expected 1 entity, got {entity_count}"

      element_count = con.execute("SELECT count(*) FROM element").fetchone()[0]
      assert element_count == 15, f"Expected 15 elements, got {element_count}"

      tx_count = con.execute("SELECT count(*) FROM transaction").fetchone()[0]
      assert tx_count == 15, f"Expected 15 transactions, got {tx_count}"

      li_count = con.execute("SELECT count(*) FROM line_item").fetchone()[0]
      assert li_count == 30, f"Expected 30 line items, got {li_count}"

      # Verify debits = credits
      result = con.execute("""
        SELECT sum(debit_amount) as debits, sum(credit_amount) as credits
        FROM line_item
      """).fetchone()
      assert abs(result[0] - result[1]) < 0.01, (
        f"Debits ({result[0]}) != Credits ({result[1]})"
      )
    finally:
      con.close()
