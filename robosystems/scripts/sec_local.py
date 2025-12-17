#!/usr/bin/env python3
# type: ignore
"""
SEC Local Pipeline - Testing and Development Tool

Triggers Dagster jobs via Docker CLI for SEC data processing.
The actual processing runs inside the Dagster container.

Usage:
  # Load NVIDIA data for 2024
  just sec-load NVDA 2024

  # Reset SEC database
  just sec-reset

  # Materialize existing processed files
  just sec-materialize
"""

import argparse
import json
import subprocess
import sys
import tempfile
from datetime import datetime
from pathlib import Path

from robosystems.logger import logger


class SECLocalPipeline:
  """Local SEC pipeline - triggers Dagster jobs via Docker CLI."""

  def __init__(self, backend: str = "ladybug"):
    """Initialize the local pipeline.

    Args:
        backend: Database backend to use ("ladybug" or "neo4j")
    """
    if backend not in ("ladybug", "neo4j"):
      raise ValueError(f"Invalid backend: {backend}. Must be 'ladybug' or 'neo4j'")

    self.backend = backend
    self.sec_database = "sec"
    logger.info(f"Initialized SEC pipeline with backend: {backend}")

  def _create_config_file(self, ticker: str, year: int) -> str:
    """Create a YAML config file for the Dagster job.

    Args:
        ticker: Company ticker symbol
        year: Year to process

    Returns:
        Path to the config file inside the container
    """
    config = {
      "ops": {
        "sec_companies_list": {
          "config": {
            "ticker_filter": [ticker.upper()],
          }
        },
        "sec_raw_filings": {
          "config": {
            "skip_existing": True,
            "form_types": ["10-K", "10-Q"],
            "tickers": [ticker.upper()],
          }
        },
        "sec_processed_filings": {
          "config": {
            "refresh": False,
            "tickers": [ticker.upper()],
          }
        },
        "sec_duckdb_staging": {
          "config": {
            "rebuild": True,
            "year_filter": [year],
          }
        },
        "sec_graph_materialized": {
          "config": {
            "graph_id": "sec",
            "ignore_errors": True,
            "rebuild": True,
          }
        },
      }
    }

    # Write config to temp file
    import yaml

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
      yaml.dump(config, f, default_flow_style=False)
      config_path = f.name

    # Set readable permissions before copying
    import os

    os.chmod(config_path, 0o644)

    # Copy to container
    container_path = f"/tmp/sec_config_{ticker}_{year}.yaml"
    subprocess.run(
      ["docker", "cp", config_path, f"robosystems-dagster-webserver:{container_path}"],
      check=True,
      capture_output=True,
    )

    # Clean up local temp file
    Path(config_path).unlink()

    return container_path

  def load_company(self, ticker: str, year: int = None) -> bool:
    """Load a single company's data using Dagster job.

    Args:
        ticker: Company ticker symbol (e.g., "NVDA", "AAPL")
        year: Year to load data for (None for current year)

    Returns:
        True if successful, False otherwise
    """
    if year is None:
      year = datetime.now().year

    logger.info(f"Loading {ticker} data for {year} via Dagster...")

    try:
      # Create config file in container
      config_path = self._create_config_file(ticker, year)

      # Build the dagster command
      cmd = [
        "docker",
        "compose",
        "exec",
        "-T",
        "dagster-webserver",
        "dagster",
        "job",
        "execute",
        "-m",
        "robosystems.dagster",
        "--job",
        "sec_single_company",
        "-c",
        config_path,
        "--tags",
        json.dumps({"dagster/partition": str(year)}),
      ]

      logger.info(f"Executing Dagster job for {ticker} ({year})...")

      # Run the command
      result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=600,  # 10 minute timeout
      )

      # Check for success
      if result.returncode == 0:
        logger.info(f"Successfully loaded {ticker} data for {year}")
        return True
      else:
        # Check if the output contains RUN_SUCCESS
        if "RUN_SUCCESS" in result.stdout:
          logger.info(f"Successfully loaded {ticker} data for {year}")
          return True
        else:
          logger.error(f"Job failed with exit code {result.returncode}")
          if result.stderr:
            logger.error(f"Error output: {result.stderr[-500:]}")
          return False

    except subprocess.TimeoutExpired:
      logger.error("Job timed out after 10 minutes")
      return False
    except FileNotFoundError:
      logger.error(
        "Docker not found. Is Docker running? "
        "Make sure the Dagster containers are up with: just start"
      )
      return False
    except Exception as e:
      logger.error(f"Failed to load company: {e}")
      return False

  def materialize_only(self, year: int = None, rebuild: bool = True) -> bool:
    """Materialize existing processed files to graph.

    Args:
        year: Optional year filter
        rebuild: Whether to rebuild the graph database first

    Returns:
        True if successful, False otherwise
    """
    if year:
      logger.info(f"Materializing SEC graph for year {year}...")
    else:
      logger.info("Materializing SEC graph for ALL years...")

    try:
      # Build config for materialize-only job
      config = {
        "ops": {
          "sec_duckdb_staging": {
            "config": {
              "rebuild": rebuild,
              "year_filter": [year] if year else [],
            }
          },
          "sec_graph_materialized": {
            "config": {
              "graph_id": "sec",
              "ignore_errors": True,
              "rebuild": rebuild,
            }
          },
        }
      }

      # Write config to temp file
      import yaml

      with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(config, f, default_flow_style=False)
        config_path = f.name

      # Set readable permissions before copying
      import os

      os.chmod(config_path, 0o644)

      # Copy to container
      container_path = "/tmp/sec_materialize_config.yaml"
      subprocess.run(
        [
          "docker",
          "cp",
          config_path,
          f"robosystems-dagster-webserver:{container_path}",
        ],
        check=True,
        capture_output=True,
      )

      Path(config_path).unlink()

      # Execute the job
      cmd = [
        "docker",
        "compose",
        "exec",
        "-T",
        "dagster-webserver",
        "dagster",
        "job",
        "execute",
        "-m",
        "robosystems.dagster",
        "--job",
        "sec_materialize_only",
        "-c",
        container_path,
      ]

      result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=600,
      )

      if result.returncode == 0 or "RUN_SUCCESS" in result.stdout:
        logger.info("Materialization complete")
        return True
      else:
        logger.error(
          f"Materialization failed: {result.stderr[-500:] if result.stderr else 'Unknown error'}"
        )
        return False

    except Exception as e:
      logger.error(f"Materialization failed: {e}")
      return False

  def reset_database(self, clear_s3: bool = True) -> bool:
    """Reset SEC database with proper schema creation.

    Note: This still runs locally since it needs direct DB access.

    Args:
        clear_s3: Whether to also clear S3 buckets

    Returns:
        True if successful, False otherwise
    """
    logger.info(f"Resetting SEC database ({self.backend})...")

    try:
      # Use docker compose exec to run reset inside container
      cmd = [
        "docker",
        "compose",
        "exec",
        "-T",
        "api",
        "python",
        "-c",
        """
import asyncio
from robosystems.graph_api.client.factory import get_graph_client
from robosystems.database import SessionFactory
from robosystems.models.iam import GraphSchema

async def reset():
    db = SessionFactory()
    try:
        client = await get_graph_client(graph_id='sec', operation_type='write')

        # Delete existing database
        try:
            await client.delete_database('sec')
            print('Deleted existing database')
        except Exception as e:
            print(f'Database may not exist: {e}')

        # Get schema for recreation
        schema = GraphSchema.get_active_schema('sec', db)
        if not schema:
            print('No schema found for graph sec')
            return False

        # Recreate with schema
        create_db_kwargs = {
            'graph_id': 'sec',
            'schema_type': schema.schema_type,
            'custom_schema_ddl': schema.schema_ddl,
        }

        if schema.schema_type == 'shared':
            create_db_kwargs['repository_name'] = 'sec'

        await client.create_database(**create_db_kwargs)
        print(f'Recreated SEC database with schema: {schema.schema_type}')
        return True
    finally:
        db.close()

asyncio.run(reset())
""",
      ]

      result = subprocess.run(cmd, capture_output=True, text=True)
      if result.returncode != 0:
        logger.error(f"Reset failed: {result.stderr}")
        return False

      logger.info(result.stdout)

    except Exception as e:
      logger.error(f"Failed to reset database: {e}")
      return False

    # Clear S3 buckets if requested
    if clear_s3:
      logger.info("Clearing S3 buckets...")
      self._clear_s3_buckets()

    logger.info("SEC database reset complete")
    return True

  def _clear_s3_buckets(self):
    """Clear LocalStack S3 buckets."""
    buckets = [
      "robosystems-sec-raw",
      "robosystems-sec-processed",
      "robosystems-sec-textblocks",
    ]

    for bucket in buckets:
      try:
        cmd = [
          "aws",
          "s3",
          "rm",
          f"s3://{bucket}",
          "--recursive",
          "--endpoint-url",
          "http://localhost:4566",
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
          logger.info(f"  Cleared: {bucket}")
        elif "NoSuchBucket" in result.stderr:
          logger.debug(f"  Bucket doesn't exist: {bucket}")
        else:
          logger.warning(f"  Failed to clear {bucket}: {result.stderr}")
      except Exception as e:
        logger.warning(f"  Error clearing {bucket}: {e}")


def main():
  """Main entry point for local SEC pipeline."""

  parser = argparse.ArgumentParser(
    description="SEC Local Pipeline - Triggers Dagster jobs via Docker CLI",
    formatter_class=argparse.RawDescriptionHelpFormatter,
    epilog="""
Examples:
  # Reset LadybugDB database and start fresh
  %(prog)s reset

  # Load NVIDIA data for 2024 using Dagster pipeline
  %(prog)s load --ticker NVDA --year 2024

  # Load Apple data for current year
  %(prog)s load --ticker AAPL

  # Materialize existing processed files (skip download/processing)
  %(prog)s materialize --year 2025

  # Full rebuild from existing processed files
  %(prog)s materialize --rebuild
""",
  )

  subparsers = parser.add_subparsers(dest="command", help="Commands")

  # Reset command
  reset_parser = subparsers.add_parser("reset", help="Reset SEC database")
  reset_parser.add_argument(
    "--keep-s3", action="store_true", help="Keep S3 data (only reset database)"
  )
  reset_parser.add_argument(
    "--backend",
    default="ladybug",
    choices=["ladybug", "neo4j"],
    help="Database backend to use (default: ladybug)",
  )

  # Load command
  load_parser = subparsers.add_parser("load", help="Load company data by ticker")
  load_parser.add_argument(
    "--ticker", required=True, help="Company ticker symbol (e.g., NVDA, AAPL)"
  )
  load_parser.add_argument(
    "--year",
    type=int,
    default=None,
    help=f"Year to load (default: {datetime.now().year})",
  )
  load_parser.add_argument(
    "--backend",
    default="ladybug",
    choices=["ladybug", "neo4j"],
    help="Database backend to use (default: ladybug)",
  )

  # Materialize command
  materialize_parser = subparsers.add_parser(
    "materialize",
    help="Materialize existing processed files to graph",
  )
  materialize_parser.add_argument(
    "--year",
    type=int,
    default=None,
    help="Year to process (default: all years)",
  )
  materialize_parser.add_argument(
    "--rebuild",
    action="store_true",
    default=True,
    help="Rebuild graph database from scratch (default: True)",
  )
  materialize_parser.add_argument(
    "--backend",
    default="ladybug",
    choices=["ladybug", "neo4j"],
    help="Database backend to use (default: ladybug)",
  )

  args = parser.parse_args()

  if not args.command:
    parser.print_help()
    return

  # Initialize pipeline with selected backend
  pipeline = SECLocalPipeline(backend=args.backend)

  # Execute command
  if args.command == "reset":
    success = pipeline.reset_database(clear_s3=not args.keep_s3)
    sys.exit(0 if success else 1)

  elif args.command == "load":
    success = pipeline.load_company(
      ticker=args.ticker,
      year=args.year,
    )
    sys.exit(0 if success else 1)

  elif args.command == "materialize":
    success = pipeline.materialize_only(
      year=args.year,
      rebuild=args.rebuild,
    )
    sys.exit(0 if success else 1)


if __name__ == "__main__":
  main()
