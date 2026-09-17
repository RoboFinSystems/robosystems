"""Fixtures the database-backed Plaid cases share with the ledger's own.

``session`` opens a schema-per-test tenant in ``robosystems_test`` (skipped
without ``TEST_DATABASE_URL``); ``_skip_platform_db_checks`` stubs the two
platform-DB validations the kernel runs on capture.
"""

from tests.operations.roboledger.commands.test_reconciling_items_db import (
  _skip_platform_db_checks,
  session,
)

__all__ = ["_skip_platform_db_checks", "session"]
