"""Migration 0067 names its rules by id, so pin them to the ids the library mints."""

from __future__ import annotations

import importlib

import pytest

from robosystems.operations.taxonomy_block.library_creator import _rule_id

pytestmark = pytest.mark.unit


def test_the_migration_names_the_rules_the_library_minted():
  migration = importlib.import_module(
    "migrations.extensions.versions.0067_retire_balance_sheet_identity_rule"
  )
  assert (
    _rule_id("rs-gaap-rules", "rs-gaap-rule-bs-identity") == migration._BS_IDENTITY_RULE
  )
  assert (
    _rule_id("rs-gaap-rollup-rules", "rs-gaap-rollup-is-netincome")
    == migration._NET_INCOME_ROLLUP
  )
