"""Adapter elements carry their XBRL-intrinsic metadata — real Postgres.

The loader historically wrote adapter (QuickBooks) CoA elements with
``qname=NULL``, ``period_type='duration'`` (hardcoded — wrong for every
balance-sheet account), and ``item_type=NULL``. The OLAP materializer
papered over the missing qname by synthesizing ``'qb:' || code`` at build
time, so the graph looked healthy while OLTP — the identity-bearing
store — had no identity: no adapter element was addressable through
``update-taxonomy-block`` (patches key on qname), and the update
validator's projection crashed reconstructing associations
(``from_ref=None``).

These tests pin the sync-heals-itself contract:

- INSERT derives qname / period_type / item_type at load time.
- Re-sync heals rows loaded before the loader wrote them (and after a
  source-side account rename moves the code).
- The trait pass heals a stale adapter-written EFS classification (the
  pre-contra-promotion ``asset`` on an accumulated-amortization account)
  while never touching a non-adapter (manual/envelope) trait row.
- A schema-wide qname collision leaves identity unset and warns, rather
  than stealing the string or crashing the sync.
- The update validator reports NULL-qname elements as a legible issue
  instead of a Pydantic crash.
"""

from __future__ import annotations

import os
import uuid
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

import robosystems.models.extensions  # noqa: F401  (register models on ExtensionsBase)
from robosystems.db.extensions import ExtensionsBase
from robosystems.models.extensions import Element, ElementTrait, Trait

pytestmark = pytest.mark.unit


@pytest.fixture()
def ext_session():
  """Extensions schema in the test Postgres DB, one throwaway schema per test."""
  database_url = os.environ.get("TEST_DATABASE_URL")
  if not database_url:
    pytest.skip("TEST_DATABASE_URL not configured")

  schema = f"ext_elmeta_{uuid.uuid4().hex[:12]}"
  engine = create_engine(database_url)
  with engine.begin() as conn:
    conn.execute(text(f'CREATE SCHEMA "{schema}"'))

  session = sessionmaker(bind=engine)()
  session.execute(text(f'SET search_path TO "{schema}"'))
  ExtensionsBase.metadata.create_all(bind=session.connection())
  session.commit()
  session.execute(text(f'SET search_path TO "{schema}"'))

  try:
    yield session
  finally:
    session.rollback()
    session.close()
    with engine.begin() as conn:
      conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    engine.dispose()


def _seed_traits(session) -> dict[str, Trait]:
  """Seed the EFS + liquidity Trait rows the loader's trait pass resolves."""
  traits: dict[str, Trait] = {}
  for category, identifiers in (
    (
      "elementsOfFinancialStatements",
      ["asset", "contraAsset", "liability", "equity", "revenue", "expense"],
    ),
    ("liquidity", ["current", "noncurrent"]),
  ):
    for identifier in identifiers:
      t = Trait(category=category, identifier=identifier, name=identifier)
      session.add(t)
      traits[identifier] = t
  session.flush()
  return traits


def _make_duckdb_mock(tables: dict[str, list[dict]]):
  """DuckDB connection mock returning the given tables (mirrors test_loader)."""
  mock_con = MagicMock()
  table_names = list(tables.keys())

  def execute_side_effect(query):
    result = MagicMock()
    if query == "SHOW TABLES":
      result.fetchall.return_value = [(t,) for t in table_names]
    else:
      for t in table_names:
        if f"SELECT * FROM {t}" in query:
          rows = tables[t]
          if rows:
            columns = list(rows[0].keys())
            result.description = [(c,) for c in columns]
            result.fetchall.return_value = [tuple(r.values()) for r in rows]
          else:
            result.description = []
            result.fetchall.return_value = []
          break
      else:
        result.description = []
        result.fetchall.return_value = []
    return result

  mock_con.execute.side_effect = execute_side_effect
  return mock_con


def _account(
  ext_id: str,
  code: str,
  name: str,
  balance_type: str,
  account_type: str,
  account_sub_type: str | None = None,
) -> dict:
  meta_parts = [f'"account_type": "{account_type}"']
  if account_sub_type is not None:
    meta_parts.append(f'"account_sub_type": "{account_sub_type}"')
  return {
    "external_id": ext_id,
    "external_source": "quickbooks",
    "code": code,
    "name": name,
    "description": None,
    "balance_type": balance_type,
    "external_parent_id": None,
    "depth": 0,
    "path": "",
    "currency": "USD",
    "is_active": True,
    "is_placeholder": False,
    "metadata": "{" + ", ".join(meta_parts) + "}",
  }


def _run_load(session, accounts: list[dict], connection_id: str = "conn_1"):
  from robosystems.operations.extensions.loader import OLTPLoader

  with (
    patch("robosystems.db.extensions.provision_tenant_schema"),
    patch("robosystems.db.extensions.extensions_session") as mock_ext_session,
    patch("duckdb.connect") as mock_duckdb_connect,
  ):
    mock_duckdb_connect.return_value = _make_duckdb_mock({"elements": accounts})
    mock_ext_session.return_value.__enter__ = MagicMock(return_value=session)
    mock_ext_session.return_value.__exit__ = MagicMock(return_value=False)
    return OLTPLoader().load(
      graph_id="kg0123456789abcdef",
      source="quickbooks",
      connection_id=connection_id,
      duckdb_path="/tmp/test.duckdb",
      created_by="user_1",
    )


def _efs_identifiers(session, element_id: str) -> list[str]:
  rows = (
    session.query(Trait.identifier)
    .join(ElementTrait, ElementTrait.trait_id == Trait.id)
    .filter(
      ElementTrait.element_id == element_id,
      Trait.category == "elementsOfFinancialStatements",
    )
    .all()
  )
  return sorted(r[0] for r in rows)


class TestInsertDerivesIntrinsicMetadata:
  def test_qname_period_type_item_type_and_contra_trait(self, ext_session):
    _seed_traits(ext_session)
    ext_session.commit()

    accounts = [
      _account("1", "Truist Checking", "Truist Checking", "debit", "Bank"),
      _account("2", "Consulting Services", "Consulting Services", "credit", "Income"),
      # Contra via the known AccountSubType.
      _account(
        "3",
        "Accumulated depreciation",
        "Accumulated depreciation",
        "debit",
        "Fixed Asset",
        account_sub_type="AccumulatedDepreciation",
      ),
      # Contra via the NAME fallback: user-chosen generic detail type
      # carries no contra signal, so the sub-type rule alone misses it.
      _account(
        "4",
        "Intangible Assets:Accumulated Amortization",
        "Accumulated Amortization",
        "debit",
        "Other Asset",
        account_sub_type="OtherLongTermAssets",
      ),
    ]
    result = _run_load(ext_session, accounts)
    assert result.elements == 4

    by_ext = {
      str(e.external_id): e
      for e in ext_session.query(Element)
      .filter(Element.external_source == "quickbooks")
      .all()
    }

    bank = by_ext["1"]
    assert bank.qname == "qb:Truist Checking"
    assert bank.period_type == "instant"
    assert bank.item_type == "monetary"
    assert _efs_identifiers(ext_session, str(bank.id)) == ["asset"]

    income = by_ext["2"]
    assert income.qname == "qb:Consulting Services"
    assert income.period_type == "duration"
    assert _efs_identifiers(ext_session, str(income.id)) == ["revenue"]

    accum_dep = by_ext["3"]
    assert accum_dep.period_type == "instant"
    assert _efs_identifiers(ext_session, str(accum_dep.id)) == ["contraAsset"]

    accum_amort = by_ext["4"]
    assert accum_amort.qname == "qb:Intangible Assets:Accumulated Amortization"
    assert accum_amort.period_type == "instant"
    assert _efs_identifiers(ext_session, str(accum_amort.id)) == ["contraAsset"]

  def test_equity_accumulated_name_never_flips_to_contra(self, ext_session):
    """AOCI-style equity names must not match the contra-name fallback."""
    _seed_traits(ext_session)
    ext_session.commit()

    accounts = [
      _account(
        "10",
        "Accumulated Adjustment",
        "Accumulated Adjustment",
        "credit",
        "Equity",
      ),
    ]
    _run_load(ext_session, accounts)
    el = ext_session.query(Element).filter(Element.external_id == "10").one()
    assert _efs_identifiers(ext_session, str(el.id)) == ["equity"]
    assert el.period_type == "instant"


class TestResyncHealsPreQnameRows:
  def test_resync_heals_qname_period_type_and_stale_trait(self, ext_session):
    """A row loaded by the pre-qname loader is healed in place on re-sync."""
    traits = _seed_traits(ext_session)

    # Pre-seed the element exactly as the old loader left it: NULL qname,
    # hardcoded duration, no item_type, plain-asset EFS trait written by
    # the adapter.
    stale = Element(
      code="Intangible Assets:Accumulated Amortization",
      name="Accumulated Amortization",
      balance_type="debit",
      period_type="duration",
      source="quickbooks",
      external_id="155",
      external_source="quickbooks",
      connection_id="conn_1",
      metadata_={"account_type": "Other Asset"},
    )
    ext_session.add(stale)
    ext_session.flush()
    ext_session.add(
      ElementTrait(
        element_id=stale.id,
        trait_id=traits["asset"].id,
        is_primary=True,
        source="quickbooks",
      )
    )
    ext_session.commit()
    original_id = str(stale.id)

    accounts = [
      _account(
        "155",
        "Intangible Assets:Accumulated Amortization",
        "Accumulated Amortization",
        "debit",
        "Other Asset",
        account_sub_type="OtherLongTermAssets",
      ),
    ]
    _run_load(ext_session, accounts)

    healed = ext_session.query(Element).filter(Element.external_id == "155").one()
    assert str(healed.id) == original_id  # ULID preserved (UPSERT, not re-mint)
    assert healed.qname == "qb:Intangible Assets:Accumulated Amortization"
    assert healed.period_type == "instant"
    assert healed.item_type == "monetary"
    # The stale adapter-written asset row is gone; contraAsset replaced it.
    assert _efs_identifiers(ext_session, original_id) == ["contraAsset"]

  def test_rename_moves_qname_with_the_code(self, ext_session):
    _seed_traits(ext_session)
    accounts = [_account("7", "Old Name", "Old Name", "debit", "Bank")]
    _run_load(ext_session, accounts)
    renamed = [_account("7", "New Name", "New Name", "debit", "Bank")]
    _run_load(ext_session, renamed)

    el = ext_session.query(Element).filter(Element.external_id == "7").one()
    assert el.qname == "qb:New Name"

  def test_manual_trait_override_is_preserved(self, ext_session):
    """A non-adapter EFS row (source NULL = envelope/manual) is never healed."""
    traits = _seed_traits(ext_session)
    overridden = Element(
      code="Accumulated depreciation",
      name="Accumulated depreciation",
      balance_type="debit",
      period_type="instant",
      source="quickbooks",
      external_id="156",
      external_source="quickbooks",
      connection_id="conn_1",
      metadata_={"account_type": "Fixed Asset"},
    )
    ext_session.add(overridden)
    ext_session.flush()
    # Manual override: deliberately classified plain asset, source NULL.
    ext_session.add(
      ElementTrait(
        element_id=overridden.id,
        trait_id=traits["asset"].id,
        is_primary=True,
      )
    )
    ext_session.commit()

    accounts = [
      _account(
        "156",
        "Accumulated depreciation",
        "Accumulated depreciation",
        "debit",
        "Fixed Asset",
        account_sub_type="AccumulatedDepreciation",
      ),
    ]
    _run_load(ext_session, accounts)

    assert _efs_identifiers(ext_session, str(overridden.id)) == ["asset"]


class TestQnameCollision:
  def test_collision_leaves_qname_unset(self, ext_session):
    """A second connection with the same account code must not steal the qname."""
    _seed_traits(ext_session)
    owner = Element(
      code="Cash",
      name="Cash",
      qname="qb:Cash",
      balance_type="debit",
      period_type="instant",
      source="quickbooks",
      external_id="1",
      external_source="quickbooks",
      connection_id="conn_other",
      metadata_={"account_type": "Bank"},
    )
    ext_session.add(owner)
    ext_session.commit()

    accounts = [_account("1", "Cash", "Cash", "debit", "Bank")]
    _run_load(ext_session, accounts, connection_id="conn_new")

    newcomer = (
      ext_session.query(Element)
      .filter(Element.connection_id == "conn_new", Element.external_id == "1")
      .one()
    )
    assert newcomer.qname is None
    # The owner keeps its identity untouched.
    ext_session.refresh(owner)
    assert owner.qname == "qb:Cash"


class TestUpdateValidatorMissingQname:
  def test_null_qname_elements_reported_legibly(self, ext_session):
    """NULL-qname elements yield a coded issue, not a Pydantic crash."""
    from robosystems.models.api.taxonomy_block import UpdateTaxonomyBlockRequest
    from robosystems.models.extensions import Association, Structure, Taxonomy
    from robosystems.operations.taxonomy_block.update_validator import (
      validate_update_envelope,
    )

    taxonomy = Taxonomy(
      name="Quickbooks Chart of Accounts",
      taxonomy_type="chart_of_accounts",
    )
    ext_session.add(taxonomy)
    ext_session.flush()

    named = Element(
      code="Cash",
      name="Cash",
      qname="qb:Cash",
      balance_type="debit",
      period_type="instant",
      source="quickbooks",
      taxonomy_id=taxonomy.id,
    )
    unnamed = Element(
      code="Accumulated Amortization",
      name="Accumulated Amortization",
      balance_type="debit",
      period_type="duration",
      source="quickbooks",
      taxonomy_id=taxonomy.id,
    )
    ext_session.add_all([named, unnamed])
    ext_session.flush()

    structure = Structure(
      name="Chart of Accounts",
      block_type="chart_of_accounts",
      taxonomy_id=taxonomy.id,
    )
    ext_session.add(structure)
    ext_session.flush()
    ext_session.add(
      Association(
        structure_id=structure.id,
        from_element_id=unnamed.id,
        to_element_id=named.id,
        association_type="presentation",
      )
    )
    ext_session.commit()

    issues = validate_update_envelope(
      ext_session,
      taxonomy,
      UpdateTaxonomyBlockRequest(taxonomy_id=str(taxonomy.id)),
    )

    codes = {i.code for i in issues}
    assert "element_missing_qname" in codes
    issue = next(i for i in issues if i.code == "element_missing_qname")
    assert issue.context["count"] == 1
    assert "Accumulated Amortization" in issue.context["element_names"]
