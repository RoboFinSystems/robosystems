"""The shipped chart-of-accounts templates stay honest.

Three invariants, none of which need a database:

1. Every template is internally consistent — unique codes, every mapped
   code is an account, every equity form maps both equity accounts.
2. Every mapping target is a real rs-gaap concept in the framework source
   the library is seeded from, so a library rename surfaces here instead
   of as ``unresolved`` on a customer's first day.
3. The demos and the templates are one thing: ``saas`` and ``services``
   are imported by their demos; ``product`` is the coffee roaster's chart
   with generalized names, pinned structurally (codes, traits,
   sub-classifications, balance types, mappings).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from examples.coffee_roaster_demo import data as coffee_data
from examples.coffee_roaster_demo import mappings as coffee_mappings
from examples.roboledger_demo import data as services_demo_data
from examples.roboledger_demo import mappings as services_demo_mappings
from examples.saas_startup_demo import data as saas_demo_data
from examples.saas_startup_demo import mappings as saas_demo_mappings
from robosystems.operations.taxonomy_block.chart_templates import (
  CHART_TEMPLATES,
  TEMPLATE_KEYS,
  get_template,
  list_templates,
)
from robosystems.operations.taxonomy_block.chart_templates._forms import (
  EQUITY_BY_FORM,
  EQUITY_CODES,
)

RS_GAAP_SOURCE = (
  Path(__file__).resolve().parents[3]
  / "frameworks"
  / "rs-gaap"
  / "packages"
  / "rs-gaap"
  / "v1"
  / "taxonomy.jsonld"
)

VALID_TRAITS = {"asset", "liability", "equity", "revenue", "expense"}


@pytest.fixture(scope="module")
def rs_gaap_qnames() -> set[str]:
  graph = json.loads(RS_GAAP_SOURCE.read_text())
  nodes = graph.get("@graph", graph)
  ids: set[str] = set()

  def walk(node: object) -> None:
    if isinstance(node, dict):
      identifier = node.get("@id")
      if isinstance(identifier, str) and identifier.startswith("rs-gaap:"):
        ids.add(identifier)
      for value in node.values():
        walk(value)
    elif isinstance(node, list):
      for item in node:
        walk(item)

  walk(nodes)
  assert len(ids) > 1000, "rs-gaap source did not parse into concept ids"
  return ids


@pytest.mark.unit
class TestRegistry:
  def test_three_templates_with_stable_keys(self) -> None:
    assert TEMPLATE_KEYS == ("saas", "services", "product")
    assert [t.key for t in list_templates()] == list(TEMPLATE_KEYS)

  def test_lookup_is_case_and_whitespace_tolerant(self) -> None:
    assert get_template(" SaaS ") is CHART_TEMPLATES["saas"]
    assert get_template("nope") is None
    assert get_template("") is None

  @pytest.mark.parametrize("key", TEMPLATE_KEYS)
  def test_template_is_internally_consistent(self, key: str) -> None:
    template = CHART_TEMPLATES[key]
    codes = [row[0] for row in template.accounts]
    assert len(codes) == len(set(codes)), f"{key}: duplicate account codes"
    assert template.account_count == len(codes)
    assert template.display_name and template.description

    for code, name, trait, sub_classification, balance_type, _desc in template.accounts:
      assert code.isdigit() and len(code) == 4, (key, code)
      assert name.strip(), (key, code)
      assert trait in VALID_TRAITS, (key, code, trait)
      assert sub_classification, (key, code)
      assert balance_type in ("debit", "credit"), (key, code, balance_type)

    for form in ("corporation", "llc", "partnership", "unknown-form", ""):
      mappings = template.mappings_for(form)
      mapped_codes = [code for code, _q in mappings]
      assert set(mapped_codes) <= set(codes), (
        key,
        form,
        set(mapped_codes) - set(codes),
      )
      assert len(mapped_codes) == len(set(mapped_codes)), (key, form, "duplicate")
      assert set(EQUITY_CODES) <= set(mapped_codes), (key, form, "equity unmapped")

  def test_equity_forms_swap_only_the_equity_rows(self) -> None:
    template = CHART_TEMPLATES["saas"]
    corp = dict(template.mappings_for("corporation"))
    llc = dict(template.mappings_for("llc"))
    partnership = dict(template.mappings_for("partnership"))
    for code in corp:
      if code in EQUITY_CODES:
        continue
      assert corp[code] == llc[code] == partnership[code], code
    assert llc["3000"] == "rs-gaap:MembersEquity"
    assert partnership["3100"] == "rs-gaap:PartnersCapital"
    assert dict(EQUITY_BY_FORM["corporation"])["3100"] == corp["3100"]


@pytest.mark.unit
class TestLibraryTargets:
  @pytest.mark.parametrize("key", TEMPLATE_KEYS)
  def test_every_mapping_target_is_an_rs_gaap_concept(
    self, key: str, rs_gaap_qnames: set[str]
  ) -> None:
    template = CHART_TEMPLATES[key]
    targets = {
      qname for form in EQUITY_BY_FORM for _code, qname in template.mappings_for(form)
    }
    missing = sorted(targets - rs_gaap_qnames)
    assert not missing, f"{key}: not in rs-gaap source: {missing}"


@pytest.mark.unit
class TestDemosAreTheTemplates:
  def test_saas_demo_imports_the_template(self) -> None:
    assert saas_demo_data.ACCOUNTS is CHART_TEMPLATES["saas"].accounts
    assert saas_demo_mappings.mappings_for is CHART_TEMPLATES["saas"].mappings_for

  def test_services_demo_imports_the_template(self) -> None:
    assert services_demo_data.ACCOUNTS is CHART_TEMPLATES["services"].accounts
    assert (
      services_demo_mappings.mappings_for is CHART_TEMPLATES["services"].mappings_for
    )

  def test_product_template_is_the_coffee_chart_with_general_names(self) -> None:
    """Same codes, traits, sub-classifications, balances and mappings; only
    the roaster-flavoured names differ."""
    template = CHART_TEMPLATES["product"]

    def structure(rows):
      return [(code, trait, sub, balance) for code, _n, trait, sub, balance, _d in rows]

    assert structure(template.accounts) == structure(coffee_data.ACCOUNTS)
    for form in EQUITY_BY_FORM:
      assert template.mappings_for(form) == coffee_mappings.mappings_for(form), form
    renamed = {row[1] for row in template.accounts} ^ {
      row[1] for row in coffee_data.ACCOUNTS
    }
    assert "Inventory — Green Coffee" in renamed
    assert "Inventory — Raw Materials" in renamed
