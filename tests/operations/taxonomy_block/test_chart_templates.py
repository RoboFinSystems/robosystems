"""The shipped chart-of-accounts templates stay honest.

Templates are data — ``frameworks/chart-templates/<key>/v1/chart.jsonld``
plus one ``mappings/<framework>.jsonld`` per framework — read by the
registry at import. Four invariants, none of which need a database:

1. Every template is internally consistent — unique codes, a valid trait
   and balance on every row, every mapped code is an account, every legal
   form maps both equity accounts, and the file-level identities hold
   (``key`` = directory, ``framework`` = file stem).
2. Every mapping target is a real concept in its framework's package source
   — for rs-gaap, ``frameworks/rs-gaap/packages/rs-gaap/v1/taxonomy.jsonld``
   — so a library rename surfaces here instead of as ``unresolved`` on a
   customer's first day.
3. The demos and the templates are one thing: ``saas`` and ``services``
   read their chart and mappings from the registry; ``product`` is the
   coffee roaster's chart with generalized names, pinned structurally
   (codes, traits, sub-classifications, balance types, mappings).
4. The registry never sees the library: the templates directory carries no
   manifest, so framework discovery skips it, and nothing here is a seed
   path.
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
  DEFAULT_FORM,
  LEGAL_FORMS,
  TEMPLATE_KEYS,
  TEMPLATES_DIR,
  get_template,
  list_templates,
  resolve_form,
)
from robosystems.taxonomy.discovery import (
  FRAMEWORKS_DIR,
  list_framework_manifests,
)

RS_GAAP_SOURCE = (
  FRAMEWORKS_DIR / "rs-gaap" / "packages" / "rs-gaap" / "v1" / "taxonomy.jsonld"
)

VALID_TRAITS = {"asset", "liability", "equity", "revenue", "expense"}
EQUITY_CODES = ("3000", "3100")


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
    assert [t.ordinal for t in list_templates()] == [0, 1, 2]

  def test_lookup_is_case_and_whitespace_tolerant(self) -> None:
    assert get_template(" SaaS ") is CHART_TEMPLATES["saas"]
    assert get_template("nope") is None
    assert get_template("") is None

  def test_templates_are_data_under_frameworks(self) -> None:
    """The stencil lives beside the library, is read from disk, and carries
    no manifest — so discovery never treats it as a framework."""
    assert TEMPLATES_DIR == FRAMEWORKS_DIR / "chart-templates"
    assert (TEMPLATES_DIR / "README.md").exists()
    for template in list_templates():
      assert template.path == TEMPLATES_DIR / template.key / "v1"
      assert (template.path / "chart.jsonld").exists()
      assert (template.path / "mappings" / "rs-gaap.jsonld").exists()
    manifests = {p.parent.name for p in list_framework_manifests()}
    assert "chart-templates" not in manifests
    assert not list(TEMPLATES_DIR.glob("*.json")), (
      "a top-level .json under chart-templates/ would be read as a manifest"
    )

  @pytest.mark.parametrize("key", TEMPLATE_KEYS)
  def test_template_is_internally_consistent(self, key: str) -> None:
    template = CHART_TEMPLATES[key]
    codes = [row[0] for row in template.accounts]
    assert len(codes) == len(set(codes)), f"{key}: duplicate account codes"
    assert template.account_count == len(codes)
    assert template.display_name and template.description
    assert template.frameworks == ("rs-gaap",)

    for code, name, trait, sub_classification, balance_type, _desc in template.accounts:
      assert code.isdigit() and len(code) == 4, (key, code)
      assert name.strip(), (key, code)
      assert trait in VALID_TRAITS, (key, code, trait)
      assert sub_classification, (key, code)
      assert balance_type in ("debit", "credit"), (key, code, balance_type)

    mapping_set = template.mappings["rs-gaap"]
    assert mapping_set.structure_name == "CoA to US GAAP Mapping"
    assert set(mapping_set.variants) == set(LEGAL_FORMS)
    assert mapping_set.default_variant == DEFAULT_FORM
    for form in (*LEGAL_FORMS, "unknown-form", ""):
      arcs = mapping_set.arcs_for(form)
      mapped_codes = [code for code, _q in arcs]
      assert set(mapped_codes) <= set(codes), (
        key,
        form,
        set(mapped_codes) - set(codes),
      )
      assert len(mapped_codes) == len(set(mapped_codes)), (key, form, "duplicate")
      assert set(EQUITY_CODES) <= set(mapped_codes), (key, form, "equity unmapped")
      for _code, qname in arcs:
        assert qname.startswith("rs-gaap:"), (key, form, qname)

  @pytest.mark.parametrize("key", TEMPLATE_KEYS)
  def test_file_identities_hold(self, key: str) -> None:
    template = CHART_TEMPLATES[key]
    chart = json.loads((template.path / "chart.jsonld").read_text())
    assert chart["key"] == key == template.path.parent.name
    assert chart["@type"] == "rs:ChartTemplate"
    for row in chart["accounts"]:
      assert row["hasTrait"].startswith("trait:elementsOfFinancialStatements/")
    for mapping_path in sorted((template.path / "mappings").glob("*.jsonld")):
      doc = json.loads(mapping_path.read_text())
      assert doc["framework"] == mapping_path.stem
      assert doc["@type"] == "rs:ChartMapping"

  def test_equity_forms_swap_only_the_equity_rows(self) -> None:
    mapping_set = CHART_TEMPLATES["saas"].mappings["rs-gaap"]
    corp = dict(mapping_set.arcs_for("corporation"))
    llc = dict(mapping_set.arcs_for("llc"))
    partnership = dict(mapping_set.arcs_for("partnership"))
    for code in corp:
      if code in EQUITY_CODES:
        continue
      assert corp[code] == llc[code] == partnership[code], code
    assert llc["3000"] == "rs-gaap:MembersEquity"
    assert partnership["3100"] == "rs-gaap:PartnersCapital"
    assert corp["3100"] == "rs-gaap:RetainedEarningsAccumulatedDeficit"
    assert {c for c, _q in mapping_set.arcs}.isdisjoint(EQUITY_CODES)

  def test_resolve_form_normalises_and_falls_back(self) -> None:
    assert resolve_form(" LLC ") == "llc"
    assert resolve_form("Partnership") == "partnership"
    assert resolve_form("sole-prop") == "corporation"
    assert resolve_form("") == "corporation"
    assert resolve_form(None) == "corporation"
    mapping_set = CHART_TEMPLATES["saas"].mappings["rs-gaap"]
    assert mapping_set.resolve_variant("sole-prop") == "corporation"
    assert mapping_set.resolve_variant(" Partnership ") == "partnership"


@pytest.mark.unit
class TestLibraryTargets:
  @pytest.mark.parametrize("key", TEMPLATE_KEYS)
  def test_every_mapping_target_is_an_rs_gaap_concept(
    self, key: str, rs_gaap_qnames: set[str]
  ) -> None:
    mapping_set = CHART_TEMPLATES[key].mappings["rs-gaap"]
    targets = {
      qname for form in LEGAL_FORMS for _code, qname in mapping_set.arcs_for(form)
    }
    missing = sorted(targets - rs_gaap_qnames)
    assert not missing, f"{key}: not in rs-gaap source: {missing}"


@pytest.mark.unit
class TestDemosAreTheTemplates:
  def test_saas_demo_reads_the_template(self) -> None:
    template = CHART_TEMPLATES["saas"]
    assert saas_demo_data.ACCOUNTS is template.accounts
    assert saas_demo_mappings.mappings_for("llc") == template.mappings[
      "rs-gaap"
    ].arcs_for("llc")
    assert (
      template.mappings["rs-gaap"].arcs_for("corporation")
      == saas_demo_mappings.MAPPINGS
    )

  def test_services_demo_reads_the_template(self) -> None:
    template = CHART_TEMPLATES["services"]
    assert services_demo_data.ACCOUNTS is template.accounts
    assert services_demo_mappings.mappings_for("partnership") == template.mappings[
      "rs-gaap"
    ].arcs_for("partnership")

  def test_product_template_is_the_coffee_chart_with_general_names(self) -> None:
    """Same codes, traits, sub-classifications, balances and mappings; only
    the roaster-flavoured names differ."""
    template = CHART_TEMPLATES["product"]

    def structure(rows):
      return [(code, trait, sub, balance) for code, _n, trait, sub, balance, _d in rows]

    assert structure(template.accounts) == structure(coffee_data.ACCOUNTS)
    for form in LEGAL_FORMS:
      assert template.mappings["rs-gaap"].arcs_for(form) == (
        coffee_mappings.mappings_for(form)
      ), form
    renamed = {row[1] for row in template.accounts} ^ {
      row[1] for row in coffee_data.ACCOUNTS
    }
    assert "Inventory — Green Coffee" in renamed
    assert "Inventory — Raw Materials" in renamed


@pytest.mark.unit
class TestLoaderRejectsDrift:
  """The registry refuses a file whose identity disagrees with its path, so
  a copy-paste template cannot silently shadow another."""

  def test_key_must_match_directory(self, tmp_path: Path) -> None:
    from robosystems.operations.taxonomy_block.chart_templates import (
      _load_catalogue,
    )

    src = TEMPLATES_DIR / "saas" / "v1"
    dst = tmp_path / "retail" / "v1"
    (dst / "mappings").mkdir(parents=True)
    (dst / "chart.jsonld").write_text((src / "chart.jsonld").read_text())
    with pytest.raises(ValueError, match="does not match the directory name"):
      _load_catalogue(tmp_path)

  def test_framework_must_match_file_stem(self, tmp_path: Path) -> None:
    from robosystems.operations.taxonomy_block.chart_templates import (
      _load_catalogue,
    )

    src = TEMPLATES_DIR / "saas" / "v1"
    dst = tmp_path / "saas" / "v1"
    (dst / "mappings").mkdir(parents=True)
    (dst / "chart.jsonld").write_text((src / "chart.jsonld").read_text())
    (dst / "mappings" / "rs-irs.jsonld").write_text(
      (src / "mappings" / "rs-gaap.jsonld").read_text()
    )
    with pytest.raises(ValueError, match="does not match the file name"):
      _load_catalogue(tmp_path)
