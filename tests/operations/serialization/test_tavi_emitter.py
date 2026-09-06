"""Tests for the Tavi flavor end to end (``xbrl/tavi.py``).

The bridge is tested on its own in ``test_model_bridge.py``; these pin what a
consumer of the file sees — the envelope, the entity, the group labels the
report-components adapter titles sections from, the literal forms it reads —
and that the flavor dispatches through ``serialize_to_xbrl``.
"""

from __future__ import annotations

import json
from typing import Any

import pytest
from xbrlkit.namespaces import ENTITY_SCHEME, TAVI_REPORT_BASE
from xbrlkit.serialize.tavi import DOCTYPE_COMPILED

from robosystems.operations.serialization import XbrlFlavor, serialize_to_xbrl
from robosystems.operations.serialization.xbrl.tavi import (
  TAVI_OMITTED_CONTENT,
  serialize_to_tavi,
  tavi_description,
)
from tests.operations.serialization.test_model_bridge import _bundle


def _document() -> dict[str, Any]:
  return json.loads(serialize_to_tavi(_bundle()))


def _fact(document: dict[str, Any], concept: str) -> dict[str, Any]:
  return next(
    f
    for f in document["xbrlModel"]["facts"]
    if f["factDimensions"]["xbrl:concept"] == concept
  )


def _labels_of(document: dict[str, Any], name: str) -> dict[str, str]:
  return {
    entry["labelType"]: entry["value"]
    for entry in document["xbrlModel"]["labels"]
    if entry["forObject"] == name
  }


@pytest.mark.unit
def test_envelope_is_a_compiled_model_scoped_on_the_report() -> None:
  info = _document()["documentInfo"]
  assert info["documentType"] == DOCTYPE_COMPILED
  assert info["namespaces"]["rpt"] == f"{TAVI_REPORT_BASE}/rpt_test"
  assert info["namespaces"]["entity"] == ENTITY_SCHEME
  assert "cik" not in info["namespaces"]
  assert info["namespaces"]["rs-gaap"] == "https://robosystems.ai/taxonomy/rs-gaap/v1/"
  assert info["description"] == (
    "RoboLedger report rpt_test g2 (Test Co) projected from its StatementBundle "
    "by robosystems"
  )


@pytest.mark.unit
def test_entity_is_named_under_its_scheme_and_labelled() -> None:
  document = _document()
  assert document["xbrlModel"]["entities"] == [{"name": "entity:ent_01"}]
  assert _labels_of(document, "entity:ent_01") == {"xbrl:label": "Test Co"}
  assert _fact(document, "rs-gaap:Assets")["factDimensions"]["xbrl:entity"] == (
    "entity:ent_01"
  )


@pytest.mark.unit
def test_groups_carry_sec_shaped_definitions_the_adapter_titles_from() -> None:
  document = _document()
  model = document["xbrlModel"]
  by_uri = {g["groupURI"]: g["name"] for g in model["groups"]}
  assert _labels_of(document, by_uri["http://robosystems.ai/role/BS"]) == {
    "xbrl:label": "0001 - Statement - Balance Sheet",
    "xbrl:documentation": "Balance Sheet — Classified",
  }
  assert _labels_of(document, by_uri["https://robosystems.ai/role/struct_note1"]) == {
    "xbrl:label": "0100 - Disclosure - Significant Accounting Policies",
  }
  # The statement's presentation and calculation networks share its group.
  contents = {gc["forObject"]: gc["groupName"] for gc in model["groupContents"]}
  kinds = {n["name"]: n["relationshipTypeName"] for n in model["networks"]}
  bs_networks = {
    name
    for name, group in contents.items()
    if group == by_uri["http://robosystems.ai/role/BS"]
  }
  assert {kinds[name] for name in bs_networks} == {
    "xbrl:parent-child",
    "xbrl:summation-item",
  }


@pytest.mark.unit
def test_period_literals_are_exclusive_end_datetimes() -> None:
  document = _document()
  assert _fact(document, "rs-gaap:Assets")["factDimensions"]["xbrl:period"] == (
    "2025-01-01T00:00:00"
  )
  assert _fact(document, "custom:Widgets")["factDimensions"]["xbrl:period"] == (
    "2024-01-01T00:00:00/2025-01-01T00:00:00"
  )


@pytest.mark.unit
def test_fact_values_are_lexical_and_precise_amounts_carry_no_decimals() -> None:
  document = _document()
  assets = _fact(document, "rs-gaap:Assets")
  assert assets["factValues"] == [{"value": "295183000"}]
  assert assets["factDimensions"]["xbrl:unit"] == "iso4217:USD"
  current = _fact(document, "rs-gaap:AssetsCurrent")
  assert current["factValues"] == [{"value": "148000000", "decimals": -3}]
  shares = _fact(document, "rs-gaap:SharesOutstanding")
  assert shares["factDimensions"]["xbrl:unit"] == "xbrla:shares"


@pytest.mark.unit
def test_text_blocks_are_html_with_a_language() -> None:
  document = _document()
  policies = _fact(document, "rs-gaap:PoliciesTextBlock")
  assert policies["factDimensions"]["xbrl:language"] == "en"
  assert "xbrl:unit" not in policies["factDimensions"]
  (value,) = policies["factValues"]
  assert value["language"] == "en"
  assert value["value"].startswith("<h1>Policies</h1>")
  assert "&lt;b&gt;x&lt;/b&gt;" in value["value"]
  concepts = {c["name"]: c for c in document["xbrlModel"]["concepts"]}
  assert concepts["rs-gaap:PoliciesTextBlock"]["dataType"] == "xbrlr:textBlock"
  assert "rs-gaap:AssetsAbstract" in {
    h["name"] for h in document["xbrlModel"]["headings"]
  }


@pytest.mark.unit
def test_roots_lead_the_presentation_network_and_calc_arcs_carry_weight() -> None:
  model = _document()["xbrlModel"]
  presentation = next(
    n for n in model["networks"] if n["relationshipTypeName"] == "xbrl:parent-child"
  )
  first = presentation["relationships"][0]
  assert first["source"] == "xbrl:rootSource"
  assert first["target"] == "rs-gaap:AssetsAbstract"
  calculation = next(
    n for n in model["networks"] if n["relationshipTypeName"] == "xbrl:summation-item"
  )
  assert calculation["relationships"][-1]["properties"] == [
    {"property": "xbrl:weight", "value": 1.0}
  ]


@pytest.mark.unit
def test_serialization_is_compact_deterministic_and_dispatched() -> None:
  first = serialize_to_tavi(_bundle())
  second = serialize_to_xbrl(_bundle(), XbrlFlavor.TAVI)
  assert first == second
  assert b": " not in first and b", " not in first
  assert json.loads(first)["documentInfo"]["documentType"] == DOCTYPE_COMPILED


@pytest.mark.unit
def test_description_and_omitted_content_name_what_the_file_lacks() -> None:
  assert tavi_description(
    _bundle(
      mode="live", report_meta=None, live_meta={"snapshot_at": "2025-03-01T12:30:00Z"}
    )
  ).startswith("RoboLedger live snapshot (Test Co)")
  assert "ib_envelopes" in TAVI_OMITTED_CONTENT
  assert "definition_links" in TAVI_OMITTED_CONTENT
