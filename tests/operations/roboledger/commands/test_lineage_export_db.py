"""The holon with lineage, built from a provisioned tenant: a closed June and a
live July. June's revenue figure carries a contribution whose one member line
is the posting, with its entry and event; July's carries nothing, because a
live column has nothing frozen to stand on; every frozen figure's hash
reproduces, and xbrlkit reads the file as the same report."""

from __future__ import annotations

import json
from collections import Counter
from unittest.mock import MagicMock, patch

import pytest
from xbrlkit.deserialize.holon import from_holon_report

from robosystems.operations.serialization import (
  build_report_bundle,
  serialize_to_holon_jsonld,
  serialize_to_lineage_holon_jsonld,
)
from tests.operations.roboledger.commands.test_report_columns_db import (
  GRAPH,
  JULY,
  JUNE,
  _close,
  _report,
  _revenue_rows,
  books,  # noqa: F401
)

pytestmark = pytest.mark.integration


def _platform_graph():
  """The bundle producer reads the graph's framework pin from the platform
  database, which the tenant fixture does not provision."""
  platform = MagicMock()
  query = platform.__enter__.return_value.query.return_value
  query.filter.return_value.first.return_value = MagicMock()
  return patch("robosystems.database.platform_session", return_value=platform)


def test_the_lineage_holon_carries_the_frozen_column_and_checks_out(books):  # noqa: F811
  g = books
  _close(g, "2026-06")
  report = _report(g)
  session = g["session"]

  with (
    _platform_graph(),
    patch("robosystems.taxonomy.pins.resolve_pin", return_value={"rs-gaap": "v1"}),
  ):
    bundle = build_report_bundle(session, GRAPH, report.id)
  entitled = serialize_to_lineage_holon_jsonld(session, bundle)
  document = json.loads(entitled)
  graphs = {gr["@id"].rsplit("#", 1)[1]: gr["@graph"] for gr in document["@graph"]}
  assert set(graphs) == {"scene", "boundary", "projection", "lineage"}

  facts = {n["internalId"]: n for n in graphs["scene"] if n.get("@type") == "rs:Fact"}
  revenue = _revenue_rows(g, report.id)
  june, july = revenue[JUNE[1]], revenue[JULY[1]]
  assert "derivedFrom" in facts[june.id]
  assert "derivedFrom" not in facts[july.id]

  # Every frozen pivot figure bridged: the reconstruction is the stamper's read.
  hashed = {fid for fid, n in facts.items() if "rs:contentHash" in n}
  bridged = {fid for fid, n in facts.items() if "derivedFrom" in n}
  assert hashed and bridged == hashed

  nodes = {n["@id"]: n for n in graphs["lineage"]}
  (contribution_iri,) = facts[june.id]["derivedFrom"]
  contribution = nodes[contribution_iri]
  assert contribution["lineCount"] == 1
  assert contribution["describedLineCount"] == 1
  assert contribution["amount"] == "1000.00"
  (line_iri,) = contribution["hadMember"]
  line = nodes[line_iri]
  assert line["credit"] == "1000.00"
  entry = nodes[line["entry"]]
  assert entry["entryStatus"] == "posted"
  assert entry["postingDate"] == "2026-06-10"
  event = nodes[entry["generatedBy"]]
  assert event["eventType"] == "journal_entry_recorded"
  types = Counter(
    t
    for n in graphs["lineage"]
    for t in (n["@type"] if isinstance(n["@type"], list) else [n["@type"]])
  )
  assert "rs:Entity" not in types and "rs:Element" not in types
  assert types["rs:LedgerEntity"] == 1 and types["rs:Account"] >= 2

  plain_model, _ = from_holon_report(serialize_to_holon_jsonld(bundle))
  model, _ = from_holon_report(entitled)
  assert len(model.facts) == len(plain_model.facts)
  assert model.entity == plain_model.entity
