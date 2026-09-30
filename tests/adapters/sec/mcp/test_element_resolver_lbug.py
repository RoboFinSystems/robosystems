"""The label-text fallback's Cypher runs on a real embedded LadybugDB."""

from __future__ import annotations

from typing import Any

import ladybug as lbug
import pytest

from robosystems.adapters.sec.mcp.element_resolver import _resolve_text_fallback

LABEL_ROLE = "http://www.xbrl.org/2003/role/label"


class _EmbeddedClient:
  graph_id = "sec"

  def __init__(self, conn: lbug.Connection):
    self._conn = conn

  async def execute_query(
    self, query: str, parameters: dict[str, Any] | None = None
  ) -> list[dict[str, Any]]:
    result = self._conn.execute(query, parameters or {})
    columns = result.get_column_names()
    return [dict(zip(columns, row, strict=True)) for row in result.get_all()]


@pytest.fixture()
def client(tmp_path):
  db = lbug.Database(str(tmp_path / "sec.lbug"))
  conn = lbug.Connection(db)
  conn.execute(
    "CREATE NODE TABLE Element(identifier STRING, qname STRING, "
    "canonical_concept STRING, canonical_confidence DOUBLE, PRIMARY KEY(identifier))"
  )
  conn.execute(
    "CREATE NODE TABLE Label(identifier STRING, value STRING, type STRING, "
    "PRIMARY KEY(identifier))"
  )
  conn.execute("CREATE REL TABLE ELEMENT_HAS_LABEL(FROM Element TO Label)")
  for ident, qname, confidence, label in [
    ("e1", "us-gaap:Revenues", 0.95, "revenues"),
    ("e2", "acme:OtherRevenue", 0.4, "other revenue"),
    ("e3", "us-gaap:Assets", 0.99, "assets"),
  ]:
    conn.execute(
      "CREATE (:Element {identifier: $i, qname: $q, canonical_concept: 'c', "
      "canonical_confidence: $conf})-[:ELEMENT_HAS_LABEL]->"
      "(:Label {identifier: $l, value: $v, type: $t})",
      {
        "i": ident,
        "q": qname,
        "conf": confidence,
        "l": f"l_{ident}",
        "v": label,
        "t": LABEL_ROLE,
      },
    )
  yield _EmbeddedClient(conn)
  conn.close()
  db.close()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_untickered_text_fallback_returns_matches_by_confidence(client):
  result = await _resolve_text_fallback(
    client, {"matches": []}, "Revenue", ticker=None, report_id=None
  )

  assert [m["qname"] for m in result["matches"]] == [
    "us-gaap:Revenues",
    "acme:OtherRevenue",
  ]
