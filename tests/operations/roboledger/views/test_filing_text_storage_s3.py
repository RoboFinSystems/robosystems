"""A filing's text read from LocalStack S3 when one object's body breaks
mid-read: the read is an error to retry, and nothing degraded is cached."""

from __future__ import annotations

import json
from datetime import date

import pytest
from urllib3.exceptions import ProtocolError
from xbrlkit.model import (
  Concept,
  EntityIdentity,
  FilingMeta,
  Label,
  XbrlFact,
  XbrlModel,
)
from xbrlkit.periods import duration_period
from xbrlkit.serialize import to_holon

from robosystems.config import env
from robosystems.config.storage.shared import (
  get_filing_artifact_key,
  get_filing_catalog_key,
)
from robosystems.operations.aws.s3 import S3Client
from robosystems.operations.roboledger.views import filing_text as module
from robosystems.operations.roboledger.views.information_blocks import (
  PublicStorageError,
)

TICKER = "ZZTXT"
CIK = "0009999902"
INLINE = "0009999902-25-000001"
CLASSIC = "0009999902-17-000001"
DOC = "zz-10k.htm"
FRAGMENT = "fact_policy.html"
DOC_PHRASE = "customer concentration"
FRAGMENT_PHRASE = "recognized on shipment"


def _model(accession: str, filed: date, policy: str) -> XbrlModel:
  year = duration_period(date(filed.year - 1, 1, 1), date(filed.year - 1, 12, 31))
  concept = Concept(
    qname="us-gaap:RevenueRecognitionPolicyTextBlock",
    namespace="http://fasb.org/us-gaap/2024",
    name="RevenueRecognitionPolicyTextBlock",
    pref_label="Revenue Recognition",
    labels=[
      Label(value="Revenue Recognition", role="http://www.xbrl.org/2003/role/label")
    ],
    period_type="duration",
    is_textblock=True,
    is_text_fact=True,
  )
  return XbrlModel(
    filing=FilingMeta(
      accession=accession,
      cik=CIK,
      form="10-K",
      filing_date=filed,
      primary_document=DOC,
    ),
    entity=EntityIdentity(cik=CIK, name="ZZ Text Corp", ticker=TICKER),
    concepts={concept.qname: concept},
    periods=[year],
    facts=[
      XbrlFact(
        id="f1",
        entity_cik=CIK,
        entity_scheme="http://www.sec.gov/CIK",
        entity_identifier=CIK,
        concept_qname=concept.qname,
        period_id=year.id,
        value_kind="text",
        value_str=policy,
        raw_value=policy,
      )
    ],
  )


def _filing(accession: str, filed: str, kinds: list[str]) -> dict:
  names = {"holon": "holon.jsonld", "document": DOC}
  return {
    "accession": accession,
    "form": "10-K",
    "filing_date": filed,
    "fiscal_year": int(filed[:4]) - 1,
    "fiscal_period": "FY",
    "report_id": f"rpt-{accession}",
    "representations": [{"kind": k, "name": names[k]} for k in kinds],
  }


def _objects() -> dict[str, str]:
  inline = lambda name: get_filing_artifact_key(2025, CIK, INLINE, name)  # noqa: E731
  classic = lambda name: get_filing_artifact_key(2017, CIK, CLASSIC, name)  # noqa: E731
  holon = {"kind": "holon", "name": "holon.jsonld"}
  fragment_url = f"https://cdn.example.com/{classic(FRAGMENT)}"
  return {
    get_filing_catalog_key(TICKER): json.dumps(
      {
        "cik": CIK,
        "ticker": TICKER,
        "filings": [
          _filing(INLINE, "2025-02-05", ["holon", "document"]),
          _filing(CLASSIC, "2017-11-03", ["holon"]),
        ],
      }
    ),
    inline("manifest.json"): json.dumps(
      {
        "form": "10-K",
        "is_inline_xbrl": True,
        "representations": [holon, {"kind": "document", "name": DOC}],
      }
    ),
    inline("holon.jsonld"): to_holon(_model(INLINE, date(2025, 2, 5), "<p>Policy</p>")),
    inline(DOC): (
      "<html><body><p>FORM 10-K</p><p>Item 1. Business</p><p>One buyer is a "
      f"{DOC_PHRASE} we watch.</p></body></html>"
    ),
    classic("manifest.json"): json.dumps({"form": "10-K", "representations": [holon]}),
    classic("holon.jsonld"): to_holon(_model(CLASSIC, date(2017, 11, 3), fragment_url)),
    classic(FRAGMENT): (
      f"<p>In 2017 the company's revenue was {FRAGMENT_PHRASE} of each widget, "
      "with a reserve for returns estimated from three years of history.</p>"
    ),
  }


class _Cache:
  def __init__(self) -> None:
    self.store: dict[str, bytes] = {}

  async def get(self, key: str) -> bytes | None:
    return self.store.get(key)

  async def set(self, key: str, value: bytes, ex: int | None = None) -> None:
    self.store[key] = value


@pytest.fixture
def bucket(monkeypatch: pytest.MonkeyPatch):
  s3 = S3Client()
  if not str(s3.s3_client.meta.endpoint_url).startswith("http://localhost"):
    pytest.skip("needs LocalStack")
  name = env.PUBLIC_DATA_BUCKET
  objects = _objects()
  for key, body in objects.items():
    s3.s3_client.put_object(Bucket=name, Key=key, Body=body.encode())
  monkeypatch.setattr(module, "S3Client", lambda: s3)
  monkeypatch.setattr(module, "is_shared_repository_or_subgraph", lambda graph_id: True)
  cache = _Cache()
  monkeypatch.setattr(module, "_cache", lambda: cache)
  yield s3
  for key in objects:
    s3.s3_client.delete_object(Bucket=name, Key=key)


def _break_body_once(s3: S3Client, key_suffix: str) -> None:
  """The connection drops after S3 answered 200: botocore does not retry it."""
  left = {"n": 1}

  class _Broken:
    def read(self, *args, **kwargs):
      raise ProtocolError("Connection broken: IncompleteRead")

  def after_call(http_response, parsed, **kwargs):
    if left["n"] and http_response.url.endswith(key_suffix):
      left["n"] -= 1
      parsed["Body"] = _Broken()

  s3.s3_client.meta.events.register("after-call.s3.GetObject", after_call)


@pytest.mark.unit
@pytest.mark.asyncio
@pytest.mark.parametrize(
  ("accession", "broken", "phrase"),
  [
    (INLINE, f"{INLINE}/manifest.json", DOC_PHRASE),
    (INLINE, f"{INLINE}/holon.jsonld", DOC_PHRASE),
    (INLINE, f"{INLINE}/{DOC}", DOC_PHRASE),
    (CLASSIC, f"{CLASSIC}/{FRAGMENT}", FRAGMENT_PHRASE),
  ],
  ids=["manifest", "holon", "document", "fragment"],
)
async def test_a_broken_read_is_retried_not_cached_as_the_filing(
  bucket, accession, broken, phrase
):
  ref = await module.resolve_filing("sec", ticker=TICKER, accession=accession)
  _break_body_once(bucket, broken)

  with pytest.raises(PublicStorageError):
    await module.query_search_text("sec", ref, phrase)

  found = await module.query_search_text("sec", ref, phrase)
  assert found["total"] >= 1
