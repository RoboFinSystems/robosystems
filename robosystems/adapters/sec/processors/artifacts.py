"""Per-filing public artifacts: the holon, the Tavi model, the primary document.

Written by the processor, at process time, from the xbrlkit model it already
holds — no graph, no second parse. One folder per filing in the public-data
bucket, the same key family as the externalized text blocks, so a filing's
portable representations sit beside the note text they reference:

    {year}/{cik}/{accession}/holon.jsonld     the dataset-form JSON-LD holon
    {year}/{cik}/{accession}/tavi.json        the Project Tavi compiled model
    {year}/{cik}/{accession}/tavi.gaps.json   what the draft could not hold
    {year}/{cik}/{accession}/{primary doc}    the filing as filed
    {year}/{cik}/{accession}/manifest.json    what was written, with sizes

The holon is xbrlkit's projection of the parse with one platform touch: a
text-block fact the externalizer moved to the CDN carries that URL as its
value, the way the graph's Fact row does, so the holon stays small and a
renderer embeds the note from the CDN. The Tavi carries the text blocks
inline — the draft has no external-value construct — and its gaps sidecar
records what the filing carries that the draft has nowhere to put.

Every write is a whole object, and the emitters are deterministic, so a
reprocessed filing rewrites the same bytes unless the emitter moved (a new
Tavi draft, a holon vocabulary change) — which is exactly when a rewrite is
wanted. A failure here never fails the filing: the parquet is the product,
the artifacts are a projection of it, and the catalog lists a filing by its
manifest, so a filing whose artifacts failed is absent from the pages until
it is reprocessed.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from arelle.UrlUtil import IXDS_DOC_SEPARATOR, IXDS_SURROGATE
from xbrlkit.model import XbrlModel
from xbrlkit.serialize import to_holon, to_tavi_report
from xbrlkit.serialize.lpg import graph_id

from robosystems.config.storage.shared import (
  FILING_ARTIFACT_HOLON,
  FILING_ARTIFACT_MANIFEST,
  FILING_ARTIFACT_TAVI,
  FILING_ARTIFACT_TAVI_GAPS,
  get_filing_artifact_key,
  get_filing_artifact_prefix,
  get_public_data_url,
)
from robosystems.logger import logger

MANIFEST_VERSION = 1

# An artifact changes only when its filing is reprocessed, so a day at the
# edge is safe. The manifest is what a catalog rebuild reads, so it turns
# over faster.
ARTIFACT_CACHE_CONTROL = "public, max-age=86400"
MANIFEST_CACHE_CONTROL = "public, max-age=300"

HOLON_MEDIA_TYPE = "application/ld+json"
JSON_MEDIA_TYPE = "application/json"
DOCUMENT_MEDIA_TYPES = {
  ".htm": "text/html",
  ".html": "text/html",
  ".xml": "application/xml",
  ".txt": "text/plain",
  ".pdf": "application/pdf",
}


@dataclass
class Representation:
  """One public form of the filing, as the manifest and the catalog list it."""

  kind: str
  name: str
  media_type: str
  bytes: int
  url: str
  extra: dict[str, Any] = field(default_factory=dict)

  def to_dict(self) -> dict[str, Any]:
    return {
      "kind": self.kind,
      "name": self.name,
      "media_type": self.media_type,
      "bytes": self.bytes,
      "url": self.url,
      **self.extra,
    }


@dataclass
class FilingArtifactResult:
  prefix: str
  representations: list[Representation]
  manifest_key: str | None
  errors: list[str]


def filing_coordinates(model: XbrlModel) -> tuple[str, str, str] | None:
  """``(year, cik, accession)`` naming the filing's folder, or None when incomplete.

  The year is the filing year, the same the text-block keys use.
  """
  filing = model.filing
  if not filing.accession or not filing.cik or filing.filing_date is None:
    return None
  return str(filing.filing_date.year), filing.cik, filing.accession


def with_external_values(
  model: XbrlModel, external_values: dict[str, str]
) -> XbrlModel:
  """The model with each externalized fact's value replaced by its CDN URL.

  ``external_values`` is keyed by the graph's Fact identifier — derived from
  the fact the same way the projection derives it — and holds the URL the
  externalizer stored on the Fact row. A holon built from the result carries
  the URL as the fact's string value, which is what the graph carries and
  what a renderer embeds.
  """
  if not external_values:
    return model
  report_uri = model.filing.report_uri or model.filing.accession
  facts = []
  for fact in model.facts:
    fact_id = graph_id("fact", f"{report_uri}#fact-{fact.source_hash or fact.id}")
    url = external_values.get(fact_id)
    if url is None:
      facts.append(fact)
    else:
      facts.append(fact.model_copy(update={"value_str": url, "raw_value": url}))
  return model.model_copy(update={"facts": facts})


def primary_document_path(
  instance_path: str | None, primary_document: str | None
) -> str | None:
  """The filed primary document on disk, or None when it is not there.

  For an inline filing the instance *is* the primary document. A
  multi-document inline set arrives as the surrogate path the loader built,
  and the primary is the member EDGAR names. A non-inline filing's primary
  document is a separate file the XBRL zip may not carry.
  """
  if not instance_path or not primary_document:
    return None
  if IXDS_DOC_SEPARATOR in instance_path:
    members = instance_path.partition(IXDS_SURROGATE)[2].split(IXDS_DOC_SEPARATOR)
  else:
    members = [instance_path]
  for member in members:
    if os.path.basename(member) == primary_document and os.path.isfile(member):
      return member
  sibling = os.path.join(os.path.dirname(members[0]), primary_document)
  return sibling if os.path.isfile(sibling) else None


def _iso(value: Any) -> str | None:
  return None if value is None else value.isoformat()


class FilingArtifactWriter:
  """Writes one filing's public artifacts and the manifest naming them."""

  def __init__(
    self,
    s3_client: Any,
    bucket: str | None,
    cdn_url: str | None,
    enabled: bool = True,
  ):
    self.s3_client = s3_client
    self.bucket = bucket
    self.cdn_url = cdn_url
    self._enabled = enabled

  @property
  def enabled(self) -> bool:
    return bool(self._enabled and self.s3_client and self.bucket)

  def write(
    self,
    model: XbrlModel,
    *,
    report_id: str | None,
    instance_path: str | None,
    external_values: dict[str, str],
    processor_version: str,
  ) -> FilingArtifactResult | None:
    """Write the artifacts and the manifest; None when disabled or unplaceable."""
    if not self.enabled:
      return None
    coordinates = filing_coordinates(model)
    if coordinates is None:
      logger.warning(
        "Filing artifacts skipped: the filing has no accession, CIK or filing date"
      )
      return None
    year, cik, accession = coordinates
    prefix = get_filing_artifact_prefix(year, cik, accession)
    representations: list[Representation] = []
    errors: list[str] = []

    self._write_tavi(model, coordinates, representations, errors)
    self._write_holon(model, external_values, coordinates, representations, errors)
    self._write_document(model, instance_path, coordinates, representations, errors)
    manifest_key = self._write_manifest(
      model,
      coordinates,
      representations,
      errors,
      report_id=report_id,
      processor_version=processor_version,
    )
    return FilingArtifactResult(prefix, representations, manifest_key, errors)

  # ── the artifacts ─────────────────────────────────────────────────────────

  def _write_tavi(
    self,
    model: XbrlModel,
    coordinates: tuple[str, str, str],
    representations: list[Representation],
    errors: list[str],
  ) -> None:
    try:
      document, gaps = to_tavi_report(model)
      text = json.dumps(document, separators=(",", ":"), default=str)
      key = get_filing_artifact_key(*coordinates, FILING_ARTIFACT_TAVI)
      size = self._put_text(text, key, JSON_MEDIA_TYPE)
      if size is None:
        errors.append("tavi: upload failed")
        return
      extra: dict[str, Any] = {
        "spec": str(document.get("documentInfo", {}).get("documentType", "")),
      }
      gaps_key = get_filing_artifact_key(*coordinates, FILING_ARTIFACT_TAVI_GAPS)
      gaps_text = json.dumps(gaps.to_dict(), indent=2, default=str)
      if self._put_text(gaps_text, gaps_key, JSON_MEDIA_TYPE) is not None:
        extra["gaps"] = self._url(gaps_key)
      else:
        errors.append("tavi: gaps sidecar upload failed")
      representations.append(
        Representation(
          "tavi", FILING_ARTIFACT_TAVI, JSON_MEDIA_TYPE, size, self._url(key), extra
        )
      )
    except Exception as e:
      errors.append(f"tavi: {e}")
      logger.warning(f"Tavi projection failed for {coordinates[2]}: {e}")

  def _write_holon(
    self,
    model: XbrlModel,
    external_values: dict[str, str],
    coordinates: tuple[str, str, str],
    representations: list[Representation],
    errors: list[str],
  ) -> None:
    try:
      text = to_holon(with_external_values(model, external_values))
      key = get_filing_artifact_key(*coordinates, FILING_ARTIFACT_HOLON)
      size = self._put_text(text, key, HOLON_MEDIA_TYPE)
      if size is None:
        errors.append("holon: upload failed")
        return
      representations.append(
        Representation(
          "holon", FILING_ARTIFACT_HOLON, HOLON_MEDIA_TYPE, size, self._url(key)
        )
      )
    except Exception as e:
      errors.append(f"holon: {e}")
      logger.warning(f"Holon projection failed for {coordinates[2]}: {e}")

  def _write_document(
    self,
    model: XbrlModel,
    instance_path: str | None,
    coordinates: tuple[str, str, str],
    representations: list[Representation],
    errors: list[str],
  ) -> None:
    name = model.filing.primary_document
    path = primary_document_path(instance_path, name)
    if path is None or name is None:
      return
    try:
      media_type = DOCUMENT_MEDIA_TYPES.get(
        os.path.splitext(name)[1].lower(), "application/octet-stream"
      )
      key = get_filing_artifact_key(*coordinates, name)
      ok = self.s3_client.upload_file(
        path,
        self.bucket,
        key,
        content_type=media_type,
        cache_control=ARTIFACT_CACHE_CONTROL,
      )
      if not ok:
        errors.append("document: upload failed")
        return
      representations.append(
        Representation(
          "document", name, media_type, os.path.getsize(path), self._url(key)
        )
      )
    except Exception as e:
      errors.append(f"document: {e}")
      logger.warning(f"Primary document copy failed for {coordinates[2]}: {e}")

  def _write_manifest(
    self,
    model: XbrlModel,
    coordinates: tuple[str, str, str],
    representations: list[Representation],
    errors: list[str],
    *,
    report_id: str | None,
    processor_version: str,
  ) -> str | None:
    filing = model.filing
    year, cik, accession = coordinates
    manifest = {
      "version": MANIFEST_VERSION,
      "accession": accession,
      "cik": cik,
      "form": filing.form,
      "filing_date": _iso(filing.filing_date),
      "report_date": _iso(filing.report_date),
      "fiscal_year": filing.fiscal_year_focus,
      "fiscal_period": filing.fiscal_period_focus,
      "primary_document": filing.primary_document,
      "is_inline_xbrl": filing.is_inline_xbrl,
      "entity": {
        "cik": model.entity.cik,
        "name": model.entity.name,
        "ticker": model.entity.ticker,
      },
      "report_id": report_id,
      "folder": self._url(get_filing_artifact_prefix(year, cik, accession) + "/"),
      "representations": [r.to_dict() for r in representations],
      "errors": errors,
      "processor_version": processor_version,
      "written_at": datetime.now(UTC).isoformat(timespec="seconds"),
    }
    key = get_filing_artifact_key(*coordinates, FILING_ARTIFACT_MANIFEST)
    text = json.dumps(manifest, indent=2, default=str)
    if self._put_text(text, key, JSON_MEDIA_TYPE, MANIFEST_CACHE_CONTROL) is None:
      errors.append("manifest: upload failed")
      return None
    return key

  # ── transport ─────────────────────────────────────────────────────────────

  def _put_text(
    self,
    text: str,
    key: str,
    media_type: str,
    cache_control: str = ARTIFACT_CACHE_CONTROL,
  ) -> int | None:
    """Upload ``text`` and return its byte size, or None when the upload failed."""
    data = text.encode("utf-8")
    ok = self.s3_client.upload_bytes(
      data, self.bucket, key, content_type=media_type, cache_control=cache_control
    )
    return len(data) if ok else None

  def _url(self, key: str) -> str:
    assert self.bucket is not None
    return get_public_data_url(self.bucket, key, self.cdn_url)
