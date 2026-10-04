"""Current reports (8-K): which filings to keep, and which of their files.

An earnings release is not XBRL. An 8-K's XBRL is its cover page; the release
is an exhibit (EX-99.1) filed as plain HTML beside it. EDGAR's ``-xbrl.zip``
for an inline 8-K carries those exhibits, so one request brings the whole
filing, and EFTS names a filing's items before anything is fetched. Nothing
here touches EDGAR or S3: the pipeline brings the bytes.
"""

from __future__ import annotations

import html
import io
import re
import zipfile
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from xbrlkit.model import EntityIdentity, FilingMeta, XbrlModel
from xbrlkit.serve import TextSection, build_text, join_texts

CURRENT_REPORT_FORM = "8-K"
# Results of operations (the release itself) and Reg FD (where some filers
# put it instead).
DEFAULT_ITEMS = ("2.02", "7.01")
# The section an 8-K's own document reads as; every other section is an exhibit.
FORM_SECTION_ID = "form_8k"
# An exhibit is kept unless it names itself as something other than EX-99:
# the release and the slides are 99.x, a credit agreement is 10.x.
RELEASE_EXHIBIT = "99"

MANIFEST_VERSION = 1
PROCESSOR_VERSION = "current-reports/1"

_DOCUMENT_SUFFIXES = (".htm", ".html", ".txt")
_HEAD_SCAN_BYTES = 20_000
_HEAD_CHARS = 400
_EXHIBIT_HEAD_RE = re.compile(r"\bexhibit\s+(\d{1,3})(?:\.(\d{1,3}))?\b", re.IGNORECASE)
# Filer agents name exhibits freely: ex99-1, ex99_1, ex991, xex991,
# exhibit991. The digits after "ex" are the exhibit number run together. An
# "ex" inside a word (index1, annex1) is not one; one glued to a form code
# (a8-kex991, 20251028xex991) is, so only two letters before it disqualify.
_EXHIBIT_NAME_RE = re.compile(
  r"(?<![a-z][a-z])ex(?:hibit)?[-_ ]?(\d{1,4})(?:[-_.](\d{1,2}))?"
)
# Item 601 exhibit numbers with two digits, so a run like "1045" reads 10.45
# and not 1.045.
_TWO_DIGIT_EXHIBITS = frozenset(
  {"10", "13", "14", "15", "16", "17", "19", "21", "22", "23", "24", "25"}
  | {"31", "32", "33", "34", "35", "36", "95", "96", "97", "98", "99"}
)
_TAG_RE = re.compile(r"<[^>]+>")
_SPACE_RE = re.compile(r"\s+")
_DISPLAY_CIK_RE = re.compile(r"\s*\(CIK\s*\d+\)\s*$")
_DISPLAY_TICKERS_RE = re.compile(r"\s*\(([A-Z0-9.\-]+(?:,\s*[A-Z0-9.\-]+)*)\)\s*$")


@dataclass(frozen=True)
class CurrentReportHit:
  """One 8-K as EFTS describes it, before anything is fetched."""

  accession: str
  cik: str
  filing_date: str
  items: tuple[str, ...]
  primary_document: str | None = None
  entity_name: str | None = None
  ticker: str | None = None
  report_date: str | None = None
  # Every registrant on the filing: a combined 8-K (a utility holding company
  # with its operating subsidiaries) is one filing listed under each.
  ciks: tuple[str, ...] = ()

  @classmethod
  def from_efts(cls, hit: dict[str, Any]) -> CurrentReportHit | None:
    """Parse one ``hits.hits[]`` entry; None when it names no filing.

    The ``_id`` is ``accession:filename`` and the filename is the primary
    document. ``display_names`` reads ``Apple Inc.  (AAPL)  (CIK 0000320193)``.
    """
    source = hit.get("_source") or {}
    accession, _, filename = str(hit.get("_id") or "").partition(":")
    ciks = source.get("ciks") or []
    filing_date = source.get("file_date")
    if not accession or not ciks or not filing_date:
      return None
    name, ticker = _parse_display_name((source.get("display_names") or [None])[0])
    padded = tuple(dict.fromkeys(str(c).zfill(10) for c in ciks))
    return cls(
      accession=accession,
      cik=padded[0],
      ciks=padded,
      filing_date=str(filing_date),
      items=tuple(str(i) for i in source.get("items") or []),
      primary_document=filename or None,
      entity_name=name,
      ticker=ticker,
      report_date=source.get("period_ending") or None,
    )

  def wanted(self, items: tuple[str, ...] | list[str]) -> bool:
    return bool(set(self.items) & set(items))

  def registrants(self, corpus: set[str] | None = None) -> tuple[str, ...]:
    """The CIKs this filing is listed under: every registrant, or those the
    corpus holds."""
    ciks = self.ciks or (self.cik,)
    return ciks if corpus is None else tuple(c for c in ciks if c in corpus)


def _parse_display_name(display: str | None) -> tuple[str | None, str | None]:
  if not display:
    return None, None
  text = _DISPLAY_CIK_RE.sub("", display.strip())
  ticker = None
  if match := _DISPLAY_TICKERS_RE.search(text):
    ticker = match.group(1).split(",")[0].strip() or None
    text = text[: match.start()]
  return (text.strip() or None), ticker


@dataclass
class FiledDocument:
  """One file of a filing worth keeping: the 8-K itself or a kept exhibit."""

  name: str
  kind: str  # "document" | "exhibit"
  data: bytes
  exhibit: str | None = None

  @property
  def label(self) -> str:
    if self.kind == "document":
      return "Form 8-K"
    return self.exhibit or f"Exhibit ({self.name})"

  @property
  def section_id(self) -> str:
    if self.kind == "document":
      return FORM_SECTION_ID
    if self.exhibit:
      return self.exhibit.lower().replace("-", "_").replace(".", "_")
    stem = re.sub(r"[^a-z0-9]+", "_", self.name.lower().rsplit(".", 1)[0])
    return f"exhibit_{stem.strip('_')}"


def exhibit_number(digits: str, sub: str | None = None) -> str:
  """``EX-99.1`` from the digits a name or heading carries (``991`` or
  ``99`` + ``1``)."""
  if len(digits) > 2:
    major = digits[:2] if digits[:2] in _TWO_DIGIT_EXHIBITS else digits[:1]
    sub = digits[len(major) :] or sub
    digits = major
  return f"EX-{digits}" + (f".{sub}" if sub else "")


_ROW_RE = re.compile(r"<tr\b.*?</tr>", re.IGNORECASE | re.DOTALL)
_HREF_RE = re.compile(r"""href\s*=\s*["']([^"'#?]+)""", re.IGNORECASE)
_ROW_EXHIBIT_RE = re.compile(
  r"[\W_]*(?:exhibit\s*(?:no\.?)?\s*)?(\d{1,3})(?:\.(\d{1,3}))?(?![\d.])",
  re.IGNORECASE,
)


def exhibit_index(primary: bytes) -> dict[str, str]:
  """File name → exhibit, read off the 8-K's own exhibit index: Item 9.01
  lists each exhibit's number in the row that links its file."""
  index: dict[str, str] = {}
  html_text = primary.decode("utf-8", errors="replace")
  for row in _ROW_RE.findall(html_text):
    hrefs = _HREF_RE.findall(row)
    if not hrefs:
      continue
    text = html.unescape(_TAG_RE.sub(" ", row))
    if match := _ROW_EXHIBIT_RE.match(_SPACE_RE.sub(" ", text).strip()):
      for href in hrefs:
        index.setdefault(
          href.rsplit("/", 1)[-1],
          f"EX-{match.group(1)}" + (f".{match.group(2)}" if match.group(2) else ""),
        )
  return index


def exhibit_type(name: str, head: str, indexed: str | None = None) -> str | None:
  """The exhibit a file is: as the 8-K's exhibit index lists it, else as its
  heading says ("Exhibit 99.1"), else as its file name reads; None when
  nothing says."""
  if indexed:
    return indexed
  if match := _EXHIBIT_HEAD_RE.search(head[:_HEAD_CHARS]):
    return exhibit_number(match.group(1), match.group(2))
  base = name.rsplit("/", 1)[-1].lower()
  if match := _EXHIBIT_NAME_RE.search(base):
    return exhibit_number(match.group(1), match.group(2))
  return None


def keeps_exhibit(exhibit: str | None) -> bool:
  return exhibit is None or exhibit.startswith(f"EX-{RELEASE_EXHIBIT}")


def _head_text(data: bytes) -> str:
  raw = data[:_HEAD_SCAN_BYTES].decode("utf-8", errors="replace")
  raw = re.sub(r"<title>.*?</title>", " ", raw, flags=re.IGNORECASE | re.DOTALL)
  text = html.unescape(_TAG_RE.sub(" ", raw))
  return _SPACE_RE.sub(" ", text).strip()


def _is_cover(data: bytes) -> bool:
  return b"dei:DocumentType" in data[:2_000_000]


def documents_in_zip(
  zip_bytes: bytes, primary_document: str | None = None
) -> list[FiledDocument]:
  """The 8-K and its kept exhibits, the 8-K first, exhibits in name order.

  The primary is the file EFTS named, else the one carrying the cover page's
  ``dei:DocumentType``. Linkbases, schemas, images and viewer files are not
  documents. An exhibit the 8-K's index, its heading or its name places
  outside EX-99 (a credit agreement, a purchase deed) is dropped.
  """
  documents: list[FiledDocument] = []
  with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
    members = [
      info.filename
      for info in zf.infolist()
      if info.filename.lower().endswith(_DOCUMENT_SUFFIXES)
      and not re.match(r"^r\d+\.htm", info.filename.rsplit("/", 1)[-1].lower())
    ]
    files = {name: zf.read(name) for name in members}

  primary = None
  if primary_document:
    primary = next((n for n in files if n.rsplit("/", 1)[-1] == primary_document), None)
  if primary is None:
    primary = next((n for n, data in files.items() if _is_cover(data)), None)

  index = exhibit_index(files[primary]) if primary else {}
  for name in sorted(files):
    data = files[name]
    base = name.rsplit("/", 1)[-1]
    if name == primary:
      documents.append(FiledDocument(base, "document", data))
      continue
    exhibit = exhibit_type(base, _head_text(data), index.get(base))
    if keeps_exhibit(exhibit):
      documents.append(FiledDocument(base, "exhibit", data, exhibit))
  documents.sort(key=lambda d: (d.kind != "document", d.exhibit or "~", d.name))
  return documents


_BARE_MODEL = XbrlModel(
  filing=FilingMeta(accession="", cik=""), entity=EntityIdentity(cik="")
)


def document_text(data: bytes) -> str:
  """A filed document as plain text, tables as markdown pipes — xbrlkit's
  rendering, so the index and the text tools read the same words."""
  text, _sections = build_text(_BARE_MODEL, data.decode("utf-8", errors="replace"))
  return text


@dataclass
class CurrentReportText:
  text: str
  sections: list[TextSection] = field(default_factory=list)


def current_report_text(documents: list[FiledDocument]) -> CurrentReportText:
  """The filing read as one text: the 8-K, then each exhibit under its own
  heading, every document a section with a known offset — xbrlkit's assembly,
  so an 8-K reads the same here as loaded into xbrlkit."""
  text, sections = join_texts(
    [(d.section_id, d.label, document_text(d.data), []) for d in documents]
  )
  return CurrentReportText(text, sections)


def current_report_manifest(
  hit: CurrentReportHit,
  representations: list[dict[str, Any]],
  errors: list[str],
  folder_url: str,
) -> dict[str, Any]:
  """The filing's ``manifest.json``: the same shape the processor writes for
  an XBRL report, with the 8-K's items and no report id (it is not in the
  graph)."""
  return {
    "version": MANIFEST_VERSION,
    "accession": hit.accession,
    "cik": hit.cik,
    "form": CURRENT_REPORT_FORM,
    "filing_date": hit.filing_date,
    "report_date": hit.report_date,
    "items": list(hit.items),
    "primary_document": hit.primary_document,
    "is_inline_xbrl": True,
    "entity": {"cik": hit.cik, "name": hit.entity_name, "ticker": hit.ticker},
    "report_id": None,
    "folder": folder_url,
    "representations": representations,
    "errors": errors,
    "processor_version": PROCESSOR_VERSION,
    "written_at": datetime.now(UTC).isoformat(timespec="seconds"),
  }


def missing_document(manifest: dict[str, Any] | None) -> str | None:
  """The primary document a published filing lacks, or None.

  A filing from before inline XBRL was processed from an instance-only zip,
  so its folder holds the holon and the Tavi but not the document they were
  tagged from; the manifest still names it. A manifest that names the XBRL
  instance itself (a filing whose submissions record had no primary
  document) names nothing worth reading.
  """
  if not manifest:
    return None
  name = manifest.get("primary_document")
  if not name or manifest.get("is_inline_xbrl"):
    return None
  if not str(name).lower().endswith(_DOCUMENT_SUFFIXES):
    return None
  kinds = {r.get("kind") for r in manifest.get("representations") or []}
  return None if "document" in kinds else str(name)


def release_entry(
  hit: CurrentReportHit,
  bucket: str,
  cdn_url: str | None,
  representations: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
  """One 8-K as a filer's releases list carries it: when, what it reports,
  where its folder is, and the files in it (``document``, ``exhibits`` by
  exhibit number), so a reader can link them without opening the manifest."""
  from robosystems.config.storage.shared import (
    get_filing_artifact_prefix,
    get_public_data_url,
  )

  prefix = get_filing_artifact_prefix(hit.filing_date[:4], hit.cik, hit.accession)
  entry: dict[str, Any] = {
    "accession": hit.accession,
    "filing_date": hit.filing_date,
    "report_date": hit.report_date,
    "items": list(hit.items),
    "folder": get_public_data_url(bucket, prefix + "/", cdn_url),
  }
  if representations is not None:
    document = next(
      (
        r["name"]
        for r in representations
        if r.get("kind") == "document" and r.get("name")
      ),
      None,
    )
    if document:
      entry["document"] = document
    exhibits: dict[str, str] = {}
    for r in representations:
      if r.get("kind") == "exhibit" and r.get("name"):
        # Two files under one number keep the first; a file with no number
        # is keyed by its name, unique in the folder.
        exhibits.setdefault(str(r.get("exhibit") or r["name"]), r["name"])
    if exhibits:
      entry["exhibits"] = exhibits
  return entry


def merge_releases(
  existing: list[dict[str, Any]],
  hits: list[tuple[CurrentReportHit, list[dict[str, Any]] | None]],
  bucket: str,
  cdn_url: str | None,
) -> list[dict[str, Any]]:
  """A filer's releases list with ``hits`` folded in, one entry per
  accession, newest first. A hit given without its representations keeps the
  file names its entry already carries."""
  merged = {entry["accession"]: entry for entry in existing if entry.get("accession")}
  for hit, representations in hits:
    entry = release_entry(hit, bucket, cdn_url, representations)
    previous = merged.get(hit.accession) or {}
    for key in ("document", "exhibits"):
      if key not in entry and key in previous:
        entry[key] = previous[key]
    merged[hit.accession] = entry
  return sorted(
    merged.values(),
    key=lambda e: (e.get("filing_date") or "", e["accession"]),
    reverse=True,
  )
