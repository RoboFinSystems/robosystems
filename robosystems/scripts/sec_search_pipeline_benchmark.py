"""Benchmark search pipeline — narrative extractor + iXBRL parser on prod S3 filings.

Runs both extraction methods on the same filings to give a full picture of what
the search pipeline captures. Reports per-filing and aggregate statistics.

Usage:
    uv run python -m robosystems.scripts.sec_search_pipeline_benchmark
    uv run python -m robosystems.scripts.sec_search_pipeline_benchmark --years 2024,2025
    uv run python -m robosystems.scripts.sec_search_pipeline_benchmark --tickers AAPL,MSFT --dump-failures
    uv run python -m robosystems.scripts.sec_search_pipeline_benchmark --form-types 10-K --max-per-ticker 2
    uv run python -m robosystems.scripts.sec_search_pipeline_benchmark --expanded  # 40+ tickers
"""

import argparse
import io
import os
import random
import re
import sys
import time
import zipfile
from collections import Counter
from dataclasses import dataclass, field

import boto3

from robosystems.adapters.sec.ixbrl_parser import iXBRLParser
from robosystems.adapters.sec.narrative_extractor import NarrativeExtractor
from robosystems.config.storage.shared import DataSourceType, get_raw_key


def _resolve_bucket() -> str:
  """Resolve the shared-raw S3 bucket name from env or AWS account."""
  bucket = os.environ.get("SHARED_RAW_BUCKET")
  if bucket:
    return bucket
  sts = boto3.client("sts")
  account_id = sts.get_caller_identity()["Account"]
  return f"robosystems-{account_id}-shared-raw-prod"


# ── Ticker → CIK mappings ──────────────────────────────────────────────────

# Core set: 20 diverse filers (same as test_extractor.py)
CORE_TICKERS = {
  # Large-cap tech
  "NVDA": "1045810",
  "AAPL": "320193",
  "MSFT": "789019",
  "GOOGL": "1652044",
  # Large-cap non-tech
  "JPM": "19617",
  "JNJ": "200406",
  "XOM": "34088",
  "WMT": "104169",
  # Mid-cap / cybersecurity & cloud
  "CRWD": "1535527",
  "ZS": "1713683",
  "SNOW": "1640147",
  "DDOG": "1561550",
  # Small-cap (often messy HTML)
  "SMCI": "1375365",
  "CAVA": "1639438",
  # Foreign filers (20-F format)
  "TSM": "1046179",
  "ASML": "937966",
  # Financial services (complex disclosures)
  "GS": "886982",
  "BLK": "1364742",
  # Healthcare / biotech
  "LLY": "59478",
  "MRNA": "1682852",
}

# Expanded set: additional tickers for broader coverage
EXPANDED_TICKERS = {
  # Mega-cap / FAANG
  "AMZN": "1018724",
  "META": "1326801",
  "TSLA": "1318605",
  "NFLX": "1065280",
  # Industrials / defense
  "BA": "12927",
  "LMT": "936468",
  "CAT": "18230",
  "GE": "40554",
  # Consumer / retail
  "COST": "909832",
  "TGT": "27419",
  "NKE": "320187",
  "SBUX": "829224",
  # Energy
  "CVX": "93410",
  "COP": "1163165",
  # REITs (different disclosure style)
  "AMT": "1053507",
  "PLD": "1045609",
  # Insurance (complex financials)
  "BRK-B": "1067983",
  "AIG": "5272",
  # Semiconductors
  "AMD": "2488",
  "INTC": "50863",
  "AVGO": "1649338",
  # Biotech / pharma
  "ABBV": "1551152",
  "PFE": "78003",
  "GILD": "882095",
  # Telecom
  "T": "732717",
  "VZ": "732712",
}


@dataclass
class FilingResult:
  """Results from running both extractors on a single filing."""

  ticker: str
  cik: str
  accession: str
  form_type: str
  zip_key: str
  html_file: str = ""
  html_size: int = 0
  # Narrative extractor results
  narrative_sections: list[str] = field(default_factory=list)
  narrative_word_counts: dict[str, int] = field(default_factory=dict)
  narrative_error: str | None = None
  # iXBRL parser results
  ixbrl_sections: list[str] = field(default_factory=list)
  ixbrl_element_counts: dict[str, int] = field(default_factory=dict)
  ixbrl_total_elements: int = 0
  ixbrl_unique_elements: list[str] = field(default_factory=list)
  ixbrl_error: str | None = None
  # Timing
  elapsed_ms: int = 0


def _extract_html_from_zip(zip_bytes: bytes) -> tuple[str | None, str]:
  """Extract the main filing HTML from a ZIP. Returns (html_content, filename).

  Prefers the file with dei:DocumentType iXBRL tag (the actual filing),
  falling back to largest non-exhibit HTM file.
  """
  with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
    htm_candidates: list[tuple[str, int]] = []
    for info in zf.infolist():
      name_lower = info.filename.lower()
      if not name_lower.endswith((".htm", ".html")):
        continue
      if any(
        skip in name_lower
        for skip in [
          "filingsummary",
          "metalinks",
          "defnref",
          "_cal.",
          "_def.",
          "_lab.",
          "_pre.",
          ".xsd",
        ]
      ):
        continue
      basename = name_lower.rsplit("/", 1)[-1]
      if re.match(r"^r\d+\.htm", basename):
        continue
      htm_candidates.append((info.filename, info.file_size))

    if not htm_candidates:
      return None, ""

    # Strategy 1: Find the file with dei:DocumentType iXBRL tag
    for filename, _ in sorted(htm_candidates, key=lambda x: -x[1]):
      content = zf.read(filename).decode("utf-8", errors="replace")
      if _extract_ixbrl_doc_type(content) is not None:
        return content, filename

    # Strategy 2: Fall back to largest non-exhibit file
    fallback = [
      (f, s)
      for f, s in htm_candidates
      if not any(
        pat in f.lower().rsplit("/", 1)[-1]
        for pat in ["ex1", "ex2", "ex3", "ex4", "consent", "subsidiar"]
      )
    ]
    candidates = fallback or htm_candidates
    main_file = max(candidates, key=lambda x: x[1])[0]
    html_content = zf.read(main_file).decode("utf-8", errors="replace")
    return html_content, main_file


def _extract_ixbrl_doc_type(html: str) -> str | None:
  """Extract dei:DocumentType from iXBRL header."""
  match = re.search(
    r'name=["\']dei:DocumentType["\'][^>]*>(.*?)</ix:non',
    html[:2_000_000],
    re.IGNORECASE | re.DOTALL,
  )
  if match:
    value = re.sub(r"<[^>]+>", "", match.group(1)).strip()
    if value:
      return value
  return None


def _detect_form_type(html: str) -> str:
  """Detect form type from iXBRL tag, falling back to regex."""
  ixbrl_type = _extract_ixbrl_doc_type(html)
  if ixbrl_type:
    return ixbrl_type
  if re.search(r"FORM\s+10-Q", html[:5000], re.IGNORECASE):
    return "10-Q"
  if re.search(r"FORM\s+10-K", html[:5000], re.IGNORECASE):
    return "10-K"
  if re.search(r"FORM\s+20-F", html[:5000], re.IGNORECASE):
    return "20-F"
  return "UNKNOWN"


def _discover_ciks(s3, bucket: str, year: int) -> list[str]:
  """Discover all CIK directories in S3 for a given year."""
  prefix = f"sec/year={year}/"
  ciks = []
  paginator = s3.get_paginator("list_objects_v2")
  for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
    for cp in page.get("CommonPrefixes", []):
      cik = cp["Prefix"].replace(prefix, "").rstrip("/")
      if cik and cik.isdigit():
        ciks.append(cik)
  return ciks


def _list_filings(s3, bucket: str, cik: str, year: int) -> list[str]:
  """List ZIP files for a CIK in a given year."""
  padded_cik = cik.zfill(10)
  prefix = get_raw_key(DataSourceType.SEC, f"year={year}", padded_cik)
  keys = []
  paginator = s3.get_paginator("list_objects_v2")
  for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
    for obj in page.get("Contents", []):
      if obj["Key"].endswith(".zip"):
        keys.append(obj["Key"])
  return keys


def test_filing(
  s3,
  bucket: str,
  zip_key: str,
  ticker: str,
  cik: str,
  narrative_extractor: NarrativeExtractor,
  ixbrl_parser: iXBRLParser,
  form_types: list[str],
) -> FilingResult:
  """Run both extractors on a single filing."""
  accession = zip_key.rsplit("/", 1)[-1].replace(".zip", "")
  result = FilingResult(
    ticker=ticker, cik=cik, accession=accession, form_type="", zip_key=zip_key
  )

  start = time.monotonic()

  try:
    response = s3.get_object(Bucket=bucket, Key=zip_key)
    zip_bytes = response["Body"].read()

    html, html_file = _extract_html_from_zip(zip_bytes)
    if not html:
      result.narrative_error = "No HTML found in ZIP"
      result.ixbrl_error = "No HTML found in ZIP"
      result.elapsed_ms = int((time.monotonic() - start) * 1000)
      return result

    result.html_file = html_file
    result.html_size = len(html)

    form_type = _detect_form_type(html)
    result.form_type = form_type

    if form_types and form_type not in form_types:
      result.narrative_error = f"Skipped: form type {form_type}"
      result.ixbrl_error = f"Skipped: form type {form_type}"
      result.elapsed_ms = int((time.monotonic() - start) * 1000)
      return result

    # ── Run narrative extractor ──
    try:
      sections = narrative_extractor.extract(html, form_type)
      result.narrative_sections = [s.section_id for s in sections]
      result.narrative_word_counts = {s.section_id: s.word_count for s in sections}
    except Exception as e:
      result.narrative_error = str(e)

    # ── Run iXBRL parser ──
    try:
      disclosures = ixbrl_parser.parse(html)
      result.ixbrl_sections = [s.section_id for s in disclosures]
      result.ixbrl_element_counts = {s.section_id: s.element_count for s in disclosures}
      result.ixbrl_total_elements = sum(s.element_count for s in disclosures)
      # Collect all unique elements across all sections
      all_elements: set[str] = set()
      for s in disclosures:
        all_elements.update(s.xbrl_elements)
      result.ixbrl_unique_elements = sorted(all_elements)
    except Exception as e:
      result.ixbrl_error = str(e)

  except Exception as e:
    result.narrative_error = str(e)
    result.ixbrl_error = str(e)

  result.elapsed_ms = int((time.monotonic() - start) * 1000)
  return result


def _print_filing_result(result: FilingResult) -> None:
  """Print a single filing result line."""
  parts = [
    f"  {result.ticker:6s} {result.accession:30s} {result.form_type:8s} {result.elapsed_ms:5d}ms"
  ]

  # Narrative summary
  if result.narrative_error and result.narrative_error.startswith("Skipped"):
    parts.append("  NAR: skipped")
  elif result.narrative_error:
    parts.append(f"  NAR: ERROR {result.narrative_error[:60]}")
  elif result.narrative_sections:
    total_words = sum(result.narrative_word_counts.values())
    parts.append(f"  NAR: {len(result.narrative_sections)} sections, {total_words:,}w")
  else:
    parts.append("  NAR: 0 sections")

  # iXBRL summary
  if result.ixbrl_error and result.ixbrl_error.startswith("Skipped"):
    pass  # Already shown as skipped
  elif result.ixbrl_error:
    parts.append(f"  iXBRL: ERROR {result.ixbrl_error[:60]}")
  elif result.ixbrl_sections:
    parts.append(
      f"  iXBRL: {len(result.ixbrl_sections)} disclosures, "
      f"{result.ixbrl_total_elements} elements, "
      f"{len(result.ixbrl_unique_elements)} unique"
    )
  else:
    parts.append("  iXBRL: 0 disclosures")

  print("".join(parts))


def _print_summary(all_results: list[FilingResult]) -> None:
  """Print aggregate summary statistics."""
  tested = [
    r
    for r in all_results
    if not (r.narrative_error and r.narrative_error.startswith("Skipped"))
  ]

  if not tested:
    print("\nNo filings tested.")
    return

  # ── Narrative stats ──
  nar_ok = [r for r in tested if r.narrative_sections and not r.narrative_error]
  nar_errors = [
    r
    for r in tested
    if r.narrative_error and not r.narrative_error.startswith("Skipped")
  ]
  nar_empty = [r for r in tested if not r.narrative_sections and not r.narrative_error]

  print(f"\n{'=' * 100}")
  print(
    f"TESTED: {len(tested)} filings from {len({r.ticker for r in tested})} companies"
  )
  print(f"  Form types: {Counter(r.form_type for r in tested).most_common()}")

  print("\n── Narrative Extractor ──")
  print(
    f"  Extracted: {len(nar_ok)} ({len(nar_ok) / len(tested) * 100:.0f}%)  |  Empty: {len(nar_empty)}  |  Errors: {len(nar_errors)}"
  )

  if nar_ok:
    section_counts = Counter()
    total_words_by_section: dict[str, int] = {}
    for r in nar_ok:
      for sid in r.narrative_sections:
        section_counts[sid] += 1
      for sid, wc in r.narrative_word_counts.items():
        total_words_by_section[sid] = total_words_by_section.get(sid, 0) + wc

    total_10k = len([r for r in nar_ok if r.form_type in ("10-K", "20-F", "40-F")])
    total_10q = len([r for r in nar_ok if r.form_type == "10-Q"])
    print(f"  Coverage ({total_10k} 10-K/20-F, {total_10q} 10-Q):")
    for sid, count in section_counts.most_common():
      avg_words = total_words_by_section[sid] // count
      print(
        f"    {sid:12s}: {count:3d} filings ({count / len(nar_ok) * 100:4.0f}%)  avg {avg_words:,}w"
      )

  if nar_empty:
    print("\n  Filings with 0 narrative sections:")
    for r in nar_empty:
      print(
        f"    {r.ticker:6s} {r.accession:30s} {r.form_type:8s} html={r.html_size:,}b"
      )

  # ── iXBRL stats ──
  ixbrl_ok = [r for r in tested if r.ixbrl_sections and not r.ixbrl_error]
  ixbrl_errors = [
    r for r in tested if r.ixbrl_error and not r.ixbrl_error.startswith("Skipped")
  ]
  ixbrl_empty = [r for r in tested if not r.ixbrl_sections and not r.ixbrl_error]

  print("\n── iXBRL Parser ──")
  print(
    f"  Extracted: {len(ixbrl_ok)} ({len(ixbrl_ok) / len(tested) * 100:.0f}%)  |  Empty: {len(ixbrl_empty)}  |  Errors: {len(ixbrl_errors)}"
  )

  if ixbrl_ok:
    # Disclosure frequency
    disclosure_counts = Counter()
    for r in ixbrl_ok:
      for sid in r.ixbrl_sections:
        disclosure_counts[sid] += 1

    print(f"  Top 20 disclosures (of {len(disclosure_counts)} unique):")
    for sid, count in disclosure_counts.most_common(20):
      print(f"    {sid:55s}: {count:3d} filings ({count / len(ixbrl_ok) * 100:4.0f}%)")

    # Element stats
    all_elements: set[str] = set()
    total_elements = 0
    for r in ixbrl_ok:
      all_elements.update(r.ixbrl_unique_elements)
      total_elements += r.ixbrl_total_elements

    avg_disclosures = sum(len(r.ixbrl_sections) for r in ixbrl_ok) / len(ixbrl_ok)
    avg_elements = total_elements / len(ixbrl_ok)
    print("\n  Element stats:")
    print(f"    Avg disclosures/filing: {avg_disclosures:.1f}")
    print(f"    Avg elements/filing:    {avg_elements:.1f}")
    print(f"    Unique elements across all filings: {len(all_elements)}")

    # Top elements across all filings
    element_freq = Counter()
    for r in ixbrl_ok:
      element_freq.update(r.ixbrl_unique_elements)
    print("    Top 15 elements:")
    for elem, count in element_freq.most_common(15):
      print(f"      {elem:50s}: {count:3d} filings")

  if ixbrl_empty:
    print("\n  Filings with 0 iXBRL disclosures:")
    for r in ixbrl_empty:
      print(
        f"    {r.ticker:6s} {r.accession:30s} {r.form_type:8s} html={r.html_size:,}b"
      )

  # ── Cross-comparison ──
  both_ok = [r for r in tested if r.narrative_sections and r.ixbrl_sections]
  nar_only = [
    r
    for r in tested
    if r.narrative_sections and not r.ixbrl_sections and not r.ixbrl_error
  ]
  ixbrl_only = [
    r
    for r in tested
    if r.ixbrl_sections and not r.narrative_sections and not r.narrative_error
  ]
  neither = [
    r
    for r in tested
    if not r.narrative_sections
    and not r.ixbrl_sections
    and not r.narrative_error
    and not r.ixbrl_error
  ]

  print("\n── Coverage Overlap ──")
  print(
    f"  Both extractors:     {len(both_ok):3d} ({len(both_ok) / len(tested) * 100:.0f}%)"
  )
  print(
    f"  Narrative only:      {len(nar_only):3d} ({len(nar_only) / len(tested) * 100:.0f}%)"
  )
  print(
    f"  iXBRL only:          {len(ixbrl_only):3d} ({len(ixbrl_only) / len(tested) * 100:.0f}%)"
  )
  print(
    f"  Neither:             {len(neither):3d} ({len(neither) / len(tested) * 100:.0f}%)"
  )

  # ── Timing ──
  avg_ms = sum(r.elapsed_ms for r in tested) / len(tested)
  max_ms = max(r.elapsed_ms for r in tested)
  total_s = sum(r.elapsed_ms for r in tested) / 1000
  print("\n── Timing ──")
  print(f"  Avg: {avg_ms:.0f}ms  |  Max: {max_ms}ms  |  Total: {total_s:.1f}s")

  # ── Errors ──
  all_errors = nar_errors + ixbrl_errors
  if all_errors:
    print(f"\n── Errors ({len(all_errors)}) ──")
    for r in nar_errors:
      print(f"  NAR   {r.ticker:6s} {r.accession:30s} {r.narrative_error}")
    for r in ixbrl_errors:
      print(f"  iXBRL {r.ticker:6s} {r.accession:30s} {r.ixbrl_error}")


def run(args) -> int:
  s3 = boto3.client("s3", region_name="us-east-1")
  bucket = args.bucket or _resolve_bucket()
  narrative_extractor = NarrativeExtractor(max_section_length=args.max_length)
  ixbrl_parser = iXBRLParser(max_section_length=args.max_length)

  years = [int(y.strip()) for y in args.years.split(",")] if args.years else [2025]
  form_types = (
    [f.strip().upper() for f in args.form_types.split(",")] if args.form_types else []
  )

  # Resolve companies: --sample N picks random CIKs from S3, otherwise use ticker lists
  sample_mode = args.sample is not None
  if sample_mode:
    print(f"Discovering filers in S3 for years {years}...")
    all_ciks: list[str] = []
    for year in years:
      year_ciks = _discover_ciks(s3, bucket, year)
      all_ciks.extend(year_ciks)
    unique_ciks = sorted(set(all_ciks))
    print(f"  Found {len(unique_ciks)} unique filers")
    sample_size = min(args.sample, len(unique_ciks))
    sampled_ciks = random.sample(unique_ciks, sample_size)
    # Use CIK as both ticker and CIK (no ticker lookup for random sample)
    ticker_to_cik = {cik: cik for cik in sampled_ciks}
    print(f"  Sampled {sample_size} random filers")
  elif args.tickers:
    ticker_list = [t.strip().upper() for t in args.tickers.split(",")]
    all_tickers = {**CORE_TICKERS, **EXPANDED_TICKERS}
    ticker_to_cik = {t: all_tickers[t] for t in ticker_list if t in all_tickers}
    missing = [t for t in ticker_list if t not in all_tickers]
    if missing:
      print(f"Warning: Unknown tickers (no CIK mapping): {missing}")
  elif args.expanded:
    ticker_to_cik = {**CORE_TICKERS, **EXPANDED_TICKERS}
  else:
    ticker_to_cik = CORE_TICKERS

  print("Search Pipeline Test")
  print(f"  Companies: {len(ticker_to_cik)}")
  print(f"  Years: {years}")
  if form_types:
    print(f"  Form types: {form_types}")
  print(f"  Max per company per year: {args.max_per_ticker}")
  if sample_mode:
    print("  Mode: random sample from S3")
  print()

  all_results: list[FilingResult] = []

  for year in years:
    if len(years) > 1:
      print(f"── Year {year} ──")

    for ticker, cik in sorted(ticker_to_cik.items()):
      zip_keys = _list_filings(s3, bucket, cik, year)
      if not zip_keys:
        continue

      if args.max_per_ticker:
        zip_keys = zip_keys[: args.max_per_ticker]

      for zip_key in zip_keys:
        result = test_filing(
          s3,
          bucket,
          zip_key,
          ticker,
          cik,
          narrative_extractor,
          ixbrl_parser,
          form_types,
        )
        all_results.append(result)
        _print_filing_result(result)

  _print_summary(all_results)

  # Dump failures for debugging
  if args.dump_failures:
    from pathlib import Path

    failures = [
      r
      for r in all_results
      if (r.narrative_error and not r.narrative_error.startswith("Skipped"))
      or (r.ixbrl_error and not r.ixbrl_error.startswith("Skipped"))
      or (
        not r.narrative_sections
        and not r.ixbrl_sections
        and not r.narrative_error
        and not r.ixbrl_error
      )
    ]
    if failures:
      dump_dir = Path("data/search-pipeline-debug")
      dump_dir.mkdir(parents=True, exist_ok=True)
      print(f"\nDumping {len(failures)} failures to {dump_dir}/")
      for r in failures:
        try:
          response = s3.get_object(Bucket=bucket, Key=r.zip_key)
          zip_bytes = response["Body"].read()
          html, _ = _extract_html_from_zip(zip_bytes)
          if html:
            out = dump_dir / f"{r.ticker}_{r.accession}.html"
            out.write_text(html, encoding="utf-8")
            print(f"  Dumped: {out}")
        except Exception as e:
          print(f"  Failed to dump {r.ticker} {r.accession}: {e}")

  errors = [
    r
    for r in all_results
    if (r.narrative_error and not r.narrative_error.startswith("Skipped"))
    or (r.ixbrl_error and not r.ixbrl_error.startswith("Skipped"))
  ]
  return 1 if errors else 0


def main():
  parser = argparse.ArgumentParser(
    description="Test narrative extractor + iXBRL parser on SEC filings from prod S3",
    formatter_class=argparse.RawDescriptionHelpFormatter,
    epilog="""
Examples:
  uv run python local/scripts/test_search_pipeline.py
  uv run python local/scripts/test_search_pipeline.py --expanded --years 2024,2025
  uv run python local/scripts/test_search_pipeline.py --tickers NVDA,AAPL --form-types 10-K
  uv run python local/scripts/test_search_pipeline.py --sample 200 --form-types 10-K,10-Q
  uv run python local/scripts/test_search_pipeline.py --sample 500 --max-per-ticker 1 --dump-failures
    """,
  )
  parser.add_argument(
    "--bucket",
    type=str,
    default=None,
    help="S3 raw bucket name (default: auto-detect from AWS account)",
  )
  parser.add_argument(
    "--years",
    type=str,
    default="2025",
    help="Comma-separated filing years (default: 2025)",
  )
  parser.add_argument(
    "--tickers",
    type=str,
    default=None,
    help="Comma-separated tickers (default: core 20)",
  )
  parser.add_argument(
    "--expanded",
    action="store_true",
    help="Use expanded ticker set (45+ companies)",
  )
  parser.add_argument(
    "--sample",
    type=int,
    default=None,
    help="Randomly sample N filers from S3 (unbiased coverage test)",
  )
  parser.add_argument(
    "--form-types",
    type=str,
    default=None,
    help="Comma-separated form types to include (default: all)",
  )
  parser.add_argument(
    "--max-per-ticker",
    type=int,
    default=5,
    help="Max filings per ticker per year (default: 5)",
  )
  parser.add_argument(
    "--max-length",
    type=int,
    default=50000,
    help="Max section length for extractors (default: 50000)",
  )
  parser.add_argument(
    "--dump-failures",
    action="store_true",
    help="Save failed/empty HTML files to data/search-pipeline-debug/",
  )
  args = parser.parse_args()
  sys.exit(run(args))


if __name__ == "__main__":
  main()
