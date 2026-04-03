"""Benchmark narrative extractor against diverse SEC filings from prod S3.

Usage:
    uv run python -m robosystems.scripts.sec_extractor_benchmark
    uv run python -m robosystems.scripts.sec_extractor_benchmark --year 2025 --tickers AAPL,MSFT
    uv run python -m robosystems.scripts.sec_extractor_benchmark --top-n 50 --form-types 10-K
    uv run python -m robosystems.scripts.sec_extractor_benchmark --random-n 100 --form-types 10-K
    uv run python -m robosystems.scripts.sec_extractor_benchmark --max-per-ticker 3 --dump-failures

Connects to prod S3 raw bucket (read-only). Runs NarrativeExtractor on each
filing and reports success/failure with section counts. Use --dump-failures
to save failed HTML to local files for debugging.

Ticker sources (mutually exclusive, highest priority first):
  --tickers AAPL,MSFT   Explicit list (looked up from SEC if not in default set)
  --top-n 50            Top N by market cap (from SEC company_tickers.json)
  --random-n 100        Random N companies (from SEC company_tickers.json)
  (none)                Default set of ~25 diverse companies
"""

import argparse
import io
import json
import os
import random
import re
import sys
import time
import zipfile
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from urllib.request import Request, urlopen

import boto3

from robosystems.adapters.sec.narrative_extractor import NarrativeExtractor
from robosystems.config.storage.shared import DataSourceType, get_raw_key

SEC_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"


def _resolve_bucket() -> str:
  """Resolve the shared-raw S3 bucket name from env or AWS account.

  Uses os.environ directly (not robosystems.config.env) because this script
  runs standalone outside Docker where the config module's env vars aren't set.
  """
  bucket = os.environ.get("SHARED_RAW_BUCKET")
  if bucket:
    return bucket
  # Derive from AWS account ID
  sts = boto3.client("sts")
  account_id = sts.get_caller_identity()["Account"]
  return f"robosystems-{account_id}-shared-raw-prod"


def _fetch_sec_tickers() -> list[dict]:
  """Fetch company_tickers.json from SEC (sorted by market cap). Cached to /tmp."""
  cache = Path("/tmp/sec-debug/company_tickers.json")
  if cache.exists() and (time.time() - cache.stat().st_mtime) < 86400:
    with open(cache) as f:
      data = json.load(f)
  else:
    req = Request(
      SEC_TICKERS_URL, headers={"User-Agent": "RoboSystems admin@robosystems.ai"}
    )
    with urlopen(req, timeout=15) as resp:
      data = json.loads(resp.read())
    cache.parent.mkdir(parents=True, exist_ok=True)
    with open(cache, "w") as f:
      json.dump(data, f)
  # data is {"0": {"cik_str": ..., "ticker": ..., "title": ...}, "1": ...}
  return list(data.values())


def _resolve_tickers(
  args_tickers: str | None,
  top_n: int | None,
  random_n: int | None,
) -> dict[str, str]:
  """Resolve ticker→CIK mapping from args, --top-n, or --random-n."""
  if args_tickers:
    ticker_list = [t.strip().upper() for t in args_tickers.split(",")]
    # Try hardcoded first, then SEC lookup for any missing
    result = {
      t: DEFAULT_TICKERS_TO_CIK[t] for t in ticker_list if t in DEFAULT_TICKERS_TO_CIK
    }
    missing = [t for t in ticker_list if t not in result]
    if missing:
      all_tickers = _fetch_sec_tickers()
      sec_lookup = {e["ticker"]: str(e["cik_str"]) for e in all_tickers}
      for t in missing:
        if t in sec_lookup:
          result[t] = sec_lookup[t]
        else:
          print(f"Warning: Unknown ticker: {t}")
    return result

  if top_n or random_n:
    all_tickers = _fetch_sec_tickers()
    if top_n:
      selected = all_tickers[:top_n]
    else:
      selected = random.sample(all_tickers, min(random_n, len(all_tickers)))  # type: ignore[arg-type]
    return {e["ticker"]: str(e["cik_str"]) for e in selected}

  return DEFAULT_TICKERS_TO_CIK


# Diverse set of filers: large-cap, mid-cap, small-cap, foreign (20-F), across industries
DEFAULT_TICKERS_TO_CIK = {
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
  # Mid-cap / different formatting
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
  # Known parsing issues (cross-reference false positives, short sections)
  "CTSH": "1058290",  # Item 1A/7 grabbed from forward-looking stmts cross-ref
  "CMG": "1058090",  # Item 1A grabbed from Business section cross-ref
  "NP": "2067129",  # MD&A captured only intro paragraph (outline-only)
  "IMMR": "1058811",  # Risk factors captured summary only
  "NB": "1512228",  # Risk factors has currency table mixed in
}


@dataclass
class FilingResult:
  ticker: str
  cik: str
  accession: str
  form_type: str
  zip_key: str
  html_file: str = ""
  html_size: int = 0
  sections_found: list[str] = field(default_factory=list)
  section_word_counts: dict[str, int] = field(default_factory=dict)
  table_counts: dict[str, int] = field(
    default_factory=dict
  )  # section_id → markdown table count
  error: str | None = None
  elapsed_ms: int = 0


def _get_s3_client():
  return boto3.client("s3", region_name="us-east-1")


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
    html[:2000000],
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


def list_filings(s3, bucket: str, cik: str, year: int) -> list[str]:
  """List ZIP files for a CIK in a given year."""
  # S3 keys use zero-padded 10-digit CIKs
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
  extractor: NarrativeExtractor,
  form_types: list[str],
) -> FilingResult:
  """Test the extractor on a single filing."""
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
      result.error = "No HTML found in ZIP"
      result.elapsed_ms = int((time.monotonic() - start) * 1000)
      return result

    result.html_file = html_file
    result.html_size = len(html)

    form_type = _detect_form_type(html)
    result.form_type = form_type

    if form_types and form_type not in form_types:
      result.error = f"Skipped: form type {form_type} not in {form_types}"
      result.elapsed_ms = int((time.monotonic() - start) * 1000)
      return result

    sections = extractor.extract(html, form_type)
    result.sections_found = [s.section_id for s in sections]
    result.section_word_counts = {s.section_id: s.word_count for s in sections}
    # Count markdown tables (| ... | rows following a | --- | separator)
    for s in sections:
      table_count = len(re.findall(r"^\| ---", s.content, re.MULTILINE))
      if table_count:
        result.table_counts[s.section_id] = table_count

  except Exception as e:
    result.error = str(e)

  result.elapsed_ms = int((time.monotonic() - start) * 1000)
  return result


def run(args):
  s3 = _get_s3_client()
  bucket = args.bucket or _resolve_bucket()
  extractor = NarrativeExtractor(max_section_length=args.max_length)

  # Resolve tickers
  ticker_to_cik = _resolve_tickers(args.tickers, args.top_n, args.random_n)

  form_types = (
    [f.strip().upper() for f in args.form_types.split(",")] if args.form_types else []
  )

  print(f"Testing {len(ticker_to_cik)} companies, year={args.year}")
  if form_types:
    print(f"Filtering to form types: {form_types}")
  print()

  all_results: list[FilingResult] = []
  failures: list[FilingResult] = []
  no_sections: list[FilingResult] = []

  for ticker, cik in sorted(ticker_to_cik.items()):
    zip_keys = list_filings(s3, bucket, cik, args.year)
    if not zip_keys:
      print(f"  {ticker} ({cik}): no filings found for {args.year}")
      continue

    # Limit filings per ticker
    if args.max_per_ticker:
      zip_keys = zip_keys[: args.max_per_ticker]

    for zip_key in zip_keys:
      result = test_filing(s3, bucket, zip_key, ticker, cik, extractor, form_types)
      all_results.append(result)

      if result.error and not result.error.startswith("Skipped"):
        failures.append(result)
        status = f"ERROR: {result.error}"
      elif not result.sections_found:
        no_sections.append(result)
        status = f"0 sections ({result.form_type}, {result.html_size:,} bytes)"
      else:
        section_summary = ", ".join(
          f"{sid}({wc}w)" for sid, wc in result.section_word_counts.items()
        )
        total_tables = sum(result.table_counts.values())
        table_info = f" [{total_tables} tables]" if total_tables else ""
        status = f"{len(result.sections_found)} sections{table_info}: {section_summary}"

      print(
        f"  {ticker:6s} {result.accession:30s} {result.form_type:5s} "
        f"{result.elapsed_ms:5d}ms  {status}"
      )

  # Summary
  tested = [r for r in all_results if not (r.error and r.error.startswith("Skipped"))]
  with_sections = [r for r in tested if r.sections_found and not r.error]

  print(f"\n{'=' * 80}")
  print(
    f"RESULTS: {len(tested)} filings tested, {len(with_sections)} with sections, "
    f"{len(failures)} errors, {len(no_sections)} with 0 sections"
  )

  if no_sections:
    print("\nFilings with 0 sections extracted:")
    for r in no_sections:
      print(
        f"  {r.ticker:6s} {r.accession:30s} {r.form_type:5s} "
        f"html={r.html_size:,} bytes  file={r.html_file}"
      )

  if failures:
    print("\nFailures:")
    for r in failures:
      print(f"  {r.ticker:6s} {r.accession:30s} {r.error}")

  # Section coverage stats
  if with_sections:
    section_counts = Counter()
    for r in with_sections:
      for sid in r.sections_found:
        section_counts[sid] += 1
    total_10k = len([r for r in with_sections if r.form_type == "10-K"])
    total_10q = len([r for r in with_sections if r.form_type == "10-Q"])

    print(f"\nSection coverage ({total_10k} 10-Ks, {total_10q} 10-Qs):")
    for sid, count in section_counts.most_common():
      print(f"  {sid:12s}: {count:3d} ({count / len(with_sections) * 100:.0f}%)")

  # Table conversion stats
  if with_sections:
    filings_with_tables = [r for r in with_sections if r.table_counts]
    total_tables = sum(sum(r.table_counts.values()) for r in with_sections)
    print(
      f"\nTable conversion: {total_tables} markdown tables across "
      f"{len(filings_with_tables)}/{len(with_sections)} filings"
    )

  # Dump failures for debugging
  if args.dump_failures and (failures or no_sections):
    dump_dir = Path("data/extractor-debug")
    dump_dir.mkdir(parents=True, exist_ok=True)
    for r in failures + no_sections:
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

  success_rate = len(with_sections) / len(tested) * 100 if tested else 0
  print(f"\nSuccess rate: {success_rate:.0f}%")

  return 0 if not failures else 1


def main():
  parser = argparse.ArgumentParser(
    description="Test narrative extractor against diverse SEC filings from prod S3",
    formatter_class=argparse.RawDescriptionHelpFormatter,
  )
  parser.add_argument(
    "--bucket",
    type=str,
    default=None,
    help="S3 raw bucket name (default: auto-detect from AWS account)",
  )
  parser.add_argument(
    "--year",
    type=int,
    default=2025,
    help="Filing year to test (default: 2025)",
  )
  parser.add_argument(
    "--tickers",
    type=str,
    default=None,
    help="Comma-separated tickers to test (looked up from SEC if not in default set)",
  )
  parser.add_argument(
    "--top-n",
    type=int,
    default=None,
    help="Test top N companies by market cap (from SEC company_tickers.json)",
  )
  parser.add_argument(
    "--random-n",
    type=int,
    default=None,
    help="Test N randomly sampled companies (from SEC company_tickers.json)",
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
    help="Max filings per ticker (default: 5)",
  )
  parser.add_argument(
    "--max-length",
    type=int,
    default=50000,
    help="Max section length for extractor (default: 50000)",
  )
  parser.add_argument(
    "--dump-failures",
    action="store_true",
    help="Save failed/empty HTML files to data/extractor-debug/ for inspection",
  )
  args = parser.parse_args()
  sys.exit(run(args))


if __name__ == "__main__":
  main()
