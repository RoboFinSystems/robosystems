"""Extract narrative sections from SEC 10-K/10-Q HTML filings.

Handles inconsistent formatting across filers using multi-strategy
section detection: regex patterns + heading proximity heuristics.

Sections extracted from 10-K:
- Item 1: Business
- Item 1A: Risk Factors
- Item 1C: Cybersecurity
- Item 2: Properties
- Item 7: MD&A
- Item 7A: Market Risk

Sections extracted from 10-Q:
- Item 1A: Risk Factors (quarterly updates)
- Item 2: MD&A
- Item 3: Market Risk
"""

import re
from dataclasses import dataclass
from html.parser import HTMLParser

from robosystems.adapters.sec.html_table import html_tables_to_markdown


@dataclass
class ExtractedSection:
  """A single extracted narrative section."""

  section_id: str  # e.g., "item_1a"
  section_label: str  # e.g., "Risk Factors"
  content: str  # Clean plain text
  word_count: int


# Target sections by form type
SECTIONS_10K = {
  "1": ("item_1", "Business"),
  "1A": ("item_1a", "Risk Factors"),
  "1C": ("item_1c", "Cybersecurity"),
  "2": ("item_2", "Properties"),
  "7": ("item_7", "MD&A"),
  "7A": ("item_7a", "Market Risk"),
}

SECTIONS_10Q = {
  "1A": ("item_1a", "Risk Factors"),
  "2": ("item_2", "MD&A"),
  "3": ("item_3", "Market Risk"),
}

# Fallback: match sections by heading name when Item-number detection fails.
# Maps (section_id, label) to regex patterns that match common heading variations.
_NAME_BASED_SECTIONS_10K: dict[tuple[str, str], str] = {
  ("item_1a", "Risk Factors"): r"(?:^|\n)\s*Risk\s+Factors\s*\n",
  ("item_1c", "Cybersecurity"): r"(?:^|\n)\s*Cybersecurity\s*\n",
  ("item_2", "Properties"): r"(?:^|\n)\s*Properties\s*\n",
  ("item_7", "MD&A"): (r"(?:^|\n)\s*Management.s\s+Discussion\s+and\s+Analysis"),
  ("item_7a", "Market Risk"): (
    r"(?:^|\n)\s*Quantitative\s+and\s+Qualitative\s+Disclosures?\s+About\s+Market\s+Risk"
  ),
}

# Max section length in characters (truncate very long sections)
DEFAULT_MAX_SECTION_LENGTH = 50000


class _HTMLTextExtractor(HTMLParser):
  """Strip HTML tags, preserving meaningful whitespace."""

  def __init__(self):
    super().__init__()
    self.text: list[str] = []
    self._skip = False
    self._skip_tags = {"script", "style", "ix:header"}

  def handle_starttag(self, tag, attrs):
    tag_lower = tag.lower()
    if tag_lower in self._skip_tags:
      self._skip = True
    if tag_lower in ("p", "div", "br", "tr", "li", "h1", "h2", "h3", "h4", "h5", "h6"):
      self.text.append("\n")
    if tag_lower == "td":
      self.text.append("\t")

  def handle_endtag(self, tag):
    if tag.lower() in self._skip_tags:
      self._skip = False
    if tag.lower() in (
      "p",
      "div",
      "tr",
      "li",
      "h1",
      "h2",
      "h3",
      "h4",
      "h5",
      "h6",
      "table",
    ):
      self.text.append("\n")

  def handle_data(self, data):
    if not self._skip:
      self.text.append(data)

  def get_text(self) -> str:
    return "".join(self.text)


def _html_to_text(html_content: str) -> str:
  """Convert HTML to clean text using fast parser.

  Tables are converted to markdown pipe tables before tag stripping,
  preserving column structure for financial data.
  """
  if "<table" in html_content or "<TABLE" in html_content:
    html_content = html_tables_to_markdown(html_content)
  extractor = _HTMLTextExtractor()
  extractor.feed(html_content)
  return extractor.get_text()


def _clean_text(text: str) -> str:
  """Clean extracted text — collapse whitespace, remove junk."""
  # Remove XBRL-style data blobs
  text = re.sub(r"[a-z]{2,10}:[A-Z][A-Za-z0-9]+Member", "", text)
  text = re.sub(r"\d{10,}", "", text)

  # Collapse multiple blank lines
  text = re.sub(r"\n\s*\n\s*\n+", "\n\n", text)

  # Collapse multiple spaces/tabs
  text = re.sub(r"[ \t]+", " ", text)

  # Remove empty lines, page numbers, TOC links
  lines = text.split("\n")
  cleaned: list[str] = []
  for line in lines:
    stripped = line.strip()
    if not stripped:
      if cleaned and cleaned[-1] != "":
        cleaned.append("")
      continue
    if re.match(r"^\d{1,3}$", stripped):
      continue
    if stripped.lower() == "table of contents":
      continue
    cleaned.append(stripped)

  return "\n".join(cleaned)


def _build_boundary_list(text: str) -> list[tuple[int, str]]:
  """Build sorted list of (position, item_key) for all Item/Part boundaries.

  item_key is the normalized item number (e.g., "1", "1A", "7") or "PART" for
  Part headings. Used to skip same-item boundaries during extraction.
  """
  boundaries: list[tuple[int, str]] = []
  for m in re.finditer(
    r"(?:^|\n)\s*(?:Item|ITEM)\s+(\d+[A-Z]?)[\.\s\u2014\u2013:]", text
  ):
    boundaries.append((m.start(), m.group(1).upper()))
  for m in re.finditer(r"(?:^|\n)\s*(?:PART|Part)\s+[IV]+\b", text):
    boundaries.append((m.start(), "PART"))
  boundaries.append((len(text), "END"))
  boundaries.sort()
  return boundaries


def _find_section_end(
  start: int, item_num: str, boundaries: list[tuple[int, str]], text_len: int
) -> int:
  """Find end position for a section, spanning repeated same-item blocks.

  Some filers (e.g. MSFT) repeat the same "Item N" marker for each sub-section,
  with "PART I" markers in between. This finds the last same-item boundary,
  then returns the position of the next different-item boundary after it.
  PART boundaries between same-item blocks are treated as internal structure.
  """
  item_upper = item_num.upper()

  # Find the last same-item boundary after start
  last_same = start
  for pos, key in boundaries:
    if pos > start and key == item_upper:
      last_same = pos

  # Find the first different-item boundary after the last same-item block.
  # Skip PART boundaries that sit between same-item blocks (they're internal),
  # but stop at PART boundaries after the last same-item block.
  for pos, key in boundaries:
    if pos <= last_same + 100:
      continue
    if key != item_upper and key != "PART":
      return pos
    if key == "PART":
      # Check if there's another same-item boundary after this PART
      has_more_same = any(p > pos and k == item_upper for p, k in boundaries)
      if not has_more_same:
        return pos
  return text_len


def _content_length_to_next_boundary(
  start: int, item_num: str, boundaries: list[tuple[int, str]]
) -> int:
  """Calculate content length from start to the next *different* item boundary.

  Used by _find_item_sections to score candidates by content length.
  """
  # boundaries[-1] is always (len(text), "END") from _build_boundary_list
  text_len = boundaries[-1][0] if boundaries else start
  end = _find_section_end(start, item_num, boundaries, text_len)
  return end - start if end > start else 0


def _find_item_sections(
  text: str, target_items: dict, boundaries: list[tuple[int, str]]
) -> dict:
  """Find start positions of each Item section (content, not TOC).

  Strategy: find ALL regex matches for each target item, filter out obvious
  TOC entries (many ITEM headings clustered nearby), then pick the candidate
  with the most content to the next different-item boundary. This avoids
  selecting cross-references or section-specific TOCs that appear earlier
  in the document but contain little actual content.
  """
  sections = {}

  for item_num, (section_id, label) in target_items.items():
    # Match "ITEM 1." or "ITEM 1A." followed by separator
    # Separators: period, whitespace, em-dash (U+2014), en-dash (U+2013), colon
    # Em-dash format seen in COST filings: "Item 2—Management's Discussion..."
    if item_num[-1].isalpha():
      pattern = rf"ITEM\s+{re.escape(item_num)}[\.\s\u2014\u2013:]"
    else:
      pattern = rf"ITEM\s+{re.escape(item_num)}(?![A-Z])[\.\s\u2014\u2013:]"

    matches = list(re.finditer(pattern, text, re.IGNORECASE))

    # Collect all candidates that pass TOC heuristics
    candidates: list[tuple[int, int]] = []  # (start_pos, content_length)
    for m in matches:
      after = text[m.start() : m.start() + 1000]
      # Count other ITEM headings in next 1000 chars (skip first 50)
      other_items = len(re.findall(r"ITEM\s+\d", after[50:], re.IGNORECASE))
      # Main TOC has many Item headings clustered (4+); real content sections
      # may reference 1-3 other items in their opening paragraph.
      if other_items > 3:
        continue

      # Skip inline cross-references: real section headers appear at line
      # start, not mid-sentence. Check if there's substantial text before the
      # match on the same line (e.g. "Refer to Risk Factors (Part II, Item 1A")
      line_start = text.rfind("\n", 0, m.start())
      preceding = text[line_start + 1 : m.start()].strip() if line_start >= 0 else ""
      if len(preceding) > 10:
        continue

      # Detect section-specific TOC: look for standalone page numbers in the
      # first 500 chars. TOC entries have lines like "32", "34" (page refs).
      # Real content sections don't have multiple standalone page numbers.
      nearby_lines = after[:500].split("\n")
      page_number_lines = sum(
        1 for line in nearby_lines if re.match(r"^\s*\d{1,3}\s*$", line)
      )
      if page_number_lines >= 2:
        continue

      content_len = _content_length_to_next_boundary(m.start(), item_num, boundaries)
      candidates.append((m.start(), content_len))

    # Pick the candidate with the most content
    if candidates:
      best_start, _ = max(candidates, key=lambda c: c[1])
      sections[item_num] = {
        "section_id": section_id,
        "label": label,
        "start": best_start,
      }

  return sections


def _find_sections_by_name(text: str, boundaries: list[tuple[int, str]]) -> dict:
  """Fallback: find sections by heading name when Item-number detection fails.

  Some filers (e.g. INTC) use section headings like "Risk Factors" and
  "Management's Discussion and Analysis" without "Item X" prefixes. This
  searches for those heading names directly, applying the same TOC/page-number
  filters and best-content-length scoring.
  """
  sections = {}

  for (section_id, label), pattern in _NAME_BASED_SECTIONS_10K.items():
    matches = list(re.finditer(pattern, text, re.IGNORECASE))

    candidates: list[tuple[int, int]] = []
    for m in matches:
      after = text[m.start() : m.start() + 500]

      # Filter TOC entries: standalone page numbers in nearby text
      nearby_lines = after[:500].split("\n")
      page_number_lines = sum(
        1 for line in nearby_lines if re.match(r"^\s*\d{1,3}\s*$", line)
      )
      if page_number_lines >= 2:
        continue

      # Score by content length to next boundary (any boundary works here
      # since we don't have a specific item number to skip)
      end = len(text)
      for pos, _key in boundaries:
        if pos > m.start() + 200:
          end = pos
          break
      content_len = end - m.start()
      candidates.append((m.start(), content_len))

    if candidates:
      best_start, _ = max(candidates, key=lambda c: c[1])
      # Derive a synthetic item_num for boundary compatibility
      sections[section_id] = {
        "section_id": section_id,
        "label": label,
        "start": best_start,
      }

  return sections


class NarrativeExtractor:
  """Extract narrative sections from SEC 10-K/10-Q HTML filings."""

  def __init__(self, max_section_length: int = DEFAULT_MAX_SECTION_LENGTH) -> None:
    self.max_section_length = max_section_length

  def extract(self, html: str, form_type: str) -> list[ExtractedSection]:
    """Extract narrative sections from a filing HTML document.

    Args:
        html: Raw HTML content of the filing
        form_type: SEC form type ("10-K" or "10-Q")

    Returns:
        List of extracted sections with clean text content
    """
    # Determine target sections based on form type
    form_upper = form_type.upper().replace("/A", "")
    if form_upper in ("10-K", "10-KSB", "20-F", "40-F"):
      target_items = SECTIONS_10K
    elif form_upper in ("10-Q", "10-QSB"):
      target_items = SECTIONS_10Q
    else:
      return []

    # Convert HTML to text
    text = _html_to_text(html)

    # Build boundary list once (shared by section detection and extraction)
    boundaries = _build_boundary_list(text)

    # Find section positions (primary: Item-number regex)
    sections = _find_item_sections(text, target_items, boundaries)

    # Fallback: if Item-number detection found nothing, try name-based detection.
    # Only for 10-K/10-KSB (INTC, Duke Energy, etc. omit "Item X" prefixes).
    # Excluded: 20-F/40-F use different item numbering and section names.
    use_name_fallback = False
    if not sections and form_upper in ("10-K", "10-KSB"):
      sections = _find_sections_by_name(text, boundaries)
      use_name_fallback = bool(sections)

    if not sections:
      return []

    # Sort sections by position
    sorted_items = sorted(sections.items(), key=lambda x: x[1]["start"])

    # Extract each section
    results: list[ExtractedSection] = []
    for item_num, info in sorted_items:
      start = info["start"]

      if use_name_fallback:
        # Name-based sections don't have item numbers in the boundary list,
        # so find end by looking for the next detected section or boundary.
        remaining_starts = [
          s["start"] for k, s in sections.items() if s["start"] > start + 200
        ]
        # Also consider any Item/PART boundary
        for pos, _key in boundaries:
          if pos > start + 200:
            remaining_starts.append(pos)
        end = min(remaining_starts) if remaining_starts else len(text)
      else:
        end = _find_section_end(start, item_num, boundaries, len(text))

      section_text = _clean_text(text[start:end])

      # Truncate very long sections
      if len(section_text) > self.max_section_length:
        section_text = section_text[: self.max_section_length]
        # Try to break at a paragraph boundary
        last_para = section_text.rfind("\n\n")
        if last_para > self.max_section_length * 0.8:
          section_text = section_text[:last_para]
        section_text += "\n\n[Section truncated]"

      word_count = len(section_text.split())
      if word_count < 10:
        continue  # Skip empty/trivial sections

      results.append(
        ExtractedSection(
          section_id=info["section_id"],
          section_label=info["label"],
          content=section_text,
          word_count=word_count,
        )
      )

    return results
