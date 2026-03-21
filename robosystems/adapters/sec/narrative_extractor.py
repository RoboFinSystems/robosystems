"""Extract narrative sections from SEC 10-K/10-Q HTML filings.

Evolved from content-machine POC (tools/extract_10k_narrative.py).
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
- Item 2: MD&A
- Item 3: Market Risk
"""

import re
from dataclasses import dataclass
from html.parser import HTMLParser


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
  "2": ("item_2", "MD&A"),
  "3": ("item_3", "Market Risk"),
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
  """Convert HTML to clean text using fast parser."""
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


def _find_item_sections(text: str, target_items: dict) -> dict:
  """Find start positions of each Item section (content, not TOC).

  Uses heuristic: content sections have long runs of text after the heading,
  while TOC entries have many other ITEM headings nearby.
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

    for m in matches:
      after = text[m.start() : m.start() + 1000]
      # Count other ITEM headings in next 1000 chars (skip first 50)
      other_items = len(re.findall(r"ITEM\s+\d", after[50:], re.IGNORECASE))
      # TOC has many Item headings clustered; content has 0-1
      if other_items <= 1:
        sections[item_num] = {
          "section_id": section_id,
          "label": label,
          "start": m.start(),
        }
        break

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

    # Find section positions
    sections = _find_item_sections(text, target_items)
    if not sections:
      return []

    # Sort sections by position
    sorted_items = sorted(sections.items(), key=lambda x: x[1]["start"])

    # Build list of ALL Item/PART heading positions as boundaries
    all_boundaries: list[int] = []
    for m in re.finditer(
      r"(?:^|\n)\s*(?:Item|ITEM)\s+\d+[A-Z]?[\.\s\u2014\u2013:]", text
    ):
      all_boundaries.append(m.start())
    for m in re.finditer(r"(?:^|\n)\s*(?:PART|Part)\s+[IV]+\b", text):
      all_boundaries.append(m.start())
    all_boundaries.append(len(text))
    all_boundaries.sort()

    # Extract each section
    results: list[ExtractedSection] = []
    for _item_num, info in sorted_items:
      start = info["start"]
      # Find next boundary after this section
      next_starts = [p for p in all_boundaries if p > start + 100]
      end = next_starts[0] if next_starts else len(text)

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
