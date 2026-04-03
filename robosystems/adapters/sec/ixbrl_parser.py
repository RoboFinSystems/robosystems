"""iXBRL disclosure parser — extract sections with XBRL element metadata.

Parses inline XBRL (iXBRL) HTML to extract disclosure text blocks and the
XBRL elements they contain. This bridges the knowledge graph (structured facts)
with document search (narrative text).

The key insight: iXBRL text block elements (`ix:nonNumeric` with TextBlock names)
ARE the disclosure sections. They wrap the narrative content and contain nested
`ix:nonFraction` numeric facts. By extracting both the text and the element names,
we enable bidirectional navigation between graph facts and their filing context.

iXBRL continuation pattern:
  <ix:nonNumeric name="us-gaap:GoodwillDisclosureTextBlock" continuedAt="f-571-1">
    Goodwill
  </ix:nonNumeric>
  <ix:continuation id="f-571-1">
    <p>The following table summarizes changes in goodwill...</p>
    <ix:nonFraction name="us-gaap:Goodwill">32,431</ix:nonFraction>
  </ix:continuation>
"""

import re
from dataclasses import dataclass

from robosystems.adapters.sec.html_table import html_tables_to_markdown
from robosystems.logger import logger


@dataclass
class iXBRLSection:
  """A disclosure section extracted from iXBRL with element metadata."""

  section_id: str  # e.g., "us-gaap:GoodwillDisclosureTextBlock"
  section_label: str  # e.g., "Goodwill Disclosure"
  content: str  # Plain text (HTML stripped)
  word_count: int
  xbrl_elements: list[str]  # Element qnames found in this section
  element_count: int


# Minimum word count to keep a section (skip trivial/empty blocks)
MIN_SECTION_WORDS = 20

# Maximum section length in characters
DEFAULT_MAX_SECTION_LENGTH = 50000


def _label_from_element_name(name: str) -> str:
  """Derive a readable label from an XBRL element qname.

  Examples:
    'us-gaap:GoodwillDisclosureTextBlock' → 'Goodwill Disclosure'
    'us-gaap:RevenueFromContractWithCustomerPolicyTextBlock' → 'Revenue From Contract With Customer Policy'
  """
  # Strip namespace prefix
  local = name.split(":")[-1] if ":" in name else name

  # Strip TextBlock suffix
  for suffix in ["TextBlock", "TableTextBlock"]:
    if local.endswith(suffix):
      local = local[: -len(suffix)]

  # Split camelCase
  label = re.sub(r"([a-z])([A-Z])", r"\1 \2", local)
  label = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1 \2", label)

  return label.strip()


def _strip_html(html: str) -> str:
  """Strip HTML tags and normalize whitespace.

  Tables are converted to markdown pipe tables before tag stripping,
  preserving column structure for financial data.
  """
  if "<table" in html or "<TABLE" in html:
    html = html_tables_to_markdown(html)
  text = re.sub(
    r"<style[^>]*>.*?</\s*style\s*>", " ", html, flags=re.DOTALL | re.IGNORECASE
  )
  text = re.sub(
    r"<script[^>]*>.*?</\s*script\s*>", " ", text, flags=re.DOTALL | re.IGNORECASE
  )
  text = re.sub(r"<[^>]+>", " ", text)
  text = re.sub(r"&nbsp;?", " ", text)
  text = re.sub(r"&amp;?", "&", text)
  text = re.sub(r"&#\d+;", " ", text)
  text = re.sub(r"\s+", " ", text)
  return text.strip()


def _extract_elements_from_block(html_block: str) -> list[str]:
  """Extract unique XBRL element qnames from ix:nonFraction tags in a block."""
  elements = set()
  for match in re.finditer(
    r"<ix:nonFraction[^>]*name=\"([^\"]+)\"", html_block, re.IGNORECASE
  ):
    elements.add(match.group(1))
  # Also capture ix:nonNumeric elements nested inside (sub-blocks)
  for match in re.finditer(
    r"<ix:nonNumeric[^>]*name=\"([^\"]+)\"", html_block, re.IGNORECASE
  ):
    name = match.group(1)
    # Skip the parent text block itself and DEI metadata
    if not name.endswith("TextBlock") and not name.startswith("dei:"):
      elements.add(name)
  return sorted(elements)


class iXBRLParser:
  """Parse iXBRL HTML to extract disclosure sections with XBRL element metadata."""

  def __init__(self, max_section_length: int = DEFAULT_MAX_SECTION_LENGTH) -> None:
    self.max_section_length = max_section_length

  def parse(self, html: str) -> list[iXBRLSection]:
    """Parse iXBRL document into disclosure sections.

    Extracts ix:nonNumeric TextBlock elements as sections, resolving
    ix:continuation chains for split content. Each section includes
    the list of XBRL elements (numeric facts) it contains.

    Uses string-find based tag matching instead of regex with (.*?) to
    avoid catastrophic memory usage on large filings (the old regex
    approach caused 7GB+ allocation on 5MB files).

    Args:
        html: Raw iXBRL HTML content

    Returns:
        List of disclosure sections with element metadata
    """
    # Build continuation lookup: id → content
    continuations = self._build_continuation_map(html)

    # Find all TextBlock ix:nonNumeric elements using string-find
    sections: list[iXBRLSection] = []
    seen_ids: set[str] = set()

    for element_name, attrs, inline_content in self._find_textblocks(html):
      # Skip DEI and ECD metadata text blocks
      if element_name.startswith(("dei:", "ecd:")):
        continue

      # Deduplicate (same element can appear multiple times)
      if element_name in seen_ids:
        continue
      seen_ids.add(element_name)

      # Resolve continuation chain
      full_html = inline_content
      continued_at = re.search(r'continuedAt="([^"]+)"', attrs)
      if continued_at:
        chain_content = self._resolve_continuation_chain(
          continued_at.group(1), continuations
        )
        full_html = inline_content + chain_content

      # Extract XBRL elements from the block
      xbrl_elements = _extract_elements_from_block(full_html)

      # Strip HTML for plain text content
      content = _strip_html(full_html)

      # Truncate if needed
      if len(content) > self.max_section_length:
        content = content[: self.max_section_length] + " [Section truncated]"

      word_count = len(content.split())
      if word_count < MIN_SECTION_WORDS:
        continue

      section_label = _label_from_element_name(element_name)

      sections.append(
        iXBRLSection(
          section_id=element_name,
          section_label=section_label,
          content=content,
          word_count=word_count,
          xbrl_elements=xbrl_elements,
          element_count=len(xbrl_elements),
        )
      )

    logger.debug(
      f"iXBRL parsed: {len(sections)} disclosure sections, "
      f"{sum(s.element_count for s in sections)} total elements"
    )
    return sections

  def _find_textblocks(self, html: str) -> list[tuple[str, str, str]]:
    """Find ix:nonNumeric TextBlock elements using string-find matching.

    Uses str.find() with nesting-aware close-tag matching instead of
    regex (.*?) which causes catastrophic backtracking on large files.

    Returns list of (element_name, attrs_str, inner_html).
    """
    results: list[tuple[str, str, str]] = []
    html_lower = html.lower()
    open_tag = "<ix:nonnumeric"
    close_tag = "</ix:nonnumeric"

    pos = 0
    while True:
      start = html_lower.find(open_tag, pos)
      if start == -1:
        break

      tag_end = html.find(">", start)
      if tag_end == -1:
        break

      attrs = html[start + len(open_tag) : tag_end]
      content_start = tag_end + 1

      # Only care about TextBlock elements
      if "textblock" not in attrs.lower():
        pos = content_start
        continue

      # Find matching close tag, accounting for nesting
      depth = 1
      search_pos = content_start
      found = False
      while depth > 0:
        next_open = html_lower.find(open_tag, search_pos)
        next_close = html_lower.find(close_tag, search_pos)

        if next_close == -1:
          break

        if next_open != -1 and next_open < next_close:
          depth += 1
          search_pos = next_open + len(open_tag)
        else:
          depth -= 1
          if depth == 0:
            inner = html[content_start:next_close]
            name_match = re.search(
              r'name="([^"]*TextBlock[^"]*)"', attrs, re.IGNORECASE
            )
            if name_match:
              results.append((name_match.group(1), attrs, inner))
            close_end = html.find(">", next_close)
            pos = close_end + 1 if close_end != -1 else next_close + 1
            found = True
            break
          else:
            search_pos = next_close + len(close_tag)

      if not found:
        pos = content_start

    return results

  def _build_continuation_map(self, html: str) -> dict[str, str]:
    """Build a lookup of continuation id → HTML content.

    Uses string-find matching instead of regex (.*?) to avoid
    catastrophic memory usage on large filings.
    """
    continuations: dict[str, str] = {}
    html_lower = html.lower()
    open_tag = "<ix:continuation"
    close_tag = "</ix:continuation"

    pos = 0
    while True:
      start = html_lower.find(open_tag, pos)
      if start == -1:
        break

      tag_end = html.find(">", start)
      if tag_end == -1:
        break

      attrs = html[start:tag_end]
      content_start = tag_end + 1

      id_match = re.search(r'id="([^"]+)"', attrs)
      if not id_match:
        pos = content_start
        continue

      cont_id = id_match.group(1)

      # Find matching close tag, accounting for nesting
      depth = 1
      search_pos = content_start
      found = False
      while depth > 0:
        next_open = html_lower.find(open_tag, search_pos)
        next_close = html_lower.find(close_tag, search_pos)

        if next_close == -1:
          break

        if next_open != -1 and next_open < next_close:
          depth += 1
          search_pos = next_open + len(open_tag)
        else:
          depth -= 1
          if depth == 0:
            continuations[cont_id] = html[content_start:next_close]
            close_end = html.find(">", next_close)
            pos = close_end + 1 if close_end != -1 else next_close + 1
            found = True
            break
          else:
            search_pos = next_close + len(close_tag)

      if not found:
        pos = content_start

    return continuations

  def _resolve_continuation_chain(
    self, start_id: str, continuations: dict[str, str]
  ) -> str:
    """Follow a chain of continuedAt references to build full content."""
    parts: list[str] = []
    current_id = start_id
    visited: set[str] = set()

    while current_id and current_id not in visited:
      visited.add(current_id)
      content = continuations.get(current_id, "")
      if not content:
        break

      # Check if this continuation itself continues
      next_match = re.search(r'continuedAt="([^"]+)"', content)
      parts.append(content)

      current_id = next_match.group(1) if next_match else None

    return "".join(parts)
