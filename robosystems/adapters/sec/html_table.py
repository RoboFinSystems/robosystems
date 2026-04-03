"""Convert HTML tables to markdown pipe tables for SEC filings.

SEC filings use HTML tables heavily for financial data. When converting HTML
to plain text for search indexing, naive tag-stripping destroys table structure,
making financial data unsearchable (numbers lose column context).

This module pre-processes HTML to replace <table> blocks with markdown pipe
tables before general tag-stripping occurs. Layout tables (single-column,
mostly empty) are skipped and left for normal text extraction.

Handles SEC-specific patterns:
- Dollar signs ($) in separate cells from numbers
- HTML entities (&nbsp;, &#160;, &amp;)
- colspan/rowspan in header rows
- iXBRL tags (<ix:nonFraction>, <ix:nonNumeric>) wrapping cell values
- Deeply nested <span>/<div> within cells
"""

import re
from html.parser import HTMLParser


def html_tables_to_markdown(html: str) -> str:
  """Replace HTML <table> blocks with markdown pipe tables.

  Processes the HTML string, finds all top-level <table>...</table> blocks,
  converts data tables to markdown, and leaves layout tables as-is.

  Args:
      html: Raw HTML string (may contain iXBRL tags)

  Returns:
      HTML with <table> blocks replaced by markdown text (still contains
      other HTML tags for the caller to handle)
  """
  # Find all top-level table blocks. Use compiled regexes with pos-based
  # searching (not string slicing) to avoid O(n*m) memory on large filings.
  result: list[str] = []
  pos = 0
  table_open_re = re.compile(r"<table[\s>]", re.IGNORECASE)
  table_close_re = re.compile(r"</table\s*>", re.IGNORECASE)

  while pos < len(html):
    m = table_open_re.search(html, pos)
    if not m:
      result.append(html[pos:])
      break

    # Append everything before this table
    result.append(html[pos : m.start()])

    # Find matching </table>, accounting for nesting
    table_start = m.start()
    depth = 1
    search_pos = m.end()
    while depth > 0 and search_pos < len(html):
      next_open = table_open_re.search(html, search_pos)
      next_close = table_close_re.search(html, search_pos)

      if next_close is None:
        # Malformed HTML — no closing tag
        break

      if next_open and next_open.start() < next_close.start():
        depth += 1
        search_pos = next_open.end()
      else:
        depth -= 1
        if depth == 0:
          table_end = next_close.end()
          table_html = html[table_start:table_end]
          md = _convert_table(table_html)
          if md:
            result.append("\n" + md + "\n")
          else:
            # Layout table — keep original HTML for normal text extraction
            result.append(table_html)
          pos = table_end
          break
        else:
          search_pos = next_close.end()
    else:
      # Couldn't find closing tag — append rest as-is
      result.append(html[table_start:])
      pos = len(html)
      break

  return "".join(result)


class _CellExtractor(HTMLParser):
  """Extract text from a single table cell, stripping all tags."""

  def __init__(self):
    super().__init__()
    self.parts: list[str] = []
    self._skip = False

  def handle_starttag(self, tag, attrs):
    if tag.lower() in ("script", "style"):
      self._skip = True

  def handle_endtag(self, tag):
    if tag.lower() in ("script", "style"):
      self._skip = False

  def handle_data(self, data):
    if not self._skip:
      self.parts.append(data)

  def handle_entityref(self, name):
    """Handle named entities like &amp; &lt; &gt;."""
    if not self._skip:
      entities = {"amp": "&", "lt": "<", "gt": ">", "nbsp": " ", "quot": '"'}
      self.parts.append(entities.get(name, f"&{name};"))

  def handle_charref(self, name):
    """Handle numeric character references like &#160;."""
    if not self._skip:
      try:
        if name.startswith("x"):
          self.parts.append(chr(int(name[1:], 16)))
        else:
          self.parts.append(chr(int(name)))
      except (ValueError, OverflowError):
        pass

  def get_text(self) -> str:
    text = "".join(self.parts)
    # Decode common HTML entities that HTMLParser doesn't handle
    text = text.replace("\xa0", " ")  # &nbsp; decoded
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _cell_text(html: str) -> str:
  """Extract clean text from a table cell's inner HTML."""
  # Replace nbsp variants before parsing (HTMLParser handles &amp; etc.)
  html = html.replace("&nbsp;", " ")
  html = html.replace("&#160;", " ")

  extractor = _CellExtractor()
  try:
    extractor.feed(html)
  except Exception:
    # Fallback: regex strip then decode entities
    text = re.sub(r"<[^>]+>", " ", html)
    text = text.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
    return re.sub(r"\s+", " ", text).strip()
  return extractor.get_text()


def _get_colspan(attrs_str: str) -> int:
  """Extract colspan value from a tag's attribute string."""
  m = re.search(r'colspan\s*=\s*["\']?(\d+)', attrs_str, re.IGNORECASE)
  return int(m.group(1)) if m else 1


def _parse_rows(table_html: str) -> list[list[tuple[str, int]]]:
  """Parse table HTML into rows of (cell_text, colspan) tuples."""
  rows: list[list[tuple[str, int]]] = []

  for tr_match in re.finditer(
    r"<tr[^>]*>(.*?)</tr>", table_html, re.DOTALL | re.IGNORECASE
  ):
    tr_content = tr_match.group(1)
    cells: list[tuple[str, int]] = []

    for cell_match in re.finditer(
      r"<(t[dh])([^>]*)>(.*?)</\1>", tr_content, re.DOTALL | re.IGNORECASE
    ):
      attrs = cell_match.group(2)
      inner = cell_match.group(3)
      text = _cell_text(inner)
      colspan = _get_colspan(attrs)
      cells.append((text, colspan))

    if cells:
      rows.append(cells)

  return rows


def _merge_currency_cells(rows: list[list[tuple[str, int]]]) -> list[list[str]]:
  """Merge adjacent currency-symbol cells with their number cells.

  SEC filings often put "$" in its own <td>, separate from the number.
  This merges ["$", "11,866.1"] into ["$11,866.1"] and expands colspan.
  Also drops columns that are empty across all rows (SEC spacer cells).
  """
  merged: list[list[str]] = []

  for row in rows:
    # Expand colspan first
    expanded: list[str] = []
    for text, colspan in row:
      expanded.append(text)
      for _ in range(colspan - 1):
        expanded.append("")

    # Merge currency symbols with next non-empty cell
    final: list[str] = []
    i = 0
    while i < len(expanded):
      cell = expanded[i]
      if cell in ("$", "(", "($"):
        # Find the next non-empty cell to merge with
        j = i + 1
        while j < len(expanded) and not expanded[j].strip():
          j += 1
        if j < len(expanded):
          # Add empty cells between as-is, then the merged cell
          for k in range(i + 1, j):
            final.append("")
          final.append(cell + expanded[j])
          i = j + 1
        else:
          final.append(cell)
          i += 1
      elif cell.startswith((")", "%")) and len(cell) <= 2:
        # Closing paren/percent — attach to the last non-empty value
        for k in range(len(final) - 1, -1, -1):
          if final[k].strip():
            final[k] = final[k] + cell
            break
        else:
          final.append(cell)
        i += 1
      else:
        final.append(cell)
        i += 1

    merged.append(final)

  # Drop columns that are empty across all rows (SEC spacer <td> elements)
  if merged:
    max_cols = max(len(row) for row in merged)
    # Pad rows to uniform length for column analysis
    for row in merged:
      while len(row) < max_cols:
        row.append("")

    keep_cols = [
      col
      for col in range(max_cols)
      if any(row[col].strip() for row in merged if col < len(row))
    ]
    merged = [[row[col] for col in keep_cols] for row in merged]

  return merged


def _is_layout_table(rows: list[list[str]]) -> bool:
  """Heuristic: detect layout tables that shouldn't become markdown.

  Layout tables in SEC filings are used for page structure (addresses,
  checkboxes, single-column text wrapping). Financial data tables have
  multiple meaningful columns with data.
  """
  if len(rows) < 2:
    return True

  # Count non-empty cells per row to find effective column count
  col_counts = []
  for row in rows:
    non_empty = sum(1 for c in row if c.strip())
    col_counts.append(non_empty)

  # If most rows have only 1 non-empty cell, it's a layout table
  if col_counts:
    avg_cols = sum(col_counts) / len(col_counts)
    if avg_cols < 1.5:
      return True

  return False


def _convert_table(table_html: str) -> str | None:
  """Convert a single HTML table to a markdown pipe table.

  Returns None if the table is a layout table (should be left as HTML).
  """
  rows = _parse_rows(table_html)
  if not rows:
    return None

  merged = _merge_currency_cells(rows)
  if _is_layout_table(merged):
    return None

  # Normalize column count across all rows
  max_cols = max(len(row) for row in merged)
  if max_cols < 2:
    return None

  normalized: list[list[str]] = []
  for row in merged:
    # Strip empty-only rows
    if all(not c.strip() for c in row):
      continue
    # Pad or truncate to max_cols
    padded = row[:max_cols] + [""] * (max_cols - len(row))
    normalized.append(padded)

  if len(normalized) < 2:
    return None

  # Build markdown table
  lines: list[str] = []

  # First row as header
  header = normalized[0]
  lines.append("| " + " | ".join(c if c else " " for c in header) + " |")

  # Separator
  lines.append("| " + " | ".join("---" for _ in header) + " |")

  # Data rows
  for row in normalized[1:]:
    # Escape pipe characters in cell content
    escaped = [c.replace("|", "\\|") for c in row]
    lines.append("| " + " | ".join(c if c else " " for c in escaped) + " |")

  return "\n".join(lines)
