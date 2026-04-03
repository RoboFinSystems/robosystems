"""Tests for HTML table to markdown conversion."""

import pytest

from robosystems.adapters.sec.html_table import (
  _cell_text,
  _convert_table,
  _is_layout_table,
  _merge_currency_cells,
  _parse_rows,
  html_tables_to_markdown,
)


@pytest.mark.unit
class TestCellText:
  def test_strips_tags(self):
    assert _cell_text("<span>Hello</span>") == "Hello"

  def test_strips_nested_tags(self):
    assert _cell_text("<span><b>Bold</b> text</span>") == "Bold text"

  def test_decodes_html_entities(self):
    assert _cell_text("A&amp;B") == "A&B"
    assert _cell_text("100&#160;") == "100"

  def test_strips_ixbrl_tags(self):
    html = (
      '<ix:nonFraction name="us-gaap:Revenue" contextRef="c1">11,866.1</ix:nonFraction>'
    )
    assert _cell_text(html) == "11,866.1"

  def test_collapses_whitespace(self):
    assert _cell_text("  hello   world  ") == "hello world"

  def test_empty_cell(self):
    assert _cell_text("") == ""
    assert _cell_text("   ") == ""


@pytest.mark.unit
class TestParseRows:
  def test_simple_table(self):
    html = """
    <table>
      <tr><td>A</td><td>B</td></tr>
      <tr><td>1</td><td>2</td></tr>
    </table>
    """
    rows = _parse_rows(html)
    assert len(rows) == 2
    assert rows[0] == [("A", 1), ("B", 1)]
    assert rows[1] == [("1", 1), ("2", 1)]

  def test_colspan(self):
    html = """
    <table>
      <tr><td colspan="2">Header</td></tr>
      <tr><td>A</td><td>B</td></tr>
    </table>
    """
    rows = _parse_rows(html)
    assert rows[0] == [("Header", 2)]

  def test_th_elements(self):
    html = """
    <table>
      <tr><th>Name</th><th>Value</th></tr>
      <tr><td>Revenue</td><td>100</td></tr>
    </table>
    """
    rows = _parse_rows(html)
    assert rows[0] == [("Name", 1), ("Value", 1)]

  def test_empty_table(self):
    assert _parse_rows("<table></table>") == []


@pytest.mark.unit
class TestMergeCurrencyCells:
  def test_merges_dollar_sign(self):
    rows = [[("Revenue", 1), ("$", 1), ("11,866", 1)]]
    result = _merge_currency_cells(rows)
    assert result == [["Revenue", "$11,866"]]

  def test_merges_open_paren(self):
    rows = [[("Loss", 1), ("(", 1), ("500", 1)]]
    result = _merge_currency_cells(rows)
    assert result == [["Loss", "(500"]]

  def test_merges_dollar_paren(self):
    rows = [[("Loss", 1), ("($", 1), ("500", 1)]]
    result = _merge_currency_cells(rows)
    assert result == [["Loss", "($500"]]

  def test_expands_colspan(self):
    rows = [[("Header", 3), ("Value", 1)], [("A", 1), ("B", 1), ("C", 1), ("D", 1)]]
    result = _merge_currency_cells(rows)
    # colspan expanded + empty columns removed (cols with no data across all rows)
    assert result[1] == ["A", "B", "C", "D"]
    assert result[0][0] == "Header"
    assert result[0][-1] == "Value"

  def test_no_merge_needed(self):
    rows = [[("A", 1), ("B", 1), ("C", 1)]]
    result = _merge_currency_cells(rows)
    assert result == [["A", "B", "C"]]


@pytest.mark.unit
class TestIsLayoutTable:
  def test_single_column(self):
    assert _is_layout_table([["Hello"], ["World"]]) is True

  def test_single_row(self):
    assert _is_layout_table([["A", "B"]]) is True

  def test_data_table(self):
    rows = [["Name", "Value"], ["Revenue", "$100"], ["Cost", "$50"]]
    assert _is_layout_table(rows) is False

  def test_mostly_empty(self):
    rows = [["", "Text"], ["", "More"]]
    assert _is_layout_table(rows) is True


@pytest.mark.unit
class TestConvertTable:
  def test_simple_financial_table(self):
    html = """
    <table>
      <tr><td>Metric</td><td>2025</td><td>2024</td></tr>
      <tr><td>Revenue</td><td>$11,866</td><td>$11,247</td></tr>
      <tr><td>Net Income</td><td>$2,100</td><td>$1,900</td></tr>
    </table>
    """
    md = _convert_table(html)
    assert md is not None
    lines = md.strip().split("\n")
    assert len(lines) == 4  # header + separator + 2 data rows
    assert "| Metric | 2025 | 2024 |" in lines[0]
    assert "| --- | --- | --- |" in lines[1]
    assert "Revenue" in lines[2]

  def test_returns_none_for_layout_table(self):
    html = """
    <table>
      <tr><td>Just a paragraph of text here.</td></tr>
      <tr><td>Another paragraph below.</td></tr>
    </table>
    """
    assert _convert_table(html) is None

  def test_sec_currency_in_separate_cells(self):
    html = """
    <table>
      <tr><td>Item</td><td>Amount</td></tr>
      <tr><td>Revenue</td><td>$</td><td>11,866.1</td></tr>
      <tr><td>Cost</td><td>$</td><td>5,432.0</td></tr>
    </table>
    """
    md = _convert_table(html)
    assert md is not None
    assert "$11,866.1" in md
    assert "$5,432.0" in md

  def test_ixbrl_tags_in_cells(self):
    html = """
    <table>
      <tr><td>Metric</td><td>Value</td></tr>
      <tr>
        <td>Revenue</td>
        <td><ix:nonFraction name="us-gaap:Revenue" contextRef="c1">11,866</ix:nonFraction></td>
      </tr>
    </table>
    """
    md = _convert_table(html)
    assert md is not None
    assert "11,866" in md
    assert "ix:" not in md

  def test_colspan_in_header(self):
    html = """
    <table>
      <tr><td></td><td colspan="2">Year ended Dec 31</td></tr>
      <tr><td></td><td>2025</td><td>2024</td></tr>
      <tr><td>Revenue</td><td>100</td><td>90</td></tr>
    </table>
    """
    md = _convert_table(html)
    assert md is not None
    assert "Year ended Dec 31" in md


@pytest.mark.unit
class TestHtmlTablesToMarkdown:
  def test_replaces_data_table(self):
    html = """
    <p>Financial results:</p>
    <table>
      <tr><td>Metric</td><td>Value</td></tr>
      <tr><td>Revenue</td><td>100</td></tr>
      <tr><td>Cost</td><td>50</td></tr>
    </table>
    <p>End of results.</p>
    """
    result = html_tables_to_markdown(html)
    assert "<table" not in result
    assert "| Metric | Value |" in result
    assert "<p>Financial results:</p>" in result
    assert "<p>End of results.</p>" in result

  def test_preserves_layout_table(self):
    html = """
    <table>
      <tr><td>Single column layout text</td></tr>
      <tr><td>More layout text</td></tr>
    </table>
    """
    result = html_tables_to_markdown(html)
    # Layout table kept as HTML
    assert "<table" in result

  def test_multiple_tables(self):
    html = """
    <table>
      <tr><td>A</td><td>B</td></tr>
      <tr><td>1</td><td>2</td></tr>
      <tr><td>3</td><td>4</td></tr>
    </table>
    <p>Between tables</p>
    <table>
      <tr><td>X</td><td>Y</td></tr>
      <tr><td>5</td><td>6</td></tr>
      <tr><td>7</td><td>8</td></tr>
    </table>
    """
    result = html_tables_to_markdown(html)
    assert "| A | B |" in result
    assert "| X | Y |" in result
    assert "<p>Between tables</p>" in result

  def test_no_tables(self):
    html = "<p>No tables here</p>"
    assert html_tables_to_markdown(html) == html

  def test_nested_tables(self):
    html = """
    <table>
      <tr><td>Outer A</td><td>Outer B</td></tr>
      <tr><td>
        <table><tr><td>Inner</td></tr></table>
      </td><td>Outer C</td></tr>
      <tr><td>Outer D</td><td>Outer E</td></tr>
    </table>
    """
    # Should not crash; nested tables are handled by depth tracking
    result = html_tables_to_markdown(html)
    assert isinstance(result, str)
