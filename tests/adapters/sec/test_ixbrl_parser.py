"""Tests for iXBRL disclosure parser.

Covers:
- TextBlock extraction from ix:nonNumeric elements
- Continuation chain resolution
- Element extraction from nested ix:nonFraction tags
- Label derivation from element qnames
- Filtering (DEI/ECD skip, min word count, dedup)
"""

import pytest

from robosystems.adapters.sec.ixbrl_parser import (
  _extract_elements_from_block,
  _label_from_element_name,
  _strip_html,
  iXBRLParser,
)


@pytest.mark.unit
class TestLabelFromElementName:
  """Tests for _label_from_element_name."""

  def test_strips_namespace_and_textblock_suffix(self):
    assert (
      _label_from_element_name("us-gaap:GoodwillDisclosureTextBlock")
      == "Goodwill Disclosure"
    )

  def test_strips_table_textblock_suffix(self):
    assert (
      _label_from_element_name("us-gaap:DebtSecuritiesAvailableForSaleTableTextBlock")
      == "Debt Securities Available For Sale Table"
    )

  def test_splits_camel_case(self):
    assert (
      _label_from_element_name("us-gaap:RevenueFromContractWithCustomerPolicyTextBlock")
      == "Revenue From Contract With Customer Policy"
    )

  def test_handles_company_namespace(self):
    assert (
      _label_from_element_name("nvda:NatureOfOperationsPolicyTextBlock")
      == "Nature Of Operations Policy"
    )

  def test_handles_no_namespace(self):
    assert (
      _label_from_element_name("GoodwillDisclosureTextBlock") == "Goodwill Disclosure"
    )

  def test_handles_no_suffix(self):
    assert _label_from_element_name("us-gaap:Revenue") == "Revenue"


@pytest.mark.unit
class TestStripHtml:
  """Tests for _strip_html."""

  def test_strips_tags(self):
    assert "Hello world" in _strip_html("<p>Hello <b>world</b></p>")

  def test_removes_style_tags(self):
    result = _strip_html("<style>.hidden{}</style><p>Visible</p>")
    assert "hidden" not in result
    assert "Visible" in result

  def test_removes_script_tags(self):
    result = _strip_html("<script>alert('x')</script><p>Content</p>")
    assert "alert" not in result
    assert "Content" in result

  def test_normalizes_whitespace(self):
    result = _strip_html("<p>Hello</p>   <p>World</p>")
    assert "  " not in result

  def test_handles_html_entities(self):
    result = _strip_html("<p>A&amp;B&nbsp;C</p>")
    assert "A&B" in result

  def test_converts_tables_to_markdown(self):
    html = (
      "<table>"
      "<tr><th>Period</th><th>Revenue</th></tr>"
      "<tr><td>Q1</td><td>1000</td></tr>"
      "<tr><td>Q2</td><td>2000</td></tr>"
      "</table>"
    )
    result = _strip_html(html)
    assert "Period" in result
    assert "Revenue" in result
    assert "1000" in result
    assert "|" in result


@pytest.mark.unit
class TestExtractElementsFromBlock:
  """Tests for _extract_elements_from_block."""

  def test_extracts_nonFraction_elements(self):
    html = """
    <ix:nonFraction name="us-gaap:Goodwill" contextRef="c1">32431</ix:nonFraction>
    <ix:nonFraction name="us-gaap:GoodwillImpairmentLoss" contextRef="c2">0</ix:nonFraction>
    """
    elements = _extract_elements_from_block(html)
    assert "us-gaap:Goodwill" in elements
    assert "us-gaap:GoodwillImpairmentLoss" in elements

  def test_extracts_nonNumeric_non_textblock_elements(self):
    html = """
    <ix:nonNumeric name="us-gaap:FiscalPeriod" contextRef="c1">FY</ix:nonNumeric>
    """
    elements = _extract_elements_from_block(html)
    assert "us-gaap:FiscalPeriod" in elements

  def test_skips_textblock_elements(self):
    html = """
    <ix:nonNumeric name="us-gaap:GoodwillDisclosureTextBlock" contextRef="c1">text</ix:nonNumeric>
    """
    elements = _extract_elements_from_block(html)
    assert "us-gaap:GoodwillDisclosureTextBlock" not in elements

  def test_skips_dei_elements(self):
    html = """
    <ix:nonNumeric name="dei:EntityRegistrantName" contextRef="c1">NVIDIA</ix:nonNumeric>
    """
    elements = _extract_elements_from_block(html)
    assert len(elements) == 0

  def test_deduplicates_elements(self):
    html = """
    <ix:nonFraction name="us-gaap:Goodwill" contextRef="c1">100</ix:nonFraction>
    <ix:nonFraction name="us-gaap:Goodwill" contextRef="c2">200</ix:nonFraction>
    """
    elements = _extract_elements_from_block(html)
    assert elements.count("us-gaap:Goodwill") == 1

  def test_returns_sorted(self):
    html = """
    <ix:nonFraction name="us-gaap:Revenue" contextRef="c1">100</ix:nonFraction>
    <ix:nonFraction name="us-gaap:Assets" contextRef="c2">200</ix:nonFraction>
    """
    elements = _extract_elements_from_block(html)
    assert elements == sorted(elements)

  def test_empty_block(self):
    assert _extract_elements_from_block("<p>No XBRL tags here</p>") == []


@pytest.mark.unit
class TestiXBRLParser:
  """Tests for iXBRLParser.parse()."""

  def test_extracts_simple_textblock(self):
    html = """
    <html><body>
    <ix:nonNumeric contextRef="c-1" name="us-gaap:GoodwillDisclosureTextBlock" id="f-1">
      <p>The following table summarizes goodwill by segment.
      <ix:nonFraction name="us-gaap:Goodwill" contextRef="c1">32431</ix:nonFraction>
      million in total goodwill was recorded during the period.
      Additional goodwill details are provided below with impairment analysis.</p>
    </ix:nonNumeric>
    </body></html>
    """
    parser = iXBRLParser()
    sections = parser.parse(html)

    assert len(sections) == 1
    assert sections[0].section_id == "us-gaap:GoodwillDisclosureTextBlock"
    assert sections[0].section_label == "Goodwill Disclosure"
    assert "goodwill" in sections[0].content.lower()
    assert "us-gaap:Goodwill" in sections[0].xbrl_elements
    assert sections[0].element_count == 1

  def test_resolves_continuation_chain(self):
    html = """
    <html><body>
    <ix:nonNumeric contextRef="c-1" name="us-gaap:DebtDisclosureTextBlock" id="f-1" continuedAt="f-1-cont">
      Debt Overview
    </ix:nonNumeric>
    <ix:continuation id="f-1-cont">
      <p>The company has outstanding debt of
      <ix:nonFraction name="us-gaap:DebtInstrumentCarryingAmount" contextRef="c1">5000</ix:nonFraction>
      million. The weighted average interest rate is
      <ix:nonFraction name="us-gaap:DebtInstrumentInterestRateStatedPercentage" contextRef="c2">3.5</ix:nonFraction>
      percent. These debt instruments mature over the next ten years with various covenants and restrictions.</p>
    </ix:continuation>
    </body></html>
    """
    parser = iXBRLParser()
    sections = parser.parse(html)

    assert len(sections) == 1
    s = sections[0]
    assert s.section_id == "us-gaap:DebtDisclosureTextBlock"
    assert "5000" in s.content
    assert "us-gaap:DebtInstrumentCarryingAmount" in s.xbrl_elements
    assert "us-gaap:DebtInstrumentInterestRateStatedPercentage" in s.xbrl_elements
    assert s.element_count == 2

  def test_skips_dei_textblocks(self):
    html = """
    <html><body>
    <ix:nonNumeric contextRef="c-1" name="dei:DocumentsIncorporatedByReferenceTextBlock" id="f-1">
      Some reference text that is long enough to pass word count filters
      and contains enough words to normally be included as a section.
    </ix:nonNumeric>
    </body></html>
    """
    parser = iXBRLParser()
    sections = parser.parse(html)
    assert len(sections) == 0

  def test_skips_ecd_textblocks(self):
    html = """
    <html><body>
    <ix:nonNumeric contextRef="c-1" name="ecd:MtrlTermsOfTrdArrTextBlock" id="f-1">
      Executive compensation disclosure text that is long enough to pass
      the minimum word count filter but should be skipped anyway.
    </ix:nonNumeric>
    </body></html>
    """
    parser = iXBRLParser()
    sections = parser.parse(html)
    assert len(sections) == 0

  def test_skips_trivial_sections(self):
    html = """
    <html><body>
    <ix:nonNumeric contextRef="c-1" name="us-gaap:ShortTextBlock" id="f-1">
      Too short.
    </ix:nonNumeric>
    </body></html>
    """
    parser = iXBRLParser()
    sections = parser.parse(html)
    assert len(sections) == 0

  def test_deduplicates_same_element(self):
    html = """
    <html><body>
    <ix:nonNumeric contextRef="c-1" name="us-gaap:RevenueTextBlock" id="f-1">
      First occurrence with enough words to pass the minimum word count
      filter that requires at least twenty words in the content section.
    </ix:nonNumeric>
    <ix:nonNumeric contextRef="c-2" name="us-gaap:RevenueTextBlock" id="f-2">
      Second occurrence also with enough words to pass the minimum word
      count filter but should be deduplicated by the parser logic.
    </ix:nonNumeric>
    </body></html>
    """
    parser = iXBRLParser()
    sections = parser.parse(html)
    assert len(sections) == 1

  def test_truncates_long_sections(self):
    long_text = "word " * 20000  # ~100k chars
    html = f"""
    <html><body>
    <ix:nonNumeric contextRef="c-1" name="us-gaap:LongDisclosureTextBlock" id="f-1">
      {long_text}
    </ix:nonNumeric>
    </body></html>
    """
    parser = iXBRLParser(max_section_length=100)
    sections = parser.parse(html)
    assert len(sections) == 1
    assert sections[0].content.endswith("[Section truncated]")

  def test_multiple_sections(self):
    filler = " ".join(["disclosure"] * 25)
    html = f"""
    <html><body>
    <ix:nonNumeric contextRef="c-1" name="us-gaap:GoodwillDisclosureTextBlock" id="f-1">
      <p>{filler}</p>
    </ix:nonNumeric>
    <ix:nonNumeric contextRef="c-1" name="us-gaap:DebtDisclosureTextBlock" id="f-2">
      <p>{filler}</p>
    </ix:nonNumeric>
    <ix:nonNumeric contextRef="c-1" name="us-gaap:IncomeTaxDisclosureTextBlock" id="f-3">
      <p>{filler}</p>
    </ix:nonNumeric>
    </body></html>
    """
    parser = iXBRLParser()
    sections = parser.parse(html)
    assert len(sections) == 3
    ids = {s.section_id for s in sections}
    assert "us-gaap:GoodwillDisclosureTextBlock" in ids
    assert "us-gaap:DebtDisclosureTextBlock" in ids
    assert "us-gaap:IncomeTaxDisclosureTextBlock" in ids

  def test_continuation_chain_with_cycle_protection(self):
    """Ensure visited set prevents infinite loops from malformed data."""
    html = """
    <html><body>
    <ix:nonNumeric contextRef="c-1" name="us-gaap:TestTextBlock" id="f-1" continuedAt="f-1-a">
      Start content with enough words to pass the minimum word count filter easily.
    </ix:nonNumeric>
    <ix:continuation id="f-1-a" continuedAt="f-1-a">
      Continuation that references itself with circular chain which should be handled gracefully.
    </ix:continuation>
    </body></html>
    """
    parser = iXBRLParser()
    sections = parser.parse(html)
    # Should not hang — visited set breaks the cycle
    assert len(sections) == 1

  def test_resolves_multi_hop_continuation_chain(self):
    """A note that spans pages is a chain: nonNumeric → continuation →
    continuation → …, each link pointing to the next via its own
    ``continuedAt`` attribute. Every link's text and elements must land in
    the section. Until 2026-09-03 the parser looked for the pointer inside
    the continuation's content, so every chain stopped after one hop and
    every multi-page note lost its tail (3M FY2024 Note 6 lost the PFAS
    exit-actions paragraph; Note 19 kept 570 of 129,803 characters).
    """
    html = """
    <html><body>
    <ix:nonNumeric contextRef="c-1" name="us-gaap:RestructuringAndRelatedActivitiesDisclosureTextBlock" id="f-1" continuedAt="f-1-a">
      <p>Restructuring overview with enough words to clear the minimum word count filter for a section.</p>
    </ix:nonNumeric>
    <p>Page footer text that is not part of the note.</p>
    <ix:continuation id="f-1-a" continuedAt="f-1-b">
      <p>SECOND HOP: charges of
      <ix:nonFraction name="us-gaap:RestructuringCharges" contextRef="c1">300</ix:nonFraction>
      million were recorded.</p>
    </ix:continuation>
    <ix:continuation id="f-1-b" continuedAt="f-1-c">
      <p>THIRD HOP: the reserve balance was
      <ix:nonFraction name="us-gaap:RestructuringReserve" contextRef="c2">120</ix:nonFraction>
      million.</p>
    </ix:continuation>
    <ix:continuation id="f-1-c">
      <p>FOURTH HOP: PFAS Exit Actions paragraph with
      <ix:nonFraction name="us-gaap:BusinessExitCosts1" contextRef="c3">45</ix:nonFraction>
      million.</p>
    </ix:continuation>
    </body></html>
    """
    sections = iXBRLParser().parse(html)

    assert len(sections) == 1
    s = sections[0]
    for marker in ("SECOND HOP", "THIRD HOP", "FOURTH HOP", "PFAS Exit Actions"):
      assert marker in s.content
    assert "Page footer" not in s.content
    assert s.xbrl_elements == [
      "us-gaap:BusinessExitCosts1",
      "us-gaap:RestructuringCharges",
      "us-gaap:RestructuringReserve",
    ]

  def test_chain_pointer_is_read_from_the_tag_not_the_content(self):
    """A continuation may wrap an element that itself continues elsewhere.
    That nested pointer belongs to the nested element's chain, not to the
    enclosing note: following it would splice another note's text into
    this one. The chain ends where the continuation's own tag says it ends.
    """
    html = """
    <html><body>
    <ix:nonNumeric contextRef="c-1" name="us-gaap:DebtDisclosureTextBlock" id="f-1" continuedAt="f-1-a">
      <p>Debt overview with enough words to clear the minimum word count filter for a section.</p>
    </ix:nonNumeric>
    <ix:continuation id="f-1-a">
      <p>Debt detail, and a nested policy that continues on its own:
      <ix:nonNumeric name="us-gaap:DebtPolicyTextBlock" contextRef="c-1" id="f-2" continuedAt="f-2-a">policy start</ix:nonNumeric>
      </p>
    </ix:continuation>
    <ix:continuation id="f-2-a">
      <p>OTHER NOTE TEXT that belongs to the nested policy, not to the debt note.</p>
    </ix:continuation>
    </body></html>
    """
    sections = iXBRLParser().parse(html)

    debt = next(
      s for s in sections if s.section_id == "us-gaap:DebtDisclosureTextBlock"
    )
    assert "Debt detail" in debt.content
    assert "OTHER NOTE TEXT" not in debt.content
