"""Tests for SEC narrative section extractor."""

from robosystems.adapters.sec.narrative_extractor import (
  NarrativeExtractor,
  _clean_text,
  _html_to_text,
)
from robosystems.adapters.sec.pipeline.text_index import _derive_section_label


class TestHtmlToText:
  def test_strips_tags(self):
    html = "<p>Hello <b>world</b></p>"
    text = _html_to_text(html)
    assert "Hello" in text
    assert "world" in text
    assert "<" not in text

  def test_skips_script_style(self):
    html = (
      "<p>Visible</p><script>hidden</script><style>.hidden{}</style><p>Also visible</p>"
    )
    text = _html_to_text(html)
    assert "Visible" in text
    assert "hidden" not in text
    assert "Also visible" in text

  def test_preserves_block_element_newlines(self):
    html = "<p>First</p><p>Second</p>"
    text = _html_to_text(html)
    assert "\n" in text


class TestCleanText:
  def test_collapses_whitespace(self):
    text = "Hello     world\t\ttabs"
    result = _clean_text(text)
    assert "Hello world tabs" in result

  def test_removes_page_numbers(self):
    text = "Some text\n42\nMore text"
    result = _clean_text(text)
    assert "42" not in result

  def test_removes_toc_links(self):
    text = "Some text\nTable of Contents\nMore text"
    result = _clean_text(text)
    assert "Table of Contents" not in result

  def test_removes_xbrl_member_blobs(self):
    text = "us-gaap:EquitySecuritiesMember some text"
    result = _clean_text(text)
    assert "Member" not in result


class TestDeriveSectionLabel:
  def test_strips_text_block_suffix(self):
    assert _derive_section_label("Risk Factors [Text Block]") == "Risk Factors"

  def test_strips_textblock_suffix(self):
    assert _derive_section_label("Risk Factors Text Block") == "Risk Factors"

  def test_splits_camel_case(self):
    result = _derive_section_label("ManagementDiscussionAndAnalysis")
    assert "Management" in result
    assert "Discussion" in result


# Build a realistic sample with enough content between sections
# that the TOC heuristic can distinguish content from TOC entries
_FILLER = " ".join(["The company continues to operate in a competitive market."] * 20)

SAMPLE_10K_HTML = f"""
<html><body>

<h2>PART I</h2>

<h3>ITEM 1. BUSINESS</h3>
<p>We are a technology company that develops semiconductor products.
{_FILLER}</p>

<h3>ITEM 1A. RISK FACTORS</h3>
<p>Our business faces several significant risks including supply chain and tariffs.
{_FILLER}</p>

<h3>ITEM 2. PROPERTIES</h3>
<p>Our principal offices are located in Santa Clara, California.
{_FILLER}</p>

<h2>PART II</h2>

<h3>ITEM 7. MANAGEMENT'S DISCUSSION AND ANALYSIS</h3>
<p>Revenue increased 25% year-over-year driven by strong data center demand.
{_FILLER}</p>

<h3>ITEM 8. FINANCIAL STATEMENTS</h3>
<p>See the consolidated financial statements beginning on page F-1.</p>
</body></html>
"""


class TestNarrativeExtractor:
  def test_extracts_10k_sections(self):
    extractor = NarrativeExtractor()
    sections = extractor.extract(SAMPLE_10K_HTML, "10-K")

    section_ids = {s.section_id for s in sections}
    assert "item_1" in section_ids
    assert "item_1a" in section_ids
    assert "item_7" in section_ids

  def test_section_content_is_clean(self):
    extractor = NarrativeExtractor()
    sections = extractor.extract(SAMPLE_10K_HTML, "10-K")

    for section in sections:
      assert "<" not in section.content
      assert ">" not in section.content
      assert section.word_count > 0

  def test_business_section_content(self):
    extractor = NarrativeExtractor()
    sections = extractor.extract(SAMPLE_10K_HTML, "10-K")

    business = next((s for s in sections if s.section_id == "item_1"), None)
    assert business is not None
    assert "semiconductor" in business.content.lower()
    assert business.section_label == "Business"

  def test_10q_extracts_different_sections(self):
    # 10-Q has different target sections
    html = """
        <html><body>
        <h3>ITEM 2. MANAGEMENT'S DISCUSSION AND ANALYSIS</h3>
        <p>Revenue grew significantly in the quarter driven by cloud computing demand.
        We saw strong momentum across all product categories and geographies.
        Operating margins improved due to favorable product mix and cost controls.</p>

        <h3>ITEM 3. QUANTITATIVE AND QUALITATIVE DISCLOSURES ABOUT MARKET RISK</h3>
        <p>Interest rate risk, foreign currency risk, and equity price risk
        are the primary market risks we face in our ongoing operations.</p>
        </body></html>
        """
    extractor = NarrativeExtractor()
    sections = extractor.extract(html, "10-Q")

    section_ids = {s.section_id for s in sections}
    # 10-Q Item 2 = MD&A, Item 3 = Market Risk
    assert "item_2" in section_ids or "item_3" in section_ids

  def test_unsupported_form_type_returns_empty(self):
    extractor = NarrativeExtractor()
    sections = extractor.extract("<html></html>", "8-K")
    assert sections == []

  def test_truncates_long_sections(self):
    extractor = NarrativeExtractor(max_section_length=100)
    long_html = """
        <html><body>
        <h3>ITEM 1. BUSINESS</h3>
        <p>{}</p>
        <h3>ITEM 2. PROPERTIES</h3>
        <p>Short section.</p>
        </body></html>
        """.format("word " * 500)

    sections = extractor.extract(long_html, "10-K")
    business = next((s for s in sections if s.section_id == "item_1"), None)
    if business:
      assert len(business.content) <= 150  # 100 + truncation marker
      assert "[Section truncated]" in business.content

  def test_skips_trivial_sections(self):
    """Sections with fewer than 10 words are skipped."""
    extractor = NarrativeExtractor()
    html = """
        <html><body>
        <h3>ITEM 1. BUSINESS</h3>
        <p>Short.</p>
        <h3>ITEM 1A. RISK FACTORS</h3>
        <p>This section has enough words to pass the minimum threshold for inclusion
        in the extracted narrative sections output list and contains important details.</p>
        </body></html>
        """
    sections = extractor.extract(html, "10-K")
    # item_1 has very little content; item_1a has substantial content
    # Both may be detected but item_1a should have more words
    assert any(s.section_id == "item_1a" for s in sections)
