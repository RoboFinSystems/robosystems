"""Tests for markdown parser — heading-based sectioning and frontmatter."""

from robosystems.operations.search.markdown_parser import (
  MAX_SECTION_CHARS,
  MIN_SECTION_WORDS,
  content_hash,
  parse_document,
  parse_frontmatter,
  section_markdown,
)


class TestParseFrontmatter:
  def test_valid_frontmatter(self):
    content = "---\ntitle: My Doc\ntags: foo, bar\n---\n# Heading\nBody"
    metadata, remaining = parse_frontmatter(content)
    assert metadata["title"] == "My Doc"
    assert metadata["tags"] == "foo, bar"
    assert remaining.startswith("# Heading")

  def test_no_frontmatter(self):
    content = "# Just a heading\nSome content"
    metadata, remaining = parse_frontmatter(content)
    assert metadata == {}
    assert remaining == content

  def test_incomplete_frontmatter(self):
    content = "---\ntitle: Missing End"
    metadata, remaining = parse_frontmatter(content)
    assert metadata == {}
    assert remaining == content

  def test_empty_frontmatter(self):
    content = "---\n---\n# Content"
    metadata, remaining = parse_frontmatter(content)
    # yaml.safe_load of empty string returns None
    assert metadata == {}

  def test_frontmatter_with_list(self):
    content = "---\ntitle: Doc\ntags:\n  - alpha\n  - beta\n---\nBody"
    metadata, remaining = parse_frontmatter(content)
    assert metadata["tags"] == ["alpha", "beta"]


class TestSectionMarkdown:
  def test_basic_heading_split(self):
    filler1 = " ".join(["word"] * 25)
    filler2 = " ".join(["text"] * 25)
    content = f"# Section One\n{filler1}\n\n# Section Two\n{filler2}"
    sections = section_markdown(content)
    assert len(sections) == 2
    assert sections[0]["section_id"] == "section-one"
    assert sections[0]["section_label"] == "Section One"
    assert sections[1]["section_id"] == "section-two"

  def test_nested_headings(self):
    filler1 = " ".join(["word"] * 25)
    filler2 = " ".join(["text"] * 25)
    content = f"# Top\n{filler1}\n\n## Sub\n{filler2}"
    sections = section_markdown(content)
    assert len(sections) == 2
    assert sections[0]["heading_depth"] == 1
    assert sections[1]["heading_depth"] == 2

  def test_no_headings_single_section(self):
    content = "Just plain text without any headings, but with enough words."
    sections = section_markdown(content)
    assert len(sections) == 1
    assert sections[0]["section_id"] == "full-document"

  def test_empty_content(self):
    sections = section_markdown("")
    assert sections == []

  def test_whitespace_only(self):
    sections = section_markdown("   \n\n  ")
    assert sections == []

  def test_small_sections_merged(self):
    # Create a small section followed by a large one
    content = "# Tiny\nShort.\n\n# Big Section\n" + " ".join(
      ["word"] * (MIN_SECTION_WORDS + 5)
    )
    sections = section_markdown(content)
    # Small section should merge
    assert len(sections) <= 2

  def test_large_section_truncated(self):
    huge_content = "# Huge\n" + "x " * (MAX_SECTION_CHARS + 100)
    sections = section_markdown(huge_content)
    assert len(sections[0]["content"]) <= MAX_SECTION_CHARS

  def test_section_id_special_chars(self):
    content = "# Revenue & Expenses (Q4)!\n" + " ".join(["word"] * 25)
    sections = section_markdown(content)
    assert sections[0]["section_id"] == "revenue-expenses-q4"

  def test_preamble_included_if_long_enough(self):
    preamble = " ".join(["preamble"] * (MIN_SECTION_WORDS + 1))
    filler = " ".join(["word"] * 25)
    content = f"{preamble}\n\n# First Heading\n{filler}"
    sections = section_markdown(content)
    assert sections[0]["section_id"] == "preamble"

  def test_preamble_skipped_if_short(self):
    content = "Short intro.\n\n# Real Section\n" + " ".join(["word"] * 25)
    sections = section_markdown(content)
    assert sections[0]["section_id"] != "preamble"

  def test_heading_content_included(self):
    content = "# My Section\nBody text here."
    sections = section_markdown(content)
    assert "# My Section" in sections[0]["content"]
    assert "Body text here." in sections[0]["content"]


class TestParseDocument:
  def test_full_document(self):
    content = "---\ntitle: Test Doc\ntags: a, b\nfolder: policies\n---\n# Overview\nContent here with enough words to be a section on its own."
    metadata, sections = parse_document(content, "Fallback Title")
    assert metadata["title"] == "Test Doc"
    assert metadata["tags"] == ["a", "b"]
    assert metadata["folder"] == "policies"
    assert len(sections) >= 1
    assert sections[0]["section_label"] == "Overview"

  def test_title_fallback(self):
    content = "# Just Content\nWith enough words to pass minimum."
    metadata, sections = parse_document(content, "My Fallback")
    assert metadata["title"] == "My Fallback"

  def test_frontmatter_title_overrides(self):
    content = "---\ntitle: FM Title\n---\n# Heading\nBody text."
    metadata, sections = parse_document(content, "Fallback")
    assert metadata["title"] == "FM Title"

  def test_comma_separated_tags_parsed(self):
    content = "---\ntags: foo, bar, baz\n---\nBody."
    metadata, _ = parse_document(content, "Doc")
    assert metadata["tags"] == ["foo", "bar", "baz"]


class TestContentHash:
  def test_deterministic(self):
    assert content_hash("hello") == content_hash("hello")

  def test_different_content_different_hash(self):
    assert content_hash("hello") != content_hash("world")

  def test_length(self):
    assert len(content_hash("test")) == 12
