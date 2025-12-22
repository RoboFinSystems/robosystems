"""Tests for HTML parsing utilities."""

import os
import shutil
import tempfile

import pytest

from robosystems.utils.html_parser import (
  extract_structured_content,
  save_structured_content,
)


class TestExtractStructuredContent:
  """Test suite for extract_structured_content function."""

  def test_basic_html_extraction(self):
    """Test extraction of basic HTML content."""
    html = """
        <html>
            <body>
                <h1>Main Title</h1>
                <p>This is a paragraph.</p>
            </body>
        </html>
        """
    result = extract_structured_content(html)
    assert "# Main Title" in result
    assert "This is a paragraph." in result

  def test_multiple_heading_levels(self):
    """Test extraction of various heading levels."""
    html = """
        <html>
            <body>
                <h1>Level 1</h1>
                <h2>Level 2</h2>
                <h3>Level 3</h3>
                <h4>Level 4</h4>
                <h5>Level 5</h5>
                <h6>Level 6</h6>
            </body>
        </html>
        """
    result = extract_structured_content(html)
    assert "# Level 1" in result
    assert "## Level 2" in result
    assert "### Level 3" in result
    assert "#### Level 4" in result
    assert "##### Level 5" in result
    assert "###### Level 6" in result

  def test_table_extraction(self):
    """Test extraction of table data."""
    html = """
        <html>
            <body>
                <table>
                    <tr><th>Name</th><th>Value</th></tr>
                    <tr><td>Revenue</td><td>1000</td></tr>
                    <tr><td>Expenses</td><td>500</td></tr>
                </table>
            </body>
        </html>
        """
    result = extract_structured_content(html)
    assert "--- TABLE START ---" in result
    assert "Name | Value" in result
    assert "Revenue | 1000" in result
    assert "Expenses | 500" in result
    assert "--- TABLE END ---" in result

  def test_complex_table_with_headers(self):
    """Test extraction of complex table with multiple headers."""
    html = """
        <html>
            <body>
                <table>
                    <tr>
                        <th>Company</th>
                        <th>Q1</th>
                        <th>Q2</th>
                        <th>Q3</th>
                    </tr>
                    <tr>
                        <td>RoboSystems</td>
                        <td>100</td>
                        <td>200</td>
                        <td>300</td>
                    </tr>
                </table>
            </body>
        </html>
        """
    result = extract_structured_content(html)
    assert "Company | Q1 | Q2 | Q3" in result
    assert "RoboSystems | 100 | 200 | 300" in result

  def test_paragraph_extraction(self):
    """Test extraction of paragraph content."""
    html = """
        <html>
            <body>
                <p>First paragraph with text.</p>
                <p>Second paragraph with more content.</p>
                <p>   Paragraph with spaces.   </p>
            </body>
        </html>
        """
    result = extract_structured_content(html)
    assert "First paragraph with text." in result
    assert "Second paragraph with more content." in result
    assert "Paragraph with spaces." in result

  def test_div_extraction(self):
    """Test extraction of div content."""
    html = """
        <html>
            <body>
                <div>This is a long div content that should be included in the extraction.</div>
                <div>Short div</div>
                <div><p>Div with paragraph</p></div>
            </body>
        </html>
        """
    result = extract_structured_content(html)
    # Long div content should be included
    assert "This is a long div content" in result
    # Short div (< 20 chars) should not be included
    assert "Short div" not in result
    # Div with nested paragraph should have paragraph content only
    assert "Div with paragraph" in result

  def test_span_extraction(self):
    """Test extraction of span content."""
    html = """
        <html>
            <body>
                <span>This is a long span content that should be extracted properly.</span>
                <p><span>Span in paragraph</span></p>
                <h1><span>Span in heading</span></h1>
            </body>
        </html>
        """
    result = extract_structured_content(html)
    # Standalone long span should be included
    assert "This is a long span content" in result
    # Span in paragraph should be part of paragraph
    assert "Span in paragraph" in result
    # Span in heading should be part of heading
    assert "# Span in heading" in result

  def test_mixed_content_extraction(self):
    """Test extraction of mixed HTML content."""
    html = """
        <html>
            <body>
                <h1>Financial Report</h1>
                <p>Summary of financial data for Q4 2024.</p>
                <h2>Revenue</h2>
                <table>
                    <tr><th>Month</th><th>Amount</th></tr>
                    <tr><td>October</td><td>50000</td></tr>
                </table>
                <div>Additional notes about the financial performance this quarter.</div>
            </body>
        </html>
        """
    result = extract_structured_content(html)
    assert "# Financial Report" in result
    assert "## Revenue" in result
    assert "Month | Amount" in result
    assert "October | 50000" in result
    assert "Additional notes" in result

  def test_no_body_tag_error(self):
    """Test error handling when no body tag is present."""
    html = "<html><div>No body tag</div></html>"
    with pytest.raises(
      ValueError, match="Could not extract content from HTML document"
    ):
      extract_structured_content(html)

  def test_empty_html_document(self):
    """Test handling of empty HTML document."""
    html = "<html><body></body></html>"
    result = extract_structured_content(html)
    assert result == ""  # Empty result for empty body

  def test_nested_elements(self):
    """Test extraction of deeply nested elements."""
    html = """
        <html>
            <body>
                <div>
                    <div>
                        <h2>Nested Heading</h2>
                        <p>Nested paragraph content.</p>
                    </div>
                </div>
            </body>
        </html>
        """
    result = extract_structured_content(html)
    assert "## Nested Heading" in result
    assert "Nested paragraph content." in result

  def test_table_without_headers(self):
    """Test extraction of table without header row."""
    html = """
        <html>
            <body>
                <table>
                    <tr><td>Data 1</td><td>Data 2</td></tr>
                    <tr><td>Data 3</td><td>Data 4</td></tr>
                </table>
            </body>
        </html>
        """
    result = extract_structured_content(html)
    assert "--- TABLE START ---" in result
    assert "Data 1 | Data 2" in result
    assert "Data 3 | Data 4" in result
    assert "--- TABLE END ---" in result

  def test_multiline_cell_content(self):
    """Test handling of multiline content in table cells."""
    html = """
        <html>
            <body>
                <table>
                    <tr><td>Line 1\nLine 2</td><td>Single line</td></tr>
                </table>
            </body>
        </html>
        """
    result = extract_structured_content(html)
    # Newlines in cells should be replaced with spaces
    assert "Line 1 Line 2 | Single line" in result

  def test_whitespace_removal(self):
    """Test removal of excessive blank lines."""
    html = """
        <html>
            <body>
                <h1>Title</h1>
                <p></p>
                <p>  </p>
                <p>Content</p>
            </body>
        </html>
        """
    result = extract_structured_content(html)
    lines = result.split("\n")
    # Check no consecutive blank lines
    for i in range(len(lines) - 1):
      if lines[i] == "":
        assert lines[i + 1] != ""

  def test_heading_with_empty_text(self):
    """Test handling of headings with no text content."""
    html = """
        <html>
            <body>
                <h1></h1>
                <h2>   </h2>
                <h3>Valid Heading</h3>
            </body>
        </html>
        """
    result = extract_structured_content(html)
    assert "### Valid Heading" in result
    # Empty headings should not be included - only the valid one should be present
    lines = [line for line in result.split("\n") if line.strip()]
    assert len(lines) == 1
    assert lines[0] == "### Valid Heading"


class TestSaveStructuredContent:
  """Test suite for save_structured_content function."""

  def setup_method(self):
    """Set up test fixtures."""
    self.temp_dir = tempfile.mkdtemp()

  def teardown_method(self):
    """Clean up test fixtures."""
    if os.path.exists(self.temp_dir):
      shutil.rmtree(self.temp_dir)

  def test_basic_save_functionality(self):
    """Test basic save functionality with simple HTML."""
    html = """
        <html>
            <body>
                <h1>Test Document</h1>
                <p>Test content.</p>
            </body>
        </html>
        """
    url = "https://example.com/document.html"

    output_path, structured_text, plain_text = save_structured_content(
      html, url, self.temp_dir
    )

    assert output_path == os.path.join(self.temp_dir, "document.txt")
    assert os.path.exists(output_path)
    assert "# Test Document" in structured_text
    assert "Test content." in structured_text
    assert "Test Document" in plain_text
    assert "Test content." in plain_text

  def test_url_to_filename_conversion(self):
    """Test conversion of various URL formats to filenames."""
    html = "<html><body><p>Content</p></body></html>"

    # URL with .html extension
    url1 = "https://example.com/page.html"
    path1, _, _ = save_structured_content(html, url1, self.temp_dir)
    assert path1.endswith("page.txt")

    # URL without extension
    url2 = "https://example.com/page"
    path2, _, _ = save_structured_content(html, url2, self.temp_dir)
    assert path2.endswith("page.txt")

    # URL with query parameters
    url3 = "https://example.com/page.aspx?id=123"
    path3, _, _ = save_structured_content(html, url3, self.temp_dir)
    assert path3.endswith("page.txt")

  def test_url_edge_cases(self):
    """Test edge cases in URL handling."""
    html = "<html><body><p>Content</p></body></html>"

    # Empty URL
    path1, _, _ = save_structured_content(html, "", self.temp_dir)
    assert path1.endswith("unknown.txt")

    # URL without slashes
    path2, _, _ = save_structured_content(html, "nopath", self.temp_dir)
    assert path2.endswith("unknown.txt")

    # None URL
    path3, _, _ = save_structured_content(html, None, self.temp_dir)
    assert path3.endswith("unknown.txt")

  def test_directory_creation(self):
    """Test automatic directory creation."""
    html = "<html><body><p>Content</p></body></html>"
    url = "https://example.com/test.html"

    # Use a non-existent subdirectory
    output_dir = os.path.join(self.temp_dir, "nested", "dir", "path")
    assert not os.path.exists(output_dir)

    output_path, _, _ = save_structured_content(html, url, output_dir)

    assert os.path.exists(output_dir)
    assert os.path.exists(output_path)

  def test_file_content_writing(self):
    """Test that structured content is correctly written to file."""
    html = """
        <html>
            <body>
                <h1>Title</h1>
                <table>
                    <tr><th>Col1</th><th>Col2</th></tr>
                    <tr><td>Val1</td><td>Val2</td></tr>
                </table>
            </body>
        </html>
        """
    url = "https://example.com/data.html"

    output_path, structured_text, _ = save_structured_content(html, url, self.temp_dir)

    # Read the file and verify content
    with open(output_path, encoding="utf-8") as f:
      file_content = f.read()

    assert file_content == structured_text
    assert "# Title" in file_content
    assert "Col1 | Col2" in file_content
    assert "Val1 | Val2" in file_content

  def test_unicode_content_handling(self):
    """Test handling of unicode characters in content."""
    html = """
        <html>
            <body>
                <h1>Unicode Test 测试 🎉</h1>
                <p>Special chars: €£¥ñ</p>
            </body>
        </html>
        """
    url = "https://example.com/unicode.html"

    output_path, structured_text, plain_text = save_structured_content(
      html, url, self.temp_dir
    )

    # Verify unicode is preserved
    assert "测试" in structured_text
    assert "🎉" in structured_text
    assert "€£¥ñ" in structured_text

    # Verify file can be read back with unicode
    with open(output_path, encoding="utf-8") as f:
      file_content = f.read()
    assert "测试" in file_content

  def test_plain_text_extraction(self):
    """Test plain text extraction alongside structured content."""
    html = """
        <html>
            <body>
                <h1>Heading</h1>
                <p>Paragraph text.</p>
                <table>
                    <tr><td>Cell 1</td><td>Cell 2</td></tr>
                </table>
            </body>
        </html>
        """
    url = "https://example.com/test.html"

    _, structured_text, plain_text = save_structured_content(html, url, self.temp_dir)

    # Plain text should contain all text without formatting
    assert "Heading" in plain_text
    assert "Paragraph text." in plain_text
    assert "Cell 1" in plain_text
    assert "Cell 2" in plain_text

    # Structured text should have markdown formatting
    assert "# Heading" in structured_text
    assert "--- TABLE START ---" in structured_text

  def test_no_body_tag_in_save(self):
    """Test save function with HTML missing body tag."""
    html = "<html><div>No body</div></html>"
    url = "https://example.com/nobdy.html"

    with pytest.raises(
      ValueError, match="Could not extract content from HTML document"
    ):
      save_structured_content(html, url, self.temp_dir)

  def test_overwrite_existing_file(self):
    """Test that existing files are overwritten."""
    html1 = "<html><body><p>First version</p></body></html>"
    html2 = "<html><body><p>Second version</p></body></html>"
    url = "https://example.com/same.html"

    # Write first version
    path1, _, _ = save_structured_content(html1, url, self.temp_dir)
    with open(path1) as f:
      content1 = f.read()
    assert "First version" in content1

    # Write second version to same location
    path2, _, _ = save_structured_content(html2, url, self.temp_dir)
    assert path1 == path2
    with open(path2) as f:
      content2 = f.read()
    assert "Second version" in content2
    assert "First version" not in content2
