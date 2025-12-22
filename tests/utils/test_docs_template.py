"""Tests for documentation template utilities."""

import html
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from robosystems.utils.docs_template import (
  REDOC_DARK_THEME,
  _get_fallback_template,
  _get_redoc_fallback_template,
  _load_template_safely,
  _sanitize_input,
  generate_lbug_docs,
  generate_redoc_docs,
  generate_robosystems_docs,
  generate_robosystems_redoc,
  generate_swagger_docs,
)


class TestSanitizeInput:
  """Test suite for _sanitize_input function."""

  def test_basic_sanitization(self):
    """Test basic HTML entity escaping."""
    assert (
      _sanitize_input("<script>alert('xss')</script>")
      == "&lt;script&gt;alert(&#x27;xss&#x27;)&lt;/script&gt;"
    )
    assert _sanitize_input('"quotes"') == "&quot;quotes&quot;"
    assert _sanitize_input("'apostrophe'") == "&#x27;apostrophe&#x27;"
    assert _sanitize_input("&ampersand&") == "&amp;ampersand&amp;"

  def test_safe_strings(self):
    """Test that safe strings are not modified."""
    assert _sanitize_input("normal text") == "normal text"
    assert _sanitize_input("path/to/file.json") == "path/to/file.json"
    assert _sanitize_input("https://example.com") == "https://example.com"

  def test_empty_and_none(self):
    """Test handling of edge cases."""
    assert _sanitize_input("") == ""
    with pytest.raises(AttributeError):
      _sanitize_input(None)


class TestLoadTemplateSafely:
  """Test suite for _load_template_safely function."""

  def test_load_existing_file(self):
    """Test loading an existing template file."""
    mock_path = MagicMock(spec=Path)
    mock_path.exists.return_value = True
    mock_path.is_file.return_value = True
    mock_path.read_text.return_value = "template content"

    fallback_func = MagicMock(return_value="fallback content")

    result = _load_template_safely(mock_path, fallback_func)

    assert result == "template content"
    mock_path.read_text.assert_called_once_with(encoding="utf-8")
    fallback_func.assert_not_called()

  def test_file_not_exists(self):
    """Test fallback when file doesn't exist."""
    mock_path = MagicMock(spec=Path)
    mock_path.exists.return_value = False

    fallback_func = MagicMock(return_value="fallback content")

    result = _load_template_safely(mock_path, fallback_func)

    assert result == "fallback content"
    fallback_func.assert_called_once()

  def test_file_is_directory(self):
    """Test fallback when path is a directory."""
    mock_path = MagicMock(spec=Path)
    mock_path.exists.return_value = True
    mock_path.is_file.return_value = False

    fallback_func = MagicMock(return_value="fallback content")

    result = _load_template_safely(mock_path, fallback_func)

    assert result == "fallback content"
    fallback_func.assert_called_once()

  def test_io_error_handling(self):
    """Test handling of IO errors."""
    mock_path = MagicMock(spec=Path)
    mock_path.exists.return_value = True
    mock_path.is_file.return_value = True
    mock_path.read_text.side_effect = OSError("Permission denied")

    fallback_func = MagicMock(return_value="fallback content")

    with patch("robosystems.utils.docs_template.logger") as mock_logger:
      result = _load_template_safely(mock_path, fallback_func)

    assert result == "fallback content"
    fallback_func.assert_called_once()
    mock_logger.warning.assert_called_once()


class TestGenerateSwaggerDocs:
  """Test suite for generate_swagger_docs function."""

  @patch("robosystems.utils.docs_template._load_template_safely")
  def test_default_parameters(self, mock_load):
    """Test generation with default parameters."""
    mock_load.return_value = "Title: {title}, URL: {openapi_url}, Expansion: {doc_expansion}, Persist: {persist_auth}, Models: {models_expand_depth}, Model: {model_expand_depth}"

    result = generate_swagger_docs()

    assert "Title: API Documentation" in result
    assert "URL: /openapi.json" in result
    assert "Expansion: list" in result
    assert "Persist: true" in result
    assert "Models: 0" in result
    assert "Model: 1" in result

  @patch("robosystems.utils.docs_template._load_template_safely")
  def test_custom_parameters(self, mock_load):
    """Test generation with custom parameters."""
    mock_load.return_value = "Title: {title}, URL: {openapi_url}, Expansion: {doc_expansion}, Persist: {persist_auth}"

    result = generate_swagger_docs(
      title="Custom API",
      openapi_url="/custom/spec.json",
      doc_expansion="full",
      persist_auth=False,
    )

    assert "Title: Custom API" in result
    assert "URL: /custom/spec.json" in result
    assert "Expansion: full" in result
    assert "Persist: false" in result

  @patch("robosystems.utils.docs_template._load_template_safely")
  def test_xss_prevention(self, mock_load):
    """Test that potentially malicious input is sanitized."""
    mock_load.return_value = "Title: {title}, URL: {openapi_url}"

    result = generate_swagger_docs(
      title="<script>alert('xss')</script>", openapi_url="javascript:alert('xss')"
    )

    assert "&lt;script&gt;" in result
    assert "javascript:" not in result or "javascript:" in html.escape(result)
    assert "alert" not in result or "&" in result

  @patch("robosystems.utils.docs_template._load_template_safely")
  def test_template_path_construction(self, mock_load):
    """Test that template path is correctly constructed."""
    mock_load.return_value = "template"

    generate_swagger_docs()

    # Verify _load_template_safely was called
    mock_load.assert_called_once()

    # Verify the fallback function was passed
    assert mock_load.call_args[0][1] is not None

    # The fallback function should return a template
    fallback_func = mock_load.call_args[0][1]
    fallback_result = fallback_func()
    assert "<!DOCTYPE html>" in fallback_result
    assert "SwaggerUIBundle" in fallback_result


class TestGenerateRedocDocs:
  """Test suite for generate_redoc_docs function."""

  @patch("robosystems.utils.docs_template.Path")
  def test_with_existing_template(self, mock_path_cls):
    """Test generation when template file exists."""
    # Create the mock path instance
    mock_path_instance = MagicMock()
    mock_path_instance.exists.return_value = True
    mock_path_instance.is_file.return_value = True
    mock_path_instance.read_text.return_value = "Title: {title}, URL: {openapi_url}"

    # Set up the chain of calls: Path(__file__).parent.parent.parent / "static" / "redoc-template.html"
    mock_path = MagicMock()
    mock_path.parent.parent.parent.__truediv__.return_value.__truediv__.return_value = (
      mock_path_instance
    )
    mock_path_cls.return_value = mock_path

    result = generate_redoc_docs(title="Test API", openapi_url="/test/spec.json")

    assert result == "Title: Test API, URL: /test/spec.json"

  @patch("robosystems.utils.docs_template.Path")
  def test_with_fallback_template(self, mock_path_cls):
    """Test generation with fallback template when file doesn't exist."""
    # Create the mock path instance
    mock_path_instance = MagicMock()
    mock_path_instance.exists.return_value = False  # File doesn't exist

    # Set up the chain of calls
    mock_path = MagicMock()
    mock_path.parent.parent.parent.__truediv__.return_value.__truediv__.return_value = (
      mock_path_instance
    )
    mock_path_cls.return_value = mock_path

    result = generate_redoc_docs(
      title="Fallback Test", openapi_url="/fallback/spec.json"
    )

    # Check fallback template content
    assert "Fallback Test" in result
    assert "/fallback/spec.json" in result
    assert "redoc" in result.lower()
    assert json.dumps(REDOC_DARK_THEME) in result

  @patch("robosystems.utils.docs_template.Path")
  def test_io_error_fallback(self, mock_path_cls):
    """Test fallback when IO error occurs."""
    # Create the mock path instance
    mock_path_instance = MagicMock()
    mock_path_instance.exists.return_value = True
    mock_path_instance.is_file.return_value = True
    mock_path_instance.read_text.side_effect = OSError("Permission denied")

    # Set up the chain of calls
    mock_path = MagicMock()
    mock_path.parent.parent.parent.__truediv__.return_value.__truediv__.return_value = (
      mock_path_instance
    )
    mock_path_cls.return_value = mock_path

    with patch("robosystems.utils.docs_template.logger") as mock_logger:
      result = generate_redoc_docs(title="Error Test", openapi_url="/error/spec.json")

    # Should use fallback
    assert "Error Test" in result
    assert "/error/spec.json" in result
    mock_logger.warning.assert_called_once()

  def test_xss_prevention_redoc(self):
    """Test XSS prevention in ReDoc generation."""
    result = generate_redoc_docs(
      title="<script>alert('xss')</script>", openapi_url="javascript:alert('xss')"
    )

    # Malicious content should be escaped
    assert "<script>alert" not in result
    assert "&lt;script&gt;" in result or "\\u003c" in result


class TestConvenienceFunctions:
  """Test suite for convenience functions."""

  @patch("robosystems.utils.docs_template.generate_swagger_docs")
  def test_generate_robosystems_docs(self, mock_generate):
    """Test RoboSystems-specific Swagger generation."""
    mock_generate.return_value = "robosystems swagger"

    result = generate_robosystems_docs()

    assert result == "robosystems swagger"
    mock_generate.assert_called_once_with(title="RoboSystems API")

  @patch("robosystems.utils.docs_template.generate_swagger_docs")
  def test_generate_lbug_docs(self, mock_generate):
    """Test RoboSystems Graph API-specific Swagger generation."""
    mock_generate.return_value = "lbug swagger"

    result = generate_lbug_docs()

    assert result == "lbug swagger"
    mock_generate.assert_called_once_with(title="RoboSystems Graph API")

  @patch("robosystems.utils.docs_template.generate_redoc_docs")
  def test_generate_robosystems_redoc(self, mock_generate):
    """Test RoboSystems-specific ReDoc generation."""
    mock_generate.return_value = "robosystems redoc"

    result = generate_robosystems_redoc()

    assert result == "robosystems redoc"
    mock_generate.assert_called_once_with(title="RoboSystems API")


class TestFallbackTemplates:
  """Test suite for fallback template functions."""

  def test_get_fallback_template(self):
    """Test Swagger fallback template generation."""
    template = _get_fallback_template()

    # Check template structure
    assert "<!DOCTYPE html>" in template
    assert "{title}" in template
    assert "{openapi_url}" in template
    assert "{doc_expansion}" in template
    assert "{persist_auth}" in template
    assert "{models_expand_depth}" in template
    assert "{model_expand_depth}" in template
    assert "swagger-ui-dist" in template
    assert "SwaggerUIBundle" in template

  def test_get_redoc_fallback_template(self):
    """Test ReDoc fallback template generation."""
    template = _get_redoc_fallback_template("Test Title", "/test/api.json")

    # Check template structure
    assert "<!DOCTYPE html>" in template
    assert "Test Title" in template
    assert "/test/api.json" in template
    assert "redoc" in template.lower()

    # Check dark theme is included
    theme_json = json.dumps(REDOC_DARK_THEME)
    assert theme_json in template

    # Check CSS for dark mode
    assert "background: #1a1a1a" in template
    assert "color: #ffffff" in template


class TestRedocDarkTheme:
  """Test suite for REDOC_DARK_THEME configuration."""

  def test_theme_structure(self):
    """Test that theme has expected structure."""
    assert "colors" in REDOC_DARK_THEME
    assert "typography" in REDOC_DARK_THEME
    assert "sidebar" in REDOC_DARK_THEME
    assert "rightPanel" in REDOC_DARK_THEME
    assert "schema" in REDOC_DARK_THEME

  def test_color_definitions(self):
    """Test color definitions are present."""
    colors = REDOC_DARK_THEME["colors"]
    assert "primary" in colors
    assert "success" in colors
    assert "error" in colors
    assert "text" in colors
    assert "background" in colors
    assert "http" in colors

  def test_http_methods_colors(self):
    """Test HTTP method colors are defined."""
    http_colors = REDOC_DARK_THEME["colors"]["http"]
    expected_methods = ["get", "post", "put", "delete", "patch", "options", "head"]
    for method in expected_methods:
      assert method in http_colors

  def test_typography_settings(self):
    """Test typography configuration."""
    typography = REDOC_DARK_THEME["typography"]
    assert "fontSize" in typography
    assert "fontFamily" in typography
    assert "code" in typography

    # Check code block settings
    code_config = typography["code"]
    assert "fontSize" in code_config
    assert "fontFamily" in code_config
    assert "backgroundColor" in code_config
    assert code_config["wrap"] is True
