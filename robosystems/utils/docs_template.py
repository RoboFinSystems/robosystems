"""
Swagger and ReDoc documentation template utility for generating custom API docs.
"""

import html
from pathlib import Path

from ..config.logging import get_logger

logger = get_logger(__name__)

# ReDoc dark theme configuration
REDOC_DARK_THEME = {
  "colors": {
    "primary": {
      "main": "#2563eb",
      "light": "#60a5fa",
      "dark": "#1d4ed8",
      "contrastText": "#ffffff",
    },
    "success": {
      "main": "#16a34a",
      "light": "#4ade80",
      "dark": "#15803d",
      "contrastText": "#ffffff",
    },
    "warning": {"main": "#f59e0b", "light": "#fbbf24", "dark": "#d97706"},
    "error": {"main": "#dc2626", "light": "#f87171", "dark": "#b91c1c"},
    "text": {"primary": "#ffffff", "secondary": "#d1d5db"},
    "background": {"primary": "#1a1a1a", "secondary": "#0d0d0d"},
    "border": {"dark": "#374151", "light": "#4b5563"},
    "responses": {
      "success": {"color": "#10b981", "backgroundColor": "rgba(16, 185, 129, 0.1)"},
      "error": {"color": "#ef4444", "backgroundColor": "rgba(239, 68, 68, 0.1)"},
      "redirect": {"color": "#f59e0b", "backgroundColor": "rgba(245, 158, 11, 0.1)"},
      "info": {"color": "#60a5fa", "backgroundColor": "rgba(96, 165, 250, 0.1)"},
    },
    "http": {
      "get": "#2563eb",
      "post": "#16a34a",
      "put": "#f59e0b",
      "options": "#9333ea",
      "patch": "#d97706",
      "delete": "#dc2626",
      "basic": "#6b7280",
      "link": "#60a5fa",
      "head": "#9333ea",
    },
  },
  "typography": {
    "fontSize": "14px",
    "fontFamily": '"Space Grotesk", "Roboto", sans-serif',
    "headings": {
      "fontFamily": '"Orbitron", "Montserrat", sans-serif',
      "fontWeight": "600",
    },
    "code": {
      "fontSize": "13px",
      "fontFamily": '"Monaco", "Menlo", "Ubuntu Mono", monospace',
      "fontWeight": "400",
      "color": "#93c5fd",
      "backgroundColor": "#1f2937",
      "wrap": True,
    },
    "links": {"color": "#60a5fa", "visited": "#60a5fa", "hover": "#93c5fd"},
  },
  "sidebar": {
    "width": "260px",
    "backgroundColor": "#111111",
    "textColor": "#e5e7eb",
    "activeTextColor": "#ffffff",
    "groupItems": {"textTransform": "uppercase"},
    "level1Items": {"textTransform": "none"},
    "arrow": {"size": "1.2em", "color": "#6b7280"},
  },
  "rightPanel": {"backgroundColor": "#0d0d0d", "width": "40%", "textColor": "#d1d5db"},
  "codeBlock": {"backgroundColor": "#1a1a1a"},
  "schema": {
    "nestedBackground": "#1f2937",
    "linesColor": "#374151",
    "typeNameColor": "#60a5fa",
    "typeTitleColor": "#93c5fd",
    "requireLabelColor": "#ef4444",
    "labelsTextColor": "#d1d5db",
    "constraintsTextColor": "#9ca3af",
    "caretColor": "#6b7280",
  },
  "theme": {
    "spacing": {"unit": 4, "sectionHorizontal": 40, "sectionVertical": 40},
    "breakpoints": {"xs": 0, "small": "550px", "medium": "900px", "large": "1200px"},
  },
}


def _sanitize_input(value: str) -> str:
  """
  Sanitize input to prevent XSS and injection attacks.

  Args:
      value: Input string to sanitize

  Returns:
      Sanitized string safe for HTML inclusion
  """
  return html.escape(value, quote=True)


def _load_template_safely(template_path: Path, fallback_func) -> str:
  """
  Load template with proper error handling.

  Args:
      template_path: Path to template file
      fallback_func: Function to generate fallback template

  Returns:
      Template content or fallback
  """
  try:
    if template_path.exists() and template_path.is_file():
      return template_path.read_text(encoding="utf-8")
  except OSError as e:
    logger.warning(f"Failed to load template {template_path}: {e}")
  return fallback_func()


def generate_swagger_docs(
  title: str = "API Documentation",
  openapi_url: str = "/openapi.json",
  doc_expansion: str = "list",
  persist_auth: bool = True,
  models_expand_depth: int = 0,
  model_expand_depth: int = 1,
) -> str:
  """
  Generate a Swagger UI HTML page with configurable parameters.

  Args:
      title: Page title (e.g., "RoboSystems API", "RoboSystems Graph API")
      openapi_url: URL to OpenAPI JSON spec (default: "/openapi.json")
      doc_expansion: How to expand docs ("list", "full", "none")
      persist_auth: Whether to persist authorization between refreshes
      models_expand_depth: Default depth for expanding models section
      model_expand_depth: Default depth for expanding individual models

  Returns:
      HTML string for the Swagger UI page
  """
  # Sanitize inputs
  title = _sanitize_input(title)
  openapi_url = _sanitize_input(openapi_url)
  doc_expansion = _sanitize_input(doc_expansion)

  template_path = (
    Path(__file__).parent.parent.parent / "static" / "swagger-docs-template.html"
  )

  template_content = _load_template_safely(
    template_path, lambda: _get_fallback_template()
  )

  return template_content.format(
    title=title,
    openapi_url=openapi_url,
    doc_expansion=doc_expansion,
    persist_auth="true" if persist_auth else "false",
    models_expand_depth=models_expand_depth,
    model_expand_depth=model_expand_depth,
  )


def _get_fallback_template() -> str:
  """Fallback template if the file doesn't exist."""
  return """<!DOCTYPE html>
<html>
  <head>
    <title>{title}</title>
    <link rel="icon" type="image/x-icon" href="/static/favicon.ico" />
    <link
      rel="stylesheet"
      type="text/css"
      href="https://unpkg.com/swagger-ui-dist@5.9.0/swagger-ui.css"
    />
    <link rel="stylesheet" type="text/css" href="/static/swagger-custom.css" />
  </head>
  <body>
    <div id="swagger-ui"></div>
    <script src="https://unpkg.com/swagger-ui-dist@5.9.0/swagger-ui-bundle.js"></script>
    <script>
      SwaggerUIBundle({{
        url: "{openapi_url}",
        dom_id: "#swagger-ui",
        presets: [
          SwaggerUIBundle.presets.apis,
          SwaggerUIBundle.presets.standalone,
        ],
        docExpansion: "{doc_expansion}",
        persistAuthorization: {persist_auth},
        defaultModelsExpandDepth: {models_expand_depth},
        defaultModelExpandDepth: {model_expand_depth},
      }});
    </script>
  </body>
</html>"""


# Convenience functions for common use cases
def generate_robosystems_docs() -> str:
  """Generate docs for main RoboSystems API."""
  return generate_swagger_docs(title="RoboSystems API")


def generate_lbug_docs() -> str:
  """Generate docs for RoboSystems Graph API."""
  return generate_swagger_docs(title="RoboSystems Graph API")


def generate_redoc_docs(
  title: str = "API Documentation",
  openapi_url: str = "/openapi.json",
) -> str:
  """
  Generate a ReDoc HTML page with dark theme styling.

  Args:
      title: Page title (e.g., "RoboSystems API", "RoboSystems Graph API")
      openapi_url: URL to OpenAPI JSON spec (default: "/openapi.json")

  Returns:
      HTML string for the ReDoc page with dark theme
  """
  # Sanitize inputs to prevent injection
  title = _sanitize_input(title)
  openapi_url = _sanitize_input(openapi_url)

  template_path = Path(__file__).parent.parent.parent / "static" / "redoc-template.html"

  # Check if template file exists
  try:
    if template_path.exists() and template_path.is_file():
      template_content = template_path.read_text(encoding="utf-8")
      return template_content.format(
        title=title,
        openapi_url=openapi_url,
      )
  except OSError as e:
    logger.warning(f"Failed to load template {template_path}: {e}")

  # Use fallback template
  return _get_redoc_fallback_template(title, openapi_url)


def _get_redoc_fallback_template(title: str, openapi_url: str) -> str:
  """
  Fallback ReDoc template with dark theme if the file doesn't exist.

  Args:
      title: The page title
      openapi_url: The OpenAPI spec URL

  Returns:
      Complete HTML string with dark theme
  """
  import json

  # Convert Python dict to JSON string for embedding
  theme_json = json.dumps(REDOC_DARK_THEME)

  # Use string template that won't conflict with JSON braces
  template = """<!DOCTYPE html>
<html>
  <head>
    <title>__TITLE__</title>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <link rel="icon" type="image/x-icon" href="/static/favicon.ico" />
    <link
      href="https://fonts.googleapis.com/css?family=Montserrat:300,400,700|Roboto:300,400,700"
      rel="stylesheet"
    />
    <style>
      body {
        margin: 0;
        padding: 0;
        font-family: "Roboto", sans-serif;
        background: #1a1a1a;
      }
      /* Override any default light backgrounds */
      .sc-hKFxyN {
        background-color: #1f2937 !important;
        color: #ffffff !important;
      }
      /* Ensure all headers have dark backgrounds */
      h1, h2, h3, h4, h5, h6 {
        color: #ffffff !important;
      }
      /* Fix parameter headers */
      [role="button"] {
        background-color: #1f2937 !important;
        color: #ffffff !important;
      }
      /* Fix nested sections */
      div[label] {
        background-color: #1f2937 !important;
      }
    </style>
  </head>
  <body>
    <redoc spec-url="__OPENAPI_URL__" theme='__THEME_JSON__'></redoc>
    <!-- ReDoc latest stable -->
    <script src="https://cdn.redoc.ly/redoc/latest/bundles/redoc.standalone.js"></script>
  </body>
</html>"""

  # Replace all placeholders
  return (
    template.replace("__TITLE__", title)
    .replace("__OPENAPI_URL__", openapi_url)
    .replace("__THEME_JSON__", theme_json)
  )


# Convenience function for main RoboSystems ReDoc with dark theme styling
def generate_robosystems_redoc() -> str:
  """Generate ReDoc documentation with dark theme for main RoboSystems API."""
  return generate_redoc_docs(title="RoboSystems API")
