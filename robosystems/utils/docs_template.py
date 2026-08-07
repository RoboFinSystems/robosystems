"""
Render the Swagger UI and ReDoc pages that serve this API's documentation.

Pages are built from templates under `/static`, with inline fallbacks so docs
still render if a template file is missing. All interpolated values are HTML-
escaped, since titles and spec URLs can come from configuration.
"""

import html
from pathlib import Path

from ..config import env
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
  """Escape a value for interpolation into the docs HTML."""
  return html.escape(value, quote=True)


def _load_template_safely(template_path: Path, fallback_func) -> str:
  """Read a template file, falling back to `fallback_func()` if unreadable."""
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
  """Render the Swagger UI page as an HTML string.

  Args:
      title: Page title.
      openapi_url: URL the page fetches the OpenAPI spec from.
      doc_expansion: Initial operation expansion: "list", "full", or "none".
      persist_auth: Keep entered credentials across page reloads.
      models_expand_depth: Expansion depth of the models section; -1 hides it.
      model_expand_depth: Expansion depth within an individual model.
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
  """Fallback template if the file doesn't exist.

  Assets are self-hosted from /static/vendor (no third-party CDN), and the
  initializer lives in /static/swagger-init.js reading its configuration from
  data attributes, so the page needs no inline script.
  """
  return """<!DOCTYPE html>
<html>
  <head>
    <title>{title}</title>
    <link rel="icon" type="image/x-icon" href="/static/favicon.ico" />
    <link
      rel="stylesheet"
      type="text/css"
      href="/static/vendor/swagger-ui-5.9.0/swagger-ui.css"
    />
    <link rel="stylesheet" type="text/css" href="/static/swagger-custom.css" />
  </head>
  <body>
    <div
      id="swagger-ui"
      data-openapi-url="{openapi_url}"
      data-doc-expansion="{doc_expansion}"
      data-persist-auth="{persist_auth}"
      data-models-expand-depth="{models_expand_depth}"
      data-model-expand-depth="{model_expand_depth}"
    ></div>
    <script src="/static/vendor/swagger-ui-5.9.0/swagger-ui-bundle.js"></script>
    <script src="/static/swagger-init.js"></script>
  </body>
</html>"""


# Convenience functions for common use cases.
# Authorization persistence in browser storage is a dev convenience only;
# outside dev, credentials live no longer than the page.
def generate_robosystems_docs() -> str:
  """Generate docs for main RoboSystems API."""
  return generate_swagger_docs(
    title="RoboSystems API", persist_auth=env.is_development()
  )


def generate_lbug_docs() -> str:
  """Generate docs for RoboSystems Graph API."""
  return generate_swagger_docs(
    title="RoboSystems Graph API", persist_auth=env.is_development()
  )


def generate_redoc_docs(
  title: str = "API Documentation",
  openapi_url: str = "/openapi.json",
) -> str:
  """Render the dark-themed ReDoc page as an HTML string."""
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
  """Inline ReDoc page used when the template file is unavailable.

  Placeholders are substituted rather than `str.format`-ed, because the
  embedded theme JSON is full of braces.
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
    <style>
      @font-face {
        font-family: "Orbitron";
        src: url("/static/fonts/Orbitron/Orbitron-Bold.ttf") format("truetype");
        font-weight: 700;
        font-display: swap;
      }
      @font-face {
        font-family: "Orbitron";
        src: url("/static/fonts/Orbitron/Orbitron-SemiBold.ttf") format("truetype");
        font-weight: 600;
        font-display: swap;
      }
      @font-face {
        font-family: "Space Grotesk";
        src: url("/static/fonts/SpaceGrotesk/SpaceGrotesk-Regular.ttf")
          format("truetype");
        font-weight: 400;
        font-display: swap;
      }
      @font-face {
        font-family: "Space Grotesk";
        src: url("/static/fonts/SpaceGrotesk/SpaceGrotesk-Medium.ttf")
          format("truetype");
        font-weight: 500;
        font-display: swap;
      }
      @font-face {
        font-family: "Space Grotesk";
        src: url("/static/fonts/SpaceGrotesk/SpaceGrotesk-SemiBold.ttf")
          format("truetype");
        font-weight: 600;
        font-display: swap;
      }
      body {
        margin: 0;
        padding: 0;
        font-family: "Space Grotesk", sans-serif;
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
    <script src="/static/vendor/redoc-2.5.3/redoc.standalone.js"></script>
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
