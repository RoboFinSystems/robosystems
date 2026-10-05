"""Markdown document parser with heading-based sectioning.

Parses YAML frontmatter and splits markdown content into sections
based on heading boundaries, ready for OpenSearch indexing.
"""

from __future__ import annotations

import hashlib
import re
from typing import Any

_HEADING_PATTERN = re.compile(r"^(#{1,6}) (\S.*)$", re.MULTILINE)

_SECTION_ID_STRIP = re.compile(r"[^a-z0-9-]")

# Smaller sections merge into the previous one (a small first one merges forward).
MIN_SECTION_WORDS = 20

MAX_SECTION_CHARS = 50_000

MAX_FRONTMATTER_TAGS = 50
MAX_FRONTMATTER_TAG_LENGTH = 100
MAX_FRONTMATTER_TITLE_LENGTH = 500
MAX_FRONTMATTER_FOLDER_LENGTH = 200


def frontmatter_tags(raw_tags: object) -> list[str]:
  """Scalar tags only, bounded in count and length."""
  if isinstance(raw_tags, str):
    raw_tags = raw_tags.split(",")
  if not isinstance(raw_tags, list):
    return []
  tags: list[str] = []
  for raw in raw_tags[:MAX_FRONTMATTER_TAGS]:
    if not isinstance(raw, str | int | float):
      continue
    if isinstance(raw, str) and len(raw) > MAX_FRONTMATTER_TAG_LENGTH:
      continue
    tag = str(raw).strip()
    if tag:
      tags.append(tag)
  return tags


def frontmatter_folder(raw_folder: object) -> str | None:
  """A non-empty string folder, capped in length; anything else is ignored."""
  if not isinstance(raw_folder, str):
    return None
  folder = raw_folder.strip()[:MAX_FRONTMATTER_FOLDER_LENGTH]
  return folder or None


def frontmatter_title(raw_title: object) -> str | None:
  """A non-empty string title, capped in length; anything else is ignored."""
  if not isinstance(raw_title, str):
    return None
  title = raw_title.strip()[:MAX_FRONTMATTER_TITLE_LENGTH]
  return title or None


def parse_frontmatter(content: str) -> tuple[dict[str, Any], str]:
  """Split markdown into its ``(frontmatter, remaining_content)`` pair.

  Frontmatter is delimited by --- at the start and end:
      ---
      title: My Document
      tags: foo, bar
      ---
      # Content starts here

  Returns ``({}, content)`` unchanged when there is no frontmatter, or when
  it is unterminated or doesn't parse as a YAML mapping.
  """
  stripped = content.lstrip()
  if not stripped.startswith("---"):
    return {}, content

  end_idx = stripped.find("---", 3)
  if end_idx == -1:
    return {}, content

  frontmatter_text = stripped[3:end_idx].strip()
  remaining = stripped[end_idx + 3 :].lstrip("\n")

  try:
    from robosystems.utils.yaml_input import load_untrusted_yaml

    metadata = load_untrusted_yaml(frontmatter_text)
    if not isinstance(metadata, dict):
      return {}, content
    return metadata, remaining
  except Exception:
    return {}, content


def _make_section_id(heading_text: str) -> str:
  """Convert heading text to a URL-safe section ID.

  "Revenue Analysis" -> "revenue-analysis"
  "Q4 Performance!" -> "q4-performance"
  """
  slug = heading_text.lower().strip()
  slug = slug.replace(" ", "-")
  slug = _SECTION_ID_STRIP.sub("", slug)
  slug = re.sub(r"-+", "-", slug).strip("-")
  return slug or "section"


def section_markdown(
  content: str, default_title: str = "Document"
) -> list[dict[str, Any]]:
  """Split markdown content into sections based on headings.

  ``content`` must already have its frontmatter removed. Each section
  includes its own heading plus everything up to the next heading, and is
  returned as a dict with keys ``section_id``, ``section_label``,
  ``heading_depth``, and ``content``. Sections shorter than
  ``MIN_SECTION_WORDS`` merge into a neighbour so headings with no body
  don't become standalone hits, and anything over ``MAX_SECTION_CHARS`` is
  truncated. ``default_title`` labels the preamble and the
  no-headings-at-all case.
  """
  headings = list(_HEADING_PATTERN.finditer(content))

  if not headings:
    text = content.strip()
    if not text:
      return []
    return [
      {
        "section_id": "full-document",
        "section_label": default_title,
        "heading_depth": 0,
        "content": text[:MAX_SECTION_CHARS],
      }
    ]

  raw_sections: list[dict[str, Any]] = []

  preamble = content[: headings[0].start()].strip()
  if preamble and len(preamble.split()) >= MIN_SECTION_WORDS:
    raw_sections.append(
      {
        "section_id": "preamble",
        "section_label": default_title,
        "heading_depth": 0,
        "content": preamble,
      }
    )

  for i, match in enumerate(headings):
    depth = len(match.group(1))
    label = match.group(2).strip()
    section_id = _make_section_id(label)

    start = match.end()
    end = headings[i + 1].start() if i + 1 < len(headings) else len(content)
    section_content = content[start:end].strip()

    full_content = (
      f"{'#' * depth} {label}\n\n{section_content}"
      if section_content
      else f"{'#' * depth} {label}"
    )

    raw_sections.append(
      {
        "section_id": section_id,
        "section_label": label,
        "heading_depth": depth,
        "content": full_content,
      }
    )

  merged: list[dict[str, Any]] = []
  for section in raw_sections:
    word_count = len(section["content"].split())
    if word_count < MIN_SECTION_WORDS and merged:
      merged[-1]["content"] += "\n\n" + section["content"]
    elif word_count < MIN_SECTION_WORDS and not merged:
      merged.append(section)
    else:
      if merged and len(merged[-1]["content"].split()) < MIN_SECTION_WORDS:
        section["content"] = merged[-1]["content"] + "\n\n" + section["content"]
        merged[-1] = section
      else:
        merged.append(section)

  for section in merged:
    if len(section["content"]) > MAX_SECTION_CHARS:
      section["content"] = section["content"][:MAX_SECTION_CHARS]

  return merged


def parse_document(
  content: str, title: str
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
  """Parse a markdown document into ``(metadata, sections)``.

  Combines frontmatter extraction with heading-based sectioning. A
  frontmatter ``title`` overrides the ``title`` argument, and the returned
  metadata always carries the winning title. Frontmatter ``tags`` are
  normalized to a list whether written as a YAML list or a comma-separated
  string.
  """
  metadata, body = parse_frontmatter(content)

  doc_title = frontmatter_title(metadata.get("title")) or title

  if "tags" in metadata:
    tags = frontmatter_tags(metadata["tags"])
    if tags:
      metadata["tags"] = tags
    else:
      del metadata["tags"]

  if "folder" in metadata:
    folder = frontmatter_folder(metadata["folder"])
    if folder:
      metadata["folder"] = folder
    else:
      del metadata["folder"]

  sections = section_markdown(body, default_title=doc_title)

  metadata["title"] = doc_title

  return metadata, sections


def content_hash(content: str) -> str:
  """Generate a short deterministic hash of content for document IDs."""
  return hashlib.sha256(content.encode()).hexdigest()[:12]
