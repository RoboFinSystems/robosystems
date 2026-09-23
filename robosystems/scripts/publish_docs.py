#!/usr/bin/env python3
"""Build the public docs catalog: markdown bodies plus one ``index.json``.

Two sources feed it:

- **Technical docs** — the GitHub wiki, checked out beside this repo. GitHub
  serves wiki pages ``X-Robots-Tag: none``, so the wiki is where these are
  written, never where they are found. They render at
  ``robosystems.ai/docs/technical``.
- **Product docs** — ``docs/product/{site}/*.md`` in this repo, one folder per
  site, rendered at that site's docs path.

The output directory is what the Publish Docs workflow syncs to the content CDN:

    index.json
    technical/{slug}.md           one body per wiki page, links rewritten
    technical/images/...          images the wiki references
    product/{site}/{slug}.md      one body per product page
    product/{site}/images/...     screenshots and clips the product pages show

The apps read ``index.json`` with ISR and fetch each body by its ``body`` key.
Nothing here renders HTML, so the bodies stay plain GitHub-flavored markdown:
the wiki's prose carries literal ``{`` and ``<`` that an MDX compiler rejects.

Wiki-relative links become site paths, and a link to a page that does not
exist fails the build, so a renamed page breaks the publish rather than the
site. Anchors that match no heading are reported as warnings. When both are
built in one run, a product page's link into the technical docs must name a
page and a heading that exist, or the build fails.

``digest`` hashes everything else in the output, so the workflow can skip the
upload when nothing changed.

Usage:
    just docs-build ../robosystems.wiki
    uv run --no-project python robosystems/scripts/publish_docs.py \\
        --wiki ../robosystems.wiki --product docs/product --out build/docs
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

SCHEMA_VERSION = 1
WIKI_URL = "https://github.com/RoboFinSystems/robosystems/wiki"
REPO_BLOB_URL = "https://github.com/RoboFinSystems/robosystems/blob/main"
DEFAULT_ASSET_BASE = "https://assets.robosystems.ai/docs/"

TECHNICAL_BASE_PATH = "/docs/technical"
TECHNICAL_URL = f"https://robosystems.ai{TECHNICAL_BASE_PATH}"
TECHNICAL_HOME_TITLE = "Technical documentation"
PRODUCT_BASE_PATHS = {
  "robosystems": "/docs/guides",
  "roboledger": "/docs",
  "roboinvestor": "/docs",
}

DESCRIPTION_MAX = 160

_FENCE = re.compile(r"^\s*(```|~~~)")
_INLINE_CODE = re.compile(r"`[^`\n]*`")
_LINK = re.compile(r"(!?)\[([^\]]*)\]\(([^)\s]+)((?:\s+\"[^\"]*\")?)\)")
_HEADING = re.compile(r"^(#{1,6})\s+(.*?)\s*#*\s*$")
_SIDEBAR_SECTION = re.compile(r"^#{2,6}\s+(.+?)\s*$")
_SIDEBAR_ITEM = re.compile(r"^\s*[-*]\s+\[[^\]]+\]\(([^)\s#]+)\)")


@dataclass
class Page:
  site: str
  layer: str
  slug: str
  path: str
  title: str
  description: str
  section: str | None
  order: int
  updated: str | None
  body: str
  source_url: str

  def record(self) -> dict[str, object]:
    return {
      "site": self.site,
      "layer": self.layer,
      "slug": self.slug,
      "path": self.path,
      "title": self.title,
      "description": self.description,
      "section": self.section,
      "order": self.order,
      "updated": self.updated,
      "body": self.body,
      "source_url": self.source_url,
    }


@dataclass
class Build:
  pages: list[Page] = field(default_factory=list)
  collections: list[dict[str, object]] = field(default_factory=list)
  files: dict[str, bytes] = field(default_factory=dict)
  errors: list[str] = field(default_factory=list)
  warnings: list[str] = field(default_factory=list)
  # Heading anchors per technical page path, set when the wiki is built in the
  # same run. Product pages link the technical docs by absolute URL, and those
  # links are checked against it; without the wiki there is nothing to check.
  technical_anchors: dict[str, set[str]] | None = None


# ── Markdown helpers ────────────────────────────────────────────────────────


def split_code(text: str) -> list[tuple[bool, str]]:
  """Split markdown into (is_fenced_code, chunk) runs, preserving every line."""
  runs: list[tuple[bool, str]] = []
  in_code = False
  buf: list[str] = []
  for line in text.splitlines(keepends=True):
    if _FENCE.match(line):
      if not in_code:
        if buf:
          runs.append((False, "".join(buf)))
        buf = [line]
        in_code = True
      else:
        buf.append(line)
        runs.append((True, "".join(buf)))
        buf = []
        in_code = False
      continue
    buf.append(line)
  if buf:
    runs.append((in_code, "".join(buf)))
  return runs


def map_prose(text: str, fn) -> str:
  """Apply ``fn`` to prose only: fenced blocks and inline code pass through."""
  out: list[str] = []
  for is_code, chunk in split_code(text):
    if is_code:
      out.append(chunk)
      continue
    spans: list[str] = []

    def hide(match: re.Match[str]) -> str:
      spans.append(match.group(0))
      return f"\x00{len(spans) - 1}\x00"

    hidden = _INLINE_CODE.sub(hide, chunk)
    changed = fn(hidden)
    out.append(re.sub(r"\x00(\d+)\x00", lambda m: spans[int(m.group(1))], changed))
  return "".join(out)


def plain_text(markdown: str) -> str:
  """Inline markdown reduced to the words a reader sees."""
  text = re.sub(r"!\[[^\]]*\]\([^)]*\)", "", markdown)
  text = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", text)
  text = re.sub(r"`([^`]*)`", r"\1", text)
  text = text.replace("**", "").replace("__", "")
  text = re.sub(r"(?<!\w)\*(?!\s)(.+?)(?<!\s)\*(?!\w)", r"\1", text)
  return re.sub(r"\s+", " ", text).strip()


def github_slug(heading: str) -> str:
  """The anchor GitHub gives a heading (github-slugger, before de-duplication)."""
  text = plain_text(heading).lower()
  text = re.sub(r"[^\w\- ]", "", text)
  return text.replace(" ", "-")


def heading_anchors(text: str) -> set[str]:
  """Every anchor a page's headings produce, with GitHub's ``-1`` suffixes."""
  anchors: set[str] = set()
  seen: dict[str, int] = {}
  for is_code, chunk in split_code(text):
    if is_code:
      continue
    for line in chunk.splitlines():
      match = _HEADING.match(line)
      if not match:
        continue
      slug = github_slug(match.group(2))
      count = seen.get(slug, 0)
      seen[slug] = count + 1
      anchors.add(slug if count == 0 else f"{slug}-{count}")
  return anchors


def strip_title(text: str) -> tuple[str | None, str]:
  """Remove a leading H1 and return its text; the renderer prints the title once."""
  lines = text.splitlines(keepends=True)
  index = 0
  while index < len(lines) and not lines[index].strip():
    index += 1
  if index < len(lines) and re.match(r"^#\s+", lines[index]):
    title = plain_text(re.sub(r"^#\s+", "", lines[index]).strip().rstrip("#").strip())
    index += 1
    while index < len(lines) and not lines[index].strip():
      index += 1
    return title, "".join(lines[index:])
  return None, text


def describe(text: str) -> str:
  """The first prose paragraph, cut at a sentence boundary under the limit."""
  for is_code, chunk in split_code(text):
    if is_code:
      continue
    for paragraph in re.split(r"\n\s*\n", chunk):
      stripped = paragraph.strip()
      if not stripped or re.match(r"^(#|\||>|[-*+]\s|\d+\.\s|<|---|!\[)", stripped):
        continue
      return truncate(plain_text(stripped))
  return ""


def truncate(text: str, limit: int = DESCRIPTION_MAX) -> str:
  if len(text) <= limit:
    return text
  ends = [m.end() for m in re.finditer(r"[.!?](?=\s)", text) if m.end() <= limit]
  if ends:
    return text[: ends[-1]]
  cut = text[: limit - 1].rsplit(" ", 1)[0].rstrip(",;:")
  return f"{cut}…"


def split_front_matter(text: str) -> tuple[dict[str, str], str]:
  """A ``---``-fenced block of ``key: value`` lines; nothing nested."""
  if not text.startswith("---\n"):
    return {}, text
  end = text.find("\n---\n", 4)
  if end == -1:
    return {}, text
  meta: dict[str, str] = {}
  for line in text[4:end].splitlines():
    if not line.strip() or line.lstrip().startswith("#"):
      continue
    key, sep, value = line.partition(":")
    if not sep:
      continue
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
      value = value[1:-1]
    meta[key.strip()] = value
  return meta, text[end + 5 :].lstrip("\n")


def last_commit_date(path: Path) -> str | None:
  """The file's last commit time, or None outside git or before its first commit."""
  try:
    result = subprocess.run(
      ["git", "-C", str(path.parent), "log", "-1", "--format=%cI", "--", path.name],
      capture_output=True,
      text=True,
      check=False,
    )
  except OSError:
    return None
  value = result.stdout.strip()
  return value or None


# ── The wiki ────────────────────────────────────────────────────────────────


def technical_path(page_name: str) -> str:
  if page_name == "Home":
    return TECHNICAL_BASE_PATH
  return f"{TECHNICAL_BASE_PATH}/{page_name.lower()}"


def parse_sidebar(text: str) -> list[tuple[str, list[str]]]:
  """``_Sidebar.md`` as (section title, wiki page names), in order."""
  sections: list[tuple[str, list[str]]] = []
  for line in text.splitlines():
    heading = _SIDEBAR_SECTION.match(line)
    if heading:
      sections.append((plain_text(heading.group(1)), []))
      continue
    item = _SIDEBAR_ITEM.match(line)
    if item and sections:
      sections[-1][1].append(item.group(1))
  return sections


def rewrite_wiki_links(
  text: str,
  source: str,
  pages: set[str],
  anchors: dict[str, set[str]],
  asset_base: str,
  build: Build,
) -> str:
  def replace(match: re.Match[str]) -> str:
    bang, label, target, title = match.groups()
    new = resolve_wiki_target(target, source, pages, anchors, asset_base, build)
    return f"{bang}[{label}]({new}{title})"

  return map_prose(text, lambda chunk: _LINK.sub(replace, chunk))


def resolve_wiki_target(
  target: str,
  source: str,
  pages: set[str],
  anchors: dict[str, set[str]],
  asset_base: str,
  build: Build,
) -> str:
  if target == WIKI_URL or target.startswith((f"{WIKI_URL}/", f"{WIKI_URL}#")):
    rest = target[len(WIKI_URL) :].lstrip("/")
    # The wiki root is Home, so a bare `#anchor` after it is Home's anchor.
    if not rest or rest.startswith("#"):
      rest = f"Home{rest}"
    return resolve_wiki_target(rest, source, pages, anchors, asset_base, build)
  if target.startswith(("http://", "https://", "mailto:")):
    return target
  if target.startswith("#"):
    check_anchor(source, source, target[1:], anchors, build)
    return target
  if target.startswith("images/"):
    return f"{asset_base}technical/{target}"
  name, _, anchor = target.partition("#")
  if name not in pages:
    build.errors.append(
      f"{source}.md: link to a wiki page that does not exist: {target}"
    )
    return target
  if anchor:
    check_anchor(source, name, anchor, anchors, build)
    return f"{technical_path(name)}#{anchor}"
  return technical_path(name)


def check_anchor(
  source: str, page: str, anchor: str, anchors: dict[str, set[str]], build: Build
) -> None:
  if anchor not in anchors.get(page, set()):
    build.warnings.append(f"{source}.md: no heading in {page} for #{anchor}")


def build_wiki(wiki_dir: Path, asset_base: str, build: Build) -> None:
  sources = {
    path.stem: path.read_text(encoding="utf-8")
    for path in sorted(wiki_dir.glob("*.md"))
    if not path.name.startswith("_")
  }
  if "Home" not in sources:
    build.errors.append(f"{wiki_dir}: no Home.md")
    return
  pages = set(sources)
  anchors = {name: heading_anchors(text) for name, text in sources.items()}
  build.technical_anchors = {
    technical_path(name): found for name, found in anchors.items()
  }

  sidebar_path = wiki_dir / "_Sidebar.md"
  sidebar = (
    parse_sidebar(sidebar_path.read_text(encoding="utf-8"))
    if sidebar_path.exists()
    else []
  )
  placement: dict[str, tuple[str, int]] = {}
  order = 0
  sections: list[dict[str, object]] = []
  for title, names in sidebar:
    slugs: list[str] = []
    for name in names:
      if name not in pages:
        build.errors.append(
          f"_Sidebar.md: link to a wiki page that does not exist: {name}"
        )
        continue
      if name in placement or name == "Home":
        continue
      order += 1
      placement[name] = (title, order)
      slugs.append(name.lower())
    sections.append({"title": title, "slugs": slugs})

  orphans = sorted(pages - set(placement) - {"Home"})
  if orphans:
    build.warnings.append(
      f"_Sidebar.md: pages not in the sidebar: {', '.join(orphans)}"
    )
    for name in orphans:
      order += 1
      placement[name] = ("Other", order)
    sections.append({"title": "Other", "slugs": [name.lower() for name in orphans]})

  for name, raw in sources.items():
    title, body = strip_title(raw)
    body = rewrite_wiki_links(body, name, pages, anchors, asset_base, build)
    slug = "index" if name == "Home" else name.lower()
    section, position = placement.get(name, (None, 0))
    key = f"technical/{slug}.md"
    build.files[key] = body.encode("utf-8")
    build.pages.append(
      Page(
        site="robosystems",
        layer="technical",
        slug=slug,
        path=technical_path(name),
        title=TECHNICAL_HOME_TITLE
        if name == "Home"
        else (title or name.replace("-", " ")),
        description=describe(body),
        section=section,
        order=position,
        updated=last_commit_date(wiki_dir / f"{name}.md"),
        body=key,
        source_url=WIKI_URL if name == "Home" else f"{WIKI_URL}/{name}",
      )
    )

  for image in (
    sorted((wiki_dir / "images").glob("**/*")) if (wiki_dir / "images").is_dir() else []
  ):
    if image.is_file():
      build.files[
        f"technical/images/{image.relative_to(wiki_dir / 'images').as_posix()}"
      ] = image.read_bytes()

  build.collections.append(
    {
      "site": "robosystems",
      "layer": "technical",
      "base_path": TECHNICAL_BASE_PATH,
      "sections": sections,
    }
  )


# ── Product pages ───────────────────────────────────────────────────────────


def build_product(
  product_dir: Path,
  repo_root: Path,
  build: Build,
  asset_base: str = DEFAULT_ASSET_BASE,
) -> None:
  for site_dir in sorted(p for p in product_dir.iterdir() if p.is_dir()):
    site = site_dir.name
    base_path = PRODUCT_BASE_PATHS.get(site)
    if base_path is None:
      build.errors.append(f"{site_dir}: unknown site {site!r}")
      continue
    files = sorted(p for p in site_dir.glob("*.md") if p.name != "README.md")
    slugs = {p.stem for p in files}
    media = product_media(site_dir)
    shown: set[str] = set()
    entries: list[tuple[int, str, str | None]] = []
    for path in files:
      meta, body = split_front_matter(path.read_text(encoding="utf-8"))
      label = path.relative_to(product_dir).as_posix()
      title = meta.get("title")
      if not title:
        build.errors.append(f"{label}: front matter needs a title")
        continue
      try:
        position = int(meta.get("order", ""))
      except ValueError:
        build.errors.append(f"{label}: front matter needs an integer order")
        continue
      description = meta.get("description", "")
      if len(description) > DESCRIPTION_MAX:
        build.errors.append(
          f"{label}: description is {len(description)} characters (limit {DESCRIPTION_MAX})"
        )
        continue
      _, body = strip_title(body)
      body = rewrite_product_links(
        body,
        label,
        base_path,
        slugs,
        build,
        media,
        f"{asset_base}product/{site}/",
        shown,
      )
      slug = path.stem
      key = f"product/{site}/{slug}.md"
      build.files[key] = body.encode("utf-8")
      try:
        repo_path = path.resolve().relative_to(repo_root.resolve()).as_posix()
      except ValueError:
        repo_path = label
      build.pages.append(
        Page(
          site=site,
          layer="product",
          slug=slug,
          path=base_path if slug == "index" else f"{base_path}/{slug}",
          title=title,
          description=description or describe(body),
          section=meta.get("section") or None,
          order=position,
          updated=last_commit_date(path),
          body=key,
          source_url=f"{REPO_BLOB_URL}/{repo_path}",
        )
      )
      entries.append((position, slug, meta.get("section") or None))
    for name in sorted(shown):
      build.files[f"product/{site}/{name}"] = media[name].read_bytes()
    unused = sorted(set(media) - shown)
    if unused:
      build.warnings.append(f"{site}: images no page shows: {', '.join(unused)}")
    # A page's `section` groups it in the sidebar. Sections appear in the order
    # of their first page, and a site that names none keeps one untitled list.
    grouped: dict[str | None, list[str]] = {}
    for _, slug, section in sorted(entries, key=lambda e: (e[0], e[1])):
      grouped.setdefault(section, []).append(slug)
    build.collections.append(
      {
        "site": site,
        "layer": "product",
        "base_path": base_path,
        "sections": [
          {"title": title, "slugs": names} for title, names in grouped.items()
        ],
      }
    )


def product_media(site_dir: Path) -> dict[str, Path]:
  """Files under a site's ``images/``, keyed the way a page names them."""
  root = site_dir / "images"
  if not root.is_dir():
    return {}
  return {
    path.relative_to(site_dir).as_posix(): path
    for path in sorted(root.glob("**/*"))
    if path.is_file() and not path.name.startswith(".")
  }


def rewrite_product_links(
  text: str,
  label: str,
  base_path: str,
  slugs: set[str],
  build: Build,
  media: dict[str, Path] | None = None,
  media_base: str = "",
  shown: set[str] | None = None,
) -> str:
  def replace(match: re.Match[str]) -> str:
    bang, text_, target, title = match.groups()
    if target.startswith(TECHNICAL_URL):
      check_technical_link(label, target, build)
      return match.group(0)
    if target.startswith(("http://", "https://", "mailto:", "#")):
      return match.group(0)
    # A site path is a fine link and a broken image: nothing serves it.
    if target.startswith("/") and not bang:
      return match.group(0)
    if bang:
      if target not in (media or {}):
        build.errors.append(
          f"{label}: image must be a file under images/ that exists: {target}"
        )
        return match.group(0)
      if shown is not None:
        shown.add(target)
      return f"![{text_}]({media_base}{target}{title})"
    name, _, anchor = target.partition("#")
    if not name.endswith(".md") or "/" in name:
      build.errors.append(
        f"{label}: relative links must name a sibling page (x.md): {target}"
      )
      return match.group(0)
    slug = name[:-3]
    if slug not in slugs:
      build.errors.append(f"{label}: link to a page that does not exist: {target}")
      return match.group(0)
    path = base_path if slug == "index" else f"{base_path}/{slug}"
    return f"[{text_}]({path}{'#' + anchor if anchor else ''}{title})"

  return map_prose(text, lambda chunk: _LINK.sub(replace, chunk))


def check_technical_link(label: str, target: str, build: Build) -> None:
  """A product page's jump into the technical docs must land on a real heading.

  Product pages are public, so a dead jump-off is a defect, not a warning. The
  check needs the wiki built in the same run and is skipped without it.
  """
  if build.technical_anchors is None:
    return
  url, _, anchor = target.partition("#")
  path = url[len("https://robosystems.ai") :].rstrip("/")
  if path != TECHNICAL_BASE_PATH and not path.startswith(f"{TECHNICAL_BASE_PATH}/"):
    return
  anchors = build.technical_anchors.get(path)
  if anchors is None:
    build.errors.append(
      f"{label}: link to a technical page that does not exist: {target}"
    )
  elif anchor and anchor not in anchors:
    build.errors.append(f"{label}: no heading in {path} for #{anchor}: {target}")


# ── The catalog ─────────────────────────────────────────────────────────────


def catalog(build: Build) -> dict[str, object]:
  pages = sorted(build.pages, key=lambda p: (p.site, p.layer, p.order, p.slug))
  index: dict[str, object] = {
    "schema_version": SCHEMA_VERSION,
    "collections": build.collections,
    "pages": [page.record() for page in pages],
  }
  hasher = hashlib.sha256(json.dumps(index, sort_keys=True).encode("utf-8"))
  for key in sorted(build.files):
    hasher.update(key.encode("utf-8"))
    hasher.update(build.files[key])
  return {"digest": hasher.hexdigest(), **index}


def write_output(
  out_dir: Path, index: dict[str, object], files: dict[str, bytes]
) -> None:
  if out_dir.exists():
    shutil.rmtree(out_dir)
  for key, content in files.items():
    target = out_dir / key
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(content)
  (out_dir / "index.json").write_text(
    json.dumps(index, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
  )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
  parser = argparse.ArgumentParser(
    description="Build the public docs catalog from the wiki and docs/product",
    formatter_class=argparse.RawDescriptionHelpFormatter,
  )
  parser.add_argument("--wiki", type=Path, help="a checkout of robosystems.wiki")
  parser.add_argument("--product", type=Path, help="the docs/product directory")
  parser.add_argument(
    "--out", type=Path, required=True, help="output directory (replaced)"
  )
  parser.add_argument(
    "--asset-base",
    default=DEFAULT_ASSET_BASE,
    help=f"public URL the output is served under (default {DEFAULT_ASSET_BASE})",
  )
  return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
  args = parse_args(argv)
  asset_base = (
    args.asset_base if args.asset_base.endswith("/") else f"{args.asset_base}/"
  )
  build = Build()
  if args.wiki:
    build_wiki(args.wiki, asset_base, build)
  if args.product and args.product.is_dir():
    build_product(args.product, Path.cwd(), build, asset_base)
  if not build.pages and not build.errors:
    build.errors.append("nothing to publish: pass --wiki and/or --product")

  for warning in build.warnings:
    print(f"warning: {warning}", file=sys.stderr)
  if build.errors:
    for error in build.errors:
      print(f"error: {error}", file=sys.stderr)
    return 1

  index = catalog(build)
  write_output(args.out, index, build.files)
  print(f"{len(build.pages)} pages, {len(build.files)} files, digest {index['digest']}")
  return 0


if __name__ == "__main__":
  sys.exit(main())
