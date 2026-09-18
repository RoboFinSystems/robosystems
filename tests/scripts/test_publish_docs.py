"""Tests for the public docs catalog builder."""

import json
import subprocess

import pytest

from robosystems.scripts import publish_docs

ASSET_BASE = "https://assets.example.test/docs/"

SIDEBAR = """**[Home](Home)**

### Getting Started
- [Quick Start](Quick-Start)
- [Core Concepts](Core-Concepts)

### Operations
- [Graph Operations](Graph-Operations)
"""


def _wiki(tmp_path, pages, sidebar=SIDEBAR):
  wiki = tmp_path / "wiki"
  wiki.mkdir()
  (wiki / "_Sidebar.md").write_text(sidebar)
  (wiki / "_Footer.md").write_text("footer")
  for name, text in pages.items():
    (wiki / f"{name}.md").write_text(text)
  return wiki


def _pages(**overrides):
  pages = {
    "Home": "# Welcome to the Wiki!\n\nRoboSystems is a platform.\n",
    "Quick-Start": "# Quick Start\n\nStart here.\n\n## First Query\n\nRun it.\n",
    "Core-Concepts": "# Core Concepts\n\nThe vocabulary.\n",
    "Graph-Operations": "# Graph Operations\n\nOperations on graphs.\n",
  }
  pages.update(overrides)
  return pages


def _build_wiki(wiki):
  build = publish_docs.Build()
  publish_docs.build_wiki(wiki, ASSET_BASE, build)
  return build


def _page(build, slug):
  return next(p for p in build.pages if p.slug == slug)


@pytest.mark.unit
class TestGithubSlug:
  @pytest.mark.parametrize(
    ("heading", "slug"),
    [
      ("CI/CD & Deployment", "cicd--deployment"),
      ("Authenticated REST: `GET /whoami`", "authenticated-rest-get-whoami"),
      ("Atomic vs. Molecular", "atomic-vs-molecular"),
      ("Option 5: G.V() Graph IDE", "option-5-gv-graph-ide"),
      ("[Linked](Other) heading", "linked-heading"),
    ],
  )
  def test_matches_github(self, heading, slug):
    assert publish_docs.github_slug(heading) == slug

  def test_repeated_headings_get_numeric_suffixes(self):
    text = "## Setup\n\n## Setup\n\n```\n## Setup\n```\n"
    assert publish_docs.heading_anchors(text) == {"setup", "setup-1"}


@pytest.mark.unit
class TestMarkdownHelpers:
  def test_strip_title_removes_the_leading_h1(self):
    title, body = publish_docs.strip_title("\n# Quick Start\n\nBody text.\n")
    assert title == "Quick Start"
    assert body == "Body text.\n"

  def test_strip_title_leaves_a_body_without_one(self):
    assert publish_docs.strip_title("Body.\n") == (None, "Body.\n")

  def test_describe_skips_lists_tables_code_and_headings(self):
    text = "## Heading\n\n- item\n\n| a | b |\n\n```\ncode\n```\n\nThe **first** [real](X) paragraph.\n"
    assert publish_docs.describe(text) == "The first real paragraph."

  def test_describe_cuts_at_the_last_sentence_under_the_limit(self):
    first = "A" * 100 + "."
    text = f"{first} {'B' * 100}."
    assert publish_docs.describe(text) == first

  def test_describe_cuts_a_long_sentence_at_a_word(self):
    text = " ".join(["word"] * 60)
    result = publish_docs.describe(text)
    assert len(result) <= publish_docs.DESCRIPTION_MAX
    assert result.endswith("word…")

  def test_front_matter(self):
    meta, body = publish_docs.split_front_matter(
      '---\ntitle: "Connect: step one"\norder: 2\n---\n\nBody\n'
    )
    assert meta == {"title": "Connect: step one", "order": "2"}
    assert body == "Body\n"


@pytest.mark.unit
class TestWikiLinks:
  def test_page_links_become_site_paths(self, tmp_path):
    build = _build_wiki(
      _wiki(
        tmp_path,
        _pages(
          **{
            "Core-Concepts": "# Core Concepts\n\nSee [Quick Start](Quick-Start), "
            "[its query](Quick-Start#first-query) and [home](Home).\n"
          }
        ),
      )
    )
    body = build.files["technical/core-concepts.md"].decode()
    assert "[Quick Start](/docs/technical/quick-start)" in body
    assert "[its query](/docs/technical/quick-start#first-query)" in body
    assert "[home](/docs/technical)" in body
    assert build.errors == []
    assert build.warnings == []

  def test_absolute_wiki_urls_are_rewritten(self, tmp_path):
    text = (
      "# Core Concepts\n\n- [Wiki](https://github.com/RoboFinSystems/robosystems/wiki)\n"
      "- [QS](https://github.com/RoboFinSystems/robosystems/wiki/Quick-Start)\n"
      "- [Home anchor](https://github.com/RoboFinSystems/robosystems/wiki#overview)\n"
      "- [Not the wiki](https://github.com/RoboFinSystems/robosystems/wikis)\n"
    )
    pages = _pages(
      **{"Core-Concepts": text, "Home": "# Welcome\n\nIntro.\n\n## Overview\n"}
    )
    build = _build_wiki(_wiki(tmp_path, pages))
    body = build.files["technical/core-concepts.md"].decode()
    assert "[Wiki](/docs/technical)" in body
    assert "[QS](/docs/technical/quick-start)" in body
    assert "[Home anchor](/docs/technical#overview)" in body
    assert "[Not the wiki](https://github.com/RoboFinSystems/robosystems/wikis)" in body
    assert build.errors == []
    assert build.warnings == []

  def test_code_and_external_links_are_untouched(self, tmp_path):
    text = (
      "# Core Concepts\n\n"
      "Inline `[x](Quick-Start)` stays. [Repo](https://github.com/RoboFinSystems/robosystems/blob/main/README.md) stays.\n\n"
      "```markdown\n[x](Missing-Page)\n```\n"
    )
    build = _build_wiki(_wiki(tmp_path, _pages(**{"Core-Concepts": text})))
    body = build.files["technical/core-concepts.md"].decode()
    assert "`[x](Quick-Start)`" in body
    assert "[x](Missing-Page)" in body
    assert "blob/main/README.md" in body
    assert build.errors == []

  def test_images_point_at_the_cdn_and_ship(self, tmp_path):
    wiki = _wiki(
      tmp_path,
      _pages(**{"Core-Concepts": "# Core Concepts\n\n![viz](images/viz.png)\n"}),
    )
    (wiki / "images").mkdir()
    (wiki / "images" / "viz.png").write_bytes(b"png")
    build = _build_wiki(wiki)
    body = build.files["technical/core-concepts.md"].decode()
    assert f"![viz]({ASSET_BASE}technical/images/viz.png)" in body
    assert build.files["technical/images/viz.png"] == b"png"

  def test_a_link_to_a_missing_page_is_an_error(self, tmp_path):
    build = _build_wiki(
      _wiki(
        tmp_path,
        _pages(**{"Core-Concepts": "# Core Concepts\n\n[Gone](Renamed-Page)\n"}),
      )
    )
    assert build.errors == [
      "Core-Concepts.md: link to a wiki page that does not exist: Renamed-Page"
    ]

  def test_an_anchor_with_no_heading_is_a_warning(self, tmp_path):
    build = _build_wiki(
      _wiki(
        tmp_path,
        _pages(
          **{
            "Core-Concepts": "# Core Concepts\n\n[x](#nowhere) [y](Quick-Start#gone)\n"
          }
        ),
      )
    )
    assert build.errors == []
    assert build.warnings == [
      "Core-Concepts.md: no heading in Core-Concepts for #nowhere",
      "Core-Concepts.md: no heading in Quick-Start for #gone",
    ]


@pytest.mark.unit
class TestWikiCatalog:
  def test_pages_sections_and_home(self, tmp_path):
    build = _build_wiki(_wiki(tmp_path, _pages()))
    home = _page(build, "index")
    assert home.path == "/docs/technical"
    assert home.title == "Technical documentation"
    assert home.description == "RoboSystems is a platform."
    assert "Welcome" not in build.files["technical/index.md"].decode()

    quick = _page(build, "quick-start")
    assert (quick.title, quick.section, quick.order) == (
      "Quick Start",
      "Getting Started",
      1,
    )
    assert (
      quick.source_url
      == "https://github.com/RoboFinSystems/robosystems/wiki/Quick-Start"
    )
    assert build.collections == [
      {
        "site": "robosystems",
        "layer": "technical",
        "base_path": "/docs/technical",
        "sections": [
          {"title": "Getting Started", "slugs": ["quick-start", "core-concepts"]},
          {"title": "Operations", "slugs": ["graph-operations"]},
        ],
      }
    ]

  def test_pages_missing_from_the_sidebar_land_in_other(self, tmp_path):
    pages = _pages(**{"Orphan-Page": "# Orphan\n\nAlone.\n"})
    build = _build_wiki(_wiki(tmp_path, pages))
    assert build.collections[0]["sections"][-1] == {
      "title": "Other",
      "slugs": ["orphan-page"],
    }
    assert build.warnings == ["_Sidebar.md: pages not in the sidebar: Orphan-Page"]

  def test_a_sidebar_link_to_a_missing_page_is_an_error(self, tmp_path):
    build = _build_wiki(
      _wiki(tmp_path, _pages(), sidebar=SIDEBAR + "- [Gone](Gone-Page)\n")
    )
    assert (
      "_Sidebar.md: link to a wiki page that does not exist: Gone-Page" in build.errors
    )

  def test_updated_is_the_last_commit_date(self, tmp_path):
    wiki = _wiki(tmp_path, _pages())
    env = {
      "GIT_AUTHOR_NAME": "t",
      "GIT_AUTHOR_EMAIL": "t@example.test",
      "GIT_COMMITTER_NAME": "t",
      "GIT_COMMITTER_EMAIL": "t@example.test",
      "GIT_COMMITTER_DATE": "2026-01-02T03:04:05-05:00",
      "GIT_AUTHOR_DATE": "2026-01-02T03:04:05-05:00",
      "PATH": "/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin",
    }
    subprocess.run(["git", "init", "-q"], cwd=wiki, check=True, env=env)
    subprocess.run(["git", "add", "."], cwd=wiki, check=True, env=env)
    subprocess.run(["git", "commit", "-qm", "init"], cwd=wiki, check=True, env=env)
    build = _build_wiki(wiki)
    assert _page(build, "quick-start").updated == "2026-01-02T03:04:05-05:00"


@pytest.mark.unit
class TestProductPages:
  def _product(self, tmp_path, pages):
    root = tmp_path / "docs" / "product" / "roboledger"
    root.mkdir(parents=True)
    for name, text in pages.items():
      (root / name).write_text(text)
    return tmp_path / "docs" / "product"

  def test_front_matter_paths_and_sibling_links(self, tmp_path):
    product = self._product(
      tmp_path,
      {
        "connect.md": "---\ntitle: Connect your books\ndescription: How to connect.\norder: 1\n---\n\n"
        "Next, see [the close](close.md#review) or [RoboSystems](https://robosystems.ai/docs/guides).\n",
        "close.md": "---\ntitle: Close the month\norder: 2\n---\n\n# Close the month\n\nThe close.\n",
        "README.md": "conventions, not a page",
      },
    )
    build = publish_docs.Build()
    publish_docs.build_product(product, tmp_path, build)
    assert build.errors == []
    connect = _page(build, "connect")
    assert (connect.path, connect.title, connect.description, connect.order) == (
      "/docs/connect",
      "Connect your books",
      "How to connect.",
      1,
    )
    assert connect.source_url.endswith("/blob/main/docs/product/roboledger/connect.md")
    body = build.files["product/roboledger/connect.md"].decode()
    assert "[the close](/docs/close#review)" in body
    assert "[RoboSystems](https://robosystems.ai/docs/guides)" in body
    close = _page(build, "close")
    assert close.description == "The close."
    assert (
      "# Close the month" not in build.files["product/roboledger/close.md"].decode()
    )
    assert build.collections == [
      {
        "site": "roboledger",
        "layer": "product",
        "base_path": "/docs",
        "sections": [{"title": None, "slugs": ["connect", "close"]}],
      }
    ]

  def test_missing_title_order_and_broken_links_are_errors(self, tmp_path):
    product = self._product(
      tmp_path,
      {
        "a.md": "---\norder: 1\n---\n\nBody\n",
        "b.md": "---\ntitle: B\n---\n\nBody\n",
        "c.md": "---\ntitle: C\norder: 3\n---\n\n[x](gone.md) [y](../other/page.md)\n",
        "d.md": f"---\ntitle: D\norder: 4\ndescription: {'x' * 161}\n---\n\nBody\n",
      },
    )
    build = publish_docs.Build()
    publish_docs.build_product(product, tmp_path, build)
    assert build.errors == [
      "roboledger/a.md: front matter needs a title",
      "roboledger/b.md: front matter needs an integer order",
      "roboledger/c.md: link to a page that does not exist: gone.md",
      "roboledger/c.md: relative links must name a sibling page (x.md): ../other/page.md",
      "roboledger/d.md: description is 161 characters (limit 160)",
    ]

  def test_images_point_at_the_cdn_and_ship(self, tmp_path):
    product = self._product(
      tmp_path,
      {
        "plan.md": "---\ntitle: Plan\norder: 1\n---\n\n"
        '![The Plan page](images/plan.png "The grid")\n\n'
        "![A clip](images/clips/plan.mp4)\n\n"
        "![Elsewhere](https://example.test/x.png)\n\n"
        "```\n![not a link](images/gone.png)\n```\n",
      },
    )
    images = product / "roboledger" / "images"
    (images / "clips").mkdir(parents=True)
    (images / "plan.png").write_bytes(b"png")
    (images / "clips" / "plan.mp4").write_bytes(b"mp4")
    (images / ".DS_Store").write_bytes(b"junk")
    build = publish_docs.Build()
    publish_docs.build_product(product, tmp_path, build, ASSET_BASE)
    assert build.errors == []
    assert build.warnings == []
    body = build.files["product/roboledger/plan.md"].decode()
    assert (
      f'![The Plan page]({ASSET_BASE}product/roboledger/images/plan.png "The grid")'
      in body
    )
    assert f"![A clip]({ASSET_BASE}product/roboledger/images/clips/plan.mp4)" in body
    assert "![Elsewhere](https://example.test/x.png)" in body
    assert "![not a link](images/gone.png)" in body
    assert build.files["product/roboledger/images/plan.png"] == b"png"
    assert build.files["product/roboledger/images/clips/plan.mp4"] == b"mp4"
    assert "product/roboledger/images/.DS_Store" not in build.files

  def test_a_missing_or_misplaced_image_is_an_error(self, tmp_path):
    product = self._product(
      tmp_path,
      {
        "a.md": "---\ntitle: A\norder: 1\n---\n\n"
        "![gone](images/gone.png) ![outside](../shot.png) ![rooted](/images/shot.png)\n"
        "[a site path](/pricing) still links.\n",
      },
    )
    build = publish_docs.Build()
    publish_docs.build_product(product, tmp_path, build)
    assert build.errors == [
      "roboledger/a.md: image must be a file under images/ that exists: images/gone.png",
      "roboledger/a.md: image must be a file under images/ that exists: ../shot.png",
      "roboledger/a.md: image must be a file under images/ that exists: /images/shot.png",
    ]

  def test_an_image_no_page_shows_is_a_warning_and_stays_home(self, tmp_path):
    product = self._product(
      tmp_path, {"a.md": "---\ntitle: A\norder: 1\n---\n\nNo pictures.\n"}
    )
    images = product / "roboledger" / "images"
    images.mkdir()
    (images / "old.png").write_bytes(b"png")
    build = publish_docs.Build()
    publish_docs.build_product(product, tmp_path, build)
    assert build.errors == []
    assert build.warnings == ["roboledger: images no page shows: images/old.png"]
    assert "product/roboledger/images/old.png" not in build.files

  def test_sections_group_the_sidebar_in_page_order(self, tmp_path):
    def page(title, order, section=None):
      line = f"section: {section}\n" if section else ""
      return f"---\ntitle: {title}\norder: {order}\n{line}---\n\nBody.\n"

    product = self._product(
      tmp_path,
      {
        "index.md": page("Docs", 0),
        "connect.md": page("Connect", 1, "Get started"),
        "plan.md": page("Plan", 3, "Work with your books"),
        "needs.md": page("Needs", 2, "Get started"),
        "late.md": page("Late", 9, "Get started"),
      },
    )
    build = publish_docs.Build()
    publish_docs.build_product(product, tmp_path, build)
    assert build.errors == []
    assert build.collections[0]["sections"] == [
      {"title": None, "slugs": ["index"]},
      {"title": "Get started", "slugs": ["connect", "needs", "late"]},
      {"title": "Work with your books", "slugs": ["plan"]},
    ]
    assert _page(build, "plan").section == "Work with your books"

  def test_an_unknown_site_is_an_error(self, tmp_path):
    root = tmp_path / "product" / "nowhere"
    root.mkdir(parents=True)
    build = publish_docs.Build()
    publish_docs.build_product(tmp_path / "product", tmp_path, build)
    assert build.errors and "unknown site 'nowhere'" in build.errors[0]


@pytest.mark.unit
class TestMain:
  def test_writes_the_catalog_and_a_stable_digest(self, tmp_path, capsys):
    wiki = _wiki(tmp_path, _pages())
    out = tmp_path / "out"
    assert (
      publish_docs.main(
        ["--wiki", str(wiki), "--out", str(out), "--asset-base", ASSET_BASE]
      )
      == 0
    )
    index = json.loads((out / "index.json").read_text())
    assert index["schema_version"] == 1
    assert [p["slug"] for p in index["pages"]] == [
      "index",
      "quick-start",
      "core-concepts",
      "graph-operations",
    ]
    assert (out / "technical" / "quick-start.md").read_text().startswith("Start here.")

    assert (
      publish_docs.main(
        ["--wiki", str(wiki), "--out", str(out), "--asset-base", ASSET_BASE]
      )
      == 0
    )
    assert json.loads((out / "index.json").read_text())["digest"] == index["digest"]

    (wiki / "Core-Concepts.md").write_text("# Core Concepts\n\nChanged.\n")
    assert (
      publish_docs.main(
        ["--wiki", str(wiki), "--out", str(out), "--asset-base", ASSET_BASE]
      )
      == 0
    )
    assert json.loads((out / "index.json").read_text())["digest"] != index["digest"]

  def test_errors_fail_without_writing(self, tmp_path, capsys):
    wiki = _wiki(tmp_path, _pages(**{"Core-Concepts": "# C\n\n[x](Nope)\n"}))
    out = tmp_path / "out"
    assert publish_docs.main(["--wiki", str(wiki), "--out", str(out)]) == 1
    assert not out.exists()
    assert "does not exist: Nope" in capsys.readouterr().err

  def test_nothing_to_publish_is_an_error(self, tmp_path):
    assert publish_docs.main(["--out", str(tmp_path / "out")]) == 1
