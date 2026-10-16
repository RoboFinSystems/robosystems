# Technical docs

How RoboSystems is built, run and extended, for the developer, self-hoster or integrator: the API, the MCP server, the extensions, the data model, and running a stack of your own. They are published at [robosystems.ai/docs/technical](https://robosystems.ai/docs/technical). How to *use* the products lives in the [product docs](../product/README.md); one endpoint's request and response is in the API reference, generated from the OpenAPI spec.

One file per page. `Home.md` is the landing page, and the file name is the URL: `Quick-Start.md` is served at `/docs/technical/quick-start`. These pages lived in the GitHub wiki until October 2026 and kept their history in the move.

## A page

```markdown
# Quick Start

The first paragraph is the page's description in search results and link previews.
```

- The H1 is the title. There is no front matter.
- Add the page to `_Sidebar.md` under its section, or it lands under "Other" and the build warns.
- Link to a sibling page by file name (`[Period Close](Period-Close.md#blockers)`). A link to a page or heading that does not exist fails the build, and so does a link into the old wiki.
- Images go in `images/` and are shown with `![what it shows](images/name.png)`.
- Examples call the hosted API at `https://api.robosystems.ai`. Running a stack yourself is one page, [Local Development](Local-Development.md).
- Point at served sources (`GET /v1/offering`, `/openapi.json`, `.github/configs/graph.yml`) rather than restating volatile configuration.

## Publishing

The Publish Docs workflow (`.github/workflows/publish-docs.yml`) builds these pages and the product docs into one catalog on every push to `main` that touches either folder. `tests/scripts/test_technical_docs.py` runs the same build in CI, so a broken link fails the pull request rather than the publish. Preview locally with `just docs-build`.

A page's "last updated" date is its last commit's author date. A sweep that changes no meaning (a link format, a renamed term) carries a `Docs-Mechanical: true` trailer in its commit message, and the pages it touches keep their dates.
