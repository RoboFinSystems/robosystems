# Product docs

How to use RoboSystems, RoboLedger and RoboInvestor through Claude, ChatGPT or any MCP client, and what each needs to do the job well. Written for the person using the product and for the model working for them. How the platform is built, run and extended lives in the [technical documentation](https://robosystems.ai/docs/technical); the API reference is generated from the OpenAPI spec.

One folder per site, one file per page:

| Folder | Published at |
|---|---|
| `roboledger/` | `roboledger.ai/docs/{slug}` |
| `robosystems/` | `robosystems.ai/docs/guides/{slug}` |
| `roboinvestor/` | `roboinvestor.ai/docs/{slug}` |

A file named `index.md` is the site's docs landing page.

## A page

```markdown
---
title: Connect your books to Claude
description: One sentence, under 160 characters, for search results and link previews.
order: 1
---

Body in GitHub-flavored markdown. No H1: the title is rendered from the front matter.
```

- `title` and `order` are required. `description` falls back to the first paragraph.
- Link to a sibling page by file name (`[the close](month-end-close.md)`). Link to another site with its full URL.
- Name an MCP tool only where the reader has to recognise it. Every backticked tool name is checked against the server's tools in CI (`tests/scripts/test_product_docs.py`).
- Every page is a public claim. Describe what the product does today, never a feature that is planned.

## Publishing

The Publish Docs workflow (`.github/workflows/publish-docs.yml`) builds these pages and the wiki into one catalog on every push to `main` that touches this folder, and uploads it to the content CDN the sites read from. Preview the build locally with `just docs-build`.
