# Industry classification

Which industry a company is in, as one vocabulary shared by the SEC repository
and every tenant's entities. Not a reporting framework: it has no manifest and
is not copied into tenant schemas.

| File | What it holds |
| --- | --- |
| `industries.csv` | One row per node: `id`, `parent`, `level`, `label`, `carried`, `replaced_by` |
| `companies.csv` | One row per filer and industry: `key`, `cik`, `name`, `industry`, `primary`, `method`, `confidence`, `review` |

## The vocabulary

Four levels: sector > subsector > industry > sub-industry. A node's `id` is its
slug path (`health-care/biotechnology/clinical-stage`), so its ancestors are
the prefixes of its id.

- **Every subsector has an industry.** One that nobody has divided carries
  itself over as its single industry (`carried = yes`); dividing it later
  replaces that row with real industries.
- **An id is never deleted or reused.** A node that goes away keeps its row with
  `replaced_by` naming its successor, so an id stored on an entity always
  resolves.
- **Thin industries are fine.** Two or three similar companies are worth
  comparing, and they roll up to the subsector.

## The filers

Every company in the public filer index has one `primary` industry and any
number of others; a second industry needs a material share of revenue, and
rollups count the primary only. Funds are not classified here.

**The `cik` is the filer.** `key` (the ticker when the row was written) and
`name` are there so a reader can see which company a row was written about.
Neither is a source for a company's ticker or name: tickers are reassigned and
the SEC's names change, so anything that needs them reads the filer catalog and
joins here on `cik`.

| Column | Values |
| --- | --- |
| `method` | `filing` (read from its annual report), `recall` (placed from knowledge of the company), `crosswalk` (its SIC code alone, unjudged) |
| `confidence` | `high`, `medium`, `low` |
| `review` | `yes` where two independent readings disagreed |

SIC is a starting hint, never the answer.

## Changing it

Edit the CSVs, then check them:

```bash
uv run pytest tests/taxonomy/test_industries.py
```

The vocabulary is seeded into `public.industries` by a migration calling
`robosystems.taxonomy.industries.seed_industries`. After editing
`industries.csv`, add a migration that calls it again.
