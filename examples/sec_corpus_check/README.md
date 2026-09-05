# SEC corpus check

Load a known set of filings through the SEC pipeline on the local stack, then
audit what the text index holds against what the parsers produced. Built for the
Filing Ladder's 26-filing corpus, whose `bin/check_text_layer.py` checks the
parsers (`xbrlkit.text`) against each filing's own text-block facts; this checks
the platform's index against those parsers, so a parse defect and an index
defect are told apart.

```bash
just sec-corpus-check load    --corpus ../filing-ladder/data
just sec-corpus-check audit   --corpus ../filing-ladder/data --check text-layer.json
just sec-corpus-check probe   --corpus ../filing-ladder/data
just sec-corpus-check reindex --corpus ../filing-ladder/data
```

The corpus directory holds one folder per accession with a `meta.json`
(`accession`, `cik`, `form`, `filing_date`, `ticker`) — the layout
`filing-ladder materialize` writes.

| Command | What it does |
|---|---|
| `load` | `just sec-load`'s stages, with the download stage selecting filers **by CIK per quarter** — a delisted filer no longer resolves through the ticker map, and an empty filter would download the whole quarter. Resets the local SEC graph first, as the pipeline does. |
| `audit` | For each accession in OpenSearch: the section sets and part counts against the ladder check's JSON (`--check`), the part chains (`part` 1..n, `parent_document_id`, `next_document_id`), the metadata the index assets add (ticker, name, CIK, form, fiscal year), embeddings on every document, and one CDN copy per source fetched. Disclosure documents whose fact was stored inline (short, tag-free) have no CDN copy by design. |
| `probe` | Two searches per filing through the API, the way an agent would ask — the MD&A Item and the accounting-policies disclosure — reading the top hit's section. |
| `reindex` | Force re-indexes both source types for the corpus quarters, to re-run `audit` after a parser change without reloading. |

`audit` and `probe` exit non-zero when anything is flagged. `probe` needs
`just demo-user` and `just demo-sec-subscribe`.

Found with this on 2026-09-05: a ticker-less filer indexed under the string
`<NA>` (#1357), beside the two parser defects the ladder's check found
(xbrlkit #24).
