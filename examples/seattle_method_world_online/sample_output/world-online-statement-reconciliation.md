# The World Online — Statement-Level Reconciliation

Diffs our rendered four-statement Report's **anchor totals** against Charlie Hoffman's **published XBRL reference instance** (`mini/ref-num/instance.xml`, the source of his `index2.html`). Complements the GL-pivot `reconcile.py` (which validates ingestion against `SummaryOfTransactions.csv`) at the rendered-statement level.

Our values are read from the **v2 graph-native bundle** (`world-online.jsonld` — `rs:Fact` nodes). Charlie's instance is labelled FY2022 (EUR); ours spans 2023→2028 — amounts are matched by **period position** (latest = current), since they tie regardless of the label.

## Scorecard

- **Anchors compared**: 7
- **Matching (current period, within $0.50)**: 7 / 7

| Anchor | Charlie (current) | Ours (current) | Δ | Charlie (prior) | Ours (prior) | |
|---|---:|---:|---:|---:|---:|:---:|
| Total Assets | $3,084,325.68 | $3,084,325.68 | $0.00 | $3,364,281.75 | $3,364,281.75 | ✓ |
| Total Liabilities & Equity | $3,084,325.68 | $3,084,325.68 | $(0.00) | $3,364,281.75 | $3,364,281.75 | ✓ |
| Net Income (Loss) | $(1,351,122.32) | $(1,351,122.32) | $0.00 | — | $(1,351,122.32) | ✓ |
| Receivables | $2,035,468.27 | $2,035,468.27 | $0.00 | $1,231,338.47 | $1,231,338.47 | ✓ |
| Property, Plant & Equipment | $1,245,567.16 | $1,245,567.16 | $0.00 | $1,266,995.32 | $1,266,995.32 | ✓ |
| Long-term Debt | $338,349.05 | $338,349.05 | $0.00 | $361,285.69 | $361,285.69 | ✓ |
| Cash & Equivalents | $(648,551.94) | $(648,551.94) | $0.00 | $398,937.76 | $398,937.76 | ✓ |

A ✓ means the rendered statement ties to Charlie's published reference for that anchor (within the published summary's per-cell rounding). The negative ending cash is **in Charlie's reference report too** — it is a property of the source dataset's tagging (financing cash legs tagged operating), not an ingestion error; see `README.md` §"Cash flow statement".

## How to reproduce

```bash
just demo-world-online                                 # full pipeline
just demo-world-online-statement-reconcile             # this report only
```
