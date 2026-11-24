# Driftline Coffee Roasters — Holon Validation (xbrlkit)

## Result: ✅ **Conforms to the holon format**

### `coffee-roaster-demo.holon.jsonld`

| Graph | Verdict | Triples | Shapes | Violations | Warnings |
|---|---|---:|---:|---:|---:|
| `#scene` | conforms | 1,917 | 12 | 0 | 0 |
| `#projection` | conforms | 3,535 | 8 | 0 | 0 |
| `#boundary` | conforms | 820 | 5 | 0 | 0 |
| `#lineage` | absent | | | | |
| dataset | conforms | 5,563 | 11 | 0 | 0 |

- **Report**: holds together — 86 facts, 26 of 26 calculations foot, 2 balance-sheet comparison(s) made

### `coffee-roaster-demo.lineage.holon.jsonld`

| Graph | Verdict | Triples | Shapes | Violations | Warnings |
|---|---|---:|---:|---:|---:|
| `#scene` | conforms | 1,973 | 12 | 0 | 0 |
| `#projection` | conforms | 3,535 | 8 | 0 | 0 |
| `#boundary` | conforms | 820 | 5 | 0 | 0 |
| `#lineage` | conforms | 29,069 | 13 | 0 | 0 |
| dataset | conforms | 34,687 | 11 | 0 | 0 |

- **Report**: holds together — 86 facts, 26 of 26 calculations foot, 2 balance-sheet comparison(s) made

Validated on the host with **xbrlkit** directly against the on-disk holons — no API, no container. Each named graph (`#scene`, what the report states; `#projection`, how it is arranged; `#boundary`, the roll-ups it must obey; `#lineage`, the ledger behind it) is checked against its own SHACL shapes, and the graphs together for references that cross them. Conformance is the claim about the *file*. The *report* line is a second check, on the report read back from it: every fact points at a concept, period and unit that exist, every calculation foots and the balance sheet balances. Neither says the report is complete or true; they say the file is what it claims to be and the report is consistent with itself.
