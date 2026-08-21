# Information Block Operations

Registry-driven construction and reading of structured financial data blocks — schedules, statements, rollforwards, forecasts, disclosures, and metrics.

## What is an Information Block?

A molecular view of a financial structure: a `Structure` row (the skeleton) bundled with its `Element`s, `Association`s (connections), `Fact`s, `Rule`s, `VerificationResult`s, and `FactSet`. The `FactSet` carries a typed `provenance` field — the grounding axis (origin plus temporal stance) of the facts it bundles. `InformationBlockEnvelope` in `models/api/information_block.py` is the wire shape.

Every block has a `block_type` that determines how it is constructed and read. `registry.py` maps `block_type` strings to handler modules.

## Layout

| Module | Contents |
| ------ | -------- |
| `__init__.py` | Public API: `create_information_block`, get/list, `REGISTRY` |
| `types.py` | `BlockTypeRegistryEntry` dataclass, `ConstructionMode` literal |
| `registry.py` | `REGISTRY` dict — one entry per block type, frozen at import |
| `commands.py` | Generic create/update/delete dispatch, routed by `block_type` |
| `reads.py` | `get_information_block`, `list_information_blocks` |
| `envelope.py` | Shared ORM → Lite projectors used by every handler |
| `schedule.py`, `rollforward.py` | Declarative handlers |
| `forecast.py` | Declarative forecast scenarios (authored surface) |
| `forecast_compute.py` | `compute-forecast` — walks a scenario's driver cascade into forward FactSets |
| `forecast_articulation.py` | Balance-sheet roll, schedule projection, derived cash flow per forward month |
| `forecast_history.py` | Back-solves a scenario's levers from closed months |
| `statement.py` | Compositional statement handlers plus the server-computed rendering projection |
| `disclosure.py` | `regulatory_disclosure` handler, parameterised on the statement builder |
| `text_block.py` | Envelope builder for narrative (`Nonnumeric`) disclosure structures |
| `metric.py` | Derivative metric handler — renders the standing metric time series |
| `metrics.py` | `compute-metrics` / `assert-metrics` — the metric write paths |
| `chart.py` | Chart View projection (panels and series over a rendering) |
| `classify.py` | Reserved import path for the association classifier (not implemented) |
| `rules/` | Rule evaluation engine — `engine.py`, `evaluators.py`, `expressions.py`, `commands.py` |

## Construction modes

| Mode | Meaning | Block types |
| ---- | ------- | ----------- |
| `declarative` | The user declares mechanics plus seed params; the system generates atoms | `schedule`, `rollforward`, `forecast` |
| `compositional` | Atoms already exist; the block is a view assembled at read time | `balance_sheet`, `income_statement`, `cash_flow_statement`, `equity_statement`, `comprehensive_income`, `regulatory_disclosure` |
| `derivative` | Facts are computed from other blocks | `metric` |

Ten block types are registered. Not all of them are authored through `create-information-block`: statements are produced by `create-report`, disclosure structures are authored as vocabulary through `create-taxonomy-block`, and metrics are written by `compute-metrics` / `assert-metrics`. Those types install not-implemented create/update/delete handlers (HTTP 501) via `make_not_implemented_handler`, while their `build_envelope` paths are fully wired and serve read envelopes normally.

## Adding a block type

1. **Write a handler module** exposing:
   - `create(session, payload, created_by) -> str` (returns `structure_id`)
   - `update(session, payload, updated_by) -> str`
   - `delete(session, payload, deleted_by) -> str`
   - `build_envelope(session, structure_id) -> InformationBlockEnvelope | None`
2. **Add a mechanics model** to `models/api/information_block.py` and add it to the `ArtifactMechanics` discriminated union (existing arms: `ScheduleMechanics`, `RollforwardMechanics`, `ForecastMechanics`, `StatementMechanics`, `MetricMechanics`).
3. **Register the entry** in `registry.py` — declare a `BlockTypeRegistryEntry` and insert it into `REGISTRY`. The generic REST operations and the MCP tools pick it up from there; no further wiring.
4. **Widen the database CHECK constraint** — add the new `block_type` value in `migrations/extensions/versions/` and to `_widen_library_checks` in `db/extensions.py`.

## Envelope assembly

`envelope.py` holds the shared ORM → Lite projectors, so no handler duplicates mapping logic:

- `element_to_lite` / `elements_to_lites` / `association_to_connection` — atom projectors
- `load_classifications_for_associations` — one query over the junction, grouped in memory (no N+1)
- `load_rules_for_structure` — structure-, element-, and association-scoped rules in one OR query
- `load_verification_results_for_structure` — ordered by `evaluated_at` descending
- `load_latest_fact_set_for_structure`, `load_fact_set_by_id_for_structure`, `load_statement_fact_set_series`
- `fact_to_lite` / `fact_set_to_lite` / `verification_result_to_lite` / `rule_to_lite`
- `load_base_envelope_atoms` — the common load path for a structure's atoms

Each `build_envelope` follows the same pattern: load the structure, count runtime state, load atoms, call the helpers, assemble the envelope.

**N+1 note**: `list_information_blocks` calls `build_envelope` per row (roughly nine queries per block), so this is the path to batch if listing latency becomes a concern.

## Rule evaluation engine

```python
from robosystems.operations.information_block.rules.engine import evaluate_rules_for_structure

results = evaluate_rules_for_structure(
    session,
    structure_id,
    fact_set_id=...,      # optional — scope to a specific FactSet
    period_start=...,     # optional date bound
    period_end=...,       # optional date bound
    created_by="engine",
)
```

The engine loads rules via `envelope.load_rules_for_structure` (so element- and association-scoped rules are included), binds `$Variable` names to fact values by qname, dispatches to the per-pattern evaluator, writes one `VerificationResult` row per rule, and returns the written rows. It calls `session.flush()` before returning; **the caller owns the commit.**

**Binding**: every fact is structure-scoped (`Fact.structure_id`) and FactSet-anchored (`Fact.fact_set_id`). The engine filters on `structure_id`, or on `fact_set_id` when the caller pins one.

**Patterns**: `EqualTo` and `RollForward` (strict arithmetic equality with configurable tolerance), `RollUp` (`$Parent = Σ children`), `Exists`, `CoExists`, `SumEquals`. Any other pattern returns `skipped`.

**Expression safety**: `expressions.py` rewrites `$Variable` to `_var_Name`, normalizes a bare `=` to `==`, parses with `ast.parse(mode='eval')`, and walks the AST through a whitelist. **`eval()` is never called** — the tree is evaluated recursively by `_eval_arith`.

## FactSet construction

`operations/roboledger/fact_set.py::create_fact_set` is the single blessed writer. It validates a typed `FactProvenance` descriptor and writes `fact_sets.provenance`; `ProvenanceRequiredError` plus a `before_insert` model backstop reject any unstamped insert. Every producer — the report pivot, schedules, statement sets, text blocks, metrics, forecasts — routes through it, so every FactSet is stamped and `provenance` surfaces on the envelope as JSON. The union arms are `pivot`, `schedule`, `derived`, `asserted`, `document`, `forecast`, and `filed`.

## Not implemented

- **Association classifier (`classify.py`)** — the module reserves the import path so handlers have a stable hook, and exports nothing. The working SEC-side classifier lives in `adapters/sec/processors/classify.py` and writes graph-side Classification nodes; a PostgreSQL-backed equivalent that runs over OLTP `associations` and `elements` to produce `association_classifications` rows has not been ported.
- **Custom metric authoring** — `metric.py`'s create/update/delete handlers raise `NotImplementedError` (HTTP 501). The metric write surface is the seeded catalog plus `compute-metrics` / `assert-metrics`.
