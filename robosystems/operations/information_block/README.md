# Information Block Operations

The Information Block subsystem provides a registry-driven, type-safe system for constructing and reading structured financial data blocks — schedules, statements, rollforwards, reconciliations, metrics, and policies.

**Architectural context**: `local/docs/specs/information-block.md` has the full spec. This README is the code-level orientation.

## What is an Information Block?

An Information Block is a molecular view of a financial structure: a `Structure` row (the skeleton), bundled with its `Element`s, `Association`s (connections), `Fact`s, `Rule`s, `VerificationResult`s, and `FactSet`. The `InformationBlockEnvelope` (in `models/api/information_block.py`) is the wire shape.

Every block has a `block_type` that determines how it's constructed and read. The registry maps `block_type` strings to handler modules.

## Directory Map

```
information_block/
├── __init__.py        # Public API: create_information_block, get/list, REGISTRY
├── types.py           # BlockTypeRegistryEntry dataclass, ConstructionMode literal
├── registry.py        # REGISTRY dict — one entry per block type, frozen at import
├── commands.py        # Generic create/update/delete dispatch — routes by block_type
├── reads.py           # get_information_block, list_information_blocks
├── envelope.py        # Shared ORM → Lite projection helpers used by every handler
├── schedule.py        # block_type='schedule' handler (declarative construction)
├── statement.py       # block_type='balance_sheet|income_statement|...' handlers (stub)
├── metric.py          # block_type='metric' handler (stub)
├── classify.py        # Scaffold for the association classifier (not yet implemented)
└── rules/
    ├── engine.py      # evaluate_rules_for_structure — entry point
    ├── evaluators.py  # Per-pattern dispatch (EqualTo, RollUp, Exists, CoExists, …)
    ├── expressions.py # Safe AST parser: $Variable substitution + whitelist node walk
    └── commands.py    # cmd_evaluate_rules — mounted as the evaluate-rules operation
```

## Construction Modes

Each registered block type declares one of three construction modes:

| Mode | Meaning | Current example |
|------|---------|-----------------|
| `declarative` | User declares mechanics + seed params; the system generates atoms | `schedule` |
| `compositional` | Atoms exist from report ingest; block is a view assembled at read time | `balance_sheet`, `income_statement`, `cash_flow_statement`, `equity_statement` |
| `derivative` | Facts are computed from other blocks at read time | `metric` |

The compositional family (statements) and the derivative family (metrics) raise `NotImplementedError` (→ HTTP 501) on the create / update / delete paths today — statements are produced via `create-report`, and the metric derivation evaluator is not yet implemented. Their `build_envelope` paths are fully wired and surface read envelopes normally.

## Adding a New Block Type

1. **Create a handler module** (e.g., `rollforward.py`) with:
   - `create(session, payload, created_by) -> str` — returns `structure_id`
   - `update(session, payload, updated_by) -> str`
   - `delete(session, payload, deleted_by) -> str`
   - `build_envelope(session, structure_id) -> InformationBlockEnvelope | None`

2. **Add a mechanics model** to `models/api/information_block.py` (e.g., `RollforwardMechanics`) and add it to the `ArtifactMechanics` discriminated union.

3. **Register the entry** in `registry.py` — add a `BlockTypeRegistryEntry` literal and insert it into `REGISTRY`. That's it: the generic REST ops (`create-information-block`, etc.) and MCP tools pick it up automatically via the registry.

4. **Widen the DB CHECK constraint** — add the new `structure_type` value to `migrations/extensions/versions/` and to `_widen_library_checks` in `db/extensions.py`.

## Envelope Assembly

`envelope.py` contains the shared ORM → Lite projectors that every handler uses to build the wire shape without duplicating mapping logic:

- `element_to_lite` / `association_to_connection` — atom projectors
- `load_classifications_for_associations` — single query over the junction, grouped in-memory (O(1) per association, no N+1)
- `load_rules_for_structure` — fetches structure-, element-, and association-scoped rules in one OR query
- `load_verification_results_for_structure` — ordered by `evaluated_at` desc
- `load_latest_fact_set_for_structure` — most recent FactSet for the structure
- `fact_to_lite` / `fact_set_to_lite` / `verification_result_to_lite` / `rule_to_lite`

Each block type's `build_envelope` function calls these helpers once per type of atom, then assembles the `InformationBlockEnvelope`. The pattern is: load structure → count runtime state → load atoms → call helpers → return envelope.

**N+1 note**: `list_information_blocks` calls `build_envelope` per row (~9 queries per block). Acceptable at current graph sizes; will need batching before it scales to dozens of blocks per graph.

## Rule Evaluation Engine

`rules/engine.py` is the entry point. Call:

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

The engine loads rules via `envelope.load_rules_for_structure` (so element- and association-scoped rules are included), binds `$Variable` names to fact values via qname lookup, dispatches to the per-pattern evaluator, writes one `VerificationResult` row per rule, and returns the written rows. `session.flush()` is called before returning; the caller owns `commit`.

**Binding semantics**: schedule facts are structure-scoped (`Fact.structure_id`). Statement facts are currently report-scoped (`Fact.report_id`, `structure_id=NULL`) — the engine falls back to the most recent matching report for non-schedule blocks until report-write paths stamp `structure_id` on every fact.

**Expression safety**: `expressions.py` rewrites `$Variable` → `_var_Name`, normalizes bare `=` → `==`, parses with `ast.parse(mode='eval')`, then walks the AST through a whitelist. `eval()` is never called — the AST is evaluated recursively by `_eval_arith`.

## Status of Pending Components

- **Rule evaluation engine**: pattern dispatchers and the safe expression parser are implemented and wired through `evaluate-rules`. Handles `EqualTo` / `RollUp` / `RollForward` / `Exists` / `CoExists` / `SumEquals`; other patterns return `skipped`.
- **Association classifier (`classify.py`)**: not yet implemented. The current SEC-side classifier lives in `adapters/sec/processors/classify.py` and writes graph-side Classification nodes; a Postgres-backed version that runs over OLTP `associations` + `elements` and produces `association_classifications` rows for any tenant has not been ported. The `classify.py` module reserves the import path so handlers have a stable hook.
- **FactSet expansion**: not yet implemented. Write paths do not yet stamp every fact with a `fact_set_id`, so `FactSetLite` on the envelope is `None` for blocks whose creation pre-dates FactSet stamping.
- **Metric derivation evaluator**: not yet implemented. `MetricMechanics` is registered as a typed union arm; the create / update / delete paths raise `NotImplementedError` and `build_envelope` returns the typed mechanics with `facts=[]`.

If `classify.py` grows into multiple files, promote it to `classify/` then — don't do it preemptively.
