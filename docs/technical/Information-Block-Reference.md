# Information Block Reference

The field-level reference for Information Blocks: the envelope, each block type's mechanics and operations, the rule patterns, and the view projections. It covers every envelope field, which operations each type supports, and which rules evaluate today. For the concepts behind it, read [Information Blocks](Information-Blocks.md) first.

Names below are the REST and MCP spelling (snake_case). GraphQL serves the same fields in camelCase (`block_type` → `blockType`, `fact_set` → `factSet`). The live reference at [robosystems.ai/docs/extensions](https://robosystems.ai/docs/extensions) is the source of truth for exact types.

## Table of Contents

- [The Envelope](#the-envelope)
- [Atom Shapes](#atom-shapes)
- [FactSets and Provenance](#factsets-and-provenance)
- [Information Model Vocabulary](#information-model-vocabulary)
- [The Registry](#the-registry)
- [Per-Type Mechanics](#per-type-mechanics)
- [Rule Patterns](#rule-patterns)
- [View Projection Status](#view-projection-status)
- [Reads](#reads)
- [Gotchas](#gotchas)
- [Related Documentation](#related-documentation)
- [Support](#support)

## The Envelope

`InformationBlockEnvelope` is the one wire shape for every block type. It is returned by `create-information-block` and `update-information-block` (inside the `OperationEnvelope`'s `result`), by `informationBlock` / `informationBlocks` in GraphQL, by each item of `reportPackage`, and by the MCP tools `get-information-block` and `list-information-blocks`.

| Field | Type | Meaning |
| --- | --- | --- |
| `id` | string | The Structure id (`struct_…`). The block's identity across every read and write. |
| `block_type` | string | The registry discriminator. See [The Registry](#the-registry). |
| `name` | string | The block's own name, e.g. "Office Building Depreciation". |
| `display_name` | string | The registry label for the type, e.g. "Schedule". |
| `category` | string | The registry grouping: `Close`, `Reporting`, or `Planning`. |
| `taxonomy_id` | string \| null | The taxonomy the Structure belongs to. |
| `taxonomy_name` | string \| null | Its display name. |
| `disclosure_id` | string \| null | Qname of the named Disclosure the block corresponds to (e.g. `disclosures:BalanceSheet`), when one is mapped. Null for tenant-authored blocks without one. |
| `information_model` | object | `concept_arrangement` and `member_arrangement`. See [Information Model Vocabulary](#information-model-vocabulary). |
| `artifact.topic` | string \| null | The block's human-readable topic. |
| `artifact.renderer_note` | string \| null | A presentation note such as "in thousands". |
| `artifact.template` | object \| null | Reserved for reusable layouts; always null on the types that ship today. |
| `artifact.mechanics` | object | The typed per-type payload, discriminated on `kind`. See [Per-Type Mechanics](#per-type-mechanics). |
| `elements` | list | The accounts and concepts the block touches. See [Atom Shapes](#atom-shapes). |
| `connections` | list | The associations between those elements. |
| `facts` | list | The facts of the bound FactSet. |
| `rules` | list | The verification and derivation rules attached to the block. |
| `dimensions` | list | Reserved; empty today. |
| `fact_set` | object \| null | The FactSet this envelope binds. Null when the Structure has none yet. |
| `verification_results` | list | One row per evaluated rule, from the last evaluation. |
| `verification_summary` | object \| null | Counts over those results. Null until the block has been evaluated. |
| `view` | object | Server-side projections: `rendering` and `chart`. See [View Projection Status](#view-projection-status). |

## Atom Shapes

**Element** (`elements[]`): `id`, `qname` (null for tenant chart-of-accounts accounts), `name`, `code`, `element_type`, `is_abstract`, `is_monetary`, `balance_type` (`debit` / `credit`), `period_type` (`instant` / `duration`), `item_type` (the value-domain format family), `documentation`.

**Connection** (`connections[]`): `id`, `from_element_id`, `to_element_id`, `association_type` (`presentation`, `calculation`, `mapping`, `equivalence`, `general-special`, `essence-alias`), `arcrole`, `order_value`, `weight`, and `classifications[]` (each `id`, `category`, `identifier`, `is_primary`, `confidence`, `source`).

**Fact** (`facts[]`):

| Field | Meaning |
| --- | --- |
| `id`, `element_id` | The fact and the element it reports |
| `element_name`, `element_qname` | Denormalized from the element, so a consumer can render rows without a join |
| `value` | The numeric value, in the unit (dollars for `USD`, not cents). Null for text facts. |
| `text_value`, `content_type` | The text payload and its MIME type, for text-block facts |
| `fact_type` | `Numeric` or `Nonnumeric` |
| `period_start`, `period_end`, `period_type` | The period; `period_start` is null for instants |
| `unit` | Default `USD` |
| `fact_scope` | `historical` (before an onboarding watermark) or `in_scope` |
| `fact_set_id` | The FactSet the fact belongs to |

**Rule** (`rules[]`): `id`, `rule_category`, `rule_pattern` or `rule_check_kind` (exactly one is set), `rule_expression`, `rule_target` (`target_kind`: `structure` / `element` / `association` / `taxonomy`, plus `target_ref_id`), `rule_variables[]` (`variable_name`, `variable_qname`, `variable_element_id`), `rule_message`, `rule_severity` (`info` / `warning` / `error`), `rule_origin`. See [Rule Patterns](#rule-patterns).

**Verification result** (`verification_results[]`): `id`, `rule_id`, `structure_id`, `fact_set_id`, `status` (`pass` / `fail` / `error` / `skipped`), `message`, `period_start`, `period_end`, `evaluated_at`.

**Verification summary**: `total`, `passed`, `failed`, `errored`, `skipped`, and `by_category[]` with the same counts per `category`.

## FactSets and Provenance

`fact_set` carries `id` (`fs_…`), `structure_id`, `period_start`, `period_end`, `factset_type`, `entity_id`, `report_id`, `scenario_id`, and `provenance`.

| `factset_type` | Written by |
| --- | --- |
| `report` | Statement sets: a Report run (`create-report`, `regenerate-report`), the canonical sets a period close stamps, and the forward months `compute-forecast` writes under a scenario |
| `schedule` | Schedule creation; one set spanning the schedule's window |
| `disclosure` | Narrative facts bound to a disclosure note with `bind-text-block` |
| `metric` | `compute-metrics` and `assert-metrics`, one set per metric structure and period end |
| `custom` | A forecast scenario's own authored assertions, and a reconciliation's comparison for each period end |

`report_id` points at the owning Report when the set belongs to a package. `scenario_id` is null for actuals and holds a forecast block's id for that scenario's sets.

`provenance` is a typed descriptor discriminated on `origin`:

| `origin` | Carries | Meaning |
| --- | --- | --- |
| `pivot` | `mapping_id`, `period`, `arc_type`, `posting_filter` | Pivoted from the ledger through a chart-of-accounts mapping |
| `schedule` | `structure_id`, `method`, `params`, `period_index` | Generated by a schedule from its method and parameters |
| `derived` | `formula`, `computation`, `source_fact_ids` | Computed from other facts |
| `asserted` | `source_system`, `asserted_by`, `basis_note` | Supplied from outside; also used for a schedule's custom `periodic_amounts` curve |
| `document` | `document_id`, `section_id`, `content_hash`, `asserted_by` | Text bound from a platform document |
| `forecast` | `scenario_structure_id`, `base_period`, `month_index`, `drivers` | Computed by a forecast scenario |
| `filed` | `source`, `accession`, `filing_date`, `filer_cik`, `form` | Taken from a regulatory filing |
| `observed` | `source`, `method`, `as_of`, `observed_at`, `connection_id`, `basis` | Balances read from a source outside the ledger for a reconciliation: the synced accounting system, the tenant's schedules, or a recorded statement. `observed_at` says when, since a later reading of the same date can differ. |

## Information Model Vocabulary

`concept_arrangement` names how the block's concepts relate:

- Canonical: `set`, `roll_up`, `roll_forward`, `roll_forward_info`, `adjustment`, `variance`, `arithmetic`, `text_block`
- Text-block and detail levels: `level1_textblock`, `level2_textblock`, `level3_textblock`, `level4_detail`, `table_equivalent_textblock`
- Pseudo-patterns: `grid`, `compound_fact`

A disclosure whose arrangement is one of the text-block values (`text_block`, `level1_textblock` through `level3_textblock`, `table_equivalent_textblock`) holds narrative facts and renders as text rows; `level4_detail` is a numeric detail table.

`member_arrangement` names how dimension members aggregate, from none to full: `is_a`, `whole_part`, `nested_whole_part`, `two_dimension_aggregation`, `complex_aggregating_whole_part`. Null for blocks without a hypercube.

## The Registry

One registry maps each `block_type` to its construction mode, default arrangements, mechanics class, and handlers. `create-information-block`, `update-information-block`, and `delete-information-block` take a body of `{ "block_type": …, "payload": … }` and dispatch through it.

| `block_type` | Display | Category | Mode | Mechanics `kind` | Default arrangement | create / update / delete | How it is actually made |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `schedule` | Schedule | Close | declarative | `closing_entry_generator` | `roll_forward` | yes / yes / yes | `create-information-block` |
| `rollforward` | Rollforward | Reporting | declarative | `rollforward` | `roll_forward` | yes / yes / yes | `create-information-block` |
| `forecast` | Forecast | Planning | declarative | `forecast` | `set` | yes / yes / yes | `create-information-block`, then `compute-forecast` |
| `balance_sheet` | Balance Sheet | Reporting | compositional | `statement_renderer` | `roll_up` / `whole_part` | 501 | `create-report`, or stamped by `close-period` |
| `income_statement` | Income Statement | Reporting | compositional | `statement_renderer` | `roll_up` / `whole_part` | 501 | same |
| `cash_flow_statement` | Cash Flow Statement | Reporting | compositional | `statement_renderer` | `roll_up` / `whole_part` | 501 | same |
| `equity_statement` | Equity Statement | Reporting | compositional | `statement_renderer` | `roll_up` / `whole_part` | 501 | same |
| `comprehensive_income` | Statement of Comprehensive Income | Reporting | compositional | `statement_renderer` | `roll_up` / `whole_part` | 501 | same |
| `regulatory_disclosure` | Disclosure | Reporting | compositional | `statement_renderer` | `roll_up` | 501 | Structure via `create-taxonomy-block`; facts via `create-report` (text blocks via `bind-text-block`) |
| `metric` | Metric | Reporting | derivative | `metric` | `arithmetic` | 501 | Structure via `create-taxonomy-block`; facts via `compute-metrics` or `assert-metrics` |
| `reconciliation` | Reconciliation | Close | derivative | `reconciliation` | `adjustment` | 501 | `refresh-reconciliations` (or `record-statement-balance` for a statement); policy via `set-reconciliation-policy` |

A 501 carries a message naming the right path. An unknown `block_type` is a 422.

The statement Structures are seeded from the taxonomy library and cannot be deleted per tenant; to remove their facts, delete the originating Report. On the library graph, only the statement types are listed; schedules, rollforwards, forecasts, disclosures, metrics, and reconciliations exist only on tenant graphs.

## Per-Type Mechanics

### Schedule

A closing-entry generator: one fact per element per month across a window, and a draft closing entry each month from its entry template.

`create-information-block` payload (`block_type: "schedule"`):

| Field | Required | Meaning |
| --- | --- | --- |
| `name` | yes | Schedule name |
| `element_ids` | yes | Chart-of-accounts element ids the schedule touches, usually the debit and credit ids of the template |
| `period_start`, `period_end` | yes | The window; one period per month, at most 600 months |
| `monthly_amount` | yes | Amount per month, **integer cents**. Ignored when `periodic_amounts` is set. |
| `entry_template` | yes | See below |
| `schedule_metadata` | no | See below |
| `taxonomy_id` | no | Created if omitted |
| `closed_through` | no | Onboarding watermark. Facts ending on or before it are `historical` and their monthly obligations are voided, so the close starts drafting at the first open period. Set it to the last day of the calendar's closed-through month; omitting it when earlier periods exist leaves those periods as obligations that block the first close. |
| `source_transaction_id` | no | Free-form reference to the originating transaction, kept for audit |

`entry_template`: `debit_element_id`, `credit_element_id` (chart-of-accounts ids, not qnames; one pair per schedule, so model a multi-account entry as several schedules), `entry_type` (`standard`, `adjusting`, `closing` (default), `reversing`), `memo_template` (`{structure_name}` is replaced), `auto_reverse` (post a reversing entry on the first day of the next period).

`schedule_metadata`:

| Field | Meaning |
| --- | --- |
| `method` | `straight_line` (default) spreads `monthly_amount` evenly, the last period absorbing rounding. `custom` uses `periodic_amounts`. Any other value is a label only. |
| `original_amount` | Cost basis, integer cents. When set, the platform adds a `SumEquals` rule proving the periodic amounts total this basis less `residual_value`. |
| `residual_value` | Salvage value, integer cents. The schedule expenses `original_amount` less this, so net book value ends at it. Requires `original_amount`, and must be less than it. |
| `useful_life_months` | Useful life |
| `asset_element_id` | Balance-sheet asset for a net-book-value cross-reference |
| `booked_on` | The date the cost went on the books, when that is before the first period: a policy paid in December that starts amortizing in January. From this date to the first period the schedule carries its full cost, so the schedule reconciliation does not report that balance as unscheduled. Omit it when the cost is booked in the first period. |
| `periodic_amounts` | Explicit per-month amounts in integer cents, for uneven curves (effective-interest amortization, day-count accrual, variable lease payments). Length must equal the number of months in the window, every entry must be non-negative, and the sum must equal `original_amount` less `residual_value` exactly. The resulting FactSet's provenance is `asserted`. |

`ScheduleMechanics` on the envelope: `kind: "closing_entry_generator"`, `entry_template`, `schedule_metadata`, and `periods_with_entries` (months that already have a closing entry, computed at read time).

Update takes `structure_id` plus any of `name`, `entry_template`, `schedule_metadata`; delete takes `structure_id`. Two further operations change a live schedule: `terminate-schedule` (end it early with a `new_end_date` and a `reason`; removes the facts and voids the obligations past that date) and `rebuild-schedule` (re-run the generator in place from the stored definition, keeping the structure id: the old facts, rules, and pending obligations are replaced, and the historical/in-scope split is re-derived from the current close state). How schedule drafts post is in [Period Close](Period-Close.md).

### Rollforward

Decomposes one balance-sheet account's period change across declared flow concepts (cash-flow and equity-statement lines), evaluated against ledger line items when the block is read.

Payload: `name`, `bs_source_qname`, `default_change_tag_qname` (the flow concept that receives any residual), `attribution_filters[]`, `validation_mode`, `taxonomy_id`. Each filter has a `target_qname` (the flow concept it produces) and a `predicate` of `kind: "line_item_metadata_field"` whose `values` are flow-concept qnames; a line item matches when its flow tag is one of them.

`RollforwardMechanics`: `kind: "rollforward"`, `bs_source_element_id`, `bs_source_qname`, `default_change_tag_element_id`, `default_change_tag_qname`, `attribution_filters`, `validation_mode`:

- `residual_as_default` (default): the unattributed remainder becomes a fact on the default change tag
- `strict`: a remainder is an error
- `warn_only`: the imbalance is logged and allowed

### Forecast

An authored scenario. The block **is** the scenario: its id is the `scenario_id` every forecast FactSet carries. `compute-forecast` walks the months forward from the last closed actuals and writes them into the existing statement and metric blocks, stamped with that scenario.

`ForecastMechanics`: `kind: "forecast"`, `scenario_kind` (`budget` / `forecast` / `projection`), `horizon_months` (1 to 36), `base_period` (`YYYY-MM`), `base_anchor` (`seam` re-anchors on the newest closed month; `fixed` pins the walk to `base_period`), `levers[]`, `line_assertions[]`, `line_growth[]`, and `computed_months`. Authoring and computing scenarios is covered in [Forecasting and Metrics](Forecasting-and-Metrics.md).

### Statement family

`balance_sheet`, `income_statement`, `cash_flow_statement`, `equity_statement`, `comprehensive_income`. `StatementMechanics`: `kind: "statement_renderer"`, `template_id` (reserved, not yet used), `rollup_root_element_ids` (the roll-up roots, such as the Assets total), `period_comparisons` (1 to 4 columns). Envelopes carry `view.rendering`. Rendering is covered in [Reporting and Rendering](Reporting-and-Rendering.md).

### Disclosure

`regulatory_disclosure`: a note beyond the statements (inventory by category, PP&E by class, debt maturities). The structure is vocabulary, authored with `create-taxonomy-block`; its numeric facts arrive when a Report reaches its mapped concepts, and its narrative facts are bound from a document with `bind-text-block`. Uses `StatementMechanics`. A numeric disclosure renders like a statement; a text-block disclosure renders one row per narrative fact.

### Metric

A standing per-period series: ratios, covenant tests, KPIs. The structure (elements, presentation arcs, `Derive` rules) is authored with `create-taxonomy-block`. Facts are written by `compute-metrics`, which evaluates the `Derive` rules against a period's closed statements, or by `assert-metrics`, which records externally observed values. Envelopes carry `view.rendering` and `view.chart`.

`MetricMechanics` (`kind: "metric"`, `source_block_ids`, `derivation_type`, `expression`, `unit`) is declared for a block-level derivation expression that is not evaluated yet; the computation runs through the `Derive` rules. See [Forecasting and Metrics](Forecasting-and-Metrics.md).

### Reconciliation

Ties the ledger to a balance outside it. Each period end carries one comparison: what the ledger holds, what the independent source says, and what is left unexplained. The close maintains these blocks; you do not author them, so create, update, and delete all return 501. `refresh-reconciliations` creates the blocks a ledger needs and records each period's comparison, `record-statement-balance` records a statement's ending balance for an account, and `set-reconciliation-policy` changes how much the close cares.

`ReconciliationMechanics`:

| Field | Meaning |
| --- | --- |
| `kind` | `"reconciliation"` |
| `scope` | `ledger` (the whole ledger against one source) or `account` (one account against an independent balance) |
| `method` | Where the independent side comes from: `source_ledger` (the synced accounting system's own trial balance), `schedule_register` (what an account's schedules say it carries), or `statement` (a recorded statement balance) |
| `element_id` | The account reconciled; null for a ledger-scope block |
| `required_for_close` | Whether the period's close waits on it (default true) |
| `materiality` | A difference up to this amount still counts as reconciled; zero means the sides agree to the cent |
| `review_required` | Whether the close also waits for a sign-off |
| `separate_reviewer` | Whether the sign-off must come from someone other than the person who ran the comparison |

Each period's comparison is its own FactSet (`factset_type: "custom"`, provenance `observed`). A ledger-scope block presents `rs-rec:UnreconciledDifference`, `rs-rec:AccountsCompared` and `rs-rec:AccountsDifferent`; an account-scope block presents `rs-rec:LedgerBalance`, `rs-rec:IndependentBalance` and `rs-rec:UnreconciledDifference`. One `EqualTo` rule, `$UnreconciledDifference = 0` with the materiality as its tolerance, decides whether the block reconciles for a period. The live read returns the whole series, one rendering column per period end, and ignores `scenarioId`. How reconciliations hold the close, sign-off, and staleness are covered in [Period Close § Reconciliations](Period-Close.md#reconciliations).

## Rule Patterns

A rule has either a `rule_pattern` (arithmetic over fact values) or a `rule_check_kind` (a structural check over the model), never both.

| `rule_pattern` | Checks | Evaluates today |
| --- | --- | --- |
| `EqualTo` | Left and right sides are equal within tolerance | Yes |
| `RollForward` | Opening + movements = closing | Yes (same evaluator as `EqualTo`) |
| `RollUp` | A subtotal equals the weighted sum of its direct calculation children, taken from the live calculation arcs; a missing child counts as zero | Yes |
| `Exists` | A fact exists for the concept in the period | Yes |
| `CoExists` | All the named facts are present, or all are absent | Yes |
| `SumEquals` | The sum of a concept's facts across the block equals an expected total | Yes |
| `Adjustment`, `GreaterThan`, `GreaterThanOrEqualToZero`, `LessThan`, `Variance` | As named | No; reported `skipped` |
| `Derive` | Not a check: computes a value (`compute-metrics`, `compute-forecast`) | Not run by `evaluate-rules`; produces no result row |

A rule whose variables have no bound fact reports `skipped` rather than failing. The default tolerance can be overridden per rule.

`rule_check_kind` values are `LeafHasClassification`, `LibraryOriginImmutability`, `NoCycles`, `NoOrphanArcs`, `ParentBeforeChild`, and `UniqueQNameInTaxonomy`. They are emitted automatically when a taxonomy block is created, and the same invariants are enforced when the taxonomy block is written; `evaluate-rules` does not evaluate them.

`rule_category`: `AutomatedAccountingAndReportingChecks`, `DisclosureMechanicsRule`, `FundamentalAccountingConceptRelation`, `PeerConsistencyRule`, `PriorPeriodConsistencyRule`, `ReportLevelModelStructureRule`, `ReportingSystemSpecificRule`, `ToDoManualTask`, `XBRLTechnicalSyntaxRule`.

`rule_origin`: `native` (authored in a seed or by a tenant), `forked` (taken from an upstream artifact), `auto` (emitted by the platform for a taxonomy block).

`evaluate-rules` takes `structure_id` and optionally `fact_set_id`, `period_start`, `period_end`. It returns `structure_id`, `results[]`, and `summary` keyed `pass` / `fail` / `error` / `skipped`.

## View Projection Status

| View | Source | Status |
| --- | --- | --- |
| Rendering | `view.rendering` | Server-computed for the statement family, disclosures, metrics, forecast blocks, and reconciliations (one column per period end). Null for schedules and rollforwards, whose facts a client groups by period. |
| Chart | `view.chart` | Server-computed for metric blocks: panels grouped by format family, each with series keyed by element id. Values join the rendering rows by element id. |
| Facts | `facts` | Client-side projection of the envelope |
| Elements | `elements` (with `facts`) | Client-side projection of the envelope |
| Validation | `verification_results`, `verification_summary`, `rules` | Client-side projection; empty until the block has been evaluated |
| Rules | `rules` | Client-side projection, whether or not the rules have run |
| Model structure | `connections` | Not shipped as a view |

The rendering payload (`rows`, `periods`, `validation`, `unmapped_count`) is described in [Reporting and Rendering § The View Projections](Reporting-and-Rendering.md#the-view-projections).

## Reads

GraphQL at `POST /extensions/{graph_id}/graphql`:

- `informationBlock(id, scenarioId, series, seriesHistory, seriesForecast)`: one envelope. `scenarioId` selects a forecast scenario (omit for actuals). `series: true` renders a statement block as its whole monthly series; non-statement types ignore it. `seriesHistory` / `seriesForecast` cap the series at the last N actual and first N forecast columns.
- `informationBlocks(blockType, category, limit, offset, scenarioId)`: a list; filters combine as AND; `limit` defaults to 50.
- `reportPackage(reportId)`: a saved Report's metadata plus `items[]`, each with `fact_set_id`, `structure_id`, `display_order`, and the pinned `block` envelope.

MCP: `get-information-block` (`id`, `scenario_id`, `series`, `series_history`, `series_forecast`) and `list-information-blocks` (`block_type`, `category`, `limit`, `offset`, `include_atoms`, `scenario_id`).

## Gotchas

- **Cents in, dollars out.** Request amounts (`monthly_amount`, `original_amount`, `residual_value`, `periodic_amounts`) are integer cents. Fact `value`s on the envelope are in the unit, so a `monthly_amount` of `83333` reads back as `833.33`.
- **Statements are rendered, not created.** `create-information-block` with a statement, disclosure, metric, or reconciliation type returns 501. Author a `schedule`, `rollforward`, or `forecast`; produce statements with `create-report`.
- **Element ids, not qnames.** Schedule templates take chart-of-accounts element ids. A qname such as `us-gaap:Depreciation` does not resolve there.
- **`periodic_amounts` is strict.** One entry per month in the window, none negative, summing exactly to `original_amount` less `residual_value`; anything else is rejected at create.
- **The graph is the URL.** `informationBlock` and `informationBlocks` take no `graphId` argument.
- **`fact_set: null` and `facts: []` are normal** for a statement Structure no report or close has instantiated yet.
- **`verification_summary` is null until the rules run.** Call `evaluate-rules`, or close the period.
- **`disclosure_id` is derived.** It is present only when a Disclosure mapping exists; tenant-authored schedules carry null.
- **The `information-block` view operation is a different call.** It reads one section of a filing or report whole; see [Information Blocks § Live and Pinned Reads](Information-Blocks.md#live-and-pinned-reads).

## Related Documentation

**Guides:**
- **[Information Blocks](Information-Blocks.md)** - The concepts: atoms and molecules, FactSets, rules, views
- **[Reporting and Rendering](Reporting-and-Rendering.md)** - The rendering payload and statement production
- **[Forecasting and Metrics](Forecasting-and-Metrics.md)** - Forecast scenarios and metric blocks
- **[Period Close](Period-Close.md)** - Schedule drafts and the close-time rule run
- **[Taxonomy and Frameworks](Taxonomy-and-Frameworks.md)** - Taxonomy blocks, which author disclosure and metric structures

**Codebase Documentation:**
- **[API reference](https://robosystems.ai/docs/extensions)** - Full request and response schemas
- **[API Models](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/models/api/README.md)** - The Pydantic models behind the envelope

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Technical docs](Home.md)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)
