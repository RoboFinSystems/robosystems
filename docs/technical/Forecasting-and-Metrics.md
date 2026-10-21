# Forecasting & Metrics

RoboLedger plans and measures in the same shape it reports in. A **forecast** is a scenario authored as an Information Block. `compute-forecast` projects it forward month by month into statement FactSets keyed by the scenario, and each projected month is checked with the same rules that check the actuals. A **metric** block is a standing set of ratios or observed values per period. `compute-metrics` derives the ratios, and `assert-metrics` records figures observed outside the ledger. All of it is deterministic and uses no AI credits.

The product doc *Plan and forecast with your AI assistant* at roboledger.ai describes scenarios for end users. This page covers the block shape, the operations and their payloads, and how the engine walks a scenario.

> **Running your own stack?** Every example here works against a local deployment: use `http://localhost:8000` and the key from `just demo-user`. See [Local Development](Local-Development.md).

## Table of Contents

- [Overview](#overview)
- [The Forecast Block](#the-forecast-block)
- [Authoring a Scenario](#authoring-a-scenario)
- [Computing a Scenario](#computing-a-scenario)
- [Following the Close vs. Pinned Scenarios](#following-the-close-vs-pinned-scenarios)
- [Plan History](#plan-history)
- [Metrics](#metrics)
- [Gotchas and Pitfalls](#gotchas-and-pitfalls)
- [Self-hosted deployments](#self-hosted-deployments)
- [Related Documentation](#related-documentation)
- [Support](#support)

## Overview

| Operation | Purpose |
| --- | --- |
| `create-information-block` (`block_type: "forecast"`) | Author a scenario: name, horizon, base month, assumptions |
| `update-information-block` (`block_type: "forecast"`) | Change a scenario's assumptions. Does not recompute. |
| `delete-information-block` | Remove a scenario and every month it computed |
| `compute-forecast` | Walk the scenario forward and write its monthly statements |
| `backfill-plan-history` | Stamp statements for historical months behind the close boundary, so a scenario has actuals to stand on |
| `compute-metrics` | Derive a metric block's ratios for one period, from actuals or a scenario |
| `assert-metrics` | Record externally observed values into a metric block for one period |

Scenarios are read through `get-information-block` / GraphQL `informationBlock` with a `scenarioId`, and on the **Plan** page in RoboLedger. Forecast months never enter the actual statements, reports, or the ledger. A scenario writes only FactSets stamped with its own `scenario_id`, and actuals carry `scenario_id = null`.

```bash
export ROBOSYSTEMS_API_KEY=rfs...
export GRAPH_ID=kg...
```

## The Forecast Block

The forecast block **is** the scenario: its structure id is the `scenario_id` every projected FactSet carries. Its mechanics (`ForecastMechanics`) hold only what is authored:

| Field | Meaning |
| --- | --- |
| `scenario_kind` | `budget`, `forecast` or `projection`. A label for display and filtering. All three compute identically. |
| `horizon_months` | Months projected past the base month, 1 to 36 |
| `base_period` | The month (`YYYY-MM`) the authored window counts from. Resolved at create time and stored. |
| `base_anchor` | `seam` (the default) or `fixed`. See [Following the Close vs. Pinned Scenarios](#following-the-close-vs-pinned-scenarios). |
| `levers[]` | Driver assertions |
| `line_assertions[]` | Values set directly on statement lines |
| `line_growth[]` | Month-over-month growth rates on income-statement lines |

Everything else is derived by `compute-forecast`.

### The four drivers

The lever catalog is the `rs-driver` package. It has exactly four levers. Each is backed by a library-seeded rule that says what it moves:

| Lever | Unit | What it drives |
| --- | --- | --- |
| `rs-driver:RevenueGrowthRate` | Decimal rate per month | Revenue grows by the rate each month, compounding: `0.03` is +3% a month |
| `rs-driver:CostOfRevenueRate` | Decimal fraction | Cost of revenue as a fraction of the **same month's** revenue: `0.62` is 62%. It is not a growth rate. |
| `rs-driver:DaysSalesOutstanding` | Days | Accounts receivable from revenue and collection days |
| `rs-driver:DaysPayableOutstanding` | Days | Accounts payable from cost of revenue and payment days |

A lever acts only in the months that carry a value for it. Anything the four levers don't reach is set as a line value or a line growth rate. A rule like "payroll is 20% of revenue" is worked out and set month by month. It is not kept as a live relationship.

### Line values and line growth

- **Line values** (`line_assertions`) set a statement line directly for the months named. The target must be a calc-DAG **leaf**: a detail line, never a subtotal. It can be an income-statement line (duration) or a balance-sheet line (instant). A line value wins over a driver and over carry-forward in its months. A displaced driver rule shows up in the compute result's `skipped[]`. Because subtotals are always derived, a line value still flows through gross profit, net income, retained earnings, cash and the cash flow, and the month still has to balance.
- **Line growth** (`line_growth`) grows an **income-statement** leaf month over month: `line[t] = line[t-1] × (1 + rate[t])`, compounding from the base month. Rates must be greater than −1. A month with no rate holds the line where it was. Balance-sheet lines can't be grown directly; they roll from the income statement and the working-capital levers.

**One owner per line.** A line can't be both set and grown, and a line already moved by an active driver can't also be grown. For example, `Revenues` can't take line growth when `RevenueGrowthRate` is set. Such requests are rejected with `422`, naming the conflict.

### Value grammar

Levers, line values and line growth share one grammar: `value` fills every month of the horizon, and `values_by_period` overrides individual months (`"YYYY-MM"` keys). At least one of the two is required. Seasonality is a set of month overrides. There is no seasonal formula. Every month named must fall inside `base_period + 1 … base_period + horizon_months`.

## Authoring a Scenario

A twelve-month operating budget for Cadence Labs, a SaaS company. Revenue grows 4% a month with a slower December. Cost of revenue steps down as hosting contracts renew. Customers pay in 40 days and vendors in 30. Two engineers join in October. Research and development grows 2% a month after that, and last month's one-time grant income is zeroed so it isn't carried forward:

```bash
curl -X POST "https://api.robosystems.ai/extensions/roboledger/$GRAPH_ID/operations/create-information-block" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "block_type": "forecast",
    "payload": {
      "name": "FY27 Operating Budget",
      "scenario_kind": "budget",
      "horizon_months": 12,
      "base_period": "2026-08",
      "base_anchor": "seam",
      "levers": [
        {
          "qname": "rs-driver:RevenueGrowthRate",
          "value": 0.04,
          "values_by_period": {"2026-12": 0.01}
        },
        {
          "qname": "rs-driver:CostOfRevenueRate",
          "value": 0.28,
          "values_by_period": {"2026-09": 0.31, "2026-10": 0.30}
        },
        {"qname": "rs-driver:DaysSalesOutstanding", "value": 40},
        {"qname": "rs-driver:DaysPayableOutstanding", "value": 30}
      ],
      "line_assertions": [
        {"qname": "rs-gaap:NonoperatingIncomeExpense", "value": 0}
      ],
      "line_growth": [
        {
          "qname": "rs-gaap:ResearchAndDevelopmentExpense",
          "values_by_period": {
            "2026-10": 0.35,
            "2026-11": 0.02, "2026-12": 0.02, "2027-01": 0.02,
            "2027-02": 0.02, "2027-03": 0.02, "2027-04": 0.02,
            "2027-05": 0.02, "2027-06": 0.02, "2027-07": 0.02,
            "2027-08": 0.02
          }
        }
      ]
    }
  }'
```

`levers` needs at least one entry. `base_period` is optional. When it is omitted, it resolves to the fiscal calendar's `closed_through`, or failing that the newest month with actual statements, so a scenario starts from real closing balances. That is why a scenario needs at least one closed month or backfilled history (see [Plan History](#plan-history)). Line qnames must be leaves in the tenant's library. `get-information-block` on a forecast block returns the lever catalog with each lever's documentation. The envelope's `id` is the scenario id used by every call below.

**Updating** uses `update-information-block` with `block_type: "forecast"` and a `structure_id` in the payload. `levers`, `line_assertions` and `line_growth` are each replaced whole when supplied, because a partial edit would make the asserted set ambiguous. Changing `horizon_months` or `base_period` moves the window, so all three sets must be supplied again (an empty list clears one). Switching `base_anchor` needs nothing else. **An update does not recompute.** Months computed earlier stay as they were until the next `compute-forecast`.

**Deleting** a scenario removes its assumptions and every FactSet it computed. Actuals are never touched.

## Computing a Scenario

```bash
curl -X POST "https://api.robosystems.ai/extensions/roboledger/$GRAPH_ID/operations/compute-forecast" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"structure_id": "struct_01J..."}'
```

`months` limits the run to the first N months of the horizon (never more than `horizon_months`). `entity_id` defaults to the entity the scenario was authored against.

For each forward month, in order, the engine:

1. **Builds the income statement.** Each detail line takes its value from its one owner for the month: a line value if one is set, else its driver rule (evaluated in dependency order) or its growth rate, else the prior month's value carried forward. [Schedules](Information-Blocks.md) keep running, so depreciation and amortization continue at their scheduled amounts and stop when the schedule ends. The calc-DAG subtotals are then derived down to net income.
2. **Rolls the balance sheet.** Receivables and payables follow DSO and DPO. Schedules move the assets they belong to. Net income rolls into retained earnings. Cash is the balancing figure, so assets equal liabilities plus equity by construction.
3. **Derives the cash flow** by the indirect method, from the balance-sheet changes and net income, reconciled to the change in cash.
4. **Verifies the month** with the rules that check actual statements: subtotals foot, the balance sheet balances, the cash flow ties to cash.

Each month writes one income-statement, balance-sheet and cash-flow FactSet stamped with the scenario. Re-running replaces them.

**A failed check halts the run.** Each month opens on the previous month's closing balances, so every month computed after a failed one would be built from numbers known to be wrong. The walk stops at the failing month and returns it as `halted_at`. That month's facts are kept so the failure can be inspected, and no later month is computed.

```json
{
  "structure_id": "struct_01J...",
  "scenario_id": "struct_01J...",
  "entity_id": "ent_01J...",
  "base_period": "2026-08",
  "anchor_period": "2026-08",
  "months": 12,
  "months_computed": [
    {
      "period": "2026-09",
      "period_start": "2026-09-01",
      "period_end": "2026-09-30",
      "income_statement_fact_set_id": "fs_...",
      "balance_sheet_fact_set_id": "fs_...",
      "cash_flow_fact_set_id": "fs_...",
      "computed_count": 64,
      "verification_passed": true,
      "verification_failures": []
    }
  ],
  "halted_at": null,
  "skipped": [],
  "diagnostics": []
}
```

`verification_passed` has three states. `true` means the rules ran and passed. `false` means one failed, and the run halted there. `null` means **no rules ran**, so treat the month as unverified, not verified. `skipped[]` lists rule-and-month pairs that fell back to carry-forward, each with a reason (a lever with no value that month, an unbound operand, a rule displaced by a line value). A skip never stops the walk. `diagnostics[]` carries informational notes, for example a schedule contribution with nowhere to land.

## Following the Close vs. Pinned Scenarios

`base_period` never moves after creation. Every assumption is keyed to a month inside the window it opens. `base_anchor` decides where the **walk** takes its opening balances:

- **`seam`** (the default) re-anchors on the newest closed month at or after `base_period`. When you close September and recompute, September becomes an actual, and the first forecast month rolls off September's real closing balances. The assumptions stay as written. The result reports the month actually used as `anchor_period`. Once the horizon is entirely in the past, nothing is left to compute, so lengthen the horizon (which means re-supplying the assumptions).
- **`fixed`** pins the walk to `base_period`. This is the counterfactual scenario ("what if we had raised prices last January"), whose balances are meant to diverge from actuals.

Most scenarios should follow the close. After every close, recompute them.

## Plan History

A scenario stands on stamped monthly statements, which the close writes. A graph that arrived from QuickBooks with years of history, but has closed few months in RoboLedger, has no such statements for its past. `backfill-plan-history` fills them in:

```bash
curl -X POST "https://api.robosystems.ai/extensions/roboledger/$GRAPH_ID/operations/backfill-plan-history" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"start_period": "2024-09", "max_periods": 24}'
```

It runs on the background worker, since a chunk recloses up to 24 months: the call returns `202` with a `pending` envelope, and the result arrives on the operation's stream at `/v1/operations/{operation_id}/stream`. It seeds any missing months of the fiscal calendar back to `start_period`, as already closed. It then works oldest-first through months that lack stamped statements, running the real reopen → re-close cycle on each. Balance validation, statement rules and audit events all apply to every month. Behavior to plan around:

- **Chunked.** At most `max_periods` months per call: default 12, maximum **24**. Loop until `remaining_periods` comes back empty.
- **Idempotent.** Months that already have stamped statements are skipped. `restamp: true` re-derives them too, after an engine change; a restamp run doesn't resume on its own, so advance `start_period` between chunks.
- **Bounded by the ledger.** `start_period` is clamped to the earliest month with entries. The backfill only covers closed history, so `start_period` after `closed_through` is refused.
- **Never posts.** A month holding draft entries is reported `skipped_drafts` and left alone.
- **Stops on failure.** A month whose re-close fails is reported `failed` and the chunk halts, because continuing would leave a hole in the series. Untried months stay in `remaining_periods`.

Each month's outcome is in `processed[]` (`stamped`, `skipped_drafts` or `failed`, with detail). A precondition comes back as the operation's result under `error`: `nothing_closed` (close one month first), `no_ledger_data`, or `start_after_boundary`. The same five override flags as `close-period` (`allow_stale_sync`, `allow_stranded_obligations`, `allow_reconciling_items`, `allow_unposted_source_events`, `allow_unreconciled_accounts`) apply to each re-close. On a ledger with a reconciliation the close waits on, `allow_unreconciled_accounts` is needed for any month closed before that reconciliation existed, because it was never compared for that month. See [Period Close](Period-Close.md#reconciliations).

## Metrics

A metric block (`block_type: "metric"`) holds one **standing** FactSet per structure, entity and period end. Each run for a new period extends the time series, and re-running a period replaces its values. Two operations write to it, and a structure is owned by exactly one of them.

### compute-metrics: derived ratios

Every tenant receives the library block `rs-metric:KeyFinancialMetrics`, whose metrics are computed from statement facts by library-seeded rules:

| Metric | Format |
| --- | --- |
| Working Capital | monetary |
| Current Ratio | ratio |
| Quick Ratio | ratio |
| Debt to Equity | ratio |
| Interest Coverage | multiple |
| Return on Equity | percent (on average equity) |
| Net Profit Margin | percent |
| Asset Turnover | multiple |
| Equity Multiplier | multiple |
| Return on Equity (DuPont) | percent (margin × turnover × multiplier) |

```bash
curl -X POST "https://api.robosystems.ai/extensions/roboledger/$GRAPH_ID/operations/compute-metrics" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"structure_id": "struct_01J...", "period_end": "2026-08-31", "period_start": "2026-08-01"}'
```

Operands bind to the entity's persisted statement facts whose period ends exactly on `period_end`: balances as of that date, and flows ending on it. Those facts come from the statements stamped at close or saved in a report. A metric whose operand is missing (interest expense for a company with no debt) or whose ratio is undefined (a zero denominator) is listed in `skipped[]` with a reason. It never fails the run. Metrics can build on other metrics, and they are computed in dependency order.

Pass `scenario_id` (a forecast block's id) to compute a metric series over a scenario's forward months. Operands bind the scenario's facts, falling back to actuals across the boundary between actual and forecast months.

### assert-metrics: observed values

`assert-metrics` writes figures the ledger can't derive (usage counts, pipeline numbers, hand-carried KPIs) into a metric block with asserted provenance:

```bash
curl -X POST "https://api.robosystems.ai/extensions/roboledger/$GRAPH_ID/operations/assert-metrics" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "structure_id": "struct_01J...",
    "period_start": "2026-08-01",
    "period_end": "2026-08-31",
    "source_system": "product-analytics",
    "basis_note": "Month-end export from the billing system",
    "observations": [
      {"qname": "cadence:ActiveSubscriptions", "value": 1184},
      {"qname": "cadence:NetRevenueRetention", "value": 1.07}
    ]
  }'
```

Each `qname` must be a concept on the structure's presentation, and duplicates are rejected. A structure that carries derivation rules belongs to `compute-metrics` and refuses assertions, so derived and observed series never share a structure. Asserted series are actuals only: they have no scenario axis. The response reports `replaced: true` when an existing period was overwritten.

## Gotchas and Pitfalls

### An update is not a recompute

After `update-information-block`, the scenario's months still hold the old numbers until `compute-forecast` runs.

### Recompute after every close

A `seam` scenario only rolls forward to the newest actuals when it is recomputed.

### Carry-forward repeats one-offs

A line nothing touches repeats last month's value. Zero out an unusual gain or annual payment with a line value, or it recurs every month.

### `null` verification is not a pass

`verification_passed: null` means no rules ran for that month.

### Owner distributions aren't modeled

Retained earnings grows by net income. Distributions and dividends are not projected.

## Self-hosted deployments

`just demo-saas-startup` (Cadence Labs) and `just demo-coffee-roaster` (Driftline Coffee) provision tenants whose books have a financial story worth planning against. See [Showcase Scenarios](RoboLedger-Demo-Walkthrough.md#showcase-scenarios). Use `http://localhost:8000` and the key from `just demo-user`.

## Related Documentation

- **[Information Blocks](Information-Blocks.md)** - The block envelope, FactSets and `scenario_id`
- **[Period Close](Period-Close.md)** - The stamped statements every scenario and metric reads
- **[Chart of Accounts Mapping](Chart-of-Accounts-Mapping.md)** - Statements, and therefore forecasts, need a mapped chart
- **[Reporting and Rendering](Reporting-and-Rendering.md)** - The calc DAG, and how forecast columns render
- **[Taxonomy and Frameworks](Taxonomy-and-Frameworks.md)** - The `rs-driver` and `rs-metric` packages
- **[RoboLedger Operations](RoboLedger-Operations.md)** - The full operations catalog

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Technical docs](Home.md)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)
