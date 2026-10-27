# Group Consolidation

A RoboLedger graph is one reporting group: the group parent and the companies under it, each with its own books, chart and close, and one more entity that holds only the consolidating entries. The consolidated statements are ASC 810 on that model, computed from the books month by month and posted where only the consolidated read looks. This page is the model end to end: how an entity is treated, what the charts must carry, how the engine drafts the eliminations and what it leaves for a person, the group close, the lifecycle of a member from acquisition to sale, and how the statements behave while members come and go.

The product doc *Consolidate a group* at roboledger.ai describes the workflow for the finance lead. [RoboLedger Operations](RoboLedger-Operations.md) is the per-operation reference with request and response shapes; this page links to it rather than repeating it.

> **Running your own stack?** Every example here works against a local deployment: use `http://localhost:8000` and the key from `just demo-user`. See [Local Development](Local-Development.md).

## Table of Contents

- [The Group Model](#the-group-model)
- [Charts and Mapping](#charts-and-mapping)
- [The Consolidating Entries](#the-consolidating-entries)
- [The Group Close](#the-group-close)
- [The Lifecycle of a Member](#the-lifecycle-of-a-member)
- [Statements and the Worksheet](#statements-and-the-worksheet)
- [Limits](#limits)

## The Group Model

**One graph, one group.** The graph boundary is the consolidation boundary. The entity graph creation makes is the **group parent**; `create-entity` adds a company under it with the share the parent holds (`ownership_pct`). Unrelated books never share a graph: a firm's clients or a fund's portfolio companies are one graph each, and the cross-graph report share is their path to each other. An entity with `source: linked` is another graph's company received that way, never a scope of this ledger. A graph with a single company is the default and the degenerate case; nothing on it changes.

**Books, chart and close are per entity; the roll-up is rs-gaap.** Every entry, event, fiscal calendar, schedule, reconciliation and report belongs to one entity, and so does the chart of accounts and its mapping. The statements combine at the reporting concepts, never at account codes: each entity's accounts map to rs-gaap concepts, each entity's subtotals derive from its own mapping, and the group sums concept by concept under the parent's Reporting Style. An entry cannot post to another entity's account.

**How an entity is treated** is three fields on the entity row, all settable with `update-entity`:

| Field | Values | What it decides |
|---|---|---|
| `entity_role` | `operating`, `consolidation` | `operating` keeps a company's books. `consolidation` is the group's one entity for the consolidating entries; only the consolidated statements read it, and no operating company's statements ever include it. Its method is `none`. |
| `consolidation_method` | `full`, `equity`, `cost`, `none` | `full`: the books are summed and an outside owner's share is carried as noncontrolling interest. `equity`: never summed; the group carries one investment line and one income line, the parent's share of the result. `cost`: an investment at cost. `none`: the parent, the consolidation entity, a linked counterparty. Derived from `ownership_pct` when not given, 50 and above `full`, 20 to 50 `equity`, less `cost`, and set by hand where control is not the percentage. |
| `joined_on`, `left_on` | a date, or null | Membership is by whole month. Null `joined_on` is a member since the beginning; null `left_on` is a member still. The consolidated statements include an entity from the month of `joined_on` through the month of `left_on`. Once the month of `left_on` is over, the entity reads `is_former: true`, its closed months stay readable on their own, and the group sums none of it. |

Neither date, nor the method, may move inside the months the group has already closed: that would restate them, and `update-entity` refuses.

**Counterparties.** Every company in the group is also one of the group's counterparty agents, and a transaction is intercompany when its event names another group entity as its counterparty. `create-entity` makes the agent. The engine pairs intercompany balances and charges by that agent, never by account, so a parent's fee-revenue account can hold fees from two subsidiaries and from outsiders without splitting the chart.

## Charts and Mapping

Every read the group makes goes through the concept an account maps to. That gives two requirements, one for every entity and one for the group's own charts.

**Map every account that has postings.** An account with landed lines and no mapping arc is in no statement and in no consolidating entry: its balances vanish rather than mis-sort. The closeable gate refuses the month with `unmapped_accounts`, naming up to five, and nothing lifts it; see [Period Close](Period-Close.md#blockers). A chart from a template arrives mapped. A chart brought from QuickBooks or built by hand is mapped through `get-unmapped-elements`, `suggest-mapping` or the MappingOperator, and `create-mapping-association`; see [Chart of Accounts Mapping](Chart-of-Accounts-Mapping.md). The concept chosen decides the treatment: classification (asset, liability, equity, revenue, expense), period type, and the default cash-flow row all come from the concept, so a wrong arc is a wrong treatment, not a missing one.

**The consolidation entity's chart must carry the concepts the engine posts to.** The engine finds its posting account by mapped concept and consolidation role, never by code, and refuses by name when one is missing (`… has no account mapped to rs-gaap:Goodwill; add one to its chart`). The `consolidation` chart template carries the whole set, so `initialize-chart-of-accounts` with `template: "consolidation"` on an entity with `entity_role: consolidation` is the normal path. A chart built by hand, or one made before a row existed, takes the row through `update-taxonomy-block` and a mapping arc. The set, by whose chart it sits on:

| Chart | Concepts | Used by |
|---|---|---|
| Consolidation entity | `InvestmentsInAffiliatesSubsidiariesAssociatesAndJointVentures`, `MinorityInterest`, `NetIncomeLossAttributableToNoncontrollingInterest`, `Goodwill`, `GainOrLossOnSaleOfStockInSubsidiary`, `AdditionalPaidInCapital` and the parent's close-target equity concept (the template carries a row for each form: `RetainedEarningsAccumulatedDeficit`, `PartnersCapital`, `MembersEquity`), `EquityMethodInvestments`, `IncomeLossFromEquityMethodInvestments`, and an account on every balance and result concept the members' intercompany lines sit on (`DueFromAffiliateCurrent`, `DueToAffiliateCurrent`, the note and interest concepts, the fee's revenue and expense concepts) | the eliminations, the outside owners' interest and share of income, the acquisition, a change in the parent's share, the deconsolidation, the equity-method pickup |
| Parent | `InvestmentsInAffiliatesSubsidiariesAssociatesAndJointVentures` for a member in full, `EquityMethodInvestments` for an associate, `GainOrLossOnSaleOfStockInSubsidiary` if a sale's gain is to be reversed on the same line | the investment elimination reads what the parent carries; a sale's gain reversal lands on the same concept the parent booked it on |
| Members | nothing specific | capital is anything equity-classified, a result anything revenue or expense, and intercompany pairs match by counterparty on whatever concept the lines use. The consolidation entity must be able to post to that concept, or the pair is named in `notes[]` and left out, and the month is `incomplete` |

A consolidation entity carries its members' form. The eliminations tag the cash-flow rows the members' lines took, and an entity can tag only rows its own Style's cash flow presents, so a group of LLCs wants an LLC consolidation entity even under a corporate parent. The parent's form is what the consolidated statements render under, which is why the template carries the close-target row for every form.

## The Consolidating Entries

`preview-consolidation-entries` computes what a month needs from the books and posts nothing; `propose-consolidation-entries` writes the same entries as drafts in the consolidation entity, which its close posts. Both take `period` as `YYYY-MM`. The shapes are on [RoboLedger Operations](RoboLedger-Operations.md#preview-consolidation-entries--tenant-graphs).

**Target minus held.** For each kind and each member the engine computes the month-end target, subtracts what the consolidation entity already holds of that kind for that member, and proposes the difference. A hand entry that names its kind and the member it adjusts counts toward the target by construction, and drafting a month again replaces only the engine's own drafts. The kinds:

| Kind | What it brings to target |
|---|---|
| `elimination_investment` | The parent's investment in a member in full against the parent's share of the member's capital (its equity lines other than closing entries: contributions and acquired equity, net of distributions), with the outside owners' share of that capital as noncontrolling interest. The share is read as of the month, so a change in it moves the elimination within the month it took effect. |
| `elimination_intercompany` | Balances the group's entities owe each other, by pair, and the month's intercompany income against the matching expense. Pairs match by counterparty agent; equal debits and credits cancel, and where one of each is left the lower amount is eliminated. |
| `nci_allocation` | The outside owners' share of a member's result for the month, pro rata by default. `set-allocation-override` records a different share for a month, as cents or a percentage with a basis, and the engine proposes that instead. |
| `equity_method_pickup`, `equity_method_distribution` | For an associate: the parent's share of its result against the investment, and a distribution the parent took as income reclassed against the investment. |

The posting account for each line is the consolidation entity's account mapped to the concept, chosen by role. Where an elimination offsets a flow the members' lines tagged, the entry's lines carry the offsetting cash-flow rows, so the consolidated cash flow nets what the group paid itself. A consolidation-entity line with a flow tag counts on the cash flow on its own, cash-effect signed.

**What the engine cannot explain is returned, never plugged.** Three results come back beside the proposals:

- `residuals[]`: the parent's investment in a member does not match its share of the member's capital. That member's investment elimination is withheld, and the residual says why it might be. If the parent booked the member's distributions as income, the engine explains the difference itself and reverses that income against the investment in the same entry. If the member joined with an opening balance and no acquisition is recorded, the cause is `pre_acquisition` and the remedy is `record-acquisition`. If the parent bought or sold shares and recorded nothing, the remedy is `record-ownership-change`. Otherwise the books are wrong, or an explaining entry is yours to post.
- `reconciling_items[]`: the two sides of an intercompany pair disagree. The lower amount is eliminated and the difference named with both entities and the concept.
- `notes[]`: what the month needs that the engine could not produce. A balance on a concept the consolidation entity has no account for; a recorded consideration the parent's investment account does not hold; a former member whose goodwill or outside owners' interest still waits for `record-deconsolidation`. Each of these also marks the month `incomplete`.

An intercompany expense with no income against it, a cost one company paid and recharged at cost, stays in the group's result and is only noted.

## The Group Close

The consolidation entity's close is the group close. `close-period` with the consolidation entity's `entity_id` closes the group's month, posts the month's consolidating drafts, and stamps the consolidated statements as the consolidation entity's own canonical statements, so every reader of closed statements reads the group's with that entity's id and the parent's own stamp is untouched. Members close first, then the parent, then the group; once the group has closed a month, a member's month reopens only after the group's.

Beside the blockers every close has, the consolidation entity's gate adds four, each naming its items on the calendar read:

| Code | Meaning | Named in |
|---|---|---|
| `entity_open` | An entity the month consolidates has not closed it: the parent, each member inside its interval, and an equity-method member, whose result the pickup reads | `group_open_entities` |
| `adjustments_unposted` | The month's consolidating entries are neither held nor drafted, or the engine left something out with a note; run `propose-consolidation-entries` | `group_pending_adjustments` |
| `residual_unexplained` | A residual or intercompany disagreement is left. `allow_unexplained_residuals` closes over it and names each in the audit note; the difference stays in the consolidated statements | `group_unexplained` |
| `consolidated_does_not_foot` | The consolidated balance sheet or equity statement fails a check that could run. Checked again once the close has posted the month's drafts, so a draft that would leave them not tying rolls the close back | `group_not_footing` |

The `unmapped_accounts` blocker applies to every entity's close, the consolidation entity's included; see [Period Close](Period-Close.md#blockers).

## The Lifecycle of a Member

A member can be formed in the group, bought into it, change hands while in it, and leave it. Each step is one operation on the consolidation entity's books, dated to the month membership turns on, and each is drafted for the close to post. All three recording operations refuse a posted entry; reverse it first to record again, and recording a month again replaces its draft.

### Joining: the acquisition

A member the parent bought from its owners joins mid-life. Set `joined_on` to the day it joined, open its books on its balance sheet at acquisition with `record-opening-balance` (dated the eve of its first month), and post the parent's purchase in the parent's books naming the seller, not the member: the cash line tagged *Payments to acquire businesses, net of cash acquired*, so the join month's consolidated cash flow shows the acquisition on one row, net of the cash that came with the member. Until the acquisition is recorded, the member's investment elimination is withheld with `cause: pre_acquisition`.

`record-acquisition` takes the consideration paid in cents and the basis, optionally the fair value of the outside owners' interest (`nci_fair_value`, for goodwill on the whole business) and fair-value step-ups (`allocations`, each an identifiable asset or liability's concept and a nonzero amount). It writes a draft of kind `acquisition` dated the join: the excess of the consideration over the parent's share of the member's equity at acquisition goes to the step-ups and the rest to goodwill, with the outside owners' share of what they add as noncontrolling interest. The engine then reads the consideration as the parent's investment in the member. A bargain purchase is refused until `confirm_bargain_purchase` is set, after the reassessment ASC 805 asks for, and then recognized as a gain. On the equity statement the outside owners' interest that arrived with the member is its own row, *Noncontrolling Interest, Increase from Business Combination*.

### Changing hands within the method

`record-ownership-change` records the parent buying more of a member in full from its outside owners, or selling some of it to them while keeping control (the share stays 50 or more): the month the new share holds from, the share, the price in cents and the basis. ASC 810 makes it an equity transaction, never a gain or loss. The investment elimination moves the share bought or sold of the member's capital at the new share; the draft of kind `ownership_change` carries what that cannot.

Buying more: the price above the book value of the capital bought goes against the investment, the share bought of the member's results since it joined, and of any interest the acquisition gave the outside owners above book, comes out of noncontrolling interest, and the difference is the parent's equity (paid-in capital under a corporate Style, the capital account under an LLC or partnership one). The entry's tags move the price from investing to a payment to the outside owners.

Selling some: the parent's own entry, naming the buyer, takes the proceeds against the carrying amount of the shares sold (its cost, pro rata, returned as `carrying_released`) and books its gain or loss on `GainOrLossOnSaleOfStockInSubsidiary`. The draft reverses that gain (`gain_reversed`), releases the parent's cost above book on those shares against the investment, credits the outside owners their larger share of the member's results, and takes the difference to the parent's equity. Its tags move the proceeds from the divestiture row to a financing inflow from the outside owners. The consideration is recorded as the carrying amount released, so the engine's check of recorded considerations against the parent's investment account still holds and a sale at a loss does not hold the month. A sale below 50 is a change of method, which is not supported; see [Limits](#limits).

### Leaving: the exit and the deconsolidation

A member leaves at a month end, sold, dissolved or control lost: `update-entity` with `left_on` set to its last day. The guards refuse while its books are open through that month, while a bank feed is still linked to its chart, or while an intercompany balance disagrees with the group's in that month; each names what to settle. An agreed balance is not a guard: it stays on the parent's books as a related-party balance once the member is a third party. A member sold early in a month is consolidated through it; set `left_on` to the prior month end to leave the month out.

From the month after, the group reads sum none of the member, its entity row reads `is_former: true`, and that month's proposals reverse its investment and intercompany eliminations on their own. What the consolidation entity still holds for it beyond those waits for `record-deconsolidation`: goodwill and step-ups from its acquisition, the price above book of the parent's investment, the outside owners' accumulated share of its results, an associate's pickups. It takes the consideration the parent received in cents (0 for a dissolution) and the basis, and writes a draft of kind `deconsolidation` dated the first day of the month after the member's last: each of those balances is taken off, a member in full has the results it earned in the group credited back to the group's equity (the parent's close target) as they leave with its books, and the difference is the gain or loss on its leaving. That is the group's adjustment to the gain the parent's own sale entry took, so the consolidated gain is the consideration less the carrying amount of what left. The parent's own entry stays on its books.

In that month the group's cash flow opens on the previous month's close and carries the member's cash on the divestiture row, net of cash divested, its capital leaves on the equity statement's other-movements row, and the noncontrolling interest derecognized is its own row, *Noncontrolling Interest, Decrease from Deconsolidation*.

## Statements and the Worksheet

`live-financial-statement` on the parent takes `scope`: `entity` is its own books (the default), `combined` sums every member in full and leaves the consolidation entity out, the pre-adjustment total with nothing eliminated, and `consolidated` adds the consolidation entity. An equity- or cost-method member is never summed; the group carries its investment and its share of the result instead. Every column renders under the parent's Reporting Style over each member's own books, summed at the concepts. A concept the parent's Style presents no row for, an LLC's members' equity under a corporate parent, takes a row under its nearest presented ancestor.

`consolidation-worksheet` lays one window out with the entity as the column: each member in full, `combined`, the consolidation entity as `adjustments`, `consolidated`, and each equity- or cost-method member as `not_summed` beside the sum. Its two totals are the numbers the live statement returns for the two group scopes, and it is the read to check a consolidating entry against.

**Membership decides the columns.** A balance sheet holds the members of the month it is struck in. An income statement sums each member's own months of the window. A cash flow or equity statement column that a member joined or left within would show balances arriving or leaving with no line for them, so that column is left out and named in `omitted_periods`; read the months on either side. The one-month columns at the edges are stated in full: the join month carries the member's cash as cash acquired, and the month after the exit carries its cash as cash divested and its capital on the other-movements row. A year with an acquisition inside it is therefore read month by month; the single annual column over it is an open row ([Limits](#limits)).

A consolidated report saves the group's statements for a period: `create-report` with the consolidation entity's mapping, or in the app *Consolidated group* beside the parent alone. The group close stamps the same statements as the consolidation entity's canonical sets, so an information-block or plan read with its `entity_id` returns the group's closed statements in the parent's Style.

## Limits

- **A change across methods**, equity to full as a step acquisition with the earlier interest remeasured, or full to equity with a retained interest, is not supported. The read takes the method off the entity row, so a method switched from a date would be summed back through every month; once the group has closed a month with the member on a method, changing it is refused. A change of share within the method, either way, is `record-ownership-change`.
- **Multi-level groups** are not modelled: the tree is flat, every member under the one parent, and noncontrolling interest is not computed bottom-up.
- **A foreign member** is not translated; one currency per entity is queued, not built.
- **Unrealized intercompany profit** in inventory or fixed assets is a hand entry with a basis.
- **Shares bought from or sold to outside owners** ride the equity statement's contribution and distribution rows for noncontrolling interest; us-gaap's dedicated rows are not yet in the library.
- **A step-up's amortization schedule** in the consolidation entity is not ended by the member's exit; terminate it.
- **The parent's gain on a sale within the method** is reversed at the amount the engine computes, proceeds less the carrying amount released. A parent that booked a different gain shows the difference on the consolidated income statement's gain line, which is visible but not named by the gate.
- **Serialization** of the consolidated statements carries no legal-entity axis yet: the exported group statements are the consolidated totals, without the per-entity breakdown.

## Related pages

- [RoboLedger Operations](RoboLedger-Operations.md): request and response shapes for every operation named here.
- [Period Close](Period-Close.md): the fiscal calendar, every blocker, the close and its receipt.
- [Chart of Accounts Mapping](Chart-of-Accounts-Mapping.md): coverage, renderability, how arcs get made, the closed-period lock.
- [Reporting and Rendering](Reporting-and-Rendering.md): how a statement is rendered from the mapping and the Style.
- [RoboLedger Demos](RoboLedger-Demo-Walkthrough.md): the Driftline group, the exemplar every mechanism here is proven against.
