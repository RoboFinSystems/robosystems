---
title: Glossary
description: The words you meet in RoboLedger, from captured to write-back, in a sentence or two each, with the page that covers each one.
order: 31
section: Reference
---

The words RoboLedger and your AI assistant use, in plain terms. Where the technical documentation calls something by another name, that name is given too, so you can follow a term from one to the other.

**Agent.** A customer, vendor or employee: someone your company does business with. They come over from QuickBooks and are listed under **Agents** in the app. See [Find your way around the app](the-roboledger-app.md).

**Captured.** An event that is waiting for a person. Nothing is in the ledger yet. It sits in the Inbox until someone approves or rejects it. See [Review events in the Inbox](inbox.md).

**Changed transaction.** A transaction someone edited in QuickBooks after RoboLedger recorded it. RoboLedger flags it instead of overwriting what it posted, and a close won't run over changes nobody has looked at. You settle each one: restate the month, catch up the difference in an open month, or mark it as handled. See [When QuickBooks changes after a sync](changes-after-sync.md). In the technical docs: [reconciling item](https://robosystems.ai/docs/technical/quickbooks-sync-and-write-policy).

**Chart of accounts.** Your company's own list of accounts, as it comes from QuickBooks. See [Map your chart of accounts](map-your-chart-of-accounts.md). In the technical docs: [chart-of-accounts elements](https://robosystems.ai/docs/technical/chart-of-accounts-mapping).

**Classified.** An event whose accounts have been decided. If it arrived with its entry worked out, such as an adjustment your assistant drafted, that draft is already in the ledger and the next close posts it unless someone rejects it. One classified by hand in the Inbox has no entry until it's committed. See [Review events in the Inbox](inbox.md).

**Close.** Ending a month: posting its draft entries, locking it, and saving its statements. Months close in order, one at a time, and only once they're over. See [Close the month with your AI assistant](month-end-close.md). In the technical docs: [period close](https://robosystems.ai/docs/technical/period-close).

**Committed.** An event someone approved. Its draft entry posts at the next close. See [Review events in the Inbox](inbox.md).

**Connection.** Two things share the name. A graph's connection to QuickBooks, under **Entity → Connections**, links it to one QuickBooks company; see [Connect your books to your AI assistant](connect-your-books.md). A connection in Claude, ChatGPT or another MCP client reaches one graph; see [Work across several companies](working-across-companies.md).

**Credits.** What RoboLedger's own AI uses: mapping your chart of accounts, questions in the Console, and **Ask about this report**. Reading, reporting, forecasting and closing use none, and neither does anything your own AI client does. See [What RoboLedger needs to work well](what-it-needs.md#credits).

**Draft entry.** An entry that's in the ledger but not final yet, such as a schedule's depreciation or an adjustment your assistant drafted. Drafts usually post when the month closes. See [How RoboLedger works](how-roboledger-works.md).

**Driver.** An assumption in a forecast that moves several lines at once, the way a business does: revenue growth, cost of revenue as a share of revenue, days sales outstanding, and days payable outstanding. See [Plan and forecast with your AI assistant](plan-and-forecast.md). In the technical docs: [driver](https://robosystems.ai/docs/technical/forecasting-and-metrics).

**Event.** Something that happened in the business: an invoice issued, a bill received, a payment, a check written. Every entry in the ledger starts as one. See [How RoboLedger works](how-roboledger-works.md). In the technical docs: [event block](https://robosystems.ai/docs/technical/event-block-reference).

**Fact.** One number with its meaning attached: the concept it reports, the period it covers, and its unit. Statements, metrics and forecasts are made of facts. See [Explore statements and metrics over time](explorer.md).

**Fiscal calendar.** The record of which months are closed and which month you're working towards. See [What RoboLedger needs to work well](what-it-needs.md#a-fiscal-calendar).

**Graph.** One company's own database: its books, reports, plans and documents. It runs on a dedicated instance, and no other customer's data is in it. See [Graphs, tiers and credits](https://robosystems.ai/docs/guides/graphs-tiers-and-credits).

**Information block.** A statement, note, schedule, set of metrics or forecast, held together with the accounts and concepts it's built from, how its lines add up, and the checks it must pass. See [How RoboLedger works](how-roboledger-works.md#information-blocks). In the technical docs: [information block](https://robosystems.ai/docs/technical/information-blocks).

**Library.** The reporting concepts your accounts map to, with their definitions and where each sits on the statements. Most of it is **rs-gaap**, drawn from the US GAAP taxonomy public companies report in. See [The reporting library](the-library.md). In the technical docs: [frameworks and taxonomies](https://robosystems.ai/docs/technical/taxonomy-and-frameworks#the-three-frameworks).

**Mapping.** The link from one of your accounts to a reporting concept, which decides where the account lands on the statements. See [Map your chart of accounts](map-your-chart-of-accounts.md). In the technical docs: [mapping association](https://robosystems.ai/docs/technical/chart-of-accounts-mapping).

**Metrics.** A block of ratios or other standing figures, one column a month, computed from closed statements. See [Explore statements and metrics over time](explorer.md#metrics). In the technical docs: [metric block](https://robosystems.ai/docs/technical/forecasting-and-metrics).

**Plan history.** Past months' statements filled in behind a plan, for books that came over from QuickBooks without being closed month by month in RoboLedger. It never posts an entry. See [Plan and forecast with your AI assistant](plan-and-forecast.md#what-you-need-first).

**Posting.** Making an entry final. A posted entry doesn't change; to undo one, post a reversing entry. See [Close the month with your AI assistant](month-end-close.md#fixing-a-closed-month).

**Publish list.** A set of graphs you send reports to, such as an investor's RoboInvestor graph. Manage them under **Reports → Publish Lists**. See [Reports and sharing](reports-and-sharing.md#share-it).

**Reopen.** Unlocking a closed month to fix it. Reopening works backwards from the most recent closed month, and each reopen needs a reason. See [Close the month with your AI assistant](month-end-close.md#fixing-a-closed-month).

**Report.** Financial statements for a period, created from the ledger and kept as a snapshot to download or share. It changes only if you regenerate it. See [Reports and sharing](reports-and-sharing.md). In the technical docs: [report package](https://robosystems.ai/docs/technical/reporting-and-rendering#the-report-is-the-package).

**Reporting concept.** A standard line item, such as cash, accounts receivable or cost of revenue, that your accounts map to. See [The reporting library](the-library.md). In the technical docs: [element](https://robosystems.ai/docs/technical/taxonomy-and-frameworks#the-element-atom-and-associations).

**Scenario.** A named set of forecast assumptions, projected forward month by month from your last closed month. See [Plan and forecast with your AI assistant](plan-and-forecast.md). In the technical docs: [forecast block](https://robosystems.ai/docs/technical/forecasting-and-metrics).

**Schedule.** A recurring entry set up once, such as depreciation or a prepaid expense rolling off, that every close drafts and every forecast keeps running. See [Schedules for recurring entries](schedules.md). In the technical docs: [schedule block](https://robosystems.ai/docs/technical/information-blocks#authoring-an-information-block).

**Statement.** A balance sheet, income statement, cash flow or statement of equity. A live statement is built from the ledger when you ask and moves with every sync; the statements saved at a close and those in a report stay put. See [Ask about your books](ask-about-your-books.md#live-closed-and-reported).

**Subgraph.** A separate workspace inside a graph, for a test or a what-if, that shares its parent's permissions and credits. See [Data lake, subgraphs and backups](https://robosystems.ai/docs/guides/data-lake-and-subgraphs).

**Sync.** Bringing QuickBooks changes into RoboLedger. It happens when you ask, not on a schedule, and a regular sync looks back 60 days. See [Connect your books to your AI assistant](connect-your-books.md#2-connect-quickbooks).

**Write-back.** Sending an entry RoboLedger posted to QuickBooks. Only entries RoboLedger drafted are written back, when they post, unless one is marked to stay in RoboLedger. A catch-up entry for a change made in QuickBooks always stays. Syncing, reading and reporting never write. See [Nothing writes to QuickBooks until you post](quickbooks-write-back.md). In the technical docs: [write policy](https://robosystems.ai/docs/technical/quickbooks-sync-and-write-policy).
