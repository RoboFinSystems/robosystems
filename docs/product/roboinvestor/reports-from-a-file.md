---
title: Load a report from a PDF or a spreadsheet
description: A portfolio company that isn't on RoboLedger sends a board package. Your AI assistant reads it into your graph, checked total by total.
order: 3
section: Get started
---

Most portfolio companies do not keep their books in RoboLedger. They send a workbook or a PDF: a balance sheet, a profit and loss statement, perhaps a cash flow statement and a page of metrics. Your AI assistant can read that file into your RoboInvestor graph as a report, so it sits beside the reports companies [share with you](reports-you-receive.md) and beside your holding in that company.

You do this through your assistant. The app shows the result; it has no upload button.

A company can also send the report as a file that is already structured: a holon (a file ending in .holon.jsonld) or a Tavi model (.tavi.json), which RoboLedger and other reporting tools export. Give your assistant that file instead and it is loaded as it stands, with nothing to read out or name. It goes through the same checks and is kept the same way.

## What your assistant does

1. **Adds the company**, once, as an issuer in your graph. You do not need a holding in the company to load its report; a company you only follow works the same way. A holding you do have can then be attached to it on the Portfolio page (see [Track private-company holdings](your-portfolio.md)). A report is always loaded onto an issuer, never onto your own fund: your own entity's reports come from a ledger, and a load aimed at it is refused.
2. **Reads the statements out of the file**, value by value. Each number keeps the cell or the page it came from.
3. **Names each line** with a standard reporting concept, so a line means the same thing in every company's report. The company's own wording is kept: a line they call "Personnel Expense" still reads that way.
4. **Checks the report before anything is saved.** Every total has to equal the lines beneath it, and total assets have to equal total liabilities and equity. A report that fails is refused, with the total, the period and the lines that were counted. A figure the package states twice, such as a total rounded to thousands on a summary page and in full on the statement, is loaded once, at the more precise of the two. If the two disagree, the report is refused.
5. **Saves it and reads it back**, so you can compare it with the file.

A second load for the same company, period and kind of report replaces the first, so a corrected package is just loaded again.

## What to give it

- The file itself. A spreadsheet reads more reliably than a PDF, because every value has a cell.
- The company's name, and an identifier if you have one (a tax ID or an LEI), so the same company is not added twice.
- Anything the file does not say: whether figures are in dollars or thousands, and which columns are actual results.

Budget and forecast columns are not loaded yet. Neither are columns that only restate other columns, such as a variance or a percentage change.

One report holds up to 3,000 values and 750 concepts, a concept being one named line. A monthly package is a few hundred values, so the limit leaves room for a full annual report with its comparative years. A larger one is refused before anything is saved.

## What you should check

Your assistant tells you what it decided. Read that before you rely on the report:

- **The units**, where a header and the values disagree.
- **Lines the company arranges differently** than the standard layout, such as depreciation shown below operating expenses. The report still loads, and each difference is listed.
- **Anything the file itself gets wrong.** If the company's own totals do not add up, your assistant should stop and tell you which one, rather than change a number.

Nothing ties the cash flow statement to the balance sheet automatically. Ask your assistant to confirm that opening cash plus the change in cash equals closing cash.

## In the app

**Portfolio Reports** lists every report you have received, with an icon that says whether it was shared from the company's graph or loaded from a file. Open one to read its statements.

## Removing one

Ask your assistant to delete the report. Its values go, and so do the line names that were added only for that report. The company stays in your graph.

## Try asking

- "Load this board package for [company] as their June quarterly report. Check it before you save it."
- "Do a dry run first and tell me what doesn't add up."
- "Read back the income statement you just loaded and compare it with the file."
- "How does [company]'s gross margin this quarter compare with the last four?"

## Go deeper

- [Receiving a report](https://robosystems.ai/docs/technical/roboinvestor-operations#receiving-a-report-from-a-company-that-is-not-on-the-platform): the request your assistant sends, the checks, and what a refusal says.
- The roboinvestor-receive-report skill in the [RoboSystems plugin](https://github.com/RoboFinSystems/robosystems-plugin) gives your assistant the whole method.
