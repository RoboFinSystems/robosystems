---
title: Reports and sharing
description: Build financial statements from your books with Claude, download them as XBRL or JSON-LD, and share a report with another RoboLedger or RoboInvestor graph.
order: 4
---

A report turns your ledger for a period into financial statements: the balance sheet, income statement, cash flow and statement of equity. It lives in your graph, and you choose who else gets a copy.

## Create a report

Ask Claude, or open **Reports → Create Report**. The report is built from your mapped chart of accounts, so map your accounts first. See [What RoboLedger needs to work well](what-it-needs.md).

- "Create a report for September."
- "Create a report for the third quarter and tell me anything that looks off."

## Download it

Open a report in **Reports → View Reports** to download it as:

- an **XBRL 2.1 package**, the standard behind public company filings
- a **JSON-LD bundle**
- a **holon** (JSON-LD) or **Tavi** (JSON) file, which open in the free viewer at [xbrlkit.com](https://xbrlkit.com)

## Share it

Sharing happens in the app, not through Claude. Open a report, choose **Share**, and pick a publish list. A publish list is a set of graphs you send reports to, such as your investor's RoboInvestor graph or your advisor's RoboLedger graph. Manage lists under **Reports → Publish Lists**.

**The recipient gets the statement, never the ledger.** A shared report copies the report, its statements and their facts, and the report files. Your transactions, customers, vendors and journal entries stay in your graph.

If someone sends you reports you don't want, add them under **Reports → Blocked Senders**.
