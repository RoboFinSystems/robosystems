---
title: Map your chart of accounts
description: Mapping ties each QuickBooks account to a standard reporting concept, so statements, reports and forecasts come out right. How to review it and keep it current.
order: 20
section: Keep the books right
---

Your chart of accounts is yours: the names, the numbering, the accounts you added over the years. Mapping ties each of those accounts to a standard US GAAP reporting concept, like cash, accounts receivable or cost of revenue. It's how RoboLedger knows where an account belongs on the balance sheet, income statement, cash flow and statement of equity.

Everything built from statements depends on it: live statements, reports, the statements saved at each close, forecasts, and comparisons with public companies, which report in the same concepts.

## The first sync maps your accounts

When QuickBooks first syncs, RoboLedger maps your chart of accounts with AI. This is the one setup step that uses credits.

- Matches it's confident about are applied.
- Less certain matches are applied and flagged for you to review.
- Accounts it can't place are left unmapped.

This runs once. Accounts you add in QuickBooks later arrive unmapped, so check mapping whenever your chart of accounts changes.

If the graph keeps native books instead of QuickBooks, the Chart of Accounts page offers a template to start from. Mapping still has to be reviewed before you close.

## Review it

**In the app.** Open **Ledger → Chart of Accounts** and choose **Show mappings**. It shows your accounts, what each one is mapped to, and how much of the chart is covered. Change or clear a mapping on any account. **Auto-Map** runs the AI mapping again over what's left, and it uses credits. The [reporting library](the-library.md) is the vocabulary those mappings point at.

![The chart of accounts with each account's reporting concept and a coverage bar reading 20 of 20](images/chart-of-accounts.png)

**With your AI assistant.** It can list the accounts that aren't mapped, suggest a concept for each one, and apply the ones you agree with. Its suggestions come from matching on names and account types, not from AI inside RoboLedger, so they use no credits.

Ask: "Which accounts aren't mapped yet? Suggest where each one belongs, and wait for me before applying anything."

## What to look for

- **Unmapped accounts with activity.** Their balances are missing from your statements. An income statement that looks light, or a balance sheet that doesn't balance, usually starts here.
- **Flagged matches.** Check each one. A shareholder loan mapped as a bank loan still balances, and is still wrong.
- **Catch-all concepts.** An account mapped to an "other" concept shows up on the statement, but inside a line that tells you little. Give it a more specific concept if one fits.
- **Contra accounts.** Accumulated depreciation and allowances belong with the asset they reduce.

## Map before you close

Closing a month saves that month's statements, and they're built through the mapping as it stood. After that, RoboLedger refuses to change the mapping of an account that has activity in a closed month, because the saved statements would stop agreeing with the live ones. The message names the earliest month affected.

So map first, close second. If you need to change a mapping after the fact, reopen the months it touches, change it, and close them again. See [Close the month with your AI assistant](month-end-close.md).

## Try asking

- "How much of our chart of accounts is mapped?"
- "Show me every account mapped to an 'other' concept and suggest something more specific."
- "We added three accounts in QuickBooks this month. Sync, then map them."
- "Why is the balance sheet out of balance? Check for unmapped accounts with balances."
