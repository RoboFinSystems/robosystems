---
title: Close the month with Claude
description: How Claude closes a month on RoboLedger. It finds what's blocking, drafts the adjusting entries, shows you what will post, and closes when you approve.
order: 6
---

Closing a month in RoboLedger locks the period, posts its adjusting entries, and saves that month's balance sheet, income statement and cash flow. Claude does the legwork. It checks what's blocking the close, drafts the entries, and shows you every one before anything posts.

Close is the last thing to set up, on purpose. Connect your books, ask questions, build reports and plans first. Once you trust the numbers, closing is the same conversation with one more step.

## Before your first close

- **Your books are connected and synced.** See [Connect your books to Claude](connect-your-books.md).
- **Your chart of accounts is mapped.** A close still posts without it, but it can't save the month's statements. See [What RoboLedger needs to work well](what-it-needs.md).
- **Your recurring entries are schedules.** Depreciation, amortization and prepaid expenses that roll off each month are set up once as schedules, and every close drafts them. Claude can build them with you. It reads a past month's entries for the amounts and asks you for what history can't tell it, like an asset's cost, its useful life and the method.

## What happens during a close

1. **Claude checks where you stand.** It reads the last closed month, whether your QuickBooks sync is current, and anything blocking the next close. Months close in order, one at a time, and a month can't close until it's over.
2. **It clears the blockers with you.**
   - *The sync is behind.* Claude runs a fresh sync and waits for it.
   - *Transactions changed in QuickBooks after they synced.* These are normal. Claude shows you each one, and you choose to restate the month it belongs to, post the difference in an open month, or mark it as already handled.
   - *A schedule's entry for the month isn't drafted yet.* Claude drafts it.
3. **It drafts the month's entries.** Every active schedule gets its entry for the month, plus any one-off adjustment you ask for.
4. **It shows you the drafts.** Every entry with its debits and credits, whether they all balance, and which ones will be written to QuickBooks.
5. **You approve, and Claude closes the month.** Posting the entries, checking that the balance sheet balances, and marking the month closed happen in one step.
6. **Claude reads you the receipt.** How many entries posted, how many went to QuickBooks, and whether the month's statements were saved and passed their checks.

The close tool tells Claude to show you the drafts and wait for your explicit approval before it closes. Your MCP client may also ask you to allow the call.

## Fixing a closed month

A closed month can be reopened to fix a missed or wrong entry. Reopening works backwards one month at a time, starting from the most recent, and each reopen needs a reason, which is kept in the audit trail. Entries that already posted stay posted. To undo one, post a reversing entry, then close the month again. Its statements are saved fresh.

## Try asking

- "What's blocking the August close?"
- "Set up a depreciation schedule for the delivery van we bought in March."
- "Draft September's closing entries and show me what will be written to QuickBooks."
- "Close September."
- "Reopen September. We missed an accrual for the October rent."
