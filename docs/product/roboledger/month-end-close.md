---
title: Close the month with your AI assistant
description: How your AI closes a month on RoboLedger. It finds what's blocking, drafts the adjusting entries, shows you what will post, and closes when you approve.
order: 23
section: Keep the books right
---

Closing a month in RoboLedger locks the period, posts its adjusting entries, and saves that month's balance sheet, income statement and cash flow. Your AI assistant does the legwork. It checks what's blocking the close, drafts the entries, and shows you every one before anything posts.

Close is the last thing to set up, on purpose. Connect your books, ask questions, build reports and plans first. Once you trust the numbers, closing is the same conversation with one more step.

## Before your first close

- **Your books are connected and synced.** See [Connect your books to your AI assistant](connect-your-books.md).
- **Your chart of accounts is mapped.** A close still posts without it, but it can't save the month's statements. Map before you close: once a month is closed, the mapping of an account with activity in that month can't change unless you reopen it. See [Map your chart of accounts](map-your-chart-of-accounts.md).
- **Your recurring entries are schedules.** Depreciation, amortization and prepaid expenses that roll off each month are set up once as schedules, and every close drafts them. Your assistant can build them with you. It reads a past month's entries for the amounts and asks you for what history can't tell it, like an asset's cost, its useful life and the method. See [Schedules for recurring entries](schedules.md).
- **Your own procedures are written down, if you have any.** Ask your assistant to save them as a close procedures document. It looks for that document before every close and follows it.

## What happens during a close

1. **Your assistant checks where you stand.** It reads the last closed month, whether your QuickBooks sync is current, and anything blocking the next close. Months close in order, one at a time, and a month can't close until it's over.
2. **It clears the blockers with you.**
   - *The sync is behind.* Your assistant runs a fresh sync and waits for it.
   - *Transactions changed in QuickBooks after they synced.* These are normal. Your assistant shows you each one, and you choose to restate the month it belongs to, post the difference in an open month, or mark it as already handled. See [When QuickBooks changes after a sync](changes-after-sync.md).
   - *A schedule ends this month.* If an asset was sold or a policy cancelled, end its schedule before the month's entries are drafted.
   - *A schedule's entry for the month isn't drafted yet.* Your assistant drafts it.
3. **It drafts the month's entries.** Every active schedule gets its entry for the month, plus any one-off adjustment you ask for.
4. **It shows you the drafts.** Every entry with its debits and credits, whether they all balance, and which ones will be written to QuickBooks.
5. **You approve, and your assistant closes the month.** Posting the entries, checking that the balance sheet balances, and marking the month closed happen in one step.
6. **Your assistant reads you the receipt.** How many entries posted, how many went to QuickBooks, and whether the month's statements were saved and passed their checks.

A busy month can take longer to close than your assistant's call waits for. The close keeps running, and your assistant checks back until the receipt is ready. It never needs to be started twice.

The close tool tells your assistant to show you the drafts and wait for your explicit approval before it closes. Your MCP client may also ask you to allow the call.

## In the app

**Ledger → Closing Book** shows the same picture your assistant works from: the last closed month, the month you're working towards, what's blocking it, and each schedule's entry for the month with its amount and status. You can draft entries, close a month and reopen one from here too.

![The Closing Book period close view, showing the last closed month, a blocker, and four schedule entries pending for August](images/closing-book.png)

## After the close

- **Your statements for the month are saved.** They're what the Plan page and Explorer show for that month from now on, and they don't move when the live ledger does.
- **Run your forecasts again.** The month you closed becomes an actual, and each scenario picks up from its real closing balances. See [Plan and forecast with your AI assistant](plan-and-forecast.md).
- **Create a report** if you send statements to anyone. See [Reports and sharing](reports-and-sharing.md).

## Fixing a closed month

A closed month can be reopened to fix a missed or wrong entry. Reopening works backwards one month at a time, starting from the most recent, and each reopen needs a reason, which is kept in the audit trail. Entries that already posted stay posted. To undo one, post a reversing entry, then close the month again. Its statements are saved fresh.

## Try asking

- "What's blocking the August close?"
- "Set up a depreciation schedule for the delivery van we bought in March."
- "Draft September's closing entries and show me what will be written to QuickBooks."
- "Close September."
- "September is closed. Run our forecasts again and create the September report."
- "Reopen September. We missed an accrual for the October rent."
