---
title: What RoboLedger needs to work well
description: Current books, a mapped chart of accounts, a fiscal calendar and schedules for recurring entries. What each one does, and how to check it with your AI.
order: 3
section: Get started
---

Your AI assistant's answers are only as good as the books behind them. Four things decide that, and most are set up for you on the first sync.

## Books that are current

RoboLedger syncs each connection on its own once a day. For anything more recent, press **Sync Now** on the QuickBooks card, or ask your assistant to sync, before you ask about recent activity.

- **Sync Now** asks how far back to go. **Last 60 days** picks up recent changes. If something older changed in QuickBooks, choose **From a specific date** or **Full rebuild**, or ask your assistant to sync from a date.
- A close is blocked until QuickBooks has synced after the month ended.
- If a transaction was edited in QuickBooks after it synced, RoboLedger flags it and asks you how to treat it. See [When QuickBooks changes after a sync](changes-after-sync.md).

Ask: "When did QuickBooks last sync, and is it current enough to close August?"

## A mapped chart of accounts

Mapping ties each of your accounts to a standard reporting concept, so RoboLedger knows where it belongs on the balance sheet, income statement, cash flow and statement of equity. Without it, statements come back empty, and a close can't save the month's statements.

The first sync maps your accounts with AI:

- matches it's confident about are applied
- less certain matches are flagged for you to review
- the rest are left unmapped

Review them in **Ledger → Chart of Accounts**. **Auto-Map** runs the AI mapping again, and it uses credits. Your assistant can also list what's unmapped, suggest a match for each, and apply the ones you agree with. Its suggestions use no credits. Accounts you add in QuickBooks later arrive unmapped, and mapping has to be right before a month closes. See [Map your chart of accounts](map-your-chart-of-accounts.md).

Ask: "Which accounts aren't mapped yet? Suggest where each one belongs."

## A fiscal calendar

The calendar records which months are closed and which month you're working towards. The first sync sets it up with the month two months back marked as the last one closed, and last month as the next to close. The fiscal year starts in January.

A company with no calendar shows **Set up calendar** on the **Entities** page. Setting one up asks where that company's books start, and that choice can't be changed later:

- **New books, or moving over at a month-end:** pick the first month these books close. It starts open, so a cutover's opening balances go in it.
- **History already closed in another system:** pick the last month closed there. It and every month before it are locked, and the first close is the month after.

The dialog shows the first close before you confirm. If it's months back, every month from there has to close in order. A subsidiary follows the group's fiscal year. Your assistant can set the calendar up too, once you tell it which of the two applies and the month. If QuickBooks hasn't finished its first sync, wait for it instead: the sync sets the calendar up with last month as the next to close.

Ask: "Where does our fiscal calendar stand, and what's blocking the next close?"

## Schedules for recurring entries

Depreciation, amortization and prepaid expenses that roll off each month are set up once as schedules, and every close drafts their entries. Without them, those adjustments have to be asked for by hand every month. Schedules also carry into every forecast. See [Schedules for recurring entries](schedules.md).

## Reconciliations, if the close should wait on them

A reconciliation ties an account's balance to something outside the ledger: QuickBooks, a statement balance you record, or a schedule's own figures. Each one can be set to hold the close until it ties, or until someone signs it off, and a difference up to its threshold still counts as tied. When the books change after a reconciliation ran, it's marked out of date until it runs again. See [Reconcile your accounts](reconcile-your-accounts.md).

Ask: "Which reconciliations are holding the August close, and what doesn't tie?"

## Closed months, if you want to plan

A forecast starts from closed months. If your history came over from QuickBooks and you haven't closed month by month in RoboLedger, your assistant can fill in that history for you. It does this over the general address or your graph's own address, not RoboLedger's own ([Which address to use](connect-your-books.md#which-address-to-use)). See [Plan and forecast with your AI assistant](plan-and-forecast.md).

## A close procedures document, if you have one

If your company does things its own way, like a specific accrual or an account that needs a manual check, write it down and ask your assistant to save it as a close procedures document in your graph. Before a close, your assistant looks for that document and follows it.

## Credits

Reading your books, building reports, running forecasts and closing the month use no credits. AI mapping does, from your plan's monthly allowance. So do questions you ask in the app's **Console** and in **Ask about this report**, which run on RoboLedger's own AI. Claude, ChatGPT and other clients you connect yourself don't. Check your balance on the **Usage** page at [robosystems.ai](https://robosystems.ai).
