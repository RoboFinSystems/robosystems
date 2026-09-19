---
title: What RoboLedger needs to work well
description: Current books, a mapped chart of accounts, a fiscal calendar and schedules for recurring entries. What each one does, and how to check it with your AI.
order: 3
section: Get started
---

Your AI assistant's answers are only as good as the books behind them. Four things decide that, and most are set up for you on the first sync.

## Books that are current

RoboLedger syncs when you ask it to, not on a schedule. Press **Sync Now** on the QuickBooks card, or ask your assistant to sync, before you ask about recent activity.

- A regular sync picks up the last 60 days of changes. If something older changed in QuickBooks, choose a full sync in the sync options, or ask your assistant to sync from a specific date.
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

If your graph has no calendar, use **Initialize Calendar** on the QuickBooks card. A calendar set up this way starts with no closed months, so the first close is the earliest month in the calendar.

Ask: "Where does our fiscal calendar stand, and what's blocking the next close?"

## Schedules for recurring entries

Depreciation, amortization and prepaid expenses that roll off each month are set up once as schedules, and every close drafts their entries. Without them, those adjustments have to be asked for by hand every month. Schedules also carry into every forecast. See [Schedules for recurring entries](schedules.md).

## Closed months, if you want to plan

A forecast starts from closed months. If your history came over from QuickBooks and you haven't closed month by month in RoboLedger, your assistant can fill in that history for you. See [Plan and forecast with your AI assistant](plan-and-forecast.md).

## A close procedures document, if you have one

If your company does things its own way, like a specific accrual or an account that needs a manual check, write it down and ask your assistant to save it as a close procedures document in your graph. Before a close, your assistant looks for that document and follows it.

## Credits

Reading your books, building reports, running forecasts and closing the month use no credits. AI mapping does, from your plan's monthly allowance. The **Console** in the app, and **Ask about this report**, also use credits — they run on RoboLedger's own AI. Claude, ChatGPT and other clients you connect yourself don't. Check your balance on the **Usage** page at [robosystems.ai](https://robosystems.ai).
