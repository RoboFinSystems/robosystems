---
title: See who owes you and whom you owe
description: Open invoices and bills as of any date, by customer or vendor and by how late they are, tied out to your receivables and payables accounts.
order: 17
section: Work with your books
---

**Ledger → Receivables & Payables** shows what is still open on a date you choose: the invoices your customers haven't paid, or the bills you haven't paid your vendors, grouped by counterparty and by how far past due each one is. Below the totals, the page ties the open items out to your receivables or payables account, so you can see whether the list and the ledger agree.

## What the page shows

Choose **Receivables** or **Payables** under **Show**, and a date under **As of**. The date starts at the last month-end you've closed, or today if you haven't closed one, and you can move it to any day. An invoice or bill counts if it was issued on or before that date and something on it was still unpaid at the end of that day. A payment dated after it hasn't happened yet, so you can see the aging as it stood at any past month-end.

The cards along the top total the open amounts in five buckets by days past due: **Current** (not yet due), **1–30 days**, **31–60 days**, **61–90 days** and **Over 90 days**, then **Total open**. Where a customer or vendor has been paid more than an invoice or bill was for, the overpayment isn't aged: it's an unapplied credit, a negative amount, and the **Total open** card says how much of the total it is.

The table has a row for each customer or vendor, largest open balance first, with the number of open items beside the name, the amount in each bucket, the total and the oldest due date. Items with no counterparty share a row of their own, **No counterparty**. Open a row to see its documents: each invoice or bill with its date, its due date and how many days late it is, its amount (**Original**), what has been paid against it by the as-of date (**Settled**) and what is left (**Open**).

**Export CSV** downloads the table: one row per counterparty with each bucket, the unapplied credit, the total, the number of open items and the oldest due date.

## Where the open items come from

- **On books synced from QuickBooks**, the invoices and bills come over with the sync, with the due dates QuickBooks holds, and the payments QuickBooks applied to them settle them here. What a QuickBooks payment pays is changed in QuickBooks, and the next sync brings the change.
- **On books kept in RoboLedger**, your assistant records an invoice or bill with its due date or its terms, such as net 30. One recorded with neither is due on the day it was issued. Like any other entry, it's a draft until the close posts it, and it appears here once its month closes.
- **A sales receipt**, paid at the time of the sale, is never an open item.

An invoice or bill that is still waiting in the Inbox to be approved isn't in the books yet, so it isn't open here either.

When some open items have no due date of their own, the page says how many and ages them as due on the day they were issued. Ask your assistant to set the due date on each, and they age on their terms. A due date can be changed in a month you've already closed, since it doesn't change any entry.

## The tie-out to the ledger

**Tie-out to the ledger** sets each company's open items beside the posted balance of its receivables or payables account at the same date. Your receivables account is the one mapped to receivables in your chart of accounts, or QuickBooks' own Accounts Receivable account; your payables account is the one mapped to accounts payable, or QuickBooks' Accounts Payable account. The page names the account it compared with, then shows **Ledger**, **Open items**, and **Ties** when the two agree, or **Difference** and the amount when they don't.

A difference is never plugged. It's whatever moved the account without a document behind it: a write-off, a payment that wasn't linked to the invoice it paid, or an opening balance.

A payment waiting for the close is not a difference. A bank line you've approved this month settles its invoice right away, but its entry posts to the account only when the month closes. The tie-out counts what the close will post from documents already in the list, so the two still tie before the month is closed.

**No control account mapped** means the company has no account to compare with: no account in its chart is mapped to receivables (or to accounts payable), and it has no QuickBooks Accounts Receivable (or Accounts Payable) account. The open items are still listed. Map the account and the tie-out appears. See [Map your chart of accounts](map-your-chart-of-accounts.md).

## A group of companies

On the group parent of a graph with more than one operating company, a **Scope** control offers **This entity**, the parent's own books, and **Group**: the parent and every company consolidated in full that month, added together. A company on the equity method isn't added in. Pick a subsidiary from the company menu to see its books on their own.

In **Group**, what the companies owe each other is left out of the totals and the table, since consolidation eliminates it. It's shown instead under **Between group entities**: for each pair, the company owed (**Owed to**), the company that owes (**Owed by**), the open invoices one company holds against the other (**Receivable**), the open bills the other holds against it (**Payable**), and **Agrees** when the two match, or the difference when they don't. A difference there is something the two companies' books disagree on, and the quickest thing to settle before the month's consolidating entries are drafted. See [Consolidate a group of companies](consolidate-a-group.md).

Each company in the group keeps its own tie-out line. In **Group**, the table's rows don't open into documents, because a row can add up more than one company's books; switch to **This entity** or pick the company to see them.

## When a bank line pays an invoice or bill

A payment settles an invoice or bill only when someone says which one it pays. In the Inbox, a bank line that pays one open bill or invoice of its counterparty for exactly its amount arrives with an **Open item** suggestion, and taking it settles that item. Where a payment covers several, or part of one, you tick the ones it pays under **Settles a bill?** or **Settles an invoice?**, and the line is applied to them in the order shown. A line approved with nothing ticked leaves the bill or invoice open. See [Lines from a bank feed](inbox.md#lines-from-a-bank-feed).

Your assistant can do the same: ask it which open items a deposit or payment could settle, and it lists them, an exact amount first, then the oldest due.

## Corrections

On books kept in RoboLedger, a posted invoice or bill is corrected by reopening its month and reversing its entry. Once it's reversed, the invoice or bill leaves the list, and you can issue it again. Reversing a payment's entry works the same way: the invoice or bill it paid is open again. A reversal leaves the receivables or payables account where it was before the entry, so the tie-out still holds.

## Try asking

- "Who owes us money, and what's more than 60 days late?"
- "Age our payables as of October 31. Whom do we owe, and when does each bill fall due?"
- "Does the receivables aging tie to the books at September 30? If not, what's in the difference?"
- "Record an invoice to Harbor & Pine Studio for $4,800, issued today, net 30."
- "Harbor & Pine has until December 15 to pay invoice 1042. Change its due date."
- "This $9,600 deposit pays two invoices. Show me which ones it could settle, and settle both."
- "Across the group, do the companies agree on what they owe each other?"

## Go deeper

- [Receivables & Payables](https://robosystems.ai/docs/technical/receivables-and-payables): what makes an open item, how a payment settles one, and the aging and open-item reads behind this page.
- [Settling an item](https://robosystems.ai/docs/technical/receivables-and-payables#settling-an-item): one payment applied to several invoices or bills, and what is refused.
