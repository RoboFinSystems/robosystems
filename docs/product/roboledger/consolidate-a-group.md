---
title: Consolidate a group of companies
description: Several companies in one set of books, the consolidated statements drafted from them each month, and what changes when a company is bought or sold.
order: 26
section: Keep the books right
---

When your books hold more than one company, RoboLedger keeps each company's books, chart and close its own, and produces the group's consolidated statements from them: every company added together, the balances they owe each other and the business they do with each other taken out, and an outside owner's share shown as noncontrolling interest. The consolidating entries are drafted for you from the books every month. This page is how to set a group up, what each month asks of you, and how the group changes when a company is bought, changes hands or is sold.

For moving between companies, who can see what, and connecting your assistant to each, see [Work across several companies](working-across-companies.md).

## Set the group up

**One graph is one group.** The company your books were created for is the group parent. Add each subsidiary as a company in the same graph, with the share the parent holds. Companies that have nothing to do with each other, such as a firm's clients, never share a graph.

**The share decides the treatment.** Half or more and the company is consolidated in full: its books are summed, and the part the parent does not own is carried as noncontrolling interest. A fifth to a half takes the equity method: it is left out of the sum, and the group carries the investment and its share of the company's result instead. Less is an investment at cost. Where control is not the percentage, set the treatment yourself.

**Add the consolidation entity.** It is one more company in the group that holds only the consolidating entries. Add it, mark it as the group's consolidation entity, and start its chart from the *Consolidation adjustments* template. The template carries every account the consolidating entries need; a chart you build by hand has to carry the same ones, and your assistant tells you which one is missing when an entry needs it.

**Map every account.** Each company's accounts are mapped to the reporting concepts, and the group is built entirely from those concepts, never from account numbers or names. An account with activity and no mapping is in no statement, so the close will not run until it is mapped. A chart started from a template arrives mapped; a chart from QuickBooks or one you built is mapped with your assistant, account by account, and the mapping page explains how: [Map your chart of accounts](map-your-chart-of-accounts.md).

## Each month

**The drafts.** Once every company has its month in the books, ask your assistant for the month's consolidating entries. It works out what the group needs: the parent's investment eliminated against each subsidiary's capital, with an outside owner's share of that capital as noncontrolling interest; the balances the companies owe each other; income one company earned from another against the other's expense; the outside owners' share of the month's result; and the parent's share of an equity-method company's result. Ask to see them first, then have them written as drafts in the consolidation entity. Each entry brings its balance to what the month needs, less what the consolidation entity already holds, so an entry you post yourself counts, and drafting a month again replaces only the earlier drafts.

A transaction is intercompany when it names another company in the group as its counterparty, so record the fee, the loan, the rebill with the sibling company as the counterparty on both sides. Where an outside owner's share of a month's result is not their percentage, with a preferred return or a waterfall, tell your assistant the amount and why, and the drafts use it.

**What the books do not explain is left for you.** When the parent's investment in a subsidiary does not match its share of the subsidiary's capital, that elimination is held back and the difference is named, with the likely reason: the parent booked the subsidiary's distributions as income, which the draft reverses on its own; the company was bought and the purchase has not been recorded yet; or the parent bought or sold shares without recording it. When two companies disagree about what one owes the other, the lower amount is eliminated and the difference is listed. Correct the books, post the explaining entry, or close over the difference knowingly.

**The group close.** Close each company's month, then the parent's, then the consolidation entity's, which is the group close. It waits until every company in the month has closed, the month's consolidating entries are drafted or posted, nothing is left unexplained, and the consolidated balance sheet and statement of changes in equity tie. It posts the drafts and saves the month's consolidated statements. See [Close a group of companies](month-end-close.md#close-a-group-of-companies).

## When the group changes

**You buy a company.** Add it with the day it joined and the share you bought, open its books on its balance sheet at that date, and record the purchase in the parent's books, naming the seller. Then have your assistant record the acquisition: what you paid, and if you have them, the fair value of the outside owners' interest and any step-ups to named assets. The price above your share of the company's equity becomes goodwill. The month it joins, the group's cash flow shows the acquisition on one row, net of the cash that came with the company.

**You buy more of it, or sell some of it, and keep control.** Record the parent's side in its books, then have your assistant record the change in share from the month it holds. There is no gain or loss in the group either way. Buying, the price above the book value of what you bought adjusts the parent's equity; selling, the gain the parent's own books take is reversed, the outside owners get their larger share, and the difference adjusts the parent's equity. The price moves out of investing and into a payment to, or from, the outside owners.

**You sell it, or it is dissolved.** Close its books through its last month, disconnect any bank feed on it, and settle anything the companies still disagree about. Then set the month it left. From the next month the group sums none of it, it shows as a former member, and the drafts reverse its eliminations on their own. Record the parent's sale in its books, and have your assistant record the deconsolidation with the price: goodwill and the outside owners' interest come off, the results the company earned while in the group go back to the group's equity, and the difference is the group's gain or loss, which is smaller than the parent's own because the group had already counted those results as its income. The month after, the group's cash flow shows the cash that left on the divestiture row.

## What you read

On the group parent, **Ledger → Statements** has a Scope control: *This entity*, *Combined* (the companies summed, nothing eliminated), *Consolidated* (with the consolidation entity's adjustments), and *Worksheet*, each company in its own column, then the combined sum, the adjustments and the consolidated total, with an equity-method company beside the sum. The worksheet is where to check a consolidating entry.

A balance sheet shows the companies in the group that month. An income statement over several months sums each company's own months. A cash flow or equity statement over a window a company joined or left inside is not shown for that window, because it would show balances arriving or leaving with no line for them; read the months on either side. The month a company joins and the month after it leaves are shown in full.

**Reports → Create Report** on the group parent offers *Consolidated group* beside the parent alone, and the group close saves the month's consolidated statements for every later read. See [Reports and sharing](reports-and-sharing.md#create-a-report).

## What it does not do yet

A company cannot move between the equity method and full consolidation from a date; its treatment is fixed once the group has closed a month with it. Groups are flat, one parent and its companies. A company in another currency is not translated. Unrealized profit on goods or assets sold within the group is an entry you make. Ask your assistant when one of these is yours; the limit, and what to do instead, is in its answer.

## Try asking

- "Draft this month's consolidating entries and tell me what you could not explain."
- "Why is the elimination for Harbor Café held back?"
- "Show me the consolidation worksheet for August."
- "We bought Northside Café on 1 October for $31,500. Record the acquisition."
- "The founders bought a tenth of Northside back in April for $7,000. Record it."
- "We sold Northside at the end of May for $62,000. Take it out of the group."
- "What is the group's gain on the sale, and why is it smaller than ours?"
