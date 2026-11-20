---
title: Track private-company holdings
description: Create portfolios, add securities and positions, and link a holding to its issuer's RoboLedger graph when you have one.
order: 1
section: Get started
---

A RoboInvestor graph holds what you own: portfolios, the securities in them, and the positions that make up each holding. It is built for holders of private-company securities (funds, trusts and people who hold stock, notes, SAFEs and similar instruments), not for a public brokerage feed.

Create a graph with RoboInvestor turned on at [robosystems.ai](https://robosystems.ai). Organization owners and admins create graphs, and creating one starts a subscription. If the page says graph creation requires approval, choose **Request access** and we'll set it up.

## In the app

Open **Portfolio**. You can:

- create a portfolio: a name, and optionally a strategy and a description
- add a security: its name, its type (common, preferred, a SAFE, a convertible note, LLC units and so on) and an optional subtype, with its starting position: the quantity, its unit (**Shares**, **Units** or **Principal**) and, optionally, the cost basis

**Add Security** needs the starting position, because the page lists holdings from their active positions. A security your assistant creates without a position doesn't show. Holdings are grouped by the company that issued them, once the security is linked to one. A security that isn't linked yet sits under **Unlinked Securities**.

Each company's card shows the total cost of its positions and, once any of them has a current value, their total value. A position with no current value shows two dashes and adds nothing to the total. When only some are valued, the total is followed by a note such as **(2 of 3 marked)**, so a partial total is not read as the whole holding. Current values are set through your AI assistant; the app does not ask for one. Amounts are shown in the portfolio's base currency, US dollars unless your assistant set another.

Adding a position to a security you already have, changing or disposing of a position, setting a portfolio's inception date, and deleting portfolios and securities are done through your AI assistant. See below.

A company that wants to share reports with you needs this graph's ID. It is on the graph's **Dashboard** at [robosystems.ai](https://robosystems.ai). **All Entities** lists your own entities: the fund or holder each RoboInvestor graph is for. Portfolio companies are not listed there. A company that has shared a report with you, or one your assistant added for a report sent as a file, shows up in three places: the **Company** list when you add or link a security, the heading over its holdings on **Portfolio**, and the **From** column of **Portfolio Reports**.

## Link an issuer

When a portfolio company keeps its books in RoboLedger, you can record its graph ID on the security, when you add it or later with the pencil beside it (**Link Security**). The two graphs stay separate.

The company decides where its reports go, by adding your graph to a publish list. The link is how a report meets the holding: when that company first shares a report with this graph, every security carrying its graph ID is attached to it. A security you add with its graph ID after that first share is attached at once. A graph ID you add to an existing security later is attached the next time the company shares. See [Reports shared with you](reports-you-receive.md).

A company that is not on RoboSystems has no graph ID. Once your assistant has added it as an issuer (see [Load a report from a PDF or a spreadsheet](reports-from-a-file.md)), choose it under **Company** when you add the security, or with the pencil afterwards. Its holdings then sit under its name, beside the reports loaded for it. The pencil can set or change a link, not remove one; ask your assistant for that.

You do not need the link to track a position. Use it when both sides are on RoboSystems.

## Ask your AI assistant

Connect the graph as in the [overview](index.md). Your assistant can list portfolios, positions and values, and read the reports companies have shared with you. It can also make the changes the Portfolio page doesn't: add a position to an existing security, update a position's quantity, cost or current value, dispose of a position, set a portfolio's inception date, and delete portfolios and securities. Deleting a security retires it but keeps its positions, so dispose of the position to take it off the page.

Reading and updating the graph through your assistant uses no credits. The **Console**, in a drawer at the bottom of every page, runs on RoboSystems' own AI and does use credits. See [Graphs, tiers and credits](https://robosystems.ai/docs/guides/graphs-tiers-and-credits).

## Try asking

- "What are my largest positions by current value?"
- "Which holdings aren't linked to a company yet?"
- "Update the value of our [company] SAFE to $250,000 as of June 30."
- "Show me every position in [portfolio], with cost and current value."

## Go deeper

- [Cross-graph issuer linking](https://robosystems.ai/docs/technical/roboinvestor-operations#cross-graph-issuer-linking): how a holding meets the reports its issuer shares.
