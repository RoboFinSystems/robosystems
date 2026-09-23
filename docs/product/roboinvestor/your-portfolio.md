---
title: Track private-company holdings
description: Create portfolios, add securities and positions, and link a holding to its issuer's RoboLedger graph when you have one.
order: 1
section: Get started
---

A RoboInvestor graph holds what you own: portfolios, the securities in them, and the positions that make up each holding. It is built for private companies — funds, trusts and people who hold stock, notes, SAFEs and similar instruments — not for a public brokerage feed.

Create a graph with RoboInvestor turned on at [robosystems.ai](https://robosystems.ai). Organization owners and admins create graphs, and creating one starts a subscription. If the page says graph creation requires approval, choose **Request access** and we'll set it up.

## In the app

Open **Portfolio**. You can:

- create a portfolio: a name, and optionally a strategy and a description
- add a security: its name, its type (common, preferred, a SAFE, a convertible note, LLC units and so on) and an optional subtype
- give the new security a starting position: the quantity, its unit (shares, units or a percentage) and the cost basis

The page lists holdings from their active positions, so a security added without a starting position doesn't show. Holdings are grouped by the company that issued them, once the security is linked to one. A security that isn't linked yet sits under **Unlinked Securities**.

Adding a position to a security you already have, changing or disposing of a position, setting a portfolio's inception date, and deleting portfolios and securities are done through your AI assistant. See below.

A company that wants to share reports with you needs this graph's ID. It is on the graph's **Dashboard** at [robosystems.ai](https://robosystems.ai). **All Entities** lists the companies in your RoboInvestor graphs, including each company that has shared a report with you, a minute or two after its first share.

## Link an issuer

When a portfolio company keeps its books in RoboLedger, you can record its graph ID on the security, when you add it or later with the pencil beside it (**Link Security**). The two graphs stay separate.

The company decides where its reports go, by adding your graph to a publish list. The link is how a report meets the holding: when that company first shares a report with this graph, every security carrying its graph ID is attached to it. A graph ID you add after that first share is attached the next time the company shares. See [Reports shared with you](reports-you-receive.md).

You do not need the link to track a position. Use it when both sides are on RoboSystems.

## Ask your AI assistant

Connect the graph as in the [overview](index.md). Your assistant can list portfolios, positions and values, and read the reports companies have shared with you. It can also make the changes the Portfolio page doesn't: add a position to an existing security, update a position's quantity, cost or current value, dispose of a position, set a portfolio's inception date, and delete portfolios and securities. Deleting a security retires it but keeps its positions, so dispose of the position to take it off the page.

Reading and updating the graph through your assistant uses no credits. The **Console** in the app runs on RoboSystems' own AI and does use credits. See [Graphs, tiers and credits](https://robosystems.ai/docs/guides/graphs-tiers-and-credits).

## Try asking

- "What are my largest positions by current value?"
- "Which holdings aren't linked to a company yet?"
- "Update the value of our [company] SAFE to $250,000 as of June 30."
- "Show me every position in [portfolio], with cost and current value."

## Go deeper

- [Cross-graph issuer linking](https://robosystems.ai/docs/technical/roboinvestor-operations#cross-graph-issuer-linking): how a holding meets the reports its issuer shares.
