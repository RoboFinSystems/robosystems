---
title: Billing and subscriptions
description: What RoboSystems bills for, who in your organization can change it, what each subscription status does to access, and how to change a tier, a plan or a trial.
order: 13
section: Your account and data
---

RoboSystems bills your organization, never a person. Each graph you create is its own monthly subscription, and each person's access to the SEC filings is a seat of its own. This page covers what you pay for, who can change it, and what the organization page's **Billing**, **Subscriptions** and **Invoices** tabs show.

## What you pay for

**Each graph is a subscription** at its tier's monthly price, started when the graph is created. Members aren't billed for graphs: one graph serves everyone you give access to.

**The SEC filings are billed per person.** A subscription is one person's seat, billed to the organization, so three analysts who each need the filings are three seats.

Credits pay only for AI that runs inside RoboSystems. Queries, reports, syncs, backups and every tool call from Claude, ChatGPT or another MCP client use none. See [Graphs, tiers and credits](graphs-tiers-and-credits.md) for what each tier includes, and the [pricing page](https://robosystems.ai/pricing) for current prices. A subscription's own price is shown on its row in **Subscriptions**.

## Who can do what

The **Billing**, **Subscriptions** and **Invoices** tabs are on your organization's page, which opens from its name at the top of the sidebar. Owners and admins see them; members don't.

| | Owner | Admin | Member |
|---|---|---|---|
| See billing, subscriptions and invoices | Yes | Yes | No |
| Add or change the payment method | Yes | No | No |
| Create a graph, starting its subscription | Yes | Yes, once a payment method is on file | No |
| Change a graph's tier, or cancel a graph | Yes | No | No |
| Start an SEC trial without a card | Yes | No | No |
| Subscribe yourself to the SEC filings once a card is on file | Yes | Yes | Yes |
| Change or cancel your own SEC seat | Yes | Yes | Yes |
| Cancel a member's SEC seat | Yes | Yes | No |

Owners and admins can also subscribe a member to the SEC filings over the API. Removing someone from the organization cancels their seats. See [Invite your team](teams-and-roles.md).

## The Billing tab

**Payment Method** shows the card on file, **No payment method on file**, or **Invoice billing enabled**. The owner adds a card with **Add Payment Method** and changes it with **Manage Payment Methods**; both open the payment portal. Everyone else sees "Only the organization owner can manage payment methods."

**Invoice billing** replaces the card: RoboSystems invoices the organization for each period instead of charging one. It is set up on request; contact support. An organization on invoice billing creates graphs without a card.

**Upcoming Invoice** previews one subscription's next invoice: its period, the amount due and its lines. Each graph and each seat is invoiced on its own, so with several subscriptions, the **Invoices** tab is the full record.

## The Subscriptions tab

**Graph Subscriptions** lists each graph with its tier, price and start date. **Repository Subscriptions** lists every SEC seat in the organization, each marked **Your seat** or **A member's seat**.

Each row carries a status:

| Status | What it means | What it does to access |
|---|---|---|
| **Active** | Paid and current | Everything the tier or plan includes |
| **Free trial**, with its end date | An SEC seat on its 14-day trial | The analysis tools at the trial's limits. See [the SEC trial](#the-sec-trial) |
| **Setting up** | Being created after a checkout or a subscription | Opens when setup finishes |
| **Upgrading** | A graph is moving to a new tier | The graph is read-only for a few minutes, and no backup can be made until it's done |
| **Cancelling**, with its end date | Cancelled at the end of the period, or a trial that won't continue | A graph can be read and exported but not changed until that date; a seat keeps full access until then |
| **Payment failed** | A charge was declined, and is being retried | A graph refuses every read and write until the payment goes through. A seat keeps its access while the payment is retried |
| **Paused** | Paused by RoboSystems support. Nothing pauses a subscription automatically | A paused graph refuses reads and writes until it is resumed. Contact support |

### When a payment fails

The graph's row says "The last payment for this graph failed." Update the card on the **Billing** tab; the graph opens again as soon as the payment goes through. The charge is retried for a while. If the retries run out with the payment still failed, the subscription lapses: a graph is suspended and removed seven days later, as a cancelled graph is (see [Take your data with you](take-your-data-with-you.md)), and a seat loses its access.

## Change a graph's tier

The owner chooses **Change Tier** on the graph's row, picks a tier and confirms with **Confirm Change**. The difference for the rest of the month is prorated onto the next invoice: a charge for a move up, a credit for a move down. From the next monthly refill, the graph gets the new tier's credits.

While the graph moves, its row shows **Upgrading**. The graph stays readable but refuses changes for a few minutes, no backup can be made, and it can't be cancelled until the move finishes. Your connections, keys and data stay the same.

- **Moving down** is refused if the graph has more subgraphs or more storage than the smaller tier allows. Delete subgraphs or reduce data first.
- **Large and XLarge** are provisioned on request. If the tier has no room, the change is refused; ask for it with **Request access** when you create a graph, or contact support.
- **Only an active graph can change tier**, so a graph with a failed payment pays first.

## The SEC trial

Your organization's first SEC seat starts with a **14-day free trial** of the plan you pick. The trial is once per organization: the first person to subscribe gets it, and later seats start on the plan.

**Starting it.** In **Repositories**, choose **Browse Repositories**, then **Start Starter free trial** or **Start Advanced free trial**. Without a card on file, only the organization owner can start a trial, no card is asked for, and the owner needs a verified email address. With a card on file, anyone in the organization can start it, and the card is charged when the trial ends.

**What it opens.** The analysis tools from Claude, ChatGPT, any MCP client and the API, search, and one download of the repository, all at lower limits than the plan. Writing your own graph queries, the AI analyst, the plan's limits and its monthly credits open with the first payment. The details are in [Analyze SEC filings](sec-filings.md#get-access).

**A trial started without a card** ends on its last day and nothing is charged. To keep the plan, the owner adds a card before then: with **Add a Card** on the trial banner on Home and **Repositories**, or **Add Payment Method** on the **Billing** tab. The plan then starts when the trial ends. A trial that has ended can't be taken again: subscribing later starts on the plan, with a card.

**Start Plan Now**, on your seat's row in **Subscriptions**, ends the trial today. Confirm with **Start Plan and Pay**: the card is charged the plan's first month, and the rest of the plan opens as soon as the payment confirms. Without a card on file, the dialog offers **Add a Card** instead. **Keep Trial** leaves it as it is.

**A seat can't be bought twice.** If you already hold one, on a trial or on the plan, change its plan or add a card instead of subscribing again.

## Change or cancel an SEC plan

**Change plan.** Choose **Change Plan** on your seat, in **Repositories** or on its row in **Subscriptions**, then **Upgrade to Advanced** or **Downgrade to Starter**. On the plan, the change takes effect at once, and the difference for the rest of the month is prorated onto the next invoice. On a trial, the trial carries on for the new plan: nothing is charged today, the trial's limits stay as they are, and the first charge is the new plan's price when the trial ends.

**Cancel.** Choose **Cancel Subscription** on the seat, in **Repositories** for your own or in **Subscriptions** for anyone's if you are an owner or admin. Pick when:

- **At end of billing period**: the seat keeps its access until then, and nothing more is charged. A trial cancelled this way runs to its last day.
- **Cancel Now**: access ends at once and the rest of the paid period is forfeited. Type the repository's name, `sec`, to confirm.

## Cancel a graph

The owner cancels a graph with **Cancel Subscription** on its row, either at the end of the billing period or at once with **Delete Graph Now**. What each removes, the credit for unused time and the final backup are in [Take your data with you](take-your-data-with-you.md#cancel-or-delete-a-graph). Download what you want to keep first.

## The Invoices tab

Every invoice the organization has received, newest first, with its number, date, amount and status, such as **paid** or **open**. Download a copy with the icon on its row. Owners and admins see this tab; only the owner can change the card that pays them.

## Go deeper

- [Billing endpoints](https://robosystems.ai/docs/technical/credits-and-billing#billing-endpoints): subscriptions, invoices and checkout over the API.
- [Graphs, tiers and credits](graphs-tiers-and-credits.md): what each tier includes and how credits refill.
