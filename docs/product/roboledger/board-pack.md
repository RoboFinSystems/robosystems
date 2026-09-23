---
title: Build a board pack
description: Turn a closed quarter into a board deck, as an HTML file and a PDF, with every figure checked. What it needs, and how to ask for it in any client.
order: 13
section: Work with your books
---

A board pack is your closed books, presented. The RoboSystems plugin has a skill for it: your AI assistant pulls the closed period from your graph, checks the figures, and builds a slide deck you can send. After the meeting, it can revise your plan from what the board decided.

## What you get

Two files side by side in the folder you're working in: the deck as an HTML file, and a PDF of it for the board. Unless your company has its own board pack format, the deck runs:

- a cover saying which period is closed, how current the QuickBooks sync is, and any changed transactions still waiting to be settled
- where you stand, and a scoreboard of your key metrics
- revenue against plan, margins and operating expenses
- the balance sheet, cash and runway, and working capital, including who owes you and whom you owe
- the operating plan
- controls and the close: how much of the chart of accounts is mapped, and what's still open
- risks, the decisions you want from the board, and an appendix saying where each number came from

## What it needs

- **A closed period.** The pack reports a closed month or quarter. If the period isn't closed, your assistant says so and offers to close it first, or to build a pack from live figures that's clearly labeled as one. See [Close the month with your AI assistant](month-end-close.md).
- **A mapped chart of accounts,** so the statements come out right. See [Map your chart of accounts](map-your-chart-of-accounts.md).
- **A plan, if you want actuals against it.** Revenue against plan and the forward view come from a forecast scenario. See [Plan and forecast with your AI assistant](plan-and-forecast.md).
- **Computed metrics, if you have them.** Without them, your assistant works the ratios out from the statements and says so on the slide.

Before it starts, your assistant asks three things at once: which period, who's in the room, and the two or three decisions you want out of the meeting. It also looks in your graph for a board reporting document and for what it remembers from the last pack, so the next one picks up where this one left off.

## How the figures are checked

The skill has your assistant check every figure before it reaches a slide. Each statement comes with its own checks, and a statement that fails them doesn't go on a slide as though it passed. Net income has to agree across the income statement, the balance sheet and the cash flow. Every slide names where its numbers came from, and the appendix lists them.

## Where it runs

The skill writes the deck into your working folder and prints the PDF with Google Chrome on your computer. It runs in an assistant that works with files on your machine, such as Claude Code with the RoboSystems plugin installed. See [Skills and the RoboSystems plugin](https://robosystems.ai/docs/guides/skills-and-the-plugin).

Building the pack only reads your books. When it's done, your assistant offers to save the pack to your graph as a document and to remember the decisions you asked for. Each needs your yes.

## After the meeting

Tell your assistant what the board decided. It records the decisions and names the assumptions they move in your plan, such as a hiring date or a price. With your yes, it changes them and runs the plan again, then shows you the new figures before anything goes on a slide. The revised deck keeps the original plan beside the new one and prints to a new PDF, so the one the board saw stays as it was.

## In other clients

In Claude, ChatGPT or any MCP client without the skill, ask for the same thing in steps. Your assistant can read everything the pack is built from:

- the statements for the closed period, with their checks
- your metrics over time, and actuals against a plan scenario
- what each customer owes and what you owe each vendor
- where the close stands and how much of the chart of accounts is mapped
- your documents, such as a board reporting procedure or last quarter's memo

It can then lay the figures out in whatever form your client can make.

## Try asking

- "Build the board pack for the third quarter."
- "Is the quarter closed? If not, what's blocking it?"
- "Show me revenue against our budget for each month of the quarter, and what's driving the gap."
- "The board approved two hires in November. Update the plan and show me what changes before you touch the deck."

## Go deeper

- [Guard rails](https://robosystems.ai/docs/technical/reporting-and-rendering#guard-rails): the checks every statement runs before it's shown.
- [Forecasting and metrics](https://robosystems.ai/docs/technical/forecasting-and-metrics): how plan scenarios and metric series are computed.
