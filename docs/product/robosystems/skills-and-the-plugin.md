---
title: Skills and the RoboSystems plugin
description: Skills teach your AI assistant a multi-step job on RoboSystems and the mistakes to avoid. The four skills, how to install them, and what to do without them.
order: 3
section: Start here
---

A connection gives your AI assistant the tools. A skill teaches it a job: which tools to use in which order, what to check before it trusts a number, and the mistakes that are easy to make along the way. The RoboSystems plugin bundles the connection with four skills.

## The four skills

- **robosystems** gets your assistant oriented: which graph the connection reaches, which tools answer which kind of question, and how to explore a graph from its schema and example queries.
- **sec-filing-analysis** reads public companies from their SEC filings: statements by ticker, comparisons across periods and companies, matching a concept in your words to the tags companies report, and searching the text of filings. It carries the rules that decide whether a number is right. See [Analyze SEC filings](sec-filings.md).
- **roboledger-close** runs a month-end close on RoboLedger: where the books stand, what's blocking the close, the schedule entries for the month, your review, the close and its receipt. It also sets up schedules before a first close. See [Close the month](https://roboledger.ai/docs/month-end-close).
- **roboledger-board-pack** turns a closed period into a board deck, as an HTML file and a PDF, with every figure checked, then revises the plan from what the board decides. See [Build a board pack](https://roboledger.ai/docs/board-pack).

You don't need to name a skill. When what you ask for matches what a skill is for, your assistant uses it.

## Install the plugin

The plugin is for Claude Code. It adds the RoboSystems server as well as the skills, so there's no separate connection to set up.

```bash
claude plugin marketplace add RoboFinSystems/robosystems-plugin
claude plugin install robosystems@robosystems
```

The first time a tool runs, your browser opens so you can sign in and choose a graph. To work on a different graph later, run `/mcp` and sign in again. See [Sign-in and graph access](oauth-and-graph-scope.md).

## Skills are public

The skills are plain Markdown, plus one HTML template for the board deck, in a public repository, [RoboFinSystems/robosystems-plugin](https://github.com/RoboFinSystems/robosystems-plugin), so you can read exactly what they tell your assistant. The plugin has no hooks and no commands, and nothing in it runs on its own. The only server it reaches is RoboSystems, through your sign-in.

The board pack skill does two things the others don't: it writes the deck into your working folder, and it asks your assistant to print it to PDF with Google Chrome on your computer. You see both before they happen.

## Without skills

Claude, ChatGPT and any other MCP client can do the same jobs without the plugin. Ask for them in plain language, one step at a time if it helps. RoboSystems tells every connected client what the graph is and where to start, and before a close your assistant reads the close steps from RoboLedger itself.

- A month-end close: [Close the month with your AI assistant](https://roboledger.ai/docs/month-end-close)
- A board pack: [Build a board pack](https://roboledger.ai/docs/board-pack)
- Public companies: [Analyze SEC filings](sec-filings.md)
- Anything else on your books: [Ask about your books](https://roboledger.ai/docs/ask-about-your-books)

## Try asking

- "Which RoboSystems skills do you have, and what is each one for?"
- "What's blocking the August close?"
- "Build the board pack for the third quarter."
- "Compare gross margin for [ticker] and [ticker] over the last eight quarters."

## Go deeper

- [The MCP tool surface](https://robosystems.ai/docs/technical/ai-operators-and-mcp#the-mcp-tool-surface): every tool the server offers, and how they're grouped.
