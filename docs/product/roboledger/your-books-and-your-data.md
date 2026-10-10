---
title: Your books and your data
description: What RoboLedger copies from QuickBooks, who can see it, what your AI assistant sees, and how to take it all with you.
order: 7
section: Get started
---

Connecting QuickBooks to an AI assistant raises fair questions: where do the books go, who can see them, and can you get them back out. The short version is below. The full answers are in the RoboSystems guides, because RoboLedger runs on the RoboSystems platform.

## What RoboLedger keeps

The first sync copies your QuickBooks history into your company's graph, and later syncs keep it current. The ledger records sit in a store of their own for your graph, and the graph's database runs on a dedicated instance shared with no other customer. RoboLedger reads QuickBooks and writes back only the entries you post. See [Nothing writes to QuickBooks until you post](quickbooks-write-back.md).

RoboLedger never sees your QuickBooks password. The access Intuit grants it is encrypted, no screen or API returns it, and it is used only to sync and to write back the entries you post.

## Who can see it

- **People you give a role on the graph**, and your organization's owners and admins: viewers read, members work on the books, admins also manage who has access. See [Invite your team](https://robosystems.ai/docs/guides/teams-and-roles).
- **The AI clients those people connect**, each reaching one graph with its user's role, and revocable at any time.
- **People you share a report with** get the statements, never the ledger. See [Reports and sharing](reports-and-sharing.md).

## What your AI assistant sees

Your assistant reads what it needs to answer, through RoboLedger's tools, and those answers go to its model provider under your AI client's terms. **Ask about this report**, the Console and account mapping run instead on RoboSystems' own AI: Claude models hosted in AWS Bedrock, with inference kept in the United States. A change you ask the Console to make runs on Claude Opus, the most capable of them; questions, report answers and mapping run on Claude Sonnet. RoboSystems' AI doesn't use your data to train models. See [Security and your data](https://robosystems.ai/docs/guides/security-and-your-data).

## Taking it with you

- **Reports** download as holon, Tavi or XBRL 2.1 files. The holon is the complete one.
- **Explorer** series and **Plan** scenarios download as CSV or JSON.
- **Journal entries, accounts and transactions** can be read row by row over the API, or ask your assistant to list them.
- **The graph's database** downloads as a backup from robosystems.ai, for graph admins. It holds the ledger and plans as of the graph's last refresh, and what your assistant has been asked to remember. Documents and report files aren't in it, so download reports on their own. Downloads are limited each month by tier.
- **Disconnecting QuickBooks** keeps everything already imported.
- **Before cancelling**, download what you want to keep.

See [Take your data with you](https://robosystems.ai/docs/guides/take-your-data-with-you).
