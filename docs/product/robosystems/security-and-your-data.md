---
title: Security and your data
description: How RoboSystems keeps each graph separate, encrypts it, controls sign-in, and what an AI client or RoboSystems' own AI sees of your data.
order: 10
section: Your account and data
---

A graph holds a company's books, reports and plans, so the first question is who else can see them. The short answer is: people you give a role on the graph, your organization's owners and admins, and the AI clients those people connect. Everything else on this page is how that holds.

## Each graph is kept apart

- **Its own database.** Every graph is a separate database on a dedicated instance. No other customer's data or workload shares it.
- **Its own ledger records.** RoboLedger and RoboInvestor records are stored in a schema of their own for each graph, never in shared tables.
- **Nothing crosses on its own.** Data leaves a graph only when someone with access shares a report, and a shared report copies the statements, never the ledger. See [Reports and sharing](https://roboledger.ai/docs/reports-and-sharing).

## Encryption and where it runs

RoboSystems runs on AWS in the United States (us-east-1).

- Data is encrypted in transit between you and RoboSystems (TLS), and at rest (AES-256), backups included.
- The access QuickBooks and other connections grant is encrypted separately, with a key of its own. No screen or API returns it, to you or anyone else. It is used to sync, and to write back the entries RoboLedger posts; see [Nothing writes to QuickBooks until you post](https://roboledger.ai/docs/quickbooks-write-back). RoboSystems never sees your QuickBooks password.
- API keys and connected-app tokens are stored as one-way hashes, so a stored key can't be read back.

## What an AI client sees

When you connect Claude, ChatGPT or another MCP client, it works through tools, and each tool answers with the data it was asked for: a statement, a list of accounts, a search result. Those answers go to your AI client, and from there to its model provider, under the terms of the client you chose. RoboSystems doesn't send your data to them. Your client asks for it, one request at a time.

What the client can reach is fenced:

- **One graph per connection**, chosen by you on the consent screen.
- **Your role on that graph.** A client can't do more than you can. A viewer's client can read but not write.
- **Revocable.** **User Settings → Connected apps** ends a connection immediately, and changing your password signs every connected app out.

See [Sign-in and graph access](oauth-and-graph-scope.md).

## AI that runs inside RoboSystems

Some features use RoboSystems' own AI rather than your client's: the **Console**, **Ask about this report** in RoboLedger, and chart-of-accounts mapping (automatic when a QuickBooks company has no mappings yet, and the **Auto-Map** button). They run on Claude models hosted in AWS Bedrock, with inference kept in the United States, and each kind of work goes to the model that fits it. A change made through the Console's `/do` runs on Claude Opus, the most capable of them, because a wrong change costs more than a wrong answer. Questions, report answers and account mapping run on Claude Sonnet. RoboSystems' AI does not use your data to train models, ours or the model provider's. What your own AI client's provider does with what it reads is governed by that client's terms. These features use credits; see [Graphs, tiers and credits](graphs-tiers-and-credits.md).

## Signing in

- Sign in with a password, and add a **passkey** under **User Settings** for a sign-in that can't be phished. Owners and admins should.
- Once you have a passkey, a password sign-in also asks for it, as a second factor. You can instead choose **Sign in with a passkey** and skip the password.
- Your first passkey comes with one-time recovery codes, shown once. Keep them somewhere safe: a recovery code stands in for the passkey if you lose the device.
- Graph access is checked on every request, not only at sign-in. Someone removed from a graph loses it within ten minutes, and usually at once.
- Removing someone from your organization ends their sessions, API keys and connected apps. See [Invite your team](teams-and-roles.md).

## Audits and the code

RoboSystems is not SOC 2 attested today. A SOC 2 Type II examination is in progress.

The platform is open source under Apache 2.0, so the controls on this page can be read in the code rather than taken on trust. The [Security and compliance](https://robosystems.ai/docs/technical/security-and-compliance) page in the technical docs lists them.

## Report a problem

Email [security@robosystems.ai](mailto:security@robosystems.ai), or open a [private security advisory](https://github.com/RoboFinSystems/robosystems/security/advisories/new) on GitHub. Please don't open a public issue.

## Go deeper

- [Privacy policy](https://robosystems.ai/pages/privacy): what we collect, how it is used, and your rights.
- [Graphs and multi-tenancy](https://robosystems.ai/docs/technical/graphs-and-multi-tenancy): how graphs are isolated, in technical detail.
- [Take your data with you](take-your-data-with-you.md): exports, backups, and what happens when you leave.
