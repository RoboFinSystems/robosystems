---
title: Invite your team
description: Invite people to your organization, give them a role on each graph, and remove them. What owners, admins, members and viewers can each do.
order: 12
section: Your account and data
---

People work on graphs through an **organization**. You invite someone into the organization, then give them a role on each graph they need. Members aren't billed per seat; each graph is its own subscription.

## Invite someone

1. At [robosystems.ai](https://robosystems.ai), click your organization's name at the top of the sidebar and open **Members**.
2. Choose **Invite Member**, enter their email, and pick **Member** or **Admin**.
3. They get an email with a link to create their account. It expires after seven days. Pending invitations are listed on the same page, where you can resend or revoke them.

Owners and admins can invite. **Invitations are for people who don't have an account yet.** Each account belongs to one organization, so someone who already has an account, in your organization or another, can't be invited.

An invitation gives no graph access by itself. Admins see every graph; a member sees only the graphs you add them to.

## Organization roles

| | Owner | Admin | Member |
|---|---|---|---|
| Create graphs | Yes | Yes, once the owner has added a payment method | No |
| Every graph in the organization, as its admin | Yes | Yes | Only graphs they're given |
| Invite, change roles, remove people | Yes | Yes | No |
| Payment method, plan changes, cancelling | Yes | No | No |
| View invoices | Yes | Yes | No |

The person who creates the organization is its owner. Ownership can't be changed in the app; contact support if it needs to move.

## Give someone a graph

A graph admin opens the graph's **Dashboard** and chooses **Members**, then picks someone from the organization and a role. Access to a graph covers its subgraphs.

| Graph role | Can |
|---|---|
| **Viewer** | Read: statements, reports, close status, documents, search, and ask questions |
| **Member** | Everything a viewer can, plus change things: post and approve entries, map accounts, set up schedules, create and share reports, close and reopen periods, connect and sync QuickBooks |
| **Admin** | Everything a member can, plus manage the graph's members, disconnect QuickBooks, make and download backups, create and delete subgraphs, and edit the graph's details |

Organization owners and admins are admins on every graph and show as **Via org role**; change their access on the organization's page.

A connected AI client has exactly its user's role. A viewer's assistant can read the books but can't post or close.

## Remove someone

- **From one graph**: remove them in the graph's **Members**. Their access ends within ten minutes, and usually at once.
- **From the organization**: remove them on the organization's **Members** page. They lose every graph, and because an account belongs to one organization, their account is deactivated: sessions, API keys and connected apps all stop working. SEC subscriptions the organization paid for them are cancelled.

Owners and admins can't remove themselves. There's no button for a member to leave on their own; ask an owner or admin.

## Your accountant or advisor

An outside accountant needs an account in your organization to work in your books, which means an account that doesn't already belong to their firm's organization.

A fractional CFO who keeps several companies' books usually does it the other way round: the client graphs live in the CFO's own organization. See [Work across several companies](https://roboledger.ai/docs/working-across-companies).

To give an investor, lender or advisor your statements without access to the books, share a report with their graph. See [Reports and sharing](https://roboledger.ai/docs/reports-and-sharing).

## Go deeper

- [Graph membership](https://robosystems.ai/docs/technical/authentication-and-api-keys#graph-membership): how organization roles and graph roles combine.
- [Security and your data](security-and-your-data.md): passkeys, and what a connected AI client can reach.
