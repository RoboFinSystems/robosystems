# Connecting Bank Feeds Locally

How to turn on the two bank feeds, Mercury and Plaid, on a **self-hosted** RoboSystems stack: the settings each needs, which credential route works where, and what connecting and disconnecting do on your machine. On robosystems.ai none of this is needed, because connecting a feed is a button in the app.

Both feeds work the same way once connected. Each posted transaction lands in the ledger's Inbox with a suggested account, and nothing reaches the books until it is classified and committed. The [Pipeline Guide](Pipeline-Guide.md#bank-feeds-mercury-plaid) covers the sync, and [Event Block Reference](Event-Block-Reference.md) covers the captured lines.

| Feed | Reaches | Local route | Production route |
| --- | --- | --- | --- |
| **Mercury** | Mercury accounts only | A read-only API token per company: no tunnel, no partner agreement | The same token route, or a Mercury partner OAuth client |
| **Plaid** | Most US banks and cards | Plaid Sandbox over `http://localhost` | Plaid Production over an HTTPS origin (a tunnel or a real domain) |

## Before You Start

1. Complete the base setup so `just start` works ([Local Development](Local-Development.md)), and run roboledger-app with `npm run dev` on port 3001.
2. **Set `CONNECTION_CREDENTIALS_KEY` in `.env`.** Every feed credential, a Mercury token or a Plaid access token, is stored encrypted with this key, one row per connection. Without it a development stack derives a key from `JWT_SECRET_KEY` and logs a warning, and a stack with `ENVIRONMENT=prod` refuses to start. Generate one with:

   ```bash
   uv run python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
   ```

   Keep it with your other secrets. Losing it makes every stored credential unreadable, and each feed has to be connected again.
3. Give the company a chart of accounts. A feed links each account it reads to a chart account by name, or adds one, so the entity it books to needs a chart first: **Ledger → Chart of Accounts → start from a template**. A feed cannot book to an entity whose books a connected ledger keeps: the group parent while QuickBooks is connected, or any entity an external ledger source mirrors. Connect the feed for another entity of the group. The consolidation entity takes no feed either.

Flags and credentials are read when the containers start. After editing `.env`, run `just start` (not `just restart`, which keeps each container's old environment).

## Mercury

Mercury connects one company's Mercury organization to one graph. There are two credential routes.

- **A read-only API token** (`MERCURY_API_KEY_CONNECTIONS_ENABLED=true`). Each company creates a token in its own Mercury account and pastes it into the connect form, so one installation can serve many companies, each with its own token. This is the route for a self-hosted stack.
- **Partner OAuth** (`MERCURY_CLIENT_ID`, `MERCURY_CLIENT_SECRET`). The user signs in at Mercury and approves read-only access. The client is issued to Mercury's API partners under its partner agreement, after a compliance review, so most self-hosters will not have one. This is the route robosystems.ai uses, and the hosted service never accepts pasted tokens.

The connect form follows what the deployment has. With a partner client, it offers the Mercury sign-in, and the token field appears as an alternative when tokens are allowed. Without one, the token is required and the sign-in is hidden. With neither, Mercury is not listed.

### Settings

```bash
# robosystems/.env
CONNECTION_MERCURY_ENABLED=true
MERCURY_API_KEY_CONNECTIONS_ENABLED=true
MERCURY_ENVIRONMENT=production   # "sandbox" for test tokens from sandbox.mercury.com

# Only with a Mercury partner OAuth client:
# MERCURY_CLIENT_ID=...
# MERCURY_CLIENT_SECRET=...
```

`MERCURY_ENVIRONMENT` picks the API host for both routes: `production` reaches `api.mercury.com` and takes tokens from real Mercury accounts; `sandbox` reaches Mercury's sandbox and takes tokens from a sandbox account. A token from one is rejected by the other.

### Connecting a company

1. In the company's Mercury account, open **Settings → API Tokens** and create a **read-only** token. Read-only tokens need no IP allowlist.
2. In roboledger-app, open **Connections → Add Connection → Mercury**, pick the entity the accounts book to, paste the token, and choose **Connect with API token**.
3. The backend checks the token against Mercury before saving it, stores it encrypted with the connection, records the consent, and starts the first sync. The first sync pulls every posted transaction from the backfill date (two years back unless the connection named another). Each later sync reaches 60 days back from the last sync that succeeded, so a feed that was quiet, or failing, for longer than that still pulls the days in between.

A graph holds one Mercury connection, so one Mercury organization. Its accounts can still book to several entities in a group, each account moved to its own entity. Each company on the installation connects its own organization in its own graph.

If the token stops working (it was deleted in Mercury, or Mercury removed it after a long time unused), syncs fail until the connection is replaced: disconnect it and connect again with a new token.

*Mercury is a fintech company, not an FDIC-insured bank. Banking services provided through Choice Financial Group and Column N.A., Members FDIC.*

## Plaid

Plaid connects one bank login at a time, chosen inside Plaid Link, an embedded window in the app. A graph can hold one Plaid connection per bank login.

### Settings

```bash
# robosystems/.env
CONNECTION_PLAID_ENABLED=true
PLAID_CLIENT_ID=...
PLAID_SECRET=...            # the secret for the environment below
PLAID_ENVIRONMENT=sandbox   # or "production"
```

The client ID and secrets are in the Plaid Dashboard under **Team Settings → Keys**.

### Redirect URI

The app asks Plaid for every Link session with a redirect URI of `<app origin>/connections/plaid-callback`, where a bank that signs in through OAuth sends the user back. Plaid requires that URI to be on the **Allowed redirect URIs** list in its Dashboard, and its rules decide where Plaid can run:

- **Sandbox** allows `http://localhost`. Add `http://localhost:3001/connections/plaid-callback` and Link works from a local stack with no tunnel. Plaid's sandbox institutions sign in with its published test credentials.
- **Production** requires an `https` URI. A local stack needs a public HTTPS origin for the app: an ngrok or cloudflared tunnel, set up as in [Connecting QuickBooks Locally](Connecting-QuickBooks-Locally.md#path-b--production--public-tunnel), with the tunnel's origin in `EXTRA_CORS_ORIGINS` and its callback URL on Plaid's list. A server with its own domain registers that domain's callback instead.

Plaid's production environment is available on a paid plan or a trial, which you arrange with Plaid.

### Connecting a bank

Open **Connections → Add Connection → Bank account (Plaid)**, pick the entity, and choose the bank and accounts in Link. When Link finishes, the backend exchanges Plaid's public token, stores the access token encrypted with the connection, and starts the first sync. A bank already connected to the graph is refused.

## Disconnecting

**Disconnect** on a feed's connection card ends it the same way for both feeds:

- The credential is given up: a Mercury OAuth grant is revoked at Mercury, a Plaid Item is removed at Plaid, and the stored credential is emptied. A pasted Mercury token is deleted from the stack; it stays valid in Mercury until you delete it there.
- Lines still waiting in the Inbox are deleted, with the counterparties only they used.
- Entries already posted stay in the books, without the feed's transaction details.
- The chart accounts stay, and stop being linked to the feed.

Connecting again starts with a fresh full sync, which links the existing chart accounts by name and skips lines already posted.

## Troubleshooting

**Mercury is missing from Add Connection.** `CONNECTION_MERCURY_ENABLED` is off, or it is on with neither a partner client nor `MERCURY_API_KEY_CONNECTIONS_ENABLED`. Check the running container (`docker exec robosystems-api printenv | grep MERCURY`) and run `just start` after changing `.env`.

**"Mercury rejected the API token, or it can read no accounts."** The token is mistyped, deleted, or from the other environment: a sandbox token with `MERCURY_ENVIRONMENT=production`, or the reverse.

**Plaid Link will not open in production.** The redirect URI the app sent is not on Plaid's allowed list, or it is `http`. Open the app through its HTTPS origin, not `localhost`.

**Connecting is refused because the company has no chart of accounts.** Start one from a template first, under Ledger → Chart of Accounts.
