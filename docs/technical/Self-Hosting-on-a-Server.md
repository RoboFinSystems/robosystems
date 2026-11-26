# Self-Hosting on a Server

Run the stack on one machine on your own network and reach it from the others: a hostname, a certificate, one open port, users and API keys, and MCP clients on laptops. [Local Development](Local-Development.md) puts the stack on your own laptop and the [Bootstrap Guide](Bootstrap-Guide.md) deploys the platform into your own AWS account. This page is the shape between them. It describes what works today and ends with what still takes work by hand. On AWS, [one template](#on-aws-from-one-template) does the steps on this page for you.

## What This Shape Is

The same compose stack as Local Development, on a server, running the published images and the self-hosted environment (`ENVIRONMENT=selfhost`). No AWS account is involved: SeaweedFS serves S3, DynamoDB Local serves the graph registries, both on Docker volumes, and the graph databases live on the server's disk. Users reach the API through a reverse proxy on the one port you open. The server-side operators can run on an open-weight model you host ([Self-Hosted AI Models](Self-Hosted-AI-Models.md)), and MCP clients on laptops bring their own model.

## The Box

**Images.** Clone the repository and copy `.env.example` to `.env`. The image lines at the top of the file already point at the published images on Docker Hub. Replace `latest` with the release you tested (`robofinsystems/robosystems:v1.21.12`, for example), so that an upgrade is a decision you make by changing the tag. `just start` pulls the images and starts the `robosystems` profile: the API on 8000, the Graph API on 8001, Dagster (its webserver on 8002, and its daemon), the background worker, PostgreSQL, Valkey, SeaweedFS, DynamoDB Local, Mailpit and OpenSearch. The web apps are the separate `apps` profile and the observability services their own. The RoboInvestor app reads its filing catalog from the stack's own public-data bucket, which holds filings only once the SEC pipeline has run on the server; set `FILINGS_CDN_URL=https://public.robosystems.ai` in `.env` to search the hosted catalog instead. To upgrade, run `just upgrade-to v1.22.0` with the release you want, or `just upgrade-to latest` for the newest release, which is resolved to its tag first so the pin is always a version you can name: it checks that the image exists, lists the settings the release's `.env.example` names that your `.env` does not, moves the checkout to the tag so compose, scripts and image come from one ref, pins the tag in `.env`, pulls and recreates only the containers whose image changed, and waits for the API to report the new version. `--dry-run` shows the plan. It refuses to start while tracked files in the checkout have uncommitted changes, so keep local changes in `.env` or a compose override rather than in tracked files, and it saves the previous `.env` under `.local/env-backups`. Rolling back is the same command with the previous tag; migrations only go forward, so a release that migrated the database does not roll back.

**The environment.** Set `ENVIRONMENT=selfhost` in `.env`. It runs the same services as `dev` and holds them to the posture of a deployment rather than a developer's laptop: session cookies are marked secure, responses carry HSTS, the GraphQL IDE is off, query errors are not echoed back, and the API refuses to start on a configuration it would only warn about in development. That last part asks for two secrets of your own: a `JWT_SECRET_KEY` that is not the example's placeholder (`openssl rand -hex 32`), and a `CONNECTION_CREDENTIALS_KEY`, which encrypts the tokens of connected accounts (`just generate-key` prints one). A stack that already ran as `dev` encrypted those tokens with a key derived from its JWT secret; pin that same key before switching, or every connection has to be made again:

```bash
printf '%s_connection_encryption' "$(grep -E '^JWT_SECRET_KEY=' .env | cut -d= -f2-)" \
  | openssl dgst -sha256 -binary | base64 | tr '+/' '-_'
```

`selfhost` also lets the operators run on the Anthropic API (`ANTHROPIC_API_ENABLED=true` with `ANTHROPIC_API_KEY`), as `dev` does. The graph registry tables and the other names the stack creates are the `dev` ones, so a stack keeps its data when it switches.

**Email.** The API sends mail through Amazon SES unless `SMTP_HOST` is set in `.env`, in which case it sends over SMTP: `SMTP_HOST`, `SMTP_PORT` (465 is implicit TLS, any other port negotiates STARTTLS unless `SMTP_STARTTLS=false`), and `SMTP_USERNAME` and `SMTP_PASSWORD` if the relay wants them. Point them at a relay on your network, Microsoft 365 or Google Workspace, and verification, password reset and invitation mail is delivered. The sender is `EMAIL_FROM_ADDRESS`, which the relay has to be allowed to send as. The `.env.example` values point at Mailpit, the stack's own inbox on `8025`, which holds mail for reading but delivers nothing.

**Memory.** Each container has a memory cap that `compose.yaml` reads from `.env`, with defaults sized for a development machine with 24 GB. The "Container Memory Caps" block in `.env.example` lists the variables and a set for a 16 GB machine. Keep each cap under the memory Docker can use, and keep a heap set inside a container, such as the OpenSearch JVM, under that container's cap. Docker also refuses a CPU limit above the number of CPUs it has: on a 2-CPU machine set `DAGSTER_WEBSERVER_CPU_LIMIT=2`, the one limit above that.

**Where the data lives.** Graph databases are plain files under `./data/lbug-dbs` in the checkout, beside the staging, artifact and vector directories under `./data`. `just start` creates those directories before Docker does, so that on a Linux server they belong to the account running the stack. The platform image runs as uid 1000, which is the first account on most servers; under another account `just start` prints the one `chown` to run. PostgreSQL, Valkey, OpenSearch, SeaweedFS and DynamoDB Local keep their data in named Docker volumes (`pg_data`, `valkey_data`, `opensearch_data`, `seaweedfs_data`, `dynamodb_data`). A backup of the server is those directories and volumes together, taken while the stack is stopped or from a snapshot. Anything the platform would put in an S3 bucket, such as uploaded files and documents, exports and graph backups, goes to SeaweedFS on its volume, and survives a restart of the container and of the server. The `storage-init` container creates the buckets and registry tables on each start and leaves existing ones alone.

## A Name, a Certificate, One Port

Give the API a hostname on your network's DNS, an A record to the server's address, and keep that address fixed with a DHCP reservation or a static address. Get a certificate for the name from your own certificate authority, or from a public one through a DNS challenge if the name is in public DNS. Then run one reverse proxy on 443 that terminates TLS and forwards to the API on 8000. Run it on the host itself: the stack publishes its ports on the loopback address only, so a proxy on another machine, or in a container of its own, cannot reach them. With Caddy:

```
api.example.internal {
	tls /etc/caddy/certs/api.pem /etc/caddy/certs/api-key.pem
	reverse_proxy 127.0.0.1:8000 {
		flush_interval -1
	}
}
```

`flush_interval -1` passes the API's server-sent event streams through unbuffered, which the operation and operator endpoints use.

Tell the API its own address in `.env`:

```bash
ROBOSYSTEMS_API_URL=https://api.example.internal
```

The API uses this for the OAuth callbacks of connectors, SCIM resource locations, the connector URL it reports for a graph, and the issuer of the tokens it mints, so set it before anyone signs in. A changed `.env` reaches the containers when you run `just start` again, which recreates the ones whose settings changed. Check the result from another machine:

```bash
curl https://api.example.internal/v1/status
```

## The Web Apps

The three web apps are the `apps` profile; the `all` profile is the platform and the apps together, so `just start all` and `just upgrade all` run both. The published app images take their addresses when the container starts, from the same `.env` settings the API reads, so serving them on your names is the same pattern as the API: a name, a certificate and a proxy site for each, then the addresses in `.env`.

Give each app a name, and the object store one as well. Browsers upload and download files straight from the object store through signed links, and a page served over HTTPS cannot fetch a plain `http://` address, so the store needs a name behind the same proxy. One certificate can carry all five names, or each can have its own. With Caddy, beside the API's site:

```
app.example.internal {
	tls /etc/caddy/certs/app.pem /etc/caddy/certs/app-key.pem
	reverse_proxy 127.0.0.1:3000
}

ledger.example.internal {
	tls /etc/caddy/certs/ledger.pem /etc/caddy/certs/ledger-key.pem
	reverse_proxy 127.0.0.1:3001
}

investor.example.internal {
	tls /etc/caddy/certs/investor.pem /etc/caddy/certs/investor-key.pem
	reverse_proxy 127.0.0.1:3002
}

s3.example.internal {
	tls /etc/caddy/certs/s3.pem /etc/caddy/certs/s3-key.pem
	reverse_proxy 127.0.0.1:8333
}
```

Caddy passes the request's `Host` header through unchanged, which the object store needs: the signed links are signed for the name the browser uses. Then set the addresses in `.env`:

```bash
ROBOSYSTEMS_API_URL=https://api.example.internal
ROBOSYSTEMS_URL=https://app.example.internal
ROBOLEDGER_URL=https://ledger.example.internal
ROBOINVESTOR_URL=https://investor.example.internal
AWS_S3_PRESIGN_ENDPOINT_URL=https://s3.example.internal
```

and run `just start all`. The apps read these for the links between them and to the API, and the API reads them for the links in the mail it sends, for redirects after sign-in, and for the origins it accepts requests from: in the development environment that is localhost plus these three app URLs, so nothing goes in `EXTRA_CORS_ORIGINS` for them. The object store's upload bucket takes the same three origins for the browser's uploads, set by `storage-init` on each start. Sign-in happens on the RoboSystems app and carries over to the other two.

Two settings name an app address on their own. Passkeys, when `PASSKEYS_ENABLED=true`, are bound to the sign-in app's name, which `selfhost` takes from `ROBOSYSTEMS_URL`; under `dev` the default is `localhost`, so set `PASSKEY_RP_ID=app.example.internal` and `PASSKEY_ORIGIN=https://app.example.internal` there. A QuickBooks connection returns to `INTUIT_REDIRECT_URI`, which has to be `https://ledger.example.internal/connections/qb-callback` and registered with Intuit as such.

The addresses reach the apps at container start because the published images carry placeholders that their entrypoint fills in. An app image built from its source on this machine (an `*_APP_IMAGE` setting without a registry) bakes the addresses from its own repository's `.env` at build time instead, so a server should run the published images. The apps release on their own schedule: pin each `*_APP_IMAGE` line to the release you tested, as with the platform image. `just upgrade-to` moves the platform image only, and `just upgrade all` pulls the app images at the tags they are pinned to.

## Users and API Keys

Everything a user needs can be done over the API from a laptop with nothing but the name. Registration is `POST /v1/auth/register` with an email, a name and a password. `POST /v1/auth/login` returns a session token. `POST /v1/user/api-keys` with that token as a bearer creates an API key, which every later call sends as `X-API-Key`. The demo script runs exactly this sequence:

```bash
just demo-user --base-url https://api.example.internal
```

It registers a user, creates a key, and saves both to `.local/config.json`. [Authentication & API Keys](Authentication-and-API-Keys.md) describes the model behind them.

Registration is open by default: anyone who can reach the API can create an account. Once the first accounts exist, set `USER_REGISTRATION_ENABLED=false` and `ORG_MEMBER_INVITATIONS_ENABLED=true` in `.env` and run `just start` again. From then on a new account needs an organization invitation, which an org owner or admin sends; invitations are off until that second flag is set. For single sign-on against your identity provider, see [Enterprise SSO & SCIM](Enterprise-SSO-and-SCIM.md).

## Clients on Laptops

**MCP.** Every graph has an MCP endpoint at `https://api.example.internal/v1/graphs/{graph_id}/mcp`, authenticated by the API key. Claude Code connects to it directly:

```bash
claude mcp add --transport http robosystems \
  https://api.example.internal/v1/graphs/kg.../mcp \
  --header "X-API-Key: rfs..."
```

Clients that cannot send a header, and anyone who wants one connection for every graph on the server, use [the MCP bridge](The-MCP-Bridge.md) with `ROBOSYSTEMS_API_URL` set to the server's address. The operator endpoint and the tool surface are described in [AI Operators & MCP](AI-Operators-and-MCP.md). Connectors that run on a vendor's servers, such as the claude.ai and ChatGPT connectors, cannot reach a name that only your network resolves. That is the point of this shape.

**Trusting your certificate.** If the certificate comes from your own CA, each laptop has to trust its root. The operating system's store covers browsers, `curl` on macOS, and Claude Code, which reads the OS store by default ([Claude Code network configuration](https://code.claude.com/docs/en/network-config)). Python does not. The Python SDK, the demo scripts and the integration template trust a bundled list of public roots instead, and fail with `CERTIFICATE_VERIFY_FAILED` until `SSL_CERT_FILE` names a file that holds the public roots plus yours. The variable replaces the bundled list rather than adding to it, so build a bundle:

```bash
cat "$(uv run python -c 'import certifi; print(certifi.where())')" /path/to/your-root.pem > ca-bundle.pem
```

Then set `SSL_CERT_FILE=/path/to/ca-bundle.pem` in `.env.local`, which the `just demo-*` recipes load. The `truststore` package, which makes Python use the OS store, is the alternative.

## The Model Lane

The operators, the server-side AI, need a model, and a stack you run reaches one in any of three ways:

- **The Anthropic API, on your own key.** `ANTHROPIC_API_KEY` with `ANTHROPIC_API_ENABLED=true` in `.env`. This is the shortest route to the Claude models and the one [the AWS template](#on-aws-from-one-template) is built around.
- **A model you host.** Any server that implements the OpenAI Chat Completions API with tool calling, on the same machine or on another one on your network. [Self-Hosted AI Models](Self-Hosted-AI-Models.md) has the settings, which models have been tested, and what to check before pointing one at real books.
- **AWS Bedrock.** Where neither of the above is set the operators call Claude through Bedrock, with the `AWS_BEDROCK_*` credentials in `.env`. It is what the hosted platform uses, and it needs an AWS account with access to the Claude models.

With none of the three configured the operators do not run and everything else does: ingestion, reporting, the close, search and the MCP tools use no model on the server.

## On AWS, From One Template

RoboSystems Serve is this page as one CloudFormation stack: `cloudformation/serve.yaml` starts one EC2 instance in your own AWS account and does the steps above on it. From a checkout:

```bash
aws sso login --profile my-account
just serve launch
```

It asks four things: which AWS CLI profile (the account to launch into), the base name, the one address allowed to reach the stack, where `me` is the machine you run it from, and the size. It takes a single address, not a range. The base name has to sit in a public Route 53 hosted zone of that account, which the command finds from the name. Then it shows the account and what it will create, and asks before launching. The first three can be given as arguments instead, `just serve launch my-account serve.example.com me`, and the size as `SIZE=medium`. The account is always named or chosen and never read from the shell's own credentials, so a terminal signed in to another account cannot receive the stack. The stack is complete when the API answers, usually within fifteen minutes, and its outputs are the addresses.

**What the stack creates.** One ARM instance on Amazon Linux 2023, with 16 GB of memory unless you pick a larger size, reached through Session Manager, with no SSH. A separate encrypted data volume, mounted at `/data`, that holds the checkout and its `.env`, the graph files, Docker's volumes and the certificates. An Elastic IP and five DNS records under your base name: `api`, `app`, `ledger`, `investor` and `s3`. Caddy on the instance gets a public certificate for each name by itself. The stack runs `ENVIRONMENT=selfhost` ([The Box](#the-box)). The JWT secret, the credentials key, the admin key and the PostgreSQL and Valkey passwords are generated on the instance at first boot. Docker Compose and Caddy are installed at pinned versions. The platform runs the newest release whose image is published, and the three apps their latest images; `ROBOSYSTEMS_VERSION` picks another release at launch, and the other launch settings are listed at the top of `bin/tools/serve.sh`.

**Size.** Three sizes, all memory-optimised Graviton instances: `small` is an `r7g.large` with 16 GB, the default and the least the stack runs on; `medium` is an `r7g.xlarge` with 32 GB; `large` is an `r7g.2xlarge` with 64 GB. The instance sets its container memory caps from the memory it has, at every boot: the Graph API gets 4, 12 or 34 GB and the Dagster daemon, where jobs run, 3, 6 or 12 GB, with PostgreSQL and OpenSearch scaled beside them. `just serve resize medium` moves a running stack to another size: the instance stops, changes type and starts, a few minutes of downtime, and the caps follow. A cap you set yourself with `just serve set`, `GRAPH_API_MEMORY_LIMIT` for example, is kept over the sized one. Which size a workload needs is yours to judge; `just serve status` shows memory in use.

**Reaching it.** Port 443 is open to the address you gave and to nothing else; when your address changes, `just serve allow me` moves it. That is what this shape is: one operator's own deployment, reached from their own network. The object store's credentials are the same on every installation ([Limits Today](#limits-today)) and registration starts open, so it is not built to face the internet; a deployment a team or a client reaches from anywhere is what the hosted platform is for. Port 80 is open to the world for the certificate challenge and the redirect to HTTPS. Everything under [Users and API Keys](#users-and-api-keys) and [Clients on Laptops](#clients-on-laptops) applies as written, with public certificates, so no laptop needs a root installed.

**Running it.** The instance is yours to run, and `just serve` is how: `launch` remembers the profile, region and stack in `.local/serve.json`, and every other command acts on that stack through Systems Manager, so nothing on the instance listens for administration.

| Command | What it does |
| --- | --- |
| `just serve status` | The release running, each container's state, disk and memory |
| `just serve upgrade v1.25.0` | Moves the platform to a release (`latest` is the default) and waits for the API to report it; it is `just upgrade-to` on the instance |
| `just serve logs api` | The last lines of one service's log; a second argument is how many, up to 200 |
| `just serve restart worker` | Restarts one service, or every service with no argument |
| `just serve reboot` | Reboots the instance; the containers start by themselves |
| `just serve resize medium` | Moves the instance to another size (`small`, `medium`, `large`); a few minutes down |
| `just serve allow me` | Moves the one address 443 opens to, to this machine's or the one you name; it replaces the address, never adds one |
| `just serve snapshot` | Snapshots the data volume now, before an upgrade for instance |
| `just serve set KEY` | Changes a setting in `.env` and recreates the containers that read it |
| `just serve unset KEY` | Removes a setting made that way, back to the release's default |
| `just serve tunnel dagster` | Forwards the Dagster UI to `http://127.0.0.1:18002`; `mailpit` forwards the inbox to `18025` |
| `just serve shell` | A shell on the instance |

`set` asks for the value without showing it, or takes it as a second argument, which leaves it in your shell's history. Either way the value goes to a Secrets Manager secret in your account and the instance reads it from there, so a key is never in a command's text or in Systems Manager's history. What that secret holds is written into `.env` again on every `set`, `unset` and boot, so a line edited by hand on the instance lasts only until then if the secret names it; change such a setting with `set`. The secrets generated at first boot (the database and cache passwords, the JWT secret, the admin key) are refused, because the data on the volume was created with them.

A stack launched before `selfhost` existed runs `ENVIRONMENT=dev`. Upgrade it to a release that has `selfhost`, then run `just serve set ENVIRONMENT selfhost`, which first pins the credentials key the stack has been using, so its connections carry over.

`launch` creates a stack and never updates one: run against a stack that exists, it stops. Move a running stack to another release with `upgrade`.

The data volume is also snapshotted daily, seven kept. A snapshot is taken while the stack runs, so it is what a power cut would leave and the databases recover from it on start. Deleting the stack leaves a final snapshot, and `SNAPSHOT_ID=snap-... just serve launch` starts a new stack from one. The instance, the volume and the address bill until the stack is deleted.

**The operators.** The server-side operators run on the Anthropic API, on a key you provide. Set `ANTHROPIC_API_KEY` in your environment when you launch and the stack is configured for it. On a stack that is already running, two commands do the same, in this order:

```bash
just serve set ANTHROPIC_API_KEY            # asks for the key without showing it
just serve set ANTHROPIC_API_ENABLED true
```

Each one recreates the containers that read the setting, so there is nothing to restart or reboot afterwards. The key goes first because the API does not start with the Anthropic API turned on and no key to use. Without a key the operators stay unconfigured and everything else works.

The stack is not set up for Bedrock, on purpose: the instance's role can open a Session Manager session and read its own settings secret, and nothing else in your account. To run the operators on a model you host instead, set the `OPENAI_COMPAT_*` settings from [Self-Hosted AI Models](Self-Hosted-AI-Models.md) the same way.

## Limits Today

What this shape does not do yet, and what to do about it:

- **Service ports listen on the loopback address only** (`127.0.0.1`): 8000, 8001, 8002, 5432, 6379, 8333, 1025, 8025 and 9200 in the `robosystems` profile, 3000 to 3002 with the apps, and 4318, 8889, 9090 and 4000 with observability. Users reach the stack through the proxy and nothing else. To open the Dagster UI or Mailpit from another machine, forward the port over ssh (`ssh -L 8002:127.0.0.1:8002 <server>`) rather than publishing it.
- **The object store's credentials are fixed.** The S3 identity the stack uses (`test`/`test`) is the same on every installation, and the public-data bucket allows anonymous reads, which the browser needs. The keys are set in four places that must agree: `bin/setup/seaweedfs-s3.json`, `.env`, `.env.local` (the `just` recipes run on the host), and the `storage-init` service's environment in `compose.yaml`, which the backend services wait on. Change all four before exposing the box beyond a trusted network (the two tracked files then count as local changes, which `just upgrade-to` refuses to run over until they are committed or stashed), and keep port 8333 behind the host firewall like the others.
- **Outbound access the box needs**: Docker Hub for image pulls, the model endpoint if it is not on the box, and the APIs of any connector you turn on.

## Related Documentation

**Guides:**

- **[Local Development](Local-Development.md)** - The same stack on your own laptop, from `just start` to the first query
- **[Self-Hosted AI Models](Self-Hosted-AI-Models.md)** - Run the operators on an open-weight model
- **[Authentication & API Keys](Authentication-and-API-Keys.md)** - Users, sessions and keys
- **[AI Operators & MCP](AI-Operators-and-MCP.md)** - The MCP tool surface and connecting your own client
- **[Enterprise SSO & SCIM](Enterprise-SSO-and-SCIM.md)** - OIDC login and provisioning against your identity provider
- **[Security & Compliance](Security-and-Compliance.md)** - The security posture and built-in controls
- **[Bootstrap Guide](Bootstrap-Guide.md)** - The other half of self-hosting: your own AWS account

**Codebase Documentation:**

- **[Examples](https://github.com/RoboFinSystems/robosystems/blob/main/examples/README.md)** - The demo scripts behind `just demo-user`
- **[Configuration](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/config/README.md)** - Every setting named on this page

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Technical docs](Home.md)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)
