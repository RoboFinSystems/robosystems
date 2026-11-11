# Self-Hosting on a Server

Run the stack on one machine on your own network and reach it from the others: a hostname, a certificate, one open port, users and API keys, and MCP clients on laptops. [Local Development](Local-Development.md) puts the stack on your own laptop and the [Bootstrap Guide](Bootstrap-Guide.md) deploys the platform into your own AWS account. This page is the shape between them. It describes what works today and ends with what still takes work by hand.

## What This Shape Is

The same compose stack as Local Development, on a server, running the published images and the development environment profile (`ENVIRONMENT=dev`). No AWS account is involved: SeaweedFS serves S3, DynamoDB Local serves the graph registries, both on Docker volumes, and the graph databases live on the server's disk. Users reach the API through a reverse proxy on the one port you open. The server-side operators can run on an open-weight model you host ([Self-Hosted AI Models](Self-Hosted-AI-Models.md)), and MCP clients on laptops bring their own model.

## The Box

**Images.** Clone the repository and copy `.env.example` to `.env`. The image lines at the top of the file already point at the published images on Docker Hub. Replace `latest` with the release you tested (`robofinsystems/robosystems:v1.21.12`, for example), so that an upgrade is a decision you make by changing the tag. `just start` pulls the images and starts the `robosystems` profile: the API on 8000, the Graph API on 8001, Dagster on 8002, PostgreSQL, Valkey, SeaweedFS, DynamoDB Local and OpenSearch. The web apps are the separate `apps` profile and the observability services their own. To upgrade, run `just upgrade-to v1.22.0` with the release you want, or `just upgrade-to latest` for the newest release, which is resolved to its tag first so the pin is always a version you can name: it checks that the image exists, lists the settings the release's `.env.example` names that your `.env` does not, moves the checkout to the tag so compose, scripts and image come from one ref, pins the tag in `.env`, pulls and recreates only the containers whose image changed, and waits for the API to report the new version. `--dry-run` shows the plan. Rolling back is the same command with the previous tag; migrations only go forward, so a release that migrated the database does not roll back.

**Email.** The API sends mail through Amazon SES unless `SMTP_HOST` is set in `.env`, in which case it sends over SMTP: `SMTP_HOST`, `SMTP_PORT` (465 is implicit TLS, any other port negotiates STARTTLS unless `SMTP_STARTTLS=false`), and `SMTP_USERNAME` and `SMTP_PASSWORD` if the relay wants them. Point them at a relay on your network, Microsoft 365 or Google Workspace, and verification, password reset and invitation mail is delivered. The sender is `EMAIL_FROM_ADDRESS`, which the relay has to be allowed to send as. The `.env.example` values point at Mailpit, the stack's own inbox on `8025`, which holds mail for reading but delivers nothing.

**Memory.** Each container has a memory cap that `compose.yaml` reads from `.env`, with defaults sized for a development machine with 24 GB. The "Container Memory Caps" block in `.env.example` lists the variables and a set for a 16 GB machine. Keep each cap under the memory Docker can use, and keep a heap set inside a container, such as the OpenSearch JVM, under that container's cap.

**Where the data lives.** Graph databases are plain files under `./data/lbug-dbs` in the checkout, beside the staging, artifact and vector directories under `./data`. PostgreSQL, Valkey, OpenSearch, SeaweedFS and DynamoDB Local keep their data in named Docker volumes (`pg_data`, `valkey_data`, `opensearch_data`, `seaweedfs_data`, `dynamodb_data`). A backup of the server is those directories and volumes together, taken while the stack is stopped or from a snapshot. Anything the platform would put in an S3 bucket, such as uploaded files and documents, exports and graph backups, goes to SeaweedFS on its volume, and survives a restart of the container and of the server. The `storage-init` container creates the buckets and registry tables on each start and leaves existing ones alone.

## A Name, a Certificate, One Port

Give the API a hostname on your network's DNS, an A record to the server's address, and keep that address fixed with a DHCP reservation or a static address. Get a certificate for the name from your own certificate authority, or from a public one through a DNS challenge if the name is in public DNS. Then run one reverse proxy on 443 that terminates TLS and forwards to the API on 8000. With Caddy:

```
api.example.internal {
	tls /etc/caddy/certs/api.pem /etc/caddy/certs/api-key.pem
	reverse_proxy localhost:8000 {
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
	reverse_proxy localhost:3000
}

ledger.example.internal {
	tls /etc/caddy/certs/ledger.pem /etc/caddy/certs/ledger-key.pem
	reverse_proxy localhost:3001
}

investor.example.internal {
	tls /etc/caddy/certs/investor.pem /etc/caddy/certs/investor-key.pem
	reverse_proxy localhost:3002
}

s3.example.internal {
	tls /etc/caddy/certs/s3.pem /etc/caddy/certs/s3-key.pem
	reverse_proxy localhost:8333
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

and run `just start all`. The apps read these for the links between them and to the API, and the API reads them for the links in the mail it sends, for redirects after sign-in, and for the origins it accepts requests from: in the development environment that is localhost plus these three app URLs, so nothing goes in `EXTRA_CORS_ORIGINS` for them. Sign-in happens on the RoboSystems app and carries over to the other two.

Two settings name an app address on their own. Passkeys, when `PASSKEYS_ENABLED=true`, are bound to the sign-in app's name: set `PASSKEY_RP_ID=app.example.internal` and `PASSKEY_ORIGIN=https://app.example.internal`, since the development default is `localhost`. A QuickBooks connection returns to `INTUIT_REDIRECT_URI`, which has to be `https://ledger.example.internal/connections/qb-callback` and registered with Intuit as such.

The addresses reach the apps at container start because the published images carry placeholders that their entrypoint fills in. An app image built from its source on this machine (an `*_APP_IMAGE` setting without a registry) bakes the addresses from its own repository's `.env` at build time instead, so a server should run the published images. The apps release on their own schedule: pin each `*_APP_IMAGE` line to the release you tested, as with the platform image. `just upgrade-to` moves the platform image only, and `just upgrade all` pulls the app images at the tags they are pinned to.

## Users and API Keys

Everything a user needs can be done over the API from a laptop with nothing but the name. Registration is `POST /v1/auth/register` with an email, a name and a password. `POST /v1/auth/login` returns a session token. `POST /v1/user/api-keys` with that token as a bearer creates an API key, which every later call sends as `X-API-Key`. The demo script runs exactly this sequence:

```bash
just demo-user --base-url https://api.example.internal
```

It registers a user, creates a key, and saves both to `.local/config.json`. [Authentication & API Keys](Authentication-and-API-Keys.md) describes the model behind them.

Registration is open by default: anyone who can reach the API can create an account. Once the first accounts exist, set `USER_REGISTRATION_ENABLED=false` in `.env` and run `just start` again. From then on a new account needs an organization invitation. For single sign-on against your identity provider, see [Enterprise SSO & SCIM](Enterprise-SSO-and-SCIM.md).

## Clients on Laptops

**MCP.** Every graph has an MCP endpoint at `https://api.example.internal/v1/graphs/{graph_id}/mcp`, authenticated by the API key. Claude Code connects to it directly:

```bash
claude mcp add --transport http robosystems \
  https://api.example.internal/v1/graphs/kg.../mcp \
  --header "X-API-Key: rfs..."
```

Clients that cannot send a header use the stdio bridge. The operator endpoint and the tool surface are described in [AI Operators & MCP](AI-Operators-and-MCP.md). Connectors that run on a vendor's servers, such as the claude.ai and ChatGPT connectors, cannot reach a name that only your network resolves. That is the point of this shape.

**Trusting your certificate.** If the certificate comes from your own CA, each laptop has to trust its root. The operating system's store covers browsers, `curl` on macOS, and Claude Code, which reads the OS store by default ([Claude Code network configuration](https://code.claude.com/docs/en/network-config)). Python does not. The Python SDK, the demo scripts and the integration template trust a bundled list of public roots instead, and fail with `CERTIFICATE_VERIFY_FAILED` until `SSL_CERT_FILE` names a file that holds the public roots plus yours. The variable replaces the bundled list rather than adding to it, so build a bundle:

```bash
cat "$(uv run python -c 'import certifi; print(certifi.where())')" /path/to/your-root.pem > ca-bundle.pem
```

Then set `SSL_CERT_FILE=/path/to/ca-bundle.pem` in `.env.local`, which the `just demo-*` recipes load. The `truststore` package, which makes Python use the OS store, is the alternative.

## The Model Lane

The operators, the server-side AI, call Claude through AWS Bedrock by default and can be pointed at any server that implements the OpenAI Chat Completions API with tool calling, on the same machine or on another one on your network. [Self-Hosted AI Models](Self-Hosted-AI-Models.md) has the settings, which models have been tested, and what to check before pointing one at real books.

## Limits Today

What this shape does not do yet, and what to do about it:

- **Every service port is published on all of the server's interfaces**: 8000, 8001, 8002, 5432, 6379, 8333, 1025, 8025 and 9200 in the `robosystems` profile, 3000 to 3002 with the apps, 9090 and 4000 with observability. Put a host firewall in front that allows 443 from your users and ssh from your administrator and nothing else, or bind the published ports to loopback in a compose override (`127.0.0.1:8000:8000`).
- **The object store's credentials are fixed.** The S3 identity the stack uses (`test`/`test`, in `bin/setup/seaweedfs-s3.json` and `.env`) is the same on every installation, and the public-data bucket allows anonymous reads, which the browser needs. Change the keys in both files before exposing the box beyond a trusted network, and keep port 8333 behind the host firewall like the others.
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
