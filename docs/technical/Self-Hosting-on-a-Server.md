# Self-Hosting on a Server

Run the stack on one machine on your own network and reach it from the others: a hostname, a certificate, one open port, users and API keys, and MCP clients on laptops. [Local Development](Local-Development.md) puts the stack on your own laptop and the [Bootstrap Guide](Bootstrap-Guide.md) deploys the platform into your own AWS account. This page is the shape between them. It describes what works today and ends with what still takes work by hand.

## What This Shape Is

The same compose stack as Local Development, on a server, running the published images and the development environment profile (`ENVIRONMENT=dev`). No AWS account is involved: SeaweedFS serves S3, DynamoDB Local serves the graph registries, both on Docker volumes, and the graph databases live on the server's disk. Users reach the API through a reverse proxy on the one port you open. The server-side operators can run on an open-weight model you host ([Self-Hosted AI Models](Self-Hosted-AI-Models.md)), and MCP clients on laptops bring their own model.

## The Box

**Images.** Clone the repository and copy `.env.example` to `.env`. The image lines at the top of the file already point at the published images on Docker Hub. Replace `latest` with the release you tested (`robofinsystems/robosystems:v1.21.12`, for example), so that an upgrade is a decision you make by changing the tag. `just start` pulls the images and starts the `robosystems` profile: the API on 8000, the Graph API on 8001, Dagster on 8002, PostgreSQL, Valkey, SeaweedFS, DynamoDB Local and OpenSearch. The web apps are the separate `apps` profile and the observability services their own. To upgrade, change the tag and run `just upgrade`, which pulls the new images and recreates only the containers whose image changed.

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

- **The web apps do not follow the name.** The three app services in `compose.yaml` carry their URLs as fixed `http://localhost` values in their `environment` blocks. To serve the apps on your name, edit those lines, or run the apps from their own repositories with their own `.env`, and add the apps' origins to `EXTRA_CORS_ORIGINS` in `.env`, since the API's allowed origins in the development environment are localhost plus that variable. Without the apps, users work through the API and MCP, which this page covers.
- **Every service port is published on all of the server's interfaces**: 8000, 8001, 8002, 5432, 6379, 8333 and 9200 in the `robosystems` profile, 3000 to 3002 with the apps, 9090 and 4000 with observability. Put a host firewall in front that allows 443 from your users and ssh from your administrator and nothing else, or bind the published ports to loopback in a compose override (`127.0.0.1:8000:8000`).
- **Email does not send.** The API sends mail through Amazon SES, so without AWS credentials invitations and password resets are not delivered. Create users over the API and hand the credentials over directly, or use single sign-on.
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
