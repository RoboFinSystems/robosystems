"""MCP OAuth client administration commands (thin HTTP wrappers)."""

import click
from rich.console import Console
from rich.table import Table

console = Console()


@click.group()
def oauth():
  """Manage pre-registered MCP OAuth clients."""
  pass


@oauth.command("create-client")
@click.option("--name", "client_name", required=True, help="Display name on consent.")
@click.option(
  "--redirect-uri",
  "redirect_uris",
  multiple=True,
  required=True,
  help="Registered callback (repeatable). HTTPS, http loopback, or a native scheme.",
)
@click.option(
  "--confidential",
  is_flag=True,
  help="Issue a client_secret (for held-credential and gateway clients).",
)
@click.option("--client-uri", default=None, help="Client homepage (https).")
@click.option("--logo-uri", default=None, help="Client logo (https).")
@click.pass_obj
def create_client(
  client, client_name, redirect_uris, confidential, client_uri, logo_uri
):
  """Mint a trusted, pre-registered client.

  The client_secret (if requested) prints ONCE and is never recoverable.
  """
  data = {
    "client_name": client_name,
    "redirect_uris": list(redirect_uris),
    "confidential": confidential,
  }
  if client_uri:
    data["client_uri"] = client_uri
  if logo_uri:
    data["logo_uri"] = logo_uri

  result = client._make_request("POST", "/admin/v1/oauth/clients", data=data)

  console.print()
  console.print(
    f"[bold]Client:[/bold] {result['client_name']} ({result['oauth_client_id']})"
  )
  console.print(f"[bold]client_id:[/bold] [green]{result['client_id']}[/green]")
  console.print(f"[bold]Auth method:[/bold] {result['token_endpoint_auth_method']}")
  for uri in result["redirect_uris"]:
    console.print(f"[bold]Redirect:[/bold] {uri}")
  if result.get("client_secret"):
    console.print()
    console.print("[bold yellow]client_secret (shown once):[/bold yellow]")
    console.print(f"[green]{result['client_secret']}[/green]")
  console.print()


@oauth.command("list-clients")
@click.option("--source", default=None, help="dcr | cimd | preregistered")
@click.option("--include-inactive", is_flag=True)
@click.option("--limit", default=100, type=click.IntRange(1, 500))
@click.pass_obj
def list_clients(client, source, include_inactive, limit):
  """List registered OAuth clients."""
  params = {"limit": limit, "include_inactive": include_inactive}
  if source:
    params["source"] = source
  result = client._make_request("GET", "/admin/v1/oauth/clients", params=params)

  table = Table(title="OAuth clients")
  for column in ("id", "name", "source", "auth", "active", "trusted", "last used"):
    table.add_column(column)
  for row in result["clients"]:
    table.add_row(
      row["oauth_client_id"],
      row["client_name"],
      row["registration_source"],
      row["token_endpoint_auth_method"],
      "yes" if row["is_active"] else "no",
      "yes" if row["is_trusted"] else "no",
      row["last_used_at"] or "-",
    )
  console.print(table)


@oauth.command("deactivate-client")
@click.argument("oauth_client_id")
@click.pass_obj
def deactivate_client(client, oauth_client_id):
  """Deactivate a client (no new consents; its grants and tokens are revoked now)."""
  result = client._make_request(
    "POST", f"/admin/v1/oauth/clients/{oauth_client_id}/deactivate"
  )
  console.print(f"Deactivated {result['oauth_client_id']}")
