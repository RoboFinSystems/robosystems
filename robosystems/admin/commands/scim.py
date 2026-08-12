"""SCIM provisioning administration commands (thin HTTP wrappers)."""

import click
from rich.console import Console

console = Console()


@click.group()
def scim():
  """Manage SCIM provisioning for dedicated tenants."""
  pass


@scim.command("bootstrap")
@click.option("--org-id", help="Attach the token to this existing org.")
@click.option(
  "--org-name", help="Create a new ENTERPRISE org with this name (omit --org-id)."
)
@click.option("--token-name", default="scim-provisioning", help="Label for the token.")
@click.pass_obj
def bootstrap(client, org_id, org_name, token_name):
  """Create-or-reuse the enterprise org and mint a SCIM token.

  The raw token prints ONCE and is never recoverable — paste it into the
  customer's IdP now.
  """
  if not org_id and not org_name:
    raise click.UsageError("Provide either --org-id or --org-name.")

  data = {"token_name": token_name}
  if org_id:
    data["org_id"] = org_id
  if org_name:
    data["org_name"] = org_name

  result = client._make_request("POST", "/admin/v1/scim/bootstrap", data=data)

  console.print()
  console.print(f"[bold]Org:[/bold] {result['org_name']} ({result['org_id']})")
  console.print(f"[bold]Token ID:[/bold] {result['scim_token_id']}")
  console.print()
  console.print("[bold yellow]SCIM bearer token (shown once):[/bold yellow]")
  console.print(f"[green]{result['token']}[/green]")
  console.print()
  console.print("[dim]Paste this into the IdP's SCIM connector now.[/dim]")


@scim.command("revoke-token")
@click.argument("token_id")
@click.pass_obj
def revoke_token(client, token_id):
  """Revoke a SCIM token by id."""
  result = client._make_request("POST", f"/admin/v1/scim/tokens/{token_id}/revoke")
  if result.get("revoked"):
    console.print(f"[green]Revoked SCIM token {token_id}.[/green]")
  else:
    console.print(f"[yellow]SCIM token {token_id} was not revoked.[/yellow]")
