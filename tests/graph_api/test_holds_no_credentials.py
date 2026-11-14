"""The Graph API server holds no application credentials.

It is the process that runs tenant statements and parses tenant files, so it
is started scoped to its own key and the Valkey token, opens no connection to
the platform database, and reads no credential from its own configuration.
"""

import re
from pathlib import Path

import pytest

SERVER = Path(__file__).parents[2] / "robosystems" / "graph_api"
ENTRYPOINT = Path(__file__).parents[2] / "bin" / "entrypoint.sh"

# The client package runs in the API, worker and Dagster, not on the instance.
SERVER_FILES = sorted(
  path
  for path in SERVER.rglob("*.py")
  if "client" not in path.relative_to(SERVER).parts
)

FORBIDDEN = {
  "a platform or extensions database session": re.compile(
    r"robosystems\.database|robosystems\.db\b|SessionFactory|extensions_session"
  ),
  "a credential read from instance configuration": re.compile(
    r"get_r2_config|env\.(JWT_SECRET_KEY|CONNECTION_CREDENTIALS_KEY|DATABASE_URL"
    r"|EXTENSIONS_DATABASE_URL|STRIPE_\w+|PLAID_\w+|INTUIT_\w+|MERCURY_\w+|R2_\w+)"
  ),
}


@pytest.mark.parametrize("what", sorted(FORBIDDEN))
def test_server_code_has_no(what):
  offenders = [
    f"{path.relative_to(SERVER.parent.parent)}:{number}"
    for path in SERVER_FILES
    for number, line in enumerate(path.read_text().splitlines(), 1)
    if FORBIDDEN[what].search(line)
  ]
  assert not offenders, f"Graph API server code uses {what}: {offenders}"


def test_guard_sees_the_server_files():
  names = {path.name for path in SERVER_FILES}
  assert {"app.py", "main.py", "backup_service.py", "manager.py"} <= names


def test_both_graph_profiles_start_scoped():
  script = ENTRYPOINT.read_text()
  for profile in ('"ladybug-writer")', '"ladybug-shared-writer")'):
    branch = script[script.index(profile) :].split(";;", 1)[0]
    assert 'export SECRETS_SCOPE="graph-api,valkey"' in branch, profile
    assert branch.index("SECRETS_SCOPE") < branch.index("robosystems.graph_api")
