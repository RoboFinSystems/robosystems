"""The shared release tools under bin/tools: the settings catalog diff, the
version wait, and the box upgrade's refusals. They are plain bash so a GitHub
workflow and a self-hosted server run the same code."""

from __future__ import annotations

import json
import subprocess
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

TOOLS = Path(__file__).resolve().parents[2] / "bin" / "tools"


def run(
  script: str, *args: str, cwd: Path | None = None
) -> subprocess.CompletedProcess[str]:
  return subprocess.run(
    ["bash", str(TOOLS / script), *args],
    cwd=cwd,
    capture_output=True,
    text=True,
    timeout=60,
  )


class TestSettingsDrift:
  def test_lists_set_and_optional_keys_the_env_lacks(self, tmp_path: Path):
    (tmp_path / ".env.example").write_text(
      "## Mail\nSMTP_HOST=mailpit\nSMTP_PORT=1025\n# SMTP_USERNAME=\n# SMTP_PASSWORD=\nAWS_REGION=us-east-1\n"
    )
    (tmp_path / ".env").write_text("AWS_REGION=us-east-1\n# SMTP_USERNAME=robot\n")

    result = run("settings-drift.sh", ".env", ".env.example", cwd=tmp_path)

    assert result.returncode == 0
    assert "Set in .env.example, missing from .env:" in result.stdout
    assert "   SMTP_HOST\n   SMTP_PORT\n" in result.stdout
    assert "Optional in .env.example, not mentioned in .env:" in result.stdout
    assert "   SMTP_PASSWORD\n" in result.stdout
    assert "SMTP_USERNAME" not in result.stdout.split("Optional")[1]
    assert "AWS_REGION" not in result.stdout

  def test_a_commented_key_counts_as_read(self, tmp_path: Path):
    (tmp_path / ".env.example").write_text("SMTP_HOST=mailpit\n")
    (tmp_path / ".env").write_text("# SMTP_HOST=\n")

    result = run("settings-drift.sh", ".env", ".env.example", cwd=tmp_path)

    assert result.returncode == 0
    assert result.stdout.startswith("✅")

  def test_strict_refuses_when_a_set_key_is_missing(self, tmp_path: Path):
    (tmp_path / ".env.example").write_text("SMTP_HOST=mailpit\n# SMTP_USERNAME=\n")
    (tmp_path / ".env").write_text("")

    result = run("settings-drift.sh", ".env", ".env.example", "--strict", cwd=tmp_path)

    assert result.returncode == 2
    assert "SMTP_HOST" in result.stdout

  def test_strict_passes_when_only_optional_keys_are_missing(self, tmp_path: Path):
    (tmp_path / ".env.example").write_text("# SMTP_USERNAME=\n")
    (tmp_path / ".env").write_text("")

    result = run("settings-drift.sh", ".env", ".env.example", "--strict", cwd=tmp_path)

    assert result.returncode == 0

  def test_the_repo_env_example_is_a_valid_catalog(self):
    repo = TOOLS.parents[1]
    result = run("settings-drift.sh", ".env.example", ".env.example", cwd=repo)
    assert result.returncode == 0
    assert result.stdout.startswith("✅")


class _Status(BaseHTTPRequestHandler):
  version = "1.22.0"

  def do_GET(self):
    body = json.dumps(
      {"status": "healthy", "details": {"version": self.version}}
    ).encode()
    self.send_response(200)
    self.send_header("Content-Type", "application/json")
    self.send_header("Content-Length", str(len(body)))
    self.end_headers()
    self.wfile.write(body)

  def log_message(self, *args, **kwargs):
    pass


@pytest.fixture
def status_server():
  server = HTTPServer(("127.0.0.1", 0), _Status)
  thread = threading.Thread(target=server.serve_forever, daemon=True)
  thread.start()
  try:
    yield f"http://127.0.0.1:{server.server_port}/v1/status"
  finally:
    server.shutdown()


class TestWaitForVersion:
  def test_returns_when_the_version_matches(self, status_server: str):
    result = run("wait-for-version.sh", status_server, "v1.22.0", "20")
    assert result.returncode == 0
    assert "reports 1.22.0" in result.stdout

  def test_times_out_naming_the_version_still_served(self, status_server: str):
    result = run("wait-for-version.sh", status_server, "1.23.0", "4")
    assert result.returncode == 1
    assert "last seen: 1.22.0" in result.stderr

  def test_usage(self):
    result = run("wait-for-version.sh")
    assert result.returncode == 2


class TestUpgradeToRefusals:
  def test_rejects_a_malformed_tag(self, tmp_path: Path):
    (tmp_path / ".env").write_text(
      "ROBOSYSTEMS_IMAGE=robofinsystems/robosystems:v1.21.12\n"
    )
    result = run("upgrade-to.sh", "1.22", cwd=tmp_path)
    assert result.returncode == 2
    assert "usage" in result.stderr

  def test_refuses_a_checkout_that_builds_from_source(self, tmp_path: Path):
    (tmp_path / ".env").write_text("ROBOSYSTEMS_IMAGE=robosystems-local\n")
    result = run("upgrade-to.sh", "v1.22.0", cwd=tmp_path)
    assert result.returncode == 1
    assert "built from source here" in result.stderr

  def test_refuses_without_an_env_file(self, tmp_path: Path):
    result = run("upgrade-to.sh", "v1.22.0", cwd=tmp_path)
    assert result.returncode == 1
    assert "no .env here" in result.stderr

  def test_refuses_uncommitted_changes_to_tracked_files(self, tmp_path: Path):
    git = ["git", "-c", "user.name=t", "-c", "user.email=t@example.com"]
    subprocess.run([*git, "init", "-q"], cwd=tmp_path, check=True)
    (tmp_path / "compose.yaml").write_text("services: {}\n")
    subprocess.run([*git, "add", "compose.yaml"], cwd=tmp_path, check=True)
    subprocess.run([*git, "commit", "-q", "-m", "init"], cwd=tmp_path, check=True)
    (tmp_path / "compose.yaml").write_text("services: {changed: {}}\n")
    (tmp_path / ".env").write_text(
      "ROBOSYSTEMS_IMAGE=robofinsystems/robosystems:v1.21.12\n"
    )

    result = run("upgrade-to.sh", "v1.22.0", cwd=tmp_path)

    assert result.returncode == 1
    assert "uncommitted changes to tracked files" in result.stderr

  def test_an_untracked_file_does_not_block_the_preflight(self, tmp_path: Path):
    """Operators keep notes and backups beside the checkout; only tracked
    changes would be lost by moving the checkout, so only they refuse."""
    git = ["git", "-c", "user.name=t", "-c", "user.email=t@example.com"]
    subprocess.run([*git, "init", "-q"], cwd=tmp_path, check=True)
    (tmp_path / "compose.yaml").write_text("services: {}\n")
    subprocess.run([*git, "add", "compose.yaml"], cwd=tmp_path, check=True)
    subprocess.run([*git, "commit", "-q", "-m", "init"], cwd=tmp_path, check=True)
    (tmp_path / "notes.txt").write_text("x")
    (tmp_path / ".env").write_text(
      "ROBOSYSTEMS_IMAGE=robofinsystems/robosystems:v1.21.12\n"
    )

    result = run("upgrade-to.sh", "v1.22.0", cwd=tmp_path)

    # The preflight passes the tree check and fails at the next step, the
    # fetch, because this repository has no remote.
    assert "uncommitted changes" not in result.stderr
    assert result.returncode != 0

  def test_latest_resolves_to_the_newest_release_tag_and_never_pins_latest(
    self, tmp_path: Path
  ):
    """The tags sort as versions, not strings, so v1.10.0 beats v1.9.0; the
    registry check then runs against that tag, and this stub registry has no
    daemon route, so the run stops there."""
    git = ["git", "-c", "user.name=t", "-c", "user.email=t@example.com"]
    origin = tmp_path / "origin.git"
    subprocess.run(["git", "init", "-q", "--bare", str(origin)], check=True)
    clone = tmp_path / "clone"
    subprocess.run([*git, "clone", "-q", str(origin), str(clone)], check=True)
    (clone / "compose.yaml").write_text("services: {}\n")
    (clone / ".env.example").write_text(
      "ROBOSYSTEMS_IMAGE=registry.example.internal/robosystems:latest\n"
    )
    subprocess.run([*git, "add", "."], cwd=clone, check=True)
    subprocess.run([*git, "commit", "-q", "-m", "init"], cwd=clone, check=True)
    for tag in ("v1.9.0", "v1.10.0", "v1.2.3"):
      subprocess.run([*git, "tag", tag], cwd=clone, check=True)
    subprocess.run(
      [*git, "push", "-q", "origin", "HEAD", "--tags"], cwd=clone, check=True
    )
    (clone / ".env").write_text(
      "ROBOSYSTEMS_IMAGE=registry.example.internal/robosystems:v1.9.0\n"
    )

    result = run("upgrade-to.sh", "latest", "--dry-run", cwd=clone)

    assert "latest → v1.10.0" in result.stdout
    assert "Preflight for v1.10.0" in result.stdout
    assert (
      "ROBOSYSTEMS_IMAGE=registry.example.internal/robosystems:v1.9.0"
      in (clone / ".env").read_text()
    )
    assert result.returncode != 0  # stopped at the registry check, before any change
