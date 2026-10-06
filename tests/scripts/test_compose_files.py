"""Tests for the compose file selector behind `just start`."""

import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "bin" / "tools" / "compose-files.sh"

BASE = "-f compose.yaml"
BACKEND = "-f compose.source.yaml"
APPS = "-f compose.source.apps.yaml"

PUBLISHED = {
  "ROBOSYSTEMS_IMAGE": "robofinsystems/robosystems:latest",
  "ROBOSYSTEMS_APP_IMAGE": "robofinsystems/robosystems-app:latest",
  "ROBOLEDGER_APP_IMAGE": "robofinsystems/roboledger-app:latest",
  "ROBOINVESTOR_APP_IMAGE": "robofinsystems/roboinvestor-app:latest",
}


def _env_text(images, commented=()):
  return "".join(
    f"{'# ' if name in commented else ''}{name}={image}\n"
    for name, image in images.items()
  )


def _files(tmp_path, env_text=None, shell=None):
  env_file = tmp_path / ".env"
  if env_text is not None:
    env_file.write_text(env_text)
  result = subprocess.run(
    [str(SCRIPT), str(env_file)],
    capture_output=True,
    text=True,
    check=True,
    env={"PATH": "/usr/bin:/bin", **(shell or {})},
  )
  return result.stdout.strip()


@pytest.mark.unit
class TestComposeFiles:
  def test_published_images_get_no_source_mounts(self, tmp_path):
    assert _files(tmp_path, _env_text(PUBLISHED)) == BASE

  def test_local_builds_get_their_source_mounts(self, tmp_path):
    env_text = _env_text(PUBLISHED, commented=PUBLISHED)
    assert _files(tmp_path, env_text) == f"{BASE} {BACKEND} {APPS}"

  def test_bare_tag_is_a_local_build(self, tmp_path):
    env_text = _env_text({**PUBLISHED, "ROBOSYSTEMS_IMAGE": "robosystems-local"})
    assert _files(tmp_path, env_text) == f"{BASE} {BACKEND}"

  def test_backend_built_beside_published_apps(self, tmp_path):
    env_text = _env_text(PUBLISHED, commented={"ROBOSYSTEMS_IMAGE"})
    assert _files(tmp_path, env_text) == f"{BASE} {BACKEND}"

  def test_app_mounts_need_every_app_built_locally(self, tmp_path):
    env_text = _env_text(PUBLISHED, commented={"ROBOLEDGER_APP_IMAGE"})
    assert _files(tmp_path, env_text) == BASE

  def test_shell_environment_wins_over_env_file(self, tmp_path):
    env_text = _env_text(PUBLISHED, commented=PUBLISHED)
    shell = {"ROBOSYSTEMS_IMAGE": PUBLISHED["ROBOSYSTEMS_IMAGE"]}
    assert _files(tmp_path, env_text, shell) == f"{BASE} {APPS}"

  def test_empty_shell_value_overrides_a_published_image(self, tmp_path):
    shell = {"ROBOSYSTEMS_IMAGE": ""}
    assert _files(tmp_path, _env_text(PUBLISHED), shell) == f"{BASE} {BACKEND}"

  def test_missing_env_file_means_local_builds(self, tmp_path):
    assert _files(tmp_path) == f"{BASE} {BACKEND} {APPS}"
