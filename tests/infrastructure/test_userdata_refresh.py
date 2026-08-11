"""Behavioral tests for bin/userdata/common/refresh-graph-container.sh.

That script decides whether to bounce a customer's graph container, so its
fail-open branches, its refusal branches, and its no-op branch all need pinning
down. `instance_busy` is a coordination signal rather than a guard
(ref/data-plane.md §65), which means every one of the "proceed anyway" paths is
deliberate — and a well-meaning future edit that tightens one into a real guard
would look like a bug fix. These tests are what make that edit fail.

The script is driven as a real bash process rather than reimplemented: it is
copied into a sandbox with its two absolute paths (`/etc/environment` and
`/usr/local/bin/`) rewritten, and PATH is pointed at stub `aws`/`docker`
binaries. Nothing is added to the production script to make it testable.
"""

import os
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "bin" / "userdata" / "common" / "refresh-graph-container.sh"
LAMBDA = REPO_ROOT / "bin" / "lambda" / "graph_container_refresh.py"

# Exit codes the script uses to mean "this instance has not picked up the new
# deployment yet." The refresh Lambda classifies these as skips rather than
# failures, so they are a contract between two files — see
# TestExitCodeContract at the bottom.
EXIT_STALE_ENV = 3

REQUIRED_ENV = {
  "DATABASE_TYPE": "ladybug",
  "NODE_TYPE": "shared_replica",
  "CONTAINER_PORT": "8001",
  "ECR_URI": "123.dkr.ecr.us-east-1.amazonaws.com/robosystems",
  "ECR_IMAGE_TAG": "prod",
  "ENVIRONMENT": "prod",
  "INSTANCE_ID": "i-test",
  "PRIVATE_IP": "10.0.0.1",
  "AVAILABILITY_ZONE": "us-east-1a",
  "INSTANCE_TYPE": "r7g.large",
  "AWS_REGION": "us-east-1",
  "CLUSTER_TIER": "shared",
}

RUN_CONTAINER_STUB = """#!/bin/bash
if [ "${1:-}" = "--print-container-name" ]; then
  if [ "${NODE_TYPE}" = "shared_master" ] || [ "${NODE_TYPE}" = "shared_replica" ]; then
    echo "graph-api-shared"
  else
    echo "graph-api"
  fi
  exit 0
fi
echo "$ECR_IMAGE" > "$STUB_STARTED_MARKER"
exit ${STUB_RUN_EXIT:-0}
"""

AWS_STUB = """#!/bin/bash
case "$1 $2" in
  "dynamodb get-item")
    [ -n "$STUB_DDB_ROW" ] && printf '%b\\n' "$STUB_DDB_ROW"
    exit ${STUB_DDB_EXIT:-0} ;;
  "ecr get-login-password") echo "fake-token"; exit 0 ;;
esac
exit 0
"""

DOCKER_STUB = """#!/bin/bash
case "$1" in
  login) exit 0 ;;
  pull) echo "$2" >> "$STUB_PULL_LOG"; exit ${STUB_PULL_EXIT:-0} ;;
  image)
    case "$2" in
      inspect) echo "${STUB_PULLED_ID}"; exit 0 ;;
      prune) exit 0 ;;
    esac ;;
  inspect)
    case "$3" in
      '{{.Image}}') echo "${STUB_RUNNING_ID}"; exit 0 ;;
      '{{.State.Running}}') echo "${STUB_CONTAINER_RUNNING:-true}"; exit 0 ;;
    esac ;;
esac
exit 0
"""

# The target instances run AL2023 with GNU coreutils. macOS `date` has no -d,
# which would silently disable the stale-heartbeat branch under test, so the
# stub emulates GNU parsing on every platform for determinism.
DATE_STUB = """#!/bin/bash
if [ "$1" = "-u" ] && [ "$2" = "-d" ]; then
  python3 -c "
import datetime,sys
try:
  print(int(datetime.datetime.strptime(sys.argv[1],'%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=datetime.timezone.utc).timestamp()))
except Exception:
  sys.exit(1)
" "$3"
  exit $?
fi
exec /bin/date "$@"
"""


class Result:
  def __init__(self, proc, pull_log: Path, started: Path):
    self.exit_code = proc.returncode
    self.output = proc.stdout + proc.stderr
    self.pulls = pull_log.read_text().split() if pull_log.exists() else []
    self.restarted = started.exists()


@pytest.fixture
def refresh(tmp_path):
  """Return a callable that runs the real script against stubs."""
  bin_dir = tmp_path / "bin"
  bin_dir.mkdir()
  env_file = tmp_path / "environment"

  source = SCRIPT.read_text()
  sandboxed = source.replace("/etc/environment", str(env_file)).replace(
    "/usr/local/bin/", f"{bin_dir}/"
  )
  script = bin_dir / "refresh.sh"
  script.write_text(sandboxed)

  for name, body in [
    ("run-graph-container.sh", RUN_CONTAINER_STUB),
    ("aws", AWS_STUB),
    ("docker", DOCKER_STUB),
    ("date", DATE_STUB),
    ("sleep", "#!/bin/bash\nexit 0\n"),
  ]:
    path = bin_dir / name
    path.write_text(body)
    path.chmod(0o755)
  script.chmod(0o755)

  pull_log = tmp_path / "pulls.log"
  started = tmp_path / "started"

  def run(env_overrides=None, schema=2, missing=(), **stubs) -> Result:
    lines = [f"{k}={v}" for k, v in REQUIRED_ENV.items() if k not in missing]
    if env_overrides:
      lines = [line for line in lines if line.split("=")[0] not in env_overrides] + [
        f"{k}={v}" for k, v in env_overrides.items()
      ]
    if schema is not None:
      lines.append(f"GRAPH_ENV_SCHEMA={schema}")
    env_file.write_text("\n".join(lines) + "\n")

    pull_log.unlink(missing_ok=True)
    started.unlink(missing_ok=True)
    pull_log.touch()

    env = {
      **os.environ,
      "PATH": f"{bin_dir}:{os.environ['PATH']}",
      "STUB_PULL_LOG": str(pull_log),
      "STUB_STARTED_MARKER": str(started),
      "STUB_PULLED_ID": "sha256:aaa",
      "STUB_RUNNING_ID": "sha256:aaa",
      "STUB_CONTAINER_RUNNING": "true",
      "STUB_DDB_ROW": "None",
    }
    env.update({k: str(v) for k, v in stubs.items()})

    proc = subprocess.run(
      ["bash", str(script)], capture_output=True, text=True, env=env, timeout=120
    )
    return Result(proc, pull_log, started)

  return run


class TestEnvironmentContract:
  """An incomplete /etc/environment must be refused, never silently defaulted.

  A refresh sources only that file, so a variable the boot exported but did not
  persist falls back to run-graph-container.sh's defaults and the refreshed
  container differs from the running one — drift that surfaces as a missing
  volume mount rather than an error.
  """

  @pytest.mark.parametrize("schema", [None, 0, 1])
  def test_refuses_below_required_schema(self, refresh, schema):
    result = refresh(schema=schema)
    assert result.exit_code == EXIT_STALE_ENV
    assert "REFRESH_RESULT=skipped-stale-env" in result.output
    assert not result.restarted

  def test_refuses_when_a_required_variable_is_absent(self, refresh):
    result = refresh(missing=("CLUSTER_TIER",))
    assert result.exit_code == 1
    assert "CLUSTER_TIER" in result.output
    assert not result.restarted

  def test_proceeds_when_complete(self, refresh):
    assert refresh().exit_code == 0


class TestBusyCounterFailsOpen:
  """Every one of these paths is deliberate; see ref/data-plane.md §65.

  The counter reports, it does not decide. A broken or absent counter must never
  block a refresh, and tightening any of these into a real guard is the mistake
  that doc records someone making.
  """

  def test_missing_registry_row_proceeds(self, refresh):
    result = refresh(STUB_DDB_ROW="None")
    assert result.exit_code == 0
    assert "no registry entry" in result.output

  def test_row_without_counter_is_idle(self, refresh):
    result = refresh(STUB_DDB_ROW="None\\tNone\\tNone")
    assert result.exit_code == 0
    assert "is idle" in result.output

  def test_negative_counter_is_idle_and_warns(self, refresh):
    result = refresh(STUB_DDB_ROW="-1\\tNone\\tunknown")
    assert result.exit_code == 0
    assert "negative busy counter" in result.output

  def test_unreadable_registry_proceeds(self, refresh):
    result = refresh(STUB_DDB_ROW="", STUB_DDB_EXIT=255)
    assert result.exit_code == 0
    assert "no registry entry" in result.output

  def test_stale_heartbeat_is_treated_as_crashed(self, refresh):
    result = refresh(STUB_DDB_ROW="2\\t2000-01-01T00:00:00Z\\tmaterialize")
    assert result.exit_code == 0
    assert "stale busy counter" in result.output

  def test_busy_with_fresh_heartbeat_times_out(self, refresh):
    now = subprocess.run(
      ["date", "-u", "+%Y-%m-%dT%H:%M:%SZ"], capture_output=True, text=True
    ).stdout.strip()
    result = refresh(STUB_DDB_ROW=f"2\\t{now}\\tmaterialize", MAX_WAIT_MINUTES=2)
    assert result.exit_code == 1
    assert "timed out after 2" in result.output
    assert not result.restarted

  def test_force_ignore_busy_bypasses_the_wait(self, refresh):
    now = subprocess.run(
      ["date", "-u", "+%Y-%m-%dT%H:%M:%SZ"], capture_output=True, text=True
    ).stdout.strip()
    result = refresh(STUB_DDB_ROW=f"2\\t{now}\\tmaterialize", FORCE_IGNORE_BUSY="true")
    assert result.exit_code == 0
    assert "bypassing the busy-counter" in result.output


class TestDigestSkip:
  """An unchanged image must be a no-op pull, not a customer-visible restart."""

  def test_same_digest_and_running_is_a_noop(self, refresh):
    result = refresh(STUB_PULLED_ID="sha256:aaa", STUB_RUNNING_ID="sha256:aaa")
    assert result.exit_code == 0
    assert "REFRESH_RESULT=no-op" in result.output
    assert not result.restarted

  def test_new_digest_restarts(self, refresh):
    result = refresh(STUB_PULLED_ID="sha256:bbb", STUB_RUNNING_ID="sha256:aaa")
    assert result.exit_code == 0
    assert "REFRESH_RESULT=updated" in result.output
    assert result.restarted

  def test_stopped_container_restarts_even_on_a_digest_match(self, refresh):
    """A no-op on a dead container would leave it dead."""
    result = refresh(
      STUB_PULLED_ID="sha256:aaa",
      STUB_RUNNING_ID="sha256:aaa",
      STUB_CONTAINER_RUNNING="false",
    )
    assert result.exit_code == 0
    assert "REFRESH_RESULT=updated" in result.output
    assert result.restarted

  def test_absent_container_restarts(self, refresh):
    result = refresh(
      STUB_PULLED_ID="sha256:bbb",
      STUB_RUNNING_ID="",
      STUB_CONTAINER_RUNNING="false",
    )
    assert result.exit_code == 0
    assert result.restarted

  def test_force_restart_overrides_a_digest_match(self, refresh):
    """Secrets rotation refreshes containers to make them re-read rotated
    credentials, and rotation does not change the image. Without this override the
    digest-skip correctly finds nothing to pull and the rotation refresh becomes a
    no-op on every instance, leaving each process on its cached secrets until the
    TTL lapses.
    """
    result = refresh(
      STUB_PULLED_ID="sha256:aaa",
      STUB_RUNNING_ID="sha256:aaa",
      STUB_CONTAINER_RUNNING="true",
      FORCE_RESTART="true",
    )
    assert result.exit_code == 0
    assert "REFRESH_RESULT=updated" in result.output
    assert result.restarted
    assert "FORCE_RESTART=true" in result.output

  def test_digest_match_still_no_ops_without_force_restart(self, refresh):
    """The override must be opt-in, or every deploy bounces the whole fleet."""
    result = refresh(
      STUB_PULLED_ID="sha256:aaa",
      STUB_RUNNING_ID="sha256:aaa",
      STUB_CONTAINER_RUNNING="true",
    )
    assert "REFRESH_RESULT=no-op" in result.output
    assert not result.restarted


class TestFailurePropagation:
  """Non-zero exits are what let a fleet-wide --max-errors budget halt a rollout."""

  def test_pull_failure_exits_nonzero(self, refresh):
    result = refresh(
      STUB_PULLED_ID="sha256:bbb", STUB_RUNNING_ID="sha256:aaa", STUB_PULL_EXIT=1
    )
    assert result.exit_code == 1
    assert "docker pull failed" in result.output
    assert not result.restarted

  def test_unhealthy_container_exits_nonzero(self, refresh):
    result = refresh(
      STUB_PULLED_ID="sha256:bbb", STUB_RUNNING_ID="sha256:aaa", STUB_RUN_EXIT=1
    )
    assert result.exit_code == 1
    assert "did not come up healthy" in result.output


class TestImageTag:
  def test_pull_honors_the_recorded_tag_not_the_environment(self, refresh):
    """The shared replicas can be pinned to a build tag during a
    storage-format-breaking engine upgrade, precisely so a boot cannot pull a new
    engine before the new-format sec.lbug is published. A refresh that hardcoded
    ":$ENVIRONMENT" would defeat that pin during the upgrade it protects.
    """
    result = refresh(
      env_overrides={"ECR_IMAGE_TAG": "v1.7.9-build"},
      STUB_PULLED_ID="sha256:bbb",
      STUB_RUNNING_ID="sha256:aaa",
    )
    assert result.pulls == [
      "123.dkr.ecr.us-east-1.amazonaws.com/robosystems:v1.7.9-build"
    ]
    assert not any(pull.endswith(":prod") for pull in result.pulls)


class TestExitCodeContract:
  """The skip exit codes are a contract between the script and its aggregator.

  SSM records any non-zero exit as `Failed`, so something has to know that 3 and
  4 mean "this instance has not cycled onto the new scripts yet, and its
  container was deliberately left alone." If the script's codes drift from the
  aggregator's classification, an un-cycled fleet goes from a warning back to a
  failed deploy — the exact regression this pairing exists to prevent, and one
  nothing else would catch. The aggregator moved from the composite action to the
  Lambda when the per-instance matrix was retired; the contract did not change.
  """

  def test_lambda_classifies_the_scripts_stale_env_code_as_a_skip(self):
    script = SCRIPT.read_text()
    handler = LAMBDA.read_text()
    assert f"EXIT_STALE_ENV={EXIT_STALE_ENV}" in script
    assert f'{EXIT_STALE_ENV}: "skipped-stale-env"' in handler

  def test_lambda_guards_for_a_missing_script(self):
    """Instances install common scripts at boot, so an un-cycled instance has no
    script to run. That must be a distinct exit code rather than an inference
    from bash's 127, which a missing `docker` would also produce.
    """
    handler = LAMBDA.read_text()
    assert "-x {REFRESH_SCRIPT}" in handler or "-x /usr/local/bin" in handler
    assert "exit 4" in handler
    assert '4: "skipped-no-script"' in handler

  def test_skips_are_excluded_from_the_failure_count(self):
    """`failed` is what the workflow fails on, so skips must not reach it."""
    handler = LAMBDA.read_text()
    assert '"failed": len(failures)' in handler
    assert '"skipped": skipped' in handler
