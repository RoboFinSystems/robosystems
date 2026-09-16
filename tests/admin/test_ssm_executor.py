"""Guards on how `SSMExecutor.execute` distinguishes "still running" from "failed".

The distinction is load-bearing rather than cosmetic: this executor runs
`search delete` against the shared OpenSearch index, and a command reported as
failed invites a re-run. `aws ssm wait command-executed` gives up at ~100s
(botocore waiter, delay 5 / maxAttempts 20, neither configurable), so a long
delete reliably outlives the wait while succeeding.
"""

import json
import subprocess
from unittest.mock import patch

import pytest

from robosystems.admin.ssm_executor import SSMCommandStillRunning, SSMExecutor


def _invocation(status: str, response_code: int | None, stdout: str = "") -> str:
  """One `aws ssm get-command-invocation` payload.

  An in-flight invocation genuinely has no `ResponseCode` — that absence is what
  used to be read as exit code -1.
  """
  payload: dict[str, object] = {
    "Status": status,
    "StandardOutputContent": stdout,
    "StandardErrorContent": "",
  }
  if response_code is not None:
    payload["ResponseCode"] = response_code
  return json.dumps(payload)


def _completed(stdout: str) -> subprocess.CompletedProcess:
  return subprocess.CompletedProcess(args=[], returncode=0, stdout=stdout, stderr="")


@pytest.fixture
def executor():
  ex = SSMExecutor(environment="prod", timeout=20)
  with (
    patch.object(SSMExecutor, "_get_bastion_instance", return_value="i-test"),
    patch.object(SSMExecutor, "_ensure_instance_running", return_value=None),
  ):
    yield ex


def _run_execute(executor, invocation_payloads, waiter_rc=1):
  """Drive execute() with a scripted sequence of invocation payloads."""
  sent = _completed("cmd-1234\n")
  waiter = subprocess.CompletedProcess(args=[], returncode=waiter_rc, stdout=b"")
  calls = [sent, waiter] + [_completed(p) for p in invocation_payloads]

  with (
    patch("subprocess.run", side_effect=calls) as mock_run,
    patch("time.sleep"),
  ):
    return executor.execute("echo hi", stream_output=False), mock_run


@pytest.mark.unit
class TestStillRunningIsNotFailure:
  def test_in_progress_raises_still_running_not_generic_failure(self, executor):
    """The regression this fixes: InProgress used to raise 'exit code -1'."""
    payloads = [_invocation("InProgress", None)] * 40

    with (
      patch(
        "subprocess.run",
        side_effect=[_completed("cmd-1234\n"), subprocess.CompletedProcess([], 1, b"")]
        + [_completed(p) for p in payloads],
      ),
      patch("time.sleep"),
      patch("time.time", side_effect=[0] + [100] * 50),
    ):
      with pytest.raises(SSMCommandStillRunning) as exc:
        executor.execute("long-delete", stream_output=False)

    assert exc.value.status == "InProgress"
    assert exc.value.command_id == "cmd-1234"
    assert exc.value.instance_id == "i-test"

  def test_still_running_error_names_the_command_and_warns_off_rerunning(
    self, executor
  ):
    payloads = [_invocation("Pending", None)] * 5

    with (
      patch(
        "subprocess.run",
        side_effect=[_completed("cmd-9999\n"), subprocess.CompletedProcess([], 1, b"")]
        + [_completed(p) for p in payloads],
      ),
      patch("time.sleep"),
      patch("time.time", side_effect=[0] + [999] * 10),
    ):
      with pytest.raises(SSMCommandStillRunning) as exc:
        executor.execute("long-delete", stream_output=False)

    message = str(exc.value)
    assert "cmd-9999" in message
    assert "NOT failed" in message
    assert "re-running" in message

  def test_still_running_is_a_runtime_error_subclass(self):
    """Existing `except RuntimeError` callers keep catching it."""
    assert issubclass(SSMCommandStillRunning, RuntimeError)


@pytest.mark.unit
class TestPollingPastTheWaiterCeiling:
  def test_command_finishing_after_the_waiter_gave_up_still_succeeds(self, executor):
    """The waiter returning non-zero says nothing about the command."""
    payloads = [
      _invocation("InProgress", None),
      _invocation("InProgress", None),
      _invocation("Success", 0, stdout="merged"),
    ]
    (stdout, _stderr, code), _ = _run_execute(executor, payloads, waiter_rc=1)

    assert code == 0
    assert stdout == "merged"

  def test_terminal_failure_still_raises_plain_runtime_error(self, executor):
    payloads = [_invocation("Failed", 1)]

    with pytest.raises(RuntimeError) as exc:
      _run_execute(executor, payloads, waiter_rc=0)

    assert not isinstance(exc.value, SSMCommandStillRunning)
    assert "exit code 1" in str(exc.value)

  def test_success_on_the_first_poll_does_not_sleep(self, executor):
    payloads = [_invocation("Success", 0, stdout="done")]
    sent = _completed("cmd-1\n")
    waiter = subprocess.CompletedProcess(args=[], returncode=0, stdout=b"")

    with (
      patch("subprocess.run", side_effect=[sent, waiter, _completed(payloads[0])]),
      patch("time.sleep") as mock_sleep,
    ):
      _stdout, _stderr, code = executor.execute("quick", stream_output=False)

    assert code == 0
    mock_sleep.assert_not_called()
