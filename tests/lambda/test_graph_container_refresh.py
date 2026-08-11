"""Tests for the fleet-wide graph container refresh Lambda.

Two things here are load-bearing and would fail silently if they broke.

**Targeting.** The command is dispatched at a tag expression, not an instance
list, so a wrong expression does not error — it matches nothing and reports
success. SSM ANDs across target keys and ORs within one, which is why "all"
cannot be a single command and is instead one per node-type group.

**Skip classification.** SSM records any non-zero exit as `Failed`. Exit 3 and 4
mean "this instance has not cycled onto the new scripts, and its container was
deliberately left running" — a transitional state. If those leak into the failure
count, an un-cycled fleet fails a deploy, which is the regression this
classification exists to prevent.
"""

from unittest.mock import patch

import pytest

pytestmark = pytest.mark.unit


def _invocation(status: str, response_code: int | None, output: str = ""):
  inv = {"InstanceId": "i-abc", "Status": status}
  if response_code is not None:
    inv["ResponseCode"] = response_code
  if output:
    inv["CommandPlugins"] = [{"Output": output}]
  return inv


class TestTargeting:
  def test_writer_group_targets_the_ladybug_role_tag(self, gcr):
    targets = gcr._targets_for("writer", "prod")
    assert {"Key": "tag:Environment", "Values": ["prod"]} in targets
    assert {"Key": "tag:LadybugRole", "Values": ["writer"]} in targets

  def test_shared_group_narrows_by_writer_tier(self, gcr):
    """Every writer tier carries LadybugRole=writer; the tier is on WriterTier."""
    targets = gcr._targets_for("shared", "prod")
    assert {"Key": "tag:WriterTier", "Values": ["shared"]} in targets

  def test_replicas_target_node_type_not_ladybug_role(self, gcr):
    """Replicas only got LadybugRole=replica later, and a tag reaches an instance
    at boot. Matching the older NodeType tag means this works during that
    rollout rather than silently selecting nothing.
    """
    targets = gcr._targets_for("shared-replicas", "prod")
    assert {"Key": "tag:NodeType", "Values": ["shared_replica"]} in targets
    assert not any(t["Key"] == "tag:LadybugRole" for t in targets)

  def test_unknown_group_raises(self, gcr):
    with pytest.raises(ValueError, match="Unknown node_type"):
      gcr._targets_for("nonsense", "prod")


class TestCommandConstruction:
  def test_guard_precedes_the_refresh_and_owns_exit_4(self, gcr):
    commands = gcr._build_commands(30, False, False)
    assert len(commands) == 2
    assert commands[0].startswith("[ -x /usr/local/bin/refresh-graph-container.sh ]")
    assert "exit 4" in commands[0]
    assert commands[1].endswith("/usr/local/bin/refresh-graph-container.sh")

  def test_overrides_are_passed_as_env_prefixes(self, gcr):
    commands = gcr._build_commands(45, True, True)
    assert "MAX_WAIT_MINUTES=45" in commands[1]
    assert "FORCE_IGNORE_BUSY=true" in commands[1]
    assert "FORCE_RESTART=true" in commands[1]

  def test_force_flags_default_to_false(self, gcr):
    commands = gcr._build_commands(30, False, False)
    assert "FORCE_IGNORE_BUSY=false" in commands[1]
    assert "FORCE_RESTART=false" in commands[1]


class TestStart:
  def test_all_dispatches_one_command_per_group(self, gcr):
    with patch.object(gcr, "ssm") as ssm:
      ssm.send_command.return_value = {"Command": {"CommandId": "cmd-1"}}
      result = gcr.start({"environment": "prod", "node_types": "all"})

    assert ssm.send_command.call_count == len(gcr.ALL_GROUPS)
    assert [c["node_type"] for c in result["commands"]] == gcr.ALL_GROUPS

  def test_execution_timeout_exceeds_the_busy_wait(self, gcr):
    """Left implicit, AWS-RunShellScript defaults to 3600s and a raised wait would
    silently truncate the command mid-refresh."""
    with patch.object(gcr, "ssm") as ssm:
      ssm.send_command.return_value = {"Command": {"CommandId": "cmd-1"}}
      result = gcr.start({"node_types": "writer", "max_wait_minutes": 50})

    assert result["execution_timeout_seconds"] > 50 * 60
    params = ssm.send_command.call_args.kwargs["Parameters"]
    assert params["executionTimeout"] == [str(result["execution_timeout_seconds"])]

  def test_rate_control_defaults_are_applied_by_the_lambda(self, gcr):
    """Not just by the caller — otherwise a hand-rolled invocation hits the fleet
    at SSM's default concurrency of 50."""
    with patch.object(gcr, "ssm") as ssm:
      ssm.send_command.return_value = {"Command": {"CommandId": "cmd-1"}}
      gcr.start({"node_types": "writer"})

    kwargs = ssm.send_command.call_args.kwargs
    assert kwargs["MaxConcurrency"] == gcr.DEFAULT_MAX_CONCURRENCY
    assert kwargs["MaxErrors"] == gcr.DEFAULT_MAX_ERRORS


class TestStatusSkipClassification:
  def _status(self, gcr, invocations):
    class _Paginator:
      def paginate(self, **_):
        return [{"CommandInvocations": invocations}]

    with patch.object(gcr, "ssm") as ssm:
      ssm.get_paginator.return_value = _Paginator()
      return gcr.status({"command_id": "cmd-1"})

  def test_stale_env_exit_is_a_skip_not_a_failure(self, gcr):
    result = self._status(gcr, [_invocation("Failed", 3)])
    assert result["failed"] == 0
    assert result["skipped"] == 1
    assert result["refresh_results"]["skipped-stale-env"] == 1

  def test_missing_script_exit_is_a_skip_not_a_failure(self, gcr):
    result = self._status(gcr, [_invocation("Failed", 4)])
    assert result["failed"] == 0
    assert result["skipped"] == 1
    assert result["refresh_results"]["skipped-no-script"] == 1

  def test_a_real_failure_still_counts(self, gcr):
    """127 from a missing binary inside the script is a genuine failure and must
    not be downgraded just because it is also non-zero."""
    result = self._status(gcr, [_invocation("Failed", 127)])
    assert result["failed"] == 1
    assert result["skipped"] == 0

  def test_refresh_result_is_read_from_stdout(self, gcr):
    result = self._status(
      gcr, [_invocation("Success", 0, "[refresh] ...\nREFRESH_RESULT=no-op\n")]
    )
    assert result["refresh_results"]["no-op"] == 1
    assert result["failed"] == 0

  def test_mixed_fleet_separates_the_three_outcomes(self, gcr):
    result = self._status(
      gcr,
      [
        _invocation("Success", 0, "REFRESH_RESULT=updated"),
        _invocation("Success", 0, "REFRESH_RESULT=no-op"),
        _invocation("Failed", 4),
        _invocation("Failed", 1),
      ],
    )
    assert result["refresh_results"] == {
      "updated": 1,
      "no-op": 1,
      "skipped-no-script": 1,
    }
    assert result["failed"] == 1
    assert result["skipped"] == 1

  def test_incomplete_while_invocations_are_in_progress(self, gcr):
    result = self._status(gcr, [_invocation("InProgress", None)])
    assert result["complete"] is False

  def test_complete_once_all_terminal(self, gcr):
    result = self._status(gcr, [_invocation("Success", 0)])
    assert result["complete"] is True


class TestHandler:
  def test_unknown_action_is_rejected(self, gcr):
    assert gcr.handler({"action": "nope"}, None)["statusCode"] == 400

  def test_status_requires_a_command_id(self, gcr):
    assert gcr.handler({"action": "status"}, None)["statusCode"] == 400
