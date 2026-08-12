"""Tests for the fleet-wide graph container refresh Lambda.

Two things here are load-bearing and would fail silently if they broke.

**Targeting.** The command is dispatched at a tag expression, not an instance
list, so a wrong expression does not error — it matches nothing and reports
success. SSM ANDs across target keys and ORs within one, which is why "all"
cannot be a single command and is instead one per node-type group.

**Skip classification.** A skip — "this instance has not cycled onto the new
scripts, and its container was deliberately left running" — is a transitional
state that must not fail a deploy. The refresh document normalizes skip exits to
0 (a non-zero exit consumes SSM's MaxErrors budget and terminates the rest of
the fleet's invocations), and the Lambda classifies skips off the
REFRESH_RESULT stdout marker, never off an exit code. If skips leak into the
failure count, an un-cycled fleet fails a deploy, which is the regression this
classification exists to prevent — and which shipped once, precisely because
the original classification read `ResponseCode` at the invocation level, a
field the real API only sets on the plugin.
"""

from unittest.mock import patch

import pytest

pytestmark = pytest.mark.unit


def _invocation(
  status: str,
  response_code: int | None = None,
  output: str = "",
  status_details: str = "",
):
  """Shape an invocation the way `list_command_invocations(Details=True)`
  actually returns it: the exit code lives on the plugin, never on the
  invocation. The first version of this fixture put ResponseCode at the
  invocation level — a shape the real API never produces — which is exactly how
  the classification bug these tests exist to catch survived them.
  """
  inv: dict = {"InstanceId": "i-abc", "Status": status}
  if status_details:
    inv["StatusDetails"] = status_details
  plugin: dict = {}
  if response_code is not None:
    plugin["ResponseCode"] = response_code
  if output:
    plugin["Output"] = output
  if plugin:
    inv["CommandPlugins"] = [plugin]
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


class TestParameterConstruction:
  """The wrapper shell lives in the document (graph-infra.yaml); the Lambda only
  supplies the per-dispatch values. tests/infrastructure covers the document's
  side of that contract."""

  def test_overrides_become_document_parameters(self, gcr):
    params = gcr._build_parameters(45, True, True, 3600)
    assert params["MaxWaitMinutes"] == ["45"]
    assert params["ForceIgnoreBusy"] == ["true"]
    assert params["ForceRestart"] == ["true"]
    assert params["ExecutionTimeout"] == ["3600"]

  def test_force_flags_default_to_false(self, gcr):
    params = gcr._build_parameters(30, False, False, 2700)
    assert params["ForceIgnoreBusy"] == ["false"]
    assert params["ForceRestart"] == ["false"]


class TestStart:
  def test_all_dispatches_one_command_per_group(self, gcr):
    with patch.object(gcr, "ssm") as ssm:
      ssm.send_command.return_value = {"Command": {"CommandId": "cmd-1"}}
      result = gcr.start({"environment": "prod", "node_types": "all"})

    assert ssm.send_command.call_count == len(gcr.ALL_GROUPS)
    assert [c["node_type"] for c in result["commands"]] == gcr.ALL_GROUPS

  def test_dispatches_the_stack_owned_document(self, gcr):
    """Never AWS-RunShellScript: the failure-paging rule filters on the document
    name, so the generic document would silently un-scope the page."""
    with patch.object(gcr, "ssm") as ssm:
      ssm.send_command.return_value = {"Command": {"CommandId": "cmd-1"}}
      gcr.start({"node_types": "writer"})

    kwargs = ssm.send_command.call_args.kwargs
    assert kwargs["DocumentName"] == gcr.REFRESH_DOCUMENT
    assert kwargs["DocumentName"] != "AWS-RunShellScript"

  def test_execution_timeout_exceeds_the_busy_wait(self, gcr):
    """Left implicit, a raised wait would silently truncate the command
    mid-refresh."""
    with patch.object(gcr, "ssm") as ssm:
      ssm.send_command.return_value = {"Command": {"CommandId": "cmd-1"}}
      result = gcr.start({"node_types": "writer", "max_wait_minutes": 50})

    assert result["execution_timeout_seconds"] > 50 * 60
    params = ssm.send_command.call_args.kwargs["Parameters"]
    assert params["ExecutionTimeout"] == [str(result["execution_timeout_seconds"])]

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

  def test_skips_arrive_as_success_and_classify_off_the_marker(self, gcr):
    """The document normalizes skip exits to 0, so a skip is a Success
    invocation whose only distinguishing feature is its stdout marker."""
    result = self._status(
      gcr, [_invocation("Success", 0, "REFRESH_RESULT=skipped-no-script")]
    )
    assert result["failed"] == 0
    assert result["skipped"] == 1
    assert result["refresh_results"]["skipped-no-script"] == 1

  def test_stale_env_marker_is_a_skip_not_a_failure(self, gcr):
    result = self._status(
      gcr, [_invocation("Success", 0, "REFRESH_RESULT=skipped-stale-env")]
    )
    assert result["failed"] == 0
    assert result["skipped"] == 1
    assert result["refresh_results"]["skipped-stale-env"] == 1

  def test_a_hand_run_skip_with_a_raw_exit_code_gets_the_same_grace(self, gcr):
    """Running refresh-graph-container.sh outside the document exits 3 with the
    marker on stdout; the marker, not the exit code, is the signal."""
    result = self._status(
      gcr,
      [_invocation("Failed", 3, "[refresh] ...\nREFRESH_RESULT=skipped-stale-env")],
    )
    assert result["failed"] == 0
    assert result["skipped"] == 1

  def test_a_real_failure_still_counts(self, gcr):
    """127 from a missing binary inside the script is a genuine failure and must
    not be downgraded just because a marker-less non-zero exit could look like
    the old skip shape."""
    result = self._status(gcr, [_invocation("Failed", 127)])
    assert result["failed"] == 1
    assert result["skipped"] == 0
    assert result["failures"][0]["response_code"] == "127"

  def test_terminated_before_running_is_a_failure_with_no_exit_code(self, gcr):
    """MaxErrors tripping elsewhere in the fleet terminates queued invocations;
    SSM marks them Failed/Terminated with a plugin ResponseCode of -1."""
    result = self._status(gcr, [_invocation("Failed", -1, status_details="Terminated")])
    assert result["failed"] == 1
    assert result["failures"][0]["response_code"] == ""
    assert result["failures"][0]["status_details"] == "Terminated"

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
        _invocation("Success", 0, "REFRESH_RESULT=skipped-no-script"),
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
