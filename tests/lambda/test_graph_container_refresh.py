"""Tests for the graph container refresh walk.

The walk refreshes one instance at a time and is driven by its caller: `start`
builds the queue, each `step` reads the in-flight instance and dispatches the
next. Three behaviours are load-bearing and would fail quietly if they broke.

**Targeting.** The queue comes from tag filters, so a wrong filter does not
error — it matches nothing and reports success. The `shared` group once filtered
on `WriterTier=shared` while the fleet is tagged `ladybug-shared`, and selected
nothing.

**Busy instances come back around.** A writer mid-materialization is passed
over and re-queued behind the rest, never failed and never forced. The walk that
this replaced let one busy shared master time out, fail, and cancel every other
writer in the fleet.

**Failure stops the walk.** A real failure dispatches nothing further, so a bad
image halts at the first instance. Skips and deferrals are classified off the
REFRESH_RESULT marker, never an exit code.
"""

import time
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import boto3
import pytest
from botocore.exceptions import ClientError

pytestmark = pytest.mark.unit

REGISTRY = "robosystems-graph-test-instance-registry"


def _launch(env: str = "test", state: str = "running", **tags: str) -> str:
  ec2 = boto3.client("ec2", region_name="us-east-1")
  image = ec2.describe_images(Owners=["amazon"])["Images"][0]["ImageId"]
  instance = ec2.run_instances(
    ImageId=image,
    MinCount=1,
    MaxCount=1,
    TagSpecifications=[
      {
        "ResourceType": "instance",
        "Tags": [{"Key": "Environment", "Value": env}]
        + [{"Key": k, "Value": v} for k, v in tags.items()],
      }
    ],
  )["Instances"][0]["InstanceId"]
  if state == "stopped":
    ec2.stop_instances(InstanceIds=[instance])
  return instance


def _registry(
  instance_id: str, count: int, age_seconds: int = 30, kind="materialization"
):
  last_at = (datetime.now(UTC) - timedelta(seconds=age_seconds)).isoformat()
  boto3.client("dynamodb", region_name="us-east-1").put_item(
    TableName=REGISTRY,
    Item={
      "instance_id": {"S": instance_id},
      "active_destructive_ops": {"N": str(count)},
      "last_destructive_op_at": {"S": last_at},
      "last_destructive_op_kind": {"S": kind},
    },
  )


def _invocation(status: str, output: str = "", code: int = 0, details: str = ""):
  """`get_command_invocation`'s shape: stdout and exit code on the invocation."""
  return {
    "Status": status,
    "StatusDetails": details or status,
    "ResponseCode": code,
    "StandardOutputContent": output,
  }


class Fleet:
  """A mocked SSM plus a scripted busy map, for driving the walk step by step."""

  def __init__(self, gcr, queue: list[str], busy: set[str] | None = None):
    self.gcr = gcr
    self.queue = queue
    self.busy = set(busy or ())
    self.ssm = MagicMock()
    self.ssm.list_commands.return_value = {"Commands": []}
    self.ssm.send_command.side_effect = lambda **kw: {
      "Command": {"CommandId": f"cmd-{kw['InstanceIds'][0]}"}
    }
    self.ssm.get_command_invocation.return_value = _invocation("InProgress")

  def _busy(self, instance_id):
    if instance_id in self.busy:
      return {"count": "1", "kind": "materialization", "last_at": "now"}
    return None

  def run(self, fn, *args):
    with (
      patch.object(self.gcr, "ssm", self.ssm),
      patch.object(self.gcr, "_resolve_queue", return_value=list(self.queue)),
      patch.object(self.gcr, "_busy", side_effect=self._busy),
    ):
      return fn(*args)

  def start(self, **event):
    return self.run(self.gcr.start, {"node_types": "writer", **event})

  def step(self, state, invocation=None):
    if invocation is not None:
      self.ssm.get_command_invocation.return_value = invocation
    return self.run(self.gcr.step, {"state": state})

  def dispatched(self) -> list[str]:
    return [c.kwargs["InstanceIds"][0] for c in self.ssm.send_command.call_args_list]


UPDATED = _invocation("Success", "REFRESH_RESULT=updated")


class TestTargeting:
  def test_writer_queue_covers_every_tier_with_shared_last(self, gcr):
    shared = _launch(LadybugRole="writer", WriterTier="ladybug-shared")
    standard = _launch(LadybugRole="writer", WriterTier="ladybug-standard")
    large = _launch(LadybugRole="writer", WriterTier="ladybug-large")

    queue = gcr._resolve_queue(["writer"], "test")

    assert set(queue) == {shared, standard, large}
    assert queue[-1] == shared

  def test_shared_group_matches_the_fleets_tier_tag(self, gcr):
    """The tier tag is `ladybug-shared`; filtering on `shared` selected nothing."""
    shared = _launch(LadybugRole="writer", WriterTier="ladybug-shared")
    _launch(LadybugRole="writer", WriterTier="ladybug-standard")

    assert gcr._resolve_queue(["shared"], "test") == [shared]

  def test_replicas_target_node_type_not_ladybug_role(self, gcr):
    """Replicas only got LadybugRole=replica later, and a tag reaches an instance
    at boot. Matching NodeType works during that rollout rather than silently
    selecting nothing."""
    replica = _launch(NodeType="shared_replica")

    assert gcr._resolve_queue(["shared-replicas"], "test") == [replica]

  def test_all_walks_writers_before_replicas(self, gcr):
    replica = _launch(NodeType="shared_replica")
    writer = _launch(LadybugRole="writer", WriterTier="ladybug-standard")

    assert gcr._resolve_queue(gcr.ALL_GROUPS, "test") == [writer, replica]

  def test_other_environments_and_stopped_instances_are_excluded(self, gcr):
    _launch(env="prod", LadybugRole="writer", WriterTier="ladybug-standard")
    _launch(state="stopped", LadybugRole="writer", WriterTier="ladybug-standard")

    assert gcr._resolve_queue(["writer"], "test") == []

  def test_unknown_group_raises(self, gcr):
    with pytest.raises(ValueError, match="Unknown node_type"):
      gcr._filters_for("nonsense", "test")


class TestBusyCounter:
  """Fails open exactly like its twins in refresh-graph-container.sh and
  wait-graph-writers-idle.sh — the instance repeats the check before acting."""

  def test_missing_row_is_idle(self, gcr):
    assert gcr._busy("i-unknown") is None

  @pytest.mark.parametrize("count", [0, -1])
  def test_non_positive_counter_is_idle(self, gcr, count):
    _registry("i-a", count)
    assert gcr._busy("i-a") is None

  def test_fresh_heartbeat_is_busy(self, gcr):
    _registry("i-a", 1, kind="materialization")
    assert gcr._busy("i-a")["kind"] == "materialization"

  def test_stale_heartbeat_is_a_crashed_writer(self, gcr):
    _registry("i-a", 1, age_seconds=gcr.STALE_WINDOW_SECONDS + 60)
    assert gcr._busy("i-a") is None

  def test_unreadable_registry_is_idle(self, gcr):
    with patch.object(gcr, "dynamodb") as ddb:
      ddb.get_item.side_effect = ClientError(
        {"Error": {"Code": "AccessDeniedException"}}, "GetItem"
      )
      assert gcr._busy("i-a") is None


class TestWalk:
  def test_start_dispatches_only_the_first_instance(self, gcr):
    fleet = Fleet(gcr, ["i-a", "i-b", "i-c"])
    state = fleet.start()

    assert fleet.dispatched() == ["i-a"]
    assert state["phase"] == "running"
    assert state["queue"] == ["i-b", "i-c"]

  def test_each_step_waits_for_the_instance_in_flight(self, gcr):
    fleet = Fleet(gcr, ["i-a", "i-b"])
    state = fleet.step(fleet.start())

    assert fleet.dispatched() == ["i-a"]
    assert state["current"]["instance_id"] == "i-a"

  def test_walks_the_fleet_in_order(self, gcr):
    fleet = Fleet(gcr, ["i-a", "i-b"])
    state = fleet.step(fleet.start(), UPDATED)
    state = fleet.step(state, UPDATED)

    assert fleet.dispatched() == ["i-a", "i-b"]
    assert state["phase"] == "complete"
    assert [o["result"] for o in state["outcomes"]] == ["updated", "updated"]

  def test_a_busy_instance_is_passed_over_and_comes_back_around(self, gcr):
    """The regression: a busy shared master used to fail and cancel the fleet."""
    fleet = Fleet(gcr, ["i-a", "i-b", "i-c"], busy={"i-a"})
    state = fleet.start()
    assert fleet.dispatched() == ["i-b"]
    assert state["queue"] == ["i-c", "i-a"]

    state = fleet.step(state, UPDATED)
    assert fleet.dispatched() == ["i-b", "i-c"]

    fleet.busy.clear()
    state = fleet.step(state, UPDATED)
    assert fleet.dispatched() == ["i-b", "i-c", "i-a"]

    state = fleet.step(state, UPDATED)
    assert state["phase"] == "complete"
    assert state["failure"] is None

  def test_waits_while_only_busy_instances_remain(self, gcr):
    fleet = Fleet(gcr, ["i-a"], busy={"i-a"})
    state = fleet.start()

    assert state["phase"] == "waiting"
    assert fleet.dispatched() == []
    assert "i-a" in state["busy"]

  def test_busy_past_the_deadline_is_deferred(self, gcr):
    fleet = Fleet(gcr, ["i-a", "i-b"], busy={"i-b"})
    state = fleet.step(fleet.start(max_wait_minutes=0), UPDATED)

    assert state["phase"] == "deferred"
    assert state["queue"] == ["i-b"]
    assert state["failure"] is None

  def test_a_failure_stops_the_walk(self, gcr):
    fleet = Fleet(gcr, ["i-a", "i-b", "i-c"])
    state = fleet.step(
      fleet.start(),
      _invocation("Failed", "[refresh] ERROR: docker pull failed", code=1),
    )
    state = fleet.step(state)

    assert state["phase"] == "failed"
    assert fleet.dispatched() == ["i-a"]
    assert state["queue"] == ["i-b", "i-c"]
    assert state["failure"]["instance_id"] == "i-a"
    assert state["failure"]["response_code"] == "1"
    assert "docker pull failed" in state["failure"]["output_tail"]

  def test_a_real_exit_code_stays_a_failure(self, gcr):
    """127 from a missing binary must not be downgraded to a skip."""
    fleet = Fleet(gcr, ["i-a"])
    state = fleet.step(fleet.start(), _invocation("Failed", "", code=127))

    assert state["phase"] == "failed"
    assert state["failure"]["response_code"] == "127"

  def test_undeliverable_is_a_failure(self, gcr):
    fleet = Fleet(gcr, ["i-a", "i-b"])
    state = fleet.step(
      fleet.start(), _invocation("TimedOut", code=-1, details="DeliveryTimedOut")
    )

    assert state["phase"] == "failed"
    assert state["failure"]["response_code"] == ""

  def test_an_instance_that_finds_itself_busy_is_requeued(self, gcr):
    """A materialization can start between the Lambda's read and the script's;
    the script then reports a deferral, not a failure."""
    fleet = Fleet(gcr, ["i-a", "i-b"])
    state = fleet.step(
      fleet.start(), _invocation("Success", "REFRESH_RESULT=deferred-busy")
    )

    assert state["phase"] == "running"
    assert fleet.dispatched() == ["i-a", "i-b"]
    assert state["queue"] == ["i-a"]
    assert state["failure"] is None

  @pytest.mark.parametrize("marker", ["skipped-no-script", "skipped-stale-env"])
  def test_skips_are_outcomes_not_failures(self, gcr, marker):
    fleet = Fleet(gcr, ["i-a"])
    state = fleet.step(
      fleet.start(), _invocation("Success", f"REFRESH_RESULT={marker}")
    )

    assert state["phase"] == "complete"
    assert state["outcomes"] == [{"instance_id": "i-a", "result": marker}]

  def test_a_hand_run_skip_with_a_raw_exit_code_gets_the_same_grace(self, gcr):
    """The raw script exits 3 with the marker; the marker, not the code, decides."""
    fleet = Fleet(gcr, ["i-a"])
    state = fleet.step(
      fleet.start(), _invocation("Failed", "REFRESH_RESULT=skipped-stale-env", code=3)
    )

    assert state["phase"] == "complete"
    assert state["failure"] is None

  def test_invocation_not_yet_registered_is_still_running(self, gcr):
    fleet = Fleet(gcr, ["i-a"])
    state = fleet.start()
    fleet.ssm.get_command_invocation.side_effect = ClientError(
      {"Error": {"Code": "InvocationDoesNotExist"}}, "GetCommandInvocation"
    )
    state = fleet.step(state)

    assert state["phase"] == "running"

  def test_a_refresh_already_in_flight_is_adopted_not_resent(self, gcr):
    """A retried step whose first attempt dispatched must not double-refresh."""
    fleet = Fleet(gcr, ["i-a"])
    fleet.ssm.list_commands.return_value = {
      "Commands": [{"CommandId": "cmd-earlier", "Status": "InProgress"}]
    }
    state = fleet.start()

    assert fleet.dispatched() == []
    assert state["current"] == {"instance_id": "i-a", "command_id": "cmd-earlier"}

  def test_force_ignore_busy_skips_the_counter(self, gcr):
    fleet = Fleet(gcr, ["i-a"], busy={"i-a"})
    fleet.start(force_ignore_busy=True)

    assert fleet.dispatched() == ["i-a"]

  def test_no_instances_is_complete(self, gcr):
    """Staging routinely runs with no graph fleet at all."""
    fleet = Fleet(gcr, [])
    state = fleet.start()

    assert state["phase"] == "complete"
    assert fleet.dispatched() == []

  def test_a_terminal_walk_does_not_move(self, gcr):
    fleet = Fleet(gcr, ["i-a", "i-b"])
    state = fleet.step(fleet.start(), _invocation("Failed", code=1))
    fleet.step(state)
    fleet.step(state)

    assert fleet.dispatched() == ["i-a"]

  def test_log_is_per_step(self, gcr):
    fleet = Fleet(gcr, ["i-a"])
    state = fleet.start()
    assert state["log"]
    assert fleet.step(state)["log"] == []


class TestDispatch:
  def test_dispatches_the_stack_owned_document(self, gcr):
    """Never AWS-RunShellScript: the failure-paging rule filters on the document
    name, so the generic document would silently un-scope the page."""
    fleet = Fleet(gcr, ["i-a"])
    fleet.start()

    kwargs = fleet.ssm.send_command.call_args.kwargs
    assert kwargs["DocumentName"] == gcr.REFRESH_DOCUMENT
    assert kwargs["InstanceIds"] == ["i-a"]

  def test_execution_timeout_exceeds_the_instance_side_wait(self, gcr):
    fleet = Fleet(gcr, ["i-a"])
    fleet.start(force_restart=True)

    params = fleet.ssm.send_command.call_args.kwargs["Parameters"]
    assert int(params["ExecutionTimeout"][0]) > int(params["MaxWaitMinutes"][0]) * 60
    assert params["ForceRestart"] == ["true"]
    assert params["ForceIgnoreBusy"] == ["false"]

  def test_deadline_is_max_wait_from_start(self, gcr):
    fleet = Fleet(gcr, [])
    state = fleet.start(max_wait_minutes=10)

    assert abs(state["deadline"] - (time.time() + 600)) < 5


class TestHandler:
  def test_unknown_action_is_rejected(self, gcr):
    assert gcr.handler({"action": "nope"}, None)["statusCode"] == 400

  def test_step_requires_state(self, gcr):
    assert gcr.handler({"action": "step"}, None)["statusCode"] == 400

  def test_operational_failures_propagate(self, gcr):
    """An unhandled exception is what increments the AWS/Lambda Errors metric
    the stack's alarm pages on."""
    fleet = Fleet(gcr, ["i-a"])
    fleet.ssm.send_command.side_effect = RuntimeError("dispatch broke")
    with pytest.raises(RuntimeError, match="dispatch broke"):
      fleet.run(gcr.handler, {"action": "start", "node_types": "writer"}, None)
