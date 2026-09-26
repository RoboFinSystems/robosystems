"""A writer-stack change set is classified as cycling the writers or not.

Fixtures follow the `describe-change-set` shape. A deploy that replaces
writers moves their data volumes, so it runs only when a person opts in.
"""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

SCRIPT = Path(__file__).resolve().parents[2] / "bin" / "tools" / "writer_cycle_guard.py"
_spec = importlib.util.spec_from_file_location("writer_cycle_guard", SCRIPT)
assert _spec and _spec.loader
guard = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(guard)


def _change(rtype, logical, action="Modify", replacement="False", details=()):
  return {
    "Type": "Resource",
    "ResourceChange": {
      "Action": action,
      "LogicalResourceId": logical,
      "ResourceType": rtype,
      "Replacement": replacement,
      "Scope": ["Properties"],
      "Details": list(details),
    },
  }


def _detail(name, cause=None, source="ParameterReference"):
  detail = {
    "Target": {"Attribute": "Properties", "Name": name, "RequiresRecreation": "Never"},
    "Evaluation": "Static",
    "ChangeSource": source,
  }
  if cause:
    detail["CausingEntity"] = cause
  return detail


LT = "AWS::EC2::LaunchTemplate"
ASG = "AWS::AutoScaling::AutoScalingGroup"


@pytest.mark.parametrize(
  "changes, expected_reason",
  [
    pytest.param(
      [
        _change(
          LT, "WriterLaunchTemplate", details=[_detail("LaunchTemplateData", "AmiId")]
        ),
        _change(
          ASG,
          "WriterAutoScalingGroup",
          details=[
            _detail("LaunchTemplate", "WriterLaunchTemplate.LatestVersionNumber")
          ],
        ),
      ],
      "LaunchTemplateData (via AmiId)",
      id="new AMI",
    ),
    pytest.param(
      [
        _change(
          LT,
          "WriterLaunchTemplate",
          details=[_detail("LaunchTemplateData", "ECRImageTag")],
        )
      ],
      "LaunchTemplateData (via ECRImageTag)",
      id="userdata parameter",
    ),
    pytest.param(
      [
        _change(
          ASG,
          "WriterAutoScalingGroup",
          details=[_detail("VPCZoneIdentifier", "SubnetIds")],
        )
      ],
      "VPCZoneIdentifier (via SubnetIds)",
      id="AZ move",
    ),
    pytest.param(
      [_change(ASG, "WriterAutoScalingGroup", replacement="True")],
      "replacement (True)",
      id="ASG replaced",
    ),
  ],
)
def test_a_change_that_replaces_writers_is_flagged(changes, expected_reason):
  result = guard.classify({"Changes": changes})
  assert result["cycles_writers"] is True
  assert any(expected_reason in r for r in result["reasons"])


@pytest.mark.parametrize(
  "changes",
  [
    pytest.param(
      [
        _change(
          ASG,
          "WriterAutoScalingGroup",
          details=[
            _detail("MaxSize", "MaxInstances"),
            _detail("MinSize", "MinInstances"),
          ],
        )
      ],
      id="ASG sizing",
    ),
    pytest.param(
      [
        _change(
          "AWS::CloudWatch::Alarm", "WriterCpuAlarm", details=[_detail("Threshold")]
        )
      ],
      id="an alarm",
    ),
    pytest.param(
      [_change("AWS::IAM::Role", "WriterRole", details=[_detail("Policies")])],
      id="IAM",
    ),
    pytest.param([], id="nothing"),
  ],
)
def test_a_change_that_keeps_writers_is_not_flagged(changes):
  assert guard.classify({"Changes": changes}) == {
    "cycles_writers": False,
    "reasons": [],
  }


def test_the_script_reads_stdin_and_writes_json():
  change_set = {
    "Changes": [
      _change(
        LT, "WriterLaunchTemplate", details=[_detail("LaunchTemplateData", "AmiId")]
      )
    ]
  }
  out = subprocess.run(
    [sys.executable, str(SCRIPT)],
    input=json.dumps(change_set),
    capture_output=True,
    text=True,
    check=True,
  ).stdout
  assert json.loads(out)["cycles_writers"] is True
