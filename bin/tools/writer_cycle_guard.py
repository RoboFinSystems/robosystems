#!/usr/bin/env python3
"""Classify a CloudFormation change set for a graph writer stack.

Reads `aws cloudformation describe-change-set` JSON on stdin and prints one
JSON object: `{"cycles_writers": bool, "reasons": [str, ...]}`.

A writer holds its graph on an EBS volume, and replacing the instance moves
that volume, so a deploy must not replace writers as a side effect. A change
set replaces them when it changes the launch template (AMI, userdata,
instance type) or the ASG's placement or launch configuration, or when it
replaces either resource outright.

Standard library only: it runs on the GitHub runner's stock python3.
"""

from __future__ import annotations

import json
import sys

LAUNCH_TEMPLATE = "AWS::EC2::LaunchTemplate"
AUTO_SCALING_GROUP = "AWS::AutoScaling::AutoScalingGroup"

# ASG properties whose change rolls every instance under its UpdatePolicy.
ASG_CYCLING_PROPERTIES = {
  "LaunchTemplate",
  "LaunchConfigurationName",
  "MixedInstancesPolicy",
  "VPCZoneIdentifier",
  "AvailabilityZones",
}


def _describe(detail: dict) -> str:
  target = detail.get("Target", {})
  name = target.get("Name") or target.get("Attribute", "?")
  cause = detail.get("CausingEntity")
  return f"{name} (via {cause})" if cause else name


def classify(change_set: dict) -> dict:
  reasons: list[str] = []
  for change in change_set.get("Changes", []):
    rc = change.get("ResourceChange", {})
    rtype = rc.get("ResourceType")
    logical = rc.get("LogicalResourceId", "?")
    action = rc.get("Action")
    if rtype not in (LAUNCH_TEMPLATE, AUTO_SCALING_GROUP):
      continue
    if action in ("Remove", "Add"):
      reasons.append(f"{logical}: {action}")
      continue
    if rc.get("Replacement") in ("True", "Conditional"):
      reasons.append(f"{logical}: replacement ({rc['Replacement']})")
      continue
    details = rc.get("Details", [])
    if rtype == LAUNCH_TEMPLATE:
      what = ", ".join(_describe(d) for d in details) or "modified"
      reasons.append(f"{logical}: {what}")
      continue
    cycling = [
      d for d in details if d.get("Target", {}).get("Name") in ASG_CYCLING_PROPERTIES
    ]
    if cycling:
      reasons.append(f"{logical}: " + ", ".join(_describe(d) for d in cycling))
  return {"cycles_writers": bool(reasons), "reasons": reasons}


if __name__ == "__main__":
  print(json.dumps(classify(json.load(sys.stdin))))
