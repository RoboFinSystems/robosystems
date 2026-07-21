"""Tests for the graph volume monitor Lambda's expanding-status reset.

`perform_volume_expansion` stamps a volume 'expanding' during an online resize
but nothing cleared it until the next detach/reattach, which stranded a
still-awake shared master's wake health-gate. `mark_volume_attached` resets
'expanding' -> 'attached' after a filesystem grow, guarded so it never clobbers
a row a detach has already moved to 'available'.
"""

import boto3
import pytest

pytestmark = pytest.mark.unit


def _put_volume(status: str, volume_id: str = "vol-abc") -> None:
  table = boto3.resource("dynamodb", region_name="us-east-1").Table(
    "test-volume-registry"
  )
  table.put_item(
    Item={
      "volume_id": volume_id,
      "status": status,
      "databases": ["sec"],
      "instance_id": "i-123",
    }
  )


def _status(volume_id: str = "vol-abc") -> str:
  table = boto3.resource("dynamodb", region_name="us-east-1").Table(
    "test-volume-registry"
  )
  return table.get_item(Key={"volume_id": volume_id})["Item"]["status"]


class TestMarkVolumeAttached:
  def test_resets_expanding_to_attached(self, gvmon):
    _put_volume("expanding")
    gvmon.mark_volume_attached("vol-abc")
    assert _status() == "attached"

  def test_leaves_available_untouched(self, gvmon):
    # A detach already moved the row to 'available'; the conditional write must
    # not resurrect it to 'attached'.
    _put_volume("available")
    gvmon.mark_volume_attached("vol-abc")
    assert _status() == "available"

  def test_noop_when_already_attached(self, gvmon):
    _put_volume("attached")
    gvmon.mark_volume_attached("vol-abc")
    assert _status() == "attached"

  def test_missing_row_is_non_fatal(self, gvmon):
    # No row for the volume — the conditional write fails and is swallowed.
    gvmon.mark_volume_attached("vol-does-not-exist")
