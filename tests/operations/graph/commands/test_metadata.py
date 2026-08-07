"""Tests for update_graph_metadata_cmd.

Runs against the real test database rather than mocks: the command edits a
JSONB column, and the failure mode worth guarding (mutating the dict in
place so SQLAlchemy never marks it dirty and the write silently vanishes)
only reproduces against a real session.
"""

from __future__ import annotations

import pytest
from fastapi import HTTPException

from robosystems.operations.graph.commands.metadata import update_graph_metadata_cmd


def _update(graph_id, user_id, db, **fields):
  """Call the command with every field defaulting to 'unchanged'."""
  return update_graph_metadata_cmd(
    graph_id,
    graph_name=fields.get("graph_name"),
    description=fields.get("description"),
    tags=fields.get("tags"),
    user_id=user_id,
    db=db,
  )


class TestUpdateGraphMetadata:
  def test_updates_all_three_fields(self, test_db, test_user, test_user_graph):
    graph_id = test_user_graph.graph_id

    result = _update(
      graph_id,
      test_user.id,
      test_db,
      graph_name="Renamed Graph",
      description="Now with a description",
      tags=["alpha", "beta"],
    )

    assert result.graph_name == "Renamed Graph"
    assert result.description == "Now with a description"
    assert result.tags == ["alpha", "beta"]
    assert set(result.updated_fields) == {"graph_name", "description", "tags"}

  def test_persists_across_sessions(self, test_db, test_user, test_user_graph):
    """The JSONB write must survive a re-read, not just the in-memory object."""
    from robosystems.models.core.graph import Graph

    graph_id = test_user_graph.graph_id
    _update(
      graph_id,
      test_user.id,
      test_db,
      graph_name="Persisted Name",
      description="Persisted description",
      tags=["persisted"],
    )

    test_db.expire_all()
    reloaded = Graph.get_by_id(graph_id, test_db)
    assert reloaded is not None
    assert reloaded.graph_name == "Persisted Name"
    assert reloaded.graph_metadata["description"] == "Persisted description"
    assert reloaded.graph_metadata["tags"] == ["persisted"]

  def test_partial_update_leaves_other_fields_alone(
    self, test_db, test_user, test_user_graph
  ):
    graph_id = test_user_graph.graph_id
    _update(
      graph_id,
      test_user.id,
      test_db,
      graph_name="Original",
      description="Original description",
      tags=["keep-me"],
    )

    result = _update(graph_id, test_user.id, test_db, graph_name="Only The Name")

    assert result.graph_name == "Only The Name"
    assert result.description == "Original description"
    assert result.tags == ["keep-me"]
    assert result.updated_fields == ["graph_name"]

  def test_preserves_unrelated_metadata_keys(self, test_db, test_user, test_user_graph):
    """`graph_metadata` also carries materialization state — don't clobber it."""
    from robosystems.models.core.graph import Graph

    graph_id = test_user_graph.graph_id
    _update(graph_id, test_user.id, test_db, description="A description")

    test_db.expire_all()
    reloaded = Graph.get_by_id(graph_id, test_db)
    assert reloaded is not None
    # Written by the `sample_graph` fixture at creation.
    assert reloaded.graph_metadata["purpose"] == "testing"
    assert reloaded.graph_metadata["fixture"] is True

  def test_empty_string_clears_description(self, test_db, test_user, test_user_graph):
    graph_id = test_user_graph.graph_id
    _update(graph_id, test_user.id, test_db, description="To be cleared")

    result = _update(graph_id, test_user.id, test_db, description="")

    assert result.description == ""
    assert result.updated_fields == ["description"]

  def test_empty_list_clears_tags(self, test_db, test_user, test_user_graph):
    graph_id = test_user_graph.graph_id
    _update(graph_id, test_user.id, test_db, tags=["doomed"])

    result = _update(graph_id, test_user.id, test_db, tags=[])

    assert result.tags == []
    assert result.updated_fields == ["tags"]

  def test_tags_replace_rather_than_merge(self, test_db, test_user, test_user_graph):
    graph_id = test_user_graph.graph_id
    _update(graph_id, test_user.id, test_db, tags=["old-one", "old-two"])

    result = _update(graph_id, test_user.id, test_db, tags=["new-only"])

    assert result.tags == ["new-only"]

  def test_resubmitting_identical_values_is_a_noop(
    self, test_db, test_user, test_user_graph
  ):
    graph_id = test_user_graph.graph_id
    _update(
      graph_id,
      test_user.id,
      test_db,
      graph_name="Stable",
      description="Stable description",
      tags=["stable"],
    )

    result = _update(
      graph_id,
      test_user.id,
      test_db,
      graph_name="Stable",
      description="Stable description",
      tags=["stable"],
    )

    assert result.updated_fields == []
    assert result.graph_name == "Stable"
    assert result.description == "Stable description"
    assert result.tags == ["stable"]

  def test_reports_only_the_fields_that_changed(
    self, test_db, test_user, test_user_graph
  ):
    graph_id = test_user_graph.graph_id
    _update(graph_id, test_user.id, test_db, graph_name="Same", description="Different")

    result = _update(
      graph_id, test_user.id, test_db, graph_name="Same", description="Changed"
    )

    assert result.updated_fields == ["description"]


class TestUpdateGraphMetadataGuards:
  @pytest.mark.parametrize("graph_id", ["sec", "sec_historical"])
  def test_rejects_shared_repository(self, test_db, test_user, graph_id: str):
    with pytest.raises(HTTPException) as exc:
      _update(graph_id, test_user.id, test_db, graph_name="Hijacked")

    assert exc.value.status_code == 403
    assert "platform-managed" in exc.value.detail

  def test_unknown_graph_is_404(self, test_db, test_user):
    with pytest.raises(HTTPException) as exc:
      _update("kgdoesnotexist0000000", test_user.id, test_db, graph_name="Ghost")

    assert exc.value.status_code == 404

  def test_non_member_is_403(self, test_db, test_user_graph):
    with pytest.raises(HTTPException) as exc:
      _update(
        test_user_graph.graph_id, "test-user-outsider", test_db, graph_name="Nope"
      )

    assert exc.value.status_code == 403
    assert "Admin access" in exc.value.detail

  def test_member_role_is_not_enough(self, test_db, test_user_graph):
    """Editing graph configuration is admin-only; `member` can write data."""
    import uuid

    from robosystems.models.core import GraphUser, User

    unique_id = str(uuid.uuid4())[:8]
    member = User(
      id=f"test-member-{unique_id}",
      email=f"member+{unique_id}@example.com",
      name="Member User",
      password_hash="x",
    )
    test_db.add(member)
    test_db.flush()
    GraphUser.create(
      user_id=member.id,
      graph_id=test_user_graph.graph_id,
      role="member",
      is_selected=False,
      session=test_db,
    )

    with pytest.raises(HTTPException) as exc:
      _update(test_user_graph.graph_id, member.id, test_db, graph_name="Nope")

    assert exc.value.status_code == 403
    assert "Admin access" in exc.value.detail
