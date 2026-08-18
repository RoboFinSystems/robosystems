"""Publish-list membership is the owner's to change.

`update_publish_list` / `delete_publish_list` already refused a non-owner; the
member add/remove commands did not, so any graph member could redirect where
another user's next share is delivered.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from robosystems.models.api.extensions.publish_lists import AddMembersRequest
from robosystems.operations.roboledger.commands.publish_lists import (
  PublishListNotAuthorizedError,
  PublishListNotFoundError,
  add_publish_list_members,
  remove_publish_list_member,
)

pytestmark = pytest.mark.unit


def _session_returning(row):
  session = MagicMock()
  session.execute.return_value.scalar_one_or_none.return_value = row
  return session


def _list(owner: str = "usr_owner"):
  row = MagicMock()
  row.id = "pl_1"
  row.created_by = owner
  return row


class TestAddMembersOwnership:
  def test_non_owner_is_refused_before_any_platform_lookup(self) -> None:
    session = _session_returning(_list("usr_owner"))
    with pytest.raises(PublishListNotAuthorizedError):
      add_publish_list_members(
        session,
        "pl_1",
        "kg_current",
        AddMembersRequest(target_graph_ids=["kg_target"]),
        added_by="usr_other",
      )
    session.add.assert_not_called()

  def test_missing_list_is_not_found(self) -> None:
    session = _session_returning(None)
    with pytest.raises(PublishListNotFoundError):
      add_publish_list_members(
        session,
        "pl_1",
        "kg_current",
        AddMembersRequest(target_graph_ids=["kg_target"]),
        added_by="usr_owner",
      )


class TestRemoveMemberOwnership:
  def test_non_owner_is_refused(self) -> None:
    session = _session_returning(_list("usr_owner"))
    with pytest.raises(PublishListNotAuthorizedError):
      remove_publish_list_member(session, "pl_1", "m_1", acting_user_id="usr_other")
    session.delete.assert_not_called()

  def test_owner_removes_the_member(self) -> None:
    publish_list = _list("usr_owner")
    member = MagicMock()
    session = MagicMock()
    session.execute.return_value.scalar_one_or_none.side_effect = [publish_list, member]
    assert (
      remove_publish_list_member(session, "pl_1", "m_1", acting_user_id="usr_owner")
      is True
    )
    session.delete.assert_called_once_with(member)

  def test_missing_list_returns_false(self) -> None:
    session = _session_returning(None)
    assert (
      remove_publish_list_member(session, "pl_x", "m_1", acting_user_id="usr_owner")
      is False
    )
