"""Write operations for publish lists."""

from __future__ import annotations

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from robosystems.models.api.extensions.publish_lists import (
  AddMembersRequest,
  CreatePublishListRequest,
  PublishListMemberResponse,
  PublishListResponse,
  UpdatePublishListRequest,
)
from robosystems.models.extensions import PublishList, PublishListMember
from robosystems.operations.roboledger.reads.publish_lists import (
  _list_to_response,
  enrich_members,
)


class PublishListNameConflictError(Exception):
  """Raised when a duplicate name is supplied on create/update."""


class PublishListNotFoundError(LookupError):
  """Raised when a publish_list_id does not resolve to a row."""


class PublishListNotAuthorizedError(Exception):
  """Raised when a non-owner tries to mutate a publish list."""


class PublishListMemberNotFoundError(LookupError):
  """Raised when a member_id is not part of the given publish_list_id."""


class TargetGraphsNotFoundError(LookupError):
  """Raised when one or more target_graph_ids don't resolve."""

  def __init__(self, missing: set[str]) -> None:
    super().__init__(f"Graph(s) not found: {', '.join(sorted(missing))}")
    self.missing = missing


class TargetGraphMissingExtensionError(Exception):
  """Raised when a target graph doesn't have the roboledger extension."""

  def __init__(self, graph_id: str) -> None:
    super().__init__(f"Graph '{graph_id}' does not have the roboledger extension.")
    self.graph_id = graph_id


class SelfAddError(Exception):
  """Raised when trying to add the current graph to its own publish list."""


class MembersAlreadyPresentError(Exception):
  """Raised when some target_graph_ids are already members of the list."""

  def __init__(self, existing: list[str]) -> None:
    super().__init__(f"Already in this list: {', '.join(existing)}")
    self.existing = existing


def create_publish_list(
  session: Session, body: CreatePublishListRequest, created_by: str
) -> PublishListResponse:
  """Create a new publish list.

  Raises `PublishListNameConflictError` if a list with this name already
  exists for the graph.
  """
  publish_list = PublishList(
    name=body.name,
    description=body.description,
    created_by=created_by,
  )
  session.add(publish_list)
  try:
    session.flush()
  except IntegrityError as exc:
    raise PublishListNameConflictError(
      f"A publish list named '{body.name}' already exists."
    ) from exc
  return _list_to_response(publish_list, member_count=0)


def update_publish_list(
  session: Session,
  list_id: str,
  body: UpdatePublishListRequest,
  acting_user_id: str,
) -> PublishListResponse:
  """Update a publish list's name/description.

  Raises `PublishListNotFoundError`, `PublishListNotAuthorizedError`,
  or `PublishListNameConflictError`.
  """
  row = session.execute(
    select(PublishList).where(PublishList.id == list_id)
  ).scalar_one_or_none()
  if row is None:
    raise PublishListNotFoundError(list_id)
  if row.created_by != acting_user_id:
    raise PublishListNotAuthorizedError("Not authorized to modify this publish list.")

  updates = body.model_dump(exclude_unset=True)
  for field, value in updates.items():
    setattr(row, field, value)

  try:
    session.flush()
  except IntegrityError as exc:
    raise PublishListNameConflictError(
      f"A publish list named '{body.name or row.name}' already exists."
    ) from exc

  member_count = (
    session.execute(
      select(func.count())
      .select_from(PublishListMember)
      .where(PublishListMember.publish_list_id == list_id)
    ).scalar()
    or 0
  )
  return _list_to_response(row, member_count=member_count)


def delete_publish_list(session: Session, list_id: str, acting_user_id: str) -> bool:
  """Delete a publish list. Returns False if not found.

  Raises `PublishListNotAuthorizedError` if the caller doesn't own it.
  """
  row = session.execute(
    select(PublishList).where(PublishList.id == list_id)
  ).scalar_one_or_none()
  if row is None:
    return False
  if row.created_by != acting_user_id:
    raise PublishListNotAuthorizedError("Not authorized to delete this publish list.")
  session.delete(row)  # CASCADE deletes members
  return True


def add_publish_list_members(
  session: Session,
  list_id: str,
  current_graph_id: str,
  body: AddMembersRequest,
  added_by: str,
) -> list[PublishListMemberResponse]:
  """Add target graphs to a publish list.

  Only the list's owner may change its membership — the same rule
  ``update_publish_list`` / ``delete_publish_list`` apply — since the list
  decides where that owner's next share is delivered. Validates that target
  graphs exist and can receive a share (they hold an extensions tenant:
  ``roboledger`` or ``roboinvestor``, the same rule ``share_report``
  applies to a recipient), prevents self-add, and rejects duplicates.
  """
  from robosystems.db.platform import SessionFactory
  from robosystems.models.core import Graph
  from robosystems.operations.roboledger.commands.reports import (
    _RECEIVING_EXTENSIONS,
  )

  publish_list = session.execute(
    select(PublishList).where(PublishList.id == list_id)
  ).scalar_one_or_none()
  if publish_list is None:
    raise PublishListNotFoundError(list_id)
  if publish_list.created_by != added_by:
    raise PublishListNotAuthorizedError("Not authorized to modify this publish list.")

  with SessionFactory() as platform_session:
    graphs = (
      platform_session.execute(
        select(Graph).where(Graph.graph_id.in_(body.target_graph_ids))
      )
      .scalars()
      .all()
    )
    found_ids = {str(g.graph_id) for g in graphs}

    missing = set(body.target_graph_ids) - found_ids
    if missing:
      raise TargetGraphsNotFoundError(missing)

    for g in graphs:
      extensions = g.schema_extensions or []
      if not any(ext in extensions for ext in _RECEIVING_EXTENSIONS):
        raise TargetGraphMissingExtensionError(str(g.graph_id))

  if current_graph_id in body.target_graph_ids:
    raise SelfAddError()

  existing = (
    session.execute(
      select(PublishListMember.target_graph_id).where(
        PublishListMember.publish_list_id == list_id,
        PublishListMember.target_graph_id.in_(body.target_graph_ids),
      )
    )
    .scalars()
    .all()
  )
  if existing:
    raise MembersAlreadyPresentError(list(existing))

  added: list[PublishListMember] = []
  for target_id in body.target_graph_ids:
    member = PublishListMember(
      publish_list_id=list_id,
      target_graph_id=target_id,
      added_by=added_by,
    )
    session.add(member)
    added.append(member)
  try:
    session.flush()
  except IntegrityError as exc:
    # A concurrent add of the same recipient slipped between the check above
    # and this insert; the unique key says so. Same answer as the check.
    raise MembersAlreadyPresentError(list(body.target_graph_ids)) from exc

  return enrich_members(added)


def remove_publish_list_member(
  session: Session, list_id: str, member_id: str, acting_user_id: str
) -> bool:
  """Remove a member from a publish list.

  Returns False if the member was not found in this list. Raises
  ``PublishListNotAuthorizedError`` if the caller does not own the list.
  """
  publish_list = session.execute(
    select(PublishList).where(PublishList.id == list_id)
  ).scalar_one_or_none()
  if publish_list is None:
    return False
  if publish_list.created_by != acting_user_id:
    raise PublishListNotAuthorizedError("Not authorized to modify this publish list.")
  row = session.execute(
    select(PublishListMember).where(
      PublishListMember.id == member_id,
      PublishListMember.publish_list_id == list_id,
    )
  ).scalar_one_or_none()
  if row is None:
    return False
  session.delete(row)
  return True
