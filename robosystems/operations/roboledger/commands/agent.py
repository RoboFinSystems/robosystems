"""Agent commands — create and update counterparty records."""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from robosystems.models.api.extensions.agent import (
  CreateAgentRequest,
  LearnClassificationDefaultsRequest,
  LearnClassificationDefaultsResponse,
  LedgerAgentResponse,
  UpdateAgentRequest,
)
from robosystems.models.extensions.roboledger.agent import Agent
from robosystems.operations.roboledger.classification import (
  CLASSIFICATION_KEY,
  learn_from_history,
  set_default,
)
from robosystems.operations.roboledger.reads.agent import agent_to_response


class AgentNotFoundError(Exception):
  def __init__(self, agent_id: str) -> None:
    super().__init__(f"Agent not found: {agent_id}")
    self.agent_id = agent_id


class DuplicateExternalIdError(Exception):
  def __init__(self, source: str, external_id: str) -> None:
    super().__init__(
      f"Agent with source='{source}' and external_id='{external_id}' already exists."
    )
    self.source = source
    self.external_id = external_id


def create_agent(
  session: Session,
  body: CreateAgentRequest,
  created_by: str,
) -> LedgerAgentResponse:
  agent = Agent(
    agent_type=body.agent_type,
    name=body.name,
    legal_name=body.legal_name,
    tax_id=body.tax_id,
    registration_number=body.registration_number,
    duns=body.duns,
    lei=body.lei,
    email=body.email,
    phone=body.phone,
    address=body.address,
    source=body.source,
    external_id=body.external_id,
    is_active=body.is_active,
    is_1099_recipient=body.is_1099_recipient,
    metadata_=body.metadata,
    created_at=datetime.now(UTC),
    updated_at=datetime.now(UTC),
    created_by=created_by,
  )
  session.add(agent)
  try:
    session.commit()
  except IntegrityError:
    session.rollback()
    if body.external_id:
      raise DuplicateExternalIdError(body.source, body.external_id)
    raise
  session.refresh(agent)
  return agent_to_response(agent)


def update_agent(
  session: Session,
  body: UpdateAgentRequest,
  created_by: str,
) -> LedgerAgentResponse:
  # Locked: `metadata_patch` is a read-modify-write, and concurrent patches
  # would silently drop each other's keys.
  from robosystems.operations.locking import lock_by_id

  agent = lock_by_id(
    session,
    Agent,
    body.agent_id,
    f"Agent {body.agent_id} is being written by another process. Retry in a moment.",
  )
  if agent is None:
    raise AgentNotFoundError(body.agent_id)

  for field in (
    "name",
    "legal_name",
    "tax_id",
    "registration_number",
    "duns",
    "lei",
    "email",
    "phone",
    "address",
    "is_active",
    "is_1099_recipient",
  ):
    value = getattr(body, field)
    if value is not None:
      setattr(agent, field, value)

  if body.metadata_patch:
    if CLASSIFICATION_KEY in body.metadata_patch:
      raise ValueError(
        "Set the default classification with classification_element_id and "
        "classification_mode, not metadata_patch."
      )
    merged = dict(agent.metadata_ or {})
    merged.update(body.metadata_patch)
    agent.metadata_ = merged

  if body.classification_element_id is not None or body.classification_mode:
    set_default(
      session,
      agent,
      element_id=body.classification_element_id,
      mode=body.classification_mode,
      set_by=created_by,
    )

  agent.updated_at = datetime.now(UTC)
  session.commit()
  session.refresh(agent)
  return agent_to_response(agent)


def learn_classification_defaults(
  session: Session,
  body: LearnClassificationDefaultsRequest,
  created_by: str,
) -> LearnClassificationDefaultsResponse:
  """Give each counterparty with committed bank lines and no default the
  account most of its lines went to, and re-suggest its open lines."""
  learned = learn_from_history(session, created_by, dry_run=body.dry_run)
  if body.dry_run:
    session.rollback()
  else:
    session.commit()
  return LearnClassificationDefaultsResponse(
    agents_learned=learned.agents_learned,
    agents_kept=learned.agents_kept,
    lines_read=learned.lines_read,
    open_lines_resuggested=learned.open_lines_resuggested,
    dry_run=body.dry_run,
  )
