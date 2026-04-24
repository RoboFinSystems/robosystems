"""Agent commands — create and update counterparty records."""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from robosystems.models.api.extensions.agent import (
  AgentResponse,
  CreateAgentRequest,
  UpdateAgentRequest,
)
from robosystems.models.extensions.roboledger.agent import Agent
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
) -> AgentResponse:
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
) -> AgentResponse:
  agent = session.get(Agent, body.agent_id)
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
    merged = dict(agent.metadata_ or {})
    merged.update(body.metadata_patch)
    agent.metadata_ = merged

  agent.updated_at = datetime.now(UTC)
  session.commit()
  session.refresh(agent)
  return agent_to_response(agent)
