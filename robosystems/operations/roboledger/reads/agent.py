"""Read operations for agents (counterparties)."""

from __future__ import annotations

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from robosystems.models.api.extensions.agent import (
  AgentActivityResponse,
  AgentResponse,
  TransactionSummary,
)
from robosystems.models.extensions.roboledger.agent import Agent
from robosystems.models.extensions.roboledger.event import Event
from robosystems.models.extensions.roboledger.transaction import Transaction


def agent_to_response(agent: Agent) -> AgentResponse:
  return AgentResponse(
    id=agent.id,
    agent_type=agent.agent_type,
    name=agent.name,
    legal_name=agent.legal_name,
    tax_id=agent.tax_id,
    registration_number=agent.registration_number,
    duns=agent.duns,
    lei=agent.lei,
    email=agent.email,
    phone=agent.phone,
    address=agent.address,
    source=agent.source,
    external_id=agent.external_id,
    is_active=agent.is_active,
    is_1099_recipient=agent.is_1099_recipient,
    created_at=agent.created_at,
    updated_at=agent.updated_at,
    created_by=agent.created_by,
  )


def get_agent(session: Session, agent_id: str) -> AgentResponse | None:
  agent = session.get(Agent, agent_id)
  if agent is None:
    return None
  return agent_to_response(agent)


def list_agents(
  session: Session,
  *,
  agent_type: str | None = None,
  source: str | None = None,
  is_active: bool | None = True,
  limit: int = 50,
  offset: int = 0,
) -> list[AgentResponse]:
  query = select(Agent)
  if agent_type is not None:
    query = query.where(Agent.agent_type == agent_type)
  if source is not None:
    query = query.where(Agent.source == source)
  if is_active is not None:
    query = query.where(Agent.is_active.is_(is_active))
  query = query.order_by(Agent.name).limit(limit).offset(offset)
  agents = session.execute(query).scalars().all()
  return [agent_to_response(a) for a in agents]


def get_agent_activity(
  session: Session,
  agent_id: str,
  *,
  limit: int = 100,
) -> AgentActivityResponse | None:
  agent = session.get(Agent, agent_id)
  if agent is None:
    return None

  # Recent events for this agent
  events = (
    session.execute(
      select(Event)
      .where(Event.agent_id == agent_id)
      .order_by(Event.occurred_at.desc())
      .limit(limit)
    )
    .scalars()
    .all()
  )

  # Recent transactions triggered by those events
  event_ids = [e.id for e in events]
  transactions = []
  if event_ids:
    txns = (
      session.execute(
        select(Transaction)
        .where(Transaction.triggered_by_event_id.in_(event_ids))
        .order_by(Transaction.date.desc())
        .limit(limit)
      )
      .scalars()
      .all()
    )
    transactions = [
      TransactionSummary(
        id=t.id,
        type=t.type,
        date=str(t.date),
        amount=t.amount,
        currency=t.currency,
        status=t.status,
        description=t.description,
        triggered_by_event_id=t.triggered_by_event_id,
      )
      for t in txns
    ]

  # Counts
  event_count = session.execute(
    select(func.count()).select_from(Event).where(Event.agent_id == agent_id)
  ).scalar_one()

  txn_count = session.execute(
    select(func.count(Transaction.id))
    .select_from(Transaction)
    .join(Event, Transaction.triggered_by_event_id == Event.id)
    .where(Event.agent_id == agent_id)
  ).scalar_one()

  from robosystems.operations.roboledger.reads.event_block import list_event_blocks

  recent_events = list_event_blocks(
    session,
    agent_id=agent_id,
    limit=limit,
    offset=0,
  )

  return AgentActivityResponse(
    agent=agent_to_response(agent),
    recent_events=[e.model_dump(mode="json") for e in recent_events],
    recent_transactions=transactions,
    event_count=event_count,
    transaction_count=txn_count,
  )
