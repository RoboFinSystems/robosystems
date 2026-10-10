"""Learned classification: the account a counterparty's bank lines usually go
to, kept on the counterparty and offered as the next line's suggestion.

A bank line resolves to an agent at capture, and the agent carries a default
in ``metadata.classification``: the account, whether to suggest it or always
ask, and how often committed lines agreed with it. The suggestion ladder puts
a learned default above the feed's own category hint, which is kept beside it
(``feed_suggested_*``) so it returns when the default goes. Only a committed
line teaches, and a default never posts by itself: the inbox is the gate.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import Any, Literal

from sqlalchemy import select
from sqlalchemy.orm import Session

from robosystems.models.extensions.element import Element
from robosystems.models.extensions.roboledger import Agent, Event
from robosystems.operations.locking import lock_by_id

CLASSIFICATION_KEY = "classification"
# Lines that are classified into the chart: an internal transfer knows both
# legs and is never classified.
LEARNED_EVENT_TYPES = frozenset({"bank_transaction", "bank_fee", "external_transfer"})

ClassificationMode = Literal["suggest", "always_ask"]
SUGGEST: ClassificationMode = "suggest"
ALWAYS_ASK: ClassificationMode = "always_ask"

SOURCE_AGENT_DEFAULT = "agent_default"
SOURCE_TIER0 = "tier0"

# The suggestion keys the ladder owns on a captured line.
SUGGESTED_ELEMENT_ID = "suggested_element_id"
SUGGESTED_ACCOUNT_NAME = "suggested_account_name"
FEED_SUGGESTED_ELEMENT_ID = "feed_suggested_element_id"
FEED_SUGGESTED_ACCOUNT_NAME = "feed_suggested_account_name"
SUGGESTION_SOURCE = "suggestion_source"
SUGGESTION_BASIS = "suggestion_basis"
LADDER_KEYS = (
  SUGGESTED_ELEMENT_ID,
  SUGGESTED_ACCOUNT_NAME,
  FEED_SUGGESTED_ELEMENT_ID,
  FEED_SUGGESTED_ACCOUNT_NAME,
  SUGGESTION_SOURCE,
  SUGGESTION_BASIS,
)
# Set on a commit: whether the suggestion was taken.
SUGGESTION_OUTCOME = "suggestion_outcome"
# Set on a commit to move an existing default to the account chosen.
REMEMBER_CLASSIFICATION = "remember_classification"


@dataclass(frozen=True)
class AgentDefault:
  """A counterparty's usual account and how the record for it stands."""

  element_id: str
  mode: ClassificationMode
  confirmations: int
  overrides: int
  set_by: str | None
  set_at: str | None
  learned_from: str | None

  def to_metadata(self) -> dict[str, Any]:
    return {
      "element_id": self.element_id,
      "mode": self.mode,
      "confirmations": self.confirmations,
      "overrides": self.overrides,
      "set_by": self.set_by,
      "set_at": self.set_at,
      "learned_from": self.learned_from,
    }


def read_default(metadata: dict[str, Any] | None) -> AgentDefault | None:
  raw = (metadata or {}).get(CLASSIFICATION_KEY)
  if not isinstance(raw, dict) or not raw.get("element_id"):
    return None
  mode = raw.get("mode")
  return AgentDefault(
    element_id=str(raw["element_id"]),
    mode=ALWAYS_ASK if mode == ALWAYS_ASK else SUGGEST,
    confirmations=int(raw.get("confirmations") or 0),
    overrides=int(raw.get("overrides") or 0),
    set_by=raw.get("set_by"),
    set_at=raw.get("set_at"),
    learned_from=raw.get("learned_from"),
  )


def write_default(agent: Agent, default: AgentDefault | None) -> None:
  metadata = dict(agent.metadata_ or {})
  if default is None:
    metadata.pop(CLASSIFICATION_KEY, None)
  else:
    metadata[CLASSIFICATION_KEY] = default.to_metadata()
  agent.metadata_ = metadata
  agent.updated_at = datetime.now(UTC)


def _now() -> str:
  return datetime.now(UTC).isoformat()


def _postable(session: Session, element_id: str) -> Element | None:
  element = session.get(Element, element_id)
  if element is None or element.is_abstract or element.is_active is False:
    return None
  return element


def _basis(agent: Agent, element: Element, default: AgentDefault) -> str:
  seen = default.confirmations + default.overrides
  if seen == 0:
    return f"{agent.name} is set to {element.name}."
  lines = "line" if seen == 1 else "lines"
  return (
    f"{agent.name}, classified to {element.name} on {default.confirmations} of "
    f"{seen} committed {lines}."
  )


def apply_ladder(
  session: Session, metadata: dict[str, Any], agent_id: str | None
) -> dict[str, Any]:
  """A captured line's metadata with the highest suggestion available: the
  agent's learned default, else the feed's category hint, else none.

  The feed's hint is first read from the line's own suggestion and kept in
  ``feed_suggested_*``, so applying the ladder again is idempotent.
  """
  out = dict(metadata)
  if FEED_SUGGESTED_ELEMENT_ID not in out and FEED_SUGGESTED_ACCOUNT_NAME not in out:
    if out.get(SUGGESTION_SOURCE) != SOURCE_AGENT_DEFAULT:
      out[FEED_SUGGESTED_ELEMENT_ID] = out.get(SUGGESTED_ELEMENT_ID)
      out[FEED_SUGGESTED_ACCOUNT_NAME] = out.get(SUGGESTED_ACCOUNT_NAME)

  agent = session.get(Agent, agent_id) if agent_id else None
  default = read_default(agent.metadata_) if agent is not None else None
  element = (
    _postable(session, default.element_id)
    if default is not None and default.mode == SUGGEST
    else None
  )
  if agent is not None and default is not None and element is not None:
    out[SUGGESTED_ELEMENT_ID] = str(element.id)
    out[SUGGESTED_ACCOUNT_NAME] = element.name
    out[SUGGESTION_SOURCE] = SOURCE_AGENT_DEFAULT
    out[SUGGESTION_BASIS] = _basis(agent, element, default)
    return out

  feed_id = out.get(FEED_SUGGESTED_ELEMENT_ID)
  feed_name = out.get(FEED_SUGGESTED_ACCOUNT_NAME)
  for key, value in (
    (SUGGESTED_ELEMENT_ID, feed_id),
    (SUGGESTED_ACCOUNT_NAME, feed_name),
  ):
    if value is None:
      out.pop(key, None)
    else:
      out[key] = value
  if feed_id or feed_name:
    out[SUGGESTION_SOURCE] = SOURCE_TIER0
  else:
    out.pop(SUGGESTION_SOURCE, None)
  if agent is not None and default is not None and default.mode == ALWAYS_ASK:
    out[SUGGESTION_BASIS] = f"{agent.name} is set to always ask."
  else:
    out.pop(SUGGESTION_BASIS, None)
  return out


def resuggest_open_lines(session: Session, agent_id: str) -> int:
  """Re-run the ladder on the agent's still-captured lines. A line another
  transaction holds is skipped rather than waited on: it is being written
  (most likely committed), and waiting could deadlock with that commit's
  learning, which locks the agent. Returns how many lines changed."""
  events = (
    session.execute(
      select(Event)
      .where(
        Event.agent_id == agent_id,
        Event.status == "captured",
        Event.event_type.in_(sorted(LEARNED_EVENT_TYPES)),
      )
      .order_by(Event.id)
      .with_for_update(skip_locked=True)
    )
    .scalars()
    .all()
  )
  changed = 0
  for event in events:
    before = dict(event.metadata_ or {})
    after = apply_ladder(session, before, agent_id)
    if after != before:
      event.metadata_ = after
      changed += 1
  session.flush()
  return changed


def chosen_account(metadata: dict[str, Any]) -> str | None:
  """The single account a classified line went to; None for a split or for
  a line nothing classified."""
  if metadata.get("classified_allocations"):
    return None
  element_id = metadata.get("classified_element_id")
  if not element_id and metadata.get("accept_suggestion"):
    element_id = metadata.get(SUGGESTED_ELEMENT_ID)
  return str(element_id) if element_id else None


def learn_from_commit(session: Session, event: Event, created_by: str) -> None:
  """Record a committed bank line on its counterparty's default, and stamp
  whether its suggestion was taken.

  No default yet: the account becomes it, unless the committer said not to
  remember it. The same account: one more confirmation. Another account: one
  more override, and the default moves only when the committer asked
  (``remember_classification``). A split counts toward nothing.
  """
  if event.event_type not in LEARNED_EVENT_TYPES:
    return
  metadata = dict(event.metadata_ or {})
  chosen = chosen_account(metadata)
  suggested = metadata.get(SUGGESTED_ELEMENT_ID)
  if metadata.get("classified_allocations"):
    outcome = "split"
  elif suggested is None:
    outcome = "none"
  else:
    outcome = "accepted" if chosen == suggested else "overridden"
  metadata[SUGGESTION_OUTCOME] = outcome
  event.metadata_ = metadata

  if chosen is None or not event.agent_id:
    return
  agent = lock_by_id(
    session,
    Agent,
    event.agent_id,
    f"Counterparty {event.agent_id} is being written by another process. "
    "Retry in a moment.",
  )
  if agent is None:
    return
  default = read_default(agent.metadata_)
  remember = metadata.get(REMEMBER_CLASSIFICATION)
  learned: AgentDefault | None = None
  if default is None:
    if remember is not False:
      learned = AgentDefault(
        element_id=chosen,
        mode=SUGGEST,
        confirmations=1,
        overrides=0,
        set_by=created_by,
        set_at=_now(),
        learned_from=str(event.id),
      )
  elif default.element_id == chosen:
    learned = replace(default, confirmations=default.confirmations + 1)
  elif remember is True:
    learned = AgentDefault(
      element_id=chosen,
      mode=default.mode,
      confirmations=1,
      overrides=0,
      set_by=created_by,
      set_at=_now(),
      learned_from=str(event.id),
    )
  else:
    learned = replace(default, overrides=default.overrides + 1)

  if learned is None:
    return
  moved = default is None or learned.element_id != default.element_id
  write_default(agent, learned)
  session.flush()
  if moved:
    resuggest_open_lines(session, str(agent.id))


def set_default(
  session: Session,
  agent: Agent,
  *,
  element_id: str | None,
  mode: ClassificationMode | None,
  set_by: str,
) -> None:
  """Set, change or clear a counterparty's default by hand, then re-suggest
  its open lines. ``element_id=""`` clears it; ``None`` keeps the account and
  changes only the mode. Raises ``ValueError`` for an account that cannot be
  posted to, or a mode with no account to apply to."""
  current = read_default(agent.metadata_)
  if element_id == "":
    write_default(agent, None)
  else:
    target = element_id or (current.element_id if current else None)
    if target is None:
      raise ValueError(
        "This counterparty has no default account yet; give one with "
        "classification_element_id."
      )
    if element_id is not None and _postable(session, element_id) is None:
      raise ValueError(
        f"{element_id!r} is not an active account that can be posted to."
      )
    same_account = current is not None and current.element_id == target
    write_default(
      agent,
      AgentDefault(
        element_id=target,
        mode=mode or (current.mode if current else SUGGEST),
        confirmations=current.confirmations if same_account and current else 0,
        overrides=current.overrides if same_account and current else 0,
        set_by=set_by,
        set_at=_now(),
        learned_from=current.learned_from if same_account and current else None,
      ),
    )
  session.flush()
  resuggest_open_lines(session, str(agent.id))


@dataclass(frozen=True)
class LearnedDefaults:
  agents_learned: int
  agents_kept: int
  lines_read: int
  open_lines_resuggested: int


def learn_from_history(
  session: Session, created_by: str, *, dry_run: bool = False
) -> LearnedDefaults:
  """Seed defaults from the bank lines already committed, for each
  counterparty that has none: its most-used account becomes the default, with
  the lines that agreed as confirmations and the rest as overrides. A
  counterparty that already has a default keeps it."""
  rows = session.execute(
    select(Event.agent_id, Event.id, Event.metadata_)
    .where(
      Event.status.in_(("committed", "fulfilled")),
      Event.agent_id.is_not(None),
      Event.event_type.in_(sorted(LEARNED_EVENT_TYPES)),
    )
    .order_by(Event.effective_at, Event.id)
  ).all()
  by_agent: dict[str, Counter[str]] = {}
  latest: dict[tuple[str, str], str] = {}
  lines_read = 0
  for agent_id, event_id, metadata in rows:
    chosen = chosen_account(metadata or {})
    if chosen is None:
      continue
    lines_read += 1
    by_agent.setdefault(str(agent_id), Counter())[chosen] += 1
    latest[(str(agent_id), chosen)] = str(event_id)

  learned = kept = resuggested = 0
  for agent_id, counts in sorted(by_agent.items()):
    agent = session.get(Agent, agent_id)
    if agent is None:
      continue
    if read_default(agent.metadata_) is not None:
      kept += 1
      continue
    element_id, agreed = counts.most_common(1)[0]
    if _postable(session, element_id) is None:
      continue
    learned += 1
    if dry_run:
      continue
    write_default(
      agent,
      AgentDefault(
        element_id=element_id,
        mode=SUGGEST,
        confirmations=agreed,
        overrides=sum(counts.values()) - agreed,
        set_by=created_by,
        set_at=_now(),
        learned_from=latest[(agent_id, element_id)],
      ),
    )
    session.flush()
    resuggested += resuggest_open_lines(session, agent_id)
  return LearnedDefaults(
    agents_learned=learned,
    agents_kept=kept,
    lines_read=lines_read,
    open_lines_resuggested=resuggested,
  )
