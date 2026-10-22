"""An AI proposes an event handler and a person approves it.

A handler created through an AI client (the MCP surface: an AI operator, or a
person working through Claude, ChatGPT or any MCP client) is a suggestion: it
takes no part in matching until someone approves it in the app or the API. An
AI surface cannot approve, and an AI change to what an approved handler
matches or books makes it a suggestion again. On a real tenant schema, with
the handler resolution the event dispatcher uses.
"""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

import robosystems.models.extensions  # noqa: F401  (register models on the Base)
from robosystems.config import env
from robosystems.db.extensions import ExtensionsBase, extensions_session
from robosystems.models.api.event_handler import (
  CreateEventHandlerRequest,
  TransactionTemplate,
  TransactionTemplateEntry,
  TransactionTemplateItem,
  TransactionTemplateLeg,
  UpdateEventHandlerRequest,
)
from robosystems.models.extensions.roboledger.event_handler import EventHandler
from robosystems.operations.roboledger.commands.event_handler import (
  ApprovalNotAllowedError,
  create_event_handler,
  update_event_handler,
)
from robosystems.operations.roboledger.reads.event_handler import (
  list_event_handlers_page,
)

pytestmark = pytest.mark.integration

GRAPH = "kgeeeeeeeeeeeeeeee69"


@pytest.fixture(scope="module")
def tenant():
  url = env.EXTENSIONS_DATABASE_URL
  if not url:
    pytest.skip("EXTENSIONS_DATABASE_URL not configured")
  engine = create_engine(url)
  try:
    with engine.connect() as probe:
      probe.execute(text("SELECT 1"))
  except OperationalError as exc:
    engine.dispose()
    pytest.skip(f"extensions database unreachable: {exc.orig}")
  tables = [t for t in ExtensionsBase.metadata.sorted_tables if t.schema is None]
  try:
    with engine.begin() as conn:
      conn.execute(text(f"DROP SCHEMA IF EXISTS {GRAPH} CASCADE"))
      conn.execute(text(f"CREATE SCHEMA {GRAPH}"))
      ExtensionsBase.metadata.create_all(
        bind=conn.execution_options(schema_translate_map={None: GRAPH}),
        tables=tables,
      )
    yield
  finally:
    with engine.begin() as conn:
      conn.execute(text(f"DROP SCHEMA IF EXISTS {GRAPH} CASCADE"))
    engine.dispose()


@pytest.fixture(autouse=True)
def empty(tenant):
  with extensions_session(GRAPH) as session:
    session.execute(text("DELETE FROM event_handlers"))
  yield


def _request(name: str = "Stripe payouts") -> CreateEventHandlerRequest:
  leg = TransactionTemplateLeg(element_id="elem_cash", amount="{{ event.amount }}")
  return CreateEventHandlerRequest(
    name=name,
    event_type="bank_transaction",
    match_source="stripe",
    transaction_template=TransactionTemplate(
      transactions=[
        TransactionTemplateItem(
          entry_template=TransactionTemplateEntry(debit=leg, credit=leg)
        )
      ]
    ),
  )


def _eligible(handler_id: str) -> bool:
  """The dispatcher's rule: active, and not an unapproved AI suggestion."""
  with extensions_session(GRAPH) as session:
    handler = session.get(EventHandler, handler_id)
    assert handler is not None
    return handler.is_active and not (
      handler.suggested_by == "ai" and handler.approved_by is None
    )


def test_a_person_s_handler_is_live_and_an_ai_s_waits_for_approval(tenant):
  with extensions_session(GRAPH) as session:
    person = create_event_handler(session, _request("person"), "usr_1")
    ai = create_event_handler(session, _request("ai"), "usr_1", surface="mcp")

  assert (person.suggested_by, person.awaiting_approval) == (None, False)
  assert (ai.suggested_by, ai.awaiting_approval) == ("ai", True)
  assert _eligible(person.id)
  assert not _eligible(ai.id)


def test_an_ai_client_cannot_approve(tenant):
  with extensions_session(GRAPH) as session:
    ai = create_event_handler(session, _request(), "usr_1", surface="mcp")
    with pytest.raises(ApprovalNotAllowedError):
      update_event_handler(
        session,
        UpdateEventHandlerRequest(event_handler_id=ai.id, approve=True),
        "usr_1",
        surface="mcp",
      )
  assert not _eligible(ai.id)


def test_a_person_approves_and_the_handler_goes_live(tenant):
  with extensions_session(GRAPH) as session:
    ai = create_event_handler(session, _request(), "usr_1", surface="mcp")
    approved = update_event_handler(
      session,
      UpdateEventHandlerRequest(event_handler_id=ai.id, approve=True),
      "usr_2",
    )
  assert (approved.approved_by, approved.awaiting_approval) == ("usr_2", False)
  assert _eligible(ai.id)


def test_an_ai_change_to_what_it_books_needs_approval_again(tenant):
  """Approved, then edited over MCP: the edit is the AI's proposal, so the
  handler leaves matching until a person approves it again — even one a
  person wrote. A rename or a deactivation changes nothing it books."""
  with extensions_session(GRAPH) as session:
    person = create_event_handler(session, _request(), "usr_1")
    renamed = update_event_handler(
      session,
      UpdateEventHandlerRequest(
        event_handler_id=person.id, name="Stripe payouts (net)"
      ),
      "usr_1",
      surface="mcp",
    )
    assert renamed.suggested_by is None
    retargeted = update_event_handler(
      session,
      UpdateEventHandlerRequest(event_handler_id=person.id, match_source="adyen"),
      "usr_1",
      surface="mcp",
    )
  assert (retargeted.suggested_by, retargeted.awaiting_approval) == ("ai", True)
  assert not _eligible(person.id)


def test_the_list_separates_what_waits_for_a_person(tenant):
  with extensions_session(GRAPH) as session:
    create_event_handler(session, _request("person"), "usr_1")
    create_event_handler(session, _request("ai"), "usr_1", surface="mcp")

    waiting = list_event_handlers_page(session, awaiting_approval=True)
    others = list_event_handlers_page(session, awaiting_approval=False)
    everything = list_event_handlers_page(session)

  assert [h.name for h in waiting.event_handlers] == ["ai"]
  # A person's handler has no suggested_by; it must not fall out of "others".
  assert [h.name for h in others.event_handlers] == ["person"]
  assert everything.pagination.total == 2


def test_an_ai_metadata_patch_or_reactivation_needs_approval_again(tenant):
  """A template can read the handler's metadata into an account or amount, so
  an AI client's metadata patch changes what it books; and reactivating a
  handler a person took out of matching puts it back. Both wait for a
  person; deactivating does not."""
  with extensions_session(GRAPH) as session:
    patched = create_event_handler(session, _request("patched"), "usr_1")
    update_event_handler(
      session,
      UpdateEventHandlerRequest(
        event_handler_id=patched.id, metadata_patch={"expense_account": "elem_x"}
      ),
      "usr_1",
      surface="mcp",
    )
    revived = create_event_handler(session, _request("revived"), "usr_1")
    off = update_event_handler(
      session,
      UpdateEventHandlerRequest(event_handler_id=revived.id, is_active=False),
      "usr_1",
      surface="mcp",
    )
    assert off.suggested_by is None
    update_event_handler(
      session,
      UpdateEventHandlerRequest(event_handler_id=revived.id, is_active=True),
      "usr_1",
      surface="mcp",
    )
  assert not _eligible(patched.id)
  assert not _eligible(revived.id)
