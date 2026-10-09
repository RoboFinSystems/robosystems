"""A handler on a reserved event type could never fire: those events are
written by their own operation and never dispatched, so creating one is
refused rather than left as a rule that silently does nothing."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from robosystems.models.api.event_handler import (
  CreateEventHandlerRequest,
  TransactionTemplate,
  TransactionTemplateEntry,
  TransactionTemplateItem,
  TransactionTemplateLeg,
)
from robosystems.operations.event_block.reserved import ReservedEventTypeError
from robosystems.operations.roboledger.commands.event_handler import (
  create_event_handler,
)

pytestmark = pytest.mark.unit

_COMMANDS = "robosystems.operations.roboledger.commands.event_handler"


def _request(event_type: str) -> CreateEventHandlerRequest:
  leg = TransactionTemplateLeg(element_id="elem_cash", amount="{{ event.amount }}")
  return CreateEventHandlerRequest(
    name="balance rule",
    event_type=event_type,
    transaction_template=TransactionTemplate(
      transactions=[
        TransactionTemplateItem(
          entry_template=TransactionTemplateEntry(debit=leg, credit=leg)
        )
      ]
    ),
  )


def test_a_handler_on_a_reserved_type_is_refused() -> None:
  session = MagicMock()
  with pytest.raises(ReservedEventTypeError):
    create_event_handler(session, _request("balance_observed"), "user_test")
  session.add.assert_not_called()


def test_a_handler_on_an_ordinary_type_is_created() -> None:
  session = MagicMock()
  with patch(f"{_COMMANDS}.handler_to_response") as to_response:
    create_event_handler(session, _request("bank_transaction"), "user_test")
  session.add.assert_called_once()
  to_response.assert_called_once()
