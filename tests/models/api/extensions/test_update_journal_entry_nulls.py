"""An explicit null on a field the entry cannot be without is a 422 at the
request, not a NOT NULL violation (a 500) at the write."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from robosystems.models.api.extensions.journal_entries import (
  UpdateJournalEntryRequest,
)

pytestmark = pytest.mark.unit


@pytest.mark.parametrize("field", ["posting_date", "type"])
def test_an_explicit_null_is_refused(field: str) -> None:
  with pytest.raises(ValidationError, match="cannot be null"):
    UpdateJournalEntryRequest.model_validate({"entry_id": "je_1", field: None})


def test_an_omitted_field_and_a_null_memo_are_accepted() -> None:
  body = UpdateJournalEntryRequest.model_validate({"entry_id": "je_1", "memo": None})
  assert body.posting_date is None
  assert "posting_date" not in body.model_fields_set
