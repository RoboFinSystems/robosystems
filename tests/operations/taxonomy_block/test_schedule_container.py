"""Schedule-container update/delete are not exposed; the refusal points at the
Schedule Information Block surface, not an admin path."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from robosystems.models.api.taxonomy_block import (
  DeleteTaxonomyBlockRequest,
  UpdateTaxonomyBlockRequest,
)
from robosystems.operations.taxonomy_block import schedule_container


def test_update_refusal_points_at_the_information_block_surface() -> None:
  with pytest.raises(NotImplementedError) as exc:
    schedule_container.update(
      MagicMock(), UpdateTaxonomyBlockRequest(taxonomy_id="tx_1"), "usr_1"
    )
  assert "library_creator" not in str(exc.value)
  assert "Schedule Information Blocks" in str(exc.value)


def test_delete_refusal_points_at_the_information_block_surface() -> None:
  payload = DeleteTaxonomyBlockRequest(taxonomy_id="tx_1", reason="cleanup")
  with pytest.raises(NotImplementedError) as exc:
    schedule_container.delete(MagicMock(), payload, "usr_1")
  assert "library_creator" not in str(exc.value)
