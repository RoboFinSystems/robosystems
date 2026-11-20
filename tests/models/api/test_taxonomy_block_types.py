"""The block types `create-taxonomy-block` takes for a structure."""

import pytest
from pydantic import ValidationError

from robosystems.models.api.taxonomy_block import TaxonomyBlockStructureRequest
from robosystems.operations.roboledger.commands.report_import import (
  _STRUCTURE_BLOCK_TYPES,
)

pytestmark = pytest.mark.unit


def test_the_retired_rollforward_block_is_refused():
  # No Information Block read renders one, so creating it made a structure
  # nothing could show.
  with pytest.raises(ValidationError):
    TaxonomyBlockStructureRequest(name="Rollforward", block_type="rollforward")


@pytest.mark.parametrize("block_type", sorted(_STRUCTURE_BLOCK_TYPES))
def test_a_received_report_only_keeps_block_types_the_command_takes(block_type):
  TaxonomyBlockStructureRequest(name="Received", block_type=block_type)
