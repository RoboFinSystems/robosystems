"""A memory id names one row of the graph's store and nothing else."""

import re

import pytest

from robosystems.models.api.graphs.operations import ForgetOp
from robosystems.operations.memory.service import MEMORY_ID_PATTERN, require_memory_id

VALID = "mem_" + "0123456789abcdef" * 2


@pytest.mark.unit
def test_well_formed_id_passes():
  assert require_memory_id(VALID) == VALID


@pytest.mark.unit
@pytest.mark.parametrize(
  "memory_id",
  [
    "../..",
    "..%2F..",
    "mem_../x",
    "",
    "mem_" + "0" * 31,
    "mem_" + "G" * 32,
    VALID + "/x",
    None,
  ],
)
def test_anything_else_is_refused_before_a_url_is_built(memory_id):
  with pytest.raises(ValueError):
    require_memory_id(memory_id)  # type: ignore[arg-type]


@pytest.mark.unit
def test_rest_and_service_agree_on_the_shape():
  rest_pattern = ForgetOp.model_fields["memory_id"].metadata[0].pattern
  assert rest_pattern == MEMORY_ID_PATTERN
  assert re.fullmatch(MEMORY_ID_PATTERN, VALID)
