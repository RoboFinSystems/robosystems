"""A remap of an account with closed history is judged on every arc the
account gains, in the database."""

from __future__ import annotations

import pytest

from robosystems.models.api.taxonomy_block import (
  TaxonomyBlockAssociationRequest,
  UpdateTaxonomyBlockRequest,
)
from robosystems.models.extensions import Association, Structure
from robosystems.operations.taxonomy_block import update_validator
from tests.closed_books import ext_schema_session, reclassification_books

pytestmark = pytest.mark.integration

# Same statement as the arc it replaces, and a different statement.
_SAME_STATEMENT = "rs-gaap:OtherLiabilitiesCurrent"
_OTHER_STATEMENT = "rs-gaap:OperatingLeaseExpense"


@pytest.fixture()
def ext_session():
  yield from ext_schema_session()


def _arc(to_ref: str) -> TaxonomyBlockAssociationRequest:
  return TaxonomyBlockAssociationRequest(
    structure_ref="Mapping",
    from_ref="coa:2110",
    to_ref=to_ref,
    association_type="mapping",
  )


def _codes(session, targets: list[str]) -> list[str]:
  books = reclassification_books(session)
  mapping = Structure(
    name="Mapping", block_type="coa_mapping", taxonomy_id=books["chart"].id
  )
  session.add(mapping)
  session.flush()
  old = Association(
    structure_id=mapping.id,
    from_element_id=books["card"].id,
    to_element_id=books["debt"].id,
    association_type="mapping",
    created_by="usr_1",
  )
  session.add(old)
  session.commit()
  payload = UpdateTaxonomyBlockRequest(
    taxonomy_id=str(books["chart"].id),
    associations_to_remove=[str(old.id)],
    associations_to_add=[_arc(target) for target in targets],
  )
  issues = update_validator.validate_update_envelope(session, books["chart"], payload)
  return [issue.code for issue in issues]


@pytest.mark.parametrize(
  "targets",
  [
    [_OTHER_STATEMENT],
    [_SAME_STATEMENT, _OTHER_STATEMENT],
    [_OTHER_STATEMENT, _SAME_STATEMENT],
  ],
  ids=["alone", "after-an-allowed-arc", "before-an-allowed-arc"],
)
def test_an_arc_to_another_statement_is_refused_wherever_it_sits(ext_session, targets):
  assert "protected_facts" in _codes(ext_session, targets)


def test_a_move_within_the_statement_is_still_allowed(ext_session):
  assert "protected_facts" not in _codes(ext_session, [_SAME_STATEMENT])
