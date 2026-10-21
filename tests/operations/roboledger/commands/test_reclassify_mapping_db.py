"""Moving a mapped account in one write, on a real Postgres: the old arc goes,
the new one records the move and the closed months it reaches; a move the
closed months refuse leaves the mapping as it was."""

from __future__ import annotations

import pytest

from robosystems.models.api.extensions.taxonomies import (
  CreateMappingAssociationOperation,
)
from robosystems.models.extensions import Association, Structure, Taxonomy
from robosystems.operations.roboledger.commands.taxonomies import (
  MappingAssociationExistsError,
  ReplacedArcMismatchError,
  create_mapping_association,
)
from robosystems.operations.taxonomy_block.immutability import ProtectedFactsError
from tests.closed_books import ext_schema_session, reclassification_books

pytestmark = pytest.mark.unit


@pytest.fixture()
def ext_session():
  yield from ext_schema_session()


@pytest.fixture()
def books(ext_session):
  return reclassification_books(ext_session)


@pytest.fixture()
def mapped(ext_session, books):
  """The chart's book mapping, with the card on short-term debt and the newer
  card on other current liabilities."""
  s = ext_session
  mapping_taxonomy = Taxonomy(
    name="Mapping",
    taxonomy_type="mapping",
    source_taxonomy_id=books["chart"].id,
  )
  s.add(mapping_taxonomy)
  s.flush()
  mapping = Structure(
    name="CoA mapping", block_type="coa_mapping", taxonomy_id=mapping_taxonomy.id
  )
  s.add(mapping)
  s.flush()
  card_arc = Association(
    structure_id=mapping.id,
    from_element_id=books["card"].id,
    to_element_id=books["debt"].id,
    association_type="mapping",
    created_by="usr_1",
  )
  newer_arc = Association(
    structure_id=mapping.id,
    from_element_id=books["newer"].id,
    to_element_id=books["other"].id,
    association_type="mapping",
    created_by="usr_1",
  )
  s.add_all([card_arc, newer_arc])
  s.commit()
  return {**books, "mapping": mapping, "card_arc": card_arc, "newer_arc": newer_arc}


def _move(session, m, *, to, replaces, reason=None, account=None):
  account = account or m["card"]
  return create_mapping_association(
    session,
    CreateMappingAssociationOperation(
      mapping_id=m["mapping"].id,
      from_element_id=account.id,
      to_element_id=to.id,
      association_type="mapping",
      replaces_association_id=replaces,
      reason=reason,
    ),
    created_by="usr_1",
  )


def _arcs(session, m, account) -> list[Association]:
  session.expire_all()
  return (
    session.query(Association)
    .filter(
      Association.structure_id == m["mapping"].id,
      Association.from_element_id == account.id,
    )
    .all()
  )


def test_a_move_within_the_statement_replaces_the_arc_and_records_it(
  ext_session, mapped
):
  old_id = mapped["card_arc"].id

  out = _move(
    ext_session,
    mapped,
    to=mapped["other"],
    replaces=old_id,
    reason="The card is paid monthly; operating, not debt.",
  )
  ext_session.commit()

  assert out.replaced_association_id == old_id
  assert out.reclassified_periods == ["2026-06"]
  assert out.to_element_qname == "rs-gaap:OtherLiabilitiesCurrent"
  arcs = _arcs(ext_session, mapped, mapped["card"])
  assert [a.to_element_id for a in arcs] == [mapped["other"].id]
  assert arcs[0].metadata_["reclassification"] == {
    "replaces": old_id,
    "from": "rs-gaap:DebtCurrent",
    "statement_type": "balance_sheet",
    "closed_periods": ["2026-06"],
    "reason": "The card is paid monthly; operating, not debt.",
  }


def test_the_move_reads_back_on_the_mapping(ext_session, mapped):
  """The replaced arc and the closed months it reaches are visible after the
  write, not only in its response."""
  from robosystems.operations.roboledger.reads.taxonomies import get_mapping_detail

  old_id = mapped["card_arc"].id
  _move(ext_session, mapped, to=mapped["other"], replaces=old_id)
  ext_session.commit()

  detail = get_mapping_detail(ext_session, mapped["mapping"].id)
  [moved] = [a for a in detail.associations if a.from_element_id == mapped["card"].id]
  assert moved.replaced_association_id == old_id
  assert moved.reclassified_periods == ["2026-06"]


def test_a_move_across_statements_leaves_the_mapping_as_it_was(ext_session, mapped):
  with pytest.raises(
    ProtectedFactsError, match="a restatement, not a reclassification"
  ):
    _move(ext_session, mapped, to=mapped["rent"], replaces=mapped["card_arc"].id)
  ext_session.rollback()

  arcs = _arcs(ext_session, mapped, mapped["card"])
  assert [(a.id, a.to_element_id) for a in arcs] == [
    (mapped["card_arc"].id, mapped["debt"].id)
  ]


def test_a_new_arc_without_replacing_is_still_refused(ext_session, mapped):
  """Map before you close: the plain write keeps today's guard."""
  with pytest.raises(ProtectedFactsError, match="closed history is immutable"):
    _move(ext_session, mapped, to=mapped["other"], replaces=None)


def test_an_account_without_closed_history_moves_freely(ext_session, mapped):
  old_id = mapped["newer_arc"].id

  out = _move(
    ext_session, mapped, to=mapped["rent"], replaces=old_id, account=mapped["newer"]
  )
  ext_session.commit()

  assert out.replaced_association_id == old_id
  assert out.reclassified_periods == []
  arcs = _arcs(ext_session, mapped, mapped["newer"])
  assert [a.to_element_id for a in arcs] == [mapped["rent"].id]
  assert arcs[0].metadata_ == {}


def test_replacing_another_accounts_arc_is_refused(ext_session, mapped):
  with pytest.raises(ReplacedArcMismatchError):
    _move(ext_session, mapped, to=mapped["other"], replaces=mapped["newer_arc"].id)


def test_replacing_an_arc_with_itself_is_a_duplicate(ext_session, mapped):
  with pytest.raises(MappingAssociationExistsError):
    _move(ext_session, mapped, to=mapped["debt"], replaces=mapped["card_arc"].id)
