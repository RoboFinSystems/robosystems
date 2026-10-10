"""Reconciliation blocks: the Structure, its concepts and rule, and one
standing FactSet per period.

Nothing here is a new table. The block is a ``reconciliation`` Structure, the
period's comparison is a FactSet under ``observed`` provenance, and whether it
reconciles is the ``VerificationResult`` of one rule on the block.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from robosystems.models.api.extensions.reconciliations import (
  ReconciliationMethod,
  ReconciliationPreviewResponse,
  ReconciliationRow,
)
from robosystems.models.api.fact_provenance import ObservedProvenance
from robosystems.models.api.information_block import ReconciliationMechanics
from robosystems.models.extensions import (
  Association,
  Element,
  Rule,
  Structure,
  Taxonomy,
  VerificationResult,
)
from robosystems.models.extensions.roboledger import Event, Fact, FactSet
from robosystems.operations.information_block.reconciliation import (
  RECONCILIATION_BLOCK_TYPE,
  RECONCILIATION_FACTSET_TYPE,
  own_rule_id,
)
from robosystems.operations.information_block.rules.engine import (
  evaluate_rules_for_structure,
)
from robosystems.operations.locking import bounded_lock_wait
from robosystems.operations.roboledger.entity_scope import owner_entity_id
from robosystems.operations.roboledger.fact_set import create_fact_set

from .resolvers import IndependentSide, ReconciliationWindow

_TAXONOMY_NAME = "Reconciliations"
_NAMESPACE = "rs-rec"
UNRECONCILED_DIFFERENCE = f"{_NAMESPACE}:UnreconciledDifference"
ACCOUNTS_COMPARED = f"{_NAMESPACE}:AccountsCompared"
ACCOUNTS_DIFFERENT = f"{_NAMESPACE}:AccountsDifferent"
LEDGER_BALANCE = f"{_NAMESPACE}:LedgerBalance"
INDEPENDENT_BALANCE = f"{_NAMESPACE}:IndependentBalance"
_ROOT = f"{_NAMESPACE}:ReconciliationAbstract"

# (qname, name, monetary).
_CONCEPTS: tuple[tuple[str, str, bool], ...] = (
  (UNRECONCILED_DIFFERENCE, "Unreconciled difference", True),
  (ACCOUNTS_COMPARED, "Accounts compared", False),
  (ACCOUNTS_DIFFERENT, "Accounts that do not tie", False),
  (LEDGER_BALANCE, "Ledger balance", True),
  (INDEPENDENT_BALANCE, "Independent balance", True),
)

# What each scope's block presents, in order.
_LEDGER_CONCEPTS = (UNRECONCILED_DIFFERENCE, ACCOUNTS_COMPARED, ACCOUNTS_DIFFERENT)
_ACCOUNT_CONCEPTS = (LEDGER_BALANCE, INDEPENDENT_BALANCE, UNRECONCILED_DIFFERENCE)

# The differing accounts kept on the period's FactSet; the counts stay exact.
MAX_STORED_DIFFERENCES = 100

SIGN_OFF_EVENT_TYPE = "reconciliation_signed_off"

# How the standing comparison came to be: someone ran the operation, or a
# source sync refreshed it.
ComparedVia = Literal["operation", "sync"]

# Arbitrary but must stay stable, as the period fence's class is.
_SEED_LOCK_CLASS = 872402

_SOURCE_LEDGER_NAMES = {"quickbooks": "QuickBooks"}


def lock_reconciliation_writes(session: Session, graph_id: str) -> None:
  """Serialize a graph's reconciliation writers for this transaction, so two
  refreshes cannot both seed the block or both write a period's set."""
  with bounded_lock_wait(
    session,
    "This graph's reconciliations are being refreshed by another process. "
    "Retry in a moment.",
  ):
    session.execute(
      text("SELECT pg_advisory_xact_lock(:classid, hashtext(:ident))"),
      {"classid": _SEED_LOCK_CLASS, "ident": graph_id},
    )


@dataclass(frozen=True)
class ReconciliationConcepts:
  taxonomy_id: str
  root_id: str
  element_ids: dict[str, str]


def ensure_reconciliation_concepts(
  session: Session, created_by: str
) -> ReconciliationConcepts:
  """The tenant's reconciliation taxonomy and concepts, created on first use.

  The concepts are ``system`` elements, so they never appear in the chart of
  accounts.
  """
  taxonomy = session.execute(
    select(Taxonomy)
    .where(
      Taxonomy.taxonomy_type == "custom_ontology",
      Taxonomy.name == _TAXONOMY_NAME,
      Taxonomy.is_active.is_(True),
    )
    .limit(1)
  ).scalar_one_or_none()
  if taxonomy is None:
    taxonomy = Taxonomy(
      name=_TAXONOMY_NAME,
      description="Concepts for tying the ledger to balances outside it",
      taxonomy_type="custom_ontology",
      is_shared=False,
      is_active=True,
      # Locked: the taxonomy operations refuse to change or delete it, and
      # every reconciliation block hangs off it.
      is_locked=True,
      created_by=created_by,
    )
    session.add(taxonomy)
    session.flush()
  elif not taxonomy.is_locked:
    taxonomy.is_locked = True
    session.flush()

  existing = {
    str(element.qname): str(element.id)
    for element in session.execute(
      select(Element).where(Element.taxonomy_id == taxonomy.id)
    ).scalars()
  }

  def _ensure(qname: str, name: str, *, abstract: bool, monetary: bool) -> str:
    if qname in existing:
      return existing[qname]
    # The code is what the graph builds a system element's qname from.
    element = Element(
      code=qname.split(":", 1)[1],
      name=name,
      qname=qname,
      namespace=_NAMESPACE,
      taxonomy_id=taxonomy.id,
      source="system",
      element_type="concept",
      period_type="instant",
      is_abstract=abstract,
      is_monetary=monetary,
      item_type="monetary" if monetary else "integer",
      created_by=created_by,
    )
    session.add(element)
    session.flush()
    existing[qname] = str(element.id)
    return existing[qname]

  root_id = _ensure(_ROOT, "Reconciliation", abstract=True, monetary=False)
  for qname, name, monetary in _CONCEPTS:
    _ensure(qname, name, abstract=False, monetary=monetary)
  return ReconciliationConcepts(
    taxonomy_id=str(taxonomy.id),
    root_id=root_id,
    element_ids={qname: existing[qname] for qname, _, _ in _CONCEPTS},
  )


def has_reconciliations(session: Session, entity_id: str) -> bool:
  return (
    session.execute(
      select(Structure.id)
      .where(
        Structure.block_type == RECONCILIATION_BLOCK_TYPE,
        Structure.entity_id == entity_id,
      )
      .limit(1)
    ).scalar()
    is not None
  )


def find_ledger_reconciliation(
  session: Session, method: ReconciliationMethod, entity_id: str
) -> Structure | None:
  return session.execute(
    select(Structure)
    .where(
      Structure.block_type == RECONCILIATION_BLOCK_TYPE,
      Structure.entity_id == entity_id,
      Structure.artifact_mechanics["scope"].astext == "ledger",
      Structure.artifact_mechanics["method"].astext == method,
    )
    .order_by(Structure.created_at.asc())
    .limit(1)
  ).scalar_one_or_none()


def ensure_ledger_reconciliation(
  session: Session,
  *,
  method: ReconciliationMethod,
  source: str,
  entity_id: str,
  created_by: str,
) -> Structure:
  """The entity's ledger-scope block for ``method``, created with its concepts
  and its rule on first use. The caller holds `lock_reconciliation_writes`."""
  structure = find_ledger_reconciliation(session, method, entity_id)
  if structure is not None:
    return structure

  source_name = _SOURCE_LEDGER_NAMES.get(source, source)
  return _create_block(
    session,
    mechanics=ReconciliationMechanics(scope="ledger", method=method),
    entity_id=entity_id,
    name=f"Source ledger ({source_name})",
    description=(
      f"The ledger's account balances against {source_name}'s own trial "
      "balance at each period end."
    ),
    presents=_LEDGER_CONCEPTS,
    created_by=created_by,
  )


_ACCOUNT_BLOCK_LABELS = {"schedule_register": "schedules", "statement": "statement"}
_ACCOUNT_BLOCK_DESCRIPTIONS = {
  "schedule_register": (
    "The account's balance against what its schedules say it carries at each "
    "period end."
  ),
  "statement": (
    "The account's balance against the ending balance of its statement each period."
  ),
}


def account_reconciliations(
  session: Session, method: ReconciliationMethod, entity_id: str
) -> dict[str, Structure]:
  """The entity's account-scope blocks for ``method``, by the account they
  reconcile."""
  rows = session.execute(
    select(Structure)
    .where(
      Structure.block_type == RECONCILIATION_BLOCK_TYPE,
      Structure.entity_id == entity_id,
      Structure.is_active.is_(True),
      Structure.artifact_mechanics["scope"].astext == "account",
      Structure.artifact_mechanics["method"].astext == method,
    )
    .order_by(Structure.created_at.asc(), Structure.id.asc())
  ).scalars()
  blocks: dict[str, Structure] = {}
  for structure in rows:
    blocks.setdefault(str(structure.artifact_mechanics.get("element_id")), structure)
  return blocks


def create_account_reconciliation(
  session: Session,
  *,
  method: ReconciliationMethod,
  element_id: str,
  account_name: str,
  entity_id: str,
  required_for_close: bool,
  created_by: str,
) -> Structure:
  """A new account-scope block on the entity's books. The caller holds
  `lock_reconciliation_writes` and has checked that the account has none for
  ``method``."""
  return _create_block(
    session,
    mechanics=ReconciliationMechanics(
      scope="account",
      method=method,
      element_id=element_id,
      required_for_close=required_for_close,
    ),
    entity_id=entity_id,
    name=f"{account_name} ({_ACCOUNT_BLOCK_LABELS[method]})",
    description=_ACCOUNT_BLOCK_DESCRIPTIONS[method],
    presents=_ACCOUNT_CONCEPTS,
    created_by=created_by,
  )


def _create_block(
  session: Session,
  *,
  mechanics: ReconciliationMechanics,
  entity_id: str,
  name: str,
  description: str,
  presents: tuple[str, ...],
  created_by: str,
) -> Structure:
  """A reconciliation Structure with its presentation arcs and its rule."""
  concepts = ensure_reconciliation_concepts(session, created_by)
  structure = Structure(
    name=name,
    description=description,
    block_type=RECONCILIATION_BLOCK_TYPE,
    entity_id=entity_id,
    taxonomy_id=concepts.taxonomy_id,
    concept_arrangement="set",
    member_arrangement=None,
    artifact_mechanics=mechanics.model_dump(mode="json"),
    metadata_={},
    created_by=created_by,
  )
  session.add(structure)
  session.flush()

  for order, qname in enumerate(presents, 1):
    session.add(
      Association(
        structure_id=structure.id,
        from_element_id=concepts.root_id,
        to_element_id=concepts.element_ids[qname],
        association_type="presentation",
        order_value=float(order),
        created_by=created_by,
      )
    )
  session.add(
    Rule(
      taxonomy_id=concepts.taxonomy_id,
      rule_category="AutomatedAccountingAndReportingChecks",
      rule_pattern="EqualTo",
      rule_expression="$UnreconciledDifference = 0",
      rule_message="The ledger does not tie to the independent balance.",
      rule_severity="error",
      rule_origin="native",
      target_kind="structure",
      target_structure_id=structure.id,
      rule_variables=[
        {
          "variable_name": "UnreconciledDifference",
          "variable_qname": UNRECONCILED_DIFFERENCE,
          "variable_element_id": concepts.element_ids[UNRECONCILED_DIFFERENCE],
        }
      ],
      metadata_={"tolerance": mechanics.materiality},
      created_by=created_by,
    )
  )
  session.flush()
  return structure


def reconciliation_rule(session: Session, structure_id: str) -> Rule | None:
  rule_id = session.execute(select(own_rule_id(structure_id))).scalar()
  return session.get(Rule, rule_id) if rule_id else None


def balance_digest(comparison: ReconciliationPreviewResponse) -> str:
  """A fingerprint of every compared balance, both sides, to the cent.

  Two comparisons with the same digest saw the same figures. A sign-off pins
  it, so any later change to a balance at that period end lapses the review.
  A statement's document is part of what was compared: one recorded with
  another document lapses it too, and so does a change to the lines left
  outstanding or to the carried period-end balance. A row with none of them
  fingerprints as it always has, so no standing sign-off lapses by these
  rules' arrival.
  """
  lines = sorted(
    f"{row.element_id or ''}|{row.source_account_id or ''}|"
    f"{round(row.ledger_balance * 100)}|{round(row.independent_balance * 100)}"
    + (f"|{row.as_of.isoformat()}" if row.as_of else "")
    + "".join(f"|doc:{c.document_id}" for c in row.components if c.document_id)
    + "".join(
      f"|out:{c.entry_id}:{round(c.amount * 100)}"
      for c in row.components
      if c.kind == "outstanding"
    )
    + (
      f"|carried:{round(row.roll_forward.bank_balance * 100)}"
      if row.roll_forward is not None
      else ""
    )
    for row in comparison.rows
  )
  return hashlib.sha256("\n".join(lines).encode()).hexdigest()[:32]


def account_comparison(
  comparison: ReconciliationPreviewResponse, row: ReconciliationRow
) -> ReconciliationPreviewResponse:
  """One account's part of an account-scope comparison, as its block records it."""
  tied = row.status == "tied"
  return comparison.model_copy(
    update={
      "accounts_compared": 1,
      "accounts_tied": 1 if tied else 0,
      "accounts_different": 0 if tied else 1,
      "total_difference": row.difference,
      "rows": [row],
    }
  )


def record_reconciliation(
  session: Session,
  structure: Structure,
  *,
  window: ReconciliationWindow,
  side: IndependentSide,
  comparison: ReconciliationPreviewResponse,
  created_by: str,
  compared_via: ComparedVia = "operation",
) -> VerificationResult | None:
  """Replace the period's standing set with this comparison and evaluate the
  block's rule against it. Flushes; the caller owns the commit.

  A ledger-scope block records the whole comparison; an account-scope one
  records its account's part (`account_comparison`). ``comparison`` must
  carry every row, tied ones included: the digest is taken over all of them.
  Raises ``RuntimeError`` when it does not.
  """
  if len(comparison.rows) != comparison.accounts_compared:
    raise RuntimeError(
      "A reconciliation is recorded from every compared row, tied ones "
      f"included; got {len(comparison.rows)} of {comparison.accounts_compared}."
    )
  account_scope = (structure.artifact_mechanics or {}).get("scope") == "account"
  concepts = ensure_reconciliation_concepts(session, created_by)
  entity_id = owner_entity_id(session, structure)
  provenance = ObservedProvenance(
    source=side.source,
    method=side.method,
    as_of=window.period_end.isoformat(),
    observed_at=datetime.now(UTC).isoformat(),
    connection_id=side.connection_id,
    basis=side.basis,
  )
  differences = [row for row in comparison.rows if row.status != "tied"]
  metadata = {
    "differences": [
      row.model_dump(mode="json") for row in differences[:MAX_STORED_DIFFERENCES]
    ],
    "differences_truncated": len(differences) > MAX_STORED_DIFFERENCES,
    "accounts_tied": comparison.accounts_tied,
    "notes": comparison.notes,
    "balance_digest": balance_digest(comparison),
    "compared_by": created_by,
    "compared_via": compared_via,
  }
  if not account_scope:
    if comparison._ledger_digest is None:
      # It rides on a private attribute, which a dump and re-validate drops.
      raise RuntimeError(
        "A ledger-scope comparison must carry its ledger fingerprint; record "
        "the object compute_reconciliations returned."
      )
    metadata["ledger_digest"] = comparison._ledger_digest
  if account_scope:
    (row,) = comparison.rows
    metadata["components"] = [
      component.model_dump(mode="json") for component in row.components
    ]
    metadata["balance_as_of"] = (row.as_of or window.period_end).isoformat()
    metadata["prepared_by"] = side.prepared_by.get(str(row.element_id))
    if row.roll_forward is not None:
      metadata["roll_forward"] = row.roll_forward.model_dump(mode="json")

  standing = session.execute(
    select(FactSet)
    .where(
      FactSet.structure_id == structure.id,
      FactSet.factset_type == RECONCILIATION_FACTSET_TYPE,
      FactSet.entity_id == entity_id,
      FactSet.period_end == window.period_end,
      FactSet.scenario_id.is_(None),
    )
    .order_by(FactSet.created_at.desc())
    .limit(1)
  ).scalar_one_or_none()
  if standing is not None and compared_via != "operation":
    # A sync recomputing the same figures does not unmake whoever ran them by
    # hand: they still prepared what a reviewer signs.
    prior = standing.metadata_ or {}
    if prior.get("balance_digest") == metadata["balance_digest"]:
      hand = (
        prior.get("compared_by")
        if prior.get("compared_via") == "operation"
        else prior.get("prepared_by_hand")
      )
      if hand:
        metadata["prepared_by_hand"] = hand
  if standing is None:
    standing = create_fact_set(
      session,
      structure_id=structure.id,
      period_end=window.period_end,
      factset_type=RECONCILIATION_FACTSET_TYPE,
      entity_id=entity_id,
      provenance=provenance,
      metadata=metadata,
      created_by=created_by,
    )
    session.flush()
  else:
    # Full replace: the period's comparison is recomputed state, and the
    # result it replaces described the facts being removed.
    session.query(VerificationResult).filter(
      VerificationResult.fact_set_id == standing.id
    ).delete(synchronize_session=False)
    session.query(Fact).filter(Fact.fact_set_id == standing.id).delete(
      synchronize_session=False
    )
    standing.provenance = provenance.model_dump(mode="json")
    standing.metadata_ = metadata

  if account_scope:
    (row,) = comparison.rows
    values = {
      LEDGER_BALANCE: (row.ledger_balance, "USD"),
      INDEPENDENT_BALANCE: (row.independent_balance, "USD"),
      UNRECONCILED_DIFFERENCE: (row.difference, "USD"),
    }
  else:
    values = {
      UNRECONCILED_DIFFERENCE: (comparison.total_difference, "USD"),
      ACCOUNTS_COMPARED: (float(comparison.accounts_compared), "pure"),
      ACCOUNTS_DIFFERENT: (float(comparison.accounts_different), "pure"),
    }
  for qname, (value, unit) in values.items():
    session.add(
      Fact(
        element_id=concepts.element_ids[qname],
        value=value,
        fact_type="Numeric",
        period_start=None,
        period_end=window.period_end,
        period_type="instant",
        unit=unit,
        entity_id=entity_id,
        structure_id=structure.id,
        fact_set_id=standing.id,
      )
    )
  session.flush()

  results = evaluate_rules_for_structure(
    session,
    str(structure.id),
    fact_set_id=str(standing.id),
    period_end=window.period_end,
    created_by=created_by,
  )
  own = reconciliation_rule(session, str(structure.id))
  return next((r for r in results if own is not None and r.rule_id == own.id), None)


def standing_sign_offs(
  session: Session, structure_ids: list[str], period: str
) -> dict[str, list[Event]]:
  """Each block's live sign-offs for the period, latest first, by structure id."""
  if not structure_ids:
    return {}
  rows = session.execute(
    select(Event)
    .where(
      Event.event_type == SIGN_OFF_EVENT_TYPE,
      Event.event_category == "approval",
      Event.status == "committed",
      Event.metadata_["period"].astext == period,
      Event.metadata_["structure_id"].astext.in_(structure_ids),
    )
    .order_by(Event.occurred_at.desc(), Event.id.desc())
  ).scalars()
  sign_offs: dict[str, list[Event]] = {}
  for event in rows:
    structure_id = str((event.metadata_ or {}).get("structure_id"))
    sign_offs.setdefault(structure_id, []).append(event)
  return sign_offs


POLICY_CHANGE_EVENT_TYPE = "reconciliation_policy_changed"


def preparers(comparison_metadata: dict) -> set[str]:
  """The people a comparison's figures came from: whoever ran it by hand, and
  whoever supplied its independent balance. A sync-run comparison has no
  person behind the run."""
  people = set()
  if comparison_metadata.get("compared_via") == "operation":
    people.add(comparison_metadata.get("compared_by"))
  people.add(comparison_metadata.get("prepared_by"))
  people.add(comparison_metadata.get("prepared_by_hand"))
  return {str(person) for person in people if person}


def record_policy_change(
  session: Session,
  *,
  structure: Structure,
  changes: dict[str, dict],
  changed_by: str,
) -> Event:
  """A change to a block's policy as a ``control`` support event, so loosening
  what the close waits on leaves a record of who did it and what it was."""
  now = datetime.now(UTC)
  event = Event(
    entity_id=owner_entity_id(session, structure),
    event_type=POLICY_CHANGE_EVENT_TYPE,
    event_category="control",
    event_class="support",
    occurred_at=now,
    status="committed",
    source="manual",
    description=f"Reconciliation policy changed: {structure.name}",
    metadata_={"structure_id": str(structure.id), "changes": changes},
    created_by=changed_by,
  )
  session.add(event)
  session.flush()
  return event


def record_sign_off(
  session: Session,
  *,
  structure: Structure,
  window: ReconciliationWindow,
  fact_set: FactSet,
  reviewer_id: str,
  note: str | None,
) -> Event:
  """The review as an ``approval`` support event, pinned to the comparison it
  approved. It writes no books: the status is terminal from the start, so the
  inbox and the unposted-event gate never see it.
  """
  compared = fact_set.metadata_ or {}
  now = datetime.now(UTC)
  event = Event(
    entity_id=owner_entity_id(session, structure),
    event_type=SIGN_OFF_EVENT_TYPE,
    event_category="approval",
    event_class="support",
    occurred_at=now,
    effective_at=datetime.combine(window.period_end, datetime.min.time()),
    status="committed",
    source="manual",
    description=f"Reviewed: {structure.name}, {window.period}",
    metadata_={
      "structure_id": str(structure.id),
      "period": window.period,
      "fact_set_id": str(fact_set.id),
      "balance_digest": compared.get("balance_digest"),
      "compared_by": compared.get("compared_by"),
      "compared_via": compared.get("compared_via"),
      "prepared_by": compared.get("prepared_by"),
      "self_reviewed": reviewer_id in preparers(compared),
      "note": note,
    },
    created_by=reviewer_id,
  )
  session.add(event)
  session.flush()
  return event
