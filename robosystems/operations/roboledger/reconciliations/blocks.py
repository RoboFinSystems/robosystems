"""Reconciliation blocks: the Structure, its concepts and rule, and one
standing FactSet per period.

Nothing here is a new table. The block is a ``reconciliation`` Structure, the
period's comparison is a FactSet under ``observed`` provenance, and whether it
reconciles is the ``VerificationResult`` of one rule on the block.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from robosystems.models.api.extensions.reconciliations import (
  ReconciliationMethod,
  ReconciliationPreviewResponse,
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
from robosystems.models.extensions.roboledger import Fact, FactSet
from robosystems.operations.information_block.reconciliation import (
  RECONCILIATION_BLOCK_TYPE,
  RECONCILIATION_FACTSET_TYPE,
)
from robosystems.operations.information_block.rules.engine import (
  evaluate_rules_for_structure,
)
from robosystems.operations.locking import bounded_lock_wait
from robosystems.operations.roboledger.fact_set import create_fact_set

from .resolvers import IndependentSide, ReconciliationWindow

_TAXONOMY_NAME = "Reconciliations"
_NAMESPACE = "rs-rec"
UNRECONCILED_DIFFERENCE = f"{_NAMESPACE}:UnreconciledDifference"
ACCOUNTS_COMPARED = f"{_NAMESPACE}:AccountsCompared"
ACCOUNTS_DIFFERENT = f"{_NAMESPACE}:AccountsDifferent"
_ROOT = f"{_NAMESPACE}:ReconciliationAbstract"

# (qname, name, monetary) in presentation order.
_CONCEPTS: tuple[tuple[str, str, bool], ...] = (
  (UNRECONCILED_DIFFERENCE, "Unreconciled difference", True),
  (ACCOUNTS_COMPARED, "Accounts compared", False),
  (ACCOUNTS_DIFFERENT, "Accounts that do not tie", False),
)

# The differing accounts kept on the period's FactSet; the counts stay exact.
MAX_STORED_DIFFERENCES = 100

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
      is_locked=False,
      created_by=created_by,
    )
    session.add(taxonomy)
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


def find_ledger_reconciliation(
  session: Session, method: ReconciliationMethod
) -> Structure | None:
  return session.execute(
    select(Structure)
    .where(
      Structure.block_type == RECONCILIATION_BLOCK_TYPE,
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
  created_by: str,
) -> Structure:
  """The ledger-scope block for ``method``, created with its concepts and its
  rule on first use. The caller holds `lock_reconciliation_writes`."""
  structure = find_ledger_reconciliation(session, method)
  if structure is not None:
    return structure

  concepts = ensure_reconciliation_concepts(session, created_by)
  mechanics = ReconciliationMechanics(scope="ledger", method=method)
  source_name = _SOURCE_LEDGER_NAMES.get(source, source)
  structure = Structure(
    name=f"Source ledger ({source_name})",
    description=(
      f"The ledger's account balances against {source_name}'s own trial "
      "balance at each period end."
    ),
    block_type=RECONCILIATION_BLOCK_TYPE,
    taxonomy_id=concepts.taxonomy_id,
    concept_arrangement="set",
    member_arrangement=None,
    artifact_mechanics=mechanics.model_dump(mode="json"),
    metadata_={},
    created_by=created_by,
  )
  session.add(structure)
  session.flush()

  for order, (qname, _, _) in enumerate(_CONCEPTS, 1):
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
  return session.execute(
    select(Rule)
    .where(Rule.target_structure_id == structure_id, Rule.rule_pattern == "EqualTo")
    .limit(1)
  ).scalar_one_or_none()


def _entity_id(session: Session) -> str:
  row = session.execute(
    text("SELECT id FROM entities ORDER BY created_at ASC LIMIT 1")
  ).fetchone()
  if row is None:
    raise ValueError("No entity exists on this graph; initialize the ledger first.")
  return str(row.id)


def record_ledger_reconciliation(
  session: Session,
  structure: Structure,
  *,
  window: ReconciliationWindow,
  side: IndependentSide,
  comparison: ReconciliationPreviewResponse,
  created_by: str,
) -> VerificationResult | None:
  """Replace the period's standing set with this comparison and evaluate the
  block's rule against it. Flushes; the caller owns the commit.
  """
  concepts = ensure_reconciliation_concepts(session, created_by)
  entity_id = _entity_id(session)
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
  }

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
  return results[0] if results else None
