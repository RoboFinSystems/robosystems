"""Shared loaders and ORM → wire-shape projections for Information Block
envelope builders."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from robosystems.models.api.information_block import (
  ClassificationLite,
  ConnectionLite,
  ElementLite,
  FactLite,
  FactSetLite,
  RuleLite,
  RuleTargetLite,
  RuleVariableLite,
  VerificationCategorySummary,
  VerificationResultLite,
  VerificationSummary,
)
from robosystems.models.extensions import (
  Association,
  AssociationClassification,
  Classification,
  Element,
  ElementLabel,
  Rule,
  Structure,
  VerificationResult,
)
from robosystems.models.extensions.roboledger import Fact, FactSet

# Defined here, the lowest module in the statement/disclosure import cluster.
DISCLOSURE_BLOCK_TYPE = "regulatory_disclosure"


def element_to_lite(element: Element, documentation: str | None = None) -> ElementLite:
  """Project an :class:`Element` ORM row onto :class:`ElementLite`."""
  return ElementLite(
    id=element.id,
    qname=element.qname,
    name=element.name,
    code=element.code,
    element_type=element.element_type,
    is_abstract=element.is_abstract,
    is_monetary=element.is_monetary,
    balance_type=element.balance_type,
    period_type=element.period_type,
    item_type=element.item_type,
    documentation=documentation,
  )


def load_documentation_for_elements(
  session: Session, element_ids: list[str]
) -> dict[str, str]:
  """Documentation-role label text keyed by element id; elements without one
  are absent."""
  if not element_ids:
    return {}
  rows = session.execute(
    select(ElementLabel.element_id, ElementLabel.text).where(
      ElementLabel.element_id.in_(element_ids),
      ElementLabel.role == "documentation",
    )
  ).all()
  return dict(rows)


def elements_to_lites(session: Session, elements: list[Element]) -> list[ElementLite]:
  """Project elements with their documentation labels (one batched query)."""
  docs = load_documentation_for_elements(session, [e.id for e in elements])
  return [element_to_lite(e, documentation=docs.get(e.id)) for e in elements]


def association_to_connection(
  association: Association,
  classifications: list[ClassificationLite] | None = None,
) -> ConnectionLite:
  """Project an :class:`Association` ORM row onto :class:`ConnectionLite`.

  Pass ``classifications`` from :func:`load_classifications_for_associations`
  (loaded once per envelope); omitted means an empty list.
  """
  return ConnectionLite(
    id=association.id,
    from_element_id=association.from_element_id,
    to_element_id=association.to_element_id,
    association_type=association.association_type,
    arcrole=association.arcrole,
    order_value=association.order_value,
    weight=association.weight,
    classifications=classifications or [],
  )


def load_classifications_for_associations(
  session: Session, association_ids: list[str]
) -> dict[str, list[ClassificationLite]]:
  """Fetch association classifications keyed by association id, primary first."""
  if not association_ids:
    return {}

  rows = session.execute(
    select(AssociationClassification, Classification)
    .join(
      Classification,
      Classification.id == AssociationClassification.classification_id,
    )
    .where(AssociationClassification.association_id.in_(association_ids))
    .order_by(
      AssociationClassification.association_id,
      AssociationClassification.is_primary.desc(),
      Classification.category,
      Classification.identifier,
    )
  ).all()

  grouped: dict[str, list[ClassificationLite]] = {}
  for ac, cls in rows:
    grouped.setdefault(ac.association_id, []).append(
      ClassificationLite(
        id=cls.id,
        category=cls.category,
        identifier=cls.identifier,
        is_primary=bool(ac.is_primary),
        confidence=ac.confidence,
        source=ac.source,
      )
    )
  return grouped


def fact_to_lite(
  fact: Fact,
  elements_by_id: dict[str, Element] | None = None,
) -> FactLite:
  """Project a :class:`Fact` ORM row onto :class:`FactLite`, denormalizing the
  element's name and qname when ``elements_by_id`` is supplied."""
  element = elements_by_id.get(fact.element_id) if elements_by_id else None
  return FactLite(
    id=fact.id,
    element_id=fact.element_id,
    element_name=element.name if element else None,
    element_qname=element.qname if element else None,
    value=fact.value,
    text_value=fact.string_value,
    fact_type=fact.fact_type,
    content_type=fact.content_type,
    period_start=fact.period_start,
    period_end=fact.period_end,
    period_type=fact.period_type,
    unit=fact.unit,
    fact_scope=fact.fact_scope,
    fact_set_id=fact.fact_set_id,
  )


def fact_set_to_lite(fact_set: FactSet) -> FactSetLite:
  """Project a :class:`FactSet` ORM row onto :class:`FactSetLite`."""
  return FactSetLite(
    id=fact_set.id,
    structure_id=fact_set.structure_id,
    period_start=fact_set.period_start,
    period_end=fact_set.period_end,
    factset_type=fact_set.factset_type,
    entity_id=fact_set.entity_id,
    report_id=fact_set.report_id,
    scenario_id=fact_set.scenario_id,
    provenance=fact_set.provenance,
  )


def verification_result_to_lite(row: VerificationResult) -> VerificationResultLite:
  """Project a :class:`VerificationResult` ORM row onto
  :class:`VerificationResultLite`."""
  return VerificationResultLite(
    id=row.id,
    rule_id=row.rule_id,
    structure_id=row.structure_id,
    fact_set_id=row.fact_set_id,
    status=row.status,
    message=row.message,
    period_start=row.period_start,
    period_end=row.period_end,
    evaluated_at=row.evaluated_at,
  )


def load_verification_results_for_structure(
  session: Session, structure_id: str, scenario_id: str | None = None
) -> list[VerificationResultLite]:
  """Verification results for a Structure, newest first, scoped to a slice.

  compute-forecast verifies scenario months against the statement
  structures, so results pinned to a set are filtered by that set's
  scenario (``None`` = actuals). Unpinned results always appear.
  """
  scenario_predicate = (
    FactSet.scenario_id.is_(None)
    if scenario_id is None
    else FactSet.scenario_id == scenario_id
  )
  rows = (
    session.execute(
      select(VerificationResult)
      .outerjoin(FactSet, VerificationResult.fact_set_id == FactSet.id)
      .where(
        VerificationResult.structure_id == structure_id,
        or_(VerificationResult.fact_set_id.is_(None), scenario_predicate),
      )
      .order_by(VerificationResult.evaluated_at.desc())
    )
    .scalars()
    .all()
  )
  return [verification_result_to_lite(r) for r in rows]


# Maps the ``verification_results.status`` enum onto the summary count field.
_STATUS_TO_FIELD: dict[str, str] = {
  "pass": "passed",
  "fail": "failed",
  "error": "errored",
  "skipped": "skipped",
}


def build_verification_summary(
  verification_results: list[VerificationResultLite],
  rules: list[RuleLite],
) -> VerificationSummary | None:
  """Aggregate verification results into overall + per-category counts
  (category via the result's rule). ``None`` when there are no results."""
  if not verification_results:
    return None

  category_by_rule = {r.id: r.rule_category for r in rules}
  overall = {"passed": 0, "failed": 0, "errored": 0, "skipped": 0}
  per_category: dict[str, dict[str, int]] = {}

  for vr in verification_results:
    field = _STATUS_TO_FIELD.get(vr.status)
    if field is None:
      continue
    overall[field] += 1
    category = category_by_rule.get(vr.rule_id) or "Uncategorized"
    bucket = per_category.setdefault(
      category, {"passed": 0, "failed": 0, "errored": 0, "skipped": 0}
    )
    bucket[field] += 1

  by_category = [
    VerificationCategorySummary(
      category=category,
      total=sum(counts.values()),
      passed=counts["passed"],
      failed=counts["failed"],
      errored=counts["errored"],
      skipped=counts["skipped"],
    )
    for category, counts in sorted(per_category.items())
  ]
  return VerificationSummary(
    total=sum(overall.values()),
    passed=overall["passed"],
    failed=overall["failed"],
    errored=overall["errored"],
    skipped=overall["skipped"],
    by_category=by_category,
  )


def load_latest_fact_set_for_structure(
  session: Session, structure_id: str, scenario_id: str | None = None
) -> FactSetLite | None:
  """Fetch the latest FactSet (by ``period_end``) for a Structure, if any.

  ``scenario_id=None`` pins actuals; this is load-bearing, since scenario
  sets sit at future period_ends and would otherwise win. At equal
  ``period_end``, canonical sets (``report_id IS NULL``) beat publication
  snapshots, then newest first.
  """
  row = session.execute(
    select(FactSet)
    .where(
      FactSet.structure_id == structure_id,
      FactSet.scenario_id.is_(None)
      if scenario_id is None
      else FactSet.scenario_id == scenario_id,
    )
    .order_by(
      FactSet.period_end.desc(),
      FactSet.report_id.isnot(None).asc(),
      FactSet.created_at.desc(),
    )
    .limit(1)
  ).scalar()
  return fact_set_to_lite(row) if row is not None else None


def load_statement_fact_set_series(
  session: Session, structure_id: str, scenario_id: str | None = None
) -> list[FactSetLite]:
  """The report-set series for a statement structure, one per period_end,
  ascending.

  ``scenario_id=None`` loads actuals; a scenario id adds that scenario's
  sets. Sets whose window strictly contains another's (an annual over its
  months) are dropped first. Then per ``period_end``: actual beats
  forecast, canonical beats publication snapshot, narrower window beats
  wider, newest wins.
  """
  scenario_predicate = (
    FactSet.scenario_id.is_(None)
    if scenario_id is None
    else or_(FactSet.scenario_id.is_(None), FactSet.scenario_id == scenario_id)
  )
  rows = (
    session.execute(
      select(FactSet)
      .where(
        FactSet.structure_id == structure_id,
        FactSet.factset_type == "report",
        scenario_predicate,
      )
      .order_by(
        FactSet.period_end.asc(),
        FactSet.scenario_id.asc().nulls_first(),
        FactSet.report_id.isnot(None).asc(),
        FactSet.period_start.desc().nulls_last(),
        FactSet.created_at.desc(),
      )
    )
    .scalars()
    .all()
  )
  rows = _drop_covering_windows(rows)
  by_period_end: dict[date, FactSet] = {}
  for row in rows:
    by_period_end.setdefault(row.period_end, row)
  return [fact_set_to_lite(row) for row in by_period_end.values()]


def window_series_sets(
  series_sets: list[FactSetLite],
  history: int | None,
  forecast: int | None,
) -> list[FactSetLite]:
  """Trim a collapsed statement series to its seam-adjacent window.

  ``history`` keeps the last N actual columns, ``forecast`` the first N
  forecast columns; ``None`` leaves that side unbounded. Counts are
  columns, not calendar months.
  """
  if history is None and forecast is None:
    return series_sets
  if (history is not None and history < 0) or (forecast is not None and forecast < 0):
    raise ValueError("series window counts must be >= 0")
  actuals = [fs for fs in series_sets if fs.scenario_id is None]
  forecasts = [fs for fs in series_sets if fs.scenario_id is not None]
  if history is not None:
    actuals = actuals[-history:] if history > 0 else []
  if forecast is not None:
    forecasts = forecasts[:forecast]
  keep = {fs.id for fs in actuals} | {fs.id for fs in forecasts}
  return [fs for fs in series_sets if fs.id in keep]


def window_month_axis(
  months: list[str],
  forecast_months: set[str],
  history: int | None,
  forecast: int | None,
) -> list[str]:
  """:func:`window_series_sets` for a chronological ``YYYY-MM`` axis."""
  if history is None and forecast is None:
    return months
  if (history is not None and history < 0) or (forecast is not None and forecast < 0):
    raise ValueError("series window counts must be >= 0")
  actuals = [m for m in months if m not in forecast_months]
  forwards = [m for m in months if m in forecast_months]
  if history is not None:
    actuals = actuals[-history:] if history > 0 else []
  if forecast is not None:
    forwards = forwards[:forecast]
  keep = set(actuals) | set(forwards)
  return [m for m in months if m in keep]


def _drop_covering_windows(rows: list) -> list:
  """Drop sets whose window strictly contains another loaded set's window.

  Identical windows are left to the per-period_end collapse; sets without a
  ``period_start`` neither cover nor get covered.
  """
  windowed = [r for r in rows if r.period_start is not None]
  kept = []
  for candidate in rows:
    if candidate.period_start is None:
      kept.append(candidate)
      continue
    covered_narrower = any(
      other is not candidate
      and candidate.period_start <= other.period_start
      and other.period_end <= candidate.period_end
      and (other.period_start, other.period_end)
      != (candidate.period_start, candidate.period_end)
      for other in windowed
    )
    if not covered_narrower:
      kept.append(candidate)
  return kept


def load_fact_set_by_id_for_structure(
  session: Session, structure_id: str, fact_set_id: str
) -> FactSetLite | None:
  """Fetch a specific FactSet, or ``None`` if it doesn't belong to the
  Structure."""
  row = session.execute(
    select(FactSet).where(
      FactSet.id == fact_set_id, FactSet.structure_id == structure_id
    )
  ).scalar()
  return fact_set_to_lite(row) if row is not None else None


def rule_to_lite(rule: Rule) -> RuleLite:
  """Project a :class:`Rule` ORM row onto :class:`RuleLite`."""
  target: RuleTargetLite | None = None
  if rule.target_kind == "structure" and rule.target_structure_id is not None:
    target = RuleTargetLite(
      target_kind="structure", target_ref_id=rule.target_structure_id
    )
  elif rule.target_kind == "element" and rule.target_element_id is not None:
    target = RuleTargetLite(target_kind="element", target_ref_id=rule.target_element_id)
  elif rule.target_kind == "association" and rule.target_association_id is not None:
    target = RuleTargetLite(
      target_kind="association", target_ref_id=rule.target_association_id
    )
  elif rule.target_kind == "taxonomy" and rule.target_taxonomy_id is not None:
    target = RuleTargetLite(
      target_kind="taxonomy", target_ref_id=rule.target_taxonomy_id
    )

  raw_vars = rule.rule_variables or []
  variables = [
    RuleVariableLite(
      variable_name=v.get("variable_name", ""),
      variable_qname=v.get("variable_qname"),
      variable_element_id=v.get("variable_element_id"),
    )
    for v in raw_vars
  ]

  return RuleLite(
    id=rule.id,
    rule_category=rule.rule_category,
    rule_pattern=rule.rule_pattern,
    rule_check_kind=rule.rule_check_kind,
    rule_expression=rule.rule_expression,
    rule_target=target,
    rule_variables=variables,
    rule_message=rule.rule_message,
    rule_severity=rule.rule_severity,
    rule_origin=rule.rule_origin,
  )


def load_rules_for_structure(
  session: Session,
  structure_id: str,
  element_ids: list[str] | None = None,
  association_ids: list[str] | None = None,
) -> list[RuleLite]:
  """Fetch every rule targeting the Structure or one of the given elements or
  associations, ordered by category then id."""
  conditions = [Rule.target_structure_id == structure_id]
  if element_ids:
    conditions.append(Rule.target_element_id.in_(element_ids))
  if association_ids:
    conditions.append(Rule.target_association_id.in_(association_ids))

  rules = (
    session.execute(
      select(Rule).where(or_(*conditions)).order_by(Rule.rule_category, Rule.id)
    )
    .scalars()
    .all()
  )
  return [rule_to_lite(r) for r in rules]


@dataclass(frozen=True)
class BaseEnvelopeAtoms:
  """Atoms shared by every block-type ``build_envelope``."""

  structure: Structure
  taxonomy_name: str | None
  associations: list[Association]
  elements: list[Element]
  element_ids: list[str]
  rules: list[RuleLite]
  classifications_by_assoc: dict[str, list[ClassificationLite]]
  fact_set: FactSetLite | None
  verification_results: list[VerificationResultLite]
  verification_summary: VerificationSummary | None


def load_base_envelope_atoms(
  session: Session,
  structure_id: str,
  *,
  expected_block_type: str,
  fact_set_id: str | None = None,
  scenario_id: str | None = None,
) -> BaseEnvelopeAtoms | None:
  """Load every atom shared by Information Block envelope builders.

  ``None`` when the Structure is missing, isn't ``expected_block_type``, or
  a ``fact_set_id`` pin doesn't belong to it. A pin overrides
  ``scenario_id``; without one, ``scenario_id`` selects the slice (``None``
  = actuals).
  """
  from robosystems.models.extensions import Taxonomy

  structure = session.get(Structure, structure_id)
  if structure is None or structure.block_type != expected_block_type:
    return None

  if fact_set_id is not None:
    fact_set = load_fact_set_by_id_for_structure(session, structure_id, fact_set_id)
    if fact_set is None:
      return None
  else:
    fact_set = load_latest_fact_set_for_structure(session, structure_id, scenario_id)

  taxonomy_name = session.execute(
    select(Taxonomy.name).where(Taxonomy.id == structure.taxonomy_id)
  ).scalar()

  associations = list(
    session.execute(select(Association).where(Association.structure_id == structure_id))
    .scalars()
    .all()
  )

  element_id_set = {a.from_element_id for a in associations} | {
    a.to_element_id for a in associations
  }
  element_ids = [e for e in element_id_set if e is not None]
  if element_ids:
    elements = list(
      session.execute(select(Element).where(Element.id.in_(element_ids)))
      .scalars()
      .all()
    )
  else:
    elements = []

  rules = load_rules_for_structure(
    session,
    structure_id,
    element_ids=element_ids,
    association_ids=[a.id for a in associations],
  )

  classifications_by_assoc = load_classifications_for_associations(
    session, [a.id for a in associations]
  )

  # A pinned set scopes results by its own scenario.
  verification_results = load_verification_results_for_structure(
    session,
    structure_id,
    scenario_id=fact_set.scenario_id if fact_set is not None else scenario_id,
  )

  return BaseEnvelopeAtoms(
    structure=structure,
    taxonomy_name=taxonomy_name,
    associations=associations,
    elements=elements,
    element_ids=element_ids,
    rules=rules,
    classifications_by_assoc=classifications_by_assoc,
    fact_set=fact_set,
    verification_results=verification_results,
    verification_summary=build_verification_summary(verification_results, rules),
  )


_DISCLOSURE_ROLE_PREFIX = "https://robosystems.ai/seattle/cm-roles/roles/disclosures/"

# block_type → disclosure qname, cached for the process (disclosure rows are
# library-seeded and immutable). Misses cache None.
_DISCLOSURE_QNAME_BY_TYPE: dict[str, str | None] = {}
_MISSING: object = object()


def _structure_role_uri(structure: Structure) -> str | None:
  """Read ``role_uri`` from ``metadata_`` (it isn't a column)."""
  metadata = structure.metadata_ or {}
  value = metadata.get("role_uri")
  return value if isinstance(value, str) else None


def _disclosure_qname_for_type(session: Session, block_type: str) -> str | None:
  """Memoized lookup: block_type -> disclosures:<Name> qname.

  Only meaningful for the statement family, where the mapping is 1:1;
  every note shares ``regulatory_disclosure``, so it returns None there.
  """
  if block_type == DISCLOSURE_BLOCK_TYPE:
    return None

  cached = _DISCLOSURE_QNAME_BY_TYPE.get(block_type, _MISSING)
  if cached is not _MISSING:
    return cached  # type: ignore[return-value]

  role_uri = session.execute(
    select(Structure.metadata_["role_uri"].astext)
    .where(
      Structure.block_type == block_type,
      Structure.metadata_["role_uri"].astext.like(f"{_DISCLOSURE_ROLE_PREFIX}%"),
    )
    .limit(1)
  ).scalar()

  qname: str | None
  if role_uri is None:
    qname = None
  else:
    qname = f"disclosures:{role_uri[len(_DISCLOSURE_ROLE_PREFIX) :]}"

  _DISCLOSURE_QNAME_BY_TYPE[block_type] = qname
  return qname


def load_disclosure_id_for_structure(session: Session, structure_id: str) -> str | None:
  """Return the ``disclosures:<Name>`` qname this structure corresponds to:
  from its own role_uri when it is a disclosure-namespace structure, else
  by its statement block_type."""
  target = session.get(Structure, structure_id)
  if target is None:
    return None

  role_uri = _structure_role_uri(target)
  if not role_uri:
    return None

  if role_uri.startswith(_DISCLOSURE_ROLE_PREFIX):
    return f"disclosures:{role_uri[len(_DISCLOSURE_ROLE_PREFIX) :]}"

  if target.block_type is None:
    return None

  return _disclosure_qname_for_type(session, target.block_type)


__all__ = [
  "BaseEnvelopeAtoms",
  "association_to_connection",
  "build_verification_summary",
  "element_to_lite",
  "elements_to_lites",
  "fact_set_to_lite",
  "fact_to_lite",
  "load_base_envelope_atoms",
  "load_classifications_for_associations",
  "load_disclosure_id_for_structure",
  "load_documentation_for_elements",
  "load_fact_set_by_id_for_structure",
  "load_latest_fact_set_for_structure",
  "load_rules_for_structure",
  "load_verification_results_for_structure",
  "rule_to_lite",
  "verification_result_to_lite",
]
