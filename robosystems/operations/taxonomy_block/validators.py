"""Validator pipeline for Taxonomy Block create payloads.

Runs before any handler writes. Collects every issue across all
checks and surfaces them as a single
:class:`TaxonomyBlockValidationError` — tenants see every problem in
one response, not the first-fail shape the inline handler checks
produced.

Checks (see ``local/docs/specs/taxonomy-block.md`` §3.1):

* Reference resolution — parent_ref / structure_ref / from_ref /
  to_ref / rule targets all resolve locally or (for
  ``reporting_extension``) in the parent library.
* Uniqueness — element qnames and structure names unique within the
  envelope.
* Structural integrity — no cycles in parent_child or summation_item
  arcs.
* Type-specific — CoA classification discipline; extension namespace
  prefix.
* Library-origin respect — tenants can't smuggle library-origin rows
  in ``elements[]``.
* Rule expression parse — safe-AST parse of every tenant rule
  expression; every ``variable_qname`` resolves.

Returned issues don't distinguish fatal / warning — every issue
aborts the envelope. The `severity` field on tenant-authored rules is
separate (it scopes the *evaluation outcome*, not the *envelope
validation*).
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from robosystems.models.api.taxonomy_block import (
  CreateTaxonomyBlockRequest,
  TaxonomyBlockElementRequest,
)
from robosystems.models.extensions import Element
from robosystems.operations.information_block.rules.expressions import (
  InvalidRuleExpression,
  parse_arithmetic_expression,
)


@dataclass(frozen=True)
class ValidationIssue:
  """One problem discovered by the validator.

  ``phase`` names which check caught it (for diagnosis); ``code`` is a
  stable machine-readable tag (for API consumers); ``message`` is
  human-readable; ``context`` carries the offending atoms (qnames,
  structure names, etc.) for reconstruction.
  """

  phase: str
  code: str
  message: str
  context: dict[str, Any] = field(default_factory=dict)


class TaxonomyBlockValidationError(ValueError):
  """Aggregate exception raised when any validator check produces issues."""

  def __init__(self, issues: list[ValidationIssue]):
    self.issues = issues
    head = f"{len(issues)} taxonomy block validation issue(s)"
    detail = "; ".join(f"[{i.phase}/{i.code}] {i.message}" for i in issues)
    super().__init__(f"{head}: {detail}" if issues else head)


def validate_create_envelope(
  payload: CreateTaxonomyBlockRequest,
  session: Session,
) -> list[ValidationIssue]:
  """Run all checks; return the full list of issues (empty when valid)."""
  issues: list[ValidationIssue] = []
  issues.extend(_phase_reference_resolution(payload, session))
  issues.extend(_phase_uniqueness(payload))
  issues.extend(_phase_structural_integrity(payload))
  issues.extend(_phase_type_specific(payload))
  issues.extend(_phase_library_origin_respect(payload))
  issues.extend(_phase_rule_expression_parse(payload))
  return issues


def _phase_reference_resolution(
  payload: CreateTaxonomyBlockRequest, session: Session
) -> list[ValidationIssue]:
  issues: list[ValidationIssue] = []
  local_qnames = {_element_qname(e, payload) for e in payload.elements}
  structure_names = {s.name for s in payload.structures}

  library_qnames = _load_library_qnames(payload, session)

  def _resolves_as_element(ref: str) -> bool:
    return ref in local_qnames or ref in library_qnames

  for element in payload.elements:
    if element.parent_ref and not _resolves_as_element(element.parent_ref):
      issues.append(
        ValidationIssue(
          phase="reference_resolution",
          code="phantom_parent_ref",
          message=(
            f"element {_element_qname(element, payload)!r} parent_ref "
            f"{element.parent_ref!r} does not resolve."
          ),
          context={
            "element_qname": _element_qname(element, payload),
            "parent_ref": element.parent_ref,
          },
        )
      )

  for assoc in payload.associations:
    if assoc.structure_ref not in structure_names:
      issues.append(
        ValidationIssue(
          phase="reference_resolution",
          code="phantom_structure_ref",
          message=(
            f"association references structure {assoc.structure_ref!r} "
            f"which was not declared in this envelope."
          ),
          context={"structure_ref": assoc.structure_ref},
        )
      )
    if not _resolves_as_element(assoc.from_ref):
      issues.append(
        ValidationIssue(
          phase="reference_resolution",
          code="phantom_from_ref",
          message=(f"association from_ref {assoc.from_ref!r} does not resolve."),
          context={"from_ref": assoc.from_ref},
        )
      )
    if not _resolves_as_element(assoc.to_ref):
      issues.append(
        ValidationIssue(
          phase="reference_resolution",
          code="phantom_to_ref",
          message=(f"association to_ref {assoc.to_ref!r} does not resolve."),
          context={"to_ref": assoc.to_ref},
        )
      )

  for rule in payload.rules:
    if rule.target_structure_ref and rule.target_structure_ref not in structure_names:
      issues.append(
        ValidationIssue(
          phase="reference_resolution",
          code="phantom_rule_target_structure",
          message=(
            f"rule {rule.name!r} target_structure_ref "
            f"{rule.target_structure_ref!r} is not declared in envelope."
          ),
          context={"rule_name": rule.name},
        )
      )
    if rule.target_element_qname and not _resolves_as_element(
      rule.target_element_qname
    ):
      issues.append(
        ValidationIssue(
          phase="reference_resolution",
          code="phantom_rule_target_element",
          message=(
            f"rule {rule.name!r} target_element_qname "
            f"{rule.target_element_qname!r} does not resolve."
          ),
          context={"rule_name": rule.name},
        )
      )

  return issues


def _phase_uniqueness(
  payload: CreateTaxonomyBlockRequest,
) -> list[ValidationIssue]:
  issues: list[ValidationIssue] = []

  qname_counts: dict[str, int] = defaultdict(int)
  for element in payload.elements:
    qname_counts[_element_qname(element, payload)] += 1
  for qname, count in qname_counts.items():
    if count > 1:
      issues.append(
        ValidationIssue(
          phase="uniqueness",
          code="duplicate_element_qname",
          message=(
            f"element qname {qname!r} appears {count} times in the envelope; "
            f"envelope-local qnames must be unique."
          ),
          context={"qname": qname, "count": count},
        )
      )

  name_counts: dict[str, int] = defaultdict(int)
  for structure in payload.structures:
    name_counts[structure.name] += 1
  for name, count in name_counts.items():
    if count > 1:
      issues.append(
        ValidationIssue(
          phase="uniqueness",
          code="duplicate_structure_name",
          message=(f"structure name {name!r} appears {count} times in the envelope."),
          context={"structure_name": name, "count": count},
        )
      )

  rule_names: dict[str, int] = defaultdict(int)
  for rule in payload.rules:
    rule_names[rule.name] += 1
  for name, count in rule_names.items():
    if count > 1:
      issues.append(
        ValidationIssue(
          phase="uniqueness",
          code="duplicate_rule_name",
          message=f"rule name {name!r} appears {count} times in the envelope.",
          context={"rule_name": name, "count": count},
        )
      )

  return issues


def _phase_structural_integrity(
  payload: CreateTaxonomyBlockRequest,
) -> list[ValidationIssue]:
  issues: list[ValidationIssue] = []
  arcs_by_structure_and_type: dict[tuple[str, str], list[tuple[str, str]]] = (
    defaultdict(list)
  )
  for assoc in payload.associations:
    arcs_by_structure_and_type[(assoc.structure_ref, assoc.association_type)].append(
      (assoc.from_ref, assoc.to_ref)
    )

  for (structure_ref, assoc_type), arcs in arcs_by_structure_and_type.items():
    if assoc_type not in ("presentation", "calculation"):
      continue
    cycle = _detect_cycle(arcs)
    if cycle is not None:
      issues.append(
        ValidationIssue(
          phase="structural_integrity",
          code="cycle_detected",
          message=(
            f"cycle detected in {assoc_type!r} arcs of structure "
            f"{structure_ref!r}: {' → '.join(cycle)}"
          ),
          context={
            "structure_ref": structure_ref,
            "association_type": assoc_type,
            "cycle_path": cycle,
          },
        )
      )

  return issues


_INSTANT_CLASSIFICATIONS = frozenset(
  {
    "asset",
    "contraAsset",
    "liability",
    "contraLiability",
    "equity",
    "contraEquity",
    "temporaryEquity",
  }
)
_DURATION_CLASSIFICATIONS = frozenset({"revenue", "expense", "income", "gain", "loss"})


def _phase_type_specific(
  payload: CreateTaxonomyBlockRequest,
) -> list[ValidationIssue]:
  issues: list[ValidationIssue] = []

  if payload.taxonomy_type == "chart_of_accounts":
    for element in payload.elements:
      qname = _element_qname(element, payload)
      if element.classification is None:
        issues.append(
          ValidationIssue(
            phase="type_specific",
            code="coa_missing_classification",
            message=(
              f"chart_of_accounts element {qname!r} must declare a "
              f"classification (asset/liability/equity/revenue/expense/...)."
            ),
            context={"element_qname": qname},
          )
        )
        continue
      if (
        element.period_type is not None
        and element.classification in _INSTANT_CLASSIFICATIONS
        and element.period_type != "instant"
      ):
        issues.append(
          ValidationIssue(
            phase="type_specific",
            code="coa_period_type_mismatch",
            message=(
              f"chart_of_accounts element {qname!r} classification "
              f"{element.classification!r} requires period_type='instant'; "
              f"got {element.period_type!r}."
            ),
            context={
              "element_qname": qname,
              "classification": element.classification,
              "period_type": element.period_type,
            },
          )
        )
      if (
        element.period_type is not None
        and element.classification in _DURATION_CLASSIFICATIONS
        and element.period_type != "duration"
      ):
        issues.append(
          ValidationIssue(
            phase="type_specific",
            code="coa_period_type_mismatch",
            message=(
              f"chart_of_accounts element {qname!r} classification "
              f"{element.classification!r} requires period_type='duration'; "
              f"got {element.period_type!r}."
            ),
            context={
              "element_qname": qname,
              "classification": element.classification,
              "period_type": element.period_type,
            },
          )
        )

  if payload.taxonomy_type == "reporting_extension" and not payload.parent_taxonomy_id:
    issues.append(
      ValidationIssue(
        phase="type_specific",
        code="extension_missing_parent",
        message=("reporting_extension envelopes must carry parent_taxonomy_id."),
        context={},
      )
    )

  if payload.taxonomy_type == "reporting_extension" and payload.namespace_uri:
    prefix = _namespace_prefix(payload.namespace_uri)
    if prefix:
      for element in payload.elements:
        qname = _element_qname(element, payload)
        element_prefix = qname.split(":", 1)[0] if ":" in qname else ""
        if element_prefix and element_prefix != prefix:
          issues.append(
            ValidationIssue(
              phase="type_specific",
              code="extension_namespace_mismatch",
              message=(
                f"reporting_extension element {qname!r} prefix does not "
                f"match envelope namespace_uri prefix {prefix!r}."
              ),
              context={"element_qname": qname, "expected_prefix": prefix},
            )
          )

  return issues


def _phase_library_origin_respect(
  payload: CreateTaxonomyBlockRequest,
) -> list[ValidationIssue]:
  issues: list[ValidationIssue] = []
  for element in payload.elements:
    origin = element.metadata.get("origin") if element.metadata else None
    if origin == "library":
      issues.append(
        ValidationIssue(
          phase="library_origin_respect",
          code="library_origin_smuggled",
          message=(
            f"element {_element_qname(element, payload)!r} declares "
            f"origin='library' in metadata; tenants cannot author library "
            f"rows through the envelope."
          ),
          context={"element_qname": _element_qname(element, payload)},
        )
      )
  return issues


def _phase_rule_expression_parse(
  payload: CreateTaxonomyBlockRequest,
) -> list[ValidationIssue]:
  issues: list[ValidationIssue] = []
  for rule in payload.rules:
    variable_names = [
      v.get("variable_name", "") for v in rule.variables if v.get("variable_name")
    ]
    try:
      parse_arithmetic_expression(rule.expression, variable_names)
    except InvalidRuleExpression as exc:
      issues.append(
        ValidationIssue(
          phase="rule_expression_parse",
          code="unparseable_expression",
          message=(f"rule {rule.name!r} expression did not parse: {exc}"),
          context={"rule_name": rule.name, "expression": rule.expression},
        )
      )
  return issues


# ── Helpers ────────────────────────────────────────────────────────────────


def _element_qname(
  element: TaxonomyBlockElementRequest, payload: CreateTaxonomyBlockRequest
) -> str:
  """Mirror the handler's qname derivation (used everywhere in validation)."""
  if element.qname:
    return element.qname
  ns = payload.standard or _default_namespace_for_type(payload.taxonomy_type)
  token = element.code or element.name.replace(" ", "")
  return f"{ns}:{token}"


def _default_namespace_for_type(taxonomy_type: str) -> str:
  """Default namespace token used when tenant didn't supply qname/standard.

  Matches the handler's inline defaults.
  """
  return {
    "chart_of_accounts": "coa",
    "custom_ontology": "custom",
    "reporting_extension": "ext",
    "reporting_standard": "library",
    "schedule": "schedule",
  }.get(taxonomy_type, taxonomy_type)


def _namespace_prefix(namespace_uri: str) -> str | None:
  """Extract a rough prefix from a namespace URI (last path segment)."""
  stripped = namespace_uri.rstrip("/")
  if not stripped:
    return None
  return stripped.rsplit("/", 1)[-1] or None


def _load_library_qnames(
  payload: CreateTaxonomyBlockRequest, session: Session
) -> set[str]:
  """For reporting_extension, load the qnames referenced in payload from the parent."""
  if payload.taxonomy_type != "reporting_extension" or not payload.parent_taxonomy_id:
    return set()
  referenced: set[str] = set()
  local_qnames = {_element_qname(e, payload) for e in payload.elements}
  for element in payload.elements:
    if element.parent_ref and element.parent_ref not in local_qnames:
      referenced.add(element.parent_ref)
  for assoc in payload.associations:
    if assoc.from_ref not in local_qnames:
      referenced.add(assoc.from_ref)
    if assoc.to_ref not in local_qnames:
      referenced.add(assoc.to_ref)
  if not referenced:
    return set()
  rows = session.execute(
    select(Element.qname).where(
      Element.taxonomy_id == payload.parent_taxonomy_id,
      Element.qname.in_(referenced),
    )
  ).all()
  return {row[0] for row in rows}


def _detect_cycle(arcs: list[tuple[str, str]]) -> list[str] | None:
  """Iterative DFS; return the cycle path or None.

  Treat `arcs` as directed edges from→to. Cycle = any node reachable
  from itself.
  """
  if not arcs:
    return None
  adj: dict[str, list[str]] = defaultdict(list)
  nodes: set[str] = set()
  for src, dst in arcs:
    adj[src].append(dst)
    nodes.add(src)
    nodes.add(dst)

  WHITE, GRAY, BLACK = 0, 1, 2
  color: dict[str, int] = dict.fromkeys(nodes, WHITE)

  for start in nodes:
    if color[start] != WHITE:
      continue
    stack: list[tuple[str, iter]] = [(start, iter(adj[start]))]
    path: list[str] = [start]
    color[start] = GRAY
    while stack:
      node, children = stack[-1]
      nxt = next(children, None)
      if nxt is None:
        color[node] = BLACK
        stack.pop()
        if path and path[-1] == node:
          path.pop()
        continue
      if color[nxt] == GRAY:
        # found cycle — build path from nxt onwards
        idx = path.index(nxt) if nxt in path else 0
        return [*path[idx:], nxt]
      if color[nxt] == WHITE:
        color[nxt] = GRAY
        path.append(nxt)
        stack.append((nxt, iter(adj[nxt])))
  return None


__all__ = [
  "TaxonomyBlockValidationError",
  "ValidationIssue",
  "validate_create_envelope",
]
