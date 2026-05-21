"""Generate L2 calc-arc RollUp rules from ``rs-gaap-calculations/v1``.

Each calculation Network in ``rs-gaap-calculations`` declares a subtotal
parent (``arcFrom``) and its weighted children (``arcTo`` + ``arcWeight``).
This script walks those Networks and emits one ``RollUp`` verification
rule per parent — ``$Parent = Σ child_i * weight_i`` — into the
``rs-gaap-rollup-rules/v1`` package, making calc-DAG consistency explicit
and queryable in Verification Results (today it's only enforced
implicitly by the renderer's guard rails).

Rules are **element-scoped** to the parent concept (``targetKind:
"element"``, ``targetRef`` = the parent qname). The rule engine evaluates
rules against the *rendered* presentation Network and binds facts from
that Network's FactSet; element-scoping to the parent makes the rule fire
whenever a rendered statement presents that subtotal — across every
equity-form Balance Sheet variant and Reporting Style — without coupling
to a specific presentation role-URI.

The ``RollUp`` evaluator treats a missing RHS child as 0 (matching the
renderer, which sums only the children that have facts); a missing parent
subtotal skips. See ``operations/information_block/rules/evaluators.py``.

Run: ``uv run python -m robosystems.taxonomy.scripts.generate_rollup_rules``
The committed artifact is the JSON-LD output, not this script.
"""

from __future__ import annotations

import json
from pathlib import Path

_FRAMEWORKS = Path(__file__).resolve().parents[3] / "frameworks"
_CALC_SOURCE = (
  _FRAMEWORKS
  / "rs-gaap"
  / "packages"
  / "rs-gaap-calculations"
  / "v1"
  / "taxonomy.jsonld"
)
_OUTPUT = (
  _FRAMEWORKS
  / "rs-gaap"
  / "packages"
  / "rs-gaap-rollup-rules"
  / "v1"
  / "taxonomy.jsonld"
)

_VOCAB = "https://robosystems.ai/vocab/"

_CONTEXT = {
  "rs-gaap": "https://robosystems.ai/taxonomy/rs-gaap/v1/",
  "rs": _VOCAB,
  "ruleCategory": {"@id": f"{_VOCAB}ruleCategory"},
  "rulePattern": {"@id": f"{_VOCAB}rulePattern"},
  "ruleExpression": {"@id": f"{_VOCAB}ruleExpression"},
  "ruleTarget": {"@id": f"{_VOCAB}ruleTarget"},
  "targetKind": {"@id": f"{_VOCAB}targetKind"},
  # NOTE: targetRef is a literal qname here (NOT @type:@id). The library
  # creator resolves element targets by ``Element.qname`` match, so the
  # raw "rs-gaap:Foo" string must survive parsing un-expanded.
  "targetRef": {"@id": f"{_VOCAB}targetRef"},
  "ruleVariables": {"@id": f"{_VOCAB}ruleVariables", "@container": "@list"},
  "variableName": {"@id": f"{_VOCAB}variableName"},
  "variableQname": {"@id": f"{_VOCAB}variableQname"},
  "ruleMessage": {"@id": f"{_VOCAB}ruleMessage"},
  "ruleSeverity": {"@id": f"{_VOCAB}ruleSeverity"},
  "ruleOrigin": {"@id": f"{_VOCAB}ruleOrigin"},
}


def _local(qname: str) -> str:
  """``rs-gaap:Assets`` -> ``Assets`` (the rule-expression variable name)."""
  return qname.split(":", 1)[1] if ":" in qname else qname


def _role_slug(role_uri: str) -> str:
  """``.../rs-gaap-calculations/BS-Assets`` -> ``bs-assets`` (stable @id suffix)."""
  return role_uri.rstrip("/").rsplit("/", 1)[-1].lower()


def _build_expression(parent: str, children: list[tuple[str, float]]) -> str:
  """``$Parent = ($childA + $childB - $childC ...)``.

  Weight +1 -> ``+``, -1 -> ``-``, otherwise an explicit ``* weight`` term.
  """
  parts: list[str] = []
  for idx, (child_qname, weight) in enumerate(children):
    var = f"${_local(child_qname)}"
    if weight == 1.0:
      sign, term = "+", var
    elif weight == -1.0:
      sign, term = "-", var
    else:
      # Render -0.0 cleanly and keep weight literal for non-unit weights.
      sign, term = "+", f"({var} * {weight})"
    if idx == 0:
      parts.append(term if sign == "+" else f"-{term}")
    else:
      parts.append(f"{sign} {term}")
  rhs = " ".join(parts)
  return f"${_local(parent)} = ({rhs})"


def build_rollup_rules(graph: list[dict]) -> list[dict]:
  """Pure transform: calc ``@graph`` -> list of RollUp rule nodes.

  Groups calculation arcs by ``arcRoleUri`` (one Network = one subtotal
  parent), then emits one RollUp rule per Network. Deterministic order:
  by role-URI suffix.
  """
  role_name: dict[str, str] = {}
  # role_uri -> (parent_qname, [(child_qname, weight, order)])
  groups: dict[str, list[tuple[str, float, float]]] = {}
  parents: dict[str, str] = {}

  for node in graph:
    if "roleUri" in node:
      role_name[node["roleUri"]] = node.get("structureName", "")
      continue
    if "arcFrom" not in node:
      continue
    if node.get("arcAssociationType") != "calculation":
      continue
    role = node["arcRoleUri"]
    parent = node["arcFrom"]["@id"]
    child = node["arcTo"]["@id"]
    weight = float(node.get("arcWeight", 1.0))
    order = float(node.get("arcOrder", 0.0))
    if role in parents and parents[role] != parent:
      raise ValueError(
        f"calc Network {role!r} has multiple parents "
        f"({parents[role]!r} and {parent!r}); a RollUp rule needs one parent"
      )
    parents[role] = parent
    groups.setdefault(role, []).append((child, weight, order))

  rules: list[dict] = []
  for role in sorted(groups, key=_role_slug):
    parent = parents[role]
    children_sorted = sorted(groups[role], key=lambda t: t[2])
    children = [(cq, w) for cq, w, _ in children_sorted]

    variables = [{"variableName": _local(parent), "variableQname": parent}]
    for child_qname, _w in children:
      variables.append(
        {"variableName": _local(child_qname), "variableQname": child_qname}
      )

    formula = role_name.get(role, "")
    message = (
      f"Calculation rollup ({_role_slug(role)}): {_local(parent)} must equal "
      f"the calculation sum of its children."
    )
    if "—" in formula:
      message = f"Calculation rollup: {formula.split('—', 1)[1].strip()}"

    rules.append(
      {
        "@id": f"_:rs-gaap-rollup-{_role_slug(role)}",
        "ruleCategory": "FundamentalAccountingConceptRelation",
        "rulePattern": "RollUp",
        "ruleExpression": _build_expression(parent, children),
        "ruleTarget": {"targetKind": "element", "targetRef": parent},
        "ruleVariables": variables,
        "ruleMessage": message,
        "ruleSeverity": "error",
        "ruleOrigin": "native",
      }
    )
  return rules


def build_package(rules: list[dict]) -> dict:
  return {
    "@context": _CONTEXT,
    "standard": "rs-gaap-rollup-rules",
    "version": "v1",
    "taxonomy_type": "rules",
    "namespace_uri": "https://robosystems.ai/taxonomy/rs-gaap/rollup-rules/",
    "default_block_type": "validation_rules",
    "origin": "native",
    "created_at": "2026-05-21",
    "description": (
      "L2 calc-arc RollUp verification rules — one rule per subtotal parent "
      "in rs-gaap-calculations/v1 ($Parent = Σ weighted children). "
      "RoboSystems-native (info-block §6.1 Package II.a). Element-scoped to the "
      "parent concept so the rule fires against any rendered statement that "
      "presents that subtotal, across all equity-form variants. The RollUp "
      "evaluator treats a missing RHS child as 0 (matching the renderer); a "
      "missing parent subtotal skips. Regenerate via "
      "robosystems.taxonomy.scripts.generate_rollup_rules from rs-gaap-calculations/v1."
    ),
    "@graph": rules,
  }


def main() -> None:
  graph = json.loads(_CALC_SOURCE.read_text())["@graph"]
  rules = build_rollup_rules(graph)
  package = build_package(rules)
  _OUTPUT.parent.mkdir(parents=True, exist_ok=True)
  _OUTPUT.write_text(json.dumps(package, indent=2) + "\n")
  print(f"Wrote {len(rules)} RollUp rules to {_OUTPUT.relative_to(_FRAMEWORKS.parent)}")
  for rule in rules:
    print(f"  {rule['@id']:42s} {rule['ruleExpression']}")


if __name__ == "__main__":
  main()
