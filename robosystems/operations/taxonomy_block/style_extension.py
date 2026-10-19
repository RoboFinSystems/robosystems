"""Style extensions: a reporting extension's rows layered onto a library
Reporting Style network.

A ``reporting_extension`` structure that names ``extends_network`` (a Style
network's role URI) is an extension network: its arcs render as rows of that
statement for every entity whose Style composes it (the readers merge them
through ``network_picker.network_structure_ids``). The library structure
itself stays library-only.

Anchoring is checked here, at authoring, so a row can never land where
nothing sums it:

1. the extended network is composed by a Style an entity of this graph uses,
   and the structure's statement type is the network's;
2. every presentation parent is presented in the network (or is a row the
   extension adds), and no row is one the network, or another extension of
   it, already presents;
3. on the balance sheet, income statement and cash flow, every new row has a
   calculation path to a subtotal the network presents; on the equity
   roll-forward every row hangs from the root and is a duration amount;
4. library first: a tenant concept placed as a row must refine an rs-gaap
   concept (``parent_ref``), whose traits it inherits, so every reader that
   keys on rs-gaap (metrics, plan levers, the checklist) reads it as that
   concept.
"""

from __future__ import annotations

from collections import Counter

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from robosystems.models.extensions import (
  Association,
  Element,
  ElementTrait,
  Structure,
  Taxonomy,
  Trait,
)
from robosystems.operations.roboledger.reports.calc_dag import (
  EXTENDS_NETWORK_KEY,
  load_rs_gaap_calculations,
)
from robosystems.operations.roboledger.reports.network_picker import (
  network_structure_ids,
)

STATEMENT_BLOCK_TYPES: frozenset[str] = frozenset(
  {"balance_sheet", "income_statement", "cash_flow_statement", "equity_statement"}
)


class StyleExtensionError(ValueError):
  """An extension network that would not anchor into its Style network."""


def resolve_extended_network(session: Session, role_uri: str, block_type: str) -> str:
  """The id of the Style network at ``role_uri``, checked usable for an
  extension of ``block_type`` in this graph."""
  if block_type not in STATEMENT_BLOCK_TYPES:
    raise StyleExtensionError(
      f"extends_network applies to statement structures "
      f"({', '.join(sorted(STATEMENT_BLOCK_TYPES))}), not {block_type!r}."
    )
  row = session.execute(
    text(
      """
      SELECT s.id, s.block_type
      FROM structures s
      WHERE s.metadata ->> 'role_uri' = :role
        AND s.created_by = 'library-seeder'
        AND EXISTS (
          SELECT 1 FROM reporting_style_networks rsn WHERE rsn.network_id = s.id
        )
      """
    ),
    {"role": role_uri},
  ).fetchone()
  if row is None:
    raise StyleExtensionError(
      f"extends_network {role_uri!r} is not a Reporting Style network in this "
      f"graph; name one a Style composes (e.g. "
      f"'https://robosystems.ai/seattle/cm-roles/roles/rs-gaap-presentation/"
      f"CashFlow-indirect')."
    )
  if row.block_type != block_type:
    raise StyleExtensionError(
      f"extends_network {role_uri!r} is a {row.block_type} network; the "
      f"structure extending it must be block_type {row.block_type!r}, not "
      f"{block_type!r}."
    )
  in_use = session.execute(
    text(
      """
      SELECT 1
      FROM reporting_style_networks rsn
      JOIN entities e ON e.reporting_style_id = rsn.reporting_style_id
      WHERE rsn.network_id = :nid
      LIMIT 1
      """
    ),
    {"nid": row.id},
  ).first()
  if in_use is None:
    raise StyleExtensionError(
      f"no entity in this graph uses a Reporting Style that composes "
      f"{role_uri!r}; an extension of it would render nowhere."
    )
  return str(row.id)


def inherit_parent_types(
  session: Session,
  element: Element,
  balance_type: str | None,
  period_type: str | None,
) -> None:
  """Give a tenant concept its parent's balance and period type where the
  request leaves them out; refuse a period type its parent does not have.
  No-op without a parent."""
  if element.parent_id is None:
    return
  parent = session.get(Element, element.parent_id)
  if parent is None:
    return
  if (
    period_type is not None and parent.period_type and period_type != parent.period_type
  ):
    raise StyleExtensionError(
      f"{element.qname!r} refines {parent.qname!r}, a {parent.period_type} "
      f"concept, so it cannot be {period_type}."
    )
  if balance_type is None and parent.balance_type:
    element.balance_type = parent.balance_type
  if period_type is None and parent.period_type:
    element.period_type = parent.period_type


def inherit_parent_traits(
  session: Session, element: Element, efs_override: str | None
) -> None:
  """Give a tenant concept its rs-gaap parent's traits, the parent's
  statement classification replaced by ``efs_override`` when one is given.
  No-op without a parent."""
  if element.parent_id is None:
    return
  parent_rows = session.execute(
    select(ElementTrait, Trait.category)
    .join(Trait, Trait.id == ElementTrait.trait_id)
    .where(ElementTrait.element_id == element.parent_id)
  ).all()
  have = {
    trait_id
    for (trait_id,) in session.execute(
      select(ElementTrait.trait_id).where(ElementTrait.element_id == element.id)
    )
  }
  for parent_trait, category in parent_rows:
    if efs_override and category == "elementsOfFinancialStatements":
      continue
    if parent_trait.trait_id in have:
      continue
    session.add(
      ElementTrait(
        element_id=element.id,
        trait_id=parent_trait.trait_id,
        is_primary=parent_trait.is_primary,
      )
    )
  if efs_override:
    efs = session.execute(
      select(Trait.id).where(
        Trait.category == "elementsOfFinancialStatements",
        Trait.identifier == efs_override,
      )
    ).scalar_one_or_none()
    if efs is not None and efs not in have:
      session.add(ElementTrait(element_id=element.id, trait_id=efs, is_primary=True))


def _presentation_concepts(
  session: Session, structure_ids: list[str]
) -> tuple[set[str], set[str], set[str]]:
  """(every concept on either end of a presentation arc, the parents, the
  roots) across ``structure_ids``."""
  rows = session.execute(
    select(Association.from_element_id, Association.to_element_id).where(
      Association.structure_id.in_(structure_ids),
      Association.association_type == "presentation",
    )
  ).all()
  froms = {str(f) for f, _ in rows if f is not None and str(f) not in structure_ids}
  tos = {str(t) for _, t in rows if t is not None}
  return froms | tos, froms, froms - tos


def validate_extension_network(session: Session, structure: Structure) -> None:
  """Refuse an extension network that does not anchor (module docstring)."""
  network_id = str((structure.metadata_ or {}).get(EXTENDS_NETWORK_KEY) or "")
  if not network_id:
    return
  network = session.get(Structure, network_id)
  if network is None:
    raise StyleExtensionError(
      f"structure {structure.name!r} extends a network that no longer exists."
    )
  base_rows, subtotals, roots = _presentation_concepts(session, [network_id])
  presented, _, _ = _presentation_concepts(
    session,
    [sid for sid in network_structure_ids(session, network_id) if sid != structure.id],
  )

  arcs = (
    session.execute(select(Association).where(Association.structure_id == structure.id))
    .scalars()
    .all()
  )
  presentation = [a for a in arcs if a.association_type == "presentation"]
  new_rows = {str(a.to_element_id) for a in presentation}
  names = {
    str(e.id): str(e.qname or e.name)
    for e in session.execute(
      select(Element).where(
        Element.id.in_(new_rows | {str(a.from_element_id) for a in presentation})
      )
    ).scalars()
  }

  def _name(element_id: str) -> str:
    return names.get(element_id, element_id)

  placements = Counter(str(a.to_element_id) for a in presentation)
  moved = (new_rows & presented) | {r for r, n in placements.items() if n > 1}
  if moved:
    raise StyleExtensionError(
      f"{structure.name!r} places {sorted(_name(m) for m in moved)} again; the "
      f"network, an extension of it, or this structure already presents them, "
      f"and an extension adds rows, it never moves one."
    )
  for a in presentation:
    parent = str(a.from_element_id)
    if parent not in presented and parent not in new_rows:
      raise StyleExtensionError(
        f"{structure.name!r}: the parent of {_name(str(a.to_element_id))!r}, "
        f"{_name(parent)!r}, is not on the network it extends."
      )

  _check_calculation_arcs(session, structure, arcs, new_rows, _name)
  if network.block_type == "equity_statement":
    _check_equity_rows(session, structure, presentation, roots, _name)
  else:
    _check_calculation_paths(
      session, structure, arcs, new_rows, subtotals, base_rows - subtotals, _name
    )
  _check_rows_refine_the_library(session, structure, new_rows, _name)


def _check_equity_rows(session, structure, presentation, roots, name) -> None:
  for a in presentation:
    if str(a.from_element_id) not in roots:
      raise StyleExtensionError(
        f"{structure.name!r}: a roll-forward row hangs from the root "
        f"({sorted(name(r) for r in roots)}), not from "
        f"{name(str(a.from_element_id))!r}."
      )
    row = session.get(Element, a.to_element_id)
    if row is None or row.period_type != "duration" or not row.is_monetary:
      raise StyleExtensionError(
        f"{structure.name!r}: {name(str(a.to_element_id))!r} must be a duration "
        f"amount to be a roll-forward movement."
      )


def _check_calculation_arcs(session, structure, arcs, new_rows, name) -> None:
  """An extension's calculation arcs join the graph's whole calc DAG, so
  each one must sum a row the extension adds, and give it its only parent:
  a second parent counts the row twice."""
  own: dict[str, set[str]] = {}
  for a in arcs:
    if a.association_type != "calculation":
      continue
    child = str(a.to_element_id)
    if child not in new_rows:
      raise StyleExtensionError(
        f"{structure.name!r}: a calculation arc sums {name(child)!r}, which is "
        f"not a row this extension adds; an extension sums only its own rows."
      )
    own.setdefault(child, set()).add(str(a.from_element_id))
  if not own:
    return
  others: dict[str, set[str]] = {}
  for parent, children in load_rs_gaap_calculations(session).items():
    for child, _weight in children:
      if str(child) in own and str(parent) not in own[str(child)]:
        others.setdefault(str(child), set()).add(str(parent))
  for child, parents in own.items():
    if len(parents | others.get(child, set())) > 1:
      raise StyleExtensionError(
        f"{structure.name!r}: {name(child)!r} would sum into more than one "
        f"subtotal, so totals would count it twice; give it one calculation "
        f"parent."
      )


def _check_calculation_paths(
  session, structure, arcs, new_rows, subtotals, leaves, name
) -> None:
  """A path may not run through a presented leaf: the leaf's own fact wins
  over a calc sum, so a row summing into it drops out of every total."""
  parents: dict[str, set[str]] = {}
  for parent, children in load_rs_gaap_calculations(session).items():
    for child, _weight in children:
      parents.setdefault(str(child), set()).add(str(parent))
  for a in arcs:
    if a.association_type == "calculation":
      parents.setdefault(str(a.to_element_id), set()).add(str(a.from_element_id))
  calc_parents = set().union(*parents.values()) if parents else set()
  presented = subtotals & calc_parents
  for row in new_rows:
    frontier, seen = [row], {row}
    while frontier:
      current = frontier.pop()
      reached = parents.get(current, set())
      if reached & presented:
        break
      frontier.extend(p for p in reached if p not in seen and p not in leaves)
      seen |= reached
    else:
      raise StyleExtensionError(
        f"{structure.name!r}: {name(row)!r} has no calculation path to a "
        f"subtotal the statement presents, so nothing would sum it; add a "
        f"calculation arc from its section subtotal."
      )


def _check_rows_refine_the_library(session, structure, new_rows, name) -> None:
  taxonomy = session.get(Taxonomy, structure.taxonomy_id)
  library_id = taxonomy.parent_taxonomy_id if taxonomy is not None else None
  for element in session.execute(
    select(Element).where(
      Element.id.in_(new_rows), Element.taxonomy_id == structure.taxonomy_id
    )
  ).scalars():
    parent = session.get(Element, element.parent_id) if element.parent_id else None
    if parent is None or parent.taxonomy_id != library_id:
      raise StyleExtensionError(
        f"{name(str(element.id))!r} is a tenant concept placed on a statement; "
        f"it must refine an rs-gaap concept (set parent_ref to one) so reports, "
        f"metrics and the plan read it as that concept. Prefer the rs-gaap "
        f"concept itself where one fits."
      )
