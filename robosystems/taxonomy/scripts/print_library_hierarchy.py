"""Print the rs-gaap rollup hierarchy + a coherence scan for a library graph.

A validation tool for the curated taxonomy: renders the calculation and
presentation rollup trees with each node's trait badges
([EFS / operatingNonoperating / balance / abstract]), so you can eyeball whether
the curation is coherent — every leaf rolls up, children fit their parent, no
orphan/abstract leaves. Reads through ``extensions_session`` so ``library`` ->
the public canonical library and a ``kg…`` graph_id -> that tenant's curated copy.

    uv run python -m robosystems.taxonomy.scripts.print_library_hierarchy [graph_id]

graph_id defaults to the public library. The coherence scan at the end flags the
load-bearing problems (renderable concept missing EFS, calc leaf that won't
render, presentation leaf that won't foot, abstract leaf).
"""

from __future__ import annotations

import sys

from sqlalchemy import text

from robosystems.db.extensions import LIBRARY_GRAPH_ID, extensions_session
from robosystems.operations.operators.implementations.mapping.constants import (
  RS_GAAP_SUBTOTAL_DENYLIST,
)

_RG = "rs-gaap:%"


def _load(graph_id: str) -> dict:
  with extensions_session(graph_id) as s:
    elems = {
      r[0]: {
        "qname": r[1],
        "name": r[2],
        "abstract": r[3],
        "balance": r[4],
        "period": r[5],
      }
      for r in s.execute(
        text(
          "SELECT id, qname, name, is_abstract, balance_type, period_type "
          "FROM elements WHERE qname LIKE :p"
        ),
        {"p": _RG},
      )
    }
    traits: dict[str, dict[str, str]] = {}
    for eid, cat, ident in s.execute(
      text(
        "SELECT et.element_id, t.category, t.identifier FROM element_traits et "
        "JOIN traits t ON t.id=et.trait_id"
      )
    ):
      if eid in elems:
        traits.setdefault(eid, {})[cat] = ident

    def arcs(kind: str):
      return list(
        s.execute(
          text(
            "SELECT a.from_element_id, a.to_element_id, a.weight, a.order_value, "
            "a.structure_id, st.name FROM associations a "
            "LEFT JOIN structures st ON st.id=a.structure_id "
            "WHERE a.association_type=:k"
          ),
          {"k": kind},
        )
      )

    calc = [r for r in arcs("calculation") if r[0] in elems and r[1] in elems]
    pres = [r for r in arcs("presentation") if r[0] in elems and r[1] in elems]
    gs = [r for r in arcs("general-special") if r[0] in elems and r[1] in elems]
    # active reporting-style networks (what actually renders) → block_type
    active: dict[str, str] = {}
    try:
      for sid, bt in s.execute(
        text(
          "SELECT rsn.network_id, st.block_type FROM reporting_style_networks rsn "
          "JOIN structures st ON st.id=rsn.network_id"
        )
      ):
        active[sid] = bt
    except Exception:
      pass  # public schema has no reporting_style_networks; scan falls back to all
  return {
    "elems": elems,
    "traits": traits,
    "calc": calc,
    "pres": pres,
    "gs": gs,
    "active": active,
  }


def _badge(eid: str, d: dict) -> str:
  e = d["elems"][eid]
  t = d["traits"].get(eid, {})
  parts = [
    t.get("elementsOfFinancialStatements", "—"),
    t.get("operatingNonoperating", ""),
    (e["balance"] or "")[:2],
  ]
  if e["abstract"]:
    parts.append("ABSTRACT")
  return "[" + " ".join(p for p in parts if p) + "]"


def _print_tree(d: dict, edges: list, label_weight: bool, title: str) -> None:
  children: dict[str, list] = {}
  parents = set()
  froms = set()
  for f, t_, w, o, _sid, _sn in edges:
    children.setdefault(f, []).append((t_, w, o))
    parents.add(f)
    froms.add(t_)
  roots = sorted(parents - froms, key=lambda i: d["elems"][i]["qname"])
  print(f"\n{'=' * 78}\n{title}\n{'=' * 78}")
  seen_path: set[str] = set()

  def walk(eid: str, depth: int, w: float | None, o: float | None) -> None:
    e = d["elems"][eid]
    wt = ""
    if label_weight and w is not None:
      wt = f"{'+' if w >= 0 else '-'} "
    ord_s = f"({int(o)}) " if o is not None else ""
    print(f"{'  ' * depth}{wt}{ord_s}{e['name']}  {_badge(eid, d)}")
    if eid in seen_path:  # cycle / re-entry guard
      return
    seen_path.add(eid)
    for c, cw, co in sorted(children.get(eid, []), key=lambda x: x[2] or 0):
      walk(c, depth + 1, cw, co)
    seen_path.discard(eid)

  for r in roots:
    walk(r, 0, None, None)


def _coherence_scan(d: dict) -> None:
  elems, traits, active = d["elems"], d["traits"], d["active"]
  calc_from = {r[0] for r in d["calc"]}
  calc_to = {r[1] for r in d["calc"]}
  calc_nodes = calc_from | calc_to
  calc_leaves = calc_to - calc_from

  # Scope the presentation checks to the ACTIVE reporting-style networks (what
  # actually renders) when available — the breadth presentation trees and the
  # CF/equity flow concepts otherwise drown the signal. EFS is a BS/IS axis, so
  # only BS/IS render nodes are expected to carry it (CF flow concepts and
  # roll-forward equity movements legitimately have none).
  bs_is_struct = {
    sid for sid, bt in active.items() if bt in ("balance_sheet", "income_statement")
  }
  scoped = bool(bs_is_struct)
  bs_is_pres = [r for r in d["pres"] if (not scoped or r[4] in bs_is_struct)]
  bis_from = {r[0] for r in bs_is_pres}
  bis_to = {r[1] for r in bs_is_pres}
  bis_nodes = bis_from | bis_to
  bis_leaves = bis_to - bis_from

  def q(eids):
    return sorted(elems[i]["qname"] for i in eids)

  # A denylisted rollup/total (Assets, LiabilitiesAndStockholdersEquity, …) is
  # never a CoA mapping target, so it isn't expected to carry EFS — and a total
  # that sums two different elements (e.g. liabilities + equity) genuinely isn't
  # one of the SFAC-6 elements. Only flag mappable render nodes.
  renderable_no_efs = {
    i
    for i in bis_nodes
    if not elems[i]["abstract"]
    and "elementsOfFinancialStatements" not in traits.get(i, {})
    and elems[i]["qname"] not in RS_GAAP_SUBTOTAL_DENYLIST
  }
  calc_leaf_not_rendered = (
    calc_leaves - {r[1] for r in d["pres"]} - {r[0] for r in d["pres"]}
  )
  bis_leaf_not_footed = {i for i in bis_leaves if not elems[i]["abstract"]} - calc_nodes
  abstract_calc_leaf = {i for i in calc_leaves if elems[i]["abstract"]}

  scope_note = (
    " (scoped to active BS/IS reporting-style networks)"
    if scoped
    else " (no active networks — scanning ALL presentation arcs)"
  )
  print(f"\n{'=' * 78}\nCOHERENCE SCAN{scope_note}\n{'=' * 78}")
  for label, s in [
    ("BS/IS mappable render node missing EFS (can't be mapped to)", renderable_no_efs),
    (
      "calc leaf NOT in any presentation (foots but won't render)",
      calc_leaf_not_rendered,
    ),
    (
      "BS/IS render leaf NOT a calc child (renders but won't foot)",
      bis_leaf_not_footed,
    ),
    ("ABSTRACT calc leaf (can't hold a fact)", abstract_calc_leaf),
  ]:
    items = q(s)
    flag = "OK" if not items else f"{len(items)} ⚠"
    print(f"\n  [{flag}] {label}")
    for qn in items[:40]:
      print(f"        {qn}")
    if len(items) > 40:
      print(f"        … and {len(items) - 40} more")


def main(graph_id: str) -> None:
  d = _load(graph_id)
  print(
    f"graph={graph_id}  rs-gaap elements={len(d['elems'])}  "
    f"calc_arcs={len(d['calc'])}  pres_arcs={len(d['pres'])}  gs_arcs={len(d['gs'])}"
  )
  _print_tree(
    d, d["calc"], label_weight=True, title="CALCULATION ROLLUPS (weight · order)"
  )
  # presentation: one tree per structure
  by_struct: dict[str, list] = {}
  for r in d["pres"]:
    by_struct.setdefault(r[5] or r[4], []).append(r)
  for sname, edges in sorted(by_struct.items()):
    _print_tree(d, edges, label_weight=False, title=f"PRESENTATION · {sname}")
  _coherence_scan(d)


if __name__ == "__main__":
  main(sys.argv[1] if len(sys.argv) > 1 else LIBRARY_GRAPH_ID)
