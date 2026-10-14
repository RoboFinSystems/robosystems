"""Flow-element coverage: a default flow for every mapping candidate, and a
cash flow row for every flow, in every Reporting Style.

The cash flow statement's investing and financing sections come from per-line
flows: a tag, or the default of the balance-sheet concept an account maps to
(a ``derivation`` arc in ``rs-gaap-calculations``). The defaults were
corporate-only (paid-in capital, long-term debt) and the one indirect cash
flow network was shared by the three Styles with four financing rows, so an
LLC's note draw and member contribution — concepts with no default, flows
with no row — fell into the operating reconciling adjustment. Observed live
on a native-books entity, 2026-10-10.

The committed packages now carry (spec ``transaction-enrichment`` §13):

* ``rs-gaap``: the short-term debt pair and the short-term investments pair,
  under their us-gaap names, with ``rs-gaap-traits`` bindings;
* ``rs-gaap-calculations``: default flows for short-term and current debt,
  finance leases, other noncurrent liabilities, members' and partners'
  capital, common and preferred stock, retained earnings (dividends),
  short-term and other investments and goodwill; and the calc arcs that foot
  each new row;
* ``rs-gaap-presentation``: the debt, investment and corporate equity rows on
  ``CashFlow-indirect``; ``CashFlow-indirect-LLC`` and ``-PART``, which swap
  the stock rows for the form's contributions and distributions; a
  contributions row on the LLC roll-forward; and both form roll-forwards
  distribute through ``PaymentsOfCapitalDistribution`` — one concept on both
  statements — in place of ``PaymentsOfDividends``;
* ``rs-gaap-reporting-styles``: the LLC and Partnership Styles compose their
  own cash flow network;
* ``rs-gaap-rollup-rules``: the investing and financing RollUp rules
  enumerate the new children.

This migration brings ``public`` up to the committed source and fans the
pinned packages into every tenant through ``resync_library_into_tenant``
(the 0022 / 0030 pattern). Three things the resync does not do on its own:

* a concept reaches a tenant only when ``tenant-exclude/v1.json`` admits it —
  regenerated with this change, so the new rows' concepts copy;
* a Style's composition row is additive in the resync, so the LLC and
  Partnership ``cash_flow_statement`` rows are repointed explicitly, in
  public and in each tenant, once the new network structure has landed;
* the two roll-forward arcs that moved (``to`` is part of the arc id) are
  retired by the resync's own stale-arc step, so no tenant keeps both rows.

Tenant-authored styles and arcs are never touched.

Revision ID: 0044
Revises: 0043
Create Date: 2026-10-13
"""

from __future__ import annotations

import json
from pathlib import Path

from alembic import op
from sqlalchemy import text

from migrations.extensions.helpers import for_each_tenant_schema
from robosystems.utils.uuid import generate_deterministic_uuid

# revision identifiers, used by Alembic.
revision = "0044"
down_revision = "0043"
branch_labels = None
depends_on = None

# tenant-fanout: not-required (data only; every tenant is reached through
# resync_library_into_tenant below)

# In load order. Elements and structures seed before arcs resolve them.
_PINS = {
  "rs-gaap": "v1",
  "rs-gaap-traits": "v1",
  "rs-gaap-presentation": "v1",
  "rs-gaap-calculations": "v1",
  "rs-gaap-reporting-styles": "v1",
  "rs-gaap-rollup-rules": "v1",
}

_NEW_CONCEPTS = frozenset(
  {
    "rs-gaap:ProceedsFromShortTermDebt",
    "rs-gaap:RepaymentsOfShortTermDebt",
    "rs-gaap:PaymentsToAcquireShortTermInvestments",
    "rs-gaap:ProceedsFromSaleOfShortTermInvestments",
  }
)

# Packages whose arcs or assignments changed; the others changed only in
# elements, structures or rules.
_ARC_PACKAGES = ("rs-gaap-traits", "rs-gaap-calculations", "rs-gaap-presentation")


def _seed_path(standard: str, version: str) -> Path:
  from robosystems.taxonomy.discovery import FRAMEWORKS_DIR

  return (
    FRAMEWORKS_DIR / "rs-gaap" / "packages" / standard / version / "taxonomy.jsonld"
  )


def _refresh_public(conn) -> None:
  """Bring ``public`` up to the committed JSON-LD for every pinned package."""
  from sqlalchemy.orm import Session as _Session

  from robosystems.operations.taxonomy_block.library_creator import (
    create_library_arcs,
    create_library_taxonomy_elements,
  )
  from robosystems.taxonomy import load_taxonomy_package

  packages = {
    standard: load_taxonomy_package(_seed_path(standard, version))
    for standard, version in _PINS.items()
  }
  session = _Session(bind=conn)
  try:
    # 1. The four concepts and the two cash flow networks. Every other
    #    element and structure already exists and is left alone.
    for standard, package in packages.items():
      only_new = package.model_copy(
        update={"elements": [e for e in package.elements if e.qname in _NEW_CONCEPTS]}
      )
      _taxonomy_id, counts = create_library_taxonomy_elements(session, only_new)
      session.flush()
      print(
        f"  [{standard}] elements={counts['elements']} "
        f"structures={counts['structures']}"
      )

    # 2. Arcs and trait assignments; value fixes update in place, re-pointed
    #    arcs insert, and arcs the seed no longer emits retire.
    for standard in _ARC_PACKAGES:
      counts = create_library_arcs(session, packages[standard])
      session.flush()
      print(
        f"  [{standard}] arcs={counts['associations']} "
        f"skipped={counts['associations_skipped']} "
        f"retired={counts['associations_retired']} "
        f"traits={counts['trait_assignments']}"
      )
  finally:
    session.close()

  _update_public_rules(conn, packages["rs-gaap-rollup-rules"])


def _update_public_rules(conn, package) -> None:
  """Rewrite the rollup rules' expression and variables in place (rule ids
  key on ``standard:local_id``, which the regeneration preserved)."""
  from robosystems.operations.taxonomy_block.library_creator import _rule_id

  updated = 0
  for rule in package.rules:
    result = conn.execute(
      text("""
        UPDATE public.rules
           SET rule_expression = :expr,
               rule_variables  = CAST(:vars AS jsonb)
         WHERE id = :id
           AND (rule_expression IS DISTINCT FROM :expr
                OR rule_variables IS DISTINCT FROM CAST(:vars AS jsonb))
      """),
      {
        "id": _rule_id(package.standard, rule.id),
        "expr": rule.rule_expression,
        "vars": json.dumps(
          [
            {"variableName": v.variable_name, "variableQname": v.variable_qname}
            for v in rule.rule_variables
          ]
        ),
      },
    )
    updated += result.rowcount or 0
  print(f"  [{package.standard}] rules updated in public={updated}")


def _cash_flow_compositions() -> list[tuple[str, str]]:
  """``[(style structure id, cash flow network structure id)]`` from the
  reporting-styles package, for every Style that composes one."""
  graph = json.loads(_seed_path("rs-gaap-reporting-styles", "v1").read_text()).get(
    "@graph", []
  )
  pairs: list[tuple[str, str]] = []
  for node in graph:
    networks = node.get("reportingStyleNetworks")
    role_uri = node.get("roleUri")
    if not networks or not isinstance(role_uri, str):
      continue
    for net in networks:
      if net["statementType"] == "cash_flow_statement":
        pairs.append(
          (
            generate_deterministic_uuid(role_uri, namespace="structure"),
            generate_deterministic_uuid(net["networkRoleUri"], namespace="structure"),
          )
        )
  return pairs


def _point_styles_at_their_cash_flow(conn, schema: str) -> None:
  """Repoint each Style's ``cash_flow_statement`` row at the network the
  package now declares, where that network exists in ``schema``."""
  table = (
    "public.reporting_style_networks"
    if schema == "public"
    else f'"{schema}".reporting_style_networks'
  )
  structures = "public.structures" if schema == "public" else f'"{schema}".structures'
  repointed = 0
  for style_id, network_id in _cash_flow_compositions():
    result = conn.execute(
      text(f"""
        UPDATE {table}
           SET network_id = :network
         WHERE reporting_style_id = :style
           AND statement_type = 'cash_flow_statement'
           AND network_id <> :network
           AND EXISTS (SELECT 1 FROM {structures} s WHERE s.id = :network)
      """),
      {"style": style_id, "network": network_id},
    )
    repointed += result.rowcount or 0
  if repointed:
    print(f"  [cash flow networks -> {schema}] repointed={repointed}")


def upgrade() -> None:
  from robosystems.taxonomy.writer import SET_LIBRARY_RESYNC, resync_library_into_tenant

  conn = op.get_bind()

  # The immutability triggers reject UPDATE and DELETE on library-origin rows
  # without the bypass GUC; SET LOCAL covers the public refresh and the whole
  # tenant fan-out in this transaction.
  conn.execute(text(SET_LIBRARY_RESYNC))

  _refresh_public(conn)
  _point_styles_at_their_cash_flow(conn, "public")

  def _resync(conn, schema: str) -> None:
    stats = resync_library_into_tenant(conn, schema, pin=dict(_PINS))
    print(
      f"  [flow coverage -> {schema}] elements={stats.elements} "
      f"structures={stats.structures} associations={stats.associations} "
      f"element_traits={stats.element_traits} rules={stats.rules}"
    )
    _point_styles_at_their_cash_flow(conn, schema)

  for_each_tenant_schema(conn, _resync)


def downgrade() -> None:
  """Intentionally a no-op.

  The upgrade brings library content up to the committed framework source;
  restoring the narrower defaults and rows would put an LLC's financing
  back into the operating adjustment. Rolling the schema back does not
  require rolling the taxonomy back; re-run the upgrade after any
  downgrade/upgrade cycle if content ever diverges.
  """
