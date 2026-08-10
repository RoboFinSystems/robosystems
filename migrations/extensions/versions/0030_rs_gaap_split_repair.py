"""Propagate the rs-gaap split repair into public and every tenant schema.

The 2026-06-06 Default Style split (``bfa0ebc7``) replaced four combined
leaves and orphaned every reference keyed on them. Two packages carried
those references and neither has ever been re-propagated:

* ``rs-gaap-rollup-rules/v1`` — six RollUp rules enumerated the replaced
  concepts. The engine derives RollUp children from live calc arcs on the
  normal path, but falls back to the frozen enumeration when a parent has
  no calc children in the merged DAG, where each replaced concept binds
  nothing, scores 0, and the subtotal fails by exactly the balance that
  should have counted. Observed live on an entity graph.
* ``rs-gaap-calculations/v1`` — four cash-flow ``derivation`` arcs pointed
  at concepts no calc network wires, so long-term debt and intangible
  flows could never reach investing/financing and fell through to
  operating working capital. Observed live: an $80,000 note issuance
  rendered with financing at zero.

The library copy at provision time is additive (``ON CONFLICT DO NOTHING``)
and no migration has ever pinned either package for resync, so provisioned
tenants still carry the pre-split rows.

Two things resync cannot do, handled explicitly here:

* ``resync_library_into_tenant`` never deletes (retirement propagation is
  out of scope), and
* an association's deterministic id keys on ``to_element_id``, so a
  repointed arc is a **new row**, not an update — the pre-split row would
  otherwise survive alongside its replacement and double-count the flow.

So the stale association rows are deleted by id, in public and in every
tenant, and the replacements arrive as inserts.

Rule ids key on ``standard:local_id``, which the repair preserved, so the
six rules genuinely update in place.

Revision ID: 0030
Revises: 0029
"""

from __future__ import annotations

from alembic import op
from sqlalchemy import text

from migrations.extensions.helpers import for_each_tenant_schema

revision = "0030"
down_revision = "0029"
branch_labels = None
depends_on = None

_PINS = {
  "rs-gaap-rollup-rules": "v1",
  "rs-gaap-calculations": "v1",
}

# Association rows orphaned by the split. Ids are the deterministic UUID5s
# of (structure, from, to, type) as they existed pre-repair; recomputed from
# library_creator._association_id and pinned here so the delete is exact and
# cannot match a tenant-authored arc.
_STALE_ASSOCIATION_IDS = (
  # ProceedsFromIssuanceOfLongTermDebt -> LongTermDebtNoncurrent
  "46c82ecf-b249-5e62-a7b4-c12c1ed33930",
  # RepaymentsOfLongTermDebt -> LongTermDebtNoncurrent
  "136ec98e-6a45-5359-bade-141503ed2e3b",
  # PaymentsToAcquireIntangibleAssets -> IntangibleAssetsNetIncludingGoodwill
  "c4038c29-fe40-5778-bafe-ad4630a02c32",
  # ProceedsFromSaleOfIntangibleAssets -> IntangibleAssetsNetIncludingGoodwill
  "fdc22d75-ae5a-5e1b-a811-582c7f34d00f",
  # IncreaseDecreaseInAccountsPayableAndAccruedLiabilities ->
  # AccountsPayableAndAccruedLiabilitiesCurrent (superseded by the split;
  # deriv-cf-ap-arc-2 and deriv-cf-accrued-liabilities-arc-1 carry these flows)
  "f00f7905-a89a-5f8f-8789-2f8a9b53c72e",
)


def _seed_path(standard: str, version: str):
  from robosystems.taxonomy.discovery import FRAMEWORKS_DIR

  return (
    FRAMEWORKS_DIR / "rs-gaap" / "packages" / standard / version / "taxonomy.jsonld"
  )


def _refresh_public_package(conn, standard: str, version: str) -> None:
  """Bring public.* up to the corrected JSON-LD for one package.

  ``create_library_*`` skip rows that already exist, so they insert the four
  repointed arcs (new ids) and leave everything else alone; the six rules are
  updated in place below, since their ids did not change.
  """
  from sqlalchemy.orm import Session as _Session

  from robosystems.operations.taxonomy_block.library_creator import (
    create_library_arcs,
    create_library_rules,
  )
  from robosystems.taxonomy import load_taxonomy_package

  package = load_taxonomy_package(_seed_path(standard, version))
  session = _Session(bind=conn)
  try:
    counts = create_library_arcs(session, package)
    session.flush()
    print(
      f"  [{standard} arcs] inserted={counts['associations']} "
      f"skipped={counts['associations_skipped']}"
    )
    counts = create_library_rules(session, package)
    session.flush()
    print(
      f"  [{standard} rules] inserted={counts['rules']} "
      f"skipped={counts['rules_skipped']}"
    )
  finally:
    session.close()


def _update_public_rules(conn, standard: str, version: str) -> None:
  """Rewrite rule_expression / rule_variables for an existing package.

  ``create_library_rules`` skips rows that already exist, so the six
  corrected enumerations need an explicit update keyed on the stable
  ``standard:local_id`` rule id.
  """
  import json

  from robosystems.operations.taxonomy_block.library_creator import _rule_id
  from robosystems.taxonomy import load_taxonomy_package

  package = load_taxonomy_package(_seed_path(standard, version))
  updated = 0
  for rule in package.rules:
    rid = _rule_id(standard, rule.id)
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
        "id": rid,
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
  print(f"  [{standard} rules] updated in public={updated}")


def _delete_stale_associations(conn, schema: str) -> None:
  result = conn.execute(
    text(f"""
      DELETE FROM {schema}.associations
       WHERE id = ANY(:ids)
         AND created_by = 'library-seeder'
    """),
    {"ids": list(_STALE_ASSOCIATION_IDS)},
  )
  if result.rowcount:
    print(f"  [stale arcs -> {schema}] deleted={result.rowcount}")


def upgrade() -> None:
  from robosystems.taxonomy.writer import SET_LIBRARY_RESYNC, resync_library_into_tenant

  conn = op.get_bind()

  # The immutability triggers reject UPDATE *and* DELETE on library-origin
  # rows without the bypass GUC; SET LOCAL is transaction-scoped and covers
  # the public fixes and the whole tenant fan-out.
  conn.execute(text(SET_LIBRARY_RESYNC))

  # 1. Public library — insert the repointed arcs, update the corrected
  #    rules, then drop the rows the split orphaned.
  for standard, version in _PINS.items():
    _refresh_public_package(conn, standard, version)
  _update_public_rules(conn, "rs-gaap-rollup-rules", "v1")
  _delete_stale_associations(conn, "public")

  # 2. Fan both packages into every provisioned tenant. Rules update in
  #    place; the repointed arcs arrive as inserts.
  def _resync(conn, schema: str) -> None:
    stats = resync_library_into_tenant(conn, schema, pin=dict(_PINS))
    print(
      f"  [rs-gaap split repair -> {schema}] "
      f"associations={stats.associations} rules={stats.rules}"
    )

  for_each_tenant_schema(conn, _resync)

  # 3. Remove the orphaned rows resync cannot delete. After the insert above
  #    a tenant would otherwise hold both the pre- and post-split arc and
  #    double-count the flow.
  for_each_tenant_schema(conn, _delete_stale_associations)


def downgrade() -> None:
  """Intentionally a no-op.

  The upgrade restores library content to match the committed framework
  source. Reinstating the pre-split arcs would reintroduce a defect that
  misstates cash-flow classification, and the corrected rule enumerations
  are inert on the normal evaluation path. Rolling the schema back does not
  require rolling the taxonomy back; re-run the upgrade after any
  downgrade/upgrade cycle if content ever diverges.
  """
