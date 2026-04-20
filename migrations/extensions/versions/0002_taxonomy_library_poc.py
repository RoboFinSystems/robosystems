"""Taxonomy library POC — JSON-LD seeds + multi-axis classification +
provision-time library copy + immutability + CoA SFAC 6 anchor tagging.

Replaces the Python-dict seed (`seed_reporting_taxonomy`) with JSON-LD
artifacts loaded from `robosystems/taxonomy/seeds/`. Seeds are RoboSystems
canonical forks — originally bootstrapped from Charlie Hoffman's published
XBRL, but now maintained directly as JSON-LD with no upstream tracking
(see `robosystems/taxonomy/seeds/README.md` and the one-shot importer at
`robosystems/scripts/import_upstream_seeds.py`).

Schema changes (public schema):

- CREATE TABLE public.element_labels
- CREATE TABLE public.element_references
- CREATE TABLE public.classifications (OLTP mirror of graph
  Classification node, with a `category` axis — see
  `local/docs/specs/information-modeling.md`)
- CREATE TABLE public.element_classifications (many-to-many junction)
- Widen `source` CHECK on elements to include 'fac', 'rs-gaap'
- Widen `association_type` CHECK on associations to include 'equivalence',
  'general-special', 'essence-alias'
- Relax `classification` CHECK to allow NULL and the 7 final SFAC 6
  primitives (asset | liability | equity | revenue | expense | gain |
  loss). Contributions, distributions, and comprehensive income collapse
  into `equity` — direction is captured by balance_type, and
  equity_changes statement_context preserves the BS-vs-flow distinction.
- ADD COLUMN elements.statement_context (balance_sheet | income_statement |
  cash_flow | equity_changes | disclosure | metadata | analysis), nullable
- ADD COLUMN elements.derivation_role (primitive | subtotal | total |
  reconciliation | movement | ratio | identifier | structural), nullable

Data changes (public schema):

- DELETE existing library-origin rows seeded by 0001's seed_reporting_taxonomy
  (coexistence would collide on qname uniqueness)
- INSERT new rows from JSON-LD seeds (library_writer populates both the
  denormalized columns AND the classifications junction)
- Propagate classifications from FAC → rs-gaap via equivalence arcs +
  general-special hierarchy + name patterns

Tenant-schema rollout (for every existing tenant schema `kg*`):

- Widen association_type / element source CHECKs to admit library
  vocabulary (equivalence, general-special, essence-alias; fac, rs-gaap)
- Copy pinned library rows from public.* into the tenant schema using
  the idempotent `copy_library_into_tenant` helper (row ids preserved so
  re-runs are no-ops). Respects each graph's `taxonomy_pin`; falls back
  to DEFAULT_TAXONOMY_PIN for graphs without a pin.
- Install `BEFORE UPDATE OR DELETE` triggers on the six library-backing
  tables (taxonomies, elements, element_labels, element_references,
  structures, associations) keyed on `created_by = 'library-seeder'` so
  tenant-scope writes can't mutate library rows.
- Tag every tenant-origin element with a non-null `classification` to its
  SFAC 6 anchor (or `fac:NetCashFlowFromOperatingActivities` for
  `classification='cashflow'`) via a `class-subClassOf` association.
  Gives MappingAgent a structural narrowing signal — FAC descendants of
  the anchor become the candidate set instead of the full rs-gaap space.

The immutability function `public.raise_library_immutable()` is installed
once (idempotent) and shared by every tenant trigger.

Revision ID: 0002
Revises: 0001
Create Date: 2026-04-17
"""

from __future__ import annotations

from pathlib import Path

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text
from sqlalchemy.dialects import postgresql

from migrations.extensions.helpers import TenantOps, for_each_tenant_schema
from robosystems.taxonomy.writers.tenant_writer import copy_library_into_tenant

# revision identifiers, used by Alembic.
revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


# ── Consolidated from the former 0003 + 0004 migrations ─────────────────

_IMMUTABLE_TABLES = (
  "taxonomies",
  "elements",
  "element_labels",
  "element_references",
  "structures",
  "associations",
)

_WIDENED_ASSOCIATION_CHECK = (
  "association_type IN ("
  "'presentation', 'calculation', 'mapping', "
  "'equivalence', 'general-special', 'essence-alias'"
  ")"
)
_WIDENED_ELEMENT_SOURCE_CHECK = (
  "source IN ("
  "'sfac6', 'fac', 'rs-gaap', 'us-gaap', 'ifrs', "
  "'quickbooks', 'xero', 'plaid', 'native', 'import', 'system'"
  ")"
)
_NARROW_ASSOCIATION_CHECK = (
  "association_type IN ('presentation', 'calculation', 'mapping')"
)
_NARROW_ELEMENT_SOURCE_CHECK = (
  "source IN ("
  "'sfac6', 'us-gaap', 'ifrs', "
  "'quickbooks', 'xero', 'plaid', 'native', 'import'"
  ")"
)

# classification → SFAC 6 (or cashflow) anchor qname. `inflow` collapses
# Gains into Revenues; `outflow` collapses Losses into Expenses.
_ANCHOR_QNAMES = {
  "asset": "sfac6:Assets",
  "liability": "sfac6:Liabilities",
  "equity": "sfac6:Equity",
  "inflow": "sfac6:Revenues",
  "outflow": "sfac6:Expenses",
  "cashflow": "fac:NetCashFlowFromOperatingActivities",
}

_COA_STRUCTURE_NAME = "CoA Classification Anchors"
_COA_STRUCTURE_DESC = (
  "Tenant CoA elements anchored to their SFAC 6 (or cashflow) classification "
  "via class-subClassOf arcs. Used by MappingAgent to narrow reporting-concept "
  "candidates by walking anchor descendants."
)
_COA_ARCROLE = "class-subClassOf"


def _install_raise_library_immutable_fn(conn) -> None:
  """Create the PL/pgSQL function that raises on library-origin mutations."""
  conn.execute(
    text("""
      CREATE OR REPLACE FUNCTION public.raise_library_immutable()
      RETURNS TRIGGER AS $$
      BEGIN
        IF OLD.created_by = 'library-seeder' THEN
          RAISE EXCEPTION 'library-seeded rows are immutable in tenant schemas (table=%, id=%)',
            TG_TABLE_NAME, OLD.id
            USING ERRCODE = 'P0001';
        END IF;
        IF TG_OP = 'DELETE' THEN
          RETURN OLD;
        END IF;
        RETURN NEW;
      END;
      $$ LANGUAGE plpgsql;
    """)
  )


def _install_triggers_for_tenant(conn, schema: str) -> None:
  for table in _IMMUTABLE_TABLES:
    trigger = f"{table}_library_immutable"
    conn.execute(text(f'DROP TRIGGER IF EXISTS {trigger} ON "{schema}".{table}'))
    conn.execute(
      text(
        f"CREATE TRIGGER {trigger} "
        f'BEFORE UPDATE OR DELETE ON "{schema}".{table} '
        f"FOR EACH ROW EXECUTE FUNCTION public.raise_library_immutable()"
      )
    )


def _drop_triggers_for_tenant(conn, schema: str) -> None:
  for table in _IMMUTABLE_TABLES:
    trigger = f"{table}_library_immutable"
    conn.execute(text(f'DROP TRIGGER IF EXISTS {trigger} ON "{schema}".{table}'))


def _widen_tenant_checks(conn, schema: str) -> None:
  t = TenantOps(conn, schema)
  t.add_check("associations", "check_association_type", _WIDENED_ASSOCIATION_CHECK)
  t.add_check("elements", "check_element_source", _WIDENED_ELEMENT_SOURCE_CHECK)


def _restore_narrow_tenant_checks(conn, schema: str) -> None:
  t = TenantOps(conn, schema)
  t.add_check("associations", "check_association_type", _NARROW_ASSOCIATION_CHECK)
  t.add_check("elements", "check_element_source", _NARROW_ELEMENT_SOURCE_CHECK)


def _backfill_library_into_tenant(conn, schema: str) -> None:
  """Widen CHECKs, copy pinned library rows, install immutability triggers.

  Order is load-bearing: copy runs before trigger install so library
  rows land freely; after the triggers are attached, subsequent
  UPDATE/DELETE on those rows raises from tenant scope.
  """
  _widen_tenant_checks(conn, schema)
  stats = copy_library_into_tenant(conn, schema)
  print(f"  [{schema}] library backfill: {stats.total:,} rows")
  _install_triggers_for_tenant(conn, schema)


def _tag_coa_for_tenant(conn, schema: str) -> None:
  """Tag tenant-origin elements with SFAC 6 anchor associations."""
  # Resolve anchor element ids in this tenant schema by qname.
  rows = conn.execute(
    text(
      f"SELECT qname, id FROM {schema}.elements "
      f"WHERE qname = ANY(:qnames) AND created_by = 'library-seeder'"
    ),
    {"qnames": list(_ANCHOR_QNAMES.values())},
  ).fetchall()
  anchor_ids = {row.qname: row.id for row in rows}
  if not anchor_ids:
    print(f"  [{schema}] coa tagging skipped (no library anchors present)")
    return

  # Ensure the holder Structure exists. Deterministic id qualified by
  # schema so re-runs are idempotent.
  structure_id = f"struct_coa_cls_{schema.replace('-', '_')}"
  existing = conn.execute(
    text(f"SELECT id FROM {schema}.structures WHERE id = :id"),
    {"id": structure_id},
  ).fetchone()
  if existing is None:
    tax_row = conn.execute(
      text(
        f"SELECT id FROM {schema}.taxonomies "
        f"WHERE created_by != 'library-seeder' "
        f"ORDER BY created_at LIMIT 1"
      )
    ).fetchone()
    taxonomy_id = tax_row.id if tax_row else None
    conn.execute(
      text(
        f"INSERT INTO {schema}.structures "
        f"(id, name, description, structure_type, taxonomy_id, is_active, "
        f" metadata, created_at, updated_at, created_by) "
        f"VALUES (:id, :name, :desc, 'coa_mapping', :tax, true, '{{}}'::jsonb, "
        f" now(), now(), 'coa-classifier')"
      ),
      {
        "id": structure_id,
        "name": _COA_STRUCTURE_NAME,
        "desc": _COA_STRUCTURE_DESC,
        "tax": taxonomy_id,
      },
    )

  inserted = 0
  for classification, qname in _ANCHOR_QNAMES.items():
    anchor_id = anchor_ids.get(qname)
    if anchor_id is None:
      continue
    result = conn.execute(
      text(f"""
        INSERT INTO {schema}.associations (
          id, structure_id, from_element_id, to_element_id,
          association_type, arcrole,
          order_value, weight, confidence, suggested_by, approved_by,
          approved_at, metadata, created_at, updated_at, created_by
        )
        SELECT
          'assoc_coa_' || substr(md5(e.id || ':' || :anchor_id), 1, 22),
          :structure_id, e.id, :anchor_id,
          'mapping', :arcrole,
          NULL, NULL, NULL, 'coa-classifier', 'coa-classifier',
          now(), '{{}}'::jsonb, now(), now(), 'coa-classifier'
        FROM {schema}.elements e
        WHERE e.classification = :classification
          AND e.created_by != 'library-seeder'
          AND NOT EXISTS (
            SELECT 1 FROM {schema}.associations a
            WHERE a.from_element_id = e.id
              AND a.to_element_id = :anchor_id
              AND a.arcrole = :arcrole
          )
      """),
      {
        "structure_id": structure_id,
        "anchor_id": anchor_id,
        "arcrole": _COA_ARCROLE,
        "classification": classification,
      },
    )
    inserted += result.rowcount or 0
  print(f"  [{schema}] coa anchor arcs inserted: {inserted}")


def _untag_coa_for_tenant(conn, schema: str) -> None:
  conn.execute(
    text(
      f"DELETE FROM {schema}.associations "
      f"WHERE created_by = 'coa-classifier' AND arcrole = :arcrole"
    ),
    {"arcrole": _COA_ARCROLE},
  )
  conn.execute(
    text(f"DELETE FROM {schema}.structures WHERE created_by = 'coa-classifier'")
  )


SEEDS_DIR = (
  Path(__file__).parent.parent.parent.parent / "robosystems" / "taxonomy" / "seeds"
)

SEED_FILES = [
  # Concept taxonomies first — mapping taxonomies reference them by qname.
  SEEDS_DIR / "sfac6" / "v1" / "taxonomy.jsonld",
  SEEDS_DIR / "fac" / "v1" / "taxonomy.jsonld",
  SEEDS_DIR / "rs-gaap" / "v1" / "taxonomy.jsonld",
  # Mapping taxonomies: arc-only linkbases bridging concept taxonomies.
  SEEDS_DIR / "sfac6-to-fac" / "v1" / "taxonomy.jsonld",
  SEEDS_DIR / "fac-to-rs-gaap" / "v1" / "taxonomy.jsonld",
  SEEDS_DIR / "fac-calculations" / "v1" / "taxonomy.jsonld",
  SEEDS_DIR / "fac-presentation" / "v1" / "taxonomy.jsonld",
  SEEDS_DIR / "type-subtype" / "v1" / "taxonomy.jsonld",
  SEEDS_DIR / "rs-gaap-presentation" / "v1" / "taxonomy.jsonld",
]

# Map Charlie's 20 type-subtype networks to the 5 canonical classifications.
# Cash flow + disclosure networks don't cleanly fit asset/liability/equity/
# revenue/expense — they keep whatever classification the extractor heuristic
# assigned (POC compromise; fixing the CHECK to add 'cashflow'/'disclosure'
# is a Phase 1 cleanup).
NETWORK_TO_CLASSIFICATION: dict[str, str] = {
  "CurrentAssets": "asset",
  "NoncurrentAssets": "asset",
  "CurrentLiabilities": "liability",
  "NoncurrentLiabilities": "liability",
  "EquityAttributableToParent": "equity",
  "EquityAttributableToNoncontrollingInterest": "equity",
  "TemporaryEquity": "equity",
  "Revenues": "revenue",
  "OtherOperatingIncome": "revenue",
  "CostOfRevenue": "expense",
  "OperatingExpenses": "expense",
  "NonoperatingIncomeExpenses": "expense",
}


def upgrade() -> None:
  # ──────────────────────────────────────────────────────────────────────
  # 1. New tables for XBRL label + reference linkbases
  # ──────────────────────────────────────────────────────────────────────
  op.create_table(
    "element_labels",
    sa.Column("id", sa.String(), nullable=False),
    sa.Column("element_id", sa.String(), nullable=False),
    sa.Column("role", sa.String(), nullable=False, server_default="standard"),
    sa.Column("language", sa.String(), nullable=False, server_default="en"),
    sa.Column("text", sa.Text(), nullable=False),
    sa.Column(
      "created_at",
      sa.DateTime(),
      nullable=False,
      server_default=sa.text("now()"),
    ),
    sa.Column("created_by", sa.String(), nullable=False, server_default="system"),
    sa.ForeignKeyConstraint(["element_id"], ["elements.id"]),
    sa.PrimaryKeyConstraint("id"),
    sa.UniqueConstraint(
      "element_id",
      "role",
      "language",
      name="uq_element_labels_element_role_language",
    ),
    sa.CheckConstraint(
      "role IN ("
      "'standard', 'verbose', 'terse', 'documentation', "
      "'periodStart', 'periodEnd', 'negated', 'total', "
      "'commentaryGuidance', 'deprecatedLabel', 'other'"
      ")",
      name="check_element_label_role",
    ),
  )
  op.create_index("idx_element_labels_element", "element_labels", ["element_id"])
  op.create_index("idx_element_labels_role", "element_labels", ["role"])

  op.create_table(
    "element_references",
    sa.Column("id", sa.String(), nullable=False),
    sa.Column("element_id", sa.String(), nullable=False),
    sa.Column("ref_type", sa.String(), nullable=True),
    sa.Column("citation", sa.String(), nullable=False),
    sa.Column("uri", sa.String(), nullable=True),
    sa.Column("attributes", sa.String(), nullable=True),
    sa.Column(
      "created_at",
      sa.DateTime(),
      nullable=False,
      server_default=sa.text("now()"),
    ),
    sa.Column("created_by", sa.String(), nullable=False, server_default="system"),
    sa.ForeignKeyConstraint(["element_id"], ["elements.id"]),
    sa.PrimaryKeyConstraint("id"),
  )
  op.create_index(
    "idx_element_references_element", "element_references", ["element_id"]
  )
  op.create_index("idx_element_references_type", "element_references", ["ref_type"])

  # ──────────────────────────────────────────────────────────────────────
  # 1b. Classification registry + junction (OLTP mirror of graph
  #     Classification node, extended with a `category` axis).
  # ──────────────────────────────────────────────────────────────────────
  op.create_table(
    "classifications",
    sa.Column("id", sa.String(), nullable=False),
    sa.Column("category", sa.String(), nullable=False),
    sa.Column("identifier", sa.String(), nullable=False),
    sa.Column("type", sa.String(), nullable=False),
    sa.Column("name", sa.String(), nullable=True),
    sa.Column("description", sa.String(), nullable=True),
    sa.Column("confidence", sa.Float(), nullable=True),
    sa.Column("source", sa.String(), nullable=True),
    sa.Column("metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column("created_at", sa.DateTime(), nullable=False),
    sa.Column("updated_at", sa.DateTime(), nullable=False),
    sa.Column("created_by", sa.String(), nullable=False),
    sa.CheckConstraint(
      "category IN ("
      "'economic_nature', 'statement_context', 'derivation_role', "
      "'concept_arrangement', 'member_arrangement', 'named_disclosure'"
      ")",
      name="check_classification_category",
    ),
    sa.PrimaryKeyConstraint("id"),
    sa.UniqueConstraint(
      "category",
      "identifier",
      "type",
      name="uq_classification_category_identifier_type",
    ),
  )
  op.create_index("idx_classifications_category", "classifications", ["category"])
  op.create_index("idx_classifications_type", "classifications", ["type"])

  op.create_table(
    "element_classifications",
    sa.Column("element_id", sa.String(), nullable=False),
    sa.Column("classification_id", sa.String(), nullable=False),
    sa.Column("is_primary", sa.Boolean(), nullable=False, server_default=sa.true()),
    sa.Column("confidence", sa.Float(), nullable=True),
    sa.Column("source", sa.String(), nullable=True),
    sa.Column("created_at", sa.DateTime(), nullable=False),
    sa.Column("updated_at", sa.DateTime(), nullable=False),
    sa.Column("created_by", sa.String(), nullable=False),
    sa.ForeignKeyConstraint(["classification_id"], ["classifications.id"]),
    sa.ForeignKeyConstraint(["element_id"], ["elements.id"]),
    sa.PrimaryKeyConstraint("element_id", "classification_id"),
  )
  op.create_index(
    "idx_element_classifications_classification",
    "element_classifications",
    ["classification_id"],
  )
  op.create_index(
    "idx_element_classifications_primary",
    "element_classifications",
    ["element_id", "is_primary"],
    postgresql_where="is_primary = true",
  )

  # ──────────────────────────────────────────────────────────────────────
  # 1c. New axis columns on elements (statement_context + derivation_role)
  # ──────────────────────────────────────────────────────────────────────
  op.add_column("elements", sa.Column("statement_context", sa.String(), nullable=True))
  op.add_column("elements", sa.Column("derivation_role", sa.String(), nullable=True))
  op.create_index("idx_elements_statement_context", "elements", ["statement_context"])
  op.create_index("idx_elements_derivation_role", "elements", ["derivation_role"])
  op.create_check_constraint(
    "check_element_statement_context",
    "elements",
    "statement_context IS NULL OR statement_context IN ("
    "'balance_sheet', 'income_statement', 'cash_flow', "
    "'equity_changes', 'disclosure', 'metadata', 'analysis'"
    ")",
  )
  op.create_check_constraint(
    "check_element_derivation_role",
    "elements",
    "derivation_role IS NULL OR derivation_role IN ("
    "'primitive', 'aggregate', "
    "'ratio', 'identifier', 'structural'"
    ")",
  )

  # ──────────────────────────────────────────────────────────────────────
  # 2. Widen CHECK constraints
  # ──────────────────────────────────────────────────────────────────────
  op.drop_constraint("check_element_source", "elements", type_="check")
  op.create_check_constraint(
    "check_element_source",
    "elements",
    "source IN ("
    "'sfac6', 'fac', 'rs-gaap', 'us-gaap', 'ifrs', "
    "'quickbooks', 'xero', 'plaid', 'native', 'import', 'system'"
    ")",
  )

  op.drop_constraint("check_association_type", "associations", type_="check")
  op.create_check_constraint(
    "check_association_type",
    "associations",
    "association_type IN ("
    "'presentation', 'calculation', 'mapping', "
    "'equivalence', 'general-special', 'essence-alias'"
    ")",
  )

  # Make classification nullable (flips for structural/metadata rows).
  op.alter_column(
    "elements", "classification", existing_type=sa.VARCHAR(), nullable=True
  )

  # ──────────────────────────────────────────────────────────────────────
  # 3. Clear 0001's seeded library data BEFORE tightening the classification
  #    CHECK — 0001 plants rows with legacy 'revenue'/'expense' values
  #    that would violate the new 6-value vocabulary.
  # ──────────────────────────────────────────────────────────────────────
  conn = op.get_bind()

  # Delete in FK-safe order: associations → structures → elements → taxonomies
  conn.execute(
    sa.text(
      """
      DELETE FROM public.associations
      WHERE structure_id IN (
        SELECT id FROM public.structures
        WHERE taxonomy_id IN (
          SELECT id FROM public.taxonomies WHERE is_shared = true
        )
      )
      """
    )
  )
  conn.execute(
    sa.text(
      "DELETE FROM public.structures WHERE taxonomy_id IN "
      "(SELECT id FROM public.taxonomies WHERE is_shared = true)"
    )
  )
  conn.execute(
    sa.text(
      "DELETE FROM public.elements WHERE taxonomy_id IN "
      "(SELECT id FROM public.taxonomies WHERE is_shared = true)"
    )
  )
  conn.execute(sa.text("DELETE FROM public.taxonomies WHERE is_shared = true"))

  # Now that the legacy rows are gone, tighten the classification CHECK
  # to the final 6-value vocabulary + NULL.
  #   asset | liability | equity  (balance-sheet stocks)
  #   inflow | outflow             (R+G collapse → inflow, E+L → outflow)
  #   cashflow                     (CF-statement reconciliations + movements)
  # Nullable for structural rows, metadata, and computed ratios.
  op.drop_constraint("check_element_classification", "elements", type_="check")
  op.create_check_constraint(
    "check_element_classification",
    "elements",
    "classification IS NULL OR classification IN ("
    "'asset', 'liability', 'equity', "
    "'inflow', 'outflow', 'cashflow'"
    ")",
  )

  # ──────────────────────────────────────────────────────────────────────
  # 4. Load JSON-LD seed artifacts
  # ──────────────────────────────────────────────────────────────────────
  # Deferred imports so the migration module loads without taxonomy code
  # available at collection time.
  from robosystems.taxonomy.loaders import load_taxonomy_package
  from robosystems.taxonomy.writers import (
    sync_element_classifications_bulk,
    write_taxonomy_package,
  )

  for seed_path in SEED_FILES:
    if not seed_path.exists():
      # POC — seed files are committed. Missing file is an error in
      # production; allow a skip for partial-build scenarios during
      # development with a clear log.
      print(f"  [WARN] Seed file missing, skipping: {seed_path}")
      continue

    print(f"  Loading seed: {seed_path.relative_to(SEEDS_DIR)}")
    package = load_taxonomy_package(seed_path)
    counts = write_taxonomy_package(conn, package)
    print(
      f"    → {counts['elements']} elements, {counts['labels']} labels, "
      f"{counts['references']} references, {counts['structures']} structures, "
      f"{counts['associations']} associations"
    )

  # ──────────────────────────────────────────────────────────────────────
  # 5. Propagate FAC classifications to rs-gaap via equivalence arcs
  # ──────────────────────────────────────────────────────────────────────
  # The extractor's balance+period heuristic misclassifies thousands of
  # rs-gaap concepts (credit-balance duration adjustments all land in
  # 'revenue'). FAC concepts are hand-classified correctly by Charlie,
  # and fac:X ≡ rs-gaap:Y arcs (221 of them) tell us which rs-gaap
  # concepts inherit which classification. We walk the equivalence arcs
  # and propagate.
  #
  # This fixes ~220 rs-gaap concepts with high confidence. The remaining
  # ~16,900 keep their heuristic classification — flagging that as tech
  # debt (Phase 1 work: derive classification from Charlie's type-subtype
  # networks, which requires preserving XBRL role URIs through
  # serialization — not in scope for the POC).
  print("  Propagating FAC classifications to rs-gaap via equivalence arcs…")
  result = conn.execute(
    sa.text(
      """
      UPDATE public.elements AS rs
      SET classification = fac.classification,
          statement_context = fac.statement_context,
          derivation_role = fac.derivation_role,
          updated_at = now()
      FROM public.associations a
      JOIN public.elements fac ON fac.id = a.from_element_id
      WHERE a.to_element_id = rs.id
        AND a.association_type = 'equivalence'
        AND fac.source = 'fac'
        AND rs.source = 'rs-gaap'
        AND (rs.classification IS DISTINCT FROM fac.classification
             OR rs.statement_context IS DISTINCT FROM fac.statement_context
             OR rs.derivation_role IS DISTINCT FROM fac.derivation_role)
      """
    )
  )
  print(f"    rs-gaap reclassified from FAC equivalence: {result.rowcount} elements")

  # Also propagate classification via general-special hierarchy from
  # already-classified rs-gaap concepts (fixed point iteration). When
  # rs-gaap:CostOfGoodsSold has classification='expense' (from FAC
  # equivalence), all its descendants via general-special arcs inherit
  # 'expense' too. Run a few passes until no more updates happen.
  print("  Propagating via general-special hierarchy…")
  total_propagated = 0
  for pass_num in range(1, 6):
    hop_result = conn.execute(
      sa.text(
        """
        UPDATE public.elements AS child
        SET classification = parent.classification,
            statement_context = COALESCE(child.statement_context, parent.statement_context),
            derivation_role = COALESCE(child.derivation_role, parent.derivation_role),
            updated_at = now()
        FROM public.associations a
        JOIN public.elements parent ON parent.id = a.from_element_id
        WHERE a.to_element_id = child.id
          AND a.association_type = 'general-special'
          AND parent.source = 'rs-gaap'
          AND child.source = 'rs-gaap'
          AND parent.classification IS DISTINCT FROM child.classification
          -- Only propagate from concepts that were previously reclassified
          -- (either via FAC equivalence or a prior hierarchy hop). Detect
          -- via updated_at being recent relative to the migration.
          AND parent.updated_at > now() - interval '5 minutes'
        """
      )
    )
    hopped = hop_result.rowcount
    total_propagated += hopped
    print(f"    Hop {pass_num}: {hopped} elements")
    if hopped == 0:
      break
  print(f"  Total propagated via hierarchy: {total_propagated}")

  # ──────────────────────────────────────────────────────────────────────
  # 6. Name-pattern fallback for the remaining misclassified noise
  # ──────────────────────────────────────────────────────────────────────
  # Concepts that weren't touched by FAC equivalence or hierarchy
  # propagation still carry the heuristic's wrong classification. The
  # biggest noise categories in the Revenue / Asset buckets are
  # adjustments, accruals, and accumulated contra-asset concepts. These
  # regex-based reclassifications catch the obvious cases.
  print("  Name-pattern cleanup of remaining misclassifications…")

  cleanup_passes = [
    # Accrual liabilities — "AccrualFor*" credit balance concepts
    (
      "Accrual liabilities → liability",
      """
      UPDATE public.elements
      SET classification = 'liability', updated_at = now()
      WHERE source = 'rs-gaap'
        AND balance_type = 'credit'
        AND (name LIKE 'Accrual%' OR name LIKE '%AccruedLiabilit%')
        AND classification != 'liability'
      """,
    ),
    # Accumulated depreciation / amortization — contra-assets
    (
      "AccumulatedDepreciation → asset (contra-asset)",
      """
      UPDATE public.elements
      SET classification = 'asset', updated_at = now()
      WHERE source = 'rs-gaap'
        AND (name LIKE 'AccumulatedDepreciation%'
             OR name LIKE 'AccumulatedAmortization%'
             OR name LIKE 'AccumulatedDepletion%')
        AND classification != 'asset'
      """,
    ),
    # IncreaseDecrease* items are cash-flow movements — even when the
    # name contains "Payable" or "Liability" they're movements of that
    # account in the cash flow statement, not the balance-sheet position.
    (
      "IncreaseDecrease* → cashflow",
      """
      UPDATE public.elements
      SET classification = 'cashflow', updated_at = now()
      WHERE source = 'rs-gaap'
        AND name LIKE 'IncreaseDecreaseIn%'
        AND classification IS DISTINCT FROM 'cashflow'
      """,
    ),
    # Adjustment concepts with credit balance — usually equity or liability
    # adjustments, not inflow.
    (
      "AdjustmentFor* → equity (AOCI-style) or liability",
      """
      UPDATE public.elements
      SET classification = CASE
        WHEN name LIKE '%Stock%' OR name LIKE '%Equity%'
             OR name LIKE '%RetainedEarnings%' THEN 'equity'
        ELSE 'liability'
      END, updated_at = now()
      WHERE source = 'rs-gaap'
        AND name LIKE 'AdjustmentFor%'
        AND classification = 'inflow'
      """,
    ),
  ]

  for label, sql in cleanup_passes:
    r = conn.execute(sa.text(sql))
    print(f"    {label}: {r.rowcount} elements")

  # Additional sweeps for specific conceptual categories that tend to
  # leak through the balance+period heuristic into the wrong bucket.
  additional_patterns = [
    # Paid-in capital / APIC adjustments → equity
    (
      "AdjustmentsToAdditionalPaidInCapital → equity",
      """
      UPDATE public.elements SET classification = CASE
        WHEN period_type = 'instant' THEN 'equity'
        WHEN balance_type = 'credit' THEN 'inflow'
        WHEN balance_type = 'debit' THEN 'outflow'
        ELSE classification
      END, updated_at=now()
      WHERE source='rs-gaap'
        AND (name LIKE 'AdjustmentsToAdditionalPaidInCapital%'
             OR name LIKE 'AcceleratedShareRepurchase%'
             OR name LIKE 'StockIssued%' OR name LIKE 'StockRepurchase%'
             OR name LIKE 'CommonStock%' OR name LIKE 'PreferredStock%'
             OR name LIKE 'PaidInCapital%' OR name LIKE 'TreasuryStock%')
      """,
    ),
    # Long-duration insurance liabilities → liability
    (
      "Insurance liability concepts → liability",
      """
      UPDATE public.elements SET classification='liability', updated_at=now()
      WHERE source='rs-gaap'
        AND (name LIKE 'AdditionalLiability%' OR name LIKE 'LiabilityFor%'
             OR name LIKE '%Liability%Due%' OR name LIKE 'InsurancePolicy%'
             OR name LIKE 'DeferredTax%Liability%')
        AND classification NOT IN ('liability', 'expense')
      """,
    ),
    # Depreciation/Amortization/Impairment — duration debit flows.
    (
      "Depreciation/Amortization patterns → outflow",
      """
      UPDATE public.elements SET classification='outflow', updated_at=now()
      WHERE source='rs-gaap'
        AND period_type='duration'
        AND (name LIKE '%Depreciation%' OR name LIKE '%Amortization%'
             OR name LIKE '%Impairment%' OR name LIKE '%Writedown%'
             OR name LIKE '%WriteOff%')
        AND name NOT LIKE 'Accumulated%'
        AND classification NOT IN ('outflow', 'asset')
      """,
    ),
    # Core outflow (expense-like) patterns
    (
      "Expense-shaped patterns → outflow",
      """
      UPDATE public.elements SET classification='outflow', updated_at=now()
      WHERE source='rs-gaap'
        AND period_type='duration'
        AND (name LIKE '%Expense' OR name LIKE '%Expenses'
             OR name LIKE 'CostOf%' OR name LIKE '%Cost')
        AND classification = 'inflow'
      """,
    ),
    # Core inflow (revenue-like) patterns
    (
      "Revenue/Sales/Income patterns → inflow",
      """
      UPDATE public.elements SET classification='inflow', updated_at=now()
      WHERE source='rs-gaap'
        AND period_type='duration'
        AND balance_type='credit'
        AND (name LIKE '%Revenue%' OR name LIKE '%Sales'
             OR name LIKE 'SalesRevenue%' OR name LIKE 'InterestIncome%'
             OR name LIKE 'OperatingIncome%')
        AND classification != 'inflow'
      """,
    ),
    # Remaining duration+credit flows that don't match inflow patterns —
    # usually equity adjustments or deferred liabilities.
    (
      "Remaining duration+credit non-inflow → equity/liability",
      """
      UPDATE public.elements SET classification = CASE
        WHEN name LIKE '%Equity%' OR name LIKE '%RetainedEarnings%'
             OR name LIKE '%Stock%' OR name LIKE '%Dividend%' THEN 'equity'
        ELSE 'liability'
      END, updated_at=now()
      WHERE source='rs-gaap'
        AND classification = 'inflow'
        AND period_type='duration'
        AND balance_type='credit'
        AND name NOT LIKE '%Revenue%'
        AND name NOT LIKE '%Sales%'
        AND name NOT LIKE '%Income%'
        AND name NOT LIKE '%Gain%'
      """,
    ),
  ]
  for label, sql in additional_patterns:
    r = conn.execute(sa.text(sql))
    print(f"    {label}: {r.rowcount} elements")

  # ──────────────────────────────────────────────────────────────────────
  # 7. Authoritative SFAC 6 classification overrides
  # ──────────────────────────────────────────────────────────────────────
  # SFAC 6's 10 concrete primitives collapse into the 6-value vocabulary
  # along period_type lines:
  #   Assets / Liabilities / Equity       → stock primitives (instant)
  #   Revenues / Gains                    → inflow   (credit duration)
  #   InvestmentsByOwners / Comprehensive → inflow   (credit duration,
  #                                                   equity-change flows)
  #   Expenses / Losses                   → outflow  (debit duration)
  #   DistributionsToOwners               → outflow  (debit duration)
  # economic_nature=equity is reserved for balance-sheet stocks; duration
  # flows hit the equity statement via statement_context=equity_changes.
  sfac6_overrides = {
    "sfac6:Assets": "asset",
    "sfac6:Liabilities": "liability",
    "sfac6:Equity": "equity",
    "sfac6:InvestmentsByOwners": "inflow",
    "sfac6:DistributionsToOwners": "outflow",
    "sfac6:ComprehensiveIncome": "inflow",
    "sfac6:Revenues": "inflow",
    "sfac6:Expenses": "outflow",
    "sfac6:Gains": "inflow",
    "sfac6:Losses": "outflow",
  }
  print("  SFAC 6 authoritative classification overrides…")
  for qname, canonical in sfac6_overrides.items():
    r = conn.execute(
      sa.text(
        "UPDATE public.elements SET classification=:c, updated_at=now() "
        "WHERE qname=:q AND classification IS DISTINCT FROM :c"
      ),
      {"c": canonical, "q": qname},
    )
    if r.rowcount:
      print(f"    {qname} → {canonical}")

  # Cash-flow-statement items — anything whose name indicates a cash
  # movement, activity-category flow, or indirect-method reconciliation
  # adjustment. Use qname prefix for precision; these match regardless
  # of prior classification because CF semantics override.
  cashflow_result = conn.execute(
    sa.text(
      """
      UPDATE public.elements
      SET classification='cashflow', statement_context='cash_flow', updated_at=now()
      WHERE source='rs-gaap'
        AND (qname LIKE 'rs-gaap:IncreaseDecreaseIn%'
          OR qname LIKE 'rs-gaap:IncreaseDecreaseDue%'
          OR qname LIKE 'rs-gaap:NetCashFlow%'
          OR qname LIKE 'rs-gaap:NetCashProvided%'
          OR qname LIKE 'rs-gaap:CashFlows%'
          OR qname LIKE 'rs-gaap:CashAndCashEquivalents%'
          OR qname LIKE 'rs-gaap:CashCashEquivalents%'
          OR qname LIKE 'rs-gaap:EffectOfExchangeRate%'
          OR qname LIKE 'rs-gaap:ProceedsFrom%'
          OR qname LIKE 'rs-gaap:PaymentsFor%'
          OR qname LIKE 'rs-gaap:PaymentsOf%'
          OR qname LIKE 'rs-gaap:PaymentsTo%'
          OR qname LIKE 'rs-gaap:RepaymentsOf%'
          OR qname LIKE 'rs-gaap:AdjustmentsToReconcile%'
          OR qname LIKE 'rs-gaap:AdjustmentsNoncash%')
        AND classification IS DISTINCT FROM 'cashflow'
      """
    )
  )
  print(f"    CF-pattern names → cashflow: {cashflow_result.rowcount}")

  # OCI flow items live on the Statement of Changes in Equity
  # (a.k.a. Statement of Comprehensive Income) as equity-change flows,
  # not income-statement primitives. AOCI accumulated balances stay
  # balance_sheet (they're BS stocks). This runs after the main FAC
  # propagation which may have tagged them income_statement.
  oci_flow = conn.execute(
    sa.text(
      """
      UPDATE public.elements
      SET statement_context = 'equity_changes', updated_at = now()
      WHERE source='rs-gaap'
        AND period_type='duration'
        AND (qname LIKE 'rs-gaap:OtherComprehensiveIncomeLoss%'
          OR qname LIKE 'rs-gaap:OtherComprehensiveIncome%')
        AND statement_context IS DISTINCT FROM 'equity_changes'
      """
    )
  )
  print(f"    OCI flow items → equity_changes: {oci_flow.rowcount}")

  # Equity-flow patterns — contributions, distributions, comprehensive
  # income items. These are duration flows that hit the equity statement,
  # NOT balance-sheet equity stocks. Route them to inflow/outflow based
  # on balance_type; `equity` on economic_nature is reserved for instants.
  equity_result = conn.execute(
    sa.text(
      """
      UPDATE public.elements SET classification = CASE
        WHEN balance_type = 'credit' THEN 'inflow'
        WHEN balance_type = 'debit' THEN 'outflow'
        ELSE classification
      END, updated_at=now()
      WHERE source='rs-gaap'
        AND period_type = 'duration'
        AND (
          -- Owner inflows: stock issuance, partner/member contributions
          name LIKE 'StockIssued%'
          OR name LIKE 'ProceedsFromIssuanceOfCommonStock%'
          OR name LIKE 'ProceedsFromIssuanceOfPreferredStock%'
          OR name LIKE 'ProceedsFromStockOptionsExercised%'
          OR name LIKE 'ProceedsFromIssuanceOfSharesUnderIncentive%'
          OR name LIKE '%Contribution'
          OR name LIKE '%Contributions'
          OR name LIKE 'PartnersCapitalAccount%Contribution%'
          OR name LIKE '%MemberContribution%'
          OR name LIKE '%PartnerContribution%'
          OR name LIKE 'CapitalContributions%'
          OR name LIKE 'ProceedsFromContributions%'
          OR name LIKE 'ProceedsFromCapitalContribution%'
          OR name LIKE 'LimitedPartnersCapitalAccount%Contribution%'
          OR name LIKE 'GeneralPartnersCapitalAccount%Contribution%'
          OR name LIKE 'LimitedLiabilityCompanyLLCMember%Contribution%'
          -- Owner outflows: dividends, partner draws, treasury stock
          OR name LIKE 'Dividends%'
          OR name LIKE 'PaymentsOfDividends%'
          OR name LIKE 'CommonStockDividends%'
          OR name LIKE 'PreferredStockDividends%'
          OR name LIKE 'Distribution'
          OR name LIKE 'Distributions'
          OR name LIKE 'Distributions%'
          OR name LIKE 'DistributionMadeTo%'
          OR name LIKE 'DistributionsMadeTo%'
          OR name LIKE 'PaymentsOfDistributions%'
          OR name LIKE 'PartnersCapitalAccount%Distribution%'
          OR name LIKE '%MemberDistribution%'
          OR name LIKE '%PartnerDistribution%'
          OR name LIKE 'LimitedPartnersCapitalAccount%Distribution%'
          OR name LIKE 'GeneralPartnersCapitalAccount%Distribution%'
          OR name LIKE 'LimitedLiabilityCompanyLLCMember%Distribution%'
          OR name LIKE 'TreasuryStockAcquired%'
          OR name LIKE 'PaymentsForRepurchaseOfCommonStock%'
          OR name LIKE 'PaymentsForRepurchaseOfPreferredStock%'
          OR name LIKE 'StockRepurchased%'
          OR name LIKE '%Withdrawal'
          OR name LIKE '%Withdrawals'
          -- Comprehensive income / OCI (AOCI excluded — it's a BS stock)
          OR name LIKE 'ComprehensiveIncome%'
          OR name LIKE 'OtherComprehensiveIncome%'
        )
        AND classification NOT IN ('inflow', 'outflow')
      """
    )
  )
  print(f"    Equity-flow patterns → inflow/outflow: {equity_result.rowcount}")

  # Final balance+period sanity correction for rs-gaap — catches items
  # that never reached the FAC cascade or got misclassified by the seed
  # extractor's heuristic. Balance-sheet positions (instant) align to
  # asset/liability/equity by balance_type; flows (duration) align to
  # inflow/outflow.
  print("  rs-gaap balance+period sanity correction…")
  r = conn.execute(
    sa.text(
      """
      UPDATE public.elements SET classification = CASE
        WHEN qname ILIKE '%Equity%' OR qname ILIKE '%Stock%' OR qname ILIKE '%CapitalAccount%'
          OR qname ILIKE '%RetainedEarnings%' OR qname ILIKE '%PaidInCapital%'
          OR qname ILIKE '%TreasuryStock%' OR qname ILIKE '%AccumulatedOtherComp%'
          THEN 'equity'
        ELSE 'liability'
      END, updated_at = now()
      WHERE source='rs-gaap' AND classification='asset'
        AND balance_type='credit' AND period_type='instant'
      """
    )
  )
  print(f"    credit+instant 'asset' → liability/equity: {r.rowcount}")
  r = conn.execute(
    sa.text(
      "UPDATE public.elements SET classification='inflow', updated_at=now() "
      "WHERE source='rs-gaap' AND classification='asset' "
      "AND balance_type='credit' AND period_type='duration'"
    )
  )
  print(f"    credit+duration 'asset' → inflow: {r.rowcount}")
  r = conn.execute(
    sa.text(
      "UPDATE public.elements SET classification='outflow', updated_at=now() "
      "WHERE source='rs-gaap' AND classification='asset' "
      "AND balance_type='debit' AND period_type='duration'"
    )
  )
  print(f"    debit+duration 'asset' → outflow: {r.rowcount}")

  # rs-gaap baseline axes: derive statement_context + derivation_role
  # from classification + element_type for anything still null after
  # the FAC / hierarchy cascade, and resync context after the
  # sanity correction above.
  baseline_axes = conn.execute(
    sa.text(
      """
      UPDATE public.elements
      SET statement_context = CASE
            WHEN classification IN ('asset','liability','equity') THEN 'balance_sheet'
            WHEN classification IN ('inflow','outflow') THEN 'income_statement'
            WHEN classification = 'cashflow' THEN 'cash_flow'
            ELSE statement_context
          END,
          derivation_role = COALESCE(derivation_role, CASE
            WHEN is_abstract OR element_type IN ('hypercube','axis','member') THEN 'structural'
            WHEN classification IS NOT NULL THEN 'primitive'
            ELSE NULL
          END),
          updated_at = now()
      WHERE source = 'rs-gaap'
      """
    )
  )
  print(f"    rs-gaap axes finalized: {baseline_axes.rowcount}")

  # AOCI / AccumulatedOtherComprehensiveIncome* are the balance-sheet
  # stock (instant) representation — these stay in equity.
  aoci_result = conn.execute(
    sa.text(
      """
      UPDATE public.elements SET classification='equity', updated_at=now()
      WHERE source='rs-gaap'
        AND period_type='instant'
        AND (name LIKE 'AOCI%'
             OR name LIKE 'AccumulatedOtherComprehensiveIncome%')
        AND classification IS DISTINCT FROM 'equity'
      """
    )
  )
  print(f"    AOCI instants → equity: {aoci_result.rowcount}")

  # ──────────────────────────────────────────────────────────────────────
  # 8. Bulk populate the classifications registry + junction
  # ──────────────────────────────────────────────────────────────────────
  # Once the denormalized columns on `elements` have settled, derive the
  # registry + junction in a handful of SQL statements rather than per-
  # element round-trips.
  print("  Building classifications registry + junction (bulk)…")
  bulk_counts = sync_element_classifications_bulk(conn)
  print(
    f"    classifications inserted: {bulk_counts['classifications']}, "
    f"junction rows: {bulk_counts['junction']}"
  )

  # ──────────────────────────────────────────────────────────────────────
  # 9. Tenant-schema rollout: immutability function + per-tenant backfill
  # ──────────────────────────────────────────────────────────────────────
  # Install the shared PL/pgSQL function once, then for every existing
  # tenant schema: widen CHECKs → copy library rows from public → install
  # triggers → tag CoA elements with SFAC 6 anchors. On a fresh deploy
  # with no tenant schemas, the per-tenant loops are no-ops; on an
  # existing deploy they backfill every tenant to parity with a freshly
  # provisioned graph.
  print("  Installing raise_library_immutable() function…")
  _install_raise_library_immutable_fn(conn)

  print("  Backfilling library into existing tenant schemas…")
  for_each_tenant_schema(conn, _backfill_library_into_tenant)

  print("  Tagging tenant CoA elements with SFAC 6 anchors…")
  for_each_tenant_schema(conn, _tag_coa_for_tenant)


def downgrade() -> None:
  conn = op.get_bind()

  # Tenant-schema teardown: remove CoA anchor tags, drop immutability
  # triggers, restore narrow tenant CHECKs. Library rows themselves stay
  # in the tenant schemas — removing them would orphan any tenant
  # mappings that reference library elements. Drop the shared function
  # last, after every trigger is gone.
  def _teardown_tenant(conn, schema: str) -> None:
    _untag_coa_for_tenant(conn, schema)
    _drop_triggers_for_tenant(conn, schema)
    _restore_narrow_tenant_checks(conn, schema)

  for_each_tenant_schema(conn, _teardown_tenant)
  conn.execute(text("DROP FUNCTION IF EXISTS public.raise_library_immutable()"))

  # Remove library-origin rows loaded from JSON-LD (order matters — FKs).
  conn.execute(sa.text("DELETE FROM public.element_classifications"))
  conn.execute(sa.text("DELETE FROM public.classifications"))
  conn.execute(sa.text("DELETE FROM public.element_references"))
  conn.execute(sa.text("DELETE FROM public.element_labels"))
  conn.execute(
    sa.text(
      "DELETE FROM public.associations WHERE structure_id IN "
      "(SELECT id FROM public.structures WHERE taxonomy_id IN "
      "(SELECT id FROM public.taxonomies WHERE is_shared = true))"
    )
  )
  conn.execute(
    sa.text(
      "DELETE FROM public.structures WHERE taxonomy_id IN "
      "(SELECT id FROM public.taxonomies WHERE is_shared = true)"
    )
  )
  conn.execute(
    sa.text(
      "DELETE FROM public.elements WHERE taxonomy_id IN "
      "(SELECT id FROM public.taxonomies WHERE is_shared = true)"
    )
  )
  conn.execute(sa.text("DELETE FROM public.taxonomies WHERE is_shared = true"))

  # Drop axis CHECK constraints + columns.
  op.drop_constraint("check_element_derivation_role", "elements", type_="check")
  op.drop_constraint("check_element_statement_context", "elements", type_="check")
  op.drop_index("idx_elements_derivation_role", table_name="elements")
  op.drop_index("idx_elements_statement_context", table_name="elements")
  op.drop_column("elements", "derivation_role")
  op.drop_column("elements", "statement_context")

  # Drop classification registry + junction.
  op.drop_index(
    "idx_element_classifications_primary",
    table_name="element_classifications",
    postgresql_where="is_primary = true",
  )
  op.drop_index(
    "idx_element_classifications_classification",
    table_name="element_classifications",
  )
  op.drop_table("element_classifications")

  op.drop_index("idx_classifications_type", table_name="classifications")
  op.drop_index("idx_classifications_category", table_name="classifications")
  op.drop_table("classifications")

  # Drop label + reference tables.
  op.drop_index("idx_element_references_type", table_name="element_references")
  op.drop_index("idx_element_references_element", table_name="element_references")
  op.drop_table("element_references")

  op.drop_index("idx_element_labels_role", table_name="element_labels")
  op.drop_index("idx_element_labels_element", table_name="element_labels")
  op.drop_table("element_labels")

  # Restore original CHECK constraints.
  op.drop_constraint("check_association_type", "associations", type_="check")
  op.create_check_constraint(
    "check_association_type",
    "associations",
    "association_type IN ('presentation', 'calculation', 'mapping')",
  )

  op.drop_constraint("check_element_source", "elements", type_="check")
  op.create_check_constraint(
    "check_element_source",
    "elements",
    "source IN ("
    "'sfac6', 'us-gaap', 'ifrs', "
    "'quickbooks', 'xero', 'plaid', 'native', 'import', 'system'"
    ")",
  )

  op.drop_constraint("check_element_classification", "elements", type_="check")
  op.create_check_constraint(
    "check_element_classification",
    "elements",
    "classification IN ('asset', 'liability', 'equity', 'revenue', 'expense')",
  )
  op.alter_column(
    "elements", "classification", existing_type=sa.VARCHAR(), nullable=False
  )

  # Re-run 0001's seeder so the library isn't empty after downgrade.
  from robosystems.taxonomy.seed import seed_reporting_taxonomy

  seed_reporting_taxonomy(conn)
