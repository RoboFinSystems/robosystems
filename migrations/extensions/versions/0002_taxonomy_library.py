"""Taxonomy library — XBRL-faithful model.

Replaces the Python-dict seed (``seed_reporting_taxonomy``) with JSON-LD
artifacts loaded from ``robosystems/taxonomy/seeds/``, aligned with
XBRL's association model: elements carry only XBRL-intrinsic attributes
(balance, periodType, abstract, monetary, elementType,
substitutionGroup), and all classifications are first-class
``classifications`` rows attached via the ``element_classifications``
junction — mirroring XBRL's traitConcept linkbase.

Schema changes (public schema):

- CREATE TABLE public.element_labels (XBRL label linkbase)
- CREATE TABLE public.element_references (XBRL reference linkbase)
- CREATE TABLE public.classifications (OLTP mirror of graph
  Classification node — 24 FASB metamodel trait axes + flowClassification
  + association-level categories)
- CREATE TABLE public.element_classifications (many-to-many junction)
- DROP elements.classification + sub_classification (concocted columns;
  replaced by element_classifications junction rows in the
  ``elementsOfFinancialStatements`` category)
- ADD elements.substitution_group (XBRL intrinsic; previously missing)
- Widen source CHECK to include 'fac', 'rs-gaap'
- Widen association_type CHECK to include 'equivalence',
  'general-special', 'essence-alias'

Data changes (public schema):

- DELETE 0001-seeded library rows
- INSERT from JSON-LD seeds: fac, rs-gaap (concept taxonomies);
  us-gaap-metamodel (classification vocabulary, 99 classifications
  across 25 categories); rs-gaap-to-metamodel (classification
  assignments, ~3.5k arcs); rs-gaap-hierarchy (class-subclass);
  fac-to-rs-gaap, fac-calculations, fac-presentation, type-subtype
  (mapping seeds)

Tenant-schema rollout (for every existing tenant schema ``kg*``):

- Widen association_type / element source CHECKs
- Drop the old elements.classification + sub_classification columns
- Add elements.substitution_group column
- Copy pinned library rows from public.* (taxonomies, elements, labels,
  references, structures, associations, classifications,
  element_classifications) using ``copy_library_into_tenant``
- Install immutability triggers on the library-backing tables

The immutability function ``public.raise_library_immutable()`` is
installed once (idempotent) and shared by every tenant trigger.

Revision ID: 0002
Revises: 0001
Create Date: 2026-04-20
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


_IMMUTABLE_TABLES = (
  "taxonomies",
  "elements",
  "element_labels",
  "element_references",
  "structures",
  "associations",
  "classifications",
  "element_classifications",
  "rules",
)

_WIDENED_ASSOCIATION_CHECK = (
  "association_type IN ("
  "'presentation', 'calculation', 'mapping', "
  "'equivalence', 'general-special', 'essence-alias'"
  ")"
)
_WIDENED_ELEMENT_SOURCE_CHECK = (
  "source IN ("
  "'fac', 'rs-gaap', 'us-gaap', 'ifrs', "
  "'quickbooks', 'xero', 'plaid', 'native', 'import', 'system'"
  ")"
)
_WIDENED_TAXONOMY_TYPE_CHECK = (
  "taxonomy_type IN ("
  "'chart_of_accounts', 'reporting', 'mapping', 'schedule', "
  "'classification-vocabulary', 'classification-assignment', 'rules'"
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
# Structure.structure_type widens to admit the Phase g.1 block types
# (rollforward, reconciliation, policy) required as admissible CHECK
# values for Phase d (typed artifact_mechanics + rules) and Phase eta
# (metric block type). Folded into 0002 rather than shipped as a
# standalone migration since 0002 is still unreleased. See
# ``local/docs/specs/information-block.md`` section 5.9, Phase g.1.
_WIDENED_STRUCTURE_TYPE_CHECK = (
  "structure_type IN ("
  "'chart_of_accounts', 'income_statement', 'balance_sheet', "
  "'cash_flow_statement', 'equity_statement', 'coa_mapping', 'custom', "
  "'schedule', 'rollforward', 'reconciliation', 'policy'"
  ")"
)
_NARROW_STRUCTURE_TYPE_CHECK = (
  "structure_type IN ("
  "'chart_of_accounts', 'income_statement', 'balance_sheet', "
  "'cash_flow_statement', 'equity_statement', 'coa_mapping', 'custom', 'schedule'"
  ")"
)

# Phase d.2 — Seattle Method rule taxonomy: 8 VerificationRule categories
# x 10 BusinessRulePattern mechanisms. These match the CHECK constraints on
# public.rules.rule_category / rule_pattern and the RULE_CATEGORY_VALUES /
# RULE_PATTERN_VALUES frozensets in jsonld_loader.py.
_RULE_CATEGORY_CHECK = (
  "rule_category IN ("
  "'AutomatedAccountingAndReportingChecks', "
  "'FundamentalAccountingConceptRelation', "
  "'PeerConsistencyRule', "
  "'PriorPeriodConsistencyRule', "
  "'ReportLevelModelStructureRule', "
  "'ReportingSystemSpecificRule', "
  "'ToDoManualTask', "
  "'XBRLTechnicalSyntaxRule'"
  ")"
)
_RULE_PATTERN_CHECK = (
  "rule_pattern IN ("
  "'Adjustment', 'CoExists', 'EqualTo', 'Exists', 'GreaterThan', "
  "'GreaterThanOrEqualToZero', 'LessThan', 'RollForward', 'RollUp', "
  "'Variance'"
  ")"
)
_RULE_SEVERITY_CHECK = "rule_severity IN ('info', 'warning', 'error')"
_RULE_ORIGIN_CHECK = "rule_origin IN ('forked', 'native')"
# Polymorphic target: either nothing (global rule) or exactly one of
# target_structure_id / target_element_id / target_association_id is set,
# and target_kind names which column carries the FK.
_RULE_TARGET_POLYMORPHISM_CHECK = (
  "("
  "(target_kind IS NULL AND target_structure_id IS NULL "
  "AND target_element_id IS NULL AND target_association_id IS NULL) "
  "OR (target_kind = 'structure' AND target_structure_id IS NOT NULL "
  "AND target_element_id IS NULL AND target_association_id IS NULL) "
  "OR (target_kind = 'element' AND target_element_id IS NOT NULL "
  "AND target_structure_id IS NULL AND target_association_id IS NULL) "
  "OR (target_kind = 'association' AND target_association_id IS NOT NULL "
  "AND target_structure_id IS NULL AND target_element_id IS NULL)"
  ")"
)

# The 24 FASB metamodel trait axes + flowClassification + association-level
# categories. Matches the CHECK constraint on public.classifications.category.
_CLASSIFICATION_CATEGORY_CHECK = (
  "category IN ("
  "'elementsOfFinancialStatements', 'liquidity', 'activityType', "
  "'operatingNonoperating', 'operatingIntent', 'realizationStatus', "
  "'recordedValue', 'restriction', 'hedging', 'leaseType', "
  "'interestRateType', 'convertibility', 'creditStructure', "
  "'debtGuarantee', 'derivativeContract', 'derivativeInstrument', "
  "'accrualOrPayable', 'priority', 'estimatedFutureActivity', "
  "'statisticalMeasurement', 'taxComponents', 'threshold', 'use', "
  "'indirectCashFlowReconcilingItem', "
  "'flowClassification', "
  "'concept_arrangement', 'member_arrangement', 'named_disclosure'"
  ")"
)


def _install_raise_library_immutable_fn(conn) -> None:
  """Create the PL/pgSQL functions that raise on library-origin mutations.

  Two functions:

  * ``raise_library_immutable`` — guards UPDATE/DELETE on all library
    tables by checking ``OLD.created_by = 'library-seeder'``.

  * ``raise_insert_into_library_structure`` — guards INSERT on
    ``associations`` by looking up the target ``structures.created_by``.
    Without this, a tenant caller can INSERT user-authored arcs into a
    library-seeded structure (fac-presentation, fac-to-rs-gaap, etc)
    and silently change library behavior for that tenant only. The
    service-layer ``assert_not_library_origin(structure)`` guard is the
    fast path; this is defense-in-depth.
  """
  conn.execute(
    text(
      """
      CREATE OR REPLACE FUNCTION public.raise_library_immutable()
      RETURNS trigger AS $$
      BEGIN
        IF TG_OP = 'UPDATE' AND OLD.created_by = 'library-seeder' THEN
          RAISE EXCEPTION
            'Library-seeded row is immutable in tenant scope: %.% id=%',
            TG_TABLE_SCHEMA, TG_TABLE_NAME, OLD.id;
        END IF;
        IF TG_OP = 'DELETE' AND OLD.created_by = 'library-seeder' THEN
          RAISE EXCEPTION
            'Library-seeded row is immutable in tenant scope: %.% id=%',
            TG_TABLE_SCHEMA, TG_TABLE_NAME, OLD.id;
        END IF;
        RETURN OLD;
      END;
      $$ LANGUAGE plpgsql;
      """
    )
  )
  conn.execute(
    text(
      """
      CREATE OR REPLACE FUNCTION public.raise_insert_into_library_structure()
      RETURNS trigger AS $$
      DECLARE
        structure_created_by text;
      BEGIN
        EXECUTE format(
          'SELECT created_by FROM %I.structures WHERE id = $1 LIMIT 1',
          TG_TABLE_SCHEMA
        )
        INTO structure_created_by
        USING NEW.structure_id;
        IF structure_created_by = 'library-seeder' THEN
          RAISE EXCEPTION
            'Cannot insert association into library-seeded structure: '
            '%.%.structure_id=% is owned by the library',
            TG_TABLE_SCHEMA, TG_TABLE_NAME, NEW.structure_id;
        END IF;
        RETURN NEW;
      END;
      $$ LANGUAGE plpgsql;
      """
    )
  )


def _install_triggers_for_tenant(conn, schema: str) -> None:
  for table in _IMMUTABLE_TABLES:
    trigger_name = f"raise_library_immutable_{table}"
    conn.execute(text(f"DROP TRIGGER IF EXISTS {trigger_name} ON {schema}.{table}"))
    conn.execute(
      text(
        f"""
        CREATE TRIGGER {trigger_name}
        BEFORE UPDATE OR DELETE ON {schema}.{table}
        FOR EACH ROW EXECUTE FUNCTION public.raise_library_immutable()
        """
      )
    )
  # INSERT guard on associations — catches tenant writes that target a
  # library-seeded structure (the cross-row check the per-row immutability
  # trigger can't do).
  conn.execute(
    text(
      f"DROP TRIGGER IF EXISTS raise_insert_into_library_structure ON {schema}.associations"
    )
  )
  conn.execute(
    text(
      f"""
      CREATE TRIGGER raise_insert_into_library_structure
      BEFORE INSERT ON {schema}.associations
      FOR EACH ROW EXECUTE FUNCTION public.raise_insert_into_library_structure()
      """
    )
  )


def _drop_triggers_for_tenant(conn, schema: str) -> None:
  for table in _IMMUTABLE_TABLES:
    conn.execute(
      text(
        f"DROP TRIGGER IF EXISTS raise_library_immutable_{table} ON {schema}.{table}"
      )
    )
  conn.execute(
    text(
      f"DROP TRIGGER IF EXISTS raise_insert_into_library_structure ON {schema}.associations"
    )
  )


def _widen_tenant_checks(conn, schema: str) -> None:
  t = TenantOps(conn, schema)
  t.add_check("associations", "check_association_type", _WIDENED_ASSOCIATION_CHECK)
  t.add_check("elements", "check_element_source", _WIDENED_ELEMENT_SOURCE_CHECK)
  t.add_check("structures", "check_structure_type", _WIDENED_STRUCTURE_TYPE_CHECK)


def _restore_narrow_tenant_checks(conn, schema: str) -> None:
  t = TenantOps(conn, schema)
  t.add_check("associations", "check_association_type", _NARROW_ASSOCIATION_CHECK)
  t.add_check("elements", "check_element_source", _NARROW_ELEMENT_SOURCE_CHECK)
  t.add_check("structures", "check_structure_type", _NARROW_STRUCTURE_TYPE_CHECK)


def _drop_old_element_columns(conn, schema: str) -> None:
  """Drop the concocted classification columns from a tenant's elements table."""
  for column, constraint, index in (
    ("classification", "check_element_classification", "idx_elements_classification"),
    ("sub_classification", None, None),
  ):
    if constraint:
      conn.execute(
        text(f"ALTER TABLE {schema}.elements DROP CONSTRAINT IF EXISTS {constraint}")
      )
    if index:
      conn.execute(text(f"DROP INDEX IF EXISTS {schema}.{index}"))
    conn.execute(text(f"ALTER TABLE {schema}.elements DROP COLUMN IF EXISTS {column}"))


def _add_substitution_group_column(conn, schema: str) -> None:
  """Add substitution_group column + index to a tenant's elements table."""
  conn.execute(
    text(
      f"ALTER TABLE {schema}.elements "
      f"ADD COLUMN IF NOT EXISTS substitution_group VARCHAR"
    )
  )
  conn.execute(
    text(
      f"CREATE INDEX IF NOT EXISTS idx_elements_substitution_group "
      f"ON {schema}.elements (substitution_group) "
      f"WHERE substitution_group IS NOT NULL"
    )
  )


def _add_phase_d_columns_to_tenant(conn, schema: str) -> None:
  """Add Phase d typed-mechanics columns to a tenant's structures table.

  Mirror of the public-schema DDL earlier in the upgrade. Nullable so
  existing tenant Schedule rows (which only carry ``metadata_`` today)
  pass the ALTER; the subsequent
  :func:`_backfill_tenant_schedule_mechanics` populates them.
  """
  t = TenantOps(conn, schema)
  t.add_column("structures", "concept_arrangement", "VARCHAR")
  t.add_column("structures", "member_arrangement", "VARCHAR")
  t.add_column("structures", "artifact_mechanics", "JSONB")
  t.add_column("structures", "parenthetical_note", "VARCHAR")
  t.add_column("structures", "template_id", "VARCHAR")


def _drop_phase_d_columns_from_tenant(conn, schema: str) -> None:
  """Reverse of :func:`_add_phase_d_columns_to_tenant` for downgrade."""
  t = TenantOps(conn, schema)
  for col in (
    "template_id",
    "parenthetical_note",
    "artifact_mechanics",
    "member_arrangement",
    "concept_arrangement",
  ):
    t.drop_column("structures", col)


def _backfill_tenant_schedule_mechanics(conn, schema: str) -> None:
  """Lift Schedule mechanics out of ``metadata_`` into typed columns.

  Schedule rows in tenant schemas carry ``entry_template`` + ``schedule_metadata``
  inside the ``metadata_`` JSONB blob today. Phase d moves them into the
  typed ``artifact_mechanics`` column (discriminated-union-arm shape keyed
  on ``kind='closing_entry_generator'``) and stamps
  ``concept_arrangement='roll_forward'`` to match the registry default.
  Runs after ``copy_library_into_tenant`` so library-seeded statement
  rows (already populated in public before the copy) don't get clobbered.
  """
  conn.execute(
    text(
      f"""
      UPDATE "{schema}".structures
      SET concept_arrangement = COALESCE(concept_arrangement, 'roll_forward'),
          artifact_mechanics = COALESCE(
            artifact_mechanics,
            jsonb_build_object(
              'kind', 'closing_entry_generator',
              'entry_template',
                COALESCE(metadata -> 'entry_template', '{{}}'::jsonb),
              'schedule_metadata',
                COALESCE(metadata -> 'schedule_metadata', '{{}}'::jsonb)
            )
          )
      WHERE structure_type = 'schedule'
      """
    )
  )


def _backfill_library_into_tenant(conn, schema: str) -> None:
  """Widen CHECKs, drop old columns, add substitution_group, copy library rows.

  Order is load-bearing: schema changes first, then copy, then triggers.
  """
  _widen_tenant_checks(conn, schema)
  _drop_old_element_columns(conn, schema)
  _add_substitution_group_column(conn, schema)
  _add_phase_d_columns_to_tenant(conn, schema)
  _create_tenant_library_tables(conn, schema)
  stats = copy_library_into_tenant(conn, schema)
  print(f"  [{schema}] library backfill: {stats.total:,} rows")
  _backfill_tenant_schedule_mechanics(conn, schema)
  _install_triggers_for_tenant(conn, schema)


def _create_tenant_library_tables(conn, schema: str) -> None:
  """Create element_labels/references/classifications/element_classifications in a tenant schema."""
  conn.execute(
    text(f"""
      CREATE TABLE IF NOT EXISTS {schema}.element_labels (
        id VARCHAR PRIMARY KEY,
        element_id VARCHAR NOT NULL REFERENCES {schema}.elements(id),
        role VARCHAR NOT NULL DEFAULT 'standard',
        language VARCHAR NOT NULL DEFAULT 'en',
        text TEXT NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT now(),
        created_by VARCHAR NOT NULL DEFAULT 'system',
        CONSTRAINT uq_{schema}_element_labels_element_role_language
          UNIQUE (element_id, role, language),
        CONSTRAINT check_{schema}_element_label_role
          CHECK (role IN ('standard', 'verbose', 'terse', 'documentation',
                          'periodStart', 'periodEnd', 'negated', 'total',
                          'commentaryGuidance', 'deprecatedLabel', 'other'))
      )
    """)
  )
  conn.execute(
    text(
      f"CREATE INDEX IF NOT EXISTS idx_{schema}_element_labels_element "
      f"ON {schema}.element_labels (element_id)"
    )
  )

  conn.execute(
    text(f"""
      CREATE TABLE IF NOT EXISTS {schema}.element_references (
        id VARCHAR PRIMARY KEY,
        element_id VARCHAR NOT NULL REFERENCES {schema}.elements(id),
        ref_type VARCHAR,
        citation VARCHAR NOT NULL,
        uri VARCHAR,
        attributes VARCHAR,
        created_at TIMESTAMP NOT NULL DEFAULT now(),
        created_by VARCHAR NOT NULL DEFAULT 'system'
      )
    """)
  )
  conn.execute(
    text(
      f"CREATE INDEX IF NOT EXISTS idx_{schema}_element_references_element "
      f"ON {schema}.element_references (element_id)"
    )
  )

  conn.execute(
    text(f"""
      CREATE TABLE IF NOT EXISTS {schema}.classifications (
        id VARCHAR PRIMARY KEY,
        category VARCHAR NOT NULL,
        identifier VARCHAR NOT NULL,
        type VARCHAR NOT NULL,
        name VARCHAR,
        description VARCHAR,
        confidence FLOAT,
        source VARCHAR,
        metadata JSONB NOT NULL DEFAULT '{{}}',
        created_at TIMESTAMP NOT NULL DEFAULT now(),
        updated_at TIMESTAMP NOT NULL DEFAULT now(),
        created_by VARCHAR NOT NULL DEFAULT 'system',
        CONSTRAINT uq_{schema}_classification_category_identifier_type
          UNIQUE (category, identifier, type),
        CONSTRAINT check_{schema}_classification_category
          CHECK ({_CLASSIFICATION_CATEGORY_CHECK})
      )
    """)
  )
  conn.execute(
    text(
      f"CREATE INDEX IF NOT EXISTS idx_{schema}_classifications_category "
      f"ON {schema}.classifications (category)"
    )
  )

  conn.execute(
    text(f"""
      CREATE TABLE IF NOT EXISTS {schema}.element_classifications (
        element_id VARCHAR NOT NULL REFERENCES {schema}.elements(id),
        classification_id VARCHAR NOT NULL REFERENCES {schema}.classifications(id),
        is_primary BOOLEAN NOT NULL DEFAULT true,
        confidence FLOAT,
        source VARCHAR,
        created_at TIMESTAMP NOT NULL DEFAULT now(),
        updated_at TIMESTAMP NOT NULL DEFAULT now(),
        created_by VARCHAR NOT NULL DEFAULT 'system',
        PRIMARY KEY (element_id, classification_id)
      )
    """)
  )
  conn.execute(
    text(
      f"CREATE INDEX IF NOT EXISTS idx_{schema}_element_classifications_primary "
      f"ON {schema}.element_classifications (element_id, is_primary) "
      f"WHERE is_primary = true"
    )
  )

  # Phase d.2 — verification rules. Polymorphic target (structure/element/
  # association) enforced via CHECK; FKs point at the same-schema parent
  # tables so tenant rows can't reference public library ids.
  conn.execute(
    text(f"""
      CREATE TABLE IF NOT EXISTS {schema}.rules (
        id VARCHAR PRIMARY KEY,
        taxonomy_id VARCHAR NOT NULL REFERENCES {schema}.taxonomies(id),
        rule_category VARCHAR NOT NULL,
        rule_pattern VARCHAR NOT NULL,
        rule_expression TEXT NOT NULL,
        rule_message TEXT,
        rule_severity VARCHAR NOT NULL DEFAULT 'error',
        rule_origin VARCHAR NOT NULL DEFAULT 'native',
        target_kind VARCHAR,
        target_structure_id VARCHAR REFERENCES {schema}.structures(id),
        target_element_id VARCHAR REFERENCES {schema}.elements(id),
        target_association_id VARCHAR REFERENCES {schema}.associations(id),
        rule_variables JSONB NOT NULL DEFAULT '[]',
        metadata JSONB NOT NULL DEFAULT '{{}}',
        created_at TIMESTAMP NOT NULL DEFAULT now(),
        updated_at TIMESTAMP NOT NULL DEFAULT now(),
        created_by VARCHAR NOT NULL DEFAULT 'system',
        CONSTRAINT check_{schema}_rule_category CHECK ({_RULE_CATEGORY_CHECK}),
        CONSTRAINT check_{schema}_rule_pattern CHECK ({_RULE_PATTERN_CHECK}),
        CONSTRAINT check_{schema}_rule_severity CHECK ({_RULE_SEVERITY_CHECK}),
        CONSTRAINT check_{schema}_rule_origin CHECK ({_RULE_ORIGIN_CHECK}),
        CONSTRAINT check_{schema}_rule_target_polymorphism
          CHECK ({_RULE_TARGET_POLYMORPHISM_CHECK})
      )
    """)
  )
  conn.execute(
    text(
      f"CREATE INDEX IF NOT EXISTS idx_{schema}_rules_taxonomy "
      f"ON {schema}.rules (taxonomy_id)"
    )
  )
  conn.execute(
    text(
      f"CREATE INDEX IF NOT EXISTS idx_{schema}_rules_target_structure "
      f"ON {schema}.rules (target_structure_id) "
      f"WHERE target_structure_id IS NOT NULL"
    )
  )
  conn.execute(
    text(
      f"CREATE INDEX IF NOT EXISTS idx_{schema}_rules_target_element "
      f"ON {schema}.rules (target_element_id) "
      f"WHERE target_element_id IS NOT NULL"
    )
  )
  conn.execute(
    text(
      f"CREATE INDEX IF NOT EXISTS idx_{schema}_rules_target_association "
      f"ON {schema}.rules (target_association_id) "
      f"WHERE target_association_id IS NOT NULL"
    )
  )
  conn.execute(
    text(
      f"CREATE INDEX IF NOT EXISTS idx_{schema}_rules_category "
      f"ON {schema}.rules (rule_category)"
    )
  )


SEEDS_DIR = (
  Path(__file__).parent.parent.parent.parent / "robosystems" / "taxonomy" / "seeds"
)

# Concept taxonomies first (referenced by qname from mapping + assignment
# seeds); then the classification vocabulary; then assignments/mappings.
SEED_FILES = [
  # Classification vocabulary FIRST — FAC + rs-gaap seeds reference these
  # via classifiedAs arcs; the DB lookup in write_taxonomy_package fails if
  # the classification rows don't exist yet.
  SEEDS_DIR / "us-gaap-metamodel" / "v1" / "taxonomy.jsonld",
  SEEDS_DIR / "fac" / "v1" / "taxonomy.jsonld",
  SEEDS_DIR / "rs-gaap" / "v1" / "taxonomy.jsonld",
  # Classification assignments + hierarchy (reference rs-gaap concepts)
  SEEDS_DIR / "rs-gaap-to-metamodel" / "v1" / "taxonomy.jsonld",
  SEEDS_DIR / "rs-gaap-hierarchy" / "v1" / "taxonomy.jsonld",
  SEEDS_DIR / "rs-gaap-presentation" / "v1" / "taxonomy.jsonld",
  # Cross-taxonomy mapping + calculation + presentation arc packs
  SEEDS_DIR / "fac-to-rs-gaap" / "v1" / "taxonomy.jsonld",
  SEEDS_DIR / "fac-calculations" / "v1" / "taxonomy.jsonld",
  SEEDS_DIR / "fac-presentation" / "v1" / "taxonomy.jsonld",
  SEEDS_DIR / "type-subtype" / "v1" / "taxonomy.jsonld",
  # Phase d.2 — verification rules (Seattle Method). Last because rules
  # reference structures loaded by fac-calculations/fac-presentation.
  SEEDS_DIR / "fac-rules" / "v1" / "taxonomy.jsonld",
]


def upgrade() -> None:
  conn = op.get_bind()

  # ──────────────────────────────────────────────────────────────────────
  # 1. Drop concocted classification columns + their CHECK + index.
  # ──────────────────────────────────────────────────────────────────────
  # These columns live on the 0001 schema. Replaced by
  # element_classifications junction rows in the
  # 'elementsOfFinancialStatements' category (seeded from
  # us-gaap-metamodel/v1).
  op.drop_constraint("check_element_classification", "elements", type_="check")
  op.drop_index("idx_elements_classification", table_name="elements")
  op.drop_column("elements", "classification")
  op.drop_column("elements", "sub_classification")

  # ──────────────────────────────────────────────────────────────────────
  # 2. Add substitution_group (XBRL intrinsic, previously missing).
  # ──────────────────────────────────────────────────────────────────────
  op.add_column("elements", sa.Column("substitution_group", sa.String(), nullable=True))
  op.create_index(
    "idx_elements_substitution_group",
    "elements",
    ["substitution_group"],
    postgresql_where="substitution_group IS NOT NULL",
  )

  # ──────────────────────────────────────────────────────────────────────
  # 3. Clear 0001-seeded library rows before tightening CHECKs.
  # ──────────────────────────────────────────────────────────────────────
  # 0001's Python seed inserts SFAC 6 roots with source='sfac6'. The
  # tightened check_element_source below no longer allows 'sfac6', so
  # existing rows must be removed first — PostgreSQL validates CHECK
  # constraints against existing data at CREATE time.
  conn.execute(
    text(
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
    text(
      "DELETE FROM public.structures WHERE taxonomy_id IN "
      "(SELECT id FROM public.taxonomies WHERE is_shared = true)"
    )
  )
  conn.execute(
    text(
      "DELETE FROM public.elements WHERE taxonomy_id IN "
      "(SELECT id FROM public.taxonomies WHERE is_shared = true)"
    )
  )
  conn.execute(text("DELETE FROM public.taxonomies WHERE is_shared = true"))

  # ──────────────────────────────────────────────────────────────────────
  # 4. Widen element source + association_type + taxonomy_type CHECKs.
  # ──────────────────────────────────────────────────────────────────────
  op.drop_constraint("check_element_source", "elements", type_="check")
  op.create_check_constraint(
    "check_element_source", "elements", _WIDENED_ELEMENT_SOURCE_CHECK
  )
  op.drop_constraint("check_association_type", "associations", type_="check")
  op.create_check_constraint(
    "check_association_type", "associations", _WIDENED_ASSOCIATION_CHECK
  )
  # us-gaap-metamodel uses 'classification-vocabulary',
  # rs-gaap-to-metamodel uses 'classification-assignment' — neither in
  # the 0001 CHECK. Widen before loading JSON-LD seeds in section 7.
  op.drop_constraint("check_taxonomy_type", "taxonomies", type_="check")
  op.create_check_constraint(
    "check_taxonomy_type", "taxonomies", _WIDENED_TAXONOMY_TYPE_CHECK
  )
  # Phase g.1: widen structures.structure_type to admit rollforward,
  # reconciliation, policy. Pure vocabulary expansion — no data change.
  op.drop_constraint("check_structure_type", "structures", type_="check")
  op.create_check_constraint(
    "check_structure_type", "structures", _WIDENED_STRUCTURE_TYPE_CHECK
  )

  # Phase d: typed artifact_mechanics + Information-Model axis columns
  # on public.structures. Folded into 0002 because 0002 is still
  # unreleased; shipping as a standalone migration would force a second
  # tenant-schema round-trip. Nullable on add so library loaders and
  # existing rows (none in public yet at this point in the upgrade, but
  # defense in depth for tenant ALTERs below) don't fail. Post-seed
  # UPDATEs populate concept/member_arrangement + artifact_mechanics for
  # the library-seeded statement Structures; per-tenant UPDATEs backfill
  # existing Schedule rows from metadata_. See
  # ``local/docs/specs/information-block.md`` section 4.2, Phase d.
  op.add_column(
    "structures",
    sa.Column("concept_arrangement", sa.String(), nullable=True),
    schema="public",
  )
  op.add_column(
    "structures",
    sa.Column("member_arrangement", sa.String(), nullable=True),
    schema="public",
  )
  op.add_column(
    "structures",
    sa.Column(
      "artifact_mechanics",
      postgresql.JSONB(astext_type=sa.Text()),
      nullable=True,
    ),
    schema="public",
  )
  op.add_column(
    "structures",
    sa.Column("parenthetical_note", sa.String(), nullable=True),
    schema="public",
  )
  op.add_column(
    "structures",
    sa.Column("template_id", sa.String(), nullable=True),
    schema="public",
  )

  # ──────────────────────────────────────────────────────────────────────
  # 5. Label + reference linkbase tables (XBRL).
  # ──────────────────────────────────────────────────────────────────────
  op.create_table(
    "element_labels",
    sa.Column("id", sa.String(), nullable=False),
    sa.Column("element_id", sa.String(), nullable=False),
    sa.Column("role", sa.String(), nullable=False, server_default="standard"),
    sa.Column("language", sa.String(), nullable=False, server_default="en"),
    sa.Column("text", sa.Text(), nullable=False),
    sa.Column(
      "created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")
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
      "role IN ('standard', 'verbose', 'terse', 'documentation', "
      "'periodStart', 'periodEnd', 'negated', 'total', "
      "'commentaryGuidance', 'deprecatedLabel', 'other')",
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
      "created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")
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
  # 6. Classification vocabulary + junction.
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
      _CLASSIFICATION_CATEGORY_CHECK, name="check_classification_category"
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
  # 6b. Phase d.2 — verification rules table.
  # ──────────────────────────────────────────────────────────────────────
  # Polymorphic target: nullable FKs to structures / elements /
  # associations, with a CHECK that exactly one (or none, for global
  # rules) is set. rule_variables JSONB carries the $Variable → qname
  # binding table the engine consumes at evaluation time.
  op.create_table(
    "rules",
    sa.Column("id", sa.String(), nullable=False),
    sa.Column("taxonomy_id", sa.String(), nullable=False),
    sa.Column("rule_category", sa.String(), nullable=False),
    sa.Column("rule_pattern", sa.String(), nullable=False),
    sa.Column("rule_expression", sa.Text(), nullable=False),
    sa.Column("rule_message", sa.Text(), nullable=True),
    sa.Column("rule_severity", sa.String(), nullable=False, server_default="error"),
    sa.Column("rule_origin", sa.String(), nullable=False, server_default="native"),
    sa.Column("target_kind", sa.String(), nullable=True),
    sa.Column("target_structure_id", sa.String(), nullable=True),
    sa.Column("target_element_id", sa.String(), nullable=True),
    sa.Column("target_association_id", sa.String(), nullable=True),
    sa.Column(
      "rule_variables",
      postgresql.JSONB(astext_type=sa.Text()),
      nullable=False,
      server_default=sa.text("'[]'::jsonb"),
    ),
    sa.Column(
      "metadata",
      postgresql.JSONB(astext_type=sa.Text()),
      nullable=False,
      server_default=sa.text("'{}'::jsonb"),
    ),
    sa.Column(
      "created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")
    ),
    sa.Column(
      "updated_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")
    ),
    sa.Column("created_by", sa.String(), nullable=False, server_default="system"),
    sa.ForeignKeyConstraint(["taxonomy_id"], ["taxonomies.id"]),
    sa.ForeignKeyConstraint(["target_structure_id"], ["structures.id"]),
    sa.ForeignKeyConstraint(["target_element_id"], ["elements.id"]),
    sa.ForeignKeyConstraint(["target_association_id"], ["associations.id"]),
    sa.PrimaryKeyConstraint("id"),
    sa.CheckConstraint(_RULE_CATEGORY_CHECK, name="check_rule_category"),
    sa.CheckConstraint(_RULE_PATTERN_CHECK, name="check_rule_pattern"),
    sa.CheckConstraint(_RULE_SEVERITY_CHECK, name="check_rule_severity"),
    sa.CheckConstraint(_RULE_ORIGIN_CHECK, name="check_rule_origin"),
    sa.CheckConstraint(
      _RULE_TARGET_POLYMORPHISM_CHECK, name="check_rule_target_polymorphism"
    ),
  )
  op.create_index("idx_rules_taxonomy", "rules", ["taxonomy_id"])
  op.create_index(
    "idx_rules_target_structure",
    "rules",
    ["target_structure_id"],
    postgresql_where="target_structure_id IS NOT NULL",
  )
  op.create_index(
    "idx_rules_target_element",
    "rules",
    ["target_element_id"],
    postgresql_where="target_element_id IS NOT NULL",
  )
  op.create_index(
    "idx_rules_target_association",
    "rules",
    ["target_association_id"],
    postgresql_where="target_association_id IS NOT NULL",
  )
  op.create_index("idx_rules_category", "rules", ["rule_category"])

  # ──────────────────────────────────────────────────────────────────────
  # 7. Load JSON-LD seeds — two-pass to let arcs cross package boundaries.
  # ──────────────────────────────────────────────────────────────────────
  # Phase 1 writes every package's elements + classification vocabulary;
  # phase 2 writes every package's associations + classification
  # assignments. Splitting the phases means an arc in one seed can
  # reference an element defined in ANY other seed regardless of the
  # SEED_FILES order — without the split, arcs targeting elements that
  # only a later seed defines are silently dropped.
  from robosystems.taxonomy.loaders import load_taxonomy_package
  from robosystems.taxonomy.writers import (
    write_taxonomy_arcs,
    write_taxonomy_elements,
    write_taxonomy_rules,
  )

  loaded_packages = []
  for seed_path in SEED_FILES:
    if not seed_path.exists():
      print(f"  [WARN] Seed file missing, skipping: {seed_path}")
      continue
    print(f"  Loading seed: {seed_path.relative_to(SEEDS_DIR)}")
    package = load_taxonomy_package(seed_path)
    loaded_packages.append(package)
    counts = write_taxonomy_elements(conn, package)
    print(
      f"    [phase 1] elements={counts['elements']} labels={counts['labels']} "
      f"references={counts['references']} structures={counts['structures']} "
      f"classifications={counts['classifications']}"
    )

  for package in loaded_packages:
    counts = write_taxonomy_arcs(conn, package)
    print(
      f"  [phase 2] {package.name}: "
      f"associations={counts['associations']} "
      f"(skipped={counts['associations_skipped']}) "
      f"classification_assignments={counts['classification_assignments']} "
      f"(skipped={counts['classification_assignments_skipped']})"
    )

  # Phase 3 — rules. Runs after phase 2 so association-targeted rules can
  # resolve (though none of the seeded fac-rules use that target_kind yet).
  for package in loaded_packages:
    if not package.rules:
      continue
    counts = write_taxonomy_rules(conn, package)
    print(
      f"  [phase 3] {package.name}: "
      f"rules={counts['rules']} (skipped={counts['rules_skipped']})"
    )

  # Phase d backfill on public.structures: set concept/member_arrangement
  # + empty statement_renderer mechanics for the four library-seeded
  # statement block types. Has to run BEFORE copy_library_into_tenant so
  # the tenant copy picks up populated values rather than NULL. See
  # ``local/docs/specs/information-block.md`` section 5.9 block inventory
  # for the arrangement defaults.
  conn.execute(
    text(
      """
      UPDATE public.structures
      SET concept_arrangement = 'roll_up',
          member_arrangement = 'aggregation',
          artifact_mechanics = jsonb_build_object(
            'kind', 'statement_renderer'
          )
      WHERE structure_type IN (
        'balance_sheet', 'income_statement',
        'cash_flow_statement', 'equity_statement'
      )
        AND artifact_mechanics IS NULL
      """
    )
  )

  # ──────────────────────────────────────────────────────────────────────
  # 8. Tenant-schema rollout: schema alignment + library backfill + triggers.
  # ──────────────────────────────────────────────────────────────────────
  print("  Installing raise_library_immutable() function…")
  _install_raise_library_immutable_fn(conn)

  print("  Backfilling library into existing tenant schemas…")
  for_each_tenant_schema(conn, _backfill_library_into_tenant)


def downgrade() -> None:
  conn = op.get_bind()

  # Tenant teardown: drop triggers, restore narrow CHECKs, drop Phase d
  # columns, restore the old classification columns (best-effort —
  # values can't be recovered losslessly once the junction has been
  # used, so we leave them NULL).
  def _teardown_tenant(conn, schema: str) -> None:
    _drop_triggers_for_tenant(conn, schema)
    _restore_narrow_tenant_checks(conn, schema)
    _drop_phase_d_columns_from_tenant(conn, schema)

  for_each_tenant_schema(conn, _teardown_tenant)
  conn.execute(text("DROP FUNCTION IF EXISTS public.raise_library_immutable()"))

  # Delete library-origin data in FK-safe order.
  # Rules first — they FK structures / elements / associations.
  conn.execute(text("DELETE FROM public.rules"))
  conn.execute(text("DELETE FROM public.element_classifications"))
  conn.execute(text("DELETE FROM public.classifications"))
  conn.execute(text("DELETE FROM public.element_references"))
  conn.execute(text("DELETE FROM public.element_labels"))
  conn.execute(
    text(
      "DELETE FROM public.associations WHERE structure_id IN "
      "(SELECT id FROM public.structures WHERE taxonomy_id IN "
      "(SELECT id FROM public.taxonomies WHERE is_shared = true))"
    )
  )
  conn.execute(
    text(
      "DELETE FROM public.structures WHERE taxonomy_id IN "
      "(SELECT id FROM public.taxonomies WHERE is_shared = true)"
    )
  )
  conn.execute(
    text(
      "DELETE FROM public.elements WHERE taxonomy_id IN "
      "(SELECT id FROM public.taxonomies WHERE is_shared = true)"
    )
  )
  conn.execute(text("DELETE FROM public.taxonomies WHERE is_shared = true"))

  # Drop rules table (Phase d.2). IF EXISTS on the indexes because a
  # mid-transition downgrade from a partial run might not have created them.
  conn.execute(text("DROP INDEX IF EXISTS public.idx_rules_category"))
  conn.execute(text("DROP INDEX IF EXISTS public.idx_rules_target_association"))
  conn.execute(text("DROP INDEX IF EXISTS public.idx_rules_target_element"))
  conn.execute(text("DROP INDEX IF EXISTS public.idx_rules_target_structure"))
  conn.execute(text("DROP INDEX IF EXISTS public.idx_rules_taxonomy"))
  conn.execute(text("DROP TABLE IF EXISTS public.rules"))

  # Drop junction + vocabulary.
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

  # Drop labels + references.
  op.drop_index("idx_element_references_type", table_name="element_references")
  op.drop_index("idx_element_references_element", table_name="element_references")
  op.drop_table("element_references")
  op.drop_index("idx_element_labels_role", table_name="element_labels")
  op.drop_index("idx_element_labels_element", table_name="element_labels")
  op.drop_table("element_labels")

  # Restore CHECK constraints.
  op.drop_constraint("check_association_type", "associations", type_="check")
  op.create_check_constraint(
    "check_association_type", "associations", _NARROW_ASSOCIATION_CHECK
  )
  op.drop_constraint("check_element_source", "elements", type_="check")
  op.create_check_constraint(
    "check_element_source", "elements", _NARROW_ELEMENT_SOURCE_CHECK
  )
  op.drop_constraint("check_structure_type", "structures", type_="check")
  op.create_check_constraint(
    "check_structure_type", "structures", _NARROW_STRUCTURE_TYPE_CHECK
  )

  # Drop Phase d columns from public.structures (tenant-schema columns
  # were dropped above in _teardown_tenant). IF EXISTS makes this safe
  # when downgrading from a mid-transition state where an earlier
  # migration run installed 0002 before the Phase δ columns existed.
  for col in (
    "template_id",
    "parenthetical_note",
    "artifact_mechanics",
    "member_arrangement",
    "concept_arrangement",
  ):
    conn.execute(text(f"ALTER TABLE public.structures DROP COLUMN IF EXISTS {col}"))

  # Drop substitution_group.
  op.drop_index("idx_elements_substitution_group", table_name="elements")
  op.drop_column("elements", "substitution_group")

  # Re-add the old columns for 0001 compatibility. Values lost — 0001's
  # seed_reporting_taxonomy rerun repopulates them.
  op.add_column(
    "elements",
    sa.Column(
      "classification",
      sa.String(),
      nullable=False,
      server_default="asset",
    ),
  )
  op.add_column("elements", sa.Column("sub_classification", sa.String(), nullable=True))
  op.create_check_constraint(
    "check_element_classification",
    "elements",
    "classification IN ('asset', 'liability', 'equity', 'revenue', 'expense')",
  )
  op.create_index("idx_elements_classification", "elements", ["classification"])

  from robosystems.taxonomy.seed import seed_reporting_taxonomy

  seed_reporting_taxonomy(conn)
