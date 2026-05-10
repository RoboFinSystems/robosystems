"""Frameworks + Bridges first-class tables.

Adds four public-schema tables that record taxonomy library
**composition**:

- ``frameworks`` — named, versioned bundles (rs-gaap-base@v1, etc.)
- ``framework_packages`` — junction pinning specific (package, version)
  tuples into a framework, with load-order ordinal + is_required flag
- ``bridges`` — cross-namespace equivalence taxonomies (metadata
  overlay; arc content lives in ``taxonomies`` / ``associations`` as
  with any package)
- ``framework_bridges`` — junction pinning bridges into a framework

Then walks ``robosystems/taxonomy/frameworks/*/v*.json`` and inserts
one Framework row per manifest, one FrameworkPackage row per
``packages[]`` entry, one Bridge row per bridge that's loaded as a
package (looked up via the ``standard`` field), and one
FrameworkBridge row per ``bridges[]`` entry.

Frameworks live in the public schema only — they're library metadata,
not per-tenant content. Tenants reference them by name through
``Graph.taxonomy_pin = {"framework": "rs-gaap-base@v1"}``; the
resolver expands the framework into the flat ``{standard: version}``
pin shape that ``copy_library_into_tenant`` consumes.

This migration is content-driven: it walks the framework manifests
on disk and inserts what they declare. Re-running the migration
re-inserts the same rows (UUID5-keyed; ON CONFLICT DO UPDATE for
metadata refresh).

Revision ID: 0007
Revises: 0006
Create Date: 2026-05-10
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text
from sqlalchemy.dialects import postgresql

from robosystems.taxonomy.loaders.discovery import (
  FRAMEWORKS_DIR,
)
from robosystems.utils.uuid import generate_deterministic_uuid

# revision identifiers, used by Alembic.
revision = "0007"
down_revision = "0006"
branch_labels = None
depends_on = None


def _framework_id(framework: str, version: str) -> str:
  return generate_deterministic_uuid(f"{framework}:{version}", namespace="framework")


def _framework_package_id(framework_id: str, std: str, ver: str) -> str:
  return generate_deterministic_uuid(
    f"{framework_id}:{std}:{ver}", namespace="framework_package"
  )


def _bridge_id(bridge: str, version: str) -> str:
  return generate_deterministic_uuid(f"{bridge}:{version}", namespace="bridge")


def _framework_bridge_id(framework_id: str, bridge_id: str) -> str:
  return generate_deterministic_uuid(
    f"{framework_id}:{bridge_id}", namespace="framework_bridge"
  )


def _list_framework_manifests() -> list[Path]:
  """Walk ``frameworks/{name}/{version}.json`` for every manifest."""
  if not FRAMEWORKS_DIR.exists():
    return []
  paths: list[Path] = []
  for name_dir in sorted(FRAMEWORKS_DIR.iterdir()):
    if not name_dir.is_dir() or name_dir.name.startswith("."):
      continue
    for entry in sorted(name_dir.iterdir()):
      if entry.is_file() and entry.suffix == ".json":
        paths.append(entry)
  return paths


def _seed_framework_rows(conn) -> None:
  """Insert Framework + FrameworkPackage + Bridge + FrameworkBridge
  rows from every manifest under ``frameworks/``.

  Idempotent: ON CONFLICT (id) DO UPDATE refreshes metadata so a
  manifest edit followed by a migration re-run propagates.
  """
  now = datetime.now(UTC)
  for manifest_path in _list_framework_manifests():
    manifest = json.loads(manifest_path.read_text())

    framework_name = manifest["framework"]
    version = manifest["version"]
    fid = _framework_id(framework_name, version)

    # ── frameworks row ────────────────────────────────────────────
    conn.execute(
      text("""
        INSERT INTO public.frameworks (
          id, framework, version, title, description, framework_type,
          is_active, metadata, created_at, updated_at, created_by
        ) VALUES (
          :id, :framework, :version, :title, :description, :framework_type,
          true, '{}'::jsonb, :now, :now, 'library-seeder'
        )
        ON CONFLICT (id) DO UPDATE SET
          title          = EXCLUDED.title,
          description    = EXCLUDED.description,
          framework_type = EXCLUDED.framework_type,
          is_active      = EXCLUDED.is_active,
          updated_at     = EXCLUDED.updated_at
      """),
      {
        "id": fid,
        "framework": framework_name,
        "version": version,
        "title": manifest.get("title"),
        "description": manifest.get("description"),
        "framework_type": manifest.get("framework_type", "reporting"),
        "now": now,
      },
    )

    # ── framework_packages junction rows ──────────────────────────
    for pkg in manifest.get("packages", []):
      std = pkg["standard"]
      ver = pkg["version"]
      fpid = _framework_package_id(fid, std, ver)
      conn.execute(
        text("""
          INSERT INTO public.framework_packages (
            id, framework_id, package_standard, package_version,
            ordinal, is_required, metadata, created_at, created_by
          ) VALUES (
            :id, :fid, :std, :ver, :ordinal, :is_required,
            '{}'::jsonb, :now, 'library-seeder'
          )
          ON CONFLICT (id) DO UPDATE SET
            ordinal     = EXCLUDED.ordinal,
            is_required = EXCLUDED.is_required
        """),
        {
          "id": fpid,
          "fid": fid,
          "std": std,
          "ver": ver,
          "ordinal": pkg.get("ordinal", 0),
          "is_required": pkg.get("is_required", True),
          "now": now,
        },
      )

    # ── bridges + framework_bridges rows ──────────────────────────
    # Bridge rows are deduped across frameworks by (bridge, version).
    # Each manifest declares its bridges in `bridges[]` with the
    # same ordinal/is_required shape as packages; the bridge's
    # source/target namespaces come from the bridge's
    # taxonomy.jsonld (loaded into the `taxonomies` table by
    # 0002), which we look up by `standard` to get the metadata.
    for brg in manifest.get("bridges", []):
      bridge_name = brg["bridge"]
      bridge_version = brg["version"]
      bid = _bridge_id(bridge_name, bridge_version)

      # Pull namespace metadata off the bridge's taxonomy.jsonld
      # if it's been loaded into public.taxonomies. If the bridge
      # isn't loaded yet (is_required=false on a not-yet-authored
      # bridge), default to the bridge name as both source/target —
      # the row exists for FK purposes but admins can backfill the
      # namespaces after the bridge is authored.
      tax_row = conn.execute(
        text("""
          SELECT t.metadata
          FROM public.taxonomies t
          WHERE t.standard = :std AND t.version = :ver
          LIMIT 1
        """),
        {"std": bridge_name, "ver": bridge_version},
      ).first()

      source_ns = bridge_name
      target_ns = bridge_name
      if tax_row and tax_row[0]:
        meta = tax_row[0]
        # Discover source/target namespace from the taxonomy row's
        # metadata blob if the JSON-LD declared them.
        source_ns = meta.get("source_namespace", bridge_name)
        target_ns = meta.get("target_namespace", bridge_name)

      conn.execute(
        text("""
          INSERT INTO public.bridges (
            id, bridge, version, title, description,
            source_namespace, target_namespace,
            is_active, metadata, created_at, updated_at, created_by
          ) VALUES (
            :id, :bridge, :version, NULL, NULL,
            :source_ns, :target_ns,
            true, '{}'::jsonb, :now, :now, 'library-seeder'
          )
          ON CONFLICT (id) DO UPDATE SET
            source_namespace = EXCLUDED.source_namespace,
            target_namespace = EXCLUDED.target_namespace,
            updated_at       = EXCLUDED.updated_at
        """),
        {
          "id": bid,
          "bridge": bridge_name,
          "version": bridge_version,
          "source_ns": source_ns,
          "target_ns": target_ns,
          "now": now,
        },
      )

      # framework_bridges row
      fbid = _framework_bridge_id(fid, bid)
      conn.execute(
        text("""
          INSERT INTO public.framework_bridges (
            id, framework_id, bridge_id, ordinal, is_required,
            metadata, created_at, created_by
          ) VALUES (
            :id, :fid, :bid, :ordinal, :is_required,
            '{}'::jsonb, :now, 'library-seeder'
          )
          ON CONFLICT (id) DO UPDATE SET
            ordinal     = EXCLUDED.ordinal,
            is_required = EXCLUDED.is_required
        """),
        {
          "id": fbid,
          "fid": fid,
          "bid": bid,
          "ordinal": brg.get("ordinal", 0),
          "is_required": brg.get("is_required", True),
          "now": now,
        },
      )


# Widen the elements.source CHECK constraint to admit the namespace
# prefixes used by the rs-gaap-base framework extension packages
# (disclosures, checklist, styles). Without this, elements from those
# packages get rejected at INSERT time. The widen runs in every
# tenant schema (each has its own copy of the constraint) plus the
# public schema.
_WIDENED_ELEMENT_SOURCE_CHECK = (
  "source IN ("
  "'fac', 'rs-gaap', 'us-gaap', 'ifrs', "
  "'quickbooks', 'xero', 'plaid', 'native', 'import', 'system', "
  "'disclosures', 'checklist', 'styles'"
  ")"
)
_PRIOR_ELEMENT_SOURCE_CHECK = (
  "source IN ("
  "'fac', 'rs-gaap', 'us-gaap', 'ifrs', "
  "'quickbooks', 'xero', 'plaid', 'native', 'import', 'system'"
  ")"
)

# Widen the associations.association_type CHECK to admit 'definition' arcs
# (Disclosure Mechanics + Reporting Checklist + Reporting Style packages all
# encode their semantics as definition-arcrole arcs).
_WIDENED_ASSOCIATION_TYPE_CHECK = (
  "association_type IN ("
  "'presentation', 'calculation', 'mapping', "
  "'equivalence', 'general-special', 'essence-alias', "
  "'definition'"
  ")"
)
_PRIOR_ASSOCIATION_TYPE_CHECK = (
  "association_type IN ("
  "'presentation', 'calculation', 'mapping', "
  "'equivalence', 'general-special', 'essence-alias'"
  ")"
)


def _widen_association_type_check(conn, schema: str) -> None:
  """Drop and re-add the associations.association_type CHECK with the widened list."""
  if schema == "public":
    table = "public.associations"
    constraint = "check_association_type"
  else:
    table = f'"{schema}".associations'
    constraint = f"check_{schema}_association_type"
  conn.execute(text(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {constraint}"))
  conn.execute(
    text(
      f"ALTER TABLE {table} ADD CONSTRAINT {constraint} "
      f"CHECK ({_WIDENED_ASSOCIATION_TYPE_CHECK})"
    )
  )


def _restore_association_type_check(conn, schema: str) -> None:
  """Inverse of :func:`_widen_association_type_check` for downgrade."""
  if schema == "public":
    table = "public.associations"
    constraint = "check_association_type"
  else:
    table = f'"{schema}".associations'
    constraint = f"check_{schema}_association_type"
  conn.execute(text(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {constraint}"))
  conn.execute(
    text(
      f"ALTER TABLE {table} ADD CONSTRAINT {constraint} "
      f"CHECK ({_PRIOR_ASSOCIATION_TYPE_CHECK})"
    )
  )


def _widen_element_source_check(conn, schema: str) -> None:
  """Drop and re-add the elements.source CHECK with the widened list."""
  if schema == "public":
    table = "public.elements"
    constraint = "check_element_source"
  else:
    table = f'"{schema}".elements'
    constraint = f"check_{schema}_element_source"
  conn.execute(text(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {constraint}"))
  conn.execute(
    text(
      f"ALTER TABLE {table} ADD CONSTRAINT {constraint} "
      f"CHECK ({_WIDENED_ELEMENT_SOURCE_CHECK})"
    )
  )


def _restore_element_source_check(conn, schema: str) -> None:
  """Inverse of :func:`_widen_element_source_check` for downgrade."""
  if schema == "public":
    table = "public.elements"
    constraint = "check_element_source"
  else:
    table = f'"{schema}".elements'
    constraint = f"check_{schema}_element_source"
  conn.execute(text(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {constraint}"))
  conn.execute(
    text(
      f"ALTER TABLE {table} ADD CONSTRAINT {constraint} "
      f"CHECK ({_PRIOR_ELEMENT_SOURCE_CHECK})"
    )
  )


def upgrade() -> None:
  # ── bridges table ──────────────────────────────────────────────
  op.create_table(
    "bridges",
    sa.Column("id", sa.String(), nullable=False),
    sa.Column("bridge", sa.String(), nullable=False),
    sa.Column("version", sa.String(), nullable=False),
    sa.Column("title", sa.String(), nullable=True),
    sa.Column("description", sa.String(), nullable=True),
    sa.Column("source_namespace", sa.String(), nullable=False),
    sa.Column("target_namespace", sa.String(), nullable=False),
    sa.Column("is_active", sa.Boolean(), nullable=False),
    sa.Column("metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column("created_at", sa.DateTime(), nullable=False),
    sa.Column("updated_at", sa.DateTime(), nullable=False),
    sa.Column("created_by", sa.String(), nullable=False),
    sa.PrimaryKeyConstraint("id"),
    sa.UniqueConstraint("bridge", "version", name="uq_bridges_bridge_version"),
    schema="public",
  )
  op.create_index(
    "idx_bridges_active", "bridges", ["is_active"], unique=False, schema="public"
  )
  op.create_index(
    "idx_bridges_namespaces",
    "bridges",
    ["source_namespace", "target_namespace"],
    unique=False,
    schema="public",
  )

  # ── frameworks table ───────────────────────────────────────────
  op.create_table(
    "frameworks",
    sa.Column("id", sa.String(), nullable=False),
    sa.Column("framework", sa.String(), nullable=False),
    sa.Column("version", sa.String(), nullable=False),
    sa.Column("title", sa.String(), nullable=True),
    sa.Column("description", sa.String(), nullable=True),
    sa.Column("framework_type", sa.String(), nullable=False),
    sa.Column("is_active", sa.Boolean(), nullable=False),
    sa.Column("metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column("created_at", sa.DateTime(), nullable=False),
    sa.Column("updated_at", sa.DateTime(), nullable=False),
    sa.Column("created_by", sa.String(), nullable=False),
    sa.CheckConstraint(
      "framework_type IN ('reporting', 'extension', 'custom')",
      name="check_framework_type",
    ),
    sa.PrimaryKeyConstraint("id"),
    sa.UniqueConstraint("framework", "version", name="uq_frameworks_framework_version"),
    schema="public",
  )
  op.create_index(
    "idx_frameworks_active",
    "frameworks",
    ["is_active"],
    unique=False,
    schema="public",
  )

  # ── framework_bridges junction ─────────────────────────────────
  op.create_table(
    "framework_bridges",
    sa.Column("id", sa.String(), nullable=False),
    sa.Column("framework_id", sa.String(), nullable=False),
    sa.Column("bridge_id", sa.String(), nullable=False),
    sa.Column("ordinal", sa.Integer(), nullable=False),
    sa.Column("is_required", sa.Boolean(), nullable=False),
    sa.Column("metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column("created_at", sa.DateTime(), nullable=False),
    sa.Column("created_by", sa.String(), nullable=False),
    sa.ForeignKeyConstraint(["bridge_id"], ["public.bridges.id"]),
    sa.ForeignKeyConstraint(["framework_id"], ["public.frameworks.id"]),
    sa.PrimaryKeyConstraint("id"),
    sa.UniqueConstraint(
      "framework_id",
      "bridge_id",
      name="uq_framework_bridges_framework_bridge",
    ),
    schema="public",
  )
  op.create_index(
    "idx_framework_bridges_bridge",
    "framework_bridges",
    ["bridge_id"],
    unique=False,
    schema="public",
  )
  op.create_index(
    "idx_framework_bridges_framework",
    "framework_bridges",
    ["framework_id"],
    unique=False,
    schema="public",
  )

  # ── framework_packages junction ────────────────────────────────
  op.create_table(
    "framework_packages",
    sa.Column("id", sa.String(), nullable=False),
    sa.Column("framework_id", sa.String(), nullable=False),
    sa.Column("package_standard", sa.String(), nullable=False),
    sa.Column("package_version", sa.String(), nullable=False),
    sa.Column("ordinal", sa.Integer(), nullable=False),
    sa.Column("is_required", sa.Boolean(), nullable=False),
    sa.Column("metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column("created_at", sa.DateTime(), nullable=False),
    sa.Column("created_by", sa.String(), nullable=False),
    sa.ForeignKeyConstraint(["framework_id"], ["public.frameworks.id"]),
    sa.PrimaryKeyConstraint("id"),
    sa.UniqueConstraint(
      "framework_id",
      "package_standard",
      "package_version",
      name="uq_framework_packages_framework_pkg",
    ),
    schema="public",
  )
  op.create_index(
    "idx_framework_packages_framework",
    "framework_packages",
    ["framework_id"],
    unique=False,
    schema="public",
  )
  op.create_index(
    "idx_framework_packages_pkg",
    "framework_packages",
    ["package_standard", "package_version"],
    unique=False,
    schema="public",
  )

  # ── widen elements.source CHECK in public + every tenant ──────
  conn = op.get_bind()
  from migrations.extensions.helpers import for_each_tenant_schema

  _widen_element_source_check(conn, "public")
  for_each_tenant_schema(conn, _widen_element_source_check)

  _widen_association_type_check(conn, "public")
  for_each_tenant_schema(conn, _widen_association_type_check)

  # ── seed framework manifest rows ───────────────────────────────
  _seed_framework_rows(conn)


def downgrade() -> None:
  conn = op.get_bind()
  from migrations.extensions.helpers import for_each_tenant_schema

  _restore_element_source_check(conn, "public")
  for_each_tenant_schema(conn, _restore_element_source_check)

  _restore_association_type_check(conn, "public")
  for_each_tenant_schema(conn, _restore_association_type_check)

  op.drop_index(
    "idx_framework_packages_pkg",
    table_name="framework_packages",
    schema="public",
  )
  op.drop_index(
    "idx_framework_packages_framework",
    table_name="framework_packages",
    schema="public",
  )
  op.drop_table("framework_packages", schema="public")

  op.drop_index(
    "idx_framework_bridges_framework",
    table_name="framework_bridges",
    schema="public",
  )
  op.drop_index(
    "idx_framework_bridges_bridge",
    table_name="framework_bridges",
    schema="public",
  )
  op.drop_table("framework_bridges", schema="public")

  op.drop_index("idx_frameworks_active", table_name="frameworks", schema="public")
  op.drop_table("frameworks", schema="public")

  op.drop_index("idx_bridges_namespaces", table_name="bridges", schema="public")
  op.drop_index("idx_bridges_active", table_name="bridges", schema="public")
  op.drop_table("bridges", schema="public")
