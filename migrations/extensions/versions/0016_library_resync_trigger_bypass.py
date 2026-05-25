"""library re-sync bypass for raise_library_immutable

Add a transaction-scoped escape hatch to the library-immutability trigger so
the seeder — and only the seeder — can update/delete library-origin rows in a
tenant schema during a controlled re-sync.

A re-sync transaction sets ``SET LOCAL robosystems.library_resync = 'on'``; the
trigger consults that GUC and returns NEW/OLD (lets the write through) instead
of raising. ``SET LOCAL`` is transaction-scoped — it cannot leak past
commit/rollback — and tenant-facing writes never set it, so customer mutation
of library-origin rows stays blocked exactly as before.

This replaces the two shared guard functions installed by 0002 —
``public.raise_library_immutable()`` (BEFORE UPDATE/DELETE) and
``public.raise_insert_into_library_structure()`` (BEFORE INSERT on associations).
**Both** matter for the re-sync: it issues ``INSERT … ON CONFLICT DO UPDATE`` on
associations, and the INSERT portion trips the insert guard before the conflict
even resolves. (The provision-time copy never hits either guard because it runs
before the per-tenant triggers are installed; the re-sync runs after.) Every
tenant trigger ``EXECUTE FUNCTION public.<fn>()``, so replacing the public
functions reaches all tenants without per-schema work.

Companion to ``resync_library_into_tenant`` (taxonomy/writer.py) and the
``taxonomy resync`` admin entrypoint.

Revision ID: 0016
Revises: 0015
Create Date: 2026-05-25

"""

from __future__ import annotations

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = "0016"
down_revision = "0015"
branch_labels = None
depends_on = None


# `current_setting(..., true)` is missing-ok: it returns NULL (not an error)
# when the GUC was never set, so the normal guard runs for every caller that
# has not opted into a re-sync transaction.

_IMMUTABLE_BYPASS = """
CREATE OR REPLACE FUNCTION public.raise_library_immutable()
RETURNS trigger AS $$
BEGIN
  -- Re-sync bypass: only a transaction that has set
  -- `SET LOCAL robosystems.library_resync = 'on'` may mutate library-origin
  -- rows. Transaction-scoped; cannot leak past commit/rollback.
  IF current_setting('robosystems.library_resync', true) = 'on' THEN
    IF TG_OP = 'UPDATE' THEN
      RETURN NEW;
    END IF;
    RETURN OLD;
  END IF;
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
  IF TG_OP = 'UPDATE' THEN
    RETURN NEW;
  END IF;
  RETURN OLD;
END;
$$ LANGUAGE plpgsql;
"""

# The INSERT guard on associations. The re-sync legitimately inserts/updates
# library arcs into library-seeded structures, so it must honor the same GUC.
_INSERT_GUARD_BYPASS = """
CREATE OR REPLACE FUNCTION public.raise_insert_into_library_structure()
RETURNS trigger AS $$
DECLARE
  structure_created_by text;
BEGIN
  IF current_setting('robosystems.library_resync', true) = 'on' THEN
    RETURN NEW;
  END IF;
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

# The original 0002 definitions, restored on downgrade (no GUC carve-out).
_IMMUTABLE_ORIGINAL = """
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
  IF TG_OP = 'UPDATE' THEN
    RETURN NEW;
  END IF;
  RETURN OLD;
END;
$$ LANGUAGE plpgsql;
"""

_INSERT_GUARD_ORIGINAL = """
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


def upgrade() -> None:
  conn = op.get_bind()
  conn.execute(text(_IMMUTABLE_BYPASS))
  conn.execute(text(_INSERT_GUARD_BYPASS))


def downgrade() -> None:
  conn = op.get_bind()
  conn.execute(text(_IMMUTABLE_ORIGINAL))
  conn.execute(text(_INSERT_GUARD_ORIGINAL))
