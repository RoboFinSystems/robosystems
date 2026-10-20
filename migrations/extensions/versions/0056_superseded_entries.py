"""The ``superseded`` entry status: a posted row leaves an open month's books
without being deleted.

A correction that rebuilds what an event posted (a reconciling item's
``restate``, a replaced opening balance) used to delete the posted entries,
lines and transactions and write new ones, so nothing that referred to the
old rows could resolve them again. Those rows are now kept and move to
``superseded``: neither landed nor terminal, so every reader of the books
drops them, with ``superseded_at`` and a ``supersession`` record of why and
what replaced them (spec ``posted-rows-permanent`` §3.1).

The one-opening-per-entity index skips superseded rows, or the kept opening
would block its replacement.

Triggers make the rule hold whatever path writes (§3.3): a landed entry, line
item or transaction is never deleted; a landed entry's balance-bearing columns
never change, and its status only moves ``posted → reversed`` or ``posted →
superseded`` (a draft only to ``posted`` or ``shadowed``); a landed line keeps its amounts, account and entry; an event is
not deleted while it owns a landed row. A purge of an entity's books (entity
removal) opts out for its own transaction with ``SET LOCAL
robosystems.ledger_purge = 'on'``. The functions live in ``public``; each
tenant attaches them, here for existing tenants and in ``db/extensions.py``
for new ones. They guard against our own code, not an operator: the app's
role can set the escape hatch or drop a trigger.

Autogenerate sees the two columns on ``public`` but neither the CHECK, the
partial index's predicate nor the tenant schemas, so the body is written by
hand, in ``public`` and in every tenant. The downgrade refuses while any
schema holds a superseded entry: the narrower CHECK cannot hold it, and
returning it to ``posted`` would put a replaced entry back in the books.

Revision ID: 0056
Revises: 0055
Create Date: 2026-10-20
"""

from __future__ import annotations

from alembic import op
from sqlalchemy import text
from sqlalchemy.engine import Connection

from migrations.extensions.helpers import TenantOps, for_each_tenant_schema

revision = "0056"
down_revision = "0055"
branch_labels = None
depends_on = None

_STATUS_CHECK = "check_entry_status"
_WITH = "status IN ('draft', 'posted', 'reversed', 'shadowed', 'superseded')"
_WITHOUT = "status IN ('draft', 'posted', 'reversed', 'shadowed')"
_OPENING = "uq_entries_one_opening_per_entity"

_FUNCTIONS = """
CREATE OR REPLACE FUNCTION public.guard_landed_entry()
RETURNS trigger AS $$
BEGIN
  IF current_setting('robosystems.ledger_purge', true) = 'on' THEN
    RETURN COALESCE(NEW, OLD);
  END IF;
  IF OLD.status = 'draft' THEN
    -- A draft lands by posting or a shadow close; it is never reversed or
    -- superseded before it has landed.
    IF TG_OP = 'UPDATE' AND NEW.status NOT IN ('draft', 'posted', 'shadowed') THEN
      RAISE EXCEPTION 'ledger row permanent: draft entry % cannot move to %', OLD.id, NEW.status
        USING ERRCODE = 'restrict_violation';
    END IF;
    RETURN COALESCE(NEW, OLD);
  END IF;
  IF TG_OP = 'DELETE' THEN
    RAISE EXCEPTION 'ledger row permanent: entry % is %, so it is never deleted; reverse or supersede it', OLD.id, OLD.status
      USING ERRCODE = 'restrict_violation';
  END IF;
  IF NEW.posting_date IS DISTINCT FROM OLD.posting_date
     OR NEW.entity_id IS DISTINCT FROM OLD.entity_id
     OR NEW.transaction_id IS DISTINCT FROM OLD.transaction_id
     OR NEW.type IS DISTINCT FROM OLD.type THEN
    RAISE EXCEPTION 'ledger row permanent: entry % is %, so its date, entity, transaction and type do not change', OLD.id, OLD.status
      USING ERRCODE = 'restrict_violation';
  END IF;
  IF NEW.status IS DISTINCT FROM OLD.status
     AND NOT (OLD.status = 'posted' AND NEW.status IN ('reversed', 'superseded')) THEN
    RAISE EXCEPTION 'ledger row permanent: entry % cannot move from % to %', OLD.id, OLD.status, NEW.status
      USING ERRCODE = 'restrict_violation';
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION public.guard_landed_line_item()
RETURNS trigger AS $$
DECLARE
  parent_status text;
BEGIN
  IF current_setting('robosystems.ledger_purge', true) = 'on' THEN
    RETURN COALESCE(NEW, OLD);
  END IF;
  EXECUTE format('SELECT status FROM %I.entries WHERE id = $1', TG_TABLE_SCHEMA)
    INTO parent_status USING OLD.entry_id;
  IF parent_status IS NULL OR parent_status = 'draft' THEN
    RETURN COALESCE(NEW, OLD);
  END IF;
  IF TG_OP = 'DELETE' THEN
    RAISE EXCEPTION 'ledger row permanent: line % is on a % entry, so it is never deleted', OLD.id, parent_status
      USING ERRCODE = 'restrict_violation';
  END IF;
  IF NEW.debit_amount IS DISTINCT FROM OLD.debit_amount
     OR NEW.credit_amount IS DISTINCT FROM OLD.credit_amount
     OR NEW.element_id IS DISTINCT FROM OLD.element_id
     OR NEW.entry_id IS DISTINCT FROM OLD.entry_id THEN
    RAISE EXCEPTION 'ledger row permanent: line % is on a % entry, so its amounts, account and entry do not change', OLD.id, parent_status
      USING ERRCODE = 'restrict_violation';
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION public.guard_landed_transaction()
RETURNS trigger AS $$
BEGIN
  IF current_setting('robosystems.ledger_purge', true) = 'on' THEN
    RETURN OLD;
  END IF;
  IF OLD.status = 'posted' THEN
    RAISE EXCEPTION 'ledger row permanent: transaction % is posted, so it is never deleted', OLD.id
      USING ERRCODE = 'restrict_violation';
  END IF;
  RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION public.guard_event_with_landed_rows()
RETURNS trigger AS $$
DECLARE
  landed boolean;
BEGIN
  IF current_setting('robosystems.ledger_purge', true) = 'on' THEN
    RETURN OLD;
  END IF;
  EXECUTE format(
    'SELECT EXISTS (SELECT 1 FROM %1$I.entries WHERE triggered_by_event_id = $1 AND status <> ''draft'') '
    'OR EXISTS (SELECT 1 FROM %1$I.transactions WHERE triggered_by_event_id = $1 AND status = ''posted'')',
    TG_TABLE_SCHEMA
  ) INTO landed USING OLD.id;
  IF landed THEN
    RAISE EXCEPTION 'ledger row permanent: event % owns landed ledger rows, so it is never deleted', OLD.id
      USING ERRCODE = 'restrict_violation';
  END IF;
  RETURN OLD;
END;
$$ LANGUAGE plpgsql;
"""

_DROP_FUNCTIONS = """
DROP FUNCTION IF EXISTS public.guard_landed_entry();
DROP FUNCTION IF EXISTS public.guard_landed_line_item();
DROP FUNCTION IF EXISTS public.guard_landed_transaction();
DROP FUNCTION IF EXISTS public.guard_event_with_landed_rows();
"""

# (trigger, table, events, function): a static snapshot of
# `db/extensions.py`'s `_LEDGER_PERMANENCE_TRIGGERS`.
_TRIGGERS = (
  ("entries_landed_permanent", "entries", "UPDATE OR DELETE", "guard_landed_entry"),
  (
    "line_items_landed_permanent",
    "line_items",
    "UPDATE OR DELETE",
    "guard_landed_line_item",
  ),
  (
    "transactions_landed_permanent",
    "transactions",
    "DELETE",
    "guard_landed_transaction",
  ),
  ("events_landed_permanent", "events", "DELETE", "guard_event_with_landed_rows"),
)


def _attach(conn: Connection, schema: str) -> None:
  for trigger, table, events, function in _TRIGGERS:
    conn.execute(text(f'DROP TRIGGER IF EXISTS {trigger} ON "{schema}".{table}'))
    conn.execute(
      text(
        f'CREATE TRIGGER {trigger} BEFORE {events} ON "{schema}".{table} '
        f"FOR EACH ROW EXECUTE FUNCTION public.{function}()"
      )
    )


def _detach(conn: Connection, schema: str) -> None:
  for trigger, table, _events, _function in _TRIGGERS:
    conn.execute(text(f'DROP TRIGGER IF EXISTS {trigger} ON "{schema}".{table}'))


def _expand(conn: Connection, schema: str) -> None:
  t = TenantOps(conn, schema)
  t.add_column("entries", "superseded_at", "TIMESTAMP WITHOUT TIME ZONE")
  t.add_column("entries", "supersession", "JSONB")
  t.add_check("entries", _STATUS_CHECK, _WITH)
  t.drop_index(_OPENING)
  t.create_index(
    _OPENING,
    "entries",
    ["entity_id"],
    unique=True,
    where="type = 'opening' AND status <> 'superseded'",
  )


def _contract(conn: Connection, schema: str) -> None:
  superseded = conn.execute(
    text(f"SELECT count(*) FROM \"{schema}\".entries WHERE status = 'superseded'")
  ).scalar()
  if superseded:
    raise RuntimeError(
      f"Schema {schema} holds {superseded} superseded entries; the narrower "
      "status CHECK cannot hold them. Resolve them before downgrading."
    )
  t = TenantOps(conn, schema)
  t.drop_index(_OPENING)
  t.create_index(
    _OPENING, "entries", ["entity_id"], unique=True, where="type = 'opening'"
  )
  t.add_check("entries", _STATUS_CHECK, _WITHOUT)
  t.drop_column("entries", "supersession")
  t.drop_column("entries", "superseded_at")


def upgrade() -> None:
  conn = op.get_bind()
  _expand(conn, "public")
  for_each_tenant_schema(conn, _expand)
  conn.execute(text(_FUNCTIONS))
  for_each_tenant_schema(conn, _attach)


def downgrade() -> None:
  conn = op.get_bind()
  for_each_tenant_schema(conn, _detach)
  conn.execute(text(_DROP_FUNCTIONS))
  _contract(conn, "public")
  for_each_tenant_schema(conn, _contract)
