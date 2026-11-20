"""Tenant schemas for DB-backed tests, built once per worker and reused.

Building the tenant tables is about 180 DDL statements, and provisioning a
tenant adds the library copy and its triggers. Paid per test, across a
parallel run, that was the largest cost in the suite. A fixture now claims a
schema under the name it wants and releases it afterwards:

    with engine.begin() as conn:
      claim_tenant_schema(conn, schema)        # bare tables, empty
    ...
    with engine.begin() as conn:
      release_tenant_schema(conn, schema)

    claim_provisioned_schema(GRAPH)            # as provision_tenant_schema leaves it
    ...
    with engine.begin() as conn:
      release_tenant_schema(conn, GRAPH)

The first claim of each kind on a database does the real work. A release
returns the schema to the state that work left it in and parks it under a
pool name; the next claim renames it. A schema whose structure a test changed
(a trigger, a column, a constraint) is dropped instead of parked, so
structural tests need no opt-out.
"""

from __future__ import annotations

import itertools
import os
import warnings
from dataclasses import dataclass

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection
from sqlalchemy.pool import NullPool

import robosystems.models.extensions  # noqa: F401  (register models on ExtensionsBase)
from robosystems.db.extensions import ExtensionsBase

_WORKER = os.environ.get("PYTEST_XDIST_WORKER", "main")
_BARE = "bare"
_PROVISIONED = "provisioned"


@dataclass
class _Parked:
  name: str
  kind: str
  fingerprint: tuple


_idle: dict[tuple[str, str], list[_Parked]] = {}
_claimed: dict[tuple[str, str], _Parked] = {}
_shared_tables_ready: set[str] = set()
# The rows provisioning leaves behind, per database: (snapshot schema, tables).
_snapshots: dict[str, tuple[str, list[str]]] = {}
_counter = itertools.count()

_FINGERPRINT = text(
  """
  SELECT
    (SELECT count(*) FROM pg_class c WHERE c.relnamespace = n.oid),
    (SELECT count(*) FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
      WHERE c.relnamespace = n.oid AND a.attnum > 0 AND NOT a.attisdropped),
    (SELECT count(*) FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid
      WHERE c.relnamespace = n.oid),
    (SELECT count(*) FROM pg_constraint k WHERE k.connamespace = n.oid),
    (SELECT md5(string_agg(pg_get_constraintdef(k.oid), ';' ORDER BY k.conname))
      FROM pg_constraint k WHERE k.connamespace = n.oid AND k.contype = 'c')
  FROM pg_namespace n WHERE n.nspname = :schema
  """
)


def _database(conn: Connection) -> str:
  """The server and database, whichever driver or engine reached it."""
  url = conn.engine.url.set(drivername="postgresql", query={})
  return url.render_as_string(hide_password=False)


def _tenant_tables():
  return [t for t in ExtensionsBase.metadata.sorted_tables if t.schema is None]


def _exists(conn: Connection, schema: str) -> bool:
  return bool(
    conn.execute(
      text("SELECT 1 FROM pg_namespace WHERE nspname = :schema"), {"schema": schema}
    ).scalar()
  )


def _fingerprint(conn: Connection, schema: str) -> tuple:
  return tuple(conn.execute(_FINGERPRINT, {"schema": schema}).one())


def _tables_with_rows(conn: Connection, schema: str) -> list[str]:
  probes = " UNION ALL ".join(
    f'SELECT \'{t.name}\' WHERE EXISTS (SELECT 1 FROM "{schema}"."{t.name}")'
    for t in _tenant_tables()
  )
  return list(conn.execute(text(probes)).scalars())


def _build(conn: Connection, schema: str) -> None:
  database = _database(conn)
  if database not in _shared_tables_ready:
    shared = [t for t in ExtensionsBase.metadata.sorted_tables if t.schema is not None]
    ExtensionsBase.metadata.create_all(bind=conn, tables=shared)
    _shared_tables_ready.add(database)
  conn.execute(text(f'CREATE SCHEMA "{schema}"'))
  before = conn.execute(text("SELECT current_setting('search_path')")).scalar()
  conn.execute(
    text("SELECT set_config('search_path', :path, true)"),
    {"path": f'"{schema}", public'},
  )
  ExtensionsBase.metadata.create_all(
    bind=conn, tables=_tenant_tables(), checkfirst=False
  )
  conn.execute(text("SELECT set_config('search_path', :path, true)"), {"path": before})


def _restore(conn: Connection, schema: str, kind: str) -> None:
  """Put the rows back as the build left them. Deletes, not TRUNCATE, which
  would swap every table's files; with triggers off the order of the
  statements does not matter and the library guards stay out of the way."""
  dirty = _tables_with_rows(conn, schema)
  snapshot, kept = _snapshots.get(_database(conn), ("", []))
  restored = kept if kind == _PROVISIONED else []
  if not dirty and not restored:
    return
  conn.execute(text("SELECT set_config('session_replication_role', 'replica', true)"))
  for name in dirty:
    conn.execute(text(f'DELETE FROM "{schema}"."{name}"'))
  for name in restored:
    conn.execute(
      text(f'INSERT INTO "{schema}"."{name}" SELECT * FROM "{snapshot}"."{name}"')
    )
  conn.execute(text("SELECT set_config('session_replication_role', 'origin', true)"))


def _take_parked(conn: Connection, schema: str, kind: str) -> bool:
  database = _database(conn)
  idle = _idle.setdefault((database, kind), [])
  while idle:
    parked = idle.pop()
    if _exists(conn, parked.name):
      conn.execute(text(f'ALTER SCHEMA "{parked.name}" RENAME TO "{schema}"'))
      _claimed[(database, schema)] = parked
      return True
  return False


def _record(conn: Connection, schema: str, kind: str) -> None:
  name = f"ext_pool_{_WORKER}_{next(_counter)}"
  _claimed[(_database(conn), schema)] = _Parked(name, kind, _fingerprint(conn, schema))


def claim_tenant_schema(conn: Connection, schema: str) -> None:
  """Leave ``schema`` on this database holding every tenant table, empty."""
  _claimed.pop((_database(conn), schema), None)
  conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
  if not _take_parked(conn, schema, _BARE):
    _build(conn, schema)
    _record(conn, schema, _BARE)


def claim_provisioned_schema(graph_id: str) -> None:
  """Leave ``graph_id`` on the extensions database as
  ``provision_tenant_schema`` leaves a new tenant: tables, the library copy
  and the guard triggers. The first claim in a worker runs the real thing."""
  from robosystems.db.extensions import _get_engine, provision_tenant_schema

  engine = _get_engine()
  with engine.begin() as conn:
    database = _database(conn)
    _claimed.pop((database, graph_id), None)
    conn.execute(text(f'DROP SCHEMA IF EXISTS "{graph_id}" CASCADE'))
    if _take_parked(conn, graph_id, _PROVISIONED):
      return
  provision_tenant_schema(graph_id)
  with engine.begin() as conn:
    if database not in _snapshots:
      snapshot = f"ext_snapshot_{_WORKER}"
      conn.execute(text(f'DROP SCHEMA IF EXISTS "{snapshot}" CASCADE'))
      conn.execute(text(f'CREATE SCHEMA "{snapshot}"'))
      kept = _tables_with_rows(conn, graph_id)
      for name in kept:
        conn.execute(
          text(
            f'CREATE TABLE "{snapshot}"."{name}" AS SELECT * FROM "{graph_id}"."{name}"'
          )
        )
      _snapshots[database] = (snapshot, kept)
    _record(conn, graph_id, _PROVISIONED)


def release_tenant_schema(conn: Connection, schema: str) -> None:
  """Park ``schema`` for the next claim, or drop it if a test reshaped it."""
  database = _database(conn)
  parked = _claimed.pop((database, schema), None)
  if parked is None or not _exists(conn, schema):
    conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
    return
  if _fingerprint(conn, schema) != parked.fingerprint:
    conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    return
  _restore(conn, schema, parked.kind)
  conn.execute(text(f'DROP SCHEMA IF EXISTS "{parked.name}" CASCADE'))
  conn.execute(text(f'ALTER SCHEMA "{schema}" RENAME TO "{parked.name}"'))
  _idle.setdefault((database, parked.kind), []).append(parked)


def drop_pooled_schemas() -> None:
  """Remove every parked schema; called once when the test session ends."""
  leftovers: dict[str, list[str]] = {}
  for (database, _kind), parked in _idle.items():
    leftovers.setdefault(database, []).extend(p.name for p in parked)
  for database, schema in _claimed:
    leftovers.setdefault(database, []).append(schema)
  for database, (snapshot, _tables) in _snapshots.items():
    leftovers.setdefault(database, []).append(snapshot)
  _idle.clear()
  _claimed.clear()
  _snapshots.clear()
  for database, schemas in leftovers.items():
    engine = create_engine(database, poolclass=NullPool)
    try:
      with engine.begin() as conn:
        for schema in schemas:
          conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
    except Exception as exc:
      warnings.warn(f"parked tenant schemas were not dropped: {exc!r}", stacklevel=1)
    finally:
      engine.dispose()
