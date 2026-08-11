"""Restore snake_case keys in ``rules.rule_variables``.

Migration 0030 rewrote ``rule_variables`` for every rule in the pinned
packages and serialized each entry with the JSON-LD spelling
(``variableName`` / ``variableQname``). That spelling is correct in the
framework source — ``taxonomy/loader.py`` reads those RDF predicates and
``arelle/context.py`` declares them — but it is the *wire* form. The
column's contract is snake_case: ``rules/evaluators.py`` indexes
``v["variable_name"]`` directly, so a camelCase row raises
``KeyError('variable_name')`` and the evaluation records
``status='error'`` instead of pass/fail.

The effect was every ``RollUp`` rule in the rs-gaap-rollup-rules package —
21 rows in ``public`` and 21 in each provisioned tenant — erroring on every
evaluation. Rules of other patterns were untouched (the pinned packages
carry no others), so the failure is bounded to the rollup surface rather
than the engine.

0030's guard compared the new value against the stored one, and camelCase
differs from snake_case for *every* rule, so the update landed on all 21
rather than only the six whose enumerations 0030 meant to correct. The
values themselves are right — names, qnames and ordering all survived — so
this migration is a pure key rename and preserves the #898 repair.

Matched on the presence of the camelCase key rather than on
``created_by``, so a row that acquired the bad shape by any route is
repaired; the ``WHERE`` makes a second run a no-op.

Revision ID: 0031
Revises: 0030
"""

from __future__ import annotations

from alembic import op
from sqlalchemy import text

from migrations.extensions.helpers import for_each_tenant_schema

revision = "0031"
down_revision = "0030"
branch_labels = None
depends_on = None

# COALESCE so a row that is already snake_case (or was written by a mixed
# path) keeps its value rather than being nulled out — the WHERE below
# should exclude those, but the rewrite must not depend on it.
_REKEY_SQL = """
  UPDATE {schema}.rules
     SET rule_variables = (
       SELECT jsonb_agg(
                jsonb_build_object(
                  'variable_name',
                  COALESCE(v ->> 'variableName', v ->> 'variable_name'),
                  'variable_qname',
                  COALESCE(v ->> 'variableQname', v ->> 'variable_qname')
                )
                ORDER BY ord
              )
         FROM jsonb_array_elements(rule_variables) WITH ORDINALITY AS t(v, ord)
     )
   WHERE rule_variables IS NOT NULL
     AND jsonb_typeof(rule_variables) = 'array'
     AND EXISTS (
       SELECT 1
         FROM jsonb_array_elements(rule_variables) AS e(v)
        WHERE e.v ? 'variableName' OR e.v ? 'variableQname'
     )
"""


def _rekey(conn, schema: str) -> None:
  result = conn.execute(text(_REKEY_SQL.format(schema=f'"{schema}"')))
  if result.rowcount:
    print(f"  [rule_variables rekey -> {schema}] updated={result.rowcount}")


def upgrade() -> None:
  from robosystems.taxonomy.writer import SET_LIBRARY_RESYNC

  conn = op.get_bind()

  # The 0016 immutability triggers reject UPDATE on library-origin rows
  # without the bypass GUC. SET LOCAL is transaction-scoped and covers the
  # public fix and the whole tenant fan-out.
  conn.execute(text(SET_LIBRARY_RESYNC))

  _rekey(conn, "public")
  for_each_tenant_schema(conn, _rekey)


def downgrade() -> None:
  """Intentionally a no-op.

  Reinstating the camelCase keys would restore a state in which every
  RollUp rule errors at evaluation. There is nothing to roll back to.
  """
