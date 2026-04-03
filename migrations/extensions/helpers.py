"""Helpers for multi-tenant extensions migrations.

Schema-per-tenant means every DDL change to tenant tables must be applied
to both the public schema (via standard Alembic ops) AND every existing
tenant schema (via raw SQL). These helpers eliminate the boilerplate.

Usage in a migration::

    from migrations.extensions.helpers import TenantOps, for_each_tenant_schema

    def upgrade():
        # Public schema — standard Alembic
        op.add_column("elements", sa.Column("new_field", sa.String()))

        # Tenant schemas — helper-assisted
        conn = op.get_bind()
        def apply(conn, schema):
            t = TenantOps(conn, schema)
            t.add_column("elements", "new_field", "TEXT")

        for_each_tenant_schema(conn, apply)
"""

from collections.abc import Callable

from sqlalchemy import Connection, text


def for_each_tenant_schema(
  conn: Connection,
  fn: Callable[[Connection, str], None],
) -> None:
  """Apply a function to every existing tenant schema.

  Tenant schemas match the graph ID pattern: kg + 16+ hex chars.

  Args:
      conn: A SQLAlchemy connection (from op.get_bind()).
      fn: A callable(conn, schema_name) to apply per tenant.
  """
  schemas = conn.execute(
    text(
      "SELECT schema_name FROM information_schema.schemata "
      "WHERE schema_name ~ '^kg[0-9a-f]{16,}$'"
    )
  ).fetchall()

  for (schema_name,) in schemas:
    fn(conn, schema_name)


class TenantOps:
  """Schema-qualified DDL operations for tenant migrations.

  Wraps common Alembic-style operations into raw SQL that targets a
  specific tenant schema. All methods are idempotent where possible
  (IF NOT EXISTS / IF EXISTS).

  Usage::

      t = TenantOps(conn, "kg19d355cfe0460db38a")
      t.add_column("elements", "new_field", "TEXT")
      t.create_index("idx_elements_new", "elements", ["new_field"])
  """

  def __init__(self, conn: Connection, schema: str) -> None:
    self.conn = conn
    self.s = schema

  def _q(self, table: str) -> str:
    """Quote a schema-qualified table reference."""
    return f'"{self.s}".{table}'

  # ── Column operations ──────────────────────────────────────────────

  def add_column(
    self,
    table: str,
    col: str,
    col_type: str,
    *,
    nullable: bool = True,
    default: str | None = None,
  ) -> None:
    """Add a column if it doesn't already exist."""
    parts = f"ALTER TABLE {self._q(table)} ADD COLUMN IF NOT EXISTS {col} {col_type}"
    if not nullable:
      parts += " NOT NULL"
    if default is not None:
      parts += f" DEFAULT {default}"
    self.conn.execute(text(parts))

  def drop_column(self, table: str, col: str) -> None:
    """Drop a column if it exists."""
    self.conn.execute(text(f"ALTER TABLE {self._q(table)} DROP COLUMN IF EXISTS {col}"))

  def rename_column(self, table: str, old: str, new: str) -> None:
    """Rename a column."""
    self.conn.execute(
      text(f"ALTER TABLE {self._q(table)} RENAME COLUMN {old} TO {new}")
    )

  def alter_column_nullable(self, table: str, col: str, *, nullable: bool) -> None:
    """Set or drop NOT NULL on a column."""
    action = "DROP NOT NULL" if nullable else "SET NOT NULL"
    self.conn.execute(text(f"ALTER TABLE {self._q(table)} ALTER COLUMN {col} {action}"))

  def alter_column_default(self, table: str, col: str, default: str) -> None:
    """Set a column default."""
    self.conn.execute(
      text(f"ALTER TABLE {self._q(table)} ALTER COLUMN {col} SET DEFAULT {default}")
    )

  # ── Constraint operations ──────────────────────────────────────────

  def add_check(self, table: str, name: str, expr: str) -> None:
    """Add a CHECK constraint (no IF NOT EXISTS in PostgreSQL, so drop first)."""
    self.drop_check(table, name)
    self.conn.execute(
      text(f"ALTER TABLE {self._q(table)} ADD CONSTRAINT {name} CHECK ({expr})")
    )

  def drop_check(self, table: str, name: str) -> None:
    """Drop a constraint if it exists."""
    self.conn.execute(
      text(f"ALTER TABLE {self._q(table)} DROP CONSTRAINT IF EXISTS {name}")
    )

  def add_unique(self, table: str, name: str, columns: list[str]) -> None:
    """Add a UNIQUE constraint."""
    cols = ", ".join(columns)
    self.drop_check(table, name)  # reuse drop — works for any constraint
    self.conn.execute(
      text(f"ALTER TABLE {self._q(table)} ADD CONSTRAINT {name} UNIQUE ({cols})")
    )

  def add_foreign_key(
    self,
    table: str,
    name: str,
    columns: list[str],
    ref_table: str,
    ref_columns: list[str],
    *,
    on_delete: str | None = None,
  ) -> None:
    """Add a foreign key constraint."""
    cols = ", ".join(columns)
    refs = ", ".join(ref_columns)
    sql = (
      f"ALTER TABLE {self._q(table)} ADD CONSTRAINT {name} "
      f"FOREIGN KEY ({cols}) REFERENCES {self._q(ref_table)} ({refs})"
    )
    if on_delete:
      sql += f" ON DELETE {on_delete}"
    self.conn.execute(text(sql))

  # ── Index operations ───────────────────────────────────────────────

  def create_index(
    self,
    name: str,
    table: str,
    columns: list[str],
    *,
    unique: bool = False,
    where: str | None = None,
  ) -> None:
    """Create an index if it doesn't already exist."""
    uq = "UNIQUE " if unique else ""
    cols = ", ".join(columns)
    sql = f"CREATE {uq}INDEX IF NOT EXISTS {name} ON {self._q(table)} ({cols})"
    if where:
      sql += f" WHERE {where}"
    self.conn.execute(text(sql))

  def drop_index(self, name: str) -> None:
    """Drop an index if it exists (schema-qualified)."""
    self.conn.execute(text(f'DROP INDEX IF EXISTS "{self.s}".{name}'))

  # ── Table operations ───────────────────────────────────────────────

  def create_table(self, table: str, ddl: str) -> None:
    """Create a table if it doesn't exist. Pass the column DDL only."""
    self.conn.execute(text(f"CREATE TABLE IF NOT EXISTS {self._q(table)} ({ddl})"))

  def drop_table(self, table: str) -> None:
    """Drop a table if it exists."""
    self.conn.execute(text(f"DROP TABLE IF EXISTS {self._q(table)}"))

  def rename_table(self, old: str, new: str) -> None:
    """Rename a table within the schema."""
    self.conn.execute(text(f"ALTER TABLE {self._q(old)} RENAME TO {new}"))

  # ── Raw SQL ────────────────────────────────────────────────────────

  def execute(self, sql: str) -> None:
    """Execute raw SQL with {s} replaced by the quoted schema name.

    Usage::

        t.execute('SELECT count(*) FROM {s}.elements')
    """
    self.conn.execute(text(sql.replace("{s}", f'"{self.s}"')))
