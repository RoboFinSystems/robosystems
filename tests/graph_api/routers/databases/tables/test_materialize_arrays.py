"""Fixed-size array columns (vector embeddings) through the DuckDB → Arrow → COPY path.

DuckDB 1.4.4 segfaults exporting a wide fixed-size ARRAY to Arrow on Graviton,
so array columns leave DuckDB as LIST and are cast back in Arrow before the
COPY. The segfault only reproduces on Graviton, so these tests guard the
invariant that prevents it — no fixed-size array ever crosses the DuckDB export
— and prove the vectors still land intact, with TRY_CAST's NULL-on-wrong-length
behaviour preserved.
"""

from types import SimpleNamespace

import duckdb
import ladybug as lbug
import pyarrow as pa
import pytest

from robosystems.graph_api.routers.databases.tables.materialize import (
  _build_reconciled_select,
  _build_type_safe_select,
  _fixed_array_arrow_type,
  _fixed_array_casts,
  _get_target_columns,
  _restore_fixed_arrays,
)

DIM = 384


@pytest.mark.unit
class TestFixedArrayArrowType:
  @pytest.mark.parametrize(
    "type_name,expected",
    [
      ("FLOAT[384]", pa.list_(pa.float32(), 384)),
      ("float[3]", pa.list_(pa.float32(), 3)),
      ("DOUBLE[8]", pa.list_(pa.float64(), 8)),
      ("INT64[2]", pa.list_(pa.int64(), 2)),
      ("INTEGER[2]", pa.list_(pa.int32(), 2)),
    ],
  )
  def test_maps_fixed_size_arrays(self, type_name, expected):
    assert _fixed_array_arrow_type(type_name) == expected

  @pytest.mark.parametrize("type_name", ["FLOAT", "FLOAT[]", "DOUBLE[]", "STRING[2]"])
  def test_ignores_everything_else(self, type_name):
    assert _fixed_array_arrow_type(type_name) is None

  def test_casts_skip_excluded_columns(self):
    casts = _fixed_array_casts(
      [("identifier", "STRING"), ("embedding", "FLOAT[4]"), ("other", "FLOAT[4]")],
      exclude_cols={"other"},
    )

    assert casts == {"embedding": pa.list_(pa.float32(), 4)}


@pytest.mark.unit
class TestSelectsExportArraysAsLists:
  def test_reconciled_select_casts_then_retypes_as_list(self):
    select = _build_reconciled_select(
      target_columns=[("identifier", "STRING"), ("embedding", "FLOAT[384]")],
      source_column_names=["identifier", "embedding"],
      source_table="staged",
    )

    assert '(TRY_CAST("embedding" AS FLOAT[384]))::FLOAT[] AS "embedding"' in select

  def test_type_safe_select_retypes_array_source_as_list(self):
    select = _build_type_safe_select(
      [("identifier", "VARCHAR"), ("embedding", "FLOAT[384]")]
    )

    assert select == '"identifier", ("embedding")::FLOAT[] AS "embedding"'


@pytest.mark.unit
class TestRestoreFixedArrays:
  def test_casts_list_back_to_fixed_size_and_keeps_nulls(self):
    batch = pa.RecordBatch.from_pydict(
      {
        "identifier": ["a", "b"],
        "embedding": pa.array([[1.0, 2.0, 3.0], None], pa.list_(pa.float32())),
      }
    )

    table = _restore_fixed_arrays(batch, {"embedding": pa.list_(pa.float32(), 3)})

    assert table.schema.field("embedding").type == pa.list_(pa.float32(), 3)
    assert table.column("embedding").to_pylist() == [[1.0, 2.0, 3.0], None]

  def test_leaves_batch_unchanged_without_casts(self):
    batch = pa.RecordBatch.from_pydict({"identifier": ["a"]})

    assert (
      _restore_fixed_arrays(batch, {}).schema == pa.Table.from_batches([batch]).schema
    )


def _service(conn):
  class _Ctx:
    def __enter__(self):
      return conn

    def __exit__(self, *exc):
      return False

  pool = SimpleNamespace(get_connection=lambda _graph_id: _Ctx())
  return SimpleNamespace(db_manager=SimpleNamespace(connection_pool=pool))


@pytest.fixture
def engines(tmp_path):
  """Staged Product rows shaped like a real feed: embeddings as DOUBLE[] lists,
  one of the wrong length, one missing."""
  duck = duckdb.connect(str(tmp_path / "staging.duckdb"))
  # As in production (see ARROW_STREAM_BATCH_ROWS): lists export as large_list.
  duck.execute("SET arrow_large_buffer_size=true")
  duck.execute("CREATE TABLE Product (identifier VARCHAR, embedding DOUBLE[])")
  duck.executemany(
    "INSERT INTO Product VALUES (?, ?)",
    [
      ("p1", [0.5] * DIM),
      ("p2", [0.25] * DIM),
      ("short", [1.0] * 3),
      ("none", None),
    ],
  )
  db = lbug.Database(str(tmp_path / "graph.lbug"))
  conn = lbug.Connection(db)
  conn.execute(
    f"CREATE NODE TABLE Product (identifier STRING, embedding FLOAT[{DIM}], "
    "PRIMARY KEY (identifier))"
  )
  yield duck, conn
  duck.close()


def _stream(duck, conn, select_expr, casts):
  """The production transport: DuckDB → Arrow batches → restore → COPY."""
  reader = duck.execute(f"SELECT {select_expr} FROM Product AS t").fetch_record_batch(
    1000
  )
  for arrow_batch in reader:
    exported = arrow_batch.schema.field("embedding").type
    assert not pa.types.is_fixed_size_list(exported), (
      f"a fixed-size array crossed the DuckDB → Arrow export ({exported}); "
      "that export segfaults DuckDB on Graviton"
    )
    copy_batch = _restore_fixed_arrays(arrow_batch, casts)  # noqa: F841
    conn.execute("COPY Product FROM copy_batch")


def _embeddings(conn):
  rows = conn.execute(
    "MATCH (p:Product) RETURN p.identifier, p.embedding ORDER BY p.identifier"
  ).get_all()
  return dict(rows)


@pytest.mark.unit
def test_vectors_land_intact_through_the_reconciled_path(engines):
  duck, conn = engines
  target = _get_target_columns(_service(conn), "g", "Product")
  assert target is not None
  select = _build_reconciled_select(target, ["identifier", "embedding"], "Product")

  _stream(duck, conn, select, _fixed_array_casts(target))

  embeddings = _embeddings(conn)
  assert embeddings["p1"] == pytest.approx([0.5] * DIM)
  assert embeddings["p2"] == pytest.approx([0.25] * DIM)
  assert embeddings["short"] is None, "a wrong-length vector must become NULL"
  assert embeddings["none"] is None


@pytest.mark.unit
def test_nulled_embeddings_keep_their_slot(engines):
  duck, conn = engines
  target = _get_target_columns(_service(conn), "g", "Product")
  assert target is not None
  select = _build_reconciled_select(
    target, ["identifier", "embedding"], "Product", null_cols={"embedding"}
  )

  _stream(duck, conn, select, _fixed_array_casts(target))

  assert set(_embeddings(conn).values()) == {None}
  assert len(_embeddings(conn)) == 4


@pytest.mark.unit
def test_array_typed_staging_column_exports_as_list(engines):
  """A staged column that is already a DuckDB ARRAY (type-safe path)."""
  duck, conn = engines
  duck.execute(
    f"CREATE TABLE Arr AS SELECT identifier, TRY_CAST(embedding AS FLOAT[{DIM}]) "
    "AS embedding FROM Product"
  )
  source = [
    (name, dtype)
    for name, dtype in duck.execute(
      "SELECT column_name, data_type FROM information_schema.columns "
      "WHERE table_name = 'Arr' ORDER BY ordinal_position"
    ).fetchall()
  ]
  select = _build_type_safe_select(source)
  reader = duck.execute(f"SELECT {select} FROM Arr").fetch_record_batch(1000)

  batch = next(iter(reader))

  assert not pa.types.is_fixed_size_list(batch.schema.field("embedding").type)
  restored = _restore_fixed_arrays(batch, _fixed_array_casts(source))
  assert restored.schema.field("embedding").type == pa.list_(pa.float32(), DIM)
