"""The analyzer agrees with the engine on what is not a read.

Every statement here does something other than read. Each is spelled many
ways, the engine is asked whether it accepts the spelling, and whatever the
engine accepts the analyzer must call a non-read. The separators come from the
engine itself, so an engine upgrade that widens its grammar fails here first.
"""

import itertools
import re

import ladybug as lb
import pytest

from robosystems.security.cypher_analyzer import (
  CypherSecurityAnalyzer,
  has_opaque_statement_call,
  has_unsafe_vector_query,
  is_admin_operation,
  is_bulk_operation,
  is_non_read_call,
  is_schema_ddl,
  is_write_operation,
  references_catalog_label,
)

pytestmark = pytest.mark.unit

NUMBERS = ["1", "1.5", "1e5", "1E5", "1.0e5", ".5", ".5e1", "1e-5"]


def is_non_read(statement: str) -> bool:
  return (
    is_write_operation(statement)
    or is_bulk_operation(statement)
    or is_admin_operation(statement)
    or is_schema_ddl(statement)
    or is_non_read_call(statement)
    or has_opaque_statement_call(statement)
    or references_catalog_label(statement)
  )


@pytest.fixture(scope="module")
def engine(tmp_path_factory):
  directory = tmp_path_factory.mktemp("analyzer_engine")
  database = lb.Database(str(directory / "t.lbdb"))
  connection = lb.Connection(database)
  connection.execute(
    "CREATE NODE TABLE T(id INT64, v DOUBLE, x INT64, s STRING, PRIMARY KEY(id))"
  )
  connection.execute("CREATE NODE TABLE L(id SERIAL, PRIMARY KEY(id))")
  connection.execute("CREATE SEQUENCE sq")
  connection.execute("CREATE (:T {id: 1, v: 1.0, x: 0, s: 'a'})")
  data_file = directory / "rows.csv"
  data_file.write_text("k,v\na,1\n")

  class Engine:
    path = directory
    csv = str(data_file)

    def accepts(self, statement: str) -> bool:
      try:
        connection.execute(statement)
        return True
      except Exception:
        return False
      finally:
        try:
          connection.execute("ROLLBACK")
        except Exception:
          pass

    def rows(self, statement: str) -> list[tuple]:
      result = connection.execute(statement)
      found = []
      while result.has_next():
        found.append(tuple(result.get_next()))
      return found

  yield Engine()


@pytest.fixture(scope="module")
def separators(engine):
  """Every character the engine reads as a gap between two tokens."""
  found = [
    chr(point)
    for point in range(0x10000)
    if not (0xD800 <= point <= 0xDFFF)
    and not chr(point).isalnum()
    and chr(point) not in "'\"`_"
    and engine.accepts(f"RETURN{chr(point)}1")
  ]
  assert " " in found and "᠎" in found
  return found


@pytest.fixture(scope="module")
def leaders(engine):
  """Every character the engine skips in front of a statement."""
  return [
    chr(point)
    for point in range(0x10000)
    if not (0xD800 <= point <= 0xDFFF)
    and not chr(point).isalnum()
    and chr(point) not in "'\"`_"
    and engine.accepts(f"{chr(point)}RETURN 1")
  ]


def _clauses(engine) -> list[str]:
  """Non-read clauses that can follow a predicate."""
  return [
    "SET t.x = 5",
    "DELETE t",
    "DETACH DELETE t",
    "REMOVE t.x",
    "CREATE (:L)",
    "MERGE (:T {id: 1})",
    f"LOAD FROM '{engine.csv}' (header=true) RETURN *",
    f"COPY (MATCH (u:T) RETURN u.id) TO '{engine.path}/out.csv'",
    "CALL clear_warnings() RETURN 1",
    "CALL disk_info() RETURN 1",
  ]


def _statements() -> list[str]:
  """Non-read statements that stand on their own."""
  return [
    "CHECKPOINT",
    "BEGIN TRANSACTION",
    "BEGIN TRANSACTION READ ONLY",
    "COMMENT ON TABLE T IS 'c'",
    "CALL threads=1",
    "CALL timeout=100000",
    "CALL clear_warnings()",
    "CALL disk_info() RETURN *",
    "CALL storage_info('T') RETURN *",
    "CREATE NODE TABLE IF NOT EXISTS N(id INT64, PRIMARY KEY(id))",
    "CREATE REL TABLE IF NOT EXISTS R(FROM T TO T)",
    "DROP TABLE IF EXISTS Absent",
    "ALTER TABLE T ADD IF NOT EXISTS z INT64",
    "CREATE SEQUENCE IF NOT EXISTS sq2",
    "RETURN nextval('sq')",
    "MATCH (t:T) RETURN t.id, nextval('sq')",
    "CREATE (:L)",
    "MATCH (t:T) SET t.x = 5",
    "MATCH (t:T|schema_table) RETURN count(t)",
    "MATCH (t:schema_table) RETURN count(t)",
    "MATCH (t:`schema_table`) RETURN count(t)",
    "MATCH (t:T|:schema_rel) RETURN count(t)",
  ]


def test_a_clause_run_into_a_literal_is_not_a_read(engine, separators):
  """A non-read clause straight after a literal, with and without a gap."""
  gaps = ["", "/**/", *separators]
  literals = [*NUMBERS, "'a'", "[1][0]", "(1)"]
  accepted = 0
  missed = []
  for literal, gap, clause in itertools.product(literals, gaps, _clauses(engine)):
    statement = f"MATCH (t:T) WHERE t.v < {literal}{gap}{clause}"
    if literal == "'a'":
      statement = f"MATCH (t:T) WHERE t.s = {literal}{gap}{clause}"
    if not engine.accepts(statement):
      continue
    accepted += 1
    if not is_non_read(statement):
      missed.append(statement)
  assert accepted > 1000
  assert missed == []


def test_every_number_form_is_accepted_glued(engine):
  """The loop above is not vacuous: the engine runs each glued number form."""
  for number in NUMBERS:
    assert engine.accepts(f"MATCH (t:T) WHERE t.v < {number}SET t.x = 5"), number


def test_a_statement_behind_any_separator_is_not_a_read(engine, separators, leaders):
  """Each word gap of a non-read statement, and its start, in every spelling."""
  accepted = 0
  missed = []

  def check(statement: str) -> None:
    nonlocal accepted
    if not engine.accepts(statement):
      return
    accepted += 1
    if not is_non_read(statement):
      missed.append(statement)

  for statement in _statements():
    check(statement)
    for leader in leaders:
      check(f"{leader}{statement}")
      check(f"RETURN 1;{leader}{statement}")
    for prefix in ("EXPLAIN ", "PROFILE "):
      check(f"{prefix}{statement}")
    gaps = [match.start() for match in re.finditer(" ", statement)]
    for position, separator in itertools.product(gaps[:3], separators):
      check(statement[:position] + separator + statement[position + 1 :])
  assert accepted > 1000
  assert missed == []


@pytest.mark.parametrize(
  "statement",
  [
    "MATCH (t:T) WHERE t.v < 1e9 RETURN t.id",
    "MATCH (t:T) WHERE t.v < 1.5E3 AND t.x >= .5 RETURN t.id ORDER BY t.id LIMIT 10",
    "RETURN 1e5 AS big, 1E-5 AS small, .5e1 AS five",
    "MATCH (t:T) WHERE t.s CONTAINS 'SET x = 1; CHECKPOINT' RETURN t.s",
    "MATCH (t:T) RETURN t.x // CALL threads=1",
    "MATCH (t:T) RETURN᠎t.id",
    "﻿MATCH (t:T) RETURN t.id",
    "UNWIND [1, 2, 3] AS i RETURN i",
    "CALL show_tables() RETURN *",
    "CALL table_info('T') RETURN *",
    "MATCH (t:T) RETURN {schema_table: t.id} AS m",
    "MATCH (t:T) RETURN count(t) AS n",
    "MATCH (t:T) RETURN t.id AS call",
    "WITH 1 AS call RETURN call",
    "MATCH (t:T) RETURN {call: t.id} AS m",
    "MATCH (call:T) RETURN call.id, call.x",
    "MATCH (t:T) WITH t.id AS call WHERE call = 1 RETURN call",
    "MATCH (t:T) WITH t.id AS call MATCH (u:T) RETURN call, u.id",
    "MATCH (t:T) WITH t.id AS call ORDER BY call LIMIT (1) RETURN call",
  ],
)
def test_an_honest_read_stays_a_read(engine, statement):
  assert engine.accepts(statement)
  assert not is_non_read(statement)


@pytest.mark.parametrize(
  "statement",
  [
    "MATCH (t:T) WHERE t.v < 1CALL read_csv_serial('x') RETURN 1",
    "MATCH (t:T) WHERE t.v < 1e5CALL read_parquet('x') RETURN 1",
    "CALL᠎read_csv_serial('x') RETURN *",
    "CALL᠎READ_PARQUET('x') RETURN *",
    "﻿CALL read_csv_parallel('x') RETURN *",
    "RETURN read_npy('x')",
    "CALL `read_csv_serial`('x') RETURN *",
    "CALL gql('MATCH (n) DELETE n')",
  ],
)
def test_a_file_function_is_refused_however_it_is_called(statement):
  """Never sent to the engine: these are refused by name, not by what they do."""
  assert is_non_read(statement)


@pytest.mark.parametrize(
  "statement",
  [
    "MATCH (t:T) WHERE t.id = $call RETURN t.id",
    "MATCH (t:Call) RETURN t.call, t.type, t.graph, t.index, t.begin, t.comment",
    "MATCH (t:T) RETURN t.id AS begin, t.x AS comment ORDER BY begin",
  ],
)
def test_a_name_that_reads_like_a_keyword_is_still_a_read(statement):
  assert not is_non_read(statement)


def test_every_engine_table_function_is_named(engine):
  """A table function is allowed by name or refused by name; none is unknown."""
  functions = {
    str(row[0]).lower()
    for row in engine.rows("CALL show_functions() RETURN *")
    if "TABLE FUNCTION" in str(row[1])
  }
  assert functions
  assert functions - CypherSecurityAnalyzer.ENGINE_TABLE_FUNCTIONS == set()


SEARCH = "CALL query_vector_index('T', 'idx', {vector}, 3) RETURN node.id"


@pytest.mark.parametrize(
  ("vector", "parameters"),
  [
    ("[0.1, -0.2, 3e-1]", None),
    ("$embedding", {"embedding": [0.1, 0.2, 0.3]}),
    ("$embedding", {"embedding": [1, 2, 3]}),
  ],
)
def test_a_vector_search_over_numbers_is_let_through(vector, parameters):
  assert not has_unsafe_vector_query(SEARCH.format(vector=vector), parameters)


@pytest.mark.parametrize(
  ("vector", "parameters"),
  [
    ("NULL", None),
    ("null", None),
    ("$embedding", {"embedding": None}),
    ("$embedding", {}),
    ("$embedding", None),
    ("$embedding", {"embedding": []}),
    ("$embedding", {"embedding": [0.1, None]}),
    ("$embedding", {"embedding": "0.1"}),
    ("CAST(NULL AS FLOAT[3])", None),
    ("CASE WHEN false THEN [0.1] END", None),
    ("[0.1, NULL]", None),
    ("[]", None),
    ("t.embedding", None),
    ("efs := 100, NULL", None),
  ],
)
def test_a_vector_search_over_anything_else_is_refused(vector, parameters):
  """Never sent to the engine: it does not survive a null query vector."""
  assert has_unsafe_vector_query(SEARCH.format(vector=vector), parameters)
  spaced = SEARCH.format(vector=vector).replace(
    "query_vector_index(", "QUERY_VECTOR_INDEX\u180e ("
  )
  assert has_unsafe_vector_query(spaced, parameters)


def test_a_statement_with_no_vector_search_is_not_looked_at():
  assert not has_unsafe_vector_query("MATCH (t:T) RETURN t.query_vector_index", None)
