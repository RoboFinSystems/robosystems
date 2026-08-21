"""Cypher security analyzer tests."""

import pytest

from robosystems.security.cypher_analyzer import (
  CypherOperationType,
  CypherSecurityAnalyzer,
  analyze_cypher_query,
  has_system_calls,
  is_admin_operation,
  is_bulk_operation,
  is_schema_ddl,
  is_write_operation,
)


@pytest.fixture
def analyzer():
  return CypherSecurityAnalyzer()


class TestAnalyzeQuery:
  """Tests for CypherSecurityAnalyzer.analyze_query."""

  def test_read_only_query(self, analyzer):
    result = analyzer.analyze_query("MATCH (n:Entity) RETURN n LIMIT 10")
    assert result == CypherOperationType.READ

  def test_write_query_create(self, analyzer):
    result = analyzer.analyze_query("CREATE (n:Entity {name: 'test'})")
    assert result in (CypherOperationType.WRITE, CypherOperationType.MIXED)

  def test_write_query_merge(self, analyzer):
    result = analyzer.analyze_query("MERGE (n:Entity {name: 'test'})")
    assert result in (CypherOperationType.WRITE, CypherOperationType.MIXED)

  def test_write_query_delete(self, analyzer):
    result = analyzer.analyze_query("MATCH (n) DELETE n")
    assert result in (CypherOperationType.WRITE, CypherOperationType.MIXED)

  def test_write_query_set(self, analyzer):
    result = analyzer.analyze_query("MATCH (n:Entity) SET n.name = 'updated'")
    assert result == CypherOperationType.MIXED

  def test_mixed_query(self, analyzer):
    result = analyzer.analyze_query(
      "MATCH (n:Entity) WHERE n.name = 'test' SET n.active = true RETURN n"
    )
    assert result == CypherOperationType.MIXED

  def test_empty_query_raises(self, analyzer):
    with pytest.raises(ValueError, match="non-empty string"):
      analyzer.analyze_query("")

  def test_none_query_raises(self, analyzer):
    with pytest.raises(ValueError, match="non-empty string"):
      analyzer.analyze_query(None)

  def test_detach_delete(self, analyzer):
    result = analyzer.analyze_query("MATCH (n) DETACH DELETE n")
    assert result in (CypherOperationType.WRITE, CypherOperationType.MIXED)


class TestIsWriteOperation:
  """Tests for is_write_operation convenience function."""

  def test_read_query(self):
    assert is_write_operation("MATCH (n) RETURN n") is False

  def test_write_query(self):
    assert is_write_operation("CREATE (n:Test)") is True

  def test_invalid_query_defaults_to_write(self):
    # Empty string should default to True for security
    assert is_write_operation("") is True


class TestIsSchemaDD:
  """Tests for schema DDL detection."""

  def test_create_node_table(self, analyzer):
    assert analyzer.is_schema_ddl(
      "CREATE NODE TABLE Person(name STRING, PRIMARY KEY(name))"
    )

  def test_create_rel_table(self, analyzer):
    assert analyzer.is_schema_ddl("CREATE REL TABLE KNOWS(FROM Person TO Person)")

  def test_drop_node_table(self, analyzer):
    assert analyzer.is_schema_ddl("DROP NODE TABLE Person")

  def test_alter_table(self, analyzer):
    assert analyzer.is_schema_ddl("ALTER TABLE Person ADD COLUMN age INT64")

  def test_rename_table(self, analyzer):
    assert analyzer.is_schema_ddl("RENAME TABLE Person TO Employee")

  def test_non_ddl_query(self, analyzer):
    assert analyzer.is_schema_ddl("MATCH (n:Person) RETURN n") is False

  def test_comment_on_is_catalog_ddl(self, analyzer):
    """`COMMENT ON` rewrites what every reader sees in `show_tables()`; the
    write keyword set cannot carry a bare `comment` (too common a property),
    so the two-word form is refused as DDL — on the read path too."""
    assert analyzer.is_schema_ddl("COMMENT ON TABLE Entity IS 'tenant note'")
    assert analyzer.is_schema_ddl("comment on table Entity is 'x'")
    assert analyzer.is_write_operation("COMMENT ON TABLE Entity IS 'x'") is False
    assert analyzer.is_schema_ddl("MATCH (n) RETURN n.comment") is False

  def test_module_level_function(self):
    assert is_schema_ddl("CREATE NODE TABLE Test(id STRING, PRIMARY KEY(id))")


class TestIsBulkOperation:
  """Tests for bulk operation detection."""

  def test_copy_operation(self, analyzer):
    assert analyzer.is_bulk_operation("COPY Person FROM 'file.csv'")

  def test_load_operation(self, analyzer):
    assert analyzer.is_bulk_operation("LOAD FROM 'data.parquet'")

  def test_import_operation(self, analyzer):
    assert analyzer.is_bulk_operation("IMPORT DATABASE 'backup.db'")

  def test_non_bulk_query(self, analyzer):
    assert analyzer.is_bulk_operation("MATCH (n) RETURN n") is False

  def test_module_level_function(self):
    assert is_bulk_operation("COPY Entity FROM 'file.csv'")


class TestIsAdminOperation:
  """Tests for admin operation detection."""

  def test_export_operation(self, analyzer):
    assert analyzer.is_admin_operation("EXPORT DATABASE 'backup.db'")

  def test_attach_operation(self, analyzer):
    assert analyzer.is_admin_operation("ATTACH 'other.db' AS other")

  def test_use_operation(self, analyzer):
    assert analyzer.is_admin_operation("USE other_db")

  def test_non_admin_query(self, analyzer):
    assert analyzer.is_admin_operation("MATCH (n) RETURN n") is False

  @pytest.mark.parametrize(
    "statement",
    [
      "BEGIN TRANSACTION",
      "begin transaction read only",
      "COMMIT",
      "ROLLBACK",
      "CHECKPOINT",
      "  BEGIN TRANSACTION",
      "MATCH (n) RETURN count(n); COMMIT",
    ],
  )
  def test_transaction_control_is_refused_as_admin(self, analyzer, statement):
    """A manual transaction outlives the request on a pooled engine
    connection and the next borrower inherits it; CHECKPOINT is engine
    maintenance. All are refused on every surface, like ATTACH."""
    assert analyzer.is_admin_operation(statement)

  @pytest.mark.parametrize(
    "statement",
    [
      "MATCH (n) RETURN n.begin, n.commit, n.rollback",
      "MATCH (p:Period) WHERE p.checkpoint > 1 RETURN p",
      "// begin\nMATCH (n) RETURN n",
      "MATCH (n) WHERE n.name = 'COMMIT' RETURN n",
    ],
  )
  def test_transaction_words_inside_a_read_are_not_admin(self, analyzer, statement):
    assert analyzer.is_admin_operation(statement) is False

  def test_module_level_function(self):
    assert is_admin_operation("EXPORT DATABASE 'test'")


class TestHasSystemCalls:
  """Tests for system call detection."""

  def test_show_tables(self, analyzer):
    assert analyzer.has_system_calls("CALL show_tables() RETURN *")

  def test_table_info(self, analyzer):
    assert analyzer.has_system_calls("CALL table_info('Entity') RETURN *")

  def test_current_setting(self, analyzer):
    assert analyzer.has_system_calls("CALL current_setting('threads') RETURN *")

  def test_db_version(self, analyzer):
    assert analyzer.has_system_calls("CALL db_version() RETURN *")

  def test_non_system_call(self, analyzer):
    assert analyzer.has_system_calls("MATCH (n) RETURN n") is False

  def test_module_level_function(self):
    assert has_system_calls("CALL show_tables() RETURN *")


class TestCallClassification:
  """`CALL` is classified explicitly, on an allowlist, failing closed.

  The keyword patterns match on `\\b` word boundaries and `_` is a word
  character, so an underscore-joined procedure name can never match them.
  `CALL` itself appears in no keyword set. Both gaps meant the whole `CALL`
  family classified as READ, on the surface that treats "not a write" as
  "safe to run on a read replica".
  """

  @pytest.mark.parametrize(
    "query",
    [
      "CALL CREATE_VECTOR_INDEX('Fact','x','embedding')",
      "CALL DROP_VECTOR_INDEX('Fact','x')",
      "CALL CREATE_FTS_INDEX('Fact','idx','text')",
      "call create_vector_index(1)",
      "CALL db.labels()",
      "CALL some_future_procedure()",
    ],
  )
  def test_unlisted_procedures_are_writes(self, analyzer, query):
    """Fail closed: anything not on the read-only allowlist is a write,
    including procedures that do not exist yet."""
    assert analyzer.is_write_operation(query) is True

  @pytest.mark.parametrize(
    "query",
    [
      "CALL spill_to_disk=false",
      "CALL timeout=0",
      "CALL   TIMEOUT   =   0",
    ],
  )
  def test_session_configuration_is_a_write(self, analyzer, query):
    """`CALL <name> = <value>` mutates connection state that outlives the
    statement, and connections are pooled and shared."""
    assert analyzer.is_write_operation(query) is True

  def test_trailing_configuration_after_a_read(self, analyzer):
    """A read prefix must not launder a trailing configuration verb."""
    assert analyzer.is_write_operation("MATCH (n) RETURN n; CALL timeout=0") is True

  @pytest.mark.parametrize(
    "query",
    [
      "CALL SHOW_TABLES() RETURN *",
      "CALL TABLE_INFO('Fact') RETURN *",
      "CALL db_version() RETURN *",
      "CALL current_setting('threads') RETURN *",
    ],
  )
  def test_allowlisted_read_procedures_stay_reads(self, analyzer, query):
    """These are used by first-party schema introspection — regressing them
    would break the schema endpoints and the MCP client."""
    assert analyzer.is_write_operation(query) is False

  def test_show_functions_is_a_read(self, analyzer):
    """The MCP query validator's own remediation hint tells users to run
    `CALL show_functions() RETURN *` in place of `SHOW FUNCTIONS` — the
    sanctioned replacement must not be refused as a write."""
    assert analyzer.is_write_operation("CALL show_functions() RETURN *") is False

  def test_configuration_inside_a_string_literal_is_not_a_write(self, analyzer):
    """String contents are masked before classification, so a literal that
    merely looks like a config verb must not trip the gate."""
    query = "MATCH (n:Fact) WHERE n.name = 'CALL timeout=0' RETURN n"
    assert analyzer.is_write_operation(query) is False


class TestIsNonReadCall:
  """`is_non_read_call` exposes the CALL classification to validators that
  gate on operation *family* (bulk / admin / DDL) rather than on
  `is_write_operation` — graph_api's ad-hoc query validator must allow
  ordinary graph writes (writer instances execute them) while still
  refusing the CALL surface."""

  @pytest.mark.parametrize(
    "query",
    [
      "CALL CREATE_VECTOR_INDEX('Fact','x','embedding')",
      "CALL DROP_VECTOR_INDEX('Fact','x')",
      "CALL some_future_procedure()",
      "CALL spill_to_disk=false",
      "MATCH (n) RETURN n; CALL timeout=0",
    ],
  )
  def test_non_read_call_forms_are_flagged(self, analyzer, query):
    assert analyzer.is_non_read_call(query) is True

  @pytest.mark.parametrize(
    "query",
    [
      "CALL SHOW_TABLES() RETURN *",
      "CALL show_functions() RETURN *",
      "CALL table_info('Fact') RETURN *",
      "MATCH (n) RETURN n",
      "CREATE (n:Foo {id: 1}) RETURN n",
      "MATCH (n:Fact) WHERE n.name = 'CALL timeout=0' RETURN n",
    ],
  )
  def test_reads_writes_and_allowlisted_calls_are_not_flagged(self, analyzer, query):
    """Ordinary reads AND ordinary writes pass — the predicate is about the
    CALL family only, so family-gating validators can compose it without
    breaking writer instances."""
    assert analyzer.is_non_read_call(query) is False

  def test_module_level_wrapper(self):
    from robosystems.security.cypher_analyzer import is_non_read_call

    assert is_non_read_call("CALL CREATE_VECTOR_INDEX('a','b','c')") is True
    assert is_non_read_call("MATCH (n) RETURN n") is False


class TestHasOpaqueStatementCall:
  """`has_opaque_statement_call` names the procedures whose payload is a
  statement in a string. The analyzer masks string literals by design, so
  the family gates cannot see what such a call would run — the predicate
  exists so the kernel can refuse the shape outright, on every graph and
  regardless of role."""

  @pytest.mark.parametrize(
    "query",
    [
      "CALL GQL('MATCH (n) RETURN n')",
      "CALL gql('INSERT (:X {id: 1})')",
      "call Gql('CREATE NODE TABLE Evil(id INT64, PRIMARY KEY(id))') RETURN *",
      "MATCH (n) RETURN n; CALL GQL('COPY Evil FROM \"/etc/passwd\"')",
    ],
  )
  def test_opaque_calls_are_flagged(self, analyzer, query):
    assert analyzer.has_opaque_statement_call(query) is True

  @pytest.mark.parametrize(
    "query",
    [
      "MATCH (n) RETURN n",
      "CALL show_tables() RETURN *",
      "CALL CREATE_VECTOR_INDEX('Fact','x','embedding')",
      "MATCH (n:Fact) WHERE n.name = 'CALL GQL(x)' RETURN n",
      "MATCH (n:Fact) WHERE n.name = 'gql' RETURN n",
      "MATCH (n:Gql) RETURN n",
    ],
  )
  def test_other_forms_are_not_flagged(self, analyzer, query):
    """Ordinary reads/writes, allowlisted and non-allowlisted CALLs, and the
    procedure name appearing only as data or as a label are all clean —
    the predicate is about the CALL target, nothing else."""
    assert analyzer.has_opaque_statement_call(query) is False

  def test_family_gates_are_blind_to_the_payload(self, analyzer):
    """The reason the predicate exists: DDL, bulk and admin verbs inside the
    string are invisible to every family gate, and the write gate alone does
    not stop a caller who legitimately holds write access."""
    payloads = [
      "CALL GQL('CREATE NODE TABLE Evil(id INT64, PRIMARY KEY(id))')",
      "CALL GQL('COPY Evil FROM \"/etc/passwd\"')",
      "CALL GQL('INSTALL httpfs')",
      "CALL GQL('ATTACH \"s3://x\" AS y')",
    ]
    for q in payloads:
      assert analyzer.is_schema_ddl(q) is False
      assert analyzer.is_bulk_operation(q) is False
      assert analyzer.is_admin_operation(q) is False
      assert analyzer.has_opaque_statement_call(q) is True

  @pytest.mark.parametrize(
    "query",
    [
      "CALL `GQL`('CREATE NODE TABLE Evil(id INT64, PRIMARY KEY(id))')",
      "CALL `gql`('INSTALL httpfs')",
      "call `Gql` ('COPY Evil FROM \"/etc/passwd\"') RETURN *",
      # Any quoted target, not just the known opaque names: the analyzer
      # cannot see through the mask, so it must not vouch for the name.
      "CALL `show_tables`() RETURN *",
      "MATCH (n) RETURN n; CALL `whatever`('x')",
    ],
  )
  def test_quoted_call_targets_are_refused(self, analyzer, query):
    """The engine resolves a backtick-quoted procedure name to the same
    procedure as the bare name, but the classifier masks quoted identifiers
    before it looks at the CALL target — so a name-based deny would be
    evadable by quoting. Refuse the shape outright."""
    assert analyzer.has_opaque_statement_call(query) is True

  def test_quoted_identifiers_elsewhere_are_not_call_targets(self, analyzer):
    """Quoting is only a problem *as the CALL target*. Backtick identifiers
    in patterns, property access, and aliases stay ordinary."""
    for q in [
      "MATCH (n:`Fact`) RETURN n.`value` AS `v`",
      "CALL show_tables() RETURN `name`",
      "MATCH (`n`) WHERE `n`.x = 1 RETURN `n`",
    ]:
      assert analyzer.has_opaque_statement_call(q) is False

  def test_module_level_wrapper(self):
    from robosystems.security.cypher_analyzer import has_opaque_statement_call

    assert has_opaque_statement_call("CALL GQL('MATCH (n) RETURN n')") is True
    assert has_opaque_statement_call("CALL `GQL`('MATCH (n) RETURN n')") is True
    assert has_opaque_statement_call("MATCH (n) RETURN n") is False


class TestQuerySecurityValidation:
  """Tests for query security validation."""

  def test_excessively_long_query(self, analyzer):
    long_query = "MATCH (n) RETURN n " + " " * 200000
    with pytest.raises(ValueError, match="maximum allowed length"):
      analyzer.analyze_query(long_query)

  def test_unbalanced_comments(self, analyzer):
    with pytest.raises(ValueError, match="Unbalanced comment"):
      analyzer.analyze_query("/* unclosed comment MATCH (n) RETURN n")

  def test_injection_create_user(self, analyzer):
    with pytest.raises(ValueError, match="dangerous"):
      analyzer.analyze_query("MATCH (n) RETURN n; CREATE USER admin")

  def test_injection_drop_database(self, analyzer):
    with pytest.raises(ValueError, match="dangerous"):
      analyzer.analyze_query("MATCH (n) RETURN n; DROP DATABASE test")


class TestCleanQuery:
  """Tests for query cleaning (comment/string removal)."""

  def test_removes_line_comments(self, analyzer):
    cleaned = analyzer._clean_query("MATCH (n) // this is a comment\nRETURN n")
    assert "//" not in cleaned

  def test_removes_block_comments(self, analyzer):
    cleaned = analyzer._clean_query("MATCH (n) /* comment */ RETURN n")
    assert "/*" not in cleaned
    assert "*/" not in cleaned

  def test_removes_string_literals(self, analyzer):
    cleaned = analyzer._clean_query("MATCH (n) WHERE n.name = 'CREATE' RETURN n")
    assert "STRING_LITERAL" in cleaned

  def test_write_in_string_not_detected(self, analyzer):
    # CREATE inside a string should NOT be detected as a write operation
    result = analyzer.analyze_query(
      "MATCH (n) WHERE n.name = 'CREATE something' RETURN n"
    )
    assert result == CypherOperationType.READ

  def test_write_in_comment_not_detected(self, analyzer):
    result = analyzer.analyze_query("MATCH (n) /* CREATE (m:Node) */ RETURN n")
    assert result == CypherOperationType.READ


class TestBacktickIdentifierBypass:
  """Regression tests for the backtick-identifier lexer-differential bypass.

  ``_clean_query`` used to treat backslash as an escape inside backtick-quoted
  identifiers, so ``... AS `x\\` SET ...`` masked the trailing write while Kuzu
  closed the identifier at the backtick and executed the write. The analyzer
  now closes a backtick identifier at the FIRST backtick (backslash is a
  literal char), matching the engine lexer, so a masked write can no longer
  slip past classification. Each masked payload below must trip its predicate.
  """

  MASKED_SET = "MATCH (e) WITH e, 1 AS `y\\` SET e.name = 'x' RETURN e"
  MASKED_CREATE = "MATCH (n) WITH n AS `x\\` LIMIT 1 CREATE (m:Node {v:1}) RETURN m"
  MASKED_DETACH_DELETE = "MATCH (n) WITH n AS `z\\` DETACH DELETE n"
  MASKED_ATTACH = "MATCH (n) WITH n AS `a\\` ATTACH 'other.lbug'"
  MASKED_LOAD = "MATCH (n) WITH n AS `b\\` LOAD FROM 'f.csv' RETURN 1"
  MASKED_CREATE_TABLE = (
    "MATCH (n) WITH n AS `c\\` CREATE NODE TABLE T(id INT64, PRIMARY KEY(id))"
  )

  def test_clean_query_closes_backtick_at_first_backtick(self, analyzer):
    # The write keyword after the closed identifier must survive cleaning.
    cleaned = analyzer._clean_query("MATCH (n) WITH n AS `x\\` CREATE (m:Node)")
    assert "CREATE" in cleaned.upper()

  def test_masked_set_is_write(self, analyzer):
    assert is_write_operation(self.MASKED_SET) is True
    assert analyzer.analyze_query(self.MASKED_SET) in (
      CypherOperationType.WRITE,
      CypherOperationType.MIXED,
    )

  def test_masked_create_is_write(self):
    assert is_write_operation(self.MASKED_CREATE) is True

  def test_masked_detach_delete_is_write(self):
    assert is_write_operation(self.MASKED_DETACH_DELETE) is True

  def test_masked_attach_is_admin(self):
    assert is_admin_operation(self.MASKED_ATTACH) is True

  def test_masked_load_is_bulk(self):
    assert is_bulk_operation(self.MASKED_LOAD) is True

  def test_masked_create_table_is_schema_ddl(self):
    assert is_schema_ddl(self.MASKED_CREATE_TABLE) is True

  def test_legitimate_backtick_identifier_still_read(self, analyzer):
    # A property literally named `CREATE` is an identifier, not a write — the
    # conservative fix must not over-block ordinary backtick identifiers.
    assert (
      analyzer.analyze_query("MATCH (n) RETURN n.`CREATE`") == CypherOperationType.READ
    )


class TestGetWriteOperationDetails:
  """Tests for get_write_operation_details."""

  def test_read_query_details(self, analyzer):
    details = analyzer.get_write_operation_details("MATCH (n:Entity) RETURN n")
    assert details["operation_type"] == "read"
    assert details["is_write_operation"] is False
    assert details["analysis_successful"] is True

  def test_write_query_details(self, analyzer):
    details = analyzer.get_write_operation_details("CREATE (n:Entity {name: 'test'})")
    assert details["is_write_operation"] is True
    assert "CREATE" in details["write_keywords_found"]

  def test_invalid_query_details(self, analyzer):
    details = analyzer.get_write_operation_details("")
    assert details["analysis_successful"] is False
    assert details["is_write_operation"] is True  # Safe default

  def test_module_level_function(self):
    details = analyze_cypher_query("MATCH (n) RETURN n")
    assert details["operation_type"] == "read"


class TestKeywordContextValidation:
  """Tests for _validate_keyword_context."""

  def test_keyword_not_part_of_identifier(self, analyzer):
    # "SET" should not match inside "DATASET"
    result = analyzer.analyze_query("MATCH (n:DATASET) RETURN n")
    assert result == CypherOperationType.READ

  def test_keyword_not_part_of_prefix(self, analyzer):
    # "CREATE" should not match inside "RECREATE"
    result = analyzer.analyze_query("MATCH (n) WHERE n.name = 'RECREATED' RETURN n")
    assert result == CypherOperationType.READ


class TestInStringCommentMarkerBypass:
  """Regression: a comment marker (// or /*) inside a string literal must not
  hide a real write keyword that follows the closed string.

  The old `_clean_query` stripped comments *before* strings, so an in-string
  ``//`` ate everything to end-of-line — including a trailing CREATE/SET/etc —
  and the analyzer reported READ. The single-pass scanner masks strings first.
  """

  # Appendix B payloads — each must classify as a WRITE.
  @pytest.mark.parametrize(
    "query",
    [
      "MATCH (n) WHERE n.x = '//' CREATE (m:Hacked) RETURN m",
      'MATCH (n) WHERE n.name = "x//y" SET n.hacked = true RETURN n',
      'MATCH (n) WHERE n.x = "//" DETACH DELETE n',
      "MATCH (n) WHERE n.note = '// comment' MERGE (m:Evil {id:1}) RETURN m",
      # unbalanced /* inside a string -> fail-closed WRITE (pre-check rejects)
      "MATCH (n) WHERE n.x = '/*' RETURN n",
    ],
  )
  def test_in_string_marker_does_not_hide_write(self, analyzer, query):
    assert analyzer.is_write_operation(query) is True

  def test_in_string_marker_hides_nothing_when_read(self, analyzer):
    # An in-string // with no trailing write keyword stays a READ.
    result = analyzer.analyze_query("MATCH (n) WHERE n.url = 'http://x' RETURN n")
    assert result == CypherOperationType.READ

  def test_real_line_comment_still_masks_write(self, analyzer):
    # A genuine trailing line comment (outside any string) is still a comment.
    result = analyzer.analyze_query("MATCH (n) RETURN n // CREATE (m) here")
    assert result == CypherOperationType.READ

  def test_real_block_comment_still_masks_write(self, analyzer):
    result = analyzer.analyze_query("MATCH (n) RETURN n /* CREATE (m) */")
    assert result == CypherOperationType.READ

  def test_in_string_marker_does_not_hide_admin_verb(self, analyzer):
    # The same ordering flaw weakened is_admin/is_bulk/is_schema_ddl.
    assert analyzer.is_admin_operation("MATCH (n) WHERE n.x = '//' ATTACH 'db'") is True
    assert (
      analyzer.is_bulk_operation("MATCH (n) WHERE n.x = '//' LOAD FROM 'f'") is True
    )
