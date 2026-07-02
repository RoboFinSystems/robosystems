"""Query-bounding limiter tests for the extensions GraphQL schema.

Guards against GraphQL DoS on the small extensions OLTP pool: deeply nested
queries and alias fan-out (each resolved field can open a session). Verifies
both the limiter mechanism at our configured values and that the *real* schema
has the limits wired in.
"""

from __future__ import annotations

import strawberry
from strawberry.extensions import MaxAliasesLimiter, QueryDepthLimiter

from robosystems.config import env
from robosystems.graphql.schema import schema as real_schema


@strawberry.type
class _Node:
  value: int = 1

  @strawberry.field
  def child(self) -> _Node:
    return _Node()


def _nested(levels: int) -> str:
  inner = "value"
  for _ in range(levels):
    inner = f"child {{ {inner} }}"
  return f"{{ {inner} }}"


class TestDepthLimiter:
  def test_rejects_query_deeper_than_max(self) -> None:
    max_depth = env.EXTENSIONS_GRAPHQL_MAX_DEPTH
    s = strawberry.Schema(
      query=_Node, extensions=[lambda: QueryDepthLimiter(max_depth=max_depth)]
    )
    result = s.execute_sync(_nested(max_depth + 5))
    assert result.errors, "an over-deep query must be rejected"
    assert any("depth" in (e.message or "").lower() for e in result.errors)

  def test_allows_query_within_max(self) -> None:
    max_depth = env.EXTENSIONS_GRAPHQL_MAX_DEPTH
    s = strawberry.Schema(
      query=_Node, extensions=[lambda: QueryDepthLimiter(max_depth=max_depth)]
    )
    result = s.execute_sync(_nested(max(1, max_depth - 5)))
    assert not result.errors, f"an in-bounds query must pass: {result.errors}"


class TestAliasLimiter:
  def test_rejects_alias_fan_out(self) -> None:
    max_aliases = env.EXTENSIONS_GRAPHQL_MAX_ALIASES
    s = strawberry.Schema(
      query=_Node,
      extensions=[lambda: MaxAliasesLimiter(max_alias_count=max_aliases)],
    )
    aliases = " ".join(f"a{i}: value" for i in range(max_aliases + 5))
    result = s.execute_sync(f"{{ {aliases} }}")
    assert result.errors, "alias fan-out beyond the cap must be rejected"


class TestRealSchemaWiring:
  """The limiters must actually be attached to the served schema."""

  def test_real_schema_configures_four_extensions(self) -> None:
    # 3 query-bounding limiters + the OpenTelemetry tracing extension.
    assert len(real_schema.extensions) == 4

  def test_real_schema_rejects_overdeep_query(self) -> None:
    """Depth validation runs before resolvers, so no auth context is needed.

    Uses the recursive AccountTreeNode.children path to build a query past the
    configured maximum; a shallow real query would instead fail later (auth),
    so we assert specifically on the depth error.
    """
    inner = "name"
    for _ in range(env.EXTENSIONS_GRAPHQL_MAX_DEPTH + 12):
      inner = f"children {{ {inner} }}"
    query = f"{{ accountTree {{ roots {{ {inner} }} }} }}"
    result = real_schema.execute_sync(query)
    assert result.errors
    assert any("depth" in (e.message or "").lower() for e in result.errors)
