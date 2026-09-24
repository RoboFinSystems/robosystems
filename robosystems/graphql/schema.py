"""Strawberry schema served at `/extensions/{graph_id}/graphql`.

The `Query` root composes only the domain resolver classes whose feature
flags are on. See graphql/README.md.
"""

from __future__ import annotations

import inspect
import re
import textwrap

import strawberry
from strawberry.extensions import (
  MaxAliasesLimiter,
  MaxTokensLimiter,
  QueryDepthLimiter,
)
from strawberry.extensions.tracing.opentelemetry import OpenTelemetryExtensionSync
from strawberry.types import Info

from robosystems.config import env
from robosystems.graphql.context import GraphQLContext, require_user
from robosystems.graphql.execution import MaskUnexpectedErrors, OffloadSyncResolvers
from robosystems.graphql.resolvers._common import (
  _MAX_LIMIT,
  _MIN_LIMIT,
  _MIN_OFFSET,
)
from robosystems.graphql.resolvers.information_block import InformationBlockQuery
from robosystems.graphql.resolvers.investor import InvestorQuery
from robosystems.graphql.resolvers.ledger import LedgerQuery
from robosystems.graphql.resolvers.library import LibraryQuery
from robosystems.graphql.resolvers.taxonomy_block import TaxonomyBlockQuery


@strawberry.type
class _BaseQuery:
  """Always-on probe field shared by every Query composition.

  Lives on its own type so the dynamically-built `Query` class can inherit
  it alongside whichever domain mixins are enabled. Mounted in all
  environments, not just dev.
  """

  @strawberry.field
  def hello(self, info: Info[GraphQLContext, None]) -> str:
    """Auth probe: returns `hello, {user.email}` for a valid request.

    Opens no database session and touches no domain, so it gives clients a
    three-way liveness check:

    - HTTP 200 with `data.hello` → endpoint up, credentials valid
    - HTTP 200 with an `UNAUTHENTICATED` error → endpoint up, no
      credentials presented (anonymous introspection is still allowed)
    - HTTP 401 → endpoint up, credentials presented but invalid
    """
    user = require_user(info)
    return f"hello, {user.email}"


# One description for every `limit`/`offset` argument (all validated by the
# same guard in resolvers/_common.py); a docstring `Args:` entry overrides it.
# Bounds are interpolated so the public text tracks `_MAX_LIMIT`.
COMMON_ARGUMENT_DESCRIPTIONS = {
  "limit": (
    f"Maximum rows to return, {_MIN_LIMIT}-{_MAX_LIMIT}. Omit to use this "
    "field's own default; out of range raises `INVALID_PAGINATION`."
  ),
  "offset": (
    f"Rows to skip before returning, {_MIN_OFFSET} or greater. Out of range "
    "raises `INVALID_PAGINATION`."
  ),
}

_SECTION_HEADING = re.compile(
  r"^(Args|Arguments|Returns|Raises|Yields|Note|Example)s?:\s*$"
)
_ARG_ENTRY = re.compile(r"^(\w+)\s*:\s*(.*)$")


def _split_docstring(doc: str) -> tuple[str, dict[str, str]]:
  """Split a docstring into prose and its Google-style `Args:` entries."""
  lines = doc.splitlines()
  prose: list[str] = []
  args: dict[str, str] = {}
  current: str | None = None
  in_args = False

  for line in lines:
    heading = _SECTION_HEADING.match(line.strip())
    if heading:
      in_args = heading.group(1) in ("Args", "Arguments")
      current = None
      if not in_args:
        prose.append(line)
      continue
    if not in_args:
      prose.append(line)
      continue
    if not line.strip():
      current = None
      continue
    entry = _ARG_ENTRY.match(line.strip())
    if entry:
      current = entry.group(1)
      args[current] = entry.group(2).strip()
    elif current:
      args[current] = f"{args[current]} {line.strip()}".strip()

  return textwrap.dedent("\n".join(prose)).strip(), args


def _describe_from_docstrings(cls: type) -> type:
  """Publish each resolver's docstring as its GraphQL field description.

  Strawberry ignores `__doc__`; an explicit `description=` still wins.
  **A resolver docstring is therefore public API copy** — implementation
  notes belong in comments inside the function.
  """
  for field in cls.__strawberry_definition__.fields:
    resolver = field.base_resolver
    func = getattr(resolver, "wrapped_func", None) if resolver is not None else None
    doc = inspect.getdoc(func) if func is not None else None
    prose, arg_docs = _split_docstring(doc) if doc else ("", {})

    if prose and not field.description:
      field.description = prose

    for argument in field.arguments:
      if argument.description:
        continue
      text = arg_docs.get(argument.python_name) or COMMON_ARGUMENT_DESCRIPTIONS.get(
        argument.python_name
      )
      if text:
        argument.description = text
  return cls


def _build_query_type() -> type:
  """Build the Query root from whichever domain mixins are enabled.

  A disabled domain is dropped rather than exposed as fields that fail at
  runtime, so clients can branch on the schema shape. The cross-domain
  mixins are always composed; their visibility follows the session
  `search_path`.
  """
  bases: tuple[type, ...] = (
    InformationBlockQuery,
    TaxonomyBlockQuery,
    LibraryQuery,
    _BaseQuery,
  )
  if env.ROBOLEDGER_ENABLED:
    bases = (LedgerQuery, *bases)
  if env.ROBOINVESTOR_ENABLED:
    bases = (InvestorQuery, *bases)
  return _describe_from_docstrings(strawberry.type(type("Query", bases, {})))


Query = _build_query_type()

# Limiters are factories: Strawberry constructs extensions per request.
# The Sync OTel variant is required because the async one breaks
# `execute_sync`. `OffloadSyncResolvers` must come after it: the last extension
# is outermost, and the resolver span has to open inside the worker thread.
schema = strawberry.Schema(
  query=Query,
  extensions=[
    lambda: QueryDepthLimiter(max_depth=env.EXTENSIONS_GRAPHQL_MAX_DEPTH),
    lambda: MaxAliasesLimiter(max_alias_count=env.EXTENSIONS_GRAPHQL_MAX_ALIASES),
    lambda: MaxTokensLimiter(max_token_count=env.EXTENSIONS_GRAPHQL_MAX_TOKENS),
    OpenTelemetryExtensionSync,
    OffloadSyncResolvers,
    MaskUnexpectedErrors,
  ],
)


_SNAKE_REFERENCE = re.compile(r"`([a-z][a-z0-9]*(?:_[a-z0-9]+)+)`")


def _camel(name: str) -> str:
  head, *rest = name.split("_")
  return head + "".join(word[:1].upper() + word[1:] for word in rest)


def _camelize_field_references(built: strawberry.Schema) -> strawberry.Schema:
  """Rewrite `snake_case` field references in descriptions to the wire name.

  The descriptions come from Pydantic prose shared with REST, where the
  snake_case name is correct. A token is rewritten only when its camelCase
  form is a field or argument of the same type and the snake form is not.
  """
  for gql_type in built._schema.type_map.values():
    if gql_type.name.startswith("__"):
      continue
    fields = getattr(gql_type, "fields", None)
    if not isinstance(fields, dict):
      continue

    def rewrite(text: str | None, names: set[str]) -> str | None:
      if not text:
        return text
      return _SNAKE_REFERENCE.sub(
        lambda m: (
          f"`{_camel(m.group(1))}`"
          if _camel(m.group(1)) in names and m.group(1) not in names
          else m.group(0)
        ),
        text,
      )

    field_names = set(fields)
    gql_type.description = rewrite(gql_type.description, field_names)
    for field in fields.values():
      args = getattr(field, "args", None) or {}
      field.description = rewrite(field.description, field_names | set(args))
      for argument in args.values():
        argument.description = rewrite(argument.description, set(args))
  return built


schema = _camelize_field_references(schema)
