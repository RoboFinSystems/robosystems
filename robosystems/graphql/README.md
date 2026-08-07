# Extensions GraphQL

Strawberry GraphQL schema served at `POST /extensions/{graph_id}/graphql`. This is the **read** surface for the extensions subsystem; writes go through the named command operations at `POST /extensions/{roboledger,roboinvestor}/{graph_id}/operations/*`.

## `graph_id` comes from the URL

```
POST /extensions/{graph_id}/graphql
```

The endpoint is **graph-scoped at the URL level**. `graph_id` is a path parameter, **never a query argument**. Resolvers read it from `info.context["graph_id"]` via `require_graph_id(info)`. There is deliberately no `graphId` field argument, so a client cannot pass one that disagrees with the URL.

`get_context` in `context.py` runs **before any resolver** and:

1. Reads the user from the `X-API-Key` header or an `Authorization: Bearer` JWT.
2. Reads `graph_id` from the URL path.
3. Calls `check_graph_access(user, graph_id)` eagerly — a failure raises HTTP 403 and the request never reaches a resolver.

Anonymous introspection and the `hello` probe are allowed on purpose, so SDK codegen and health checks work without credentials. Data resolvers still call `require_user`, so a request that slips through fails cleanly with `UNAUTHENTICATED`.

In development the endpoint renders the GraphiQL playground in a browser. In staging and production the playground UI is not mounted (`graphql_ide=None` unless `env.is_development()`); introspection over the API itself stays enabled.

## Layout

| Path | Contents |
| ---- | -------- |
| `schema.py` | Query root, composed per enabled domain flags, plus the schema-level cost limiters |
| `context.py` | `GraphQLContext`, `get_context`, `require_user`, `require_graph_id` |
| `auth.py` | `check_graph_access` — called by `get_context` before resolvers run |
| `resolvers/_common.py` | Pagination guards, extension gate, session openers |
| `resolvers/information_block.py` | `InformationBlockQuery` — always on |
| `resolvers/taxonomy_block.py` | `TaxonomyBlockQuery` — always on |
| `resolvers/library.py` | `LibraryQuery` — always on |
| `resolvers/ledger.py` | `LedgerQuery` — gated on `ROBOLEDGER_ENABLED` |
| `resolvers/investor.py` | `InvestorQuery` — gated on `ROBOINVESTOR_ENABLED` |
| `types/` | Strawberry types: `_pydantic.py` helpers, `common.py`, and one module per domain |

## The schema auto-derives from Pydantic response models

Adding a read field is usually a three-line change. `strawberry.experimental.pydantic.type(model=..., all_fields=True)` walks the Pydantic model and generates a GraphQL type exposing every field, snake_case to camelCase (`auto_camel_case=True`, Strawberry's default).

```python
# graphql/types/ledger.py
@strawberry.experimental.pydantic.type(model=PydanticTrialBalanceResponse, all_fields=True)
class TrialBalance:
    """Trial balance for posted entries in a date range."""
```

That is the whole type definition. When the Pydantic model grows `baseline_total: Decimal | None`, it appears on the wire as `baselineTotal` with no edit here.

The single source of truth is the Pydantic model in `robosystems/models/api/extensions/*`. REST write responses and GraphQL reads are literally the same schema, so a new response field lands in both the SDK REST types and the SDK GraphQL types from one change.

## Resolvers are thin

```python
@strawberry.field
def entity(self, info: Info[GraphQLContext, None]) -> LedgerEntity | None:
    """Return the parent ledger entity (company) for a graph."""
    try:
        with _open_session(info, "roboledger") as session:
            response = reads_entity.get_parent_entity(session)
    except (ValueError, ProgrammingError):
        _raise_ledger_not_initialized()
    if response is None:
        return None
    return LedgerEntity.from_pydantic(response)
```

Every data resolver has that shape:

1. Open a session with `open_extensions_session(info, extension)` (imported as `_open_session`). It re-asserts auth, gates on the graph having the extension provisioned, and yields `extensions_session(graph_id)`.
2. Call into `operations/{roboledger,roboinvestor}/reads/*.py` — pure functions taking a session and returning a Pydantic model.
3. Wrap the result with the generated `from_pydantic()` classmethod.
4. Let domain errors become typed GraphQL errors with `extensions.code`.

**Resolvers never contain business logic.** The same `reads/*.py` modules are called by MCP tools and REST read paths, which is why the ops layer is the single source of truth — GraphQL, MCP, and REST agree by construction.

## Dynamic schema composition

```python
# graphql/schema.py
def _build_query_type() -> type:
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
    return strawberry.type(type("Query", bases, {}))
```

**Always-on mixins** — `InformationBlockQuery`, `TaxonomyBlockQuery`, `LibraryQuery`, and the `_BaseQuery` probe — are composed unconditionally. They work on both the library sentinel (`graph_id='library'`) and any tenant graph, because visibility follows the session `search_path`.

**Domain-gated mixins** — `LedgerQuery`, `InvestorQuery` — are guarded by feature flags. A ledger-only deployment simply has no `InvestorQuery` in its schema, so introspection can't discover `portfolios` and a client can branch on the schema shape rather than trial-and-error against runtime errors. The tradeoff is that introspection sees a different schema per deployment: there is no single published SDL.

If both domain flags are off, the router mounting `/extensions/{graph_id}/graphql` never mounts. The gate is `EXTENSIONS_GRAPHQL_ENABLED AND (ROBOLEDGER_ENABLED OR ROBOINVESTOR_ENABLED)`, applied in the application factory in the repo-root `main.py`; the flags are defined in `config/env.py`.

**Rule for new resolvers**: domain-specific reads go on `LedgerQuery` or `InvestorQuery` behind the existing flag guard. Cross-domain reads that must be available regardless of product flags go on `InformationBlockQuery` (or a new always-on mixin) with no guard.

## Query cost limits

The schema carries three Strawberry limiters, since each resolved field can open a session against the small extensions OLTP pool:

| Limiter | Env / SSM tunable | Default |
| ------- | ----------------- | ------- |
| `QueryDepthLimiter` | `EXTENSIONS_GRAPHQL_MAX_DEPTH` (`graphql/MAX_DEPTH`) | 15 |
| `MaxAliasesLimiter` | `EXTENSIONS_GRAPHQL_MAX_ALIASES` (`graphql/MAX_ALIASES`) | 30 |
| `MaxTokensLimiter` | `EXTENSIONS_GRAPHQL_MAX_TOKENS` (`graphql/MAX_TOKENS`) | 2000 |

The depth limiter does not count introspection fields, so SDK codegen is unaffected. `OpenTelemetryExtensionSync` is also registered — the `Sync` variant, because the async one breaks `schema.execute_sync(...)`.

## Pagination guards

Strawberry has no `Field(ge=…, le=…)` equivalent, so bounds are asserted at the resolver boundary in `resolvers/_common.py`. Pagination arguments are declared **nullable** (`Int`, not `Int! = N`) because generated SDK clients pass explicit `null` for omitted variables, and GraphQL rejects explicit null for a non-null argument even when it has a default. `resolve_pagination` defaults them and then bounds-checks:

```python
resolved_limit, resolved_offset = resolve_pagination(limit, offset, default_limit=50)
# limit must be 1..1000, offset >= 0, else INVALID_PAGINATION
```

Every paginated resolver calls it first.

## Adding a read field

Adding `fiscalCalendar.daysUntilClose` to the existing type:

1. **Add the field to the Pydantic response model** in `robosystems/models/api/extensions/fiscal_calendar.py`, with a `Field(description=...)`.
2. **Populate it in the ops-layer read function** in `robosystems/operations/roboledger/reads/fiscal_calendar.py`.
3. **Nothing to do on the GraphQL side.** The Strawberry type uses `all_fields=True`, so the field appears as `daysUntilClose: Int` automatically.
4. **Update SDK consumers if the field is client-facing** — the TypeScript and Python facades query fields explicitly, so the field name has to be added to their GraphQL query documents.

For a whole new query rather than a new attribute:

1. Add a Pydantic response model in `models/api/extensions/*.py` (domain) or `models/api/information_block.py` (cross-domain).
2. Add a read function in `operations/{domain}/reads/` or `operations/information_block/reads.py`.
3. Add a Strawberry wrapper in `graphql/types/` — usually one decorator line.
4. Add a resolver method on the appropriate mixin (gated for domain reads, unguarded for cross-domain).
5. Add a test under `tests/graphql/extensions/`.

## Hand-written types

`strawberry.experimental.pydantic.type` cannot resolve self-referencing fields. `AccountTreeNode` has `children: list[AccountTreeNode]`, which breaks `all_fields=True`, so it is hand-written with a `from_pydantic` classmethod that recurses manually. The cost is that new fields on `PydanticAccountTreeNode` need a parallel edit here. If you add a second recursive model, use the same pattern — forward references and `update_forward_refs()` do not help, because the decorator's generator doesn't honor them.

`InformationBlock` in `types/information_block.py` is hand-written for a different reason: `artifact.mechanics` is a discriminated union on `kind`. It is exposed as a `strawberry.scalars.JSON` payload with the `kind` tag inside, and `from_pydantic` constructs it explicitly. A real `strawberry.union(...)` is deferred until the arms grow field-level query needs. **A new block type therefore requires no change to this layer** — its mechanics ride the JSON scalar.

The envelope's `FactSet` carries the typed `provenance` field (the discriminated `FactProvenance` union: `pivot`, `schedule`, `derived`, `asserted`, `document`, `forecast`, `filed`), also surfaced as a JSON scalar.

## Errors

Typed GraphQL errors carry `extensions.code`. Clients branch on the code, not the message.

| Code | Meaning | Raised by |
| ---- | ------- | --------- |
| `UNAUTHENTICATED` | No valid credentials | `require_user` |
| `INVALID_PAGINATION` | `limit` or `offset` out of range | `validate_pagination` |
| `EXTENSION_NOT_PROVISIONED` | The graph does not have this extension | `require_extension` |
| `LEDGER_NOT_INITIALIZED` | Graph has no ledger schema yet | ledger resolvers |
| `INVESTOR_NOT_INITIALIZED` | Same shape for investor | investor resolvers |
| `INVALID_ARGUMENT` | Argument failed a resolver-level check | library resolvers |
| `INVALID_EXPIRES_IN`, `REPORT_BUNDLE_NOT_AVAILABLE`, `REPORT_BUNDLE_SIGNING_FAILED` | Report download failures | `reportDownloadUrl` |

Access control is the exception: `check_graph_access` raises **HTTP 403** from `get_context`, before the GraphQL layer, so a forbidden request never produces a GraphQL error body at all.

Add a new code when introducing a typed failure a frontend would want to handle distinctly — don't overload an existing one.

## Testing

Tests live under `tests/graphql/extensions/`: `test_schema.py` (introspection and per-flag composition), `test_schema_limits.py` (the cost limiters), `test_context.py` (auth and context wiring), and one module per resolver group (`test_ledger.py`, `test_investor.py`, `test_information_block.py`, `test_taxonomy_block.py`).

The pattern is to seed an isolated extensions schema with the `extensions_test_db` fixture, then call through `TestClient` with `POST /extensions/{graph_id}/graphql`. Assert on `response.json()["data"]` for success and `response.json()["errors"][0]["extensions"]["code"]` for typed failures. The `hello` probe is the smoke test — if it fails, auth or context wiring is broken before any domain resolver runs.

## Relationship to the write surface

```
Reads                                    Writes
─────                                    ──────
/extensions/{g}/graphql                  /extensions/roboledger/{g}/operations/{op}
  ↓                                      /extensions/roboinvestor/{g}/operations/{op}
graphql/resolvers/*.py                     ↓
  ↓                                      routers/extensions/{domain}/operations.py
operations/{domain}/reads/*.py             ↓
                                         operations/{domain}/commands/*.py
```

Both sides call into `operations/{domain}/*`. Whether a caller hits GraphQL, a named operation, or an MCP tool, the same ops-layer functions run. Adding business logic in a router, resolver, or MCP tool handler is a mistake — route it through the ops layer.

### Downloads are reads

Serialization-bundle downloads live here as the `reportDownloadUrl(reportId, format)` field on the `Report` query — not as a REST resource and not as a `download-report` operation. A download is a read of stored state, so it belongs on the read surface.

The catch: neither a GraphQL JSON response nor an `OperationEnvelope` can carry a raw binary zip. So **every flavor resolves to a presigned S3 URL** — JSON-LD is stamped at publish time, XBRL is materialized and cached on first request (`operations/roboledger/reads/reports.py:get_report_download_url`). The resolver only ever returns a URL string; the client follows it to S3.

There is no analytical view-operation home for it either: view operations (`build-fact-grid`, `live-financial-statement`) are LadybugDB-backed analytical queries, whereas a presigned-URL lookup is a plain OLTP read.
