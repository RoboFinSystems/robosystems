# Extensions GraphQL

Strawberry GraphQL schema served at `POST /extensions/{graph_id}/graphql`. This is the read surface for the extensions subsystem — RoboLedger and RoboInvestor reads run here; writes go through the named command operation routers at `/extensions/{roboledger,roboinvestor}/{graph_id}/operations/*`.

## URL shape

```
POST /extensions/{graph_id}/graphql
```

The endpoint is **graph-scoped at the URL level**. `graph_id` is a path parameter, not a query argument. Auth and per-graph access are validated by `get_context` before any resolver runs — resolvers read the graph id from `info.context["graph_id"]` via the `require_graph_id(info)` helper instead of taking it as a field argument. This avoids leaking a "wrong graph" failure mode where a client could pass a `graphId` argument that didn't match the URL.

In dev, hitting the endpoint in a browser renders GraphiQL with introspection enabled. In staging/prod, introspection is still allowed (Strawberry default) but the CSP policy is tightened to block the playground UI.

## What lives here

```
robosystems/graphql/
├── schema.py              # Query root, composed per enabled domain flags + always-on mixins
├── context.py             # get_context / require_user / require_graph_id / GraphQLContext
├── auth.py                # check_graph_access — wrapped by get_context before resolvers run
├── resolvers/
│   ├── _common.py         # Shared helpers (pagination guards, session opener)
│   ├── information_block.py  # InformationBlockQuery — always-on (not feature-gated)
│   ├── ledger.py          # LedgerQuery — roboledger read fields (ROBOLEDGER_ENABLED)
│   └── investor.py        # InvestorQuery — roboinvestor read fields (ROBOINVESTOR_ENABLED)
└── types/
    ├── common.py          # PaginationInfo
    ├── information_block.py  # Strawberry types for InformationBlockEnvelope and its atoms
    ├── ledger.py          # Strawberry types wrapping roboledger Pydantic response models
    └── investor.py        # Same for roboinvestor
```

## Design

### 1. The schema auto-derives from Pydantic response models

Adding a new read field is a three-line change in most cases. Strawberry's `experimental.pydantic.type(model=..., all_fields=True)` decorator walks the Pydantic model and generates a GraphQL type that exposes every field with matching names (snake_case → camelCase via `auto_camel_case=True`, on by default).

```python
# graphql/types/ledger.py
@strawberry.experimental.pydantic.type(model=PydanticTrialBalanceResponse, all_fields=True)
class TrialBalance:
    """Trial balance for posted entries in a date range."""
```

That's the whole type definition. When the Pydantic model grows a new field — say `PydanticTrialBalanceResponse.baseline_total: Decimal | None` — it auto-appears on the wire as `baselineTotal` with no code change to the GraphQL layer.

The single source of truth is the Pydantic model in `robosystems/models/api/extensions/*`. The REST write responses and the GraphQL reads are literally the same schema. This is intentional: it means a new response field lands in both the SDK REST types and the SDK GraphQL types from the same change.

### 2. Resolvers are thin — the ops layer is the source of truth

```python
@strawberry.field
def entity(self, info: Info[GraphQLContext, None]) -> LedgerEntity | None:
    """Return the parent ledger entity (company) for a graph."""
    try:
        with _open_session(info) as session:
            response = reads_entity.get_parent_entity(session)
    except (ValueError, ProgrammingError):
        _raise_ledger_not_initialized()
    if response is None:
        return None
    return LedgerEntity.from_pydantic(response)
```

Every data resolver follows this shape:

1. Open an extensions session via `_open_session(info)` — the helper pulls `graph_id` from context and returns `extensions_session(graph_id)` as a context manager.
2. Call into `operations/roboledger/reads/*.py` or `operations/roboinvestor/reads/*.py` — pure functions that take a session and return a Pydantic response model.
3. Wrap the Pydantic response in the Strawberry type via the auto-generated `from_pydantic()` classmethod.
4. Domain errors (uninitialized ledger, missing entity) become typed GraphQL errors with `code` extensions.

Resolvers never contain business logic. The same `reads/*.py` modules are called by MCP tools and by REST read paths (where they still exist), which is why the ops layer is the single source of truth — GraphQL, MCP, and REST agree by construction.

### 3. Dynamic schema composition

```python
# graphql/schema.py
def _build_query_type() -> type:
    bases: tuple[type, ...] = (InformationBlockQuery, _BaseQuery)
    if env.ROBOLEDGER_ENABLED:
        bases = (LedgerQuery, *bases)
    if env.ROBOINVESTOR_ENABLED:
        bases = (InvestorQuery, *bases)
    return strawberry.type(type("Query", bases, {}))
```

The `Query` root is built at class-construction time from whichever domain mixins are enabled. There are two composition patterns:

**Always-on mixins** (`InformationBlockQuery`) are composed into `bases` unconditionally. They use `open_library_session` so they work on both the library sentinel (`graph_id='library'`) and any tenant graph — reads are driven by the session's `search_path`. The `informationBlock` / `informationBlocks` fields are always present in the schema regardless of which product flags are on.

**Domain-gated mixins** (`LedgerQuery`, `InvestorQuery`) are guarded by feature flags. A ledger-only deployment:

- Has `LedgerQuery` and `InformationBlockQuery` in the schema
- Does **not** have `InvestorQuery` in the schema
- Introspection reports only ledger + information-block fields — there's no way for a client to discover or call `portfolios` on a deployment where investor is off

This is strictly better than the alternative ("expose everything, throw `INVESTOR_NOT_INITIALIZED` at runtime") because clients can branch on the actual schema shape rather than trial-and-error against runtime errors. The tradeoff is that introspection tooling sees a different schema per deployment. We don't publish a single SDL; the schema is composed dynamically per tenant feature-flag combination.

Per-domain gating also short-circuits the router: if both flags are off, the FastAPI router that mounts `/extensions/{graph_id}/graphql` never mounts at all (see `main.py` line ~369).

**Rule for new resolvers**: if the read is domain-specific (roboledger data, roboinvestor data), gate it behind the appropriate flag by adding the method to `LedgerQuery` or `InvestorQuery`. If the read is cross-domain or must be available regardless of which product domains are on, add it to `InformationBlockQuery` (or a new always-on mixin) and compose it into `bases` without a flag guard.

### 4. Auth and graph access are enforced before resolvers run

`get_context` in `context.py` runs **before** any resolver and:

1. Reads the user from the `X-API-Key` header (API keys) or `Authorization: Bearer` header (JWTs)
2. Reads `graph_id` from the URL path
3. Calls `check_graph_access(user, graph_id)` — eagerly, not lazily

If auth fails or the user doesn't have access to the graph, the request never reaches a resolver at all. The `_BaseQuery.hello` probe relies on this: it calls `require_user(info)` which re-asserts the auth contract, and the three-way status check is spelled out in its docstring.

The introspection bypass (`hello` + schema introspection allowed without credentials) is intentional — it lets SDK codegen workflows and health checks call the endpoint without auth, while data resolvers still enforce `require_user` so they fail cleanly if the request slips through.

### 5. Pagination guards are re-asserted at the resolver boundary

Strawberry doesn't have a `Field(ge=…, le=…)` equivalent, so bounds that FastAPI enforced via `Query(..., ge=N, le=M)` on the retired REST routes get reasserted in `resolvers/_common.py`:

```python
def validate_pagination(limit: int, offset: int) -> None:
    if not 1 <= limit <= 1000:
        raise strawberry.exceptions.StrawberryGraphQLError(
            message="limit must be between 1 and 1000",
            extensions={"code": "INVALID_PAGINATION"},
        )
    if offset < 0:
        raise strawberry.exceptions.StrawberryGraphQLError(
            message="offset must be >= 0",
            extensions={"code": "INVALID_PAGINATION"},
        )
```

Every paginated resolver calls this first-thing. The `INVALID_PAGINATION` extension code matches the pattern used for `UNAUTHENTICATED`, `LEDGER_NOT_INITIALIZED`, etc. — clients branch on `extensions.code`, not on human-readable messages.

## Adding a new read field

Concrete walkthrough — adding `fiscalCalendar.daysUntilClose` to the existing `FiscalCalendar` type as a computed integer.

1. **Add the field to the Pydantic response model.**

   ```python
   # robosystems/models/api/extensions/fiscal_calendar.py
   class FiscalCalendarResponse(BaseModel):
       ...
       days_until_close: int | None = Field(
           default=None,
           description="Days remaining until the next close target date",
       )
   ```

2. **Populate it in the ops-layer read function.**

   ```python
   # robosystems/operations/roboledger/reads/fiscal_calendar.py
   def get_fiscal_calendar(session: Session) -> FiscalCalendarResponse | None:
       ...
       days_until_close = (
           (calendar.close_target - date.today()).days
           if calendar.close_target else None
       )
       return FiscalCalendarResponse(..., days_until_close=days_until_close)
   ```

3. **That's it for the GraphQL side.** The Strawberry type in `graphql/types/ledger.py` uses `all_fields=True`, so the new `days_until_close` field auto-appears on the wire as `daysUntilClose: Int`. No type edit, no resolver edit, no SDK regen trigger needed — just run the frontend graphql codegen to pick it up.

4. **Update SDK consumers if needed.** TypeScript and Python SDK facades query the field explicitly, so they need the new field name added to their GraphQL query documents in `sdk-extensions/graphql/queries/*`. Skip this step if the field is backend-internal.

If the new field is a whole new query (not a new attribute on an existing type):

1. Add a new Pydantic response model in `models/api/extensions/*.py` (domain) or `models/api/information_block.py` (cross-domain).
2. Add a new `reads/*.py` function in `operations/{domain}/reads/` or `operations/information_block/reads.py`.
3. Add a Strawberry wrapper in `graphql/types/{ledger,investor,information_block}.py` — usually one line with the decorator.
4. Add a resolver method on the appropriate query mixin:
   - Domain reads (`LedgerQuery`, `InvestorQuery`) — gated by the existing feature-flag guards.
   - Cross-domain reads (`InformationBlockQuery`) — added unconditionally; no flag guard.
5. Add a test under `tests/graphql/extensions/test_{ledger,investor,information_block}.py`.

## The recursive escape hatch

Strawberry's `experimental.pydantic.type` decorator cannot resolve self-referencing fields. `AccountTreeNode` has `children: list[AccountTreeNode]`, which breaks `all_fields=True`. The workaround is to hand-write the Strawberry type with a `from_pydantic` classmethod that does the recursion manually:

```python
@strawberry.type
class AccountTreeNode:
    """Recursive Chart of Accounts tree node.

    Hand-written because `children: list[AccountTreeNode]` is a
    self-reference — Strawberry's `experimental.pydantic.type` decorator
    cannot resolve that automatically. Use `from_pydantic` to convert a
    `PydanticAccountTreeNode` into this type.
    """

    id: strawberry.ID
    code: str | None
    name: str
    classification: str
    account_type: str | None
    balance_type: str
    depth: int
    is_active: bool
    children: list[AccountTreeNode]

    @classmethod
    def from_pydantic(cls, node: PydanticAccountTreeNode) -> AccountTreeNode:
        return cls(
            id=strawberry.ID(node.id),
            code=node.code,
            name=node.name,
            classification=node.classification,
            account_type=node.account_type,
            balance_type=node.balance_type,
            depth=node.depth,
            is_active=node.is_active,
            children=[cls.from_pydantic(c) for c in node.children],
        )
```

The cost is that new fields on `PydanticAccountTreeNode` require a parallel edit to this class — the auto-derivation is gone for this one type. If you find yourself adding a second recursive model, follow the same pattern. Don't try to be clever with forward references or `update_forward_refs()` — the Strawberry decorator's generator doesn't honor them.

`InformationBlock` (in `types/information_block.py`) is hand-written for a different reason — its `artifact.mechanics` field is a discriminated union on `kind` (`ScheduleMechanics | StatementMechanics | MetricMechanics`), and Strawberry's pydantic decorator can't unwrap union types cleanly. The `from_pydantic` classmethod does the union dispatch manually. New block types add a `*MechanicsType` Strawberry wrapper and extend the union in `InformationBlock.artifact_mechanics`'s return type annotation.

The envelope's `FactSet` now carries a typed `provenance` field — the discriminated `FactProvenance` union (pivot/schedule/derived/asserted) — surfaced as a JSON scalar; it auto-derives like the other Pydantic fields.

## Testing

Tests live under `tests/graphql/extensions/`:

- `test_schema.py` — schema introspection + composition (per-flag variations)
- `test_ledger.py` — ledger resolvers, happy path + a couple of error paths each
- `test_investor.py` — investor resolvers, same shape
- `test_information_block.py` — `informationBlock` / `informationBlocks` fields, always-on

Pattern: use the existing `extensions_test_db` fixture to seed an isolated extensions schema, then call through `TestClient` with `POST /extensions/{graph_id}/graphql` and a query string. Assert on `response.json()["data"]` for success, `response.json()["errors"][0]["extensions"]["code"]` for typed failures.

The hello probe at the top of `_BaseQuery` is a good smoke test — if it fails, auth or context wiring is broken before any domain resolver ever runs.

## Error surface

Typed GraphQL errors with `extensions.code`:

| Code | Meaning | Emitted by |
| --- | --- | --- |
| `UNAUTHENTICATED` | No valid credentials | `require_user` |
| `FORBIDDEN` | Credentials valid, but user lacks access to `graph_id` | `check_graph_access` |
| `INVALID_PAGINATION` | `limit` or `offset` out of range | `validate_pagination` |
| `LEDGER_NOT_INITIALIZED` | Graph has no ledger schema yet (connect a data source first) | `_raise_ledger_not_initialized()` |
| `INVESTOR_NOT_INITIALIZED` | Same shape for investor | investor resolvers |

Clients branch on the code, not the message. Add a new code when introducing a new typed failure that a frontend might want to handle distinctly — don't overload an existing one.

## Relationship to the rest of the extensions surface

```
Reads                                    Writes
─────                                    ──────
/extensions/{g}/graphql                  /extensions/roboledger/{g}/operations/{op}
  ↓                                        /extensions/roboinvestor/{g}/operations/{op}
graphql/resolvers/*.py                     ↓
  ↓                                      routers/extensions/{domain}/operations.py
operations/{domain}/reads/*.py             ↓
                                         operations/{domain}/commands/*.py
```

Both sides ultimately call into `operations/{domain}/*`. That's the load-bearing invariant of the subsystem: whether a caller hits the GraphQL endpoint, a named operation, or an MCP tool, the same ops-layer functions run. Adding business logic anywhere else — routers, resolvers, MCP tool handlers — is a mistake. Route it through the ops layer.

### Downloads are reads (issue #751)

Serialization-bundle downloads live here too, as the `reportDownloadUrl(reportId, format)` field on the `Report` query — **not** as a REST resource and **not** as a `download-report` operation. A download is a read of stored state, so it belongs on the read surface.

The catch that made this non-obvious: the XBRL flavor used to stream a raw binary zip, which neither a GraphQL JSON response nor an `OperationEnvelope` can carry. The fix is that **every flavor resolves to a presigned S3 URL** — JSON-LD is stamped at publish time, XBRL is materialized + cached on first request (`operations/roboledger/reads/reports.py:get_report_download_url`). The resolver only ever returns a URL string; the client follows it to fetch the bytes directly from S3. This also keeps the read surface uniform — there's no REST GET outlier on the roboledger extensions surface anymore.

There is no analytical "view operation" home for it: view operations (`build-fact-grid`, `live-financial-statement`) are LadybugDB-backed analytical queries; a presigned-URL lookup is a plain OLTP read.
