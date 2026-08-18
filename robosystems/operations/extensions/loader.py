"""Generic OLTP loader — reads dbt output from DuckDB and inserts into PostgreSQL.

Connector-agnostic: the same code loads QuickBooks, Xero, NetSuite, or any
future adapter. All connector-specific transformation happens in dbt staging
models; the loader only sees the standardized OLTP output tables.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from robosystems.logger import logger
from robosystems.operations.event_block.commands import fire_handler_on_commit

# Per-source rule for whether captured events auto-commit to GL on
# inbound sync (handler fires immediately, event lands
# ``status='committed'``) vs. land in the inbox for human approval.
# Keyed by the ``source`` string written to ``Event.source``.
#
# This is a property of the SOURCE, not the connection's
# ``write_policy``. Inbound sync is a *mirror-down from an external
# system of record*: anything QuickBooks sends has already been booked
# there, so we replicate it straight into GL rather than queueing it for
# re-approval — regardless of ``write_policy``. ``write_policy`` governs
# the OUTBOUND (write-back) direction only (see ``event_block.commands``
# / ``close_service``). Keeping the two decoupled means a ``native`` QB
# connection ("sync down, don't write back") still produces a populated
# ledger. Native-origin events (manual / schedule / system / AI) carry
# no external system of record and always go through the inbox.
#
# Only flip a source to ``True`` once its adapter has been tested
# end-to-end with auto-commit — otherwise every event for that source
# auto-commits silently on first sync, with no inbox review. Future
# adapters (xero, netsuite, ...) start at ``False`` and graduate
# explicitly when their handler dispatch is verified against real data.
_SOURCE_AUTO_COMMITS: dict[str, bool] = {
  "quickbooks": True,
}


def _source_auto_commits_on_sync(source: str) -> bool:
  """Whether inbound events for ``source`` auto-commit to GL on sync.

  Decoupled from ``write_policy`` (which governs outbound write-back):
  inbound auto-commit is purely a property of the source — QB rows are
  already booked in QB, so we mirror them down regardless of the
  connection's policy. See ``_SOURCE_AUTO_COMMITS``.
  """
  return _SOURCE_AUTO_COMMITS.get(source, False)


# QuickBooks AccountType → FASB elementsOfFinancialStatements trait
# identifier. QB classifies every account into one of ~14 types;
# every type maps cleanly to one of the 5 EFS traits the rest of the
# rollup machinery expects (asset / liability / equity / revenue /
# expense). Without this translation, QB-imported elements have
# ``trait IS NULL`` and the auto-map agent skips them entirely
# (see operations/operators/implementations/mapping/operator.py).
#
# QB's "Cost of Goods Sold" rolls into expense for FASB EFS purposes —
# the IS sub-classification (operating-expense vs. COGS) is captured
# separately under ``activityType`` and isn't load-bearing here.
#
# When we layer in Xero / NetSuite, each gets its own dict keyed off
# its native classification taxonomy and the same target identifiers.
_QB_ACCOUNT_TYPE_TO_TRAIT: dict[str, str] = {
  # Assets
  "Bank": "asset",
  "Accounts Receivable": "asset",
  "Other Current Asset": "asset",
  "Fixed Asset": "asset",
  "Other Asset": "asset",
  # Liabilities
  "Accounts Payable": "liability",
  "Credit Card": "liability",
  "Other Current Liability": "liability",
  "Long Term Liability": "liability",
  # Equity
  "Equity": "equity",
  # Revenue
  "Income": "revenue",
  "Other Income": "revenue",
  # Expenses
  "Expense": "expense",
  "Cost of Goods Sold": "expense",
  "Other Expense": "expense",
}


# QuickBooks AccountType → FASB ``liquidity`` trait identifier
# (current / noncurrent). QB encodes liquidity directly in the type name
# ("Other CURRENT Asset", "FIXED Asset", "LONG TERM Liability"), so the
# mapping is unambiguous. Only assets and liabilities carry a liquidity
# axis — equity / revenue / expense accounts are deliberately absent (no
# liquidity trait is emitted for them).
#
# Captured alongside the EFS trait so the auto-map agent narrows rs-gaap
# candidates by current-vs-noncurrent rather than guessing from the account
# name — a "Bank" account should never surface noncurrent-asset candidates.
# Statement classification stays structural; this trait is advisory only.
_QB_ACCOUNT_TYPE_TO_LIQUIDITY: dict[str, str] = {
  # Current assets
  "Bank": "current",
  "Accounts Receivable": "current",
  "Other Current Asset": "current",
  # Noncurrent assets
  "Fixed Asset": "noncurrent",
  "Other Asset": "noncurrent",
  # Current liabilities
  "Accounts Payable": "current",
  "Credit Card": "current",
  "Other Current Liability": "current",
  # Noncurrent liabilities
  "Long Term Liability": "noncurrent",
}


# QuickBooks AccountSubType values that denote a contra-asset — an asset-
# classified account whose balance offsets (reduces) the gross asset it
# attaches to: accumulated depreciation/amortization/depletion and the
# allowance for doubtful accounts. QB files these under an asset
# AccountType ("Fixed Asset", "Other Current Asset") with an "Asset"
# Classification, so neither ``account_type`` nor the derived
# ``balance_type`` distinguishes them from a directly-held asset like a
# prepaid. The AccountSubType is the only signal QB emits — we promote it
# to the FASB ``contraAsset`` EFS trait at load (instead of the plain
# ``asset`` trait its account_type implies) so downstream readers — the
# auto-map agent, rollups, and the schedule roll-forward generator — can
# tell a balance that accumulates UP toward cost from one that draws DOWN
# to zero.
_QB_CONTRA_ASSET_SUB_TYPES: frozenset[str] = frozenset(
  {
    "AccumulatedDepreciation",
    "AccumulatedAmortization",
    "AccumulatedDepletion",
    "AccumulatedAmortizationOfOtherAssets",
    "AllowanceForBadDebts",
  }
)


# Accumulated-* sub-type families that are contra-ASSETS. Used as a
# forward-compatible catch-all (QB periodically adds new variants, e.g.
# AccumulatedDepreciationEquipment). Deliberately NOT a bare
# ``startswith("Accumulated")`` — ``AccumulatedOtherComprehensiveIncome``
# and ``AccumulatedAdjustment`` are QB *equity* sub-types, not contra-assets.
_QB_CONTRA_ASSET_SUB_TYPE_PREFIXES: tuple[str, ...] = (
  "AccumulatedDepreciation",
  "AccumulatedAmortization",
  "AccumulatedDepletion",
)


def _is_qb_contra_asset_sub_type(sub_type: str | None) -> bool:
  """True if a QuickBooks AccountSubType denotes a contra-asset.

  Matches the known contra sub-types plus the accumulated
  depreciation/amortization/depletion families as a forward-compatible
  catch-all. Other ``Accumulated*`` sub-types (e.g. the equity
  ``AccumulatedOtherComprehensiveIncome``) are intentionally excluded.
  """
  if not sub_type:
    return False
  return sub_type in _QB_CONTRA_ASSET_SUB_TYPES or sub_type.startswith(
    _QB_CONTRA_ASSET_SUB_TYPE_PREFIXES
  )


# Contra-asset account-NAME markers — the fallback signal when the source
# system's sub-type is uninformative. QuickBooks detail types are user-chosen:
# an accumulated-amortization account created under a generic detail type
# (e.g. "Other long-term assets") carries no contra sub-type at all, so the
# sub-type rule alone silently misses it and the account lands as a plain
# asset. An asset-classified account *named* for the accumulation convention
# is a contra by accounting convention. Only consulted when the account_type
# already classified the account as an asset, so equity Accumulated-* names
# (AOCI) can never match.
_CONTRA_ASSET_NAME_MARKERS: tuple[str, ...] = (
  "accumulated depreciation",
  "accumulated amortization",
  "accumulated depletion",
  "allowance for",
)


def _looks_like_contra_asset_name(name: str | None) -> bool:
  if not name:
    return False
  lowered = name.lower()
  return any(marker in lowered for marker in _CONTRA_ASSET_NAME_MARKERS)


def _account_efs_identifier(meta: dict, name: str | None) -> str | None:
  """Classify a source-system account into its FASB EFS trait identifier.

  ``account_type`` drives the base classification; asset-typed accounts are
  promoted to ``contraAsset`` on either signal — a known contra
  ``account_sub_type``, or (fallback) a contra-convention account name.
  Returns None when the account_type is missing or unknown; callers decide
  whether that is a warning.
  """
  acct_type = meta.get("account_type")
  if not acct_type:
    return None
  efs_identifier = _QB_ACCOUNT_TYPE_TO_TRAIT.get(acct_type)
  if efs_identifier == "asset" and (
    _is_qb_contra_asset_sub_type(meta.get("account_sub_type"))
    or _looks_like_contra_asset_name(name)
  ):
    return "contraAsset"
  return efs_identifier


# qname prefix per adapter source. MUST stay in lockstep with the
# materializer's Element projection (materialize.py ``tables["Element"]``),
# which prefers the stored qname and falls back to this same
# ``{prefix}:{code}`` derivation only for rows loaded before qnames were
# written at sync time. The stored value is authoritative once healed.
_ADAPTER_QNAME_PREFIXES: dict[str, str] = {
  "quickbooks": "qb",
  "xero": "xero",
  "plaid": "plaid",
}


def _derive_adapter_qname(external_source: str | None, code: str | None) -> str | None:
  """``qb:Intangible Assets:Accumulated Amortization``-style adapter qname.

  Derived from the source system's fully-qualified account code, which the
  source guarantees unique per book. Returns None for unknown sources or
  blank codes — never guess an identity.
  """
  prefix = _ADAPTER_QNAME_PREFIXES.get(str(external_source or "").lower())
  if not prefix or not code:
    return None
  return f"{prefix}:{code}"


# Balance-sheet EFS classifications are stock concepts (point-in-time →
# instant); everything else is a flow (duration). Mirrors
# ``_INSTANT_CLASSIFICATIONS`` in operations/roboledger/commands/elements.py
# — kept local so the sync path doesn't import the command layer.
_INSTANT_EFS_IDENTIFIERS: frozenset[str] = frozenset(
  {
    "asset",
    "contraAsset",
    "liability",
    "contraLiability",
    "equity",
    "contraEquity",
    "temporaryEquity",
  }
)


def _derive_element_period_type(efs_identifier: str | None) -> str:
  """'instant' for stock classifications, 'duration' otherwise/unknown."""
  if efs_identifier in _INSTANT_EFS_IDENTIFIERS:
    return "instant"
  return "duration"


def _parse_metadata(raw) -> dict:
  """Parse metadata from dbt output — may be a dict, JSON string, or None."""
  if isinstance(raw, dict):
    return raw
  if isinstance(raw, str):
    try:
      parsed = json.loads(raw)
      return parsed if isinstance(parsed, dict) else {}
    except (ValueError, TypeError):
      return {}
  return {}


def _classify_dispatch_error(exc: BaseException) -> str:
  """Map a dispatch exception to a typed error code.

  The inbox UI uses this code to surface "fix and retry" prompts —
  ``element_unmapped`` → "map account X in the CoA mapping",
  ``closed_period`` → "reopen the period or change the posting date",
  default ``unknown_error`` → generic retry button.

  Classification stays in this module to avoid the inbox UI needing
  to import the handler-side exception types.
  """
  exc_type = type(exc).__name__
  # Pattern-match on the exception name rather than importing the
  # handler types — keeps loader.py decoupled from
  # operations/event_block/python_handlers/.
  if exc_type == "ElementResolutionError":
    return "element_unmapped"
  if exc_type == "ClosedPeriodError":
    return "closed_period"
  if exc_type == "UnbalancedJournalEntryError":
    return "unbalanced_entry"
  return "unknown_error"


def _compare_sync_tokens(existing: str | None, incoming: str | None) -> str:
  """SyncToken freshness decision for the UPSERT gate.

  Returns one of:
  - ``"fresh"`` — incoming SyncToken is newer; proceed with UPSERT.
  - ``"same"`` — incoming matches existing; idempotent re-sync, skip.
  - ``"stale"`` — incoming is older (out-of-order replay / webhook race); skip.
  - ``"no_info"`` — at least one side is NULL/non-numeric. Proceed without
    gating (backfill path for rows without a stored SyncToken or
    JournalReport-only rows that ship without one).

  Compared as integers — QB SyncTokens are monotonic non-negative ints
  serialized as strings.
  """
  if existing is None or incoming is None:
    return "no_info"
  try:
    incoming_int = int(incoming)
    existing_int = int(existing)
  except (ValueError, TypeError):
    # Defensive: if a SyncToken ever arrives non-numeric, fall through
    # to the status-based branches rather than guess.
    return "no_info"
  if incoming_int > existing_int:
    return "fresh"
  if incoming_int == existing_int:
    return "same"
  return "stale"


def _stamp_dispatch_error(evt, exc: BaseException, now: datetime) -> None:
  """Capture dispatch failure detail on the event's metadata blob.

  Mutates ``evt.metadata_`` as a new dict so SQLAlchemy detects the
  JSONB change. The error metadata lives alongside the live payload
  rather than mutating it — committed/fulfilled events are immutable;
  captured events get a side-channel error trail the UI can read
  without losing the original adapter-supplied payload.

  Increments ``dispatch_attempts`` so the UI can show "this has failed
  3 times" and decide whether to surface a stronger remediation prompt.
  """
  meta = dict(evt.metadata_ or {})
  meta["dispatch_error"] = _classify_dispatch_error(exc)
  meta["dispatch_error_message"] = str(exc)[:500]
  meta["dispatch_error_at"] = now.isoformat()
  meta["dispatch_attempts"] = int(meta.get("dispatch_attempts", 0)) + 1
  evt.metadata_ = meta


@dataclass
class LoadResult:
  """Result of an OLTP load operation.

  Counts from event-block ingest:

  - ``elements`` / ``dimensions``: inserted directly (structural).
  - ``events_captured`` / ``events_updated``: per-transaction event_block
    rows written by ``_capture_transactions_as_events``. Source-of-truth
    sources (currently QuickBooks) auto-commit on sync — the registered
    handler fires immediately so the event lands ``status='committed'``
    with backing Transaction / Entry / LineItem rows. Native-mode events
    (manual / schedule / AI) still capture-then-approve via the inbox.
  - ``events_handler_dispatched`` / ``events_dispatch_failed``: how many
    of the captured events successfully fired their handler vs. fell back
    to ``captured`` because dispatch raised (e.g., element_external_id
    not yet mapped). "Dispatched" is intentionally broader than
    "committed" — the handler may set the event's terminal status to
    ``fulfilled`` (historical-posted data) instead of ``committed``;
    both count as a successful dispatch.
  - ``transactions`` / ``entries`` / ``line_items``: produced by handler
    dispatch, not direct insert. Kept on the dataclass for backward-compat
    with callers that log result counts.
  """

  graph_id: str
  source: str
  connection_id: str
  elements: int = 0
  transactions: int = 0
  entries: int = 0
  line_items: int = 0
  dimensions: int = 0
  events_captured: int = 0
  events_updated: int = 0
  events_handler_dispatched: int = 0
  events_dispatch_failed: int = 0
  agents_inserted: int = 0
  agents_updated: int = 0
  # Counts events whose QB-side payload changed under a
  # committed/fulfilled row. Live payload stays unchanged; drift flag +
  # drift_payload land on the event for the reconciliation queue.
  events_drift_detected: int = 0
  # Counts QB-inbound rows that matched a previously
  # write-backed RL event (round-trip recognition). Each one represents
  # a "no duplicate created" event the cross-source matcher caught.
  events_cross_source_matched: int = 0
  # Counts events skipped by the SyncToken freshness
  # gate. ``skipped_stale`` = incoming SyncToken older than what we
  # already have (out-of-order replay / future webhook race). ``skipped_same``
  # = incoming matches current version (idempotent re-sync). Both surface
  # in the sync log so we can see how much work the gate is saving.
  events_skipped_stale_sync_token: int = 0
  events_skipped_same_sync_token: int = 0
  # Drop counters surface data-quality issues that the loader masks with
  # defaults — visible in logs and dagster results so a sync that quietly
  # eats half of QB's data doesn't pass unnoticed.
  dropped_unbalanced_entries: int = 0
  dropped_empty_transactions: int = 0
  errors: list[str] = field(default_factory=list)

  @property
  def total_rows(self) -> int:
    return (
      self.elements
      + self.transactions
      + self.entries
      + self.line_items
      + self.dimensions
      + self.events_captured
      + self.events_updated
      + self.agents_inserted
      + self.agents_updated
    )


@dataclass
class _CaptureResult:
  """Internal counters for ``_capture_transactions_as_events``.

  Carried back to ``LoadResult`` so the sync log surfaces what the
  loader hardening dropped — see ``LoadResult.dropped_unbalanced_entries``
  / ``dropped_empty_transactions`` for the data-quality contract.
  """

  inserted: int = 0
  updated: int = 0
  handler_dispatched: int = 0
  dispatch_failed: int = 0
  # Events where the incoming payload diffs from an
  # already-committed/fulfilled local row. Stamped on the event metadata
  # for reconciliation rather than mutating the live payload.
  drift_detected: int = 0
  # Incoming QB rows that match a previously-written
  # RL-originated event via metadata.qb_external_id. The matcher skips
  # the INSERT + handler re-fire and stamps qb_sync_confirmed_at on
  # the existing event. Counter surfaces in the sync log so we can
  # confirm the round-trip is being recognised.
  cross_source_matched: int = 0
  # SyncToken freshness gate decisions. ``skipped_stale``
  # is incoming.sync_token < existing (out-of-order CDC, replay, future
  # webhook race); ``skipped_same`` is incoming == existing (idempotent
  # re-sync of an already-current row). Both bypass the status branches
  # below — no mutation, no drift check.
  skipped_stale_sync_token: int = 0
  skipped_same_sync_token: int = 0
  dropped_unbalanced_entries: int = 0
  dropped_empty_transactions: int = 0


@dataclass
class _AgentCaptureResult:
  """Result of agent UPSERT — counts plus the lookup map used to resolve
  ``agent_external_id`` → ``Agent.id`` when capturing events."""

  inserted: int = 0
  updated: int = 0
  external_to_id: dict[str, str] = field(default_factory=dict)


class OLTPLoader:
  """Loads dbt OLTP output tables into the extensions PostgreSQL database.

  Generic for all connectors — reads from DuckDB, resolves foreign keys,
  inserts into tenant schema. Same code for QB, Xero, NetSuite.

  The load is atomic: all existing data for the given source + connection_id
  is deleted and re-inserted in a single transaction.
  """

  def load(
    self,
    graph_id: str,
    source: str,
    connection_id: str,
    duckdb_path: str | Path,
    created_by: str,
    *,
    full_rebuild: bool = False,
    since_date: str | None = None,
  ) -> LoadResult:
    """Load dbt OLTP output into the extensions tenant schema.

    Re-sync semantics:

    - ``full_rebuild=True``: pre-sync DELETE wipes
      ``captured``/``classified`` rows for ``(source, connection_id)``
      (voided/committed/fulfilled survive), then UPSERTs from dbt.
      Operator-explicit reset path — used by the OAuth-callback first
      sync and the user's "rebuild from scratch" Resync action.
    - ``since_date`` set (incremental window): pre-sync DELETE is
      skipped entirely; UPSERT path mutates ``captured``/``classified``
      rows in place and inserts new ones. Existing committed/fulfilled
      events get drift-flagged if the incoming payload changed.
    - Neither set (default lookback): treated as incremental — same as
      ``since_date``. The wipe is scoped to operator-explicit full
      rebuilds only, so incremental syncs never wipe history outside
      their window.

    ``graph_id`` names the PostgreSQL tenant schema; ``connection_id`` scopes
    the data for isolation. ``since_date`` is advisory on the loader — the dbt
    mart is already window-scoped — and never enables the pre-sync DELETE.

    Returns a ``LoadResult`` with per-table row counts.
    """
    import duckdb

    from robosystems.db.extensions import extensions_session, provision_tenant_schema
    from robosystems.models.extensions import (
      Dimension,
      Element,
    )
    from robosystems.utils.ulid import generate_prefixed_ulid

    result = LoadResult(graph_id=graph_id, source=source, connection_id=connection_id)

    provision_tenant_schema(graph_id)

    # fetchall(), not fetchdf(): pyarrow and DuckDB coexisting in the
    # containerized Dagster workers segfault on the dataframe path.
    con = duckdb.connect(str(duckdb_path), read_only=True)
    try:
      existing_tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
      dbt_data: dict[str, list[dict]] = {}
      for table in [
        "elements",
        "agents",
        "transactions",
        "entries",
        "line_items",
        "dimensions",
      ]:
        if table in existing_tables:
          result_set = con.execute(f"SELECT * FROM {table}")
          columns = [desc[0] for desc in result_set.description]
          rows = result_set.fetchall()
          dbt_data[table] = [dict(zip(columns, row, strict=True)) for row in rows]
          logger.info(f"Read {len(dbt_data[table])} rows from dbt table '{table}'")
        else:
          logger.warning(f"Table '{table}' not found in DuckDB, skipping")
    finally:
      con.close()

    now = datetime.now(UTC)

    with extensions_session(graph_id) as session:
      # ── Pre-sync deletes (full_rebuild only) ───────────────────────
      #
      # This block is constrained as follows:
      #
      # - Runs ONLY on operator-explicit ``full_rebuild=True`` —
      #   incremental syncs never wipe history outside their window.
      # - Wipes only ``captured`` / ``classified`` events for
      #   ``(source, connection_id)``. Voided / committed / fulfilled
      #   events survive — operator rejections and handler-approved
      #   entries are immutable to re-sync.
      # - GL cascade (LineItem / Entry / Transaction) scopes to the
      #   captured/classified events being wiped via
      #   ``triggered_by_event_id``. Surviving fulfilled events keep
      #   their GL rows; only orphan GL from never-completed dispatches
      #   gets cleaned.
      # - Element + Dimension wipe stays here as a structural reset
      #   (full rebuild only). The element INSERT path itself is an
      #   UPSERT, but on full_rebuild we still want the reset because
      #   that's the operator-explicit "start over" mode.
      #
      # Local imports: hoisting these to module top would pull the
      # roboledger model package eagerly, which in turn drags Strawberry
      # GraphQL types and the entire extensions schema graph into every
      # process that imports the loader (Dagster jobs, lambda warmups).
      # Imported lazily so the loader stays cheap to import.
      from robosystems.models.extensions.element_trait import ElementTrait
      from robosystems.models.extensions.roboledger.entry import Entry
      from robosystems.models.extensions.roboledger.event import Event
      from robosystems.models.extensions.roboledger.line_item import LineItem
      from robosystems.models.extensions.roboledger.transaction import Transaction

      if full_rebuild and _source_auto_commits_on_sync(source):
        # Subquery of events about to be wiped — captured/classified rows
        # for this (source, connection_id). The GL cascade scopes via
        # triggered_by_event_id to this subquery so fulfilled events'
        # GL rows survive.
        events_to_wipe_subq = session.query(Event.id).filter(
          Event.source == source,
          Event.metadata_["connection_id"].astext == connection_id,
          Event.status.in_(("captured", "classified")),
        )
        entry_subq = session.query(Entry.id).filter(
          Entry.triggered_by_event_id.in_(events_to_wipe_subq)
        )
        session.query(LineItem).filter(LineItem.entry_id.in_(entry_subq)).delete(
          synchronize_session=False
        )
        session.query(Entry).filter(
          Entry.triggered_by_event_id.in_(events_to_wipe_subq)
        ).delete(synchronize_session=False)
        session.query(Transaction).filter(
          Transaction.triggered_by_event_id.in_(events_to_wipe_subq)
        ).delete(synchronize_session=False)
        # Now the events themselves. JSONB connection_id filter (the column
        # lives in metadata_, not as a top-level FK) keeps sibling QB
        # connections on the same graph untouched.
        session.query(Event).filter(
          Event.source == source,
          Event.metadata_["connection_id"].astext == connection_id,
          Event.status.in_(("captured", "classified")),
        ).delete(synchronize_session=False)
        session.flush()

      # Element + Dimension structural reset — only on full_rebuild.
      # ElementTrait cascade lives here too: traits are adapter-derived
      # and re-derived idempotently from QB AccountType on insert
      # (loader.py ``_apply_source_element_traits`` via ON CONFLICT
      # DO NOTHING at the upsert site). Library-seeded rows are
      # protected by the immutability trigger.
      #
      # Association cascade is intentionally NOT run here — full_rebuild
      # keeps user-curated CoA → us-gaap mappings alive. The element
      # UPSERT below preserves elem_* ULIDs so the FK targets remain valid.
      from robosystems.models.extensions.association import Association  # noqa: F401

      if full_rebuild:
        element_subq = session.query(Element.id).filter(
          Element.external_source == source,
          Element.connection_id == connection_id,
        )
        session.query(ElementTrait).filter(
          ElementTrait.element_id.in_(element_subq)
        ).delete(synchronize_session=False)

        session.query(Dimension).filter(
          Dimension.metadata_.contains({"source": source}),
        ).delete(synchronize_session=False)

        session.flush()
        logger.info(
          f"Full-rebuild pre-sync cleanup complete for source={source}, "
          f"connection_id={connection_id} "
          f"(voided/committed/fulfilled events + user mappings survive)"
        )
      elif since_date:
        logger.info(
          f"Incremental sync (since_date={since_date}, full_rebuild=False) — "
          f"skipping pre-sync DELETE; UPSERT path will handle the window."
        )
      else:
        logger.info(
          "Default lookback sync (full_rebuild=False) — "
          "skipping pre-sync DELETE; UPSERT path will handle changed rows."
        )

      # --- UPSERT elements (from dbt "elements" table) ---
      #
      # Lookup-then-update or insert, matching the Agent UPSERT pattern at
      # ``_capture_agents_from_qb``. Preserves ``elem_*`` ULIDs across
      # syncs — downstream Associations / IB Fact references hold their
      # FK targets stable. Defended at the DB level by
      # ``idx_elements_upsert_key`` (partial UNIQUE on
      # ``(external_source, connection_id, external_id)``).
      element_lookup: dict[str, str] = {}  # external_id → oltp_id
      external_parent_map: dict[str, str] = {}  # oltp_id → external_parent_id

      if "elements" in dbt_data:
        rows = dbt_data["elements"]
        external_ids = [str(row["external_id"]) for row in rows]
        existing_elements: dict[str, Element] = {}
        if external_ids:
          for el in (
            session.query(Element)
            .filter(
              Element.external_source == source,
              Element.connection_id == connection_id,
              Element.external_id.in_(external_ids),
            )
            .all()
          ):
            existing_elements[str(el.external_id)] = el

        # Adapter elements carry a derived qname (``{prefix}:{code}``) so
        # they are addressable everywhere library elements are — envelope
        # patches, the update validator's projection, and the OLAP graph
        # (which now prefers this stored value over its own synthesis).
        # Derived from the book-unique account code; healed on every sync
        # (NULL from pre-qname loader versions, or a source-side rename
        # moving the code).
        desired_qnames: dict[str, str] = {}
        for row in rows:
          derived = _derive_adapter_qname(
            str(row.get("external_source") or source), str(row["code"])
          )
          if derived:
            desired_qnames[str(row["external_id"])] = derived

        # qname uniqueness is schema-wide while the derivation is only
        # book-unique — a second connection (or a native element) may
        # already own the string. Fail loud and leave identity unset
        # rather than stealing it or crashing the sync.
        qname_owner_by_qname: dict[str, Element] = {}
        if desired_qnames:
          for el in (
            session.query(Element)
            .filter(Element.qname.in_(set(desired_qnames.values())))
            .all()
          ):
            qname_owner_by_qname[str(el.qname)] = el

        def _resolve_element_qname(ext_id: str) -> str | None:
          desired = desired_qnames.get(ext_id)
          if desired is None:
            return None
          owner = qname_owner_by_qname.get(desired)
          if owner is None or (
            str(owner.external_source or "") == source
            and str(owner.connection_id or "") == str(connection_id)
            and str(owner.external_id or "") == ext_id
          ):
            return desired
          logger.warning(
            "Element qname collision: %r is already owned by element %s "
            "(source=%s connection=%s) — leaving qname unset for %s "
            "account %s; resolve the collision, then re-sync to heal",
            desired,
            owner.id,
            owner.external_source,
            owner.connection_id,
            source,
            ext_id,
          )
          return None

        new_element_objects: list[Element] = []
        updated_count = 0
        for row in rows:
          ext_id = str(row["external_id"])
          ext_parent = row.get("external_parent_id")
          parent_external = (
            str(ext_parent)
            if ext_parent and str(ext_parent) not in ("", "None", "nan")
            else None
          )

          meta = _parse_metadata(row.get("metadata"))
          efs_identifier = _account_efs_identifier(meta, str(row["name"]))
          resolved_qname = _resolve_element_qname(ext_id)

          if ext_id in existing_elements:
            # UPDATE in place — preserve elem_* ULID.
            el = existing_elements[ext_id]
            el.code = str(row["code"])
            el.name = str(row["name"])
            el.description = str(row["description"]) if row.get("description") else None
            el.balance_type = str(row["balance_type"])
            el.depth = int(row.get("depth", 0))
            el.path = str(row.get("path", ""))
            el.currency = str(row.get("currency", "USD"))
            el.is_active = bool(row.get("is_active", True))
            el.is_placeholder = bool(row.get("is_placeholder", False))
            el.metadata_ = meta
            # Heal XBRL-intrinsic metadata rows loaded before the loader
            # wrote it (or drifted since): qname follows the source code;
            # period_type follows the account's stock/flow classification.
            if resolved_qname is not None and el.qname != resolved_qname:
              el.qname = resolved_qname
            if efs_identifier is not None:
              desired_period_type = _derive_element_period_type(efs_identifier)
              if el.period_type != desired_period_type:
                el.period_type = desired_period_type
            if el.item_type is None:
              el.item_type = "monetary"
            el.updated_at = now
            element_lookup[ext_id] = el.id
            if parent_external:
              external_parent_map[el.id] = parent_external
            updated_count += 1
          else:
            # INSERT new — mint fresh ULID.
            oltp_id = generate_prefixed_ulid("elem")
            element_lookup[ext_id] = oltp_id
            if parent_external:
              external_parent_map[oltp_id] = parent_external
            new_element_objects.append(
              Element(
                id=oltp_id,
                code=str(row["code"]),
                name=str(row["name"]),
                description=str(row["description"]) if row.get("description") else None,
                qname=resolved_qname,
                balance_type=str(row["balance_type"]),
                parent_id=None,  # resolved in second pass
                depth=int(row.get("depth", 0)),
                path=str(row.get("path", "")),
                currency=str(row.get("currency", "USD")),
                is_active=bool(row.get("is_active", True)),
                is_placeholder=bool(row.get("is_placeholder", False)),
                source=source,
                period_type=_derive_element_period_type(efs_identifier),
                element_type="concept",
                is_abstract=False,
                is_monetary=True,
                item_type="monetary",
                external_id=ext_id,
                external_source=str(row["external_source"]),
                connection_id=connection_id,
                metadata_=meta,
                version=1,
                created_at=now,
                updated_at=now,
                created_by=created_by,
              )
            )

        if new_element_objects:
          session.add_all(new_element_objects)
        session.flush()

        # Second pass: resolve parent_id (works for both inserted and
        # updated rows — both populate external_parent_map above).
        for oltp_id, ext_parent_id in external_parent_map.items():
          parent_oltp_id = element_lookup.get(ext_parent_id)
          if parent_oltp_id:
            session.query(Element).filter(Element.id == oltp_id).update(
              {"parent_id": parent_oltp_id}, synchronize_session=False
            )

        session.flush()
        result.elements = len(rows)
        logger.info(
          f"UPSERTed {result.elements} elements "
          f"({len(new_element_objects)} new, {updated_count} updated)"
        )

        # --- Apply trait classifications from source-system metadata ---
        #
        # QB returns an AccountType per account; translate to the FASB
        # elementsOfFinancialStatements trait so the auto-map agent (and
        # any other reader that filters by trait) can group elements
        # without an AI roundtrip. Without this, QB elements land with
        # trait=NULL and rollup workflows silently skip them.
        traits_applied = self._apply_source_element_traits(
          session,
          source=source,
          connection_id=connection_id,
          created_by=created_by,
          now=now,
        )
        if traits_applied:
          logger.info(
            f"Applied {traits_applied} element_trait rows from {source} "
            f"AccountType metadata"
          )

      # --- UPSERT agents ---
      #
      # Customers / vendors / employees pulled per-entity from the source
      # system. UPSERT keyed on (connection_id, source, external_id) so
      # two QB connections on the same graph don't share agents. Returns
      # a lookup map external_id → Agent.id used by the event capture
      # below to resolve agent_id from header data.
      agent_capture = self._capture_agents_from_qb(
        session,
        dbt_data,
        source=source,
        connection_id=connection_id,
        created_by=created_by,
        now=now,
      )
      result.agents_inserted = agent_capture.inserted
      result.agents_updated = agent_capture.updated
      logger.info(
        "Captured %d new agents, updated %d existing",
        agent_capture.inserted,
        agent_capture.updated,
      )

      # --- CAPTURE transactions as event_blocks ---
      #
      # Each QB transaction becomes one Event row with
      # ``status='captured'`` and ``apply_handlers=False``. GL rows
      # (Transaction / Entry / LineItem) are produced after approval via
      # the inbox flow by the ``journal_entry_recorded`` handler.
      #
      # UPSERT semantics keyed on ``events(source, external_id)`` — the
      # unique partial index already guards uniqueness; this method
      # reconciles in-application so re-syncing the same window doesn't
      # grow the row count or strand handler-approved committed events.
      capture_result = self._capture_transactions_as_events(
        session,
        dbt_data,
        source=source,
        connection_id=connection_id,
        created_by=created_by,
        now=now,
        agent_lookup=agent_capture.external_to_id,
      )
      result.events_captured = capture_result.inserted
      result.events_updated = capture_result.updated
      result.events_handler_dispatched = capture_result.handler_dispatched
      result.events_dispatch_failed = capture_result.dispatch_failed
      result.events_drift_detected = capture_result.drift_detected
      result.events_cross_source_matched = capture_result.cross_source_matched
      result.events_skipped_stale_sync_token = capture_result.skipped_stale_sync_token
      result.events_skipped_same_sync_token = capture_result.skipped_same_sync_token
      result.dropped_unbalanced_entries = capture_result.dropped_unbalanced_entries
      result.dropped_empty_transactions = capture_result.dropped_empty_transactions
      logger.info(
        "Captured %d new event_blocks, updated %d existing, "
        "flagged %d drift, matched %d cross-source round-trips, "
        "skipped %d same-version + %d stale via SyncToken gate "
        "(dropped %d unbalanced entries, %d empty transactions; "
        "capture-only — no GL writes)",
        capture_result.inserted,
        capture_result.updated,
        capture_result.drift_detected,
        capture_result.cross_source_matched,
        capture_result.skipped_same_sync_token,
        capture_result.skipped_stale_sync_token,
        capture_result.dropped_unbalanced_entries,
        capture_result.dropped_empty_transactions,
      )

      # --- INSERT dimensions ---
      # TODO: populate line_item_dimensions junction table to link
      # line items to their dimensions (needed for graph materialization)
      if "dimensions" in dbt_data:
        rows = dbt_data["dimensions"]
        dim_objects = []
        for row in rows:
          oltp_id = generate_prefixed_ulid("dim")

          dim_objects.append(
            Dimension(
              id=oltp_id,
              dimension_type=str(row["dimension_type"]),
              name=str(row["name"]),
              value=str(row["value"]),
              metadata_={"source": source},
              is_active=True,
              created_at=now,
              updated_at=now,
            )
          )

        session.add_all(dim_objects)
        session.flush()
        result.dimensions = len(rows)
        logger.info(f"Inserted {result.dimensions} dimensions")

    # Update entity with connector CompanyInfo (if available)
    self._update_entity_from_company_info(
      graph_id=graph_id,
      source=source,
      connection_id=connection_id,
      duckdb_path=duckdb_path,
    )

    # Ensure CoA taxonomy + mapping structure exist (required for report generation)
    self._ensure_mapping_structure(graph_id, source, created_by)

    logger.info(
      f"OLTP load complete for graph={graph_id}, source={source}: "
      f"{result.total_rows} total rows"
    )
    return result

  def _apply_source_element_traits(
    self,
    session,
    *,
    source: str,
    connection_id: str,
    created_by: str,
    now: datetime,
  ) -> int:
    """Translate source-system account classifications into FASB EFS
    (+ liquidity) traits on the freshly-inserted elements.

    For each element scoped to ``(source, connection_id)`` whose
    ``metadata.account_type`` matches a known source-system type,
    insert an ``ElementTrait`` row pointing at the corresponding
    ``elementsOfFinancialStatements`` trait, and — for assets and
    liabilities — a second row for the ``liquidity`` (current /
    noncurrent) trait so the auto-map agent can narrow candidates by
    liquidity instead of inferring it from the account name. Returns the
    number of rows actually inserted (EFS + liquidity).

    Idempotent, and self-healing for adapter-written rows: an element
    whose stored EFS trait no longer matches what this pass derives
    (e.g. a contra-asset loaded before contra-promotion existed, stuck
    on plain ``asset``) has its adapter-written rows replaced. Rows with
    any other provenance (``source`` not equal to this adapter — the
    envelope/manual path leaves source NULL) are deliberate overrides
    and are never touched. Inserts use ON CONFLICT DO NOTHING to handle
    re-syncs cleanly.

    Currently only QuickBooks is wired. Xero / NetSuite layer in by
    adding their classification dict + a source check. The cross-source
    pattern is the same: the source's native account taxonomy is the
    seed, FASB EFS is the canonical target.
    """
    if source != "quickbooks":
      return 0
    type_to_efs = _QB_ACCOUNT_TYPE_TO_TRAIT
    type_to_liquidity = _QB_ACCOUNT_TYPE_TO_LIQUIDITY

    from robosystems.models.extensions import Element
    from robosystems.models.extensions.element_trait import ElementTrait
    from robosystems.models.extensions.trait import Trait

    def _resolve_trait_ids(category: str, identifiers: set[str]) -> dict[str, str]:
      return {
        t.identifier: t.id
        for t in session.query(Trait).filter(
          Trait.category == category,
          Trait.identifier.in_(identifiers),
        )
      }

    # EFS is required (it's the trait the auto-map agent narrows on);
    # liquidity is an additive narrowing signal — if its trait rows are
    # absent we still emit EFS rather than skipping the element.
    efs_id_by_identifier = _resolve_trait_ids(
      "elementsOfFinancialStatements",
      # ``contraAsset`` is not a target of the account_type map (every QB
      # asset type maps to plain ``asset``); it's selected per-element
      # below from the AccountSubType, so resolve its id here too.
      set(type_to_efs.values()) | {"contraAsset"},
    )
    if not efs_id_by_identifier:
      logger.warning(
        "No elementsOfFinancialStatements traits found in graph — "
        "skipping %s trait translation",
        source,
      )
      return 0
    liquidity_id_by_identifier = _resolve_trait_ids(
      "liquidity", set(type_to_liquidity.values())
    )

    # Fetch the source's elements with their metadata.
    elements = (
      session.query(Element)
      .filter(
        Element.external_source == source,
        Element.connection_id == connection_id,
      )
      .all()
    )

    # Existing EFS rows per element — loaded so a stale adapter-written
    # classification can be HEALED, not just left alongside a new row.
    # Joined on category rather than the adapter's identifier set so a
    # manual override pointing at any EFS trait is still seen (and
    # protected) here.
    existing_efs_rows: dict[str, list[ElementTrait]] = {}
    if elements:
      for et, _t in (
        session.query(ElementTrait, Trait)
        .join(Trait, ElementTrait.trait_id == Trait.id)
        .filter(
          Trait.category == "elementsOfFinancialStatements",
          ElementTrait.element_id.in_([e.id for e in elements]),
        )
        .all()
      ):
        existing_efs_rows.setdefault(str(et.element_id), []).append(et)

    rows: list[ElementTrait] = []
    healed = 0

    def _trait_row(element_id: str, trait_id: str) -> ElementTrait:
      return ElementTrait(
        element_id=element_id,
        trait_id=trait_id,
        is_primary=True,
        confidence=1.0,
        source=source,
        created_at=now,
        updated_at=now,
        created_by=created_by,
      )

    for elem in elements:
      meta = elem.metadata_ or {}
      acct_type = meta.get("account_type")
      if not acct_type:
        continue
      # ``_account_efs_identifier`` carries the contra-asset promotion:
      # asset-typed accounts flip to ``contraAsset`` on a known contra
      # AccountSubType OR a contra-convention account name (the fallback
      # for user-chosen generic detail types). The asset guard inside it
      # keeps equity Accumulated-* names (AOCI) from ever matching.
      efs_identifier = _account_efs_identifier(meta, str(elem.name))
      if efs_identifier is None:
        # Unknown source-system AccountType — log so unmapped types
        # surface in sync logs rather than silently producing
        # untraited elements.
        logger.warning(
          "Unknown %s account_type %r on element %s — skipping trait",
          source,
          acct_type,
          elem.id,
        )
        continue
      efs_trait_id = efs_id_by_identifier.get(efs_identifier)
      if efs_trait_id is not None:
        current_rows = existing_efs_rows.get(str(elem.id), [])
        if not current_rows:
          rows.append(_trait_row(elem.id, efs_trait_id))
        elif all(str(r.trait_id) == str(efs_trait_id) for r in current_rows):
          pass  # already classified correctly
        elif all((r.source or "") == source for r in current_rows):
          # Every existing EFS row was written by this adapter and no
          # longer matches the derived classification — heal in place.
          for stale_row in current_rows:
            session.delete(stale_row)
          rows.append(_trait_row(elem.id, efs_trait_id))
          healed += 1
        else:
          logger.debug(
            "Element %s EFS trait differs from %s derivation (%s) but "
            "carries non-adapter provenance — leaving the override",
            elem.id,
            source,
            efs_identifier,
          )

      # Liquidity (current / noncurrent) — assets & liabilities only;
      # absent for equity / revenue / expense. Additive narrowing signal.
      liquidity_identifier = type_to_liquidity.get(acct_type)
      if liquidity_identifier is not None:
        liquidity_trait_id = liquidity_id_by_identifier.get(liquidity_identifier)
        if liquidity_trait_id is not None:
          rows.append(_trait_row(elem.id, liquidity_trait_id))

    if healed:
      logger.info(
        "Healed %d stale adapter-written EFS trait assignment(s) from %s",
        healed,
        source,
      )
    if not rows:
      return 0

    # Bulk insert; ignore conflicts on the (element_id, trait_id) PK
    # so re-syncs (which delete + re-insert elements) and concurrent
    # runs don't blow up. The element-delete pre-pass already cascades
    # through element_traits via the FK; this is belt-and-suspenders.
    #
    # Postgres-only: ``ON CONFLICT DO NOTHING`` is a Postgres dialect
    # extension. Acceptable because the extensions DB is RDS Postgres
    # by definition (schema-per-graph tenancy lives nowhere else).
    from sqlalchemy.dialects.postgresql import insert as pg_insert

    stmt = (
      pg_insert(ElementTrait.__table__)
      .values(
        [
          {
            "element_id": r.element_id,
            "trait_id": r.trait_id,
            "is_primary": r.is_primary,
            "confidence": r.confidence,
            "source": r.source,
            "created_at": r.created_at,
            "updated_at": r.updated_at,
            "created_by": r.created_by,
          }
          for r in rows
        ]
      )
      .on_conflict_do_nothing(index_elements=["element_id", "trait_id"])
    )
    session.execute(stmt)
    session.flush()
    return len(rows)

  def _capture_agents_from_qb(
    self,
    session,
    dbt_data: dict,
    *,
    source: str,
    connection_id: str,
    created_by: str,
    now: datetime,
  ) -> _AgentCaptureResult:
    """UPSERT agents from the dbt agents mart.

    Keyed on ``(connection_id, source, external_id)`` so two connections
    on the same graph don't share agents. Existing agents are updated in
    place with the latest field values from QB. Returns a lookup map
    used by event capture to resolve ``agent_external_id`` → ``Agent.id``.
    """
    from robosystems.models.extensions.roboledger import Agent
    from robosystems.utils.ulid import generate_prefixed_ulid

    out = _AgentCaptureResult()
    rows = dbt_data.get("agents") or []
    if not rows:
      return out

    # Look up existing agents for this connection scoped by external_id.
    external_ids = [str(row["id"]) for row in rows if row.get("id")]
    existing: dict[str, Agent] = {}
    if external_ids:
      query = session.query(Agent).filter(
        Agent.source == source,
        Agent.connection_id == connection_id,
        Agent.external_id.in_(external_ids),
      )
      for agent in query.all():
        existing[agent.external_id] = agent

    new_agents: list[Agent] = []
    for row in rows:
      ext_id = str(row.get("id") or "")
      if not ext_id:
        continue

      # address is JSON-stringified at the parquet layer (pyarrow can't infer
      # struct schema for empty data) — decode back to a dict for JSONB.
      raw_address = row.get("address")
      if isinstance(raw_address, dict):
        address = raw_address
      elif isinstance(raw_address, str) and raw_address:
        try:
          parsed = json.loads(raw_address)
          address = parsed if isinstance(parsed, dict) else {}
        except (ValueError, TypeError):
          address = {}
      else:
        address = {}

      name = str(row.get("name") or "")
      legal_name = str(row.get("legal_name") or "") or None
      email = str(row.get("email") or "") or None
      phone = str(row.get("phone") or "") or None
      tax_id = str(row.get("tax_id") or "") or None
      agent_type = str(row.get("agent_type") or "other")
      is_1099 = bool(row.get("is_1099_recipient", False))
      is_active = bool(row.get("is_active", True))
      # SyncToken capture — persisted as metadata_['qb_sync_token']
      # (JSONB-only). NULL/empty acceptable; backfilled on subsequent
      # syncs. The freshness gate uses this to skip stale/duplicate rows.
      qb_sync_token_raw = row.get("sync_token")
      qb_sync_token = str(qb_sync_token_raw) if qb_sync_token_raw else None

      if ext_id in existing:
        agent = existing[ext_id]
        agent.agent_type = agent_type
        agent.name = name
        agent.legal_name = legal_name
        agent.email = email
        agent.phone = phone
        agent.address = address
        agent.tax_id = tax_id
        agent.is_1099_recipient = is_1099
        agent.is_active = is_active
        agent.updated_at = now
        # Mutate as a new dict so SQLAlchemy detects the JSONB change.
        new_meta = dict(agent.metadata_ or {})
        if qb_sync_token is not None:
          new_meta["qb_sync_token"] = qb_sync_token
        agent.metadata_ = new_meta
        out.external_to_id[ext_id] = agent.id
        out.updated += 1
      else:
        agent = Agent(
          id=generate_prefixed_ulid("agt"),
          agent_type=agent_type,
          name=name,
          legal_name=legal_name,
          email=email,
          phone=phone,
          address=address,
          tax_id=tax_id,
          source=source,
          external_id=ext_id,
          connection_id=connection_id,
          is_active=is_active,
          is_1099_recipient=is_1099,
          metadata_={"qb_sync_token": qb_sync_token} if qb_sync_token else {},
          created_at=now,
          updated_at=now,
          created_by=created_by,
        )
        new_agents.append(agent)
        out.external_to_id[ext_id] = agent.id
        out.inserted += 1

    if new_agents:
      session.add_all(new_agents)
    session.flush()
    return out

  def _capture_transactions_as_events(
    self,
    session,
    dbt_data: dict,
    *,
    source: str,
    connection_id: str,
    created_by: str,
    now: datetime,
    agent_lookup: dict[str, str] | None = None,
  ) -> _CaptureResult:
    """Capture each dbt-staged QB transaction as an event_block row.

    The dbt staging emits three flat tables — ``transactions``, ``entries``,
    ``line_items`` — joined by external IDs. This method groups them
    transaction-first (an Event for each transaction, with its entries
    and line_items packed into ``Event.metadata_``).

    Source-of-truth auto-commit: for sources where the originating system
    has already approved the entry (currently ``quickbooks`` — anything
    in QB has been booked there), the event's handler fires immediately
    and the event lands ``status='committed'`` with backing GL rows. The
    inbox is reserved for events that genuinely need human review — AI
    suggestions, manual journal entries, scheduled accruals. Inbound
    auto-commit is keyed purely on the ``source`` (``_SOURCE_AUTO_COMMITS``)
    and is independent of the connection's ``write_policy`` — that column
    governs only the OUTBOUND (write-back) direction, so a ``native`` QB
    connection still mirrors QB down into a populated ledger. Events
    whose handler dispatch fails (e.g., element_external_id not yet
    mapped) fall back to ``status='captured'`` so the user can fix the
    underlying issue and re-approve from the inbox.

    Idempotent re-sync: looks up existing events by ``(source,
    external_id)`` and updates them in place rather than inserting
    duplicates. Events already in a non-captured terminal state
    (committed / fulfilled / voided / superseded) are left untouched —
    a re-sync of QB data can't undo a handler-approved entry.

    Hardening: the captured metadata must satisfy the
    ``journal_entry_recorded`` handler's nested-entries schema at
    approve time. To make that contract enforceable on the producer
    side, this method:

    - fills `entries[].memo` from `qb_doc_number` → synthetic
      `"QB {qb_txn_type} {ext_id}"` when QB returns no memo;
    - fills `entries[].posting_date` from the transaction's
      `occurred_at` date when missing;
    - drops entries whose post-zero-filter line_items count drops below
      2 (handler requires `min_length=2`);
    - drops transactions whose surviving entries count drops to 0.

    The two drop counters surface in ``LoadResult`` so a sync that
    quietly eats half the QB data is visible.
    """
    from robosystems.models.extensions.roboledger import Event

    # 1) Group dbt rows by transaction
    txns_by_ext: dict[str, dict] = {}
    for row in dbt_data.get("transactions", []) or []:
      ext_id = str(row["external_id"])
      txns_by_ext[ext_id] = {**row, "entries": []}

    entries_by_ext: dict[str, dict] = {}
    for row in dbt_data.get("entries", []) or []:
      ent_ext_id = str(row["external_id"])
      txn_ext_id = str(row.get("external_transaction_id", ent_ext_id))
      if txn_ext_id not in txns_by_ext:
        # Orphan entry — log and skip
        continue
      entry = {**row, "line_items": []}
      txns_by_ext[txn_ext_id]["entries"].append(entry)
      entries_by_ext[ent_ext_id] = entry

    for row in dbt_data.get("line_items", []) or []:
      entry_ext_id = str(row["entry_external_id"])
      if entry_ext_id not in entries_by_ext:
        continue
      debit = int(row["debit_amount"])
      credit = int(row["credit_amount"])
      # Skip zero-amount lines (QB tax/memo placeholders)
      if debit == 0 and credit == 0:
        continue
      entries_by_ext[entry_ext_id]["line_items"].append(
        {
          "element_external_id": str(row["element_external_id"]),
          "debit_amount": debit,
          "credit_amount": credit,
          "description": str(row["description"]) if row.get("description") else None,
          "line_order": int(row.get("line_order", 0)),
        }
      )

    if not txns_by_ext:
      return _CaptureResult()

    out = _CaptureResult()
    new_events = []

    # 2) Look up existing events for the (source, external_id) pairs.
    #
    # Locked, because the auto-commit pass below dispatches handlers for the
    # rows this returns. Without the lock an inbox approval (`update_event_block`,
    # which takes the matching row lock) can commit between this read and that
    # dispatch, and the handler fires on both sides — one event, two sets of GL
    # rows, and a ledger that still foots and is still wrong. Most of these rows
    # get write-locked by the UPSERT below anyway; the gap this closes is the
    # re-sync whose payload is unchanged, which issues no UPDATE and so would
    # take no lock.
    #
    # Locking here rather than just before dispatch is deliberate: a row lock is
    # held to transaction end either way, so acquiring it at the top costs one
    # query instead of one per event, and covers the UPSERT decisions too.
    #
    # Deliberately unbounded (no `lock_timeout`): a sync is a background job and
    # should wait for a conflicting approval rather than fail the batch. The
    # approval side is the one that bounds its wait.
    existing: dict[str, Event] = {
      e.external_id: e
      for e in session.query(Event)
      .filter(
        Event.source == source,
        Event.external_id.in_(list(txns_by_ext.keys())),
      )
      # Ordered for the same reason as the other batch locks, though this
      # one's rows are disjoint from theirs by `source`. Uniform discipline:
      # the day a predicate widens, the ordering is already there.
      # See `locking.ORDERED_LOCK_KEY`.
      .order_by(Event.id)
      .with_for_update()
      .all()
    }

    # 2b) Cross-source matcher — for each incoming QB
    # external_id, look for an RL-originated event whose
    # `metadata.qb_external_id` mentions it (either exact match or as
    # an element in the comma-joined multi-entry form). These are
    # round-trips of our own write-back; we stamp confirmation and
    # skip the INSERT + handler re-fire for those external_ids.
    #
    # Index support: `idx_events_qb_external_id` (migration 0014) is a
    # partial expression index on `metadata->>'qb_external_id'`. The
    # multi-entry case (comma-joined IDs in the column) doesn't hit the
    # index efficiently for substring matching; for v1 we accept the
    # seq-scan fallback because multi-entry write-backs are rare.
    cross_source_external_ids: set[str] = set()
    if source == "quickbooks":
      incoming_ext_ids = list(txns_by_ext.keys())
      cross_candidates = (
        session.query(Event)
        .filter(Event.metadata_["qb_external_id"].astext.isnot(None))
        .filter(Event.source.in_(("manual", "schedule", "system")))
        .all()
      )
      for cand in cross_candidates:
        cand_meta = cand.metadata_ or {}
        cand_qb_ids = str(cand_meta.get("qb_external_id", "")).split(",")
        matched_ids = [qid for qid in cand_qb_ids if qid and qid in incoming_ext_ids]
        if matched_ids:
          # Stamp confirmation timestamp on the RL-originated event.
          new_meta = dict(cand_meta)
          new_meta["qb_sync_confirmed_at"] = now.isoformat()
          cand.metadata_ = new_meta
          for qid in matched_ids:
            cross_source_external_ids.add(qid)
          out.cross_source_matched += 1
      if cross_source_external_ids:
        logger.info(
          f"Cross-source matcher recognised {len(cross_source_external_ids)} "
          f"round-tripped event(s); skipping INSERT for those external_ids"
        )

    for ext_id, txn in txns_by_ext.items():
      # Skip rows that matched the cross-source pass
      # (round-trips of our own write-back). Confirmation stamp already
      # landed on the RL-originated event; no twin needed in the events
      # table.
      if ext_id in cross_source_external_ids:
        continue

      occurred_at = txn.get("date") or now
      # dbt sometimes returns dates rather than datetimes — normalize.
      if hasattr(occurred_at, "isoformat") and not hasattr(occurred_at, "hour"):
        occurred_at = datetime.combine(occurred_at, datetime.min.time(), tzinfo=UTC)

      amount = int(txn["amount"]) if txn.get("amount") is not None else None
      description = txn.get("description") or txn.get("memo")
      if description is not None:
        description = str(description)

      due_date_iso = None
      if txn.get("due_date") and hasattr(txn["due_date"], "isoformat"):
        due_date_iso = txn["due_date"].isoformat()

      qb_txn_type = str(txn.get("type")) if txn.get("type") else None
      qb_doc_number = str(txn.get("number")) if txn.get("number") else None
      occurred_date_iso = (
        occurred_at.date().isoformat() if hasattr(occurred_at, "date") else None
      )

      # Source-class fidelity + agent linkage.
      # The transactions mart carries event_type / event_category and
      # the optional agent_external_id from the per-class header join.
      event_type = str(txn.get("event_type") or "journal_entry_recorded")
      event_category = str(txn.get("event_category") or "adjustment")
      # Canonical action verb. Nullable — the dbt mart falls back to
      # NULL for unmapped QB tx_types, and the DB CHECK accepts NULL.
      event_action_raw = txn.get("event_action")
      event_action: str | None = str(event_action_raw) if event_action_raw else None
      agent_ext_id_raw = txn.get("agent_external_id")
      agent_ext_id = str(agent_ext_id_raw).strip() if agent_ext_id_raw else ""
      agent_id: str | None = None
      if agent_ext_id and agent_lookup:
        agent_id = agent_lookup.get(agent_ext_id)
        if agent_id is None:
          # Agent referenced by a transaction header but not found in the
          # agents UPSERT. Possible if the customer/vendor was soft-deleted
          # in QB. Capture the event with agent_id=NULL — better than dropping.
          logger.warning(
            "QB event %s references agent %s but no Agent record was UPSERTed; "
            "capturing event with agent_id=NULL",
            ext_id,
            agent_ext_id,
          )

      # Build entries with handler-friendly defaults; drop unbalanced ones.
      hardened_entries: list[dict] = []
      for e in txn["entries"]:
        line_items = e.get("line_items") or []
        if len(line_items) < 2:
          # min_length=2 on handler schema — a single-line entry can't
          # balance. Drop with a counter so the sync log shows what was
          # eaten.
          out.dropped_unbalanced_entries += 1
          continue

        entry_ext_id = str(e["external_id"])
        memo_raw = e.get("memo")
        memo = (
          str(memo_raw)
          if memo_raw
          else (qb_doc_number or f"QB {qb_txn_type or 'transaction'} {ext_id}")
        )

        posting_date_raw = e.get("posting_date")
        if posting_date_raw and hasattr(posting_date_raw, "isoformat"):
          posting_date_iso = posting_date_raw.isoformat()
        else:
          # Fallback to the transaction's occurred_at date — better than
          # NULL for handler validation; QB always populates TxnDate at
          # transaction level, so this is a real fallback.
          posting_date_iso = occurred_date_iso

        hardened_entries.append(
          {
            "external_id": entry_ext_id,
            "type": str(e.get("type", "standard")),
            "posting_date": posting_date_iso,
            "number": str(e.get("number")) if e.get("number") else None,
            "memo": memo,
            "line_items": line_items,
          }
        )

      if not hardened_entries:
        # Every entry in this transaction was unbalanced. Skip the whole
        # event — capturing it would just produce an unapprovable inbox row.
        out.dropped_empty_transactions += 1
        continue

      # Auto-committing sources are by definition source-of-truth — the
      # transaction was already posted in the upstream system. Stamp
      # ``status='posted'`` so the journal_entry_recorded handler creates
      # Entry rows with ``status='posted'`` (vs. the default 'draft').
      # Trial balance / reports filter on posted entries; without this,
      # QB-synced data would silently disappear from the read side.
      jeh_status = "posted" if _source_auto_commits_on_sync(source) else "draft"

      # ``metadata_blob["status"]`` is the JOURNAL-ENTRY status consumed
      # by the journal_entry_recorded handler's metadata_schema (see
      # python_handlers/journal_entry_recorded.py: ``status: Literal['draft',
      # 'posted']``). It is *not* the Event.status field — the Event row's
      # own ``status`` column lives on the Event ORM object and uses a
      # different domain (captured / classified / committed / voided /
      # fulfilled). The two field names overlap because both are coupled
      # to the handler's wire contract.
      # qb_linked_txns is a list of {txn_id, txn_type} refs naming the
      # invoices/bills this payment settles (Payment/BillPayment only;
      # other types pass through an empty list). dbt emits this as a
      # JSON string; parse to native list so the metadata blob carries
      # a structured shape the payment_received / bill_paid handlers
      # can walk without re-parsing on every dispatch.
      linked_txns_raw = txn.get("linked_txns")
      qb_linked_txns: list[dict[str, str]] = []
      if linked_txns_raw:
        try:
          parsed = json.loads(linked_txns_raw)
          if isinstance(parsed, list):
            qb_linked_txns = [
              {
                "txn_id": str(r.get("txn_id", "")),
                "txn_type": str(r.get("txn_type", "")),
              }
              for r in parsed
              if isinstance(r, dict) and r.get("txn_id") and r.get("txn_type")
            ]
        except (ValueError, TypeError) as e:
          logger.warning(
            "Failed to parse linked_txns for QB event %s: %s — proceeding with empty list",
            ext_id,
            e,
          )

      # Capture QB SyncToken (monotonic per-entity version) so the
      # SyncToken-gated UPSERT can decide freshness. NULL for
      # JournalReport-only rows (JournalEntry / Deposit / Transfer where we
      # don't yet fetch a header) — those backfill on next sync that fetches
      # the entity. Excluded from drift comparison below: a SyncToken bump
      # without any other payload change is a no-op, not drift.
      qb_sync_token_raw = txn.get("sync_token")
      qb_sync_token = str(qb_sync_token_raw) if qb_sync_token_raw else None
      metadata_blob = {
        "status": jeh_status,
        "qb_txn_type": qb_txn_type,
        "qb_doc_number": qb_doc_number,
        "qb_reference_number": str(txn.get("reference_number"))
        if txn.get("reference_number")
        else None,
        "qb_merchant_name": str(txn.get("merchant_name"))
        if txn.get("merchant_name")
        else None,
        "qb_due_date": due_date_iso,
        "qb_category": str(txn.get("category")) if txn.get("category") else None,
        "qb_source_class": qb_txn_type,
        "qb_agent_external_id": agent_ext_id or None,
        "qb_linked_txns": qb_linked_txns,
        "qb_sync_token": qb_sync_token,
        "connection_id": connection_id,
        "entries": hardened_entries,
      }

      if ext_id in existing:
        evt = existing[ext_id]
        # SyncToken freshness gate. Runs BEFORE the
        # status branches: a stale or same-version incoming row skips
        # all further processing (no UPSERT, no drift check, no handler
        # re-fire). The gate is the primary defense against
        # out-of-order CDC delivery and replayed batches; status
        # branching below only runs when the gate decides "fresh" or
        # "no_info".
        existing_token = (evt.metadata_ or {}).get("qb_sync_token")
        freshness = _compare_sync_tokens(existing_token, qb_sync_token)
        if freshness == "stale":
          logger.info(
            "QB SyncToken stale for event ext_id=%s: live=%s, incoming=%s "
            "— skipping (out-of-order replay / webhook race expected; "
            "not an error)",
            ext_id,
            existing_token,
            qb_sync_token,
          )
          out.skipped_stale_sync_token += 1
          continue
        if freshness == "same":
          # Idempotent re-sync of an already-current row — no work to do.
          out.skipped_same_sync_token += 1
          continue
        # freshness in ("fresh", "no_info") → fall through to status branching.

        # Don't overwrite handler-approved or rejected events on re-sync.
        if evt.status in ("captured", "classified"):
          evt.event_type = event_type
          evt.event_category = event_category
          evt.event_action = event_action
          evt.agent_id = agent_id
          evt.occurred_at = occurred_at
          evt.amount = amount
          evt.currency = txn.get("currency", "USD")
          evt.description = description
          # Preserve loader bookkeeping keys (dispatch_* counters from
          # prior failed-dispatch attempts) across UPSERT. The
          # adapter payload supersedes everything else, but counters
          # that track "this has failed N times" must accumulate so the
          # inbox UI can surface "this is stuck — needs attention"
          # remediation prompts.
          preserved = {
            k: v for k, v in (evt.metadata_ or {}).items() if k.startswith("dispatch_")
          }
          evt.metadata_ = {**metadata_blob, **preserved}
          out.updated += 1
        elif evt.status in ("committed", "fulfilled"):
          # Adapter resurfaced a payload for an
          # already-approved entry. Compare incoming `metadata_blob`
          # against the live `evt.metadata_` minus drift bookkeeping.
          # If different, flag drift + stash the incoming payload
          # without mutating the live business payload (handler-approved
          # entries are immutable to re-sync).
          #
          # Exclude `qb_sync_token` from the diff — a SyncToken
          # bump alone is not drift, just a version increment. But DO
          # persist the bumped SyncToken to the live event's metadata
          # (bookkeeping field, not business payload) so the next
          # sync's gate comparison stays accurate. Without this update
          # the gate would re-fire indefinitely with the same comparison.
          # `connection_id` is excluded for the same reason: it names the
          # connection that synced the row, not what happened upstream.
          # A reconnect that mints a new connection id must not read as
          # drift on every committed row in the window.
          _drift_excluded = (
            "drift_payload",
            "drift_detected_at",
            "qb_sync_token",
            "connection_id",
          )
          live_payload = {
            k: v for k, v in (evt.metadata_ or {}).items() if k not in _drift_excluded
          }
          incoming_payload = {
            k: v for k, v in metadata_blob.items() if k not in _drift_excluded
          }
          new_meta = dict(evt.metadata_ or {})
          meta_changed = False
          if live_payload != incoming_payload:
            evt.payload_drift = True
            # Mutate as a new dict so SQLAlchemy detects the JSONB
            # change (in-place mutation of mutable columns is unsafe
            # without a MutableDict wrapper).
            new_meta["drift_payload"] = metadata_blob
            new_meta["drift_detected_at"] = now.isoformat()
            out.drift_detected += 1
            meta_changed = True
          # Advance the live SyncToken regardless of drift outcome —
          # the gate above only proceeded because incoming is fresh
          # (or no_info, in which case incoming==None and the .get()
          # below leaves the live token alone).
          if (
            qb_sync_token is not None and new_meta.get("qb_sync_token") != qb_sync_token
          ):
            new_meta["qb_sync_token"] = qb_sync_token
            meta_changed = True
          # Refresh `connection_id` the same way — write-back routing
          # reads it off the event, so it must name the *live* connection
          # after a reconnect, not the one that originally synced the row.
          if (
            connection_id is not None and new_meta.get("connection_id") != connection_id
          ):
            new_meta["connection_id"] = connection_id
            meta_changed = True
          if meta_changed:
            evt.metadata_ = new_meta
        # else: leave it alone — handler already ran or user voided it
      else:
        new_events.append(
          Event(
            event_type=event_type,
            event_category=event_category,
            event_class="economic",
            event_action=event_action,
            agent_id=agent_id,
            occurred_at=occurred_at,
            status="captured",
            source=source,
            external_id=ext_id,
            amount=amount,
            currency=txn.get("currency", "USD"),
            description=description,
            metadata_=metadata_blob,
            created_at=now,
            created_by=created_by,
          )
        )
        out.inserted += 1

    if new_events:
      session.add_all(new_events)
    session.flush()

    # Auto-commit pass for source-of-truth sources.
    #
    # On a fresh sync of QB data, every captured event has already been
    # approved in QB — making the user click "approve" again on the inbox
    # would be busywork. Fire each event's registered Python handler now;
    # successful dispatch transitions the event to ``committed`` with the
    # GL rows the handler creates. Each event is dispatched in its own
    # SAVEPOINT so a single bad event (e.g., references an element_id
    # the loader hasn't seen yet) doesn't roll back the rest.
    #
    # Events that fail dispatch stay at ``status='captured'`` and surface
    # in the inbox — the user fixes the underlying issue (CoA mapping,
    # closed period, etc.) and re-approves from there. Re-syncs covered
    # under the existing idempotency rule: only ``captured`` /
    # ``classified`` events are touched, so previously-committed events
    # don't get re-fired.
    if _source_auto_commits_on_sync(source):
      events_to_commit: list[Event] = list(new_events)
      # Re-include updated captured events — covers two cases:
      # (1) old captured events left over from before auto-commit was
      # enabled and (2) a re-sync that updated metadata of an event that
      # previously failed dispatch. ``existing.values()`` and ``new_events``
      # are guaranteed disjoint by construction (existing rows are mutated
      # in place, never appended to ``new_events``), so no dedup needed.
      for evt in existing.values():
        if evt.status == "captured":
          events_to_commit.append(evt)

      for evt in events_to_commit:
        try:
          with session.begin_nested():
            prev_status = evt.status
            fire_handler_on_commit(session, evt, created_by)
            # If the handler bumped status to a terminal state ('fulfilled'
            # for historical-posted data), respect that. Only stamp
            # 'committed' when the handler left status unchanged.
            if evt.status == prev_status:
              evt.status = "committed"
          out.handler_dispatched += 1
        except Exception as e:
          # Stamp typed error metadata on the event so the
          # operator inbox can render "fix and retry" prompts. The
          # metadata write happens OUTSIDE the SAVEPOINT — the nested
          # transaction's rollback wipes any in-flight handler mutations
          # but the parent session is still live, so this assignment
          # commits with the outer transaction.
          _stamp_dispatch_error(evt, e, now)
          logger.warning(
            "Auto-commit failed for event %s (type=%s, ext_id=%s): %s — "
            "event left at status='captured' with dispatch_error stamped "
            "for inbox review (attempt %d)",
            evt.id,
            evt.event_type,
            evt.external_id,
            e,
            (evt.metadata_ or {}).get("dispatch_attempts", 1),
          )
          out.dispatch_failed += 1

      session.flush()

    return out

  def _ensure_mapping_structure(
    self,
    graph_id: str,
    source: str,
    created_by: str,
  ) -> None:
    """Ensure a CoA taxonomy, CoA→GAAP mapping structure, and entity
    adoption of the CoA taxonomy all exist.

    These are prerequisites for the report generation flow. Without them,
    the Chart of Accounts page won't show the GAAP mapping column or
    Auto-Map button, and reports can't be generated. The entity→CoA
    adoption row is also what materializes the ENTITY_HAS_TAXONOMY graph
    edge — without it the graph can't answer "what chart of accounts does
    this entity report under?" via a direct traversal.

    Idempotent — skips creation of each piece if it already exists.
    """
    from robosystems.db.extensions import extensions_session
    from robosystems.models.extensions import Element, EntityTaxonomy
    from robosystems.models.extensions.entity import Entity
    from robosystems.models.extensions.roboledger import Structure, Taxonomy
    from robosystems.utils.ulid import generate_prefixed_ulid

    try:
      with extensions_session(graph_id) as session:
        # Check if a CoA taxonomy already exists
        existing_coa = (
          session.query(Taxonomy)
          .filter(
            Taxonomy.taxonomy_type == "chart_of_accounts", Taxonomy.is_active.is_(True)
          )
          .first()
        )

        if not existing_coa:
          source_label = source.replace("_", " ").title()
          existing_coa = Taxonomy(
            id=generate_prefixed_ulid("tax"),
            name=f"{source_label} Chart of Accounts",
            taxonomy_type="chart_of_accounts",
            is_active=True,
            created_by=created_by,
          )
          session.add(existing_coa)
          session.flush()
          logger.info(f"Created CoA taxonomy for {graph_id}: {existing_coa.id}")

        # Link the source's synced accounts to the CoA taxonomy. The element
        # load runs before this taxonomy exists, so synced accounts land with
        # taxonomy_id=NULL — and then never surface under the taxonomy in the
        # Library UI (which filters by taxonomy_id). Backfill here so they do.
        # This matches the manual create-element path (which sets taxonomy_id
        # directly); without it the sync path diverges from the manual one.
        # Scoped to NULL so re-syncs leave already-linked rows untouched.
        relinked = (
          session.query(Element)
          .filter(
            Element.external_source == source,
            Element.taxonomy_id.is_(None),
          )
          .update({Element.taxonomy_id: existing_coa.id}, synchronize_session=False)
        )
        if relinked:
          session.flush()
          logger.info(
            f"Linked {relinked} {source} CoA element(s) → taxonomy "
            f"{existing_coa.id} for {graph_id}"
          )

        # Ensure the graph's entity is linked to the CoA taxonomy as its
        # primary chart_of_accounts basis. This materializes to
        # ENTITY_HAS_TAXONOMY in the graph.
        entity = session.query(Entity).first()
        if entity:
          existing_adoption = (
            session.query(EntityTaxonomy)
            .filter(
              EntityTaxonomy.entity_id == entity.id,
              EntityTaxonomy.taxonomy_id == existing_coa.id,
              EntityTaxonomy.basis == "chart_of_accounts",
            )
            .first()
          )

          if not existing_adoption:
            adoption = EntityTaxonomy(
              entity_id=entity.id,
              taxonomy_id=existing_coa.id,
              is_primary=True,
              basis="chart_of_accounts",
              adoption_context="voluntary",
            )
            session.add(adoption)
            session.flush()
            logger.info(
              f"Linked entity {entity.id} → CoA taxonomy {existing_coa.id} "
              f"(basis=chart_of_accounts, primary=true)"
            )
        else:
          logger.warning(
            f"No entity found in graph {graph_id}, skipping EntityTaxonomy adoption"
          )

        # Check if a mapping structure already exists
        existing_mapping = (
          session.query(Structure)
          .filter(Structure.block_type == "coa_mapping", Structure.is_active.is_(True))
          .first()
        )

        if not existing_mapping:
          mapping_structure = Structure(
            id=generate_prefixed_ulid("struct"),
            name="CoA to US GAAP Mapping",
            description="Maps Chart of Accounts to US GAAP reporting concepts",
            block_type="coa_mapping",
            taxonomy_id=existing_coa.id,
            is_active=True,
            created_by=created_by,
          )
          session.add(mapping_structure)
          session.flush()
          logger.info(
            f"Created mapping structure for {graph_id}: {mapping_structure.id}"
          )

    except Exception as e:
      logger.warning(f"Failed to ensure mapping structure for {graph_id}: {e}")

  def _update_entity_from_company_info(
    self,
    graph_id: str,
    source: str,
    connection_id: str,
    duckdb_path: str | Path,
  ) -> None:
    """Update the entity row with CompanyInfo from the connector.

    Reads from the dbt staging table (stg_qb_company_info or similar)
    and updates the existing entity in the extensions OLTP schema.
    Only updates if entity exists and company info is available.
    """
    import duckdb

    from robosystems.db.extensions import extensions_session
    from robosystems.models.extensions.entity import Entity

    try:
      con = duckdb.connect(str(duckdb_path), read_only=True)
      try:
        tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}

        # Look for company info staging table
        company_table = None
        for candidate in [
          "stg_qb_company_info",
          "stg_xero_organisation",
          "company_info",
        ]:
          if candidate in tables:
            company_table = candidate
            break

        if not company_table:
          logger.debug(
            "No company info table found in dbt output, skipping entity update"
          )
          return

        result_set = con.execute(f"SELECT * FROM {company_table} LIMIT 1")
        columns = [desc[0] for desc in result_set.description]
        row = result_set.fetchone()
        if not row:
          return
        company = dict(zip(columns, row, strict=True))
      finally:
        con.close()

      with extensions_session(graph_id) as session:
        entity = session.query(Entity).first()
        if not entity:
          logger.warning(
            f"No entity found in graph {graph_id}, skipping CompanyInfo update"
          )
          return

        # Update entity fields from company info
        entity.name = company.get("company_name") or entity.name
        entity.legal_name = company.get("legal_name") or entity.legal_name
        entity.address_line1 = company.get("address_line1")
        entity.address_city = company.get("city")
        entity.address_state = company.get("state")
        entity.address_postal_code = company.get("postal_code")
        entity.address_country = company.get("country", "US")
        # Reporting metadata — fall back to existing values rather than
        # overwriting with NULL if the adapter doesn't provide them.
        # (QB CompanyInfo doesn't include tax_id / industry / entity_type
        # directly; those fields stay user-curated until populated
        # through a separate path.)
        if company.get("phone"):
          entity.phone = company.get("phone")
        if company.get("website"):
          entity.website = company.get("website")
        if company.get("fiscal_year_end"):
          entity.fiscal_year_end = company.get("fiscal_year_end")
        entity.source = source
        entity.connection_id = connection_id
        entity.source_id = company.get("id", "")
        entity.updated_at = datetime.now(UTC)
        session.commit()

        logger.info(
          f"Updated entity for graph {graph_id} with {source} CompanyInfo: "
          f"name='{entity.name}'"
        )

    except Exception as e:
      logger.warning(f"Could not update entity from CompanyInfo: {e}")
