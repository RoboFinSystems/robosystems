"""Generic OLTP loader: reads dbt output from DuckDB and inserts into PostgreSQL.

Connector-agnostic; all source-specific transformation happens in dbt staging.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from robosystems.logger import logger
from robosystems.operations.event_block.commands import fire_handler_on_commit
from robosystems.operations.locking import ordered_lock_column

# Sources whose inbound events commit to GL on sync instead of landing in
# the inbox. A property of the source, not of write_policy (which governs
# write-back only): QB rows are already booked upstream. Flip a new source
# to True only once its handlers are verified end to end.
_SOURCE_AUTO_COMMITS: dict[str, bool] = {
  "quickbooks": True,
}


def _source_auto_commits_on_sync(source: str) -> bool:
  return _SOURCE_AUTO_COMMITS.get(source, False)


# QuickBooks AccountType → FASB elementsOfFinancialStatements trait. Without
# it QB elements have no trait and the auto-map operator skips them. COGS
# rolls into expense; the COGS/opex split lives under ``activityType``.
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


# QuickBooks AccountType → FASB ``liquidity`` trait. Only assets and
# liabilities carry one. Advisory: it narrows auto-map candidates; statement
# classification stays structural.
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


# QuickBooks AccountSubTypes that denote a contra-asset. QB files these under
# an asset AccountType, so the sub-type is its only signal; they load as the
# ``contraAsset`` trait rather than ``asset``.
_QB_CONTRA_ASSET_SUB_TYPES: frozenset[str] = frozenset(
  {
    "AccumulatedDepreciation",
    "AccumulatedAmortization",
    "AccumulatedDepletion",
    "AccumulatedAmortizationOfOtherAssets",
    "AllowanceForBadDebts",
  }
)


# Catch-all for new QB variants (e.g. AccumulatedDepreciationEquipment). Not a
# bare "Accumulated" prefix: AccumulatedOtherComprehensiveIncome and
# AccumulatedAdjustment are equity sub-types.
_QB_CONTRA_ASSET_SUB_TYPE_PREFIXES: tuple[str, ...] = (
  "AccumulatedDepreciation",
  "AccumulatedAmortization",
  "AccumulatedDepletion",
)


def _is_qb_contra_asset_sub_type(sub_type: str | None) -> bool:
  if not sub_type:
    return False
  return sub_type in _QB_CONTRA_ASSET_SUB_TYPES or sub_type.startswith(
    _QB_CONTRA_ASSET_SUB_TYPE_PREFIXES
  )


# Fallback when the sub-type is uninformative: QB detail types are user-chosen,
# so an accumulated-amortization account can carry a generic one. Only checked
# for asset-typed accounts, so equity names like AOCI never match.
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
  """FASB EFS trait for a source account; None when account_type is missing or unknown."""
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


# Must match the fallback derivation in materialize.py's Element projection,
# used for rows stored without a qname.
_ADAPTER_QNAME_PREFIXES: dict[str, str] = {
  "quickbooks": "qb",
  "xero": "xero",
  "plaid": "plaid",
}


def _derive_adapter_qname(external_source: str | None, code: str | None) -> str | None:
  """``qb:Intangible Assets:Accumulated Amortization``-style qname.

  The fully-qualified account code is unique per book. None for unknown
  sources or blank codes — never guess an identity.
  """
  prefix = _ADAPTER_QNAME_PREFIXES.get(str(external_source or "").lower())
  if not prefix or not code:
    return None
  return f"{prefix}:{code}"


# Balance-sheet classifications are instants; everything else is a duration.
# Mirrors ``_INSTANT_CLASSIFICATIONS`` in roboledger/commands/elements.py, kept
# local so the sync path doesn't import the command layer.
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
  """Map a dispatch exception to the error code the inbox UI keys its retry prompts on.

  ``element_unmapped`` means the account is missing from the chart of
  accounts, not a reporting-concept mapping gap.
  """
  exc_type = type(exc).__name__
  # Matched by name so this module need not import the handler types.
  if exc_type == "ElementResolutionError":
    return "element_unmapped"
  if exc_type == "ClosedPeriodError":
    return "closed_period"
  if exc_type == "UnbalancedJournalEntryError":
    return "unbalanced_entry"
  return "unknown_error"


# Bookkeeping keys ignored when checking whether an upstream row changed
# after posting. ``connection_id`` would flag every row on a reconnect;
# ``reconciliation_history`` must be ignored or a resolved item re-flags.
DRIFT_EXCLUDED_KEYS: frozenset[str] = frozenset(
  {
    "drift_payload",
    "drift_detected_at",
    "qb_sync_token",
    "connection_id",
    "reconciliation_history",
  }
)

# ``dispatch_*`` counters survive UPSERT and the adapter never sends them, so
# they would otherwise differ from every incoming payload.
DRIFT_EXCLUDED_PREFIXES: tuple[str, ...] = ("dispatch_",)


def comparable_payload(metadata: dict | None) -> dict:
  """Strip bookkeeping keys, leaving the payload the drift check compares.

  Shared with ``resolve_reconciling_item`` so a resolved item's accepted
  payload compares equal to the next sync.
  """
  return {
    k: v
    for k, v in (metadata or {}).items()
    if k not in DRIFT_EXCLUDED_KEYS and not k.startswith(DRIFT_EXCLUDED_PREFIXES)
  }


def _compare_sync_tokens(existing: str | None, incoming: str | None) -> str:
  """SyncToken freshness for the UPSERT gate: fresh, same, stale, or no_info.

  ``no_info`` (a side missing or non-numeric) proceeds ungated. QB SyncTokens
  are monotonic ints serialized as strings.
  """
  if existing is None or incoming is None:
    return "no_info"
  try:
    incoming_int = int(incoming)
    existing_int = int(existing)
  except (ValueError, TypeError):
    return "no_info"
  if incoming_int > existing_int:
    return "fresh"
  if incoming_int == existing_int:
    return "same"
  return "stale"


def _stamp_dispatch_error(evt, exc: BaseException, now: datetime) -> None:
  """Record dispatch failure detail beside the payload and bump ``dispatch_attempts``.

  Reassigns ``metadata_`` so SQLAlchemy detects the JSONB change.
  """
  meta = dict(evt.metadata_ or {})
  meta["dispatch_error"] = _classify_dispatch_error(exc)
  meta["dispatch_error_message"] = str(exc)[:500]
  meta["dispatch_error_at"] = now.isoformat()
  meta["dispatch_attempts"] = int(meta.get("dispatch_attempts", 0)) + 1
  evt.metadata_ = meta


@dataclass
class LoadResult:
  """Counts from an OLTP load.

  ``events_handler_dispatched`` counts both ``committed`` and ``fulfilled``
  outcomes; a failed dispatch leaves the event ``captured``.
  ``transactions`` / ``entries`` / ``line_items`` come from handler dispatch,
  not direct insert.
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
  # Upstream payload changed under a committed/fulfilled row; flagged for
  # reconciliation, live payload untouched.
  events_drift_detected: int = 0
  # QB rows recognized as a round-trip of an event we wrote back.
  events_cross_source_matched: int = 0
  events_skipped_stale_sync_token: int = 0
  events_skipped_same_sync_token: int = 0
  # Rows the loader dropped as malformed, surfaced so a lossy sync is visible.
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
  """Counters from ``_capture_transactions_as_events``, copied onto ``LoadResult``."""

  inserted: int = 0
  updated: int = 0
  handler_dispatched: int = 0
  dispatch_failed: int = 0
  drift_detected: int = 0
  cross_source_matched: int = 0
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
  """Loads dbt OLTP output tables into a graph's extensions tenant schema."""

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
    """Load dbt OLTP output into the extensions tenant schema, in one transaction.

    ``full_rebuild=True`` first wipes ``captured``/``classified`` events for
    ``(source, connection_id)`` (voided/committed/fulfilled survive), then
    UPSERTs. Otherwise the load is incremental: rows are UPSERTed in place and
    committed/fulfilled events are drift-flagged if the payload changed.
    ``since_date`` is advisory here (the dbt mart is already window-scoped) and
    never enables the wipe.
    """
    import duckdb

    from robosystems.db.extensions import ensure_tenant_schema, extensions_session
    from robosystems.models.extensions import (
      Dimension,
      Element,
    )
    from robosystems.utils.ulid import generate_prefixed_ulid

    result = LoadResult(graph_id=graph_id, source=source, connection_id=connection_id)

    # First-time provisioning only. Re-running the DDL on every sync would take
    # AccessExclusive locks on every tenant table against live readers.
    ensure_tenant_schema(graph_id)

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

    with extensions_session(graph_id, statement_timeout_ms=None) as session:
      # Pre-sync deletes, full_rebuild only. Voided/committed/fulfilled events
      # are immutable to re-sync; the GL cascade is scoped through
      # ``triggered_by_event_id`` so their GL rows survive.
      #
      # Lazy imports keep the loader cheap to import: the roboledger models
      # drag in the whole extensions schema.
      from robosystems.models.extensions.element_trait import ElementTrait
      from robosystems.models.extensions.roboledger.entry import Entry
      from robosystems.models.extensions.roboledger.event import Event
      from robosystems.models.extensions.roboledger.line_item import LineItem
      from robosystems.models.extensions.roboledger.transaction import Transaction

      if full_rebuild and _source_auto_commits_on_sync(source):
        events_to_wipe_subq = session.query(Event.id).filter(
          Event.source == source,
          Event.metadata_["connection_id"].astext == connection_id,
          Event.status.in_(("captured", "classified")),
        )
        entry_subq = session.query(Entry.id).filter(
          Entry.triggered_by_event_id.in_(events_to_wipe_subq)
        )
        # Fence before the deletes take row locks, the order every ledger
        # writer keeps against close. A refusal means a close is in flight;
        # this batch fails and the next tick retries.
        from robosystems.operations.roboledger.commands._guards import (
          assert_period_not_closed,
        )

        wipe_dates = (
          session.query(Entry.posting_date)
          .filter(Entry.triggered_by_event_id.in_(events_to_wipe_subq))
          .distinct()
          .all()
        )
        assert_period_not_closed(session, *(d for (d,) in wipe_dates))
        session.query(LineItem).filter(LineItem.entry_id.in_(entry_subq)).delete(
          synchronize_session=False
        )
        session.query(Entry).filter(
          Entry.triggered_by_event_id.in_(events_to_wipe_subq)
        ).delete(synchronize_session=False)
        session.query(Transaction).filter(
          Transaction.triggered_by_event_id.in_(events_to_wipe_subq)
        ).delete(synchronize_session=False)
        # connection_id lives in metadata_; filtering on it spares sibling
        # connections on the same graph.
        session.query(Event).filter(
          Event.source == source,
          Event.metadata_["connection_id"].astext == connection_id,
          Event.status.in_(("captured", "classified")),
        ).delete(synchronize_session=False)
        session.flush()

      # Structural reset on full_rebuild. Adapter-derived traits are re-derived
      # on upsert. Associations are deliberately kept: user-curated mappings
      # survive, and the element UPSERT preserves the elem_* ids they point at.
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

      # UPSERT elements, preserving elem_* ids so Associations and IB facts
      # keep their FK targets. Backed by the partial unique index
      # ``idx_elements_upsert_key``.
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

        # Adapter elements carry a derived ``{prefix}:{code}`` qname so they are
        # addressable like library elements; re-derived on every sync so a
        # source-side code change heals.
        desired_qnames: dict[str, str] = {}
        for row in rows:
          derived = _derive_adapter_qname(
            str(row.get("external_source") or source), str(row["code"])
          )
          if derived:
            desired_qnames[str(row["external_id"])] = derived

        # qname is unique schema-wide but the derivation only book-unique;
        # on a collision, warn and leave the qname unset rather than steal it.
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

        # Second pass: resolve parent_id once every element has an id.
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

      # TODO: populate the line_item_dimensions junction table (needed for
      # graph materialization).
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

    self._update_entity_from_company_info(
      graph_id=graph_id,
      source=source,
      connection_id=connection_id,
      duckdb_path=duckdb_path,
    )

    # Report generation requires the CoA taxonomy and mapping structure.
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
    """Derive EFS and liquidity traits for the source's elements; returns rows written.

    Idempotent. An adapter-written EFS trait that no longer matches the
    derivation is replaced; a trait with any other provenance is a deliberate
    override and is left alone. QuickBooks only for now.
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

    # EFS is required; liquidity is optional and its absence skips nothing.
    efs_id_by_identifier = _resolve_trait_ids(
      "elementsOfFinancialStatements",
      # contraAsset is chosen per element, never by the account_type map.
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

    elements = (
      session.query(Element)
      .filter(
        Element.external_source == source,
        Element.connection_id == connection_id,
      )
      .all()
    )

    # Existing EFS rows, so a stale adapter classification can be replaced.
    # Joined on category so a manual override to any EFS trait is seen too.
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
      efs_identifier = _account_efs_identifier(meta, str(elem.name))
      if efs_identifier is None:
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

    # ON CONFLICT DO NOTHING on the (element_id, trait_id) key: elements are
    # upserted, so a re-sync re-derives traits that already exist.
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
    """UPSERT agents from the dbt agents mart, keyed per connection.

    Two connections on the same graph never share agents.
    """
    from robosystems.models.extensions.roboledger import Agent
    from robosystems.utils.ulid import generate_prefixed_ulid

    out = _AgentCaptureResult()
    rows = dbt_data.get("agents") or []
    if not rows:
      return out

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
    """Capture each dbt-staged transaction as one Event, entries packed into metadata.

    Idempotent on ``(source, external_id)``: captured/classified events are
    updated in place; committed/fulfilled/voided ones are never rewritten.
    For auto-committing sources the handler fires immediately; a failed
    dispatch leaves the event ``captured`` for the inbox.

    The metadata must satisfy the ``journal_entry_recorded`` handler schema,
    so missing memos and posting dates are defaulted, entries with fewer than
    two non-zero lines are dropped, and so are transactions left with none.
    """
    from robosystems.models.extensions.roboledger import Event

    txns_by_ext: dict[str, dict] = {}
    for row in dbt_data.get("transactions", []) or []:
      ext_id = str(row["external_id"])
      txns_by_ext[ext_id] = {**row, "entries": []}

    entries_by_ext: dict[str, dict] = {}
    for row in dbt_data.get("entries", []) or []:
      ent_ext_id = str(row["external_id"])
      txn_ext_id = str(row.get("external_transaction_id", ent_ext_id))
      if txn_ext_id not in txns_by_ext:
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

    # Locked: the auto-commit pass dispatches handlers for these rows, and an
    # inbox approval committing in between would fire the handler twice (two
    # sets of GL rows). An unchanged re-sync issues no UPDATE, so the UPSERT
    # alone would not lock. No lock_timeout: a background sync should wait;
    # the approval side bounds its wait.
    existing: dict[str, Event] = {
      e.external_id: e
      for e in session.query(Event)
      .filter(
        Event.source == source,
        Event.external_id.in_(list(txns_by_ext.keys())),
      )
      # Ordered like every other batch lock (`locking.ordered_lock_column`).
      .order_by(ordered_lock_column())
      .with_for_update()
      .all()
    }

    # Cross-source matcher: an incoming QB id named in a native event's
    # comma-joined `metadata.qb_external_id` is a round-trip of our own
    # write-back. Stamp confirmation and skip the insert and handler re-fire.
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

      event_type = str(txn.get("event_type") or "journal_entry_recorded")
      event_category = str(txn.get("event_category") or "adjustment")
      # NULL for QB types the dbt mart does not map.
      event_action_raw = txn.get("event_action")
      event_action: str | None = str(event_action_raw) if event_action_raw else None
      agent_ext_id_raw = txn.get("agent_external_id")
      agent_ext_id = str(agent_ext_id_raw).strip() if agent_ext_id_raw else ""
      agent_id: str | None = None
      if agent_ext_id and agent_lookup:
        agent_id = agent_lookup.get(agent_ext_id)
        if agent_id is None:
          # e.g. soft-deleted in QB; keep the event rather than drop it.
          logger.warning(
            "QB event %s references agent %s but no Agent record was UPSERTed; "
            "capturing event with agent_id=NULL",
            ext_id,
            agent_ext_id,
          )

      hardened_entries: list[dict] = []
      for e in txn["entries"]:
        line_items = e.get("line_items") or []
        if len(line_items) < 2:
          # The handler schema requires two lines; one cannot balance.
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
        out.dropped_empty_transactions += 1
        continue

      # Already posted upstream, so entries land ``posted``; reports read only
      # posted entries. This is the journal entry's status in the handler's
      # metadata schema, not ``Event.status``.
      jeh_status = "posted" if _source_auto_commits_on_sync(source) else "draft"

      # The invoices/bills a Payment/BillPayment settles; dbt emits JSON text.
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
        # Freshness gate first: a stale or same-version row skips everything,
        # guarding against out-of-order CDC delivery and replayed batches.
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
          out.skipped_same_sync_token += 1
          continue

        if evt.status in ("captured", "classified"):
          evt.event_type = event_type
          evt.event_category = event_category
          evt.event_action = event_action
          evt.agent_id = agent_id
          evt.occurred_at = occurred_at
          evt.amount = amount
          evt.currency = txn.get("currency", "USD")
          evt.description = description
          # dispatch_* failure counters must accumulate across re-syncs.
          preserved = {
            k: v for k, v in (evt.metadata_ or {}).items() if k.startswith("dispatch_")
          }
          evt.metadata_ = {**metadata_blob, **preserved}
          out.updated += 1
        elif evt.status in ("committed", "fulfilled"):
          # Approved entries are immutable to re-sync: a changed payload is
          # flagged as drift and stashed, never applied.
          live_payload = comparable_payload(evt.metadata_)
          incoming_payload = comparable_payload(metadata_blob)
          new_meta = dict(evt.metadata_ or {})
          meta_changed = False
          if live_payload != incoming_payload:
            evt.payload_drift = True
            new_meta["drift_payload"] = metadata_blob
            new_meta["drift_detected_at"] = now.isoformat()
            out.drift_detected += 1
            meta_changed = True
          # Bookkeeping, not payload: advance the SyncToken so the next
          # gate comparison is accurate.
          if (
            qb_sync_token is not None and new_meta.get("qb_sync_token") != qb_sync_token
          ):
            new_meta["qb_sync_token"] = qb_sync_token
            meta_changed = True
          # Write-back routing reads connection_id, so it must name the live
          # connection after a reconnect.
          if (
            connection_id is not None and new_meta.get("connection_id") != connection_id
          ):
            new_meta["connection_id"] = connection_id
            meta_changed = True
          if meta_changed:
            evt.metadata_ = new_meta
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

    # Auto-commit pass. Each event dispatches in its own SAVEPOINT so one bad
    # event doesn't roll back the rest; a failure stays ``captured`` for the
    # inbox.
    if _source_auto_commits_on_sync(source):
      events_to_commit: list[Event] = list(new_events)
      # Retry existing captured events too (e.g. a previous failed dispatch).
      for evt in existing.values():
        if evt.status == "captured":
          events_to_commit.append(evt)

      for evt in events_to_commit:
        try:
          with session.begin_nested():
            prev_status = evt.status
            fire_handler_on_commit(session, evt, created_by)
            # Respect a status the handler set itself (e.g. 'fulfilled').
            if evt.status == prev_status:
              evt.status = "committed"
          out.handler_dispatched += 1
        except Exception as e:
          # Outside the rolled-back SAVEPOINT, so it commits with the outer
          # transaction.
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

  # Adopts whatever active chart_of_accounts taxonomy exists, or creates one.
  # Never runs over natively-kept books: `assert_provider_compatible` refuses
  # a synced GL on a graph with posted lines on non-provider elements, and a
  # severed chart's elements leave the upsert key.
  def _ensure_mapping_structure(
    self,
    graph_id: str,
    source: str,
    created_by: str,
  ) -> None:
    """Idempotently ensure the CoA taxonomy, CoA→GAAP mapping structure, and
    entity adoption exist; report generation needs all three.

    The adoption row materializes as the ENTITY_HAS_TAXONOMY edge. Failures
    are logged, not raised.
    """
    from robosystems.db.extensions import extensions_session
    from robosystems.models.extensions import Element, EntityTaxonomy
    from robosystems.models.extensions.roboledger import Structure, Taxonomy
    from robosystems.operations.roboledger.reads.entity import resolve_parent_entity
    from robosystems.utils.ulid import generate_prefixed_ulid

    try:
      with extensions_session(graph_id, statement_timeout_ms=None) as session:
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

        # The element load runs before the taxonomy exists, so synced accounts
        # land with taxonomy_id NULL and would never show under it in the
        # Library UI. Backfill only the NULLs.
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

        entity = resolve_parent_entity(session)
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
    """Update the ledger's entity from the connector's CompanyInfo staging table, if any."""
    import duckdb

    from robosystems.db.extensions import extensions_session
    from robosystems.operations.roboledger.reads.entity import resolve_parent_entity

    try:
      con = duckdb.connect(str(duckdb_path), read_only=True)
      try:
        tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}

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

      with extensions_session(graph_id, statement_timeout_ms=None) as session:
        # The ledger's own entity by predicate — a shared-in `linked`
        # counterparty must never receive the CompanyInfo overwrite.
        entity = resolve_parent_entity(session)
        if not entity:
          logger.warning(
            f"No entity found in graph {graph_id}, skipping CompanyInfo update"
          )
          return

        entity.name = company.get("company_name") or entity.name
        entity.legal_name = company.get("legal_name") or entity.legal_name
        entity.address_line1 = company.get("address_line1")
        entity.address_city = company.get("city")
        entity.address_state = company.get("state")
        entity.address_postal_code = company.get("postal_code")
        entity.address_country = company.get("country", "US")
        # Only overwrite when the adapter supplies a value.
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
