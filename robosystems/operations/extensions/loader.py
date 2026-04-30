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


@dataclass
class LoadResult:
  """Result of an OLTP load operation.

  Counts after Phase 2 (event-block ingest):

  - ``elements`` / ``dimensions``: still inserted directly (structural).
  - ``events_captured`` / ``events_updated``: per-transaction event_block
    rows written by ``_capture_transactions_as_events`` with
    ``status='captured'`` and ``apply_handlers=False``. The user later
    approves these in the inbox, which fires the registered handler and
    creates the actual GL rows.
  - ``transactions`` / ``entries`` / ``line_items``: zero on a Phase 2 sync.
    These rows are now produced by handlers (post-approval), not by the
    sync. Kept on the dataclass for backward-compat with callers that
    log result counts; ``total_rows`` includes the new event counts.
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
  agents_inserted: int = 0
  agents_updated: int = 0
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
  ) -> LoadResult:
    """Load dbt OLTP output into the extensions tenant schema.

    Args:
        graph_id: The graph ID (maps to a PostgreSQL schema).
        source: The data source name (e.g., 'quickbooks', 'xero').
        connection_id: The connection ID for data isolation.
        duckdb_path: Path to the dbt output DuckDB database.
        created_by: User ID for audit trail.

    Returns:
        LoadResult with row counts per table.
    """
    import duckdb

    from robosystems.db.extensions import extensions_session, provision_tenant_schema
    from robosystems.models.extensions import (
      Dimension,
      Element,
    )
    from robosystems.utils.ulid import generate_prefixed_ulid

    result = LoadResult(graph_id=graph_id, source=source, connection_id=connection_id)

    # Ensure tenant schema exists
    provision_tenant_schema(graph_id)

    # Read dbt output tables from DuckDB
    # Use fetchall() instead of fetchdf() to avoid pyarrow/DuckDB segfault
    # in containerized Dagster workers where both libraries coexist.
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
      # ── Pre-sync deletes ───────────────────────────────────────────
      #
      # Transaction / Entry / LineItem are NOT deleted any more. Phase 2
      # moved transactional ingestion to the event-block pattern: each
      # QB transaction is captured as an Event row with
      # ``status='captured'`` and ``apply_handlers=False``. GL rows
      # (Transaction / Entry / LineItem) are produced post-approval by
      # the registered handler. Deleting them here would destroy
      # legitimate handler-approved entries on every re-sync.
      #
      # Element / Dimension paths stay direct (structural data, not
      # transactional events).

      # Multi-connection-safe: scope element deletes by connection_id so a
      # re-sync of one QB connection doesn't stomp another's CoA.
      # Library-origin elements (rs-gaap / us-gaap / FAC) have
      # connection_id NULL and are never touched by this delete.
      session.query(Element).filter(
        Element.external_source == source,
        Element.connection_id == connection_id,
      ).delete(synchronize_session=False)

      session.query(Dimension).filter(
        Dimension.metadata_.contains({"source": source}),
      ).delete(synchronize_session=False)

      session.flush()
      logger.info(
        f"Refreshed Element + Dimension rows for source={source}, "
        f"connection_id={connection_id}"
      )

      # --- INSERT elements (from dbt "elements" table) ---
      element_lookup: dict[str, str] = {}  # external_id → oltp_id
      external_parent_map: dict[str, str] = {}  # oltp_id → external_parent_id

      if "elements" in dbt_data:
        rows = dbt_data["elements"]
        element_objects = []
        for row in rows:
          oltp_id = generate_prefixed_ulid("elem")
          ext_id = str(row["external_id"])
          element_lookup[ext_id] = oltp_id

          ext_parent = row.get("external_parent_id")
          if ext_parent and str(ext_parent) not in ("", "None", "nan"):
            external_parent_map[oltp_id] = str(ext_parent)

          element_objects.append(
            Element(
              id=oltp_id,
              code=str(row["code"]),
              name=str(row["name"]),
              description=str(row["description"]) if row.get("description") else None,
              balance_type=str(row["balance_type"]),
              parent_id=None,  # resolved in second pass
              depth=int(row.get("depth", 0)),
              path=str(row.get("path", "")),
              currency=str(row.get("currency", "USD")),
              is_active=bool(row.get("is_active", True)),
              is_placeholder=bool(row.get("is_placeholder", False)),
              source=source,
              period_type="duration",
              element_type="concept",
              is_abstract=False,
              is_monetary=True,
              external_id=ext_id,
              external_source=str(row["external_source"]),
              connection_id=connection_id,
              metadata_=_parse_metadata(row.get("metadata")),
              version=1,
              created_at=now,
              updated_at=now,
              created_by=created_by,
            )
          )

        session.add_all(element_objects)
        session.flush()

        # Second pass: resolve parent_id
        for oltp_id, ext_parent_id in external_parent_map.items():
          parent_oltp_id = element_lookup.get(ext_parent_id)
          if parent_oltp_id:
            session.query(Element).filter(Element.id == oltp_id).update(
              {"parent_id": parent_oltp_id}, synchronize_session=False
            )

        session.flush()
        result.elements = len(rows)
        logger.info(f"Inserted {result.elements} elements")

      # --- UPSERT agents (Phase 2) ---
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
      # Per Phase 2 (see ``quickbooks-adapter.md``): each QB transaction
      # becomes one Event row with ``status='captured'`` and
      # ``apply_handlers=False``. GL rows (Transaction / Entry /
      # LineItem) are produced after approval via the inbox flow
      # (Phase 4) by the ``journal_entry_recorded`` handler.
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
      result.dropped_unbalanced_entries = capture_result.dropped_unbalanced_entries
      result.dropped_empty_transactions = capture_result.dropped_empty_transactions
      logger.info(
        "Captured %d new event_blocks, updated %d existing "
        "(dropped %d unbalanced entries, %d empty transactions; "
        "capture-only — no GL writes)",
        capture_result.inserted,
        capture_result.updated,
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

      address = row.get("address")
      if not isinstance(address, dict):
        address = {}

      name = str(row.get("name") or "")
      legal_name = str(row.get("legal_name") or "") or None
      email = str(row.get("email") or "") or None
      phone = str(row.get("phone") or "") or None
      tax_id = str(row.get("tax_id") or "") or None
      agent_type = str(row.get("agent_type") or "other")
      is_1099 = bool(row.get("is_1099_recipient", False))
      is_active = bool(row.get("is_active", True))

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
    and line_items packed into ``Event.metadata_``) and writes them as
    Event rows with ``status='captured'``. Handler dispatch is deferred
    to the inbox-approval flow (Phase 4); no GL rows are produced by
    this sync.

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

    # 2) Look up existing events for the (source, external_id) pairs
    existing: dict[str, Event] = {
      e.external_id: e
      for e in session.query(Event)
      .filter(
        Event.source == source,
        Event.external_id.in_(list(txns_by_ext.keys())),
      )
      .all()
    }

    out = _CaptureResult()
    new_events = []

    for ext_id, txn in txns_by_ext.items():
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

      # Phase 2: source-class fidelity + agent linkage.
      # The transactions mart now carries event_type / event_category and
      # the optional agent_external_id from the per-class header join.
      event_type = str(txn.get("event_type") or "journal_entry_recorded")
      event_category = str(txn.get("event_category") or "adjustment")
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

      metadata_blob = {
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
        "connection_id": connection_id,
        "entries": hardened_entries,
      }

      if ext_id in existing:
        evt = existing[ext_id]
        # Don't overwrite handler-approved or rejected events on re-sync.
        if evt.status in ("captured", "classified"):
          evt.event_type = event_type
          evt.event_category = event_category
          evt.agent_id = agent_id
          evt.occurred_at = occurred_at
          evt.amount = amount
          evt.currency = txn.get("currency", "USD")
          evt.description = description
          evt.metadata_ = metadata_blob
          out.updated += 1
        # else: leave it alone — handler already ran or user voided it
      else:
        new_events.append(
          Event(
            event_type=event_type,
            event_category=event_category,
            event_class="economic",
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
    from robosystems.models.extensions import EntityTaxonomy
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
          .filter(
            Structure.structure_type == "coa_mapping", Structure.is_active.is_(True)
          )
          .first()
        )

        if not existing_mapping:
          mapping_structure = Structure(
            id=generate_prefixed_ulid("struct"),
            name="CoA to US GAAP Mapping",
            description="Maps Chart of Accounts to US GAAP reporting concepts",
            structure_type="coa_mapping",
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
