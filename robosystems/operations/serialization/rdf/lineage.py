"""The holon's fourth named graph, ``<report>#lineage``: the ledger behind a
report's frozen facts, derived at download and spliced into the holon xbrlkit
wrote.

A frozen pivot fact's descriptor names the accounts it summed (``sources``,
by the entity whose pivot summed them) and a ``content_hash`` over the landed
line ids under those accounts in its window. This module re-derives that set
through the stamper's own selection, refuses any fact whose reconstruction
does not reproduce its hash, and writes what it found as RDF:

    rs:Fact ─prov:wasDerivedFrom─▶ rs:Contribution (one per entity and account)
                                     └─prov:hadMember─▶ rs:LineItem …
    rs:LineItem ─rs:entry─▶ rs:Entry ─rs:transaction─▶ rs:Transaction
         └─prov:wasGeneratedBy─▶ rs:Event ─prov:wasAssociatedWith─▶ rs:Agent

Every contribution lists all of its member lines by IRI, so the hash is
checkable from the file; the lines *described* (a node with its entry,
transaction, event and agent) are bounded to the report's span by default,
and a contribution reaching further back states the balance and the count
of what it leaves undescribed. Only landed rows (``posted``, ``reversed``)
exist here; a superseded row is outside the hash and outside the graph.

The vocabulary is additive ``rs:`` terms on a PROV-O skeleton, declared in the
document ``@context`` (``LINEAGE_CONTEXT``). Member entities are
``rs:LedgerEntity`` and chart accounts ``rs:Account``: an ``rs:Entity`` or an
``rs:Element`` here would change what xbrlkit's reader takes the report to be.

xbrlkit is untouched: ``to_holon`` partitions a flat graph into exactly three
named graphs and drops every subject it does not know, so the lineage is
spliced into its finished JSON (:func:`splice_lineage`). Its reader returns the
same model for both files and, asked to write the entitled one back, writes
the un-entitled holon.
"""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any, Literal, Protocol

from sqlalchemy import text
from sqlalchemy.orm import Session
from xbrlkit.serialize.graph import holon_root

from robosystems.logger import logger
from robosystems.operations.roboledger.entry_status import landed_entry_bindparam
from robosystems.operations.serialization.bundle import BundleFact, StatementBundle
from robosystems.operations.serialization.model import report_identifier
from robosystems.operations.serialization.rdf.holon import serialize_to_holon_jsonld

LINEAGE_GRAPH = "lineage"

# Terms the lineage adds to the holon's @context. Checked for clashes against
# the document's own context at splice time; a clash is an error, never a
# shadow.
LINEAGE_CONTEXT: dict[str, Any] = {
  "derivedFrom": {"@id": "prov:wasDerivedFrom", "@type": "@id"},
  "substantiates": {"@id": "rs:substantiates", "@type": "@id"},
  "hadMember": {"@id": "prov:hadMember", "@type": "@id"},
  "generatedBy": {"@id": "prov:wasGeneratedBy", "@type": "@id"},
  "associatedWith": {"@id": "prov:wasAssociatedWith", "@type": "@id"},
  "account": {"@id": "rs:account", "@type": "@id"},
  "entry": {"@id": "rs:entry", "@type": "@id"},
  "transaction": {"@id": "rs:transaction", "@type": "@id"},
  "reversalOf": {"@id": "rs:reversalOf", "@type": "@id"},
  "reversedBy": {"@id": "rs:reversedBy", "@type": "@id"},
  "adjustsEntity": {"@id": "rs:adjustsEntity", "@type": "@id"},
  "parentEntity": {"@id": "rs:parentEntity", "@type": "@id"},
  "obligatedBy": {"@id": "rs:obligatedBy", "@type": "@id"},
  "discharges": {"@id": "rs:discharges", "@type": "@id"},
  "replaces": {"@id": "rs:replaces", "@type": "@id"},
  "flowConcept": {"@id": "rs:flowConcept"},
  "debit": {"@id": "rs:debit", "@type": "xsd:decimal"},
  "credit": {"@id": "rs:credit", "@type": "xsd:decimal"},
  "amount": {"@id": "rs:amount", "@type": "xsd:decimal"},
  "priorBalance": {"@id": "rs:priorBalance", "@type": "xsd:decimal"},
  "postingDate": {"@id": "rs:postingDate", "@type": "xsd:date"},
  "transactionDate": {"@id": "rs:transactionDate", "@type": "xsd:date"},
  "dueDate": {"@id": "rs:dueDate", "@type": "xsd:date"},
  "occurredAt": {"@id": "rs:occurredAt", "@type": "xsd:dateTime"},
  "postedAt": {"@id": "rs:postedAt", "@type": "xsd:dateTime"},
  "frozenAt": {"@id": "rs:frozenAt", "@type": "xsd:dateTime"},
  "windowStart": {"@id": "rs:windowStart", "@type": "xsd:date"},
  "windowEnd": {"@id": "rs:windowEnd", "@type": "xsd:date"},
  "describedFrom": {"@id": "rs:describedFrom", "@type": "xsd:date"},
  "lineCount": {"@id": "rs:lineCount", "@type": "xsd:integer"},
  "describedLineCount": {"@id": "rs:describedLineCount", "@type": "xsd:integer"},
  "priorLineCount": {"@id": "rs:priorLineCount", "@type": "xsd:integer"},
  "lineOrder": {"@id": "rs:lineOrder", "@type": "xsd:integer"},
  "code": {"@id": "rs:code"},
  "entityRole": {"@id": "rs:entityRole"},
  "entryType": {"@id": "rs:entryType"},
  "entryNumber": {"@id": "rs:entryNumber"},
  "recordedBy": {"@id": "rs:recordedBy"},
  "entryStatus": {"@id": "rs:entryStatus"},
  "memo": {"@id": "rs:memo"},
  "adjustmentKind": {"@id": "rs:adjustmentKind"},
  "transactionType": {"@id": "rs:transactionType"},
  "currency": {"@id": "rs:currency"},
  "merchantName": {"@id": "rs:merchantName"},
  "referenceNumber": {"@id": "rs:referenceNumber"},
  "eventType": {"@id": "rs:eventType"},
  "eventCategory": {"@id": "rs:eventCategory"},
  "eventClass": {"@id": "rs:eventClass"},
  "eventAction": {"@id": "rs:eventAction"},
  "eventStatus": {"@id": "rs:eventStatus"},
  "agentType": {"@id": "rs:agentType"},
  "dimensionMember": {"@id": "rs:dimensionMember"},
  "lineDescription": {"@id": "rs:lineDescription"},
}


# ── Ledger rows, as the exporter needs them ─────────────────────────────────


@dataclass(frozen=True)
class LineRow:
  id: str
  entry_id: str
  account_id: str
  flow_element_id: str | None
  debit_cents: int
  credit_cents: int
  description: str | None
  line_order: int
  entity_id: str
  posting_date: date


@dataclass(frozen=True)
class EntryRow:
  id: str
  entity_id: str
  number: str | None
  posting_date: date
  posted_at: datetime | None
  type: str
  status: str
  provenance: str | None
  memo: str | None
  transaction_id: str | None
  triggered_by_event_id: str | None
  reversal_of: str | None
  adjustment_kind: str | None
  adjusts_entity_id: str | None
  flow_retags: Sequence[Mapping[str, Any]] = ()


@dataclass(frozen=True)
class TransactionRow:
  id: str
  entity_id: str
  type: str
  category: str | None
  amount_cents: int
  currency: str
  date: date
  due_date: date | None
  merchant_name: str | None
  reference_number: str | None
  description: str | None
  source: str
  status: str
  triggered_by_event_id: str | None


@dataclass(frozen=True)
class EventRow:
  id: str
  entity_id: str
  event_type: str
  event_category: str
  event_class: str
  event_action: str | None
  status: str
  occurred_at: datetime
  due_date: date | None
  amount_cents: int | None
  currency: str
  source: str
  description: str | None
  agent_id: str | None
  obligated_by_event_id: str | None
  discharges_event_id: str | None
  replaces_event_id: str | None
  replaced_by_event_id: str | None


@dataclass(frozen=True)
class AgentRow:
  id: str
  name: str
  agent_type: str


@dataclass(frozen=True)
class ElementRow:
  id: str
  name: str
  code: str | None
  qname: str | None
  balance_type: str | None
  period_type: str | None


@dataclass(frozen=True)
class EntityRow:
  id: str
  name: str
  entity_role: str | None
  parent_entity_id: str | None


@dataclass(frozen=True)
class DischargeRow:
  payment_event_id: str
  obligation_event_id: str
  amount_cents: int


class LedgerReader(Protocol):
  """What the exporter reads from a ledger. The SQL reader is the production
  one; a test supplies rows directly."""

  def lines(
    self,
    entity_id: str,
    window_start: date | None,
    window_end: date,
    account_ids: Sequence[str],
  ) -> dict[str, list[LineRow]]: ...

  def entries(self, ids: Iterable[str]) -> dict[str, EntryRow]: ...

  def reversed_by(self, entry_ids: Iterable[str]) -> dict[str, str]: ...

  def transactions(self, ids: Iterable[str]) -> dict[str, TransactionRow]: ...

  def events(self, ids: Iterable[str]) -> dict[str, EventRow]: ...

  def discharges(self, event_ids: Iterable[str]) -> list[DischargeRow]: ...

  def agents(self, ids: Iterable[str]) -> dict[str, AgentRow]: ...

  def elements(self, ids: Iterable[str]) -> dict[str, ElementRow]: ...

  def entities(self, ids: Iterable[str]) -> dict[str, EntityRow]: ...

  def line_dimensions(self, line_ids: Iterable[str]) -> dict[str, list[str]]: ...


class SqlLedgerReader:
  """The production reader. ``lines`` is the stamper's own selection
  (``statement_sets._LineSets``), carrying the line's fields; everything else
  is a read by id."""

  _LINES = text(
    """
    SELECT li.id, li.entry_id, li.element_id, li.flow_element_id, li.debit_amount,
           li.credit_amount, li.description, li.line_order,
           e.entity_id, e.posting_date
    FROM line_items li JOIN entries e ON e.id = li.entry_id
    WHERE e.entity_id = :entity_id AND e.status IN :landed_entry_statuses
      AND li.element_id = ANY(:account_ids)
      AND e.posting_date <= :end_date
      AND (CAST(:start_date AS DATE) IS NULL OR e.posting_date >= :start_date)
    ORDER BY li.id
    """
  ).bindparams(landed_entry_bindparam())
  _REVERSED_BY = text(
    "SELECT id, reversal_of FROM entries "
    "WHERE reversal_of = ANY(:ids) AND status IN :landed_entry_statuses"
  ).bindparams(landed_entry_bindparam())

  def __init__(self, session: Session) -> None:
    self._session = session
    self._lines: dict[tuple[str, date | None, date], dict[str, list[LineRow]]] = {}

  def _rows(self, sql: str, ids: Iterable[str]) -> list[Any]:
    wanted = sorted(set(ids))
    if not wanted:
      return []
    return list(self._session.execute(text(sql), {"ids": wanted}).mappings())

  def lines(
    self,
    entity_id: str,
    window_start: date | None,
    window_end: date,
    account_ids: Sequence[str],
  ) -> dict[str, list[LineRow]]:
    held = self._lines.setdefault((entity_id, window_start, window_end), {})
    missing = sorted({a for a in account_ids if a not in held})
    if missing:
      for a in missing:
        held[a] = []
      rows = self._session.execute(
        self._LINES,
        {
          "entity_id": entity_id,
          "account_ids": missing,
          "start_date": window_start,
          "end_date": window_end,
        },
      ).mappings()
      for r in rows:
        held[str(r["element_id"])].append(
          LineRow(
            id=str(r["id"]),
            entry_id=str(r["entry_id"]),
            account_id=str(r["element_id"]),
            flow_element_id=r["flow_element_id"],
            debit_cents=int(r["debit_amount"] or 0),
            credit_cents=int(r["credit_amount"] or 0),
            description=r["description"],
            line_order=int(r["line_order"] or 0),
            entity_id=str(r["entity_id"]),
            posting_date=r["posting_date"],
          )
        )
    return held

  def entries(self, ids: Iterable[str]) -> dict[str, EntryRow]:
    return {
      str(r["id"]): EntryRow(
        id=str(r["id"]),
        entity_id=str(r["entity_id"]),
        number=r["number"],
        posting_date=r["posting_date"],
        posted_at=r["posted_at"],
        type=r["type"],
        status=r["status"],
        provenance=r["provenance"],
        memo=r["memo"],
        transaction_id=r["transaction_id"],
        triggered_by_event_id=r["triggered_by_event_id"],
        reversal_of=r["reversal_of"],
        adjustment_kind=r["adjustment_kind"],
        adjusts_entity_id=r["adjusts_entity_id"],
        flow_retags=tuple((r["metadata"] or {}).get("flow_retags") or ()),
      )
      for r in self._rows("SELECT * FROM entries WHERE id = ANY(:ids)", ids)
    }

  def reversed_by(self, entry_ids: Iterable[str]) -> dict[str, str]:
    wanted = sorted(set(entry_ids))
    if not wanted:
      return {}
    rows = self._session.execute(self._REVERSED_BY, {"ids": wanted}).mappings()
    return {str(r["reversal_of"]): str(r["id"]) for r in rows}

  def transactions(self, ids: Iterable[str]) -> dict[str, TransactionRow]:
    return {
      str(r["id"]): TransactionRow(
        id=str(r["id"]),
        entity_id=str(r["entity_id"]),
        type=r["type"],
        category=r["category"],
        amount_cents=int(r["amount"] or 0),
        currency=r["currency"],
        date=r["date"],
        due_date=r["due_date"],
        merchant_name=r["merchant_name"],
        reference_number=r["reference_number"],
        description=r["description"],
        source=r["source"],
        status=r["status"],
        triggered_by_event_id=r["triggered_by_event_id"],
      )
      for r in self._rows("SELECT * FROM transactions WHERE id = ANY(:ids)", ids)
    }

  def events(self, ids: Iterable[str]) -> dict[str, EventRow]:
    return {
      str(r["id"]): EventRow(
        id=str(r["id"]),
        entity_id=str(r["entity_id"]),
        event_type=r["event_type"],
        event_category=r["event_category"],
        event_class=r["event_class"],
        event_action=r["event_action"],
        status=r["status"],
        occurred_at=r["occurred_at"],
        due_date=r["due_date"],
        amount_cents=None if r["amount"] is None else int(r["amount"]),
        currency=r["currency"],
        source=r["source"],
        description=r["description"],
        agent_id=r["agent_id"],
        obligated_by_event_id=r["obligated_by_event_id"],
        discharges_event_id=r["discharges_event_id"],
        replaces_event_id=r["replaces_event_id"],
        replaced_by_event_id=r["replaced_by_event_id"],
      )
      for r in self._rows("SELECT * FROM events WHERE id = ANY(:ids)", ids)
    }

  def discharges(self, event_ids: Iterable[str]) -> list[DischargeRow]:
    return [
      DischargeRow(
        payment_event_id=str(r["payment_event_id"]),
        obligation_event_id=str(r["obligation_event_id"]),
        amount_cents=int(r["amount"]),
      )
      for r in self._rows(
        "SELECT payment_event_id, obligation_event_id, amount FROM event_discharges "
        "WHERE payment_event_id = ANY(:ids) OR obligation_event_id = ANY(:ids)",
        event_ids,
      )
    ]

  def agents(self, ids: Iterable[str]) -> dict[str, AgentRow]:
    return {
      str(r["id"]): AgentRow(
        id=str(r["id"]), name=r["name"], agent_type=r["agent_type"]
      )
      for r in self._rows(
        "SELECT id, name, agent_type FROM agents WHERE id = ANY(:ids)", ids
      )
    }

  def elements(self, ids: Iterable[str]) -> dict[str, ElementRow]:
    return {
      str(r["id"]): ElementRow(
        id=str(r["id"]),
        name=r["name"],
        code=r["code"],
        qname=r["qname"],
        balance_type=r["balance_type"],
        period_type=r["period_type"],
      )
      for r in self._rows(
        "SELECT id, name, code, qname, balance_type, period_type FROM elements "
        "WHERE id = ANY(:ids)",
        ids,
      )
    }

  def entities(self, ids: Iterable[str]) -> dict[str, EntityRow]:
    return {
      str(r["id"]): EntityRow(
        id=str(r["id"]),
        name=r["name"],
        entity_role=r["entity_role"],
        parent_entity_id=r["parent_entity_id"],
      )
      for r in self._rows(
        "SELECT id, name, entity_role, parent_entity_id FROM entities "
        "WHERE id = ANY(:ids)",
        ids,
      )
    }

  def line_dimensions(self, line_ids: Iterable[str]) -> dict[str, list[str]]:
    out: dict[str, list[str]] = defaultdict(list)
    for r in self._rows(
      "SELECT lid.line_item_id, d.dimension_type, d.value "
      "FROM line_item_dimensions lid JOIN dimensions d ON d.id = lid.dimension_id "
      "WHERE lid.line_item_id = ANY(:ids) ORDER BY d.dimension_type, d.value",
      line_ids,
    ):
      out[str(r["line_item_id"])].append(f"{r['dimension_type']}={r['value']}")
    return out


# ── The graph ───────────────────────────────────────────────────────────────


@dataclass
class Lineage:
  """What :func:`build_lineage` found: the ``#lineage`` nodes, the bridge
  edges to add to the facts in ``#scene`` (fact id → contribution IRIs), the
  facts refused because their reconstruction missed the frozen hash, and the
  counts a log line wants."""

  nodes: list[dict[str, Any]]
  bridges: dict[str, list[str]]
  refused: list[str] = field(default_factory=list)
  summary: dict[str, int] = field(default_factory=dict)

  @property
  def empty(self) -> bool:
    return not self.bridges


def _dollars(cents: int) -> str:
  return str((Decimal(int(cents)) / 100).quantize(Decimal("0.01")))


def _amount(value: Any) -> str:
  """A descriptor amount (a float, a Decimal or a string) to the cent."""
  return str(Decimal(str(value)).quantize(Decimal("0.01")))


def _when(text_value: Any) -> datetime | None:
  """An ISO-8601 stamp as an aware datetime, or None when it does not parse."""
  try:
    stamp = datetime.fromisoformat(str(text_value))
  except TypeError, ValueError:
    return None
  return stamp if stamp.tzinfo else stamp.replace(tzinfo=UTC)


def _iso(value: date | datetime | None) -> str | None:
  if value is None:
    return None
  if isinstance(value, datetime):
    stamp = value if value.tzinfo else value.replace(tzinfo=UTC)
    return stamp.astimezone(UTC).isoformat().replace("+00:00", "Z")
  return value.isoformat()


def _node(**fields: Any) -> dict[str, Any]:
  return {k: v for k, v in fields.items() if v not in (None, "", [], {})}


def _lineage_facts(bundle: StatementBundle) -> list[BundleFact]:
  """The frozen pivot facts whose descriptor names accounts: the only facts
  that stand on lines. A rollup or a derivation is substantiated by its parts
  (already in ``#scene``), a live column by nothing yet."""
  facts: list[BundleFact] = []
  for f in bundle.facts:
    descriptor = f.provenance or {}
    if (
      f.frozen_at is not None
      and descriptor.get("origin") == "pivot"
      and isinstance(descriptor.get("sources"), list)
    ):
      facts.append(f)
  return facts


def report_span(bundle: StatementBundle) -> tuple[date, date] | None:
  """The report's first column start to its last column end — its own
  columns when it has them, else the period nodes its facts reference (a
  duration's start, an instant's month start)."""
  if bundle.periods:
    return min(p.start for p in bundle.periods), max(p.end for p in bundle.periods)
  starts: list[date] = []
  ends: list[date] = []
  for p in bundle.period_nodes:
    ends.append(p.period_end)
    starts.append(
      p.period_start if p.period_start is not None else p.period_end.replace(day=1)
    )
  if not starts:
    return None
  return min(starts), max(ends)


def _flow_as_of(
  entry: EntryRow, line_id: str, current: str | None, frozen_at: datetime | None
) -> str | None:
  """The line's flow concept as of the freeze: a retag recorded after it
  names what the line carried before (``flow_retags[].from``), and the
  earliest such record is the tag the frozen column was pivoted with."""
  if frozen_at is None or not entry.flow_retags:
    return current
  cutoff = frozen_at if frozen_at.tzinfo else frozen_at.replace(tzinfo=UTC)
  dated = [
    (at, record)
    for record in entry.flow_retags
    if (at := _when(record.get("at"))) is not None
  ]
  for at, record in sorted(dated, key=lambda pair: pair[0]):
    if at <= cutoff:
      continue
    before = record.get("from") or {}
    if line_id in before:
      return before[line_id]
  return current


# The most lines one file describes. Past it the export refuses rather than
# build an unbounded graph inside a request; a narrower report is the answer.
LINEAGE_LINE_CEILING = 25_000


class LineageTooLargeError(ValueError):
  """The report's lineage would describe more lines than one file carries."""


def build_lineage(
  bundle: StatementBundle,
  reader: LedgerReader,
  *,
  root: str,
  describe_from: date | None,
) -> Lineage:
  """The ``#lineage`` graph for ``bundle``'s frozen pivot facts.

  ``describe_from`` bounds the lines that get a node (``None`` describes every
  member, back to the opening of the books); every member is listed by IRI
  regardless, so the frozen hash is checkable from the file.
  """

  def iri(segment: str, ident: str) -> str:
    return f"{root}/{segment}/{ident}"

  periods = {p.id: p for p in bundle.period_nodes}
  facts = _lineage_facts(bundle)

  contributions: list[dict[str, Any]] = []
  prior_rows: dict[str, list[LineRow]] = {}
  bridges: dict[str, list[str]] = defaultdict(list)
  members: dict[str, LineRow] = {}
  described_ids: set[str] = set()
  frozen_at_by_line: dict[str, datetime] = {}
  refused: list[str] = []

  for fact in facts:
    period = periods.get(fact.period_ref)
    descriptor = fact.provenance or {}
    if period is None:
      refused.append(fact.id)
      continue
    window_start = None if period.period_type == "instant" else period.period_start
    window_end = period.period_end
    all_ids: list[str] = []
    fact_contributions: list[dict[str, Any]] = []
    # Collected per fact and merged only once its hash checks out, so a
    # refused fact leaves no line, entry or event of its own in the file.
    fact_members: dict[str, LineRow] = {}
    fact_described: set[str] = set()
    fact_prior: dict[str, list[LineRow]] = {}
    for src in descriptor["sources"]:
      entity_id, account_id = str(src["entity_id"]), str(src["account_id"])
      rows = reader.lines(entity_id, window_start, window_end, [account_id]).get(
        account_id, []
      )
      all_ids.extend(r.id for r in rows)
      described = [
        r for r in rows if describe_from is None or r.posting_date >= describe_from
      ]
      prior = [
        r
        for r in rows
        if not (describe_from is None or r.posting_date >= describe_from)
      ]
      contribution_iri = iri("contribution", f"{fact.id}/{entity_id}/{account_id}")
      node = _node(
        **{
          "@id": contribution_iri,
          "@type": ["rs:Contribution", "prov:Collection"],
          "substantiates": iri("fact", fact.id),
          "entity": iri("entity", entity_id),
          "account": iri("account", account_id),
          "amount": _amount(src["value"]),
          "frozenAt": _iso(fact.frozen_at),
          "windowStart": _iso(window_start),
          "windowEnd": _iso(window_end),
          "lineCount": len(rows),
          "describedLineCount": len(described),
          "hadMember": sorted(iri("line", r.id) for r in rows),
        }
      )
      if prior:
        node["describedFrom"] = _iso(describe_from)
        node["priorLineCount"] = len(prior)
        fact_prior[contribution_iri] = prior
      fact_contributions.append(node)
      for r in rows:
        fact_members[r.id] = r
      for r in described:
        fact_described.add(r.id)
    digest = "sha256:" + hashlib.sha256("\n".join(sorted(all_ids)).encode()).hexdigest()
    if digest != descriptor.get("content_hash") or len(all_ids) != descriptor.get(
      "line_count"
    ):
      refused.append(fact.id)
      continue
    contributions.extend(fact_contributions)
    bridges[fact.id] = [c["@id"] for c in fact_contributions]
    members.update(fact_members)
    described_ids |= fact_described
    prior_rows.update(fact_prior)
    if fact.frozen_at is not None:
      for line_id in fact_members:
        held = frozen_at_by_line.get(line_id)
        if held is None or fact.frozen_at < held:
          frozen_at_by_line[line_id] = fact.frozen_at

  if len(described_ids) > LINEAGE_LINE_CEILING:
    raise LineageTooLargeError(
      f"The lineage would describe {len(described_ids):,} lines; one file carries "
      f"at most {LINEAGE_LINE_CEILING:,}. Publish a report over a narrower span."
    )

  # The closure of the described lines, one hop of duality beyond their events.
  described = [members[i] for i in described_ids]
  entries = reader.entries({r.entry_id for r in described})
  transactions = reader.transactions(
    {e.transaction_id for e in entries.values() if e.transaction_id}
  )
  event_ids = {
    e.triggered_by_event_id for e in entries.values() if e.triggered_by_event_id
  }
  event_ids |= {
    t.triggered_by_event_id for t in transactions.values() if t.triggered_by_event_id
  }
  events = reader.events(event_ids)
  hop_ids: set[str] = set()
  for ev in events.values():
    for linked in (
      ev.obligated_by_event_id,
      ev.discharges_event_id,
      ev.replaces_event_id,
      ev.replaced_by_event_id,
    ):
      if linked and linked not in events:
        hop_ids.add(linked)
  hop_events = reader.events(hop_ids)
  discharges = reader.discharges(event_ids)
  all_events = {**events, **hop_events}
  agents = reader.agents({ev.agent_id for ev in all_events.values() if ev.agent_id})
  reversed_by = reader.reversed_by(entries)
  contribution_accounts = {c["account"].rsplit("/", 1)[1] for c in contributions}
  elements = reader.elements(
    contribution_accounts | {r.flow_element_id for r in described if r.flow_element_id}
  )
  entity_ids = {c["entity"].rsplit("/", 1)[1] for c in contributions}
  entity_ids |= {e.entity_id for e in entries.values()}
  entity_ids |= {ev.entity_id for ev in all_events.values()}
  entity_ids |= {e.adjusts_entity_id for e in entries.values() if e.adjusts_entity_id}
  entities = reader.entities(entity_ids)
  dimensions = reader.line_dimensions(described_ids)

  nodes: list[dict[str, Any]] = []
  for c in contributions:
    prior = prior_rows.get(c["@id"])
    if prior:
      account = elements.get(c["account"].rsplit("/", 1)[1])
      net = sum(r.debit_cents - r.credit_cents for r in prior)
      credit = (
        (account.balance_type or "debit").lower() == "credit" if account else False
      )
      c["priorBalance"] = _dollars(-net if credit else net)
    nodes.append(c)

  report_entity = bundle.entity.id
  for entity_id in sorted(entity_ids):
    ent = entities.get(entity_id)
    nodes.append(
      _node(
        **{
          "@id": iri("entity", entity_id),
          "@type": "rs:LedgerEntity",
          "internalId": entity_id,
          # The report's own entity is labelled in #scene; restating it here
          # would be a duplicate quad under a union read.
          "prefLabel": ent.name if ent and entity_id != report_entity else None,
          "entityRole": ent.entity_role if ent else None,
          "parentEntity": iri("entity", ent.parent_entity_id)
          if ent and ent.parent_entity_id
          else None,
        }
      )
    )
  for account_id in sorted(contribution_accounts):
    a = elements.get(account_id)
    nodes.append(
      _node(
        **{
          "@id": iri("account", account_id),
          "@type": "rs:Account",
          "internalId": account_id,
          "prefLabel": a.name if a else None,
          "code": a.code if a else None,
          "balance": a.balance_type if a else None,
          "periodType": a.period_type if a else None,
        }
      )
    )
  for r in sorted(described, key=lambda r: r.id):
    entry = entries[r.entry_id]
    flow = elements.get(r.flow_element_id) if r.flow_element_id else None
    nodes.append(
      _node(
        **{
          "@id": iri("line", r.id),
          "@type": "rs:LineItem",
          "internalId": r.id,
          "entry": iri("entry", r.entry_id),
          "account": iri("account", r.account_id),
          "entity": iri("entity", entry.entity_id),
          "debit": _dollars(r.debit_cents) if r.debit_cents else None,
          "credit": _dollars(r.credit_cents) if r.credit_cents else None,
          "lineOrder": r.line_order,
          "lineDescription": r.description,
          "flowConcept": _flow_as_of(
            entry, r.id, flow.qname if flow else None, frozen_at_by_line.get(r.id)
          ),
          "dimensionMember": dimensions.get(r.id) or None,
          "generatedBy": iri("event", entry.triggered_by_event_id)
          if entry.triggered_by_event_id
          else None,
        }
      )
    )
  for entry_id, e in sorted(entries.items()):
    nodes.append(
      _node(
        **{
          "@id": iri("entry", entry_id),
          "@type": "rs:Entry",
          "internalId": entry_id,
          "entity": iri("entity", e.entity_id),
          "entryNumber": e.number,
          "postingDate": _iso(e.posting_date),
          "postedAt": _iso(e.posted_at),
          "entryType": e.type,
          "entryStatus": e.status,
          "recordedBy": e.provenance,
          "memo": e.memo,
          "transaction": iri("transaction", e.transaction_id)
          if e.transaction_id
          else None,
          "generatedBy": iri("event", e.triggered_by_event_id)
          if e.triggered_by_event_id
          else None,
          "reversalOf": iri("entry", e.reversal_of) if e.reversal_of else None,
          "reversedBy": iri("entry", reversed_by[entry_id])
          if entry_id in reversed_by
          else None,
          "adjustmentKind": e.adjustment_kind,
          "adjustsEntity": iri("entity", e.adjusts_entity_id)
          if e.adjusts_entity_id
          else None,
        }
      )
    )
  for txn_id, t in sorted(transactions.items()):
    nodes.append(
      _node(
        **{
          "@id": iri("transaction", txn_id),
          "@type": "rs:Transaction",
          "internalId": txn_id,
          "entity": iri("entity", t.entity_id),
          "transactionType": t.type,
          "category": t.category,
          "amount": _dollars(t.amount_cents),
          "currency": t.currency,
          "transactionDate": _iso(t.date),
          "dueDate": _iso(t.due_date),
          "merchantName": t.merchant_name,
          "referenceNumber": t.reference_number,
          "documentation": t.description,
          "source": t.source,
          "generatedBy": iri("event", t.triggered_by_event_id)
          if t.triggered_by_event_id
          else None,
        }
      )
    )
  for event_id, ev in sorted(all_events.items()):
    nodes.append(
      _node(
        **{
          "@id": iri("event", event_id),
          "@type": ["rs:Event", "prov:Activity"],
          "internalId": event_id,
          "entity": iri("entity", ev.entity_id),
          "eventType": ev.event_type,
          "eventCategory": ev.event_category,
          "eventClass": ev.event_class,
          "eventAction": ev.event_action,
          "eventStatus": ev.status,
          "occurredAt": _iso(ev.occurred_at),
          "dueDate": _iso(ev.due_date),
          "amount": _dollars(ev.amount_cents) if ev.amount_cents is not None else None,
          "currency": ev.currency,
          "source": ev.source,
          "documentation": ev.description,
          "associatedWith": iri("agent", ev.agent_id) if ev.agent_id else None,
          "obligatedBy": iri("event", ev.obligated_by_event_id)
          if ev.obligated_by_event_id
          else None,
          "discharges": iri("event", ev.discharges_event_id)
          if ev.discharges_event_id
          else None,
          "replaces": iri("event", ev.replaces_event_id)
          if ev.replaces_event_id
          else None,
        }
      )
    )
  for d in discharges:
    nodes.append(
      _node(
        **{
          "@id": iri("discharge", f"{d.payment_event_id}/{d.obligation_event_id}"),
          "@type": "rs:Discharge",
          "generatedBy": iri("event", d.payment_event_id),
          "discharges": iri("event", d.obligation_event_id),
          "amount": _dollars(d.amount_cents),
        }
      )
    )
  for agent_id, a in sorted(agents.items()):
    nodes.append(
      _node(
        **{
          "@id": iri("agent", agent_id),
          "@type": ["rs:Agent", "prov:Agent"],
          "internalId": agent_id,
          "prefLabel": a.name,
          "agentType": a.agent_type,
        }
      )
    )
  nodes.sort(key=lambda n: n["@id"])

  return Lineage(
    nodes=nodes,
    bridges=dict(bridges),
    refused=refused,
    summary={
      "facts": len(facts),
      "facts_bridged": len(bridges),
      "facts_refused": len(refused),
      "contributions": len(contributions),
      "member_lines": len(members),
      "lines_described": len(described_ids),
      "entries": len(entries),
      "transactions": len(transactions),
      "events": len(events),
      "events_one_hop": len(hop_events),
      "agents": len(agents),
    },
  )


class LineageContextClashError(ValueError):
  """A lineage term already means something else in the holon's context, or
  a holon term the lineage nodes reuse is not bound there."""


# Terms the lineage nodes reuse from the holon's own context rather than
# redeclare. A document that does not bind them would drop the properties.
REUSED_HOLON_TERMS = (
  "balance",
  "category",
  "documentation",
  "entity",
  "internalId",
  "periodType",
  "prefLabel",
  "source",
)


def splice_lineage(holon: str, lineage: Lineage, *, root: str) -> str:
  """The holon xbrlkit wrote, with ``#lineage`` as its fourth named graph,
  ``derivedFrom`` on the facts it substantiates and the lineage terms in
  ``@context``. The output keeps xbrlkit's byte conventions (sorted keys,
  two-space indent) so the file is stable across builds."""
  document = json.loads(holon)
  context = document["@context"]
  clashes = sorted(
    term
    for term, meaning in LINEAGE_CONTEXT.items()
    if term in context and context[term] != meaning
  )
  if clashes:
    raise LineageContextClashError(
      f"lineage terms already bound in the holon context: {', '.join(clashes)}"
    )
  unbound = sorted(term for term in REUSED_HOLON_TERMS if term not in context)
  if unbound:
    raise LineageContextClashError(
      f"holon terms the lineage reuses are not in its context: {', '.join(unbound)}"
    )
  context.update(LINEAGE_CONTEXT)
  for graph in document["@graph"]:
    if str(graph.get("@id", "")).endswith("#scene"):
      for node in graph["@graph"]:
        fact_id = node.get("internalId")
        if node.get("@type") == "rs:Fact" and fact_id in lineage.bridges:
          node["derivedFrom"] = sorted(lineage.bridges[fact_id])
  document["@graph"].append({"@id": f"{root}#{LINEAGE_GRAPH}", "@graph": lineage.nodes})
  return json.dumps(document, indent=2, sort_keys=True, ensure_ascii=False)


def build_lineage_holon(
  session: Session,
  bundle: StatementBundle,
  *,
  describe: Literal["span", "full"] = "span",
) -> tuple[str, Lineage]:
  """The entitled holon — xbrlkit's three graphs plus ``#lineage`` — and the
  lineage it was built from, whose ``refused`` names any figure left without
  a bridge because its reconstruction missed the frozen hash.

  ``describe`` bounds the lines that get a node: ``"span"`` (the default)
  those dated inside the report's own columns, ``"full"`` every member line
  back to the opening of the books. Members are listed by IRI either way.
  Raises :class:`LineageTooLargeError` past :data:`LINEAGE_LINE_CEILING`.
  """
  root = str(holon_root(report_identifier(bundle)))
  describe_from: date | None = None
  if describe == "span":
    span = report_span(bundle)
    describe_from = span[0] if span else None
  lineage = build_lineage(
    bundle, SqlLedgerReader(session), root=root, describe_from=describe_from
  )
  if lineage.refused:
    logger.warning(
      "Lineage refused for %d fact(s) of report %s: the reconstruction did not "
      "reproduce the frozen hash (%s)",
      len(lineage.refused),
      bundle.report_meta.report_id if bundle.report_meta else "?",
      ", ".join(lineage.refused[:5]),
    )
  return splice_lineage(serialize_to_holon_jsonld(bundle), lineage, root=root), lineage


def serialize_to_lineage_holon_jsonld(
  session: Session,
  bundle: StatementBundle,
  *,
  describe: Literal["span", "full"] = "span",
) -> str:
  """The entitled holon as text; see :func:`build_lineage_holon`."""
  return build_lineage_holon(session, bundle, describe=describe)[0]


__all__ = [
  "LINEAGE_CONTEXT",
  "LINEAGE_GRAPH",
  "LINEAGE_LINE_CEILING",
  "LedgerReader",
  "Lineage",
  "LineageContextClashError",
  "LineageTooLargeError",
  "SqlLedgerReader",
  "build_lineage",
  "build_lineage_holon",
  "report_span",
  "serialize_to_lineage_holon_jsonld",
  "splice_lineage",
]
