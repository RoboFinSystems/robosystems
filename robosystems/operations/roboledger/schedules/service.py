"""Schedule service — operations for schedule lifecycle.

Schedules are structured fact tables of planned values (depreciation,
amortization, accruals) organized by element and period. They use the
existing XBRL taxonomy model: Taxonomy → Structure → Association → Fact.

This service is the shared operation layer called by both API routes
and MCP tools.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, time

from sqlalchemy import select, text, update
from sqlalchemy.orm import Session

from robosystems.logger import logger
from robosystems.models.api.fact_provenance import (
  AssertedProvenance,
  ScheduleProvenance,
)
from robosystems.models.extensions.element_trait import ElementTrait
from robosystems.models.extensions.roboledger import (
  Association,
  Element,
  Entry,
  Event,
  Fact,
  LineItem,
  Structure,
  Taxonomy,
)
from robosystems.models.extensions.rule import Rule
from robosystems.models.extensions.trait import Trait
from robosystems.operations.roboledger.fact_set import create_fact_set
from robosystems.utils.ulid import generate_prefixed_ulid

# Charlie Hoffman's Seattle Method "universal" conceptual-model has-part
# arcrole. A cm:Debit / cm:Credit ─has-part→ CoA-element arc declares that
# element as the debit / credit leg of a schedule's posting template.
CM_HAS_PART_ARCROLE = "https://github.com/seattlemethod/universal/cm/arcrole/has-part"

# ── Data classes ─────────────────────────────────────────────────────────


@dataclass
class EntryTemplate:
  """Template for generating closing entries from schedule facts."""

  debit_element_id: str
  credit_element_id: str
  entry_type: str = "closing"
  memo_template: str = ""
  auto_reverse: bool = False


@dataclass
class ScheduleMetadata:
  """Informational metadata about a schedule's parameters."""

  method: str = "straight_line"
  original_amount: int = 0  # cents
  residual_value: int = 0  # cents
  useful_life_months: int = 0
  asset_element_id: str | None = None
  periodic_amounts: list[int] | None = None  # cents; see Request model


@dataclass
class PeriodCloseItem:
  """One schedule's status for a fiscal period."""

  structure_id: str
  structure_name: str
  amount: float  # dollars
  status: str  # "pending", "drafted", "posted"
  entry_id: str | None
  reversal_entry_id: str | None = None
  reversal_status: str | None = None


@dataclass
class PeriodCloseStatus:
  """Overview of close progress for a fiscal period."""

  fiscal_period_start: date
  fiscal_period_end: date
  period_status: str  # "open", "closed"
  schedules: list[PeriodCloseItem]
  total_draft: int
  total_posted: int


@dataclass
class ClosingEntryResult:
  """Result of a create_closing_entry call.

  The `outcome` field describes what the call did:

  - **created**: no prior draft existed and a fact exists → new draft created
  - **unchanged**: prior draft exists and still matches the current schedule
    fact → no-op, returning the existing entry unchanged
  - **regenerated**: prior draft exists but is stale (amount or template
    changed on the schedule) → prior deleted, fresh draft created
  - **removed**: prior draft exists but schedule no longer produces an
    in-scope fact for this period (e.g., schedule truncated) → prior deleted,
    no new draft, `entry_id` is None
  - **skipped**: no prior draft, no in-scope fact for this period → nothing
    to do, `entry_id` is None

  Entry metadata fields (entry_id, status, amount, etc.) are populated
  when `outcome` is created/unchanged/regenerated; they are None when
  `outcome` is removed/skipped.
  """

  outcome: str  # created | unchanged | regenerated | removed | skipped
  entry_id: str | None = None
  status: str | None = None
  posting_date: date | None = None
  memo: str | None = None
  debit_element_id: str | None = None
  credit_element_id: str | None = None
  amount: float | None = None  # dollars
  reason: str | None = None
  reversal: ClosingEntryResult | None = None


# ── Service ──────────────────────────────────────────────────────────────


class ScheduleService:
  """Schedule lifecycle operations.

  All methods take an extensions DB session with search_path already
  set to the tenant schema. Callers (routes, MCP tools) handle session
  management.
  """

  def _build_schedule_structure(
    self,
    session: Session,
    *,
    name: str,
    taxonomy_id: str | None,
    element_ids: list[str],
    period_start: date,
    period_end: date,
    monthly_amount: int,
    entry_template: EntryTemplate,
    schedule_metadata: ScheduleMetadata | None,
    created_by: str,
    source_transaction_id: str | None,
  ) -> tuple[Structure, dict, dict, str]:
    """Create the Structure row, its element associations, and the
    cm:Debit/cm:Credit has-part posting arcs for a new schedule.

    Factored out of :meth:`create_schedule` so the rebuild path
    (``existing_structure``) can reuse the same fact-generation tail
    without re-creating the structure, associations, or arcs (those are
    preserved on rebuild).

    Returns ``(structure, metadata, artifact_mechanics, taxonomy_id)``.
    """
    # Resolve or create taxonomy
    if not taxonomy_id:
      taxonomy_id = self._ensure_schedule_taxonomy(session, created_by)

    metadata, artifact_mechanics = self._build_schedule_definition_blobs(
      name=name,
      monthly_amount=monthly_amount,
      period_start=period_start,
      period_end=period_end,
      entry_template=entry_template,
      schedule_metadata=schedule_metadata,
      source_transaction_id=source_transaction_id,
    )

    structure = Structure(
      name=name,
      block_type="schedule",
      taxonomy_id=taxonomy_id,
      concept_arrangement="roll_forward",
      artifact_mechanics=artifact_mechanics,
      metadata_=metadata,
      created_by=created_by,
    )
    session.add(structure)
    session.flush()

    # For schedules, from_element_id is the first element (debit) to anchor
    # the presentation chain. to_element_id is the element being associated.
    anchor_element_id = element_ids[0] if element_ids else None
    for i, element_id in enumerate(element_ids, 1):
      assoc = Association(
        structure_id=structure.id,
        from_element_id=anchor_element_id,
        to_element_id=element_id,
        association_type="presentation",
        order_value=float(i),
        created_by=created_by,
      )
      session.add(assoc)

    # Emit Conceptual-Model has-part posting arcs so the debit/credit pairing
    # lives as first-class, queryable atoms of the schedule IB (its envelope
    # `connections`) rather than only as opaque entry_template mechanics:
    #   cm:Debit  ─has-part→ debit account
    #   cm:Credit ─has-part→ credit account
    # Best-effort: a tenant whose library lacks the cm concepts skips this
    # silently, and entry_template remains the generation source of truth.
    cm_role_ids = {
      e.qname: e.id
      for e in session.execute(
        select(Element).where(
          Element.source == "cm",
          Element.qname.in_(("cm:Debit", "cm:Credit")),
        )
      ).scalars()
    }
    for role_qname, account_element_id in (
      ("cm:Debit", entry_template.debit_element_id),
      ("cm:Credit", entry_template.credit_element_id),
    ):
      role_id = cm_role_ids.get(role_qname)
      if role_id is None or account_element_id is None:
        continue
      session.add(
        Association(
          structure_id=structure.id,
          from_element_id=role_id,
          to_element_id=account_element_id,
          association_type="has-part",
          arcrole=CM_HAS_PART_ARCROLE,
          created_by=created_by,
        )
      )

    return structure, metadata, artifact_mechanics, taxonomy_id

  @staticmethod
  def _build_schedule_definition_blobs(
    *,
    name: str,
    monthly_amount: int,
    period_start: date,
    period_end: date,
    entry_template: EntryTemplate,
    schedule_metadata: ScheduleMetadata | None,
    source_transaction_id: str | None,
  ) -> tuple[dict, dict]:
    """Build the ``metadata_`` and ``artifact_mechanics`` JSONB blobs that
    persist a schedule's reproducible definition.

    ``monthly_amount`` + ``period_start`` + ``period_end`` are stored
    alongside the entry template + schedule metadata so a later
    ``rebuild_schedule`` can reconstruct the exact generation inputs
    unambiguously (no derivation from facts required).
    """
    metadata: dict[str, object] = {
      "entry_template": {
        "debit_element_id": entry_template.debit_element_id,
        "credit_element_id": entry_template.credit_element_id,
        "entry_type": entry_template.entry_type,
        "memo_template": entry_template.memo_template or f"Monthly schedule - {name}",
        "auto_reverse": entry_template.auto_reverse,
      },
      # Reproducible generation inputs — let rebuild_schedule reconstruct
      # the schedule from metadata alone without re-deriving from facts.
      "monthly_amount": monthly_amount,
      "period_start": period_start.isoformat(),
      "period_end": period_end.isoformat(),
    }
    if schedule_metadata:
      metadata["schedule_metadata"] = {
        "method": schedule_metadata.method,
        "original_amount": schedule_metadata.original_amount,
        "residual_value": schedule_metadata.residual_value,
        "useful_life_months": schedule_metadata.useful_life_months,
        "asset_element_id": schedule_metadata.asset_element_id,
        "periodic_amounts": schedule_metadata.periodic_amounts,
      }

    # Both blobs are stamped: artifact_mechanics is the typed column the
    # envelope builder reads, and metadata_ stays populated so rows without
    # artifact_mechanics still resolve.
    #
    # periods_with_entries is a transient read-time value (queried from facts),
    # not a stored property — deliberately excluded here rather than using
    # ScheduleMechanics.model_dump(); the envelope builder injects it.
    artifact_mechanics: dict[str, object] = {
      "kind": "closing_entry_generator",
      "entry_template": metadata["entry_template"],
      "schedule_metadata": metadata.get("schedule_metadata"),
      "source_transaction_id": source_transaction_id,
      "monthly_amount": monthly_amount,
      "period_start": period_start.isoformat(),
      "period_end": period_end.isoformat(),
    }
    return metadata, artifact_mechanics

  def create_schedule(
    self,
    session: Session,
    *,
    name: str,
    taxonomy_id: str | None,
    element_ids: list[str],
    period_start: date,
    period_end: date,
    monthly_amount: int,
    entry_template: EntryTemplate,
    schedule_metadata: ScheduleMetadata | None = None,
    created_by: str,
    closed_through: date | None = None,
    source_transaction_id: str | None = None,
    existing_structure: Structure | None = None,
  ) -> Structure:
    """Create a schedule with pre-generated facts.

    Creates a structure (type=schedule), associations to the referenced
    elements, and facts for each monthly period between period_start and
    period_end.

    ``monthly_amount`` is in cents. ``taxonomy_id=None`` uses or creates a
    default "Schedules" taxonomy. ``element_ids`` are the elements the
    schedule spans (e.g. depreciation expense + accumulated depreciation).

    ``closed_through`` splits the generated facts: those with
    ``period_end <= closed_through`` are tagged ``fact_scope='historical'``
    (already reflected in opening balances, ignored by the close workflow),
    the rest ``in_scope``. ``None`` makes every fact in_scope.

    ``existing_structure`` regenerates facts + the obligation chain **in
    place** on that Structure instead of creating a new one: the structure,
    its element associations, and its cm has-part arcs survive; only the
    definition blobs, facts, and obligation events are rewritten, and
    ``element_ids`` is ignored. The caller (``rebuild_schedule``) must void
    the old obligation chain and delete the old facts/rules first.

    Returns the created (or rebuilt) Structure.
    """
    if existing_structure is not None:
      structure = existing_structure
      taxonomy_id = str(structure.taxonomy_id)
      metadata, artifact_mechanics = self._build_schedule_definition_blobs(
        name=name,
        monthly_amount=monthly_amount,
        period_start=period_start,
        period_end=period_end,
        entry_template=entry_template,
        schedule_metadata=schedule_metadata,
        source_transaction_id=source_transaction_id,
      )
      structure.metadata_ = metadata
      structure.artifact_mechanics = artifact_mechanics
      session.flush()
    else:
      structure, metadata, artifact_mechanics, taxonomy_id = (
        self._build_schedule_structure(
          session,
          name=name,
          taxonomy_id=taxonomy_id,
          element_ids=element_ids,
          period_start=period_start,
          period_end=period_end,
          monthly_amount=monthly_amount,
          entry_template=entry_template,
          schedule_metadata=schedule_metadata,
          created_by=created_by,
          source_transaction_id=source_transaction_id,
        )
      )

    # Generate facts for each monthly period
    fact_set_id = generate_prefixed_ulid("fs")
    entity_id = self._get_entity_id(session)

    # Block = Fact Set. Create the fact_sets row first so the facts we
    # stamp below reference an existing parent; lets the FK
    # facts.fact_set_id → fact_sets.id hold without orphan risk.
    # Explicit flush so the FactSet row hits the DB before the bulk
    # fact INSERTs — SQLAlchemy's session-flush ordering is by INSERT
    # statement type, not by FK dependency, and would otherwise emit
    # facts INSERTs ahead of the FactSet row in the same flush.
    # A custom periodic-amounts curve is an *asserted* projection (the
    # caller supplied the balanced amounts); a template/straight-line
    # schedule is *schedule*-derived from method + params.
    if schedule_metadata is not None and schedule_metadata.periodic_amounts is not None:
      provenance = AssertedProvenance(
        source_system="custom_amortization_curve",
        asserted_by=created_by,
        basis_note=f"schedule structure={structure.id}",
      )
    else:
      provenance = ScheduleProvenance(
        structure_id=structure.id,
        method=schedule_metadata.method if schedule_metadata else "straight_line",
        params=(
          {
            "original_amount": schedule_metadata.original_amount,
            "residual_value": schedule_metadata.residual_value,
            "useful_life_months": schedule_metadata.useful_life_months,
          }
          if schedule_metadata
          else None
        ),
      )
    create_fact_set(
      session,
      id=fact_set_id,
      structure_id=structure.id,
      period_start=period_start,
      period_end=period_end,
      factset_type="schedule",
      entity_id=entity_id,
      created_by=created_by,
      provenance=provenance,
    )
    session.flush()

    periods = _generate_monthly_periods(period_start, period_end)
    amount_dollars = round(monthly_amount / 100.0, 2)
    original_dollars = (
      round(schedule_metadata.original_amount / 100.0, 2)
      if schedule_metadata and schedule_metadata.original_amount > 0
      else None
    )

    # The credit fact tracks the running balance of the credited account, and
    # its direction depends on that account's role:
    #   • contra-asset (Accumulated Depreciation/Amortization) or a credit-
    #     balance account (a liability) — the period credit INCREASES it, so
    #     the balance accumulates UP from a zero opening: value = accumulated.
    #   • directly-credited asset (a prepaid) — the period credit DECREASES it,
    #     so the balance draws DOWN from its opening cost: value = original -
    #     accumulated.
    # A contra-asset is asset/debit-normal in QuickBooks, so ``balance_type``
    # alone can't distinguish it from a prepaid — the contraAsset EFS trait
    # (applied at sync from the CoA AccountSubType) is the authoritative role
    # signal. Only a debit-balance, non-contra asset draws down.
    credit_balance_type = session.execute(
      select(Element.balance_type).where(Element.id == entry_template.credit_element_id)
    ).scalar()
    # Only debit-normal accounts can be contra-assets, so skip the trait
    # lookup for credit-balance accounts (liabilities, deferred revenue) —
    # the common case.
    credit_is_contra = credit_balance_type == "debit" and _element_is_contra_asset(
      session, entry_template.credit_element_id
    )
    credit_draws_down = credit_balance_type == "debit" and not credit_is_contra
    # Drawing down needs an opening cost basis. When the author didn't supply
    # original_amount, derive it from the straight-line curve (monthly x
    # periods) so the prepaid balance declines from cost to ~0 instead of
    # falling back to the (wrong-for-a-prepaid) accumulating form. Custom
    # amortization curves always carry original_amount (validated below), so
    # this only fills the straight-line gap; contra/accumulate schedules are
    # untouched (they open at zero and need no cost basis).
    if credit_draws_down and original_dollars is None and periods:
      original_dollars = round(amount_dollars * len(periods), 2)
    # If a draw-down still has no opening (e.g. no periods), fall back to
    # accumulate rather than emit a garbage curve.
    credit_draws_down = credit_draws_down and original_dollars is not None

    # Custom amortization curve: caller supplies one integer (cents) per
    # period, pre-balanced. The generator uses the explicit values
    # instead of the straight-line formula. Use cases: day-count
    # interest accrual, effective-interest bond discount amortization,
    # variable lease payments, any pre-computed schedule.
    #
    # Invariants (enforced here so the downstream loop stays simple):
    #   - len(periodic_amounts) == len(periods)
    #   - sum(periodic_amounts) == original_amount (cents, exact)
    #   - each entry >= 0 (the SumEquals rule assumes non-negative terms)
    custom_amounts_dollars: list[float] | None = None
    if schedule_metadata and schedule_metadata.periodic_amounts is not None:
      if len(schedule_metadata.periodic_amounts) != len(periods):
        raise ValueError(
          f"periodic_amounts length ({len(schedule_metadata.periodic_amounts)}) "
          f"does not match period count ({len(periods)}) between "
          f"{period_start} and {period_end}."
        )
      if any(x < 0 for x in schedule_metadata.periodic_amounts):
        raise ValueError("periodic_amounts entries must be non-negative.")
      total_cents = sum(schedule_metadata.periodic_amounts)
      if total_cents != schedule_metadata.original_amount:
        raise ValueError(
          f"periodic_amounts sum ({total_cents} cents) does not equal "
          f"original_amount ({schedule_metadata.original_amount} cents)."
        )
      custom_amounts_dollars = [
        round(c / 100.0, 2) for c in schedule_metadata.periodic_amounts
      ]

    # Roll-forward opening balances. A roll_forward block has a Beginning
    # Balance, so every rolled balance gets one instant fact at the schedule's
    # first-period start (accumulated = 0) — making the series Begin → movements
    # → End rather than starting after the first movement. Same formulas as the
    # in-loop balance/NBV facts at accumulated_debit == 0: a contra opens at 0;
    # a directly-credited asset (and the net-book-value asset) opens at gross.
    if periods:
      opening_instant = periods[0][0]
      opening_scope = (
        "historical"
        if closed_through and opening_instant <= closed_through
        else "in_scope"
      )
      opening_credit_value = (
        original_dollars if credit_draws_down and original_dollars is not None else 0.0
      )
      session.add(
        Fact(
          element_id=entry_template.credit_element_id,
          value=opening_credit_value,
          period_start=None,
          period_end=opening_instant,
          period_type="instant",
          unit="USD",
          entity_id=entity_id,
          structure_id=structure.id,
          fact_set_id=fact_set_id,
          fact_scope=opening_scope,
        )
      )
      if (
        schedule_metadata
        and schedule_metadata.asset_element_id
        and schedule_metadata.asset_element_id != entry_template.credit_element_id
      ):
        session.add(
          Fact(
            element_id=schedule_metadata.asset_element_id,
            value=original_dollars or amount_dollars,
            period_start=None,
            period_end=opening_instant,
            period_type="instant",
            unit="USD",
            entity_id=entity_id,
            structure_id=structure.id,
            fact_set_id=fact_set_id,
            fact_scope=opening_scope,
          )
        )

    accumulated_debit = 0.0

    for i, (p_start, p_end) in enumerate(periods):
      is_last = i == len(periods) - 1

      if custom_amounts_dollars is not None:
        # Pre-balanced curve — use verbatim, no rounding fudge needed.
        period_amount = custom_amounts_dollars[i]
      else:
        # Straight-line: final period absorbs rounding so
        # Σ(debit facts) == original_amount exactly.
        period_amount = (
          round(original_dollars - accumulated_debit, 2)
          if is_last and original_dollars is not None
          else amount_dollars
        )
      accumulated_debit = round(accumulated_debit + period_amount, 2)

      # Scope: facts in periods already closed are historical (not acted on
      # by the close workflow). Only periods > closed_through are in_scope.
      fact_scope = (
        "historical" if closed_through and p_end <= closed_through else "in_scope"
      )

      # Fact for the periodic amount (expense/amortization)
      session.add(
        Fact(
          element_id=entry_template.debit_element_id,
          value=period_amount,
          period_start=p_start,
          period_end=p_end,
          period_type="duration",
          unit="USD",
          entity_id=entity_id,
          structure_id=structure.id,
          fact_set_id=fact_set_id,
          fact_scope=fact_scope,
        )
      )

      # Fact for the credited account's running balance — contra accumulates,
      # a directly-credited asset draws down (see credit_draws_down above).
      credit_value = (
        round(original_dollars - accumulated_debit, 2)
        if credit_draws_down and original_dollars is not None
        else accumulated_debit
      )
      session.add(
        Fact(
          element_id=entry_template.credit_element_id,
          value=credit_value,
          period_start=p_start,
          period_end=p_end,
          period_type="instant",
          unit="USD",
          entity_id=entity_id,
          structure_id=structure.id,
          fact_set_id=fact_set_id,
          fact_scope=fact_scope,
        )
      )

      # Net book value if we have a DISTINCT asset element. Skip when the asset
      # is the credited account itself (a direct-drawdown prepaid) — the credit
      # fact above already carries that account's running balance, so emitting
      # NBV here would double-stamp the same element/period.
      if (
        schedule_metadata
        and schedule_metadata.asset_element_id
        and schedule_metadata.asset_element_id != entry_template.credit_element_id
      ):
        session.add(
          Fact(
            element_id=schedule_metadata.asset_element_id,
            value=round((original_dollars or amount_dollars) - accumulated_debit, 2),
            period_start=p_start,
            period_end=p_end,
            period_type="instant",
            unit="USD",
            entity_id=entity_id,
            structure_id=structure.id,
            fact_set_id=fact_set_id,
            fact_scope=fact_scope,
          )
        )

    # Auto-generate a SumEquals rule so the engine can verify that
    # the sum of period debit facts equals original_amount. The rule binds
    # the debit element by id (via variable_element_id) — tenant CoA accounts
    # carry a null qname, so qname-only binding would skip the rule entirely
    # for the normal case (a schedule debiting a CoA expense account). qname
    # is still recorded for display when present.
    if original_dollars is not None:
      debit_qname: str | None = session.execute(
        select(Element.qname)
        .where(Element.id == entry_template.debit_element_id)
        .limit(1)
      ).scalar()
      var_name = "periodic_amount"
      session.add(
        Rule(
          taxonomy_id=structure.taxonomy_id,
          rule_category="ReportingSystemSpecificRule",
          rule_pattern="SumEquals",
          rule_expression=f"sum(${var_name}) = {original_dollars}",
          rule_severity="error",
          rule_origin="native",
          target_kind="structure",
          target_structure_id=structure.id,
          rule_variables=[
            {
              "variable_name": var_name,
              "variable_qname": debit_qname,
              "variable_element_id": entry_template.debit_element_id,
            }
          ],
          metadata_={"expected_total": original_dollars},
          created_by=created_by,
        )
      )

    # Materialize the obligation register: one `schedule_created` event
    # (originator) + one `pending` `schedule_entry_due` event per period,
    # linked via `obligated_by_event_id`. The obligation sensor flips the
    # pending events to `classified` at each period boundary and dispatches
    # the existing handler.
    schedule_created_event_id, pending_event_count = (
      self._materialize_pending_obligations(
        session,
        structure=structure,
        taxonomy_id=taxonomy_id,
        period_start=period_start,
        period_end=period_end,
        monthly_amount=monthly_amount,
        periods=periods,
        closed_through=closed_through,
        created_by=created_by,
      )
    )

    # Stash the originator event id on the structure so disposal and
    # supersession paths can find the obligation chain without an extra
    # query.
    metadata["schedule_created_event_id"] = schedule_created_event_id
    metadata["pending_event_count"] = pending_event_count
    structure.metadata_ = metadata
    artifact_mechanics["schedule_created_event_id"] = schedule_created_event_id
    artifact_mechanics["pending_event_count"] = pending_event_count
    structure.artifact_mechanics = artifact_mechanics

    # On the rebuild path the Structure already lives in the DB, so the
    # in-place JSONB reassignment above must be flagged for SQLAlchemy to
    # emit the UPDATE (a fresh-create row is dirty regardless).
    if existing_structure is not None:
      from sqlalchemy.orm.attributes import flag_modified

      flag_modified(structure, "metadata_")
      flag_modified(structure, "artifact_mechanics")

    session.flush()
    return structure

  def _materialize_pending_obligations(
    self,
    session: Session,
    *,
    structure: Structure,
    taxonomy_id: str,
    period_start: date,
    period_end: date,
    monthly_amount: int,
    periods: list[tuple[date, date]],
    created_by: str,
    closed_through: date | None = None,
  ) -> tuple[str, int]:
    """Emit `schedule_created` + N `schedule_entry_due` events.

    Periods at or before ``closed_through`` are emitted as ``voided``
    (with `metadata.void_reason='historical'`) so the close-period
    gate doesn't treat backfilled historical periods as outstanding
    obligations. Periods after ``closed_through`` (and all periods
    when ``closed_through`` is None) are emitted as ``pending``.

    The returned ``pending_event_count`` reflects only the still-open
    obligations — historical voids are excluded from the count.

    IDs are generated Python-side so the obligation linkage is set
    without depending on a session flush — this keeps the linkage
    verifiable in MagicMock-based tests and lets us add all rows in a
    single batch.

    Returns ``(schedule_created_event_id, pending_event_count)``.
    """
    schedule_created_event_id = generate_prefixed_ulid("evt")
    now = datetime.now(UTC)

    pending_count = sum(
      1 for _p_start, p_end in periods if not closed_through or p_end > closed_through
    )

    # `event_category='other'` reflects that schedule_created is an
    # operator/system act of recording an obligation register, not a
    # GL-impact event in any of the standard REA categories. The
    # actual GL events are the per-period schedule_entry_due children
    # (event_category='recognition'), which the handler dispatches.
    session.add(
      Event(
        id=schedule_created_event_id,
        event_type="schedule_created",
        event_category="other",
        event_class="economic",
        occurred_at=now,
        source="schedule",
        status="committed",
        metadata_={
          "schedule_id": structure.id,
          "taxonomy_id": taxonomy_id,
          "period_start": period_start.isoformat(),
          "period_end": period_end.isoformat(),
          "monthly_amount": monthly_amount,
          "pending_event_count": pending_count,
        },
        created_at=now,
        created_by=created_by,
      )
    )

    for p_start, p_end in periods:
      # End-of-day so a same-day sensor sweep at midnight picks the
      # period up exactly when it closes.
      occurred_at = datetime.combine(p_end, time(23, 59, 59), tzinfo=UTC)
      is_historical = closed_through is not None and p_end <= closed_through
      event_metadata: dict[str, object] = {
        "schedule_id": structure.id,
        "posting_date": p_end.isoformat(),
        "period_start": p_start.isoformat(),
        "period_end": p_end.isoformat(),
      }
      if is_historical:
        # Stamped at create time — never picked up by the close gate
        # or the promotion sensor (both filter on status='pending').
        # Kept in the obligation register as an audit-trail "I knew
        # about this period; it predates the close".
        event_metadata["void_reason"] = "historical"
      session.add(
        Event(
          id=generate_prefixed_ulid("evt"),
          event_type="schedule_entry_due",
          event_category="recognition",
          event_class="economic",
          occurred_at=occurred_at,
          source="schedule",
          status="voided" if is_historical else "pending",
          obligated_by_event_id=schedule_created_event_id,
          metadata_=event_metadata,
          created_at=now,
          created_by=created_by,
        )
      )

    return schedule_created_event_id, pending_count

  def void_pending_obligations(
    self,
    session: Session,
    *,
    structure: Structure,
    void_reason: str,
    voided_by_event_id: str | None = None,
  ) -> int:
    """Void all `pending` schedule_entry_due events for a schedule.

    Shared helper for the two lifecycle paths that retire a schedule's
    remaining obligations:

    - **Disposal** (asset_disposed handler): pass ``voided_by_event_id``
      = the disposal event id so the audit chain answers "what voided
      this obligation?". Caller also clears the schedule's SumEquals
      rule and posts the disposal entry.
    - **Schedule deletion** (cmd_delete_schedule): pass no
      ``voided_by_event_id`` — the originating ``schedule_created``
      event row is about to be deleted along with the schedule, so the
      pending children are voided pre-emptively to keep them from
      tripping the close-period gate after their parent disappears.

    Returns the number of voided rows. Returns 0 (no-op) when the
    schedule has no ``schedule_created_event_id`` stamped (rows from
    before the obligation register existed) or when no pending
    obligations exist.

    Also decrements the originator's ``metadata.pending_event_count``
    so reads of the obligation register stay accurate (the count is
    stamped at create time and represents "still-open obligations").
    """
    from sqlalchemy.orm.attributes import flag_modified

    metadata = structure.metadata_ or {}
    schedule_created_event_id = metadata.get("schedule_created_event_id")
    if not schedule_created_event_id:
      # Fallback: recover the originator from the obligations' own link —
      # every schedule_entry_due carries metadata.schedule_id == this
      # structure's id and obligated_by_event_id == the schedule_created
      # originator. Without it, a structure missing the
      # `schedule_created_event_id` stamp no-ops silently here and orphans its
      # pending obligations on delete, leaving phantom obligations that
      # double-post at close.
      schedule_created_event_id = session.execute(
        select(Event.obligated_by_event_id)
        .where(
          Event.event_type == "schedule_entry_due",
          Event.metadata_["schedule_id"].astext == structure.id,
          Event.obligated_by_event_id.isnot(None),
        )
        .limit(1)
      ).scalar()
    if not schedule_created_event_id:
      return 0

    update_values: dict[str, object] = {"status": "voided"}
    if voided_by_event_id is not None:
      update_values["replaced_by_event_id"] = voided_by_event_id

    result = session.execute(
      update(Event)
      .where(
        Event.obligated_by_event_id == schedule_created_event_id,
        Event.status == "pending",
      )
      .values(**update_values)
    )
    voided_count = int(result.rowcount or 0)
    if voided_count == 0:
      return 0

    # Decrement the originator's pending_event_count and stamp the
    # void_reason so audits can attribute the void wave to its cause.
    originator = session.get(Event, schedule_created_event_id)
    if originator is not None:
      orig_meta = dict(originator.metadata_ or {})
      current = int(orig_meta.get("pending_event_count", 0))
      orig_meta["pending_event_count"] = max(0, current - voided_count)
      orig_meta.setdefault("void_history", []).append(
        {
          "voided_count": voided_count,
          "void_reason": void_reason,
          "voided_by_event_id": voided_by_event_id,
          "voided_at": datetime.now(UTC).isoformat(),
        }
      )
      originator.metadata_ = orig_meta
      flag_modified(originator, "metadata_")

    return voided_count

  def supersede_pending_obligations(
    self,
    session: Session,
    *,
    structure: Structure,
    created_by: str,
  ) -> int:
    """Void each pending obligation and emit a replacement linked via the
    correction chain.

    Used by ``update_schedule`` when the ``entry_template`` changes — the
    period range stays the same (periods aren't editable), but the new
    template represents a different intent for each future closing entry.
    Re-materializing the pending events under the same originating
    ``schedule_created`` event keeps the obligation register pointing at
    a consistent set of "what's still due" with a queryable supersession
    history (``replaces_event_id`` ↔ ``replaced_by_event_id``).

    Already-classified / fulfilled / voided events are untouched — the
    new template applies prospectively. Returns the number of
    replacement events created.

    Returns 0 when the schedule has no ``schedule_created_event_id``
    stamped (rows from before the obligation register existed) or when
    no pending events exist.
    """
    metadata = structure.metadata_ or {}
    schedule_created_event_id = metadata.get("schedule_created_event_id")
    if not schedule_created_event_id:
      return 0

    # Locked: this reads `pending` obligations and voids them, and the
    # promotion sweep reads the same rows to classify and draft their closing
    # entries. Unlocked, both can proceed from the same snapshot — the sweep
    # drafts a GL entry for an obligation this call is voiding, and the
    # schedule ends up with a closing entry for a period it no longer has an
    # obligation for. Whichever gets the lock first wins; the other re-reads.
    existing_pending = list(
      session.execute(
        select(Event)
        .where(
          Event.obligated_by_event_id == schedule_created_event_id,
          Event.status == "pending",
        )
        .with_for_update()
      ).scalars()
    )
    if not existing_pending:
      return 0

    now = datetime.now(UTC)
    new_count = 0

    for old_evt in existing_pending:
      old_meta = old_evt.metadata_ or {}
      period_start_iso = old_meta.get("period_start")
      period_end_iso = old_meta.get("period_end")
      if not period_start_iso or not period_end_iso:
        # Malformed row — skip rather than crash. The void below is also
        # skipped so we don't strand a row in an inconsistent state.
        logger.warning(
          "supersede_pending_obligations: skipping malformed event %s "
          "on schedule %s (missing period_start/period_end metadata)",
          old_evt.id,
          structure.id,
        )
        continue

      new_event_id = generate_prefixed_ulid("evt")
      # Both sides of the supersession chain so the audit trail resolves
      # backward and forward: same shape as update_event_block's superseded
      # transition.
      old_evt.status = "voided"
      old_evt.replaced_by_event_id = new_event_id

      period_end = date.fromisoformat(period_end_iso)
      occurred_at = datetime.combine(period_end, time(23, 59, 59), tzinfo=UTC)

      session.add(
        Event(
          id=new_event_id,
          event_type="schedule_entry_due",
          event_category="recognition",
          event_class="economic",
          occurred_at=occurred_at,
          source="schedule",
          status="pending",
          obligated_by_event_id=schedule_created_event_id,
          replaces_event_id=old_evt.id,
          metadata_={
            "schedule_id": structure.id,
            "posting_date": period_end_iso,
            "period_start": period_start_iso,
            "period_end": period_end_iso,
          },
          created_at=now,
          created_by=created_by,
        )
      )
      new_count += 1

    return new_count

  def get_period_close_status(
    self,
    session: Session,
    period_start: date,
    period_end: date,
  ) -> PeriodCloseStatus:
    """Get close status for all schedules in a fiscal period."""
    # Get all schedule structures with their facts and best entry status for this period.
    # The best_entry CTE picks the most-advanced entry per structure
    # (posted > draft > reversed, via CASE ordering).
    result = session.execute(
      text("""
        WITH best_entry AS (
          SELECT DISTINCT ON (source_structure_id)
            source_structure_id,
            id AS entry_id,
            status AS entry_status
          FROM entries
          WHERE posting_date >= :period_start
            AND posting_date <= :period_end
            AND source_structure_id IS NOT NULL
            AND type != 'reversing'
          ORDER BY source_structure_id,
            CASE status WHEN 'posted' THEN 1 WHEN 'draft' THEN 2 ELSE 3 END
        ),
        reversal AS (
          SELECT DISTINCT ON (reversal_of)
            reversal_of,
            id AS reversal_entry_id,
            status AS reversal_status
          FROM entries
          WHERE type = 'reversing'
            AND reversal_of IS NOT NULL
          ORDER BY reversal_of,
            CASE status WHEN 'posted' THEN 1 WHEN 'draft' THEN 2 ELSE 3 END
        )
        SELECT
          s.id AS structure_id,
          s.name AS structure_name,
          s.metadata AS metadata,
          f.value AS amount,
          be.entry_id,
          be.entry_status,
          r.reversal_entry_id,
          r.reversal_status
        FROM structures s
        LEFT JOIN facts f ON f.structure_id = s.id
          AND f.period_start >= :period_start
          AND f.period_end <= :period_end
          AND f.element_id = (s.metadata->'entry_template'->>'debit_element_id')
          AND f.fact_scope = 'in_scope'
        LEFT JOIN best_entry be ON be.source_structure_id = s.id
        LEFT JOIN reversal r ON r.reversal_of = be.entry_id
        WHERE s.block_type = 'schedule'
          AND s.is_active = true
        ORDER BY s.name
      """),
      {"period_start": period_start, "period_end": period_end},
    )

    fp_result = session.execute(
      text("""
        SELECT status FROM fiscal_periods
        WHERE start_date <= :period_start AND end_date >= :period_end
        LIMIT 1
      """),
      {"period_start": period_start, "period_end": period_end},
    )
    fp_row = fp_result.fetchone()
    period_status = fp_row.status if fp_row else "open"

    items: list[PeriodCloseItem] = []
    total_draft = 0
    total_posted = 0

    for row in result:
      if row.entry_status == "posted":
        status = "posted"
        total_posted += 1
      elif row.entry_status == "draft":
        status = "drafted"
        total_draft += 1
      else:
        status = "pending"

      items.append(
        PeriodCloseItem(
          structure_id=row.structure_id,
          structure_name=row.structure_name,
          amount=row.amount or 0.0,
          status=status,
          entry_id=row.entry_id,
          reversal_entry_id=row.reversal_entry_id,
          reversal_status=row.reversal_status,
        )
      )

    return PeriodCloseStatus(
      fiscal_period_start=period_start,
      fiscal_period_end=period_end,
      period_status=period_status,
      schedules=items,
      total_draft=total_draft,
      total_posted=total_posted,
    )

  def create_closing_entry(
    self,
    session: Session,
    *,
    structure_id: str,
    posting_date: date,
    period_start: date,
    period_end: date,
    created_by: str,
    memo: str | None = None,
  ) -> ClosingEntryResult:
    """Idempotently create (or refresh) a draft closing entry from a schedule.

    This method is safe to call repeatedly — it reconciles the draft state
    with the current schedule, regenerating or removing the draft when the
    schedule has been edited since the prior call.

    Five outcomes:

    - **created** — no prior draft, fact exists → new draft created
    - **unchanged** — prior draft matches current schedule fact → no-op
    - **regenerated** — prior draft is stale (schedule amount or template
      changed) → old deleted, fresh draft created
    - **removed** — prior draft exists but schedule no longer produces an
      in-scope fact for this period → old deleted, nothing created
    - **skipped** — no prior draft, no in-scope fact → nothing to do

    ``ValueError`` is reserved for invalid inputs: structure not found, no
    entry template, or an entry for this period that is already posted (use
    the reopen flow to change a posted entry). Stale or non-existent drafts
    produce structured outcomes, not errors.
    """
    structure = session.get(Structure, structure_id)
    if not structure or structure.block_type != "schedule":
      raise ValueError(f"Schedule structure '{structure_id}' not found")

    template = (structure.metadata_ or {}).get("entry_template")
    if not template:
      raise ValueError(f"Schedule '{structure_id}' has no entry template")

    debit_element_id = template["debit_element_id"]
    credit_element_id = template["credit_element_id"]

    # ── Look up the existing entry (if any) for this structure + period ──
    existing_row = session.execute(
      text("""
        SELECT id, status
        FROM entries
        WHERE source_structure_id = :structure_id
          AND posting_date >= :period_start
          AND posting_date <= :period_end
        ORDER BY created_at DESC
        LIMIT 1
      """),
      {
        "structure_id": structure_id,
        "period_start": period_start,
        "period_end": period_end,
      },
    ).fetchone()

    existing_entry_id: str | None = existing_row.id if existing_row else None
    if existing_row and existing_row.status == "posted":
      raise ValueError(
        f"Closing entry for schedule '{structure_id}' in period "
        f"{period_start} to {period_end} has already been posted. "
        "Use the reopen flow to modify it."
      )

    # ── Find the current in_scope fact for this period ──
    fact_row = session.execute(
      text("""
        SELECT value FROM facts
        WHERE structure_id = :structure_id
          AND element_id = :element_id
          AND period_start >= :period_start
          AND period_end <= :period_end
          AND fact_scope = 'in_scope'
        LIMIT 1
      """),
      {
        "structure_id": structure_id,
        "element_id": debit_element_id,
        "period_start": period_start,
        "period_end": period_end,
      },
    ).fetchone()

    # ── Cases where no fact exists ──
    if not fact_row:
      if existing_entry_id:
        # Schedule no longer covers this period — remove the stale draft
        self._delete_draft_entry(session, existing_entry_id)
        return ClosingEntryResult(
          outcome="removed",
          reason=(
            "Schedule no longer produces an in-scope fact for this period. "
            "The stale draft has been deleted."
          ),
        )
      return ClosingEntryResult(
        outcome="skipped",
        reason=f"No in-scope fact for element '{debit_element_id}' in this period.",
      )

    amount_dollars = fact_row.value
    amount_cents = round(amount_dollars * 100)

    # Build the memo first so we can detect memo staleness too.
    memo_template = template.get("memo_template", "")
    entry_memo = memo or memo_template.replace("{structure_name}", structure.name)

    # ── Cases where an existing draft exists ──
    if existing_entry_id:
      current = session.execute(
        text("""
          SELECT
            e.memo            AS memo,
            li_dr.element_id  AS dr_element,
            li_dr.debit_amount AS dr_amount,
            li_cr.element_id  AS cr_element,
            li_cr.credit_amount AS cr_amount
          FROM entries e
          LEFT JOIN line_items li_dr ON li_dr.entry_id = e.id AND li_dr.debit_amount > 0
          LEFT JOIN line_items li_cr ON li_cr.entry_id = e.id AND li_cr.credit_amount > 0
          WHERE e.id = :entry_id
          LIMIT 1
        """),
        {"entry_id": existing_entry_id},
      ).fetchone()

      is_stale = (
        current is None
        or current.dr_element != debit_element_id
        or current.cr_element != credit_element_id
        or int(current.dr_amount or 0) != amount_cents
        or int(current.cr_amount or 0) != amount_cents
        or (current.memo or "") != (entry_memo or "")
      )

      if not is_stale:
        # Unchanged — return existing draft without touching the DB
        return ClosingEntryResult(
          outcome="unchanged",
          entry_id=existing_entry_id,
          status="draft",
          posting_date=posting_date,
          memo=current.memo,
          debit_element_id=debit_element_id,
          credit_element_id=credit_element_id,
          amount=amount_dollars,
        )

      # Stale — delete and fall through to create fresh
      self._delete_draft_entry(session, existing_entry_id)
      regenerated = True
    else:
      regenerated = False

    # Create draft entry
    entry = Entry(
      type=template.get("entry_type", "closing"),
      status="draft",
      posting_date=posting_date,
      memo=entry_memo,
      source_structure_id=structure_id,
      provenance="schedule_derived",
      created_by=created_by,
    )
    session.add(entry)
    session.flush()

    # Create line items (DR/CR)
    session.add(
      LineItem(
        entry_id=entry.id,
        element_id=debit_element_id,
        debit_amount=amount_cents,
        credit_amount=0,
        line_order=1,
      )
    )
    session.add(
      LineItem(
        entry_id=entry.id,
        element_id=credit_element_id,
        debit_amount=0,
        credit_amount=amount_cents,
        line_order=2,
      )
    )

    session.flush()

    # Auto-reverse: create a reversing entry on the first day of the next period
    reversal_result = None
    if template.get("auto_reverse", False):
      if period_end.month == 12:
        reversal_date = date(period_end.year + 1, 1, 1)
      else:
        reversal_date = date(period_end.year, period_end.month + 1, 1)

      reversal_memo = f"Reverse: {entry_memo}"

      reversal_entry = Entry(
        type="reversing",
        status="draft",
        posting_date=reversal_date,
        memo=reversal_memo,
        source_structure_id=structure_id,
        provenance="schedule_derived",
        reversal_of=entry.id,
        created_by=created_by,
      )
      session.add(reversal_entry)
      session.flush()

      # Flipped DR/CR: debit element becomes credit, credit becomes debit
      session.add(
        LineItem(
          entry_id=reversal_entry.id,
          element_id=credit_element_id,
          debit_amount=amount_cents,
          credit_amount=0,
          line_order=1,
        )
      )
      session.add(
        LineItem(
          entry_id=reversal_entry.id,
          element_id=debit_element_id,
          debit_amount=0,
          credit_amount=amount_cents,
          line_order=2,
        )
      )
      session.flush()

      reversal_result = ClosingEntryResult(
        outcome="created",
        entry_id=reversal_entry.id,
        status="draft",
        posting_date=reversal_date,
        memo=reversal_memo,
        debit_element_id=credit_element_id,
        credit_element_id=debit_element_id,
        amount=amount_dollars,
      )

    return ClosingEntryResult(
      outcome="regenerated" if regenerated else "created",
      entry_id=entry.id,
      status="draft",
      posting_date=posting_date,
      memo=entry_memo,
      debit_element_id=debit_element_id,
      credit_element_id=credit_element_id,
      amount=amount_dollars,
      reversal=reversal_result,
    )

  def create_manual_closing_entry(
    self,
    session: Session,
    *,
    posting_date: date,
    line_items: list[dict],
    memo: str,
    created_by: str,
    entry_type: str = "closing",
  ) -> ClosingEntryResult:
    """Create a manual (non-schedule) draft closing entry.

    Used for one-off adjustments that aren't derived from a schedule:
    asset disposals, impairments, reclassifications, correcting entries.

    The resulting entry has:
    - `source_structure_id = None` (not tied to any schedule)
    - `provenance = 'manual_entry'`
    - `status = 'draft'` (posted by close-period like any other draft)

    Unlike schedule-derived entries which have exactly 2 line items (DR/CR
    from the template), manual entries can have any number of line items as
    long as total_debit == total_credit. This supports 4-sided disposal
    entries (DR Cash, DR Accum Depr, CR Asset, CR Gain).

    Each ``line_items`` dict carries ``element_id`` (required),
    ``debit_amount`` / ``credit_amount`` in cents (default 0, exactly one > 0
    per line) and an optional ``description``. ``memo`` is required and
    usually cites the business event. ``entry_type`` accepts 'closing'
    (default), 'adjusting' or 'standard'.

    Returns a ``ClosingEntryResult`` with ``outcome='created'``. Raises
    ``ValueError`` when ``line_items`` is empty, doesn't balance, has an
    invalid debit/credit combination, or ``memo`` is empty.
    """
    if not memo or not memo.strip():
      raise ValueError("Manual entry requires a non-empty memo")
    if not line_items:
      raise ValueError("Manual entry requires at least one line item")

    # Normalize + validate each line
    total_debit = 0
    total_credit = 0
    normalized: list[dict] = []
    for i, li in enumerate(line_items):
      if "element_id" not in li or not li["element_id"]:
        raise ValueError(f"Line item {i}: missing element_id")
      debit = int(li.get("debit_amount") or 0)
      credit = int(li.get("credit_amount") or 0)
      if debit < 0 or credit < 0:
        raise ValueError(f"Line item {i}: amounts must be non-negative")
      if debit == 0 and credit == 0:
        raise ValueError(f"Line item {i}: must have a non-zero debit or credit amount")
      if debit > 0 and credit > 0:
        raise ValueError(f"Line item {i}: cannot have both debit and credit amounts")
      total_debit += debit
      total_credit += credit
      normalized.append(
        {
          "element_id": li["element_id"],
          "debit_amount": debit,
          "credit_amount": credit,
          "description": li.get("description"),
        }
      )

    if total_debit != total_credit:
      raise ValueError(
        f"Manual entry does not balance: "
        f"total_debit={total_debit} total_credit={total_credit}"
      )

    # Reject drafts with posting_date in an already-closed fiscal period.
    # Without this check, the draft would be inserted but close-period would
    # later refuse to transition it to posted (period is closed), leaving an
    # orphaned draft inside a locked month. To make an adjustment to a closed
    # period, the caller must reopen it first.
    self._assert_period_not_closed(session, posting_date)

    # Create the draft entry
    entry = Entry(
      type=entry_type,
      status="draft",
      posting_date=posting_date,
      memo=memo,
      source_structure_id=None,
      provenance="manual_entry",
      created_by=created_by,
    )
    session.add(entry)
    session.flush()

    for order, li in enumerate(normalized, 1):
      session.add(
        LineItem(
          entry_id=entry.id,
          element_id=li["element_id"],
          debit_amount=li["debit_amount"],
          credit_amount=li["credit_amount"],
          description=li["description"],
          line_order=order,
        )
      )
    session.flush()

    # For the result, report the first DR/CR element as a summary signal.
    # The caller has full line items on hand so they don't need to re-inspect.
    first_debit = next(
      (li["element_id"] for li in normalized if li["debit_amount"] > 0), None
    )
    first_credit = next(
      (li["element_id"] for li in normalized if li["credit_amount"] > 0), None
    )

    logger.info(
      f"Created manual closing entry {entry.id} "
      f"with {len(normalized)} line items, total={total_debit}"
    )
    return ClosingEntryResult(
      outcome="created",
      entry_id=entry.id,
      status="draft",
      posting_date=posting_date,
      memo=memo,
      debit_element_id=first_debit,
      credit_element_id=first_credit,
      amount=round(total_debit / 100.0, 2),
    )

  def truncate_schedule(
    self,
    session: Session,
    *,
    structure_id: str,
    new_end_date: date,
    reason: str,
    updated_by: str,
  ) -> dict:
    """End a schedule early — delete facts with period_start > new_end_date.

    Used for events that cut a schedule's lifespan short: an asset is sold,
    a prepaid is cancelled, a contract is terminated. The schedule stays
    (preserving its audit trail and remaining in-scope facts), but all facts
    after the new end date are hard-deleted so they never produce future
    closing entries.

    Historical facts (periods ≤ closed_through) are unaffected — they
    remain as the record of what was recognized before the truncation.

    ``new_end_date`` is the last date the schedule covers; facts with
    ``period_start`` beyond it are deleted. It cannot precede the schedule's
    earliest existing fact — that would be a delete, not a truncate.
    ``reason`` is required and captured in metadata for audit.

    Returns a dict with ``facts_deleted``, ``new_end_date`` and ``reason``.
    Raises ``ValueError`` when the structure isn't a schedule, when
    ``new_end_date`` precedes the schedule's first fact, or when any matching
    fact is linked to a posted entry.
    """
    if not reason or not reason.strip():
      raise ValueError("truncate_schedule requires a non-empty reason")

    # new_end_date must be the last day of its month. Schedule facts are
    # stored as full-month rows (period_start is day 1, period_end is the
    # last day). A mid-month end (e.g., 2026-03-15) would leave March's
    # fact in place because period_start=2026-03-01 is NOT > 2026-03-15,
    # meaning full-month depreciation still draws for a partial month. If
    # the user needs to split March, they should truncate to 2026-02-28
    # (drop March entirely) or 2026-03-31 (keep full March) — and book a
    # prorated manual adjustment for any difference.
    from calendar import monthrange

    last_day = monthrange(new_end_date.year, new_end_date.month)[1]
    if new_end_date.day != last_day:
      raise ValueError(
        f"new_end_date ({new_end_date}) must be the last day of the month "
        f"({new_end_date.year:04d}-{new_end_date.month:02d}-{last_day:02d}). "
        "Schedule facts are whole-month; mid-month truncation is ambiguous. "
        "Use the prior month-end to drop the period entirely, or this "
        "month-end to keep the full-month recognition, and book any "
        "prorated difference as a manual closing entry."
      )

    structure = session.get(Structure, structure_id)
    if not structure or structure.block_type != "schedule":
      raise ValueError(f"Schedule structure '{structure_id}' not found")

    # Check there are any facts, and that new_end_date is within range
    bounds = session.execute(
      text("""
        SELECT
          MIN(period_start) AS first_start,
          MAX(period_end)   AS last_end
        FROM facts
        WHERE structure_id = :sid
      """),
      {"sid": structure_id},
    ).fetchone()

    if bounds is None or bounds.first_start is None:
      raise ValueError(f"Schedule '{structure_id}' has no facts; nothing to truncate")

    if new_end_date < bounds.first_start:
      raise ValueError(
        f"new_end_date ({new_end_date}) is before the schedule's earliest "
        f"fact ({bounds.first_start}). Deactivate the schedule instead."
      )

    # Refuse to delete facts whose period overlaps a posted (non-draft) entry.
    # A posted entry carries the record of that period's recognition — truncating
    # underneath it would orphan the audit trail.
    overlap = session.execute(
      text("""
        SELECT COUNT(*) AS c
        FROM entries
        WHERE source_structure_id = :sid
          AND status = 'posted'
          AND posting_date > :new_end
      """),
      {"sid": structure_id, "new_end": new_end_date},
    ).fetchone()
    if overlap and overlap.c:
      raise ValueError(
        f"Cannot truncate: {overlap.c} posted entries exist for periods "
        f"after {new_end_date}. Reopen the affected periods and void those "
        "entries first — reopening alone leaves entries posted, so it does "
        "not clear this guard."
      )

    # Delete any draft entries that fall past the new end date (they're now stale)
    session.execute(
      text("""
        DELETE FROM line_items
        WHERE entry_id IN (
          SELECT id FROM entries
          WHERE source_structure_id = :sid
            AND status = 'draft'
            AND posting_date > :new_end
        )
      """),
      {"sid": structure_id, "new_end": new_end_date},
    )
    session.execute(
      text("""
        DELETE FROM entries
        WHERE source_structure_id = :sid
          AND status = 'draft'
          AND posting_date > :new_end
      """),
      {"sid": structure_id, "new_end": new_end_date},
    )

    # Delete facts beyond the new end date
    del_result = session.execute(
      text("""
        DELETE FROM facts
        WHERE structure_id = :sid
          AND period_start > :new_end
      """),
      {"sid": structure_id, "new_end": new_end_date},
    )
    facts_deleted = del_result.rowcount or 0

    # Update schedule metadata to record the truncation event for audit
    now = datetime.now(UTC)
    metadata = dict(structure.metadata_ or {})
    schedule_meta = dict(metadata.get("schedule_metadata", {}))
    truncation_log = list(metadata.get("truncations", []))
    truncation_log.append(
      {
        "new_end_date": new_end_date.isoformat(),
        "reason": reason,
        "updated_by": updated_by,
        "updated_at": now.isoformat(),
        "facts_deleted": facts_deleted,
      }
    )
    schedule_meta["end_date"] = new_end_date.isoformat()
    metadata["schedule_metadata"] = schedule_meta
    metadata["truncations"] = truncation_log
    structure.metadata_ = metadata
    structure.artifact_mechanics = {
      "kind": "closing_entry_generator",
      "entry_template": metadata.get("entry_template", {}),
      "schedule_metadata": metadata.get("schedule_metadata"),
    }

    # Force SQLAlchemy to detect the JSONB change
    from sqlalchemy.orm.attributes import flag_modified

    flag_modified(structure, "metadata_")
    flag_modified(structure, "artifact_mechanics")
    session.flush()

    logger.info(
      f"Truncated schedule {structure_id} to {new_end_date} "
      f"(deleted {facts_deleted} facts, reason: {reason})"
    )
    return {
      "structure_id": structure_id,
      "new_end_date": new_end_date,
      "facts_deleted": facts_deleted,
      "reason": reason,
    }

  def _assert_period_not_closed(self, session: Session, posting_date: date) -> None:
    """Raise ValueError if a FiscalPeriod containing `posting_date` is closed.

    Used to prevent manual entries from being drafted into closed periods —
    such drafts would be orphaned because close-period refuses to re-close a
    closed month. If an adjustment is needed in a closed period, the user
    must explicitly reopen the period first.

    If no FiscalPeriod row covers the posting_date (e.g., demo or fresh
    tenant without period rows seeded), this is a no-op: the caller's free
    to create the entry and the period-state machine will handle it later.
    """
    row = session.execute(
      text("""
        SELECT name, status
        FROM fiscal_periods
        WHERE start_date <= :posting_date AND end_date >= :posting_date
        LIMIT 1
      """),
      {"posting_date": posting_date},
    ).fetchone()

    if row is not None and row.status == "closed":
      raise ValueError(
        f"Cannot draft manual entry in closed period {row.name!r} "
        f"(posting_date={posting_date}). "
        "Reopen the period first if the adjustment is needed there."
      )

  def _delete_draft_entry(self, session: Session, entry_id: str) -> None:
    """Delete an entry and its line items (must be status='draft')."""
    # Line items first (FK cascade not guaranteed at the model layer)
    session.execute(
      text("DELETE FROM line_items WHERE entry_id = :eid"),
      {"eid": entry_id},
    )
    # Reversal entry, if one exists
    session.execute(
      text("""
        DELETE FROM line_items
        WHERE entry_id IN (SELECT id FROM entries WHERE reversal_of = :eid)
      """),
      {"eid": entry_id},
    )
    session.execute(
      text("DELETE FROM entries WHERE reversal_of = :eid"),
      {"eid": entry_id},
    )
    session.execute(
      text("DELETE FROM entries WHERE id = :eid"),
      {"eid": entry_id},
    )
    session.flush()

  # ── Private helpers ──────────────────────────────────────────────────

  def _ensure_schedule_taxonomy(self, session: Session, created_by: str) -> str:
    """Get or create the default schedule taxonomy.

    Each tenant schema gets at most one schedule taxonomy. The race window
    is narrow (single-tenant sessions) and a duplicate is harmless — the
    LIMIT 1 query will always pick one consistently.
    """
    row = session.execute(
      text("""
        SELECT id FROM taxonomies
        WHERE taxonomy_type = 'schedule' AND is_active = true
        LIMIT 1
      """)
    ).fetchone()

    if row:
      return row.id

    taxonomy = Taxonomy(
      name="Schedules",
      description="Schedule taxonomy for depreciation, amortization, and accruals",
      taxonomy_type="schedule",
      is_shared=False,
      is_active=True,
      is_locked=False,
      created_by=created_by,
    )
    session.add(taxonomy)
    session.flush()
    return taxonomy.id

  def _get_entity_id(self, session: Session) -> str:
    """Get the primary entity ID."""
    result = session.execute(
      text("SELECT id FROM entities ORDER BY created_at ASC LIMIT 1")
    )
    row = result.fetchone()
    if not row:
      raise ValueError("No entity found")
    return row.id


# ── Utility ──────────────────────────────────────────────────────────────


def _element_is_contra_asset(session: Session, element_id: str) -> bool:
  """True if the element carries the FASB ``contraAsset`` EFS trait.

  A contra-asset (accumulated depreciation/amortization, the allowance for
  doubtful accounts) accumulates *up* toward the gross asset's cost; a
  directly-credited asset (a prepaid) draws *down* from cost to zero. The
  two are indistinguishable by ``balance_type`` — QuickBooks types both as
  asset/debit-normal — so the schedule roll-forward direction is keyed off
  this trait, which the sync applies from the source CoA's AccountSubType
  (see ``operations/extensions/loader.py``). Reading the trait keeps this
  generator source-agnostic rather than re-deriving from QB-specific
  metadata strings.
  """
  return (
    session.execute(
      select(ElementTrait.element_id)
      .join(Trait, Trait.id == ElementTrait.trait_id)
      .where(
        ElementTrait.element_id == element_id,
        Trait.category == "elementsOfFinancialStatements",
        Trait.identifier == "contraAsset",
      )
      .limit(1)
    ).scalar()
    is not None
  )


def _generate_monthly_periods(start: date, end: date) -> list[tuple[date, date]]:
  """Generate monthly (first-of-month, last-of-month) periods."""
  from calendar import monthrange

  periods: list[tuple[date, date]] = []
  current = date(start.year, start.month, 1)

  while current <= end:
    _, last_day = monthrange(current.year, current.month)
    month_end = date(current.year, current.month, last_day)
    periods.append((current, month_end))

    # Next month
    if current.month == 12:
      current = date(current.year + 1, 1, 1)
    else:
      current = date(current.year, current.month + 1, 1)

  return periods
