"""Schedule lifecycle: fact tables of planned values (depreciation,
amortization, accruals) by element and period, stored as
Taxonomy → Structure → Association → Fact.
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
from robosystems.operations.roboledger.commands._guards import (
  assert_period_not_closed,
)
from robosystems.operations.roboledger.entry_status import (
  GENERATED_REVERSAL_SQL,
  LANDED_ENTRY_STATUSES,
  PRIMARY_ENTRY_SQL,
  landed_entry_bindparam,
)
from robosystems.operations.roboledger.fact_set import create_fact_set
from robosystems.utils.ulid import generate_prefixed_ulid

# Charlie Hoffman's Seattle Method "universal" conceptual-model has-part
# arcrole. A cm:Debit / cm:Credit ─has-part→ CoA-element arc declares that
# element as the debit / credit leg of a schedule's posting template.
CM_HAS_PART_ARCROLE = "https://github.com/seattlemethod/universal/cm/arcrole/has-part"


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
  # None for an open period and for a period closed before receipts existed;
  # absence does not mean a failed close.
  close_receipt: dict | None = None


@dataclass
class ClosingEntryResult:
  """Result of create_closing_entry; outcomes are documented there.

  Entry fields are None when ``outcome`` is removed or skipped.
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


class ScheduleService:
  """All methods take an extensions session with search_path already set to
  the tenant schema."""

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
    """Create the Structure, its element associations, and the
    cm:Debit/cm:Credit has-part posting arcs. Not called on rebuild, which
    keeps all three.
    """
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

    # cm:Debit / cm:Credit ─has-part→ account arcs make the posting pair
    # queryable. Best-effort: skipped when the library lacks the cm concepts;
    # entry_template remains the generation source of truth.
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
    """Build the ``metadata_`` and ``artifact_mechanics`` blobs, which carry
    every generation input so ``rebuild_schedule`` needs nothing from facts.
    """
    metadata: dict[str, object] = {
      "entry_template": {
        "debit_element_id": entry_template.debit_element_id,
        "credit_element_id": entry_template.credit_element_id,
        "entry_type": entry_template.entry_type,
        "memo_template": entry_template.memo_template or f"Monthly schedule - {name}",
        "auto_reverse": entry_template.auto_reverse,
      },
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

    # artifact_mechanics is what the envelope builder reads; metadata_ stays
    # populated for rows without it. periods_with_entries is read-time only
    # (the envelope builder injects it), hence no ScheduleMechanics.model_dump().
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
    """Create a schedule with one generated fact set per monthly period.

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

    fact_set_id = generate_prefixed_ulid("fs")
    entity_id = self._get_entity_id(session)

    # A custom periodic-amounts curve is asserted by the caller; a
    # straight-line schedule is derived from method + params.
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
    # Flush the FactSet before the facts: SQLAlchemy orders INSERTs by
    # statement type, not FK dependency.
    session.flush()

    periods = _generate_monthly_periods(period_start, period_end)
    amount_dollars = round(monthly_amount / 100.0, 2)
    original_dollars = (
      round(schedule_metadata.original_amount / 100.0, 2)
      if schedule_metadata and schedule_metadata.original_amount > 0
      else None
    )

    # The credit fact is the credited account's running balance. A contra-asset
    # or credit-balance account accumulates up from zero; a directly-credited
    # asset (a prepaid) draws down from cost. QuickBooks types contra-assets as
    # debit-normal, so the contraAsset trait decides, not balance_type.
    credit_balance_type = session.execute(
      select(Element.balance_type).where(Element.id == entry_template.credit_element_id)
    ).scalar()
    credit_is_contra = credit_balance_type == "debit" and _element_is_contra_asset(
      session, entry_template.credit_element_id
    )
    credit_draws_down = credit_balance_type == "debit" and not credit_is_contra
    # Drawing down needs a cost basis; without original_amount, derive it from
    # the straight-line curve (custom curves always carry one).
    if credit_draws_down and original_dollars is None and periods:
      original_dollars = round(amount_dollars * len(periods), 2)
    # No opening basis (e.g. no periods): accumulate instead.
    credit_draws_down = credit_draws_down and original_dollars is not None

    # The schedule expenses cost less residual (salvage) value; the carrying
    # balances still run from cost, so they end at the residual.
    residual_cents = schedule_metadata.residual_value if schedule_metadata else 0
    if residual_cents and not (schedule_metadata and schedule_metadata.original_amount):
      raise ValueError("residual_value requires original_amount (the cost basis).")
    depreciable_dollars: float | None = None
    if original_dollars is not None:
      if residual_cents < 0 or residual_cents >= round(original_dollars * 100):
        raise ValueError(
          "residual_value must be at least 0 and less than original_amount."
        )
      depreciable_dollars = round(original_dollars - residual_cents / 100.0, 2)

    # Custom curve: one pre-balanced cents value per period, used verbatim.
    # Non-negative terms because the SumEquals rule assumes them.
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
      depreciable_cents = (
        schedule_metadata.original_amount - schedule_metadata.residual_value
      )
      if total_cents != depreciable_cents:
        raise ValueError(
          f"periodic_amounts sum ({total_cents} cents) does not equal "
          f"original_amount less residual_value ({depreciable_cents} cents)."
        )
      custom_amounts_dollars = [
        round(c / 100.0, 2) for c in schedule_metadata.periodic_amounts
      ]
    elif depreciable_dollars is not None and periods:
      # Straight-line: the final month absorbs rounding, so the months before
      # it must leave it something non-negative to book.
      if round(amount_dollars * (len(periods) - 1), 2) > depreciable_dollars:
        raise ValueError(
          f"monthly_amount x {len(periods) - 1} months exceeds original_amount "
          f"less residual_value ({depreciable_dollars}); the final month would "
          f"be negative."
        )

    # Opening-balance instant facts at the first period start, so the
    # roll-forward reads Begin → movements → End. Same formulas as the loop at
    # accumulated == 0: a contra opens at 0, a drawn-down or NBV asset at cost.
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
        period_amount = custom_amounts_dollars[i]
      else:
        # Straight-line: final period absorbs rounding so
        # Σ(debit facts) == original_amount - residual_value exactly.
        period_amount = (
          round(depreciable_dollars - accumulated_debit, 2)
          if is_last and depreciable_dollars is not None
          else amount_dollars
        )
      accumulated_debit = round(accumulated_debit + period_amount, 2)

      fact_scope = (
        "historical" if closed_through and p_end <= closed_through else "in_scope"
      )

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

      # Net book value, unless the asset is the credited account itself (the
      # credit fact already carries its balance).
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

    # SumEquals rule: Σ debit facts == original_amount - residual_value. Bound
    # by element id because tenant CoA accounts have a null qname.
    if depreciable_dollars is not None:
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
          rule_expression=f"sum(${var_name}) = {depreciable_dollars}",
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
          metadata_={"expected_total": depreciable_dollars},
          created_by=created_by,
        )
      )

    # Obligation register: the obligation sensor classifies each pending
    # schedule_entry_due at its period boundary and dispatches the handler.
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

    metadata["schedule_created_event_id"] = schedule_created_event_id
    metadata["pending_event_count"] = pending_event_count
    structure.metadata_ = metadata
    artifact_mechanics["schedule_created_event_id"] = schedule_created_event_id
    artifact_mechanics["pending_event_count"] = pending_event_count
    structure.artifact_mechanics = artifact_mechanics

    # A persisted Structure needs the JSONB change flagged to emit an UPDATE.
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
    """Emit `schedule_created` + one `schedule_entry_due` per period.

    Periods at or before ``closed_through`` are emitted ``voided``
    (``void_reason='historical'``) so the close gate ignores them; the
    returned pending count excludes them. IDs are generated Python-side so
    the linkage needs no flush.

    Returns ``(schedule_created_event_id, pending_event_count)``.
    """
    schedule_created_event_id = generate_prefixed_ulid("evt")
    now = datetime.now(UTC)

    pending_count = sum(
      1 for _p_start, p_end in periods if not closed_through or p_end > closed_through
    )

    session.add(
      Event(
        id=schedule_created_event_id,
        event_type="schedule_created",
        # Moves no resource: it arranges future recognition, which the
        # economic schedule_entry_due children carry.
        event_category="schedule",
        event_class="operational",
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
      # End of day, so the sweep picks the period up exactly when it closes.
      occurred_at = datetime.combine(p_end, time(23, 59, 59), tzinfo=UTC)
      is_historical = closed_through is not None and p_end <= closed_through
      event_metadata: dict[str, object] = {
        "schedule_id": structure.id,
        "posting_date": p_end.isoformat(),
        "period_start": p_start.isoformat(),
        "period_end": p_end.isoformat(),
      }
      if is_historical:
        # Kept as an audit record; the close gate and promotion sensor both
        # filter on status='pending'.
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

  @staticmethod
  def _schedule_originator_id(session: Session, structure: Structure) -> str | None:
    """The schedule's ``schedule_created`` event id, stamped or recovered.

    An unstamped structure falls back to its obligations' own link; otherwise
    its pending obligations are orphaned on delete/supersede and double-post
    at close.
    """
    metadata = structure.metadata_ or {}
    stamped = metadata.get("schedule_created_event_id")
    if stamped:
      return stamped
    return session.execute(
      select(Event.obligated_by_event_id)
      .where(
        Event.event_type == "schedule_entry_due",
        Event.metadata_["schedule_id"].astext == structure.id,
        Event.obligated_by_event_id.isnot(None),
      )
      .limit(1)
    ).scalar()

  def void_pending_obligations(
    self,
    session: Session,
    *,
    structure: Structure,
    void_reason: str,
    voided_by_event_id: str | None = None,
    period_start_after: date | None = None,
    include_classified: bool = False,
  ) -> int:
    """Void a schedule's pending schedule_entry_due events; returns the count.

    - Disposal passes ``voided_by_event_id`` (the disposal event) for the
      audit chain.
    - Deletion passes none; the originator is about to be deleted, and
      orphaned pending children would trip the close gate.
    - Termination passes ``period_start_after`` (only obligations starting
      strictly after it) and ``include_classified=True`` to retire
      matured-but-undrafted strays past the cutoff.

    Also decrements the originator's ``metadata.pending_event_count``.
    """
    from sqlalchemy.orm.attributes import flag_modified

    schedule_created_event_id = self._schedule_originator_id(session, structure)
    if not schedule_created_event_id:
      return 0

    from robosystems.operations.locking import ordered_lock_column

    # Lock in the shared order, then update by id: a bare multi-row UPDATE
    # locks in scan order and deadlocks against the promotion sweep. The
    # status predicate stays on the UPDATE so a row not locked as voidable is
    # never voided.
    voidable_statuses = (
      ("pending", "classified") if include_classified else ("pending",)
    )
    select_filters = [
      Event.obligated_by_event_id == schedule_created_event_id,
      Event.status.in_(voidable_statuses),
    ]
    if period_start_after is not None:
      # ISO date strings, so lexicographic order is date order.
      select_filters.append(
        Event.metadata_["period_start"].astext > period_start_after.isoformat()
      )
    pending_ids = list(
      session.execute(
        select(Event.id)
        .where(*select_filters)
        .order_by(ordered_lock_column())
        .with_for_update()
      ).scalars()
    )
    if not pending_ids:
      return 0

    update_values: dict[str, object] = {"status": "voided"}
    if voided_by_event_id is not None:
      update_values["replaced_by_event_id"] = voided_by_event_id

    result = session.execute(
      update(Event)
      .where(Event.id.in_(pending_ids), Event.status.in_(voidable_statuses))
      .values(**update_values)
    )
    voided_count = int(result.rowcount or 0)
    if voided_count == 0:
      return 0

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
    """Void each pending obligation and emit a replacement linked by
    ``replaces_event_id`` / ``replaced_by_event_id``, under the same
    originator. Used when ``entry_template`` changes; non-pending events are
    untouched (the template applies prospectively). Returns the number of
    replacements.
    """
    schedule_created_event_id = self._schedule_originator_id(session, structure)
    if not schedule_created_event_id:
      return 0

    from robosystems.operations.locking import ordered_lock_column

    # Locked against the promotion sweep, which would otherwise draft a
    # closing entry for an obligation this call is voiding.
    existing_pending = list(
      session.execute(
        select(Event)
        .where(
          Event.obligated_by_event_id == schedule_created_event_id,
          Event.status == "pending",
        )
        # Same order as the promotion sweep's candidate load.
        .order_by(ordered_lock_column())
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
        logger.warning(
          "supersede_pending_obligations: skipping malformed event %s "
          "on schedule %s (missing period_start/period_end metadata)",
          old_evt.id,
          structure.id,
        )
        continue

      new_event_id = generate_prefixed_ulid("evt")
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
    """Get close status for the schedules that have work in a fiscal period.

    Scoped to schedules carrying a fact or an entry in the period, so the
    pending count agrees with the obligation gate. Terminated, run-to-term
    and not-yet-started schedules are absent rather than listed at zero.
    """
    # best_entry: the most-advanced entry per structure (posted > draft > other).
    result = session.execute(
      text(f"""
        WITH best_entry AS (
          SELECT DISTINCT ON (source_structure_id)
            source_structure_id,
            id AS entry_id,
            status AS entry_status
          FROM entries
          WHERE posting_date >= :period_start
            AND posting_date <= :period_end
            AND source_structure_id IS NOT NULL
            AND {PRIMARY_ENTRY_SQL}
          ORDER BY source_structure_id,
            CASE status WHEN 'posted' THEN 1 WHEN 'draft' THEN 2 ELSE 3 END
        ),
        reversal AS (
          SELECT DISTINCT ON (reversal_of)
            reversal_of,
            id AS reversal_entry_id,
            status AS reversal_status
          FROM entries
          WHERE {GENERATED_REVERSAL_SQL}
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
          -- A schedule with neither a fact nor an entry in this period has no
          -- work in it: it was terminated before the period, has run to term,
          -- or has not started yet. Without this the LEFT JOIN renders all
          -- three as `pending` at amount 0.00 forever, so the close summary
          -- disagreed with the obligation gate (39 "pending" against 30 real
          -- obligations on the tenant that surfaced it) and the operator had
          -- to know which rows were phantom.
          AND (f.id IS NOT NULL OR be.entry_id IS NOT NULL)
        ORDER BY s.name
      """),
      {"period_start": period_start, "period_end": period_end},
    )

    fp_result = session.execute(
      text("""
        SELECT status, close_receipt FROM fiscal_periods
        WHERE start_date <= :period_start AND end_date >= :period_end
        LIMIT 1
      """),
      {"period_start": period_start, "period_end": period_end},
    )
    fp_row = fp_result.fetchone()
    period_status = fp_row.status if fp_row else "open"
    close_receipt = fp_row.close_receipt if fp_row else None

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
      close_receipt=close_receipt,
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
    """Idempotently reconcile a schedule's draft closing entry for a period.

    Outcomes:

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
    # Locked so two writers for the same period (the sweep and an operator)
    # cannot both see "no entry" and double-post. The unique index
    # uq_entries_one_primary_per_schedule_period is the backstop; the lock
    # turns the loser into a retryable RowLockedError and covers the template
    # read the index cannot see.
    from robosystems.operations.locking import lock_by_id

    structure = lock_by_id(
      session,
      Structure,
      structure_id,
      f"Schedule {structure_id} is being written by another process. "
      "Retry in a moment.",
    )
    if not structure or structure.block_type != "schedule":
      raise ValueError(f"Schedule structure '{structure_id}' not found")

    template = (structure.metadata_ or {}).get("entry_template")
    if not template:
      raise ValueError(f"Schedule '{structure_id}' has no entry template")

    debit_element_id = template["debit_element_id"]
    credit_element_id = template["credit_element_id"]

    # Same period fence the journal writers take.
    fence_dates = [posting_date]
    if template.get("auto_reverse", False):
      if period_end.month == 12:
        fence_dates.append(date(period_end.year + 1, 1, 1))
      else:
        fence_dates.append(date(period_end.year, period_end.month + 1, 1))
    assert_period_not_closed(session, *fence_dates)

    # PRIMARY_ENTRY_SQL excludes generated reversals: an auto_reverse
    # schedule's reversal lands on the first day of the next period with the
    # same source_structure_id, and would otherwise be judged stale and
    # deleted. Keyed on the reversal link, not entry_type (caller-authored).
    existing_row = session.execute(
      text(f"""
        SELECT id, status
        FROM entries
        WHERE source_structure_id = :structure_id
          AND {PRIMARY_ENTRY_SQL}
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
    # Any landed status, including `reversed`, which regenerate would delete.
    if existing_row and existing_row.status in LANDED_ENTRY_STATUSES:
      raise ValueError(
        f"Closing entry for schedule '{structure_id}' in period "
        f"{period_start} to {period_end} has already been posted "
        f"(status: {existing_row.status}). Use the reopen flow to modify it."
      )

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

    if not fact_row:
      if existing_entry_id:
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

    # Built before the staleness check, which compares memos too.
    memo_template = template.get("memo_template", "")
    entry_memo = memo or memo_template.replace("{structure_name}", structure.name)

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

      self._delete_draft_entry(session, existing_entry_id)
      regenerated = True
    else:
      regenerated = False

    # No transaction_id, deliberately: a schedule entry has no source-system
    # record, and synthesizing a Transaction would manufacture adapter-mirror
    # rows. Reads anchor on Entry.
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

    # Auto-reverse on the first day of the next period.
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
    provenance: str = "manual_entry",
  ) -> ClosingEntryResult:
    """Create a non-schedule draft entry with any number of balanced lines
    (disposals, impairments, reclassifications).

    "Manual" means not schedule-derived, not who wrote it: event-handler
    callers should pass ``provenance='event_handler'``. Each ``line_items``
    dict has ``element_id``, cents ``debit_amount`` / ``credit_amount``
    (exactly one > 0) and optional ``description``. Raises ``ValueError`` on
    an empty memo, no lines, a bad debit/credit pair, or an imbalance.
    """
    if not memo or not memo.strip():
      raise ValueError("Manual entry requires a non-empty memo")
    if not line_items:
      raise ValueError("Manual entry requires at least one line item")

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

    # A draft in a closed period could never be posted.
    self._assert_period_not_closed(session, posting_date)

    entry = Entry(
      type=entry_type,
      status="draft",
      posting_date=posting_date,
      memo=memo,
      source_structure_id=None,
      provenance=provenance,
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
    """End a schedule early: hard-delete facts and draft entries after
    ``new_end_date`` (a month-end, not before the first fact). The structure
    and earlier facts stay as the audit trail.

    Raises ``ValueError`` when the structure isn't a schedule, the date is
    out of range, or a landed entry exists after it.
    """
    if not reason or not reason.strip():
      raise ValueError("truncate_schedule requires a non-empty reason")

    # Facts are whole-month rows; a mid-month end would keep that month's
    # full-month fact.
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

    # A landed entry (reversed included) after the cutoff is the record of
    # that period's recognition; truncating under it would orphan it.
    overlap = session.execute(
      text("""
        SELECT COUNT(*) AS c
        FROM entries
        WHERE source_structure_id = :sid
          AND status IN :landed_entry_statuses
          AND posting_date > :new_end
      """).bindparams(landed_entry_bindparam()),
      {"sid": structure_id, "new_end": new_end_date},
    ).fetchone()
    if overlap and overlap.c:
      raise ValueError(
        f"Cannot truncate: {overlap.c} posted entries exist for periods "
        f"after {new_end_date}. Reopen the affected periods and void those "
        "entries first — reopening alone leaves entries posted, so it does "
        "not clear this guard."
      )

    # Fence before the deletes take row locks: fence, then rows, as every
    # ledger writer does against close.
    stale_dates = (
      session.execute(
        text("""
          SELECT DISTINCT posting_date FROM entries
          WHERE source_structure_id = :sid
            AND status = 'draft'
            AND posting_date > :new_end
        """),
        {"sid": structure_id, "new_end": new_end_date},
      )
      .scalars()
      .all()
    )
    assert_period_not_closed(session, *stale_dates)

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

    del_result = session.execute(
      text("""
        DELETE FROM facts
        WHERE structure_id = :sid
          AND period_start > :new_end
      """),
      {"sid": structure_id, "new_end": new_end_date},
    )
    facts_deleted = del_result.rowcount or 0

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
      **(structure.artifact_mechanics or {}),
      "kind": "closing_entry_generator",
      "entry_template": metadata.get("entry_template", {}),
      "schedule_metadata": metadata.get("schedule_metadata"),
    }

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
    """Raise ClosedPeriodError if `posting_date` falls in a closed period."""
    assert_period_not_closed(session, posting_date)

  def _delete_draft_entry(self, session: Session, entry_id: str) -> None:
    """Delete a draft entry, its draft auto-reversal, and their line items.

    Raises for any landed entry rather than skipping: a caller that thinks it
    is regenerating a draft has a bug worth surfacing, and no FK stops a
    landed entry from being deleted.
    """
    row = session.execute(
      text("SELECT status FROM entries WHERE id = :eid FOR UPDATE"),
      {"eid": entry_id},
    ).fetchone()
    if row is None:
      return
    if row.status != "draft":
      raise ValueError(
        f"Refusing to delete entry {entry_id!r}: status is {row.status!r}, not "
        "'draft'. A landed entry is history — reopen the period and reverse it "
        "through the ledger instead."
      )

    # Only a draft auto-reversal can reference a draft.
    session.execute(
      text("""
        DELETE FROM line_items
        WHERE entry_id IN (
          SELECT id FROM entries WHERE reversal_of = :eid AND status = 'draft'
        )
      """),
      {"eid": entry_id},
    )
    session.execute(
      text("DELETE FROM entries WHERE reversal_of = :eid AND status = 'draft'"),
      {"eid": entry_id},
    )
    # Line items first: FK cascade is not guaranteed at the model layer.
    session.execute(
      text("DELETE FROM line_items WHERE entry_id = :eid"),
      {"eid": entry_id},
    )
    session.execute(
      text("DELETE FROM entries WHERE id = :eid AND status = 'draft'"),
      {"eid": entry_id},
    )
    session.flush()

  def _ensure_schedule_taxonomy(self, session: Session, created_by: str) -> str:
    """Get or create the tenant's schedule taxonomy. A racing duplicate is
    harmless; LIMIT 1 picks one."""
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
    result = session.execute(
      text("SELECT id FROM entities ORDER BY created_at ASC LIMIT 1")
    )
    row = result.fetchone()
    if not row:
      raise ValueError("No entity found")
    return row.id


def _element_is_contra_asset(session: Session, element_id: str) -> bool:
  """True if the element carries the FASB ``contraAsset`` EFS trait, applied
  at sync from the source CoA's AccountSubType (``operations/extensions/loader.py``).
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
  """(first-of-month, last-of-month) pairs covering start..end."""
  from calendar import monthrange

  periods: list[tuple[date, date]] = []
  current = date(start.year, start.month, 1)

  while current <= end:
    _, last_day = monthrange(current.year, current.month)
    month_end = date(current.year, current.month, last_day)
    periods.append((current, month_end))

    if current.month == 12:
      current = date(current.year + 1, 1, 1)
    else:
      current = date(current.year, current.month + 1, 1)

  return periods
