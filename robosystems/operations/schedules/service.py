"""Schedule service — operations for schedule lifecycle.

Schedules are structured fact tables of planned values (depreciation,
amortization, accruals) organized by element and period. They use the
existing XBRL taxonomy model: Taxonomy → Structure → Association → Fact.

This service is the shared operation layer called by both API routes
and MCP tools.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from sqlalchemy import text
from sqlalchemy.orm import Session

from robosystems.models.extensions.roboledger import (
  Association,
  Entry,
  Fact,
  LineItem,
  Structure,
  Taxonomy,
)
from robosystems.utils.ulid import generate_prefixed_ulid

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


@dataclass
class ScheduleFact:
  """A single schedule fact for display."""

  element_id: str
  element_name: str
  value: float  # dollars
  period_start: date
  period_end: date


@dataclass
class ScheduleSummary:
  """Summary of a schedule structure."""

  structure_id: str
  name: str
  taxonomy_name: str
  entry_template: dict | None
  schedule_metadata: dict | None
  total_periods: int
  periods_with_entries: int


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
  """Result of creating a closing entry from a schedule."""

  entry_id: str
  status: str
  posting_date: date
  memo: str
  debit_element_id: str
  credit_element_id: str
  amount: float  # dollars
  reversal: ClosingEntryResult | None = None


# ── Service ──────────────────────────────────────────────────────────────


class ScheduleService:
  """Schedule lifecycle operations.

  All methods take an extensions DB session with search_path already
  set to the tenant schema. Callers (routes, MCP tools) handle session
  management.
  """

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
  ) -> Structure:
    """Create a schedule with pre-generated facts.

    Creates a structure (type=schedule), associations to the referenced
    elements, and facts for each monthly period between period_start and
    period_end.

    Args:
        session: Extensions DB session.
        name: Schedule name (e.g., "Office Furniture Depreciation").
        taxonomy_id: Taxonomy to attach to. If None, uses or creates
            a default "Schedules" taxonomy.
        element_ids: Element IDs to include in the schedule (e.g.,
            [depreciation_expense_id, accumulated_depreciation_id]).
        period_start: First period start date.
        period_end: Last period end date.
        monthly_amount: Monthly amount in cents (e.g., depreciation per month).
        entry_template: Template for generating closing entries.
        schedule_metadata: Informational metadata about the schedule.
        created_by: User ID.

    Returns:
        The created Structure.
    """
    # Resolve or create taxonomy
    if not taxonomy_id:
      taxonomy_id = self._ensure_schedule_taxonomy(session, created_by)

    # Create structure with entry template metadata
    metadata = {
      "entry_template": {
        "debit_element_id": entry_template.debit_element_id,
        "credit_element_id": entry_template.credit_element_id,
        "entry_type": entry_template.entry_type,
        "memo_template": entry_template.memo_template or f"Monthly schedule - {name}",
        "auto_reverse": entry_template.auto_reverse,
      },
    }
    if schedule_metadata:
      metadata["schedule_metadata"] = {
        "method": schedule_metadata.method,
        "original_amount": schedule_metadata.original_amount,
        "residual_value": schedule_metadata.residual_value,
        "useful_life_months": schedule_metadata.useful_life_months,
        "asset_element_id": schedule_metadata.asset_element_id,
      }

    structure = Structure(
      name=name,
      structure_type="schedule",
      taxonomy_id=taxonomy_id,
      metadata_=metadata,
      created_by=created_by,
    )
    session.add(structure)
    session.flush()

    # Create associations to elements
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

    # Generate facts for each monthly period
    fact_set_id = generate_prefixed_ulid("fs")
    entity_id = self._get_entity_id(session)

    periods = _generate_monthly_periods(period_start, period_end)
    amount_dollars = round(monthly_amount / 100.0, 2)
    accumulated = 0.0

    for p_start, p_end in periods:
      accumulated = round(accumulated + amount_dollars, 2)

      # Fact for the periodic amount (expense/amortization)
      session.add(
        Fact(
          element_id=entry_template.debit_element_id,
          value=amount_dollars,
          period_start=p_start,
          period_end=p_end,
          period_type="duration",
          unit="USD",
          entity_id=entity_id,
          structure_id=structure.id,
          fact_set_id=fact_set_id,
        )
      )

      # Fact for the accumulated contra amount
      session.add(
        Fact(
          element_id=entry_template.credit_element_id,
          value=accumulated,
          period_start=p_start,
          period_end=p_end,
          period_type="instant",
          unit="USD",
          entity_id=entity_id,
          structure_id=structure.id,
          fact_set_id=fact_set_id,
        )
      )

      # Net book value if we have asset element info
      if schedule_metadata and schedule_metadata.asset_element_id:
        original = round(schedule_metadata.original_amount / 100.0, 2)
        session.add(
          Fact(
            element_id=schedule_metadata.asset_element_id,
            value=round(original - accumulated, 2),
            period_start=p_start,
            period_end=p_end,
            period_type="instant",
            unit="USD",
            entity_id=entity_id,
            structure_id=structure.id,
            fact_set_id=fact_set_id,
          )
        )

    session.flush()
    return structure

  def list_schedules(self, session: Session) -> list[ScheduleSummary]:
    """List all schedule structures with summary info."""
    result = session.execute(
      text("""
        SELECT
          s.id AS structure_id,
          s.name,
          t.name AS taxonomy_name,
          s.metadata AS metadata,
          (SELECT COUNT(DISTINCT (f.period_start, f.period_end))
           FROM facts f WHERE f.structure_id = s.id) AS total_periods,
          (SELECT COUNT(DISTINCT e.id)
           FROM entries e
           WHERE e.source_structure_id = s.id
             AND e.status IN ('draft', 'posted')) AS periods_with_entries
        FROM structures s
        JOIN taxonomies t ON t.id = s.taxonomy_id
        WHERE s.structure_type = 'schedule'
          AND s.is_active = true
        ORDER BY s.name
      """)
    )

    return [
      ScheduleSummary(
        structure_id=row.structure_id,
        name=row.name,
        taxonomy_name=row.taxonomy_name,
        entry_template=row.metadata.get("entry_template") if row.metadata else None,
        schedule_metadata=row.metadata.get("schedule_metadata")
        if row.metadata
        else None,
        total_periods=row.total_periods,
        periods_with_entries=row.periods_with_entries,
      )
      for row in result
    ]

  def get_schedule_facts(
    self,
    session: Session,
    structure_id: str,
    period_start: date | None = None,
    period_end: date | None = None,
  ) -> list[ScheduleFact]:
    """Get facts for a schedule, optionally filtered by period."""
    # Validate schedule exists
    struct = session.get(Structure, structure_id)
    if not struct or struct.structure_type != "schedule":
      raise ValueError(f"Schedule structure '{structure_id}' not found")

    params: dict = {"structure_id": structure_id}
    period_filter = ""

    if period_start:
      period_filter += " AND f.period_start >= :period_start"
      params["period_start"] = period_start
    if period_end:
      period_filter += " AND f.period_end <= :period_end"
      params["period_end"] = period_end

    result = session.execute(
      text(f"""
        SELECT f.element_id, e.name AS element_name,
               f.value, f.period_start, f.period_end
        FROM facts f
        JOIN elements e ON e.id = f.element_id
        WHERE f.structure_id = :structure_id
          {period_filter}
        ORDER BY f.period_start, f.period_end, e.name
      """),
      params,
    )

    return [
      ScheduleFact(
        element_id=row.element_id,
        element_name=row.element_name,
        value=row.value,
        period_start=row.period_start,
        period_end=row.period_end,
      )
      for row in result
    ]

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
        LEFT JOIN best_entry be ON be.source_structure_id = s.id
        LEFT JOIN reversal r ON r.reversal_of = be.entry_id
        WHERE s.structure_type = 'schedule'
          AND s.is_active = true
        ORDER BY s.name
      """),
      {"period_start": period_start, "period_end": period_end},
    )

    # Check fiscal period status
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
    """Create a draft closing entry from a schedule's facts for a period.

    Reads the structure's entry template metadata, finds the fact for
    the debit element in the specified period, and creates a balanced
    draft entry.

    Raises:
        ValueError: If schedule not found, no template, no fact, or
            entry already exists for this structure+period.
    """
    # Load structure with entry template
    structure = session.get(Structure, structure_id)
    if not structure or structure.structure_type != "schedule":
      raise ValueError(f"Schedule structure '{structure_id}' not found")

    template = (structure.metadata_ or {}).get("entry_template")
    if not template:
      raise ValueError(f"Schedule '{structure_id}' has no entry template")

    debit_element_id = template["debit_element_id"]
    credit_element_id = template["credit_element_id"]

    # Check for existing entry (idempotent guard)
    existing = session.execute(
      text("""
        SELECT id FROM entries
        WHERE source_structure_id = :structure_id
          AND posting_date >= :period_start
          AND posting_date <= :period_end
        LIMIT 1
      """),
      {
        "structure_id": structure_id,
        "period_start": period_start,
        "period_end": period_end,
      },
    ).fetchone()

    if existing:
      raise ValueError(
        f"Entry already exists for schedule '{structure_id}' "
        f"in period {period_start} to {period_end}"
      )

    # Find the debit fact for this period
    fact_row = session.execute(
      text("""
        SELECT value FROM facts
        WHERE structure_id = :structure_id
          AND element_id = :element_id
          AND period_start >= :period_start
          AND period_end <= :period_end
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
      raise ValueError(
        f"No fact found for element '{debit_element_id}' "
        f"in period {period_start} to {period_end}"
      )

    amount_dollars = fact_row.value
    amount_cents = round(amount_dollars * 100)

    # Build memo
    memo_template = template.get("memo_template", "")
    entry_memo = memo or memo_template.replace("{structure_name}", structure.name)

    # Create draft entry
    entry = Entry(
      type=template.get("entry_type", "closing"),
      status="draft",
      posting_date=posting_date,
      memo=entry_memo,
      source_structure_id=structure_id,
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
        entry_id=reversal_entry.id,
        status="draft",
        posting_date=reversal_date,
        memo=reversal_memo,
        debit_element_id=credit_element_id,
        credit_element_id=debit_element_id,
        amount=amount_dollars,
      )

    return ClosingEntryResult(
      entry_id=entry.id,
      status="draft",
      posting_date=posting_date,
      memo=entry_memo,
      debit_element_id=debit_element_id,
      credit_element_id=credit_element_id,
      amount=amount_dollars,
      reversal=reversal_result,
    )

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
