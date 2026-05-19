"""Filter evaluation engine for ``rollforward`` Information Blocks.

Implements **Tier 2 of the rollforward attribution design** from
``information-block.md`` §4.5. Phase 2 MVP scope: one predicate kind
(``line_item_metadata_field``) — decompose a BS account's period change
across declared flow concepts by matching ledger LineItems on a JSONB
metadata field.

Sibling pattern: ``fact_grid._derive_cash_flow_facts`` (Tier 1 default
change tag derivation, shipped 2026-05-14). Where Phase 1 derives flow
facts from period-over-period BS deltas using ``derivation`` arcs, this
module derives them from per-LineItem flow tags carried on
``line_items.metadata``. The two are complementary — Tier 1 handles
legacy data without flow tags, Tier 2 handles data with explicit
tagging (Charlie Hoffman's mini data; future XBRL GL with
``GenericFlowCategory``).

**Sign convention**: all internal aggregations are in **debit-positive
cents** (``debit_amount - credit_amount``). This matches the LineItem
model's natural form and avoids per-account balance-type sign
gymnastics inside the engine. The renderer can flip signs at
presentation time per the target statement's conventions (e.g. CF
inflows reported as positive even for a credit-balance source).

**Residual handling**: ``residual = Δ_debit_positive(BS source over
period) - Σ filter matches``. When non-zero:
 - ``residual_as_default`` (default): emit a default-tag fact for the
   residual when ``default_change_tag_element_id`` is set; otherwise
   emit it as an unattributed residual fact.
 - ``strict``: raise :class:`RollforwardResidualError`.
 - ``warn_only``: log the imbalance; emit no residual fact.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

from sqlalchemy import text
from sqlalchemy.orm import Session

from robosystems.logger import logger
from robosystems.models.api.information_block import RollforwardMechanics


class RollforwardResidualError(ValueError):
  """Σ filter matches != Δ BS and ``validation_mode='strict'``.

  Carries the BS source qname, period, expected delta, sum of matched
  amounts, and computed residual so the caller can surface a precise
  reconciliation message to the operator.
  """

  def __init__(
    self,
    bs_source_qname: str,
    period_start: date,
    period_end: date,
    bs_delta_cents: int,
    sum_matched_cents: int,
    residual_cents: int,
  ) -> None:
    self.bs_source_qname = bs_source_qname
    self.period_start = period_start
    self.period_end = period_end
    self.bs_delta_cents = bs_delta_cents
    self.sum_matched_cents = sum_matched_cents
    self.residual_cents = residual_cents
    super().__init__(
      f"Rollforward residual on {bs_source_qname} for "
      f"{period_start}..{period_end}: ΔBS={bs_delta_cents} cents, "
      f"Σ filters={sum_matched_cents} cents, residual={residual_cents} "
      f"cents (strict mode)."
    )


@dataclass(frozen=True)
class AttributedFact:
  """One filter-matched fact for a single period on a rollforward IB.

  Emitted by :func:`evaluate_attribution_filters` — one per declared
  filter that matched at least one LineItem, plus optionally one
  residual fact for the unattributed delta.

  ``value_cents`` is debit-positive (see module docstring).
  ``event_ids`` is the deduplicated set of ``entries.triggered_by_event_id``
  values for the matched lines — empty if the matched entries weren't
  created by event handlers (manual entries, legacy data).
  ``is_residual`` flags the default-tag residual fact; matched-filter
  facts have ``is_residual=False``.
  """

  target_element_id: str | None
  target_qname: str
  value_cents: int
  period_start: date
  period_end: date
  event_ids: list[str] = field(default_factory=list)
  matched_line_count: int = 0
  is_residual: bool = False


def evaluate_attribution_filters(
  session: Session,
  mechanics: RollforwardMechanics,
  period_start: date,
  period_end: date,
) -> list[AttributedFact]:
  """Evaluate a rollforward's filters over one period; return facts.

  For each declared filter in ``mechanics.attribution_filters``: match
  LineItems on the BS source element whose metadata field is in the
  filter's values; aggregate signed amounts (debit-positive cents);
  emit one :class:`AttributedFact` carrying the value and the matched
  ``event_ids``.

  Compute residual = Δ_debit_positive(BS source over period) - Σ
  filter matches. Arbitrate per ``mechanics.validation_mode``:
  ``residual_as_default`` (default) emits a default-tag fact for any
  non-zero residual; ``strict`` raises; ``warn_only`` logs and omits.

  Returns the full fact list in declaration order, with the residual
  (if any) appended last.
  """
  facts: list[AttributedFact] = []
  bs_element_id = mechanics.bs_source_element_id

  # Δ BS over the period — debit-positive cents.
  bs_delta = _bs_period_delta_cents(session, bs_element_id, period_start, period_end)

  sum_matched = 0
  for f in mechanics.attribution_filters:
    matched = _evaluate_one_filter(
      session=session,
      bs_element_id=bs_element_id,
      filter_target_element_id=f.target_element_id,
      filter_target_qname=f.target_qname,
      field_name=f.predicate.field,
      values=f.predicate.values,
      period_start=period_start,
      period_end=period_end,
    )
    if matched is None:
      # Filter matched no LineItems; skip emitting a zero-value fact.
      # The renderer treats absent facts as "no activity" — cleaner
      # than facts with value=0 cluttering the rendered output.
      continue
    sum_matched += matched.value_cents
    facts.append(matched)

  residual = bs_delta - sum_matched
  if residual != 0:
    if mechanics.validation_mode == "strict":
      raise RollforwardResidualError(
        bs_source_qname=mechanics.bs_source_qname,
        period_start=period_start,
        period_end=period_end,
        bs_delta_cents=bs_delta,
        sum_matched_cents=sum_matched,
        residual_cents=residual,
      )
    if mechanics.validation_mode == "warn_only":
      logger.warning(
        "rollforward residual: %s %s..%s ΔBS=%d, Σ=%d, residual=%d (warn_only)",
        mechanics.bs_source_qname,
        period_start,
        period_end,
        bs_delta,
        sum_matched,
        residual,
      )
    else:
      # residual_as_default — emit a default-tag fact (or unattributed
      # when no default is declared).
      facts.append(
        AttributedFact(
          target_element_id=mechanics.default_change_tag_element_id,
          target_qname=(
            mechanics.default_change_tag_element_id
            if mechanics.default_change_tag_element_id is not None
            else f"{mechanics.bs_source_qname}#residual"
          ),
          value_cents=residual,
          period_start=period_start,
          period_end=period_end,
          event_ids=[],
          matched_line_count=0,
          is_residual=True,
        )
      )

  return facts


def _bs_period_delta_cents(
  session: Session,
  bs_element_id: str,
  period_start: date,
  period_end: date,
) -> int:
  """Sum ``debit_amount - credit_amount`` on the BS source element
  across every posted LineItem whose Entry.posting_date falls in the
  period. Debit-positive cents.

  Note: this is the **flow** over the period, not the ending balance.
  For asset accounts (debit-balance), positive return = balance
  increased over the period. For liability/equity (credit-balance),
  positive return = balance decreased.
  """
  row = session.execute(
    text(
      """
      SELECT COALESCE(
        SUM(li.debit_amount - li.credit_amount),
        0
      ) AS delta_cents
      FROM line_items li
      JOIN entries en ON en.id = li.entry_id
      WHERE li.element_id = :element_id
        AND en.posting_date BETWEEN :start AND :end
        AND en.status = 'posted'
      """
    ),
    {
      "element_id": bs_element_id,
      "start": period_start,
      "end": period_end,
    },
  ).fetchone()
  return int(row[0]) if row and row[0] is not None else 0


def _evaluate_one_filter(
  session: Session,
  bs_element_id: str,
  filter_target_element_id: str | None,
  filter_target_qname: str,
  field_name: str,
  values: list[str],
  period_start: date,
  period_end: date,
) -> AttributedFact | None:
  """Aggregate matched LineItems for one filter, return the fact (or
  ``None`` if no lines matched).

  Match criteria:
   - LineItem.element_id = BS source (only lines touching the BS account)
   - Entry.posting_date in period AND Entry.status='posted'
   - LineItem.metadata->>field IN values
  """
  rows = session.execute(
    text(
      """
      SELECT
        COALESCE(SUM(li.debit_amount - li.credit_amount), 0) AS value_cents,
        COUNT(*) AS matched_count,
        COALESCE(
          ARRAY_AGG(DISTINCT en.triggered_by_event_id)
            FILTER (WHERE en.triggered_by_event_id IS NOT NULL),
          ARRAY[]::text[]
        ) AS event_ids
      FROM line_items li
      JOIN entries en ON en.id = li.entry_id
      WHERE li.element_id = :element_id
        AND en.posting_date BETWEEN :start AND :end
        AND en.status = 'posted'
        AND li.metadata ->> :field = ANY(:values)
      """
    ),
    {
      "element_id": bs_element_id,
      "start": period_start,
      "end": period_end,
      "field": field_name,
      "values": values,
    },
  ).fetchone()

  if rows is None or rows[1] == 0:
    return None

  value_cents, matched_count, event_ids = rows
  return AttributedFact(
    target_element_id=filter_target_element_id,
    target_qname=filter_target_qname,
    value_cents=int(value_cents or 0),
    period_start=period_start,
    period_end=period_end,
    event_ids=list(event_ids or []),
    matched_line_count=int(matched_count),
    is_residual=False,
  )


__all__ = [
  "AttributedFact",
  "RollforwardResidualError",
  "evaluate_attribution_filters",
]
