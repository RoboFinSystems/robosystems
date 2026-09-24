"""Filter evaluation engine for ``rollforward`` Information Blocks.

Decomposes a BS account's period change across declared flow concepts by
matching LineItems on ``flow_element_id`` (filters name flow concepts by
qname). Untagged data goes through ``fact_grid._derive_cash_flow_facts``
instead.

All amounts are **debit-positive cents** (``debit_amount - credit_amount``);
the renderer flips signs for presentation.

**Residual** = ΔBS source over the period - Σ filter matches. When non-zero:
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
from robosystems.operations.roboledger.entry_status import (
  landed_entry_bindparam,
)


class RollforwardResidualError(ValueError):
  """Σ filter matches != Δ BS and ``validation_mode='strict'``."""

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
  """One filter-matched (or residual) fact for a single period.

  ``value_cents`` is debit-positive. ``event_ids`` are the distinct
  ``triggered_by_event_id`` values of the matched entries; empty for manual
  entries.
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
  """Evaluate a rollforward's filters over one period.

  Returns one fact per filter that matched lines, in declaration order, with
  any residual fact last (per ``mechanics.validation_mode``).

  Runs 1 + N queries per period; batch across periods before wiring this
  into multi-period rendering.
  """
  facts: list[AttributedFact] = []
  bs_element_id = mechanics.bs_source_element_id

  bs_delta = _bs_period_delta_cents(session, bs_element_id, period_start, period_end)

  sum_matched = 0
  for f in mechanics.attribution_filters:
    matched = _evaluate_one_filter(
      session=session,
      bs_element_id=bs_element_id,
      filter_target_element_id=f.target_element_id,
      filter_target_qname=f.target_qname,
      flow_qnames=f.predicate.values,
      period_start=period_start,
      period_end=period_end,
    )
    if matched is None:
      # No lines matched. Lines that net to zero still emit a fact: activity
      # that cancelled out is an audit signal.
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
  """The BS source element's flow over the period (not its ending balance).

  Debit-positive: positive means an asset rose, or a liability/equity fell.
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
        AND en.status IN :landed_entry_statuses
      """
    ).bindparams(landed_entry_bindparam()),
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
  flow_qnames: list[str],
  period_start: date,
  period_end: date,
) -> AttributedFact | None:
  """Aggregate one filter's landed LineItems on the BS source in the period.

  Matches ``flow_element_id`` against the elements named by ``flow_qnames``.
  Returns ``None`` when no qname resolves or no line matched.
  """
  value_ids = [
    r[0]
    for r in session.execute(
      text("SELECT id FROM elements WHERE qname = ANY(:qnames)"),
      {"qnames": flow_qnames},
    ).fetchall()
  ]
  if not value_ids:
    return None

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
        AND en.status IN :landed_entry_statuses
        AND li.flow_element_id = ANY(:value_ids)
      """
    ).bindparams(landed_entry_bindparam()),
    {
      "element_id": bs_element_id,
      "start": period_start,
      "end": period_end,
      "value_ids": value_ids,
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
