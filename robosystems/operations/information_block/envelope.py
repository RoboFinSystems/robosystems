"""Cross-type assembly helpers for building Information Block envelopes.

Block-type handlers (``schedule.py`` today, more in future phases) each
own the logic that's specific to their shape. The helpers here are the
generic atom → Lite conversions shared by every handler — one place to
maintain the ORM → wire-shape mapping so Phase b's Statement handler
doesn't diverge from Phase a's Schedule handler.
"""

from __future__ import annotations

from robosystems.models.api.information_block import (
  ConnectionLite,
  ElementLite,
  FactLite,
)
from robosystems.models.extensions import Association, Element
from robosystems.models.extensions.roboledger import Fact


def element_to_lite(element: Element) -> ElementLite:
  """Project an :class:`Element` ORM row onto :class:`ElementLite`."""
  return ElementLite(
    id=element.id,
    qname=element.qname,
    name=element.name,
    code=element.code,
    element_type=element.element_type,
    is_abstract=element.is_abstract,
    is_monetary=element.is_monetary,
    balance_type=element.balance_type,
    period_type=element.period_type,
  )


def association_to_connection(association: Association) -> ConnectionLite:
  """Project an :class:`Association` ORM row onto :class:`ConnectionLite`."""
  return ConnectionLite(
    id=association.id,
    from_element_id=association.from_element_id,
    to_element_id=association.to_element_id,
    association_type=association.association_type,
    arcrole=association.arcrole,
    order_value=association.order_value,
    weight=association.weight,
  )


def fact_to_lite(fact: Fact) -> FactLite:
  """Project a :class:`Fact` ORM row onto :class:`FactLite`."""
  return FactLite(
    id=fact.id,
    element_id=fact.element_id,
    value=fact.value,
    period_start=fact.period_start,
    period_end=fact.period_end,
    period_type=fact.period_type,
    unit=fact.unit,
    fact_scope=fact.fact_scope,
    fact_set_id=fact.fact_set_id,
  )


__all__ = [
  "association_to_connection",
  "element_to_lite",
  "fact_to_lite",
]
