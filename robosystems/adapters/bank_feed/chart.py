"""The chart as a bank feed sees it — pure, no session.

``BankAccount`` is one account the feed exposes (a chart account is linked to
each); ``ChartIndex`` resolves a hint against the tenant's chart, by name then
by code. ``accounts.build_chart_index`` fills it from the graph.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from robosystems.adapters.bank_feed.hints import AccountHint, parse_gl_code_name


def name_key(value: str) -> str:
  """``Mercury Checking ••5424`` and ``Mercury Checking 5424`` resolve alike."""
  return re.sub(r"[^a-z0-9]", "", value.lower())


@dataclass(frozen=True)
class BankAccount:
  """One account the feed exposes — a chart account is linked to each."""

  account_id: str
  name: str
  kind: str
  trait: str
  balance_type: str
  institution: str
  legal_business_name: str | None = None

  @property
  def is_credit(self) -> bool:
    return self.trait == "liability"


@dataclass
class ChartIndex:
  """Resolve a suggestion against the graph's chart, by name then by code."""

  by_name: dict[str, str] = field(default_factory=dict)
  by_code: dict[str, str] = field(default_factory=dict)

  def resolve(self, *candidates: str | None) -> str | None:
    for candidate in candidates:
      if not candidate:
        continue
      element_id = self.by_name.get(name_key(candidate)) or self.by_code.get(
        candidate.strip()
      )
      if element_id:
        return element_id
    return None

  def resolve_hint(self, hint: AccountHint) -> str | None:
    """A hint resolves by its own name, then by the names it goes by on the
    shipped chart templates."""
    return self.resolve(hint.name, *hint.aliases)

  def resolve_gl_code(self, gl_code_name: str | None) -> str | None:
    """A ``<code> - <name>`` label resolves by its code, then by its name."""
    if not gl_code_name:
      return None
    code, name = parse_gl_code_name(gl_code_name)
    if code and code in self.by_code:
      return self.by_code[code]
    return self.resolve(name, gl_code_name)
