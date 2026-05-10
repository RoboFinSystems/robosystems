"""Account rollups response models.

Account rollups show how company-specific CoA accounts roll up to
standardized reporting line items (US GAAP elements) — the mapping
taxonomy rendered with current balances.
"""

from pydantic import BaseModel


class AccountRollupRow(BaseModel):
  """One CoA account contributing to a reporting concept's rollup."""

  element_id: str
  account_name: str
  account_code: str | None = None
  total_debits: float
  total_credits: float
  net_balance: float


class AccountRollupGroup(BaseModel):
  """All CoA accounts that roll up into a single reporting concept,
  with the group total and per-account contributions.
  """

  reporting_element_id: str
  reporting_name: str
  reporting_qname: str
  trait: str
  balance_type: str
  total: float
  accounts: list[AccountRollupRow]


class AccountRollupsResponse(BaseModel):
  """Mapping rendered as account rollups — every reporting concept the
  mapping defines, with the CoA accounts that contribute to it and the
  current balance for each. ``total_unmapped`` tracks gaps for UI.
  """

  mapping_id: str
  mapping_name: str
  groups: list[AccountRollupGroup]
  total_mapped: int
  total_unmapped: int
