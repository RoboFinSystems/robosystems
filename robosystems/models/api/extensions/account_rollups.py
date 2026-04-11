"""Account rollups response models.

Account rollups show how company-specific CoA accounts roll up to
standardized reporting line items (US GAAP elements) — the mapping
taxonomy rendered with current balances.
"""

from pydantic import BaseModel


class AccountRollupRow(BaseModel):
  element_id: str
  account_name: str
  account_code: str | None = None
  total_debits: float
  total_credits: float
  net_balance: float


class AccountRollupGroup(BaseModel):
  reporting_element_id: str
  reporting_name: str
  reporting_qname: str
  classification: str
  balance_type: str
  total: float
  accounts: list[AccountRollupRow]


class AccountRollupsResponse(BaseModel):
  mapping_id: str
  mapping_name: str
  groups: list[AccountRollupGroup]
  total_mapped: int
  total_unmapped: int
