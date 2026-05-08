"""Account/Element response models."""

from __future__ import annotations

from pydantic import BaseModel, Field

from robosystems.models.api.common import PaginationInfo


class AccountResponse(BaseModel):
  """One CoA account (Element) — the basic chart-of-accounts row.

  ``trait`` carries the FASB classification (asset/liability/equity/
  revenue/expense/etc.); ``balance_type`` is the natural side
  ('debit' or 'credit'). ``account_type`` is a free-form sub-grouping
  (e.g. 'cash', 'inventory') used by some integrations. Hierarchy is
  expressed via ``parent_id`` + ``depth``.
  """

  id: str
  code: str | None = None
  name: str
  description: str | None = None
  trait: str | None = None
  sub_classification: str | None = None
  balance_type: str
  parent_id: str | None = None
  depth: int
  currency: str
  is_active: bool
  is_placeholder: bool
  account_type: str | None = None
  external_id: str | None = None
  external_source: str | None = None


class AccountListResponse(BaseModel):
  """Paginated chart-of-accounts listing — flat (use the tree endpoint
  for parent/child structure)."""

  accounts: list[AccountResponse]
  pagination: PaginationInfo


class AccountTreeNode(BaseModel):
  id: str
  code: str | None = None
  name: str
  trait: str | None = None
  account_type: str | None = None
  balance_type: str
  depth: int
  is_active: bool
  children: list[AccountTreeNode] = Field(default_factory=list)


class AccountTreeResponse(BaseModel):
  roots: list[AccountTreeNode]
  total_accounts: int
