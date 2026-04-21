"""Account/Element response models."""

from __future__ import annotations

from pydantic import BaseModel, Field

from robosystems.models.api.common import PaginationInfo


class AccountResponse(BaseModel):
  id: str
  code: str | None = None
  name: str
  description: str | None = None
  classification: str | None = None
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
  accounts: list[AccountResponse]
  pagination: PaginationInfo


class AccountTreeNode(BaseModel):
  id: str
  code: str | None = None
  name: str
  classification: str | None = None
  account_type: str | None = None
  balance_type: str
  depth: int
  is_active: bool
  children: list[AccountTreeNode] = Field(default_factory=list)


class AccountTreeResponse(BaseModel):
  roots: list[AccountTreeNode]
  total_accounts: int
