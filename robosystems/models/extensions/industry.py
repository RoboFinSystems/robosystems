"""Industry: one node of the industry classification.

Public schema only, and never copied into a tenant: the vocabulary is one global
list, and all a tenant holds is an id on ``Entity.industry``. Seeded from
``frameworks/industries/industries.csv`` by ``robosystems.taxonomy.industries``.
"""

from sqlalchemy import Boolean, CheckConstraint, Column, Index, String

from robosystems.db.extensions import ExtensionsBase


class Industry(ExtensionsBase):
  __tablename__ = "industries"
  __table_args__ = (
    Index("idx_industries_parent", "parent_id"),
    CheckConstraint(
      "level IN ('sector', 'subsector', 'industry', 'sub-industry')",
      name="check_industry_level",
    ),
    {"schema": "public"},
  )

  # The slug path: a node's ancestors are its id's prefixes.
  id = Column(String, primary_key=True)
  parent_id = Column(String, nullable=True)
  level = Column(String, nullable=False)
  label = Column(String, nullable=False)
  # An undivided subsector's one industry, repeating its name.
  carried = Column(Boolean, nullable=False, default=False)
  # Set on a retired node: the id that stands for it now.
  replaced_by = Column(String, nullable=True)

  def __repr__(self) -> str:
    return f"<Industry {self.id}>"
