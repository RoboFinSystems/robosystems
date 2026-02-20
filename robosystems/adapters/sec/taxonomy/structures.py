"""
Structure canonical concepts for statement type classification.
"""

from .concepts import CanonicalConcept

STRUCTURE_CONCEPTS = (
  CanonicalConcept(
    id="income_statement",
    display_name="Income Statement",
    category="structure",
    description="Consolidated statements of income, operations, and earnings",
    aliases=(
      "statement of operations",
      "statement of earnings",
      "profit and loss",
      "p&l",
      "statement of income",
    ),
    expected_elements=(),
    period_type="duration",
    balance=None,
    is_monetary=False,
  ),
  CanonicalConcept(
    id="balance_sheet",
    display_name="Balance Sheet",
    category="structure",
    description="Consolidated balance sheets and statements of financial position",
    aliases=(
      "statement of financial position",
      "statement of financial condition",
    ),
    expected_elements=(),
    period_type="instant",
    balance=None,
    is_monetary=False,
  ),
  CanonicalConcept(
    id="cash_flow_statement",
    display_name="Cash Flow Statement",
    category="structure",
    description="Consolidated statements of cash flows",
    aliases=(
      "statement of cash flows",
      "cash flow",
    ),
    expected_elements=(),
    period_type="duration",
    balance=None,
    is_monetary=False,
  ),
  CanonicalConcept(
    id="equity_statement",
    display_name="Statement of Equity",
    category="structure",
    description="Consolidated statements of stockholders equity and changes in equity",
    aliases=(
      "statement of stockholders equity",
      "statement of changes in equity",
      "shareholders equity statement",
    ),
    expected_elements=(),
    period_type="duration",
    balance=None,
    is_monetary=False,
  ),
  CanonicalConcept(
    id="comprehensive_income",
    display_name="Comprehensive Income Statement",
    category="structure",
    description="Consolidated statements of comprehensive income or loss",
    aliases=(
      "statement of comprehensive income",
      "other comprehensive income",
    ),
    expected_elements=(),
    period_type="duration",
    balance=None,
    is_monetary=False,
  ),
)
