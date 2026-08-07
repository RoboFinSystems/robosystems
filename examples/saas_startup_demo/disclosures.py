"""Cadence Labs' tenant-authored disclosure notes.

The revenue-disaggregation note is authored per-tenant through the
TaxonomyBlock envelope (``reporting_extension``) — two member concepts
parented under the rs-gaap revenue leaf, one ``regulatory_disclosure``
structure with ``concept_arrangement='roll_up'``, and presentation +
calculation arcs whose total **is** the library leaf. ASC 606's
disaggregation-of-revenue note is the canonical first numeric note for
a SaaS filer: subscription vs. professional services is exactly the
split investors ask for.

Rendering is fact-driven: each revenue account multi-maps to its member
concept AND the income-statement leaf, so the face Revenue line and the
note total are literally the same fact, and the note's internal footing
(Σ members = total) is validated by the auto-emitted RollUp rule.
"""

from __future__ import annotations

REVENUE_TOTAL_QNAME = "rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax"

REVENUE_NOTE: dict = {
  "taxonomy_name": "Cadence Reporting Extension",
  "name": "Disaggregation of Revenue",
  # Note 2 — after Significant Accounting Policies (canonically Note 1).
  "note_order": 2,
  "description": (
    "Revenue disaggregated by stream — annual subscriptions and "
    "onboarding professional services — footing to the income-statement "
    "revenue line (ASC 606-10-50-5)."
  ),
  "role_uri": (
    "https://cadencelabs.example.com/roles/disclosures/DisaggregationOfRevenue"
  ),
  "total_qname": REVENUE_TOTAL_QNAME,
  # (member qname, display name, CoA code multi-mapped to the member).
  # Revenue members are credit-balance duration concepts — the runner
  # passes these through to the envelope element requests.
  "members": [
    {
      "qname": "cadence:SubscriptionRevenue",
      "name": "Subscription Revenue",
      "coa_code": "4000",
      "balance_type": "credit",
      "period_type": "duration",
    },
    {
      "qname": "cadence:ProfessionalServicesRevenue",
      "name": "Professional Services Revenue",
      "coa_code": "4100",
      "balance_type": "credit",
      "period_type": "duration",
    },
  ],
}

DISCLOSURE_NOTES: list[dict] = [REVENUE_NOTE]

# ── Text-block notes — the narrative half of the disclosure surface ────────
#
# Each concept binds one of the uploaded policy documents (policies.py
# DOCUMENTS, matched by title) as a ``Nonnumeric`` text-block fact via
# bind-text-block. The document stays the editable source of truth; the
# report snapshots the bound text at generation time.

POLICIES_NOTE: dict = {
  "taxonomy_name": "Cadence Policy Notes",
  "name": "Significant Accounting Policies",
  # Note 1 — the canonical first note of every report.
  "note_order": 1,
  "description": (
    "Significant accounting policies — ASC 606 revenue recognition for "
    "annual prepaid subscriptions, and operating-expense/burn policy — "
    "bound from the company's policy documents."
  ),
  "role_uri": (
    "https://cadencelabs.example.com/roles/disclosures/AccountingPolicies"
  ),
  "abstract_qname": "cadence:SignificantAccountingPoliciesAbstract",
  "abstract_name": "Significant Accounting Policies",
  # (concept qname, display name, uploaded document title to bind)
  "concepts": [
    {
      "qname": "cadence:RevenueRecognitionPolicyTextBlock",
      "name": "Revenue Recognition Policy",
      "document_title": "Revenue Recognition Policy",
    },
    {
      "qname": "cadence:OperatingExpensePolicyTextBlock",
      "name": "Operating Expense and Burn Policy",
      "document_title": "Operating Expense and Burn Policy",
    },
  ],
}

TEXT_BLOCK_NOTES: list[dict] = [POLICIES_NOTE]
