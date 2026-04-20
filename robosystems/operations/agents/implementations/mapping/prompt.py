"""System prompt for the MappingAgent (CoA → FAC mapping).

FAC (Fundamental Accounting Concepts — Charlie Hoffman's Seattle Method) is
the primary target for CoA mapping. Its ~177 clean semantic concepts give
a small, judgment-friendly target space compared to rs-gaap's ~2,000
variants. Filing-specific rs-gaap / us-gaap variants are derived by
deterministic equivalence-arc expansion downstream — the LLM does not
pick them.

When the caller passes an ``element_id`` (which the MappingAgent does),
the backend narrows candidates further: it reads the element's SFAC 6
anchor association (from the CoA classification tagging migration) and
returns only the FAC concepts reachable from that anchor via
``sfac6-to-fac`` arcs. Typical candidate set: 7 to 40 concepts, all already
on the correct SFAC 6 branch.
"""

MAPPING_SYSTEM_PROMPT = """You are a financial mapping specialist. Your task is to map \
Chart of Accounts (CoA) elements from a private company to the closest concept in \
FAC (Fundamental Accounting Concepts, Charlie Hoffman's Seattle Method).

FAC is deliberately a small, clean semantic space (~177 concepts) — filing-specific \
rs-gaap / us-gaap variants are expanded downstream via deterministic equivalence arcs. \
You pick the semantic anchor; the system picks the variant.

## Classification axis

Every CoA element and every FAC candidate carries a `classification` on the \
economic-nature axis. Six values (plus NULL for structural rows):

- **asset** — resources controlled by the entity (balance-sheet stock, debit balance)
- **liability** — obligations (balance-sheet stock, credit balance)
- **equity** — residual interest (balance-sheet stock, credit balance)
- **inflow** — credit flows: revenues + gains (income-statement duration, credit balance)
- **outflow** — debit flows: expenses + losses + COGS (income-statement duration, debit balance)
- **cashflow** — cash-statement reconciliation items and movements (no SFAC 6 root)

Note: `inflow` collapses SFAC 6's Revenues + Gains; `outflow` collapses Expenses + \
Losses. Candidates have already been filtered to match the CoA element's classification \
(typically to a specific SFAC 6 anchor subtree), so you don't need to re-filter — focus \
on choosing the best semantic match within the candidates.

## Matching Rules

1. **Match by semantic meaning**, not just name similarity
   - "Checking Account" (asset) → `fac:CashAndCashEquivalents` (it's cash, not a receivable)
   - "Advertising" (outflow) → `fac:SellingGeneralAndAdministrativeExpense` (it's SG&A)
2. **Prefer broader FAC concepts** when the CoA element is non-specific, more specific \
FAC concepts when the CoA element is clearly sub-categorized
   - "Sales" (inflow) → `fac:Revenues` (broad — matches "all revenue")
   - "Product Sales" (inflow) → `fac:Revenues` (still broad; FAC doesn't split product/service at this layer)
   - "Interest Income" (inflow) → `fac:InterestIncomeOperating` or `fac:NonoperatingIncomeLoss` \
if the candidate set has one (specific enough)
3. **Use external_source context** when available
   - QuickBooks account types (e.g., "Other Current Asset") provide strong classification signals
4. **One-to-one mapping** — each CoA element maps to exactly one FAC concept
5. **Skip abstract candidates** — FAC has abstract/grouping concepts (`is_abstract=true`); \
always target concrete ones

## Confidence Scoring

- **0.95+**: Exact semantic match (e.g., "Cash" → `fac:CashAndCashEquivalents`)
- **0.85-0.94**: Strong match with minor ambiguity (e.g., "AR" → `fac:ReceivablesNetCurrent`)
- **0.70-0.84**: Reasonable match, may need human review (e.g., "Misc Expense" → `fac:OtherOperatingIncomeExpenses`)
- **<0.70**: Too ambiguous — do not map, output null target

## Response Format

For each element, respond with a JSON array. Each item:
```json
{
  "element_id": "the source element ID",
  "target_id": "the target FAC element ID (or null if confidence < 0.70)",
  "target_qname": "fac:ConceptName (or null)",
  "confidence": 0.XX,
  "reasoning": "brief explanation of why this mapping was chosen"
}
```

Respond ONLY with the JSON array, no other text."""


RS_GAAP_REFINEMENT_SYSTEM_PROMPT = """You are a financial taxonomy specialist. \
A Chart of Accounts element has already been mapped to FAC (Fundamental Accounting Concepts). \
Your task is to select the most specific rs-gaap filing tag from the type-subtype children \
of the FAC element's rs-gaap equivalent.

## Selection Rules

1. **Semantic precision first** — pick the child whose economic content best matches the \
CoA account (e.g. "Accounts Receivable" → `rs-gaap:ReceivablesNetCurrent`, \
not the broad `rs-gaap:AssetsCurrent`)
2. **Use the parent if no child is more specific** — if all children are irrelevant or \
too narrow, return the rs-gaap parent
3. **Prefer commonly filed tags** — when two children are equally valid, prefer the one \
that appears in mainstream filers (shorter, less specialized name)
4. **One-to-one** — one rs-gaap tag per CoA element

## Response Format

```json
{
  "coa_element_id": "the CoA element ID",
  "rs_gaap_id": "the best rs-gaap element ID",
  "rs_gaap_qname": "rs-gaap:ConceptName",
  "confidence": 0.XX,
  "reasoning": "brief explanation"
}
```

Respond ONLY with the JSON object, no other text."""


def build_rs_gaap_refinement_prompt(
  coa_element: dict,
  fac_qname: str,
  rs_gaap_parent: dict | None,
  candidates: list[dict],
) -> str:
  """Build the user message for the rs-gaap refinement pass.

  When ``rs_gaap_parent`` is None (wide-equivalence case) the candidates are
  the industry-specific equivalents themselves — no parent to fall back on.
  """
  candidates_text = "\n".join(
    f"- id={c['id']}, qname={c['qname']}, name={c['name']}" for c in candidates
  )

  if rs_gaap_parent is not None:
    parent_section = (
      f"## rs-gaap Parent (fac-to-rs-gaap equivalence)\n"
      f"id={rs_gaap_parent['id']}, qname={rs_gaap_parent['qname']}, "
      f"name={rs_gaap_parent['name']}\n\n"
      f"## Type-Subtype Children (more specific filing options)\n"
      f"{candidates_text if candidates_text else '(none — use the parent)'}\n\n"
      f"Pick the single best rs-gaap concept. Use the parent if no child is more appropriate."
    )
  else:
    parent_section = (
      f"## rs-gaap Candidates (industry-specific filing variants)\n"
      f"{candidates_text}\n\n"
      f"Pick the single best rs-gaap concept for this specific account."
    )

  return f"""Select the most specific rs-gaap filing tag for this account.

## CoA Account
id={coa_element["id"]}, name={coa_element["name"]}, \
classification={coa_element.get("classification", "?")}

## Already Mapped To (FAC)
{fac_qname}

{parent_section}"""


def build_mapping_prompt(
  elements: list[dict],
  candidates: list[dict],
) -> str:
  """Build the user message for a batch mapping request.

  Args:
      elements: CoA elements to map (from get-unmapped-elements).
      candidates: FAC concepts to match against (from suggest-mapping; anchor-narrowed
          via the element's SFAC 6 tagging when available, classification-only
          fallback otherwise).

  Returns:
      Formatted user message for Bedrock.
  """
  elements_text = "\n".join(
    f"- id={e['id']}, code={e.get('code', '?')}, name={e['name']}, "
    f"classification={e.get('classification', '?')}, "
    f"balance_type={e.get('balance_type', '?')}, "
    f"external_source={e.get('external_source', '?')}"
    for e in elements
  )

  candidates_text = "\n".join(
    f"- id={c['id']}, qname={c.get('qname', '?')}, name={c['name']}, "
    f"classification={c.get('classification', '?')}, "
    f"depth={c.get('depth', '?')}, is_abstract={c.get('is_abstract', False)}"
    for c in candidates
  )

  return f"""Map the following Chart of Accounts elements to the best matching \
FAC (Fundamental Accounting Concepts) concept from the candidates list.

## CoA Elements to Map
{elements_text}

## Available FAC Candidates (anchor-narrowed)
{candidates_text}

Map each element to the single best candidate. Do NOT map abstract concepts. \
If no good match exists (confidence < 0.70), set target_id and target_qname to null."""
