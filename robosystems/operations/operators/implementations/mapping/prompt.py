"""System prompt for the MappingOperator (CoA → rs-gaap mapping).

rs-gaap is the canonical reporting vocabulary and the direct mapping target:
the LLM matches each Chart-of-Accounts element to the closest rs-gaap concept
the renderer consumes. (The FAC view is derived downstream from each CoA →
rs-gaap arc via the fac-to-rs-gaap bridge — it is never picked here.)

Candidates are narrowed by the backend before they reach the model: by the
element's FASB elementsOfFinancialStatements (EFS) trait
(asset/liability/equity/revenue/expense/gain/loss), by liquidity
(current/noncurrent) for assets and liabilities, and restricted to the
concepts that actually render under the active Reporting Style. So the model
chooses among a tight, section-correct set rather than the full ~2,000 rs-gaap
variants. Typical candidate set: a few dozen concepts.
"""

MAPPING_SYSTEM_PROMPT = """You are a financial mapping specialist. Your task is to map \
Chart of Accounts (CoA) elements from a private company to the closest concept in \
rs-gaap, the canonical reporting vocabulary the financial statements are rendered from.

The candidate list has already been narrowed for you — by the CoA element's FASB \
elementsOfFinancialStatements (EFS) trait, by liquidity (current/noncurrent) for assets \
and liabilities, and to the concepts that actually render under the active reporting \
style. Pick the single best concept from the candidates.

## Trait axis

Every CoA element and every candidate carries a `trait` on the \
FASB elementsOfFinancialStatements axis. Primary values for CoA mapping:

- **asset** — resources controlled by the entity (balance-sheet stock, debit balance)
- **liability** — obligations (balance-sheet stock, credit balance)
- **equity** — residual interest (balance-sheet stock, credit balance)
- **revenue** — inflows from primary operations (income-statement duration, credit balance)
- **expense** — outflows from primary operations (income-statement duration, debit balance)
- **gain** — non-operating credit flows (income-statement duration, credit balance)
- **loss** — non-operating debit flows (income-statement duration, debit balance)

Candidates have already been filtered to match the CoA element's trait, so you don't \
need to re-filter — focus on choosing the best semantic match within the candidates.

## Matching Rules

1. **Match by semantic meaning**, not just name similarity
   - "Checking Account" (asset) → `rs-gaap:CashAndCashEquivalentsAtCarryingValue` (it's cash, not a receivable)
   - "Advertising" (expense) → `rs-gaap:SellingAndMarketingExpense` (it's a marketing cost)
2. **Target the concept the statement actually lists** — pick the specific reporting \
concept that matches the account; never map to a statement-level subtotal (Assets, \
Revenues, GrossProfit, NetIncomeLoss, …), which the renderer computes by rolling up its \
children. The candidate set is leaf-oriented for this reason.
   - "Sales" (revenue) → `rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax` (the ASC 606 revenue concept)
   - "Accounts Receivable" (asset) → `rs-gaap:ReceivablesNetCurrent`
3. **Use external_source context** when available
   - QuickBooks account types (e.g., "Other Current Asset") provide strong classification signals
4. **One-to-one mapping** — each CoA element maps to exactly one rs-gaap concept
5. **Skip abstract candidates** — rs-gaap has abstract/grouping concepts (`is_abstract=true`); \
always target concrete ones
6. **Fixed assets: prefer the gross cost concept, not the net** — a fixed-asset cost \
account ("Equipment", "Furniture & Fixtures", "Vehicles", "Buildings", "Property & \
Equipment", "Gross Fixed Assets") should map to the **gross** PP&E concept \
(`rs-gaap:PropertyPlantAndEquipmentGross`) when both gross and net appear in the candidates, \
NOT the net concept. Reason: the cash-flow statement derives capital expenditures from \
the change in GROSS PP&E; mapping to net conflates purchases with depreciation and \
silently drops capex from investing activities. (The accumulated-depreciation contra \
account is routed deterministically and needs no judgment here.)

## Confidence Scoring

- **0.95+**: Exact semantic match (e.g., "Cash" → `rs-gaap:CashAndCashEquivalentsAtCarryingValue`)
- **0.85-0.94**: Strong match with minor ambiguity (e.g., "AR" → `rs-gaap:ReceivablesNetCurrent`)
- **0.70-0.84**: Reasonable match, may need human review (a vaguely-named "Misc Expense")
- **<0.70**: Too ambiguous — do not map, output null target

## Response Format

Respond with a SINGLE JSON array. The array MUST contain EXACTLY ONE \
object per input `element_id` — no skipping, no omitting, even when no \
candidate seems right (return `target_id: null` and `confidence: 0` for \
those, do NOT drop the element from the array). Length of the response \
array must equal the number of CoA elements in the prompt.

Do NOT emit one object per line, do NOT emit multiple arrays, and do NOT \
add explanatory prose before, between, or after the array. The complete \
response must parse cleanly with `json.loads(content)` as a list.

Item shape:
```json
{
  "element_id": "the source element ID — MUST appear in the input list",
  "target_id": "the target rs-gaap element ID (or null if confidence < 0.70)",
  "target_qname": "rs-gaap:ConceptName (or null)",
  "confidence": 0.XX,
  "reasoning": "brief explanation of why this mapping was chosen (or why no candidate fits)"
}
```

Respond ONLY with the JSON array, no other text."""


def build_mapping_prompt(
  elements: list[dict],
  candidates: list[dict],
) -> str:
  """Build the user message for one batch mapping request.

  `elements` comes from `get-unmapped-elements`; `candidates` comes from
  `suggest-mapping`, already EFS- and liquidity-narrowed and restricted to
  concepts that render under the active reporting style.
  """
  elements_text = "\n".join(
    f"- id={e['id']}, code={e.get('code', '?')}, name={e['name']}, "
    f"trait={e.get('trait', '?')}, "
    f"balance_type={e.get('balance_type', '?')}, "
    f"external_source={e.get('external_source', '?')}"
    for e in elements
  )

  candidates_text = "\n".join(
    f"- id={c['id']}, qname={c.get('qname', '?')}, name={c['name']}, "
    f"trait={c.get('trait', '?')}, "
    f"depth={c.get('depth', '?')}, is_abstract={c.get('is_abstract', False)}"
    for c in candidates
  )

  return f"""Map the following Chart of Accounts elements to the best matching \
rs-gaap concept from the candidates list.

## CoA Elements to Map
{elements_text}

## Available rs-gaap Candidates (trait- and style-narrowed)
{candidates_text}

Map each element to the single best candidate. Do NOT map abstract concepts. \
If no good match exists (confidence < 0.70), set target_id and target_qname to null."""
