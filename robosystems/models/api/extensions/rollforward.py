"""API request models for the ``rollforward`` Information Block type.

Mirrors the shape of ``schedules.py`` — request bodies live here, the
typed ``RollforwardMechanics`` envelope lives in
``robosystems.models.api.information_block`` alongside the other block-
type mechanics shapes.

The rollforward block-type implements **Tier 2 filter-based attribution**
— the operator authors filter predicates that match ledger LineItems by
metadata field, and at render time the engine decomposes a BS account's
period delta across the declared flow concepts. A single predicate kind
(``line_item_metadata_field``) is implemented; additional predicate
shapes (dimension, counter-account, classification, expression) are not
yet supported.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

# ── Attribution predicate ──────────────────────────────────────────────────


class LineItemMetadataPredicate(BaseModel):
  """Filter ledger LineItems by flow concept.

  The single predicate kind shipped to date. ``values`` are flow-concept
  qnames — mini's ``TransactionDescriptionCode`` values, rs-gaap flow
  concepts (what the enrichment classifier emits for QuickBooks data),
  future XBRL GL ``GenericFlowCategory`` codes. The engine resolves them
  to element_ids and matches the first-class ``LineItem.flow_element_id``
  FK; matched lines aggregate signed into the attributed fact for the
  period.

  ``field`` is **legacy and ignored** — the flow tag used to live in
  ``line_items.metadata[field]`` but has been promoted to the typed
  ``flow_element_id`` FK. Retained for wire-compatibility; the engine no
  longer reads it.
  """

  kind: Literal["line_item_metadata_field"] = Field(
    "line_item_metadata_field",
    description="Discriminator value selecting this predicate shape.",
  )
  field: str = Field(
    "transaction_description_code",
    description=(
      "Legacy/ignored. The flow tag now lives in the typed "
      "``flow_element_id`` FK, not JSONB metadata; the engine no longer "
      "reads this. Retained for wire-compatibility."
    ),
  )
  values: list[str] = Field(
    ...,
    min_length=1,
    description=(
      "Flow-concept qnames that route to this filter's target concept. "
      "A LineItem matches when its ``flow_element_id`` is one of the "
      "elements named here AND the line falls within the rollforward's "
      "period."
    ),
  )


# Future predicate shapes will land as siblings; the union widens here
# without breaking the wire shape (Pydantic discriminator). Today there's
# only one kind so the type alias is a single class.
AttributionPredicate = LineItemMetadataPredicate


# ── Attribution filter (predicate + target) ────────────────────────────────


class AttributionFilter(BaseModel):
  """One flow-concept attribution rule on a rollforward IB.

  Pairs a target concept (the flow leaf the matched amount counts
  toward) with a predicate (which LineItems match). The rollforward's
  ``attribution_filters: list[AttributionFilter]`` declares every flow
  the BS source decomposes into; the renderer evaluates them all per
  period.

  ``target_element_id`` is resolved at create time from ``target_qname``
  via the rs-gaap library + tenant taxonomy lookup. Authors only need
  to provide the qname; the element_id is filled in by the create
  handler and the resolved value is what the envelope round-trips.
  """

  target_qname: str = Field(
    ...,
    description=(
      "QName of the flow concept this filter produces facts for — e.g. "
      "``rs-gaap:ProceedsFromIssuanceOfCommonStock``. Resolved to "
      "``target_element_id`` at create time."
    ),
  )
  target_element_id: str | None = Field(
    None,
    description=(
      "Resolved element id for ``target_qname``. Null at create time; "
      "populated by the handler before persistence. Round-tripped in "
      "the envelope."
    ),
  )
  predicate: AttributionPredicate = Field(
    ...,
    description="Predicate that determines which LineItems match this filter.",
  )


# ── Create / update / delete request bodies ────────────────────────────────


class CreateRollforwardRequest(BaseModel):
  """Create a rollforward Information Block.

  Mirrors :class:`CreateScheduleRequest` in shape. The block decomposes
  the period change in ``bs_source_qname`` across the declared
  attribution filters. Residual (Δ BS - Σ filter matches) falls back to
  the default change tag — or, if no default is declared, surfaces as
  an unattributed fact tagged with a synthetic residual concept.
  """

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {
          "name": "Cash and Cash Equivalents Rollforward",
          "bs_source_qname": "mini:CashAndCashEquivalents",
          "default_change_tag_qname": (
            "rs-gaap:CashCashEquivalentsRestrictedCashAndRestrictedCash"
            "EquivalentsPeriodIncreaseDecreaseIncludingExchangeRateEffect"
          ),
          "attribution_filters": [
            {
              "target_qname": "mini:ProceedsFromInvestmentsByOwner",
              "predicate": {
                "kind": "line_item_metadata_field",
                "field": "transaction_description_code",
                "values": ["mini:ProceedsFromInvestmentsByOwner"],
              },
            }
          ],
        }
      ]
    }
  )

  name: str = Field(..., description="Human-readable block name.")
  bs_source_qname: str = Field(
    ...,
    description=(
      "QName of the balance-sheet element whose period delta this "
      "block decomposes. Resolved to ``bs_source_element_id`` at "
      "create time."
    ),
  )
  default_change_tag_qname: str | None = Field(
    None,
    description=(
      "QName of the fallback flow concept (Tier 1 default change tag). "
      "Residual amount — Δ BS minus the sum of filter-matched amounts — "
      "is attributed to this concept. When omitted, residual surfaces "
      "unattributed; the validation_mode setting governs whether that's "
      "a hard error."
    ),
  )
  attribution_filters: list[AttributionFilter] = Field(
    default_factory=list,
    description="Filter predicates routing LineItems to flow concepts.",
  )
  validation_mode: Literal["strict", "residual_as_default", "warn_only"] = Field(
    "residual_as_default",
    description=(
      "How the renderer arbitrates when Σ filter matches != Δ BS. "
      "``strict`` raises; ``residual_as_default`` emits the residual "
      "as a default-tag fact (the common case); ``warn_only`` logs and "
      "lets the imbalance pass."
    ),
  )
  taxonomy_id: str | None = Field(
    None,
    description=(
      "Owning taxonomy id (auto-resolved from ``bs_source_qname`` when omitted)."
    ),
  )


class UpdateRollforwardRequest(BaseModel):
  """Update mutable fields on a rollforward block.

  Editable: name, default_change_tag_qname, attribution_filters,
  validation_mode. The BS source is fixed once the block is created
  (changing it would invalidate every previously rendered period); to
  change BS source, delete and re-create.

  **Partial-update semantics**: omitted (``None``) fields mean "leave
  unchanged" — there is no wire-level way to *clear* a previously set
  default change tag or empty the attribution_filters list via this
  endpoint. To remove the default tag entirely, delete and re-create
  the rollforward block. The asymmetry is deliberate: an explicit
  clear-sentinel adds wire-shape complexity for a use case that rarely
  arises in practice (default tags are typically set during initial
  authoring and only swapped, not removed).
  """

  structure_id: str = Field(..., description="Structure ID of the rollforward block.")
  name: str | None = None
  default_change_tag_qname: str | None = Field(
    None,
    description=(
      "New default change tag qname. Pass a value to *change* the "
      "default; omit (``None``) to leave unchanged. There is no "
      "wire-level way to clear a previously set default — see the "
      "class docstring."
    ),
  )
  attribution_filters: list[AttributionFilter] | None = None
  validation_mode: Literal["strict", "residual_as_default", "warn_only"] | None = None


class DeleteRollforwardRequest(BaseModel):
  """Delete a rollforward block.

  Cascades through any synthetic facts produced by this block's filter
  evaluations. The underlying ledger LineItems are not touched — only
  the rollforward IB's projection of them.
  """

  structure_id: str = Field(..., description="Structure ID of the rollforward block.")
