"""Taxonomy curation and account mapping.

Taxonomy Blocks are the only tenant-facing path for ontology curation (raw
element/structure CRUD is not exposed). Mapping associations stay direct:
mapping is iterative AI-assisted craft, not a curation envelope.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, Header, Path
from pydantic import BaseModel, ConfigDict, Field

from robosystems.middleware.extensions import OperationSpec
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN
from robosystems.middleware.operations import (
  IdempotencyCache,
  OperationEnvelope,
  fingerprint_body,
  get_idempotency_cache,
  idempotent_dispatch,
  log_operation_audit,
  wrap_pending,
)
from robosystems.middleware.otel.metrics import endpoint_metrics_decorator
from robosystems.models.api.common import OPERATION_ERROR_RESPONSES, DeleteResult
from robosystems.models.api.extensions.taxonomies import (
  AssociationResponse,
  CreateMappingAssociationOperation,
  DeleteMappingAssociationOperation,
  EntityTaxonomyResponse,
  LinkEntityTaxonomyRequest,
)
from robosystems.models.api.taxonomy_block import (
  CreateTaxonomyBlockRequest,
  DeleteTaxonomyBlockRequest,
  DeleteTaxonomyBlockResponse,
  TaxonomyBlockEnvelope,
  UpdateTaxonomyBlockRequest,
)
from robosystems.models.core import User
from robosystems.operations.locking import RowLockedError
from robosystems.operations.roboledger.commands._guards import LibraryImmutableError
from robosystems.operations.roboledger.commands.taxonomies import (
  AssociationNotFoundError,
  ElementNotFoundError,
  EntityNotFoundError,
  EntityTaxonomyConflictError,
  MappingAssociationExistsError,
  MappingStructureNotFoundError,
  MappingTargetIsRollupError,
)
from robosystems.operations.roboledger.commands.taxonomies import (
  TaxonomyNotFoundError as TaxonomyMissingError,  # alias: avoids collision with commands.reports.TaxonomyNotFoundError
)
from robosystems.operations.roboledger.commands.taxonomies import (
  create_mapping_association as cmd_create_mapping_association,
)
from robosystems.operations.roboledger.commands.taxonomies import (
  delete_mapping_association as cmd_delete_mapping_association,
)
from robosystems.operations.roboledger.commands.taxonomies import (
  link_entity_taxonomy as cmd_link_entity_taxonomy,
)
from robosystems.operations.taxonomy_block.commands import (
  TaxonomyAuthoringDisabledError,
)
from robosystems.operations.taxonomy_block.commands import (
  create_taxonomy_block as cmd_create_taxonomy_block,
)
from robosystems.operations.taxonomy_block.commands import (
  delete_taxonomy_block as cmd_delete_taxonomy_block,
)
from robosystems.operations.taxonomy_block.commands import (
  update_taxonomy_block as cmd_update_taxonomy_block,
)
from robosystems.operations.taxonomy_block.immutability import ProtectedFactsError
from robosystems.routers.extensions.roboledger._common import (
  _RATE_LIMIT,
  _require_roboledger_write,
  make_registrar,
)

router = APIRouter()

_OP_TAG = "RoboLedger: Taxonomy & Mapping"
_registrar = make_registrar(router, _OP_TAG)


# ── Ontology / Taxonomy Blocks ───────────────────────────────────────────────

create_taxonomy_block_op = _registrar.register(
  OperationSpec(
    name="create-taxonomy-block",
    summary="Create Taxonomy Block",
    description=(
      "Create a taxonomy block atomically: one envelope carrying the "
      "taxonomy row plus its structures, elements, associations, and "
      "rules. Dispatches by `taxonomy_type` — `chart_of_accounts` "
      "(declarative tenant CoA), `reporting_extension`, and "
      "`custom_ontology` are supported; `reporting_standard` is "
      "library-origin (501). `reporting_extension` / `custom_ontology` "
      "authoring may be disabled per environment "
      "(TAXONOMY_AUTHORING_ENABLED) — disabled surfaces 403. "
      "NOT the path for a functional close schedule: a structure with "
      "block_type='schedule' here is a bare ontology row with none of the "
      "schedule machinery (per-period facts, schedule_entry_due "
      "obligations, closing-entry generator). To create a working "
      "schedule use create-information-block(block_type='schedule')."
    ),
    command=cmd_create_taxonomy_block,
    request_model=CreateTaxonomyBlockRequest,
    result_type=TaxonomyBlockEnvelope,
    error_map={
      TaxonomyAuthoringDisabledError: 403,
      ValueError: 422,
      NotImplementedError: 501,
    },
    mark_stale_reason="taxonomy_block_created",
  )
)


update_taxonomy_block_op = _registrar.register(
  OperationSpec(
    name="update-taxonomy-block",
    summary="Update Taxonomy Block",
    description=(
      "Incrementally mutate a taxonomy block via typed delta lists "
      "(elements/structures/associations/rules to add, update, remove). "
      "Dispatches by the target taxonomy's stored `taxonomy_type`. "
      "For a chart of accounts: add, rename and reclassify accounts freely; "
      "an account with facts or line items is never removed — retire it "
      "with `elements_to_update[].is_active=false` (history stays, new "
      "postings are refused, pickers hide it; `true` reactivates). Removal "
      "and whole-chart delete work only with no activity. "
      "Library-origin block types (`reporting_standard`) surface 501. "
      "`reporting_extension` / `custom_ontology` authoring may be "
      "disabled per environment (TAXONOMY_AUTHORING_ENABLED) — "
      "disabled surfaces 403. Closed months are immutable against "
      "curation: a mapping arc added or removed for an account with "
      "landed history in a closed month, or a `balance_type` / "
      "`period_type` / `trait` change on such an account, is refused "
      "(422, `protected_facts`) naming the months to reopen first."
    ),
    command=cmd_update_taxonomy_block,
    request_model=UpdateTaxonomyBlockRequest,
    result_type=TaxonomyBlockEnvelope,
    error_map={
      TaxonomyAuthoringDisabledError: 403,
      # Another update holds the taxonomy row; retryable.
      RowLockedError: 409,
      # Apply-side backstop; the validator reports a `protected_facts` issue first.
      ProtectedFactsError: 422,
      ValueError: 422,
      NotImplementedError: 501,
    },
    mark_stale_reason="taxonomy_block_updated",
  )
)

delete_taxonomy_block_op = _registrar.register(
  OperationSpec(
    name="delete-taxonomy-block",
    summary="Delete Taxonomy Block",
    description=(
      "Delete a taxonomy block and return a thin confirmation. "
      "`cascade_facts=True` also deletes Fact rows that reference the "
      "taxonomy's elements; default False fails the delete if such "
      "facts exist. Library-origin block types surface 501."
    ),
    command=cmd_delete_taxonomy_block,
    request_model=DeleteTaxonomyBlockRequest,
    result_type=DeleteTaxonomyBlockResponse,
    error_map={
      # A filed report's snapshot or a closed month's canonical sets would
      # go with the cascade; reopen or un-file first.
      ProtectedFactsError: 422,
      ValueError: 422,
      NotImplementedError: 501,
    },
    mark_stale_reason="taxonomy_block_deleted",
  )
)

link_entity_taxonomy_op = _registrar.register(
  OperationSpec(
    name="link-entity-taxonomy",
    summary="Link Entity to Taxonomy",
    description=(
      "Link the graph's entity to a taxonomy. Idempotent — returns "
      "existing linkage if it already exists. CoA blocks auto-link "
      "at create time; use this only to switch the primary CoA or "
      "link a reporting extension / custom ontology explicitly."
    ),
    command=cmd_link_entity_taxonomy,
    request_model=LinkEntityTaxonomyRequest,
    result_type=EntityTaxonomyResponse,
    error_map={
      EntityNotFoundError: 404,
      TaxonomyMissingError: 404,
      EntityTaxonomyConflictError: 409,
    },
    mark_stale_reason="entity_taxonomy_linked",
    requires_created_by=False,
  )
)


# ── Mapping ──────────────────────────────────────────────────────────────────
# `auto-map-elements` dispatches to the background worker and returns `pending`.

create_mapping_association_op = _registrar.register(
  OperationSpec(
    name="create-mapping-association",
    summary="Create Mapping Association",
    description=(
      "Link a chart-of-accounts element to a US GAAP reporting concept. "
      "One mapping edge per call — use `auto-map-elements` for bulk "
      "AI-assisted mapping. Duplicate (from, to, type) tuples return 409. "
      "The target must be a leaf concept: a subtotal the statement sums from "
      "its children is refused (422). "
      "Map before you close: an account with landed history in a closed "
      "month cannot be re-mapped (422) — the closed month's stamped "
      "statements were computed through the old arcs. Reopen latest-first "
      "down to the earliest month named, map, then close forward."
    ),
    command=cmd_create_mapping_association,
    request_model=CreateMappingAssociationOperation,
    result_type=AssociationResponse,
    error_map={
      MappingStructureNotFoundError: (404, lambda _e: "Mapping not found"),
      LibraryImmutableError: 403,
      ElementNotFoundError: (
        400,
        lambda e: f"{e.side.capitalize()} element not found",  # type: ignore[attr-defined]
      ),
      MappingAssociationExistsError: (
        409,
        lambda _e: "Mapping association already exists",
      ),
      MappingTargetIsRollupError: 422,
      ProtectedFactsError: 422,
    },
    mark_stale_reason="mapping_association_created",
  )
)


delete_mapping_association_op = _registrar.register(
  OperationSpec(
    name="delete-mapping-association",
    summary="Delete Mapping Association",
    description=(
      "Remove a single CoA → reporting-concept mapping edge by id. The "
      "mapping structure itself remains; only the association row is "
      "dropped. Use this to correct a wrong mapping — delete the bad edge, "
      "then `create-mapping-association` the right one. Find the "
      "association id via the `library_element_arcs` GraphQL field. "
      "Library-seeded rows cannot be deleted (403). An edge for an "
      "account with landed history in a closed month cannot be removed "
      "(422) until those months are reopened, latest-first."
    ),
    command=cmd_delete_mapping_association,
    request_model=DeleteMappingAssociationOperation,
    result_type=DeleteResult,
    requires_created_by=False,
    error_map={
      AssociationNotFoundError: (404, lambda _e: "Association not found"),
      LibraryImmutableError: 403,
      ProtectedFactsError: 422,
    },
    mark_stale_reason="mapping_association_deleted",
  )
)


class AutoMapElementsOperation(BaseModel):
  """Run the MappingOperator over a mapping structure (async).

  The MappingOperator walks every unmapped CoA element and proposes
  associations to reporting concepts. Confidence thresholds: ≥0.90
  auto-approved (association created), 0.70-0.89 flagged for review
  (created with `confidence` set; surface it in your UI), <0.70 skipped.
  Returns a `pending` envelope immediately; subscribe to the SSE stream
  for progress.
  """

  mapping_id: str = Field(..., description="The mapping structure to populate.")

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {"mapping_id": "map_01HVF8T0M2YTAY3BBNRH0V0"},
      ]
    },
  )


@router.post(
  "/auto-map-elements",
  response_model=OperationEnvelope,
  status_code=202,
  operation_id="autoMapElements",
  summary="Auto-Map Elements via AI",
  description="Dispatches to the background worker — returns a `pending` envelope immediately. Monitor via SSE at `/v1/operations/{operation_id}/stream`. Confidence thresholds: ≥0.90 auto-approved, 0.70–0.89 flagged for review, <0.70 skipped.",
  tags=[_OP_TAG],
  dependencies=[_RATE_LIMIT],
  responses={**OPERATION_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  "/extensions/roboledger/{graph_id}/operations/auto-map-elements",
  method="POST",
  business_event_type="ledger_auto_map_elements",
)
async def auto_map_elements_op(
  body: AutoMapElementsOperation,
  graph_id: str = Path(..., pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN),
  user: User = Depends(_require_roboledger_write),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
) -> OperationEnvelope:
  from robosystems.routers.graphs.operator.execute import (
    _check_operator_post_enabled,
  )
  from robosystems.worker.client import enqueue_task

  # The operator surface's kill switch covers this AI-spending dispatch too.
  _check_operator_post_enabled()

  op_name = "auto-map-elements"
  user_id = str(user.id)
  body_fingerprint = fingerprint_body(body)

  async with idempotent_dispatch(
    cache,
    user_id,
    graph_id,
    op_name,
    idempotency_key,
    body_fingerprint,
    event="extensions.operation",
  ) as idem:
    if idem.replay is not None:
      return idem.replay

    task_response = await enqueue_task(
      task_type="operator",
      graph_id=graph_id,
      user_id=user_id,
      params={"operator_type": "mapping", "mapping_id": body.mapping_id},
    )

    envelope = wrap_pending(
      op_name,
      operation_id=task_response["operation_id"],
      partial_result={
        "operation_type": task_response.get("operation_type"),
        "links": task_response.get("_links"),
        "deduplicated": task_response.get("deduplicated", False),
      },
      created_by=user_id,
    )

    await idem.record(envelope)

    log_operation_audit(
      operation_name=op_name,
      operation_id=envelope.operation_id,
      user_id=user_id,
      graph_id=graph_id,
      duration_ms=0.0,
      status="pending",
      idempotency_key=idempotency_key,
    )
    return envelope
