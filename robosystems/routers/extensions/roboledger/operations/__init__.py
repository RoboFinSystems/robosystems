"""RoboLedger operation routes — `POST /extensions/roboledger/{graph_id}/operations/{op}`.

Every route follows the same pattern:

1. `POST /extensions/roboledger/{graph_id}/operations/{op_name}`
2. Typed request body (path params embedded in the body for
   update/delete commands)
3. A `_runner()` closure that opens an extensions session, calls the ops
   layer, and translates domain errors into HTTPExceptions
4. `execute_operation(ctx, runner, cache)` handles envelope + idempotency
   + audit

One module per OpenAPI tag, and the tag is the only thing that differs
between them — the shared plumbing is `.._common`. The surface was a single
3,200-line module until it reached 52 operations under one tag, which is
also what the published reference rendered: one undifferentiated list. The
split is therefore load-bearing twice over, in the source and on the page.

Order matters here. Within a tag, the reference lists operations in route
registration order, so the sequence below is the sequence a reader sees:

| Module                 | Tag                             |
| ---------------------- | ------------------------------- |
| `setup`                | RoboLedger: Setup               |
| `taxonomy`             | RoboLedger: Taxonomy & Mapping  |
| `information_blocks`   | RoboLedger: Information Blocks  |
| `ledger`               | RoboLedger: Ledger & Events     |
| `close`                | RoboLedger: Fiscal Close        |
| `reports`              | RoboLedger: Reports             |
| `distribution`         | RoboLedger: Report Distribution |

Two more routers serve the same URL prefix from outside this package and
share the eighth tag, `RoboLedger: Analytical Views`: `views.py` (graph-backed,
mounted on `FACT_GRID_ENABLED` so SEC-only deployments get it) and `reads.py`
(OLTP-backed). `main.py` mounts them after this router, so the views land
after the commands.

Raw ontology CRUD (taxonomies, structures, elements, non-mapping
associations) is deliberately not exposed: the Taxonomy Block envelope is
the only tenant-facing construction path.
"""

from __future__ import annotations

from fastapi import APIRouter

from robosystems.routers.extensions.roboledger.operations import (
  close,
  distribution,
  information_blocks,
  ledger,
  reports,
  setup,
  taxonomy,
)
from robosystems.routers.extensions.roboledger.operations.close import (
  BackfillPlanHistoryOperation as BackfillPlanHistoryOperation,
)
from robosystems.routers.extensions.roboledger.operations.close import (
  ClosePeriodOperation as ClosePeriodOperation,
)
from robosystems.routers.extensions.roboledger.operations.close import (
  ReopenPeriodOperation as ReopenPeriodOperation,
)
from robosystems.routers.extensions.roboledger.operations.close import (
  SetCloseTargetOperation as SetCloseTargetOperation,
)
from robosystems.routers.extensions.roboledger.operations.close import (
  backfill_plan_history_op as backfill_plan_history_op,
)
from robosystems.routers.extensions.roboledger.operations.close import (
  close_period_op as close_period_op,
)
from robosystems.routers.extensions.roboledger.operations.close import (
  promote_obligations_op as promote_obligations_op,
)
from robosystems.routers.extensions.roboledger.operations.close import (
  rebuild_schedule_op as rebuild_schedule_op,
)
from robosystems.routers.extensions.roboledger.operations.close import (
  reopen_period_op as reopen_period_op,
)
from robosystems.routers.extensions.roboledger.operations.close import (
  set_close_target_op as set_close_target_op,
)
from robosystems.routers.extensions.roboledger.operations.close import (
  terminate_schedule_op as terminate_schedule_op,
)
from robosystems.routers.extensions.roboledger.operations.distribution import (
  AddPublishListMembersOperation as AddPublishListMembersOperation,
)
from robosystems.routers.extensions.roboledger.operations.distribution import (
  BlockSourceGraphOperation as BlockSourceGraphOperation,
)
from robosystems.routers.extensions.roboledger.operations.distribution import (
  DeletePublishListOperation as DeletePublishListOperation,
)
from robosystems.routers.extensions.roboledger.operations.distribution import (
  RemovePublishListMemberOperation as RemovePublishListMemberOperation,
)
from robosystems.routers.extensions.roboledger.operations.distribution import (
  RevokeReportShareOperation as RevokeReportShareOperation,
)
from robosystems.routers.extensions.roboledger.operations.distribution import (
  ShareReportOperation as ShareReportOperation,
)
from robosystems.routers.extensions.roboledger.operations.distribution import (
  UnblockSourceGraphOperation as UnblockSourceGraphOperation,
)
from robosystems.routers.extensions.roboledger.operations.distribution import (
  UpdatePublishListOperation as UpdatePublishListOperation,
)
from robosystems.routers.extensions.roboledger.operations.distribution import (
  add_publish_list_members_op as add_publish_list_members_op,
)
from robosystems.routers.extensions.roboledger.operations.distribution import (
  block_source_graph_op as block_source_graph_op,
)
from robosystems.routers.extensions.roboledger.operations.distribution import (
  create_publish_list_op as create_publish_list_op,
)
from robosystems.routers.extensions.roboledger.operations.distribution import (
  delete_publish_list_op as delete_publish_list_op,
)
from robosystems.routers.extensions.roboledger.operations.distribution import (
  remove_publish_list_member_op as remove_publish_list_member_op,
)
from robosystems.routers.extensions.roboledger.operations.distribution import (
  revoke_report_share_op as revoke_report_share_op,
)
from robosystems.routers.extensions.roboledger.operations.distribution import (
  share_report_op as share_report_op,
)
from robosystems.routers.extensions.roboledger.operations.distribution import (
  unblock_source_graph_op as unblock_source_graph_op,
)
from robosystems.routers.extensions.roboledger.operations.distribution import (
  update_publish_list_op as update_publish_list_op,
)
from robosystems.routers.extensions.roboledger.operations.information_blocks import (
  assert_metrics_op as assert_metrics_op,
)
from robosystems.routers.extensions.roboledger.operations.information_blocks import (
  bind_text_block_op as bind_text_block_op,
)
from robosystems.routers.extensions.roboledger.operations.information_blocks import (
  compute_forecast_op as compute_forecast_op,
)
from robosystems.routers.extensions.roboledger.operations.information_blocks import (
  compute_metrics_op as compute_metrics_op,
)
from robosystems.routers.extensions.roboledger.operations.information_blocks import (
  create_information_block_op as create_information_block_op,
)
from robosystems.routers.extensions.roboledger.operations.information_blocks import (
  delete_information_block_op as delete_information_block_op,
)
from robosystems.routers.extensions.roboledger.operations.information_blocks import (
  evaluate_rules_op as evaluate_rules_op,
)
from robosystems.routers.extensions.roboledger.operations.information_blocks import (
  update_information_block_op as update_information_block_op,
)
from robosystems.routers.extensions.roboledger.operations.ledger import (
  create_agent_op as create_agent_op,
)
from robosystems.routers.extensions.roboledger.operations.ledger import (
  create_event_block_op as create_event_block_op,
)
from robosystems.routers.extensions.roboledger.operations.ledger import (
  create_event_handler_op as create_event_handler_op,
)
from robosystems.routers.extensions.roboledger.operations.ledger import (
  delete_journal_entry_op as delete_journal_entry_op,
)
from robosystems.routers.extensions.roboledger.operations.ledger import (
  execute_event_block_op as execute_event_block_op,
)
from robosystems.routers.extensions.roboledger.operations.ledger import (
  preview_event_block_op as preview_event_block_op,
)
from robosystems.routers.extensions.roboledger.operations.ledger import (
  preview_reconciling_item_op as preview_reconciling_item_op,
)
from robosystems.routers.extensions.roboledger.operations.ledger import (
  resolve_reconciling_item_op as resolve_reconciling_item_op,
)
from robosystems.routers.extensions.roboledger.operations.ledger import (
  update_agent_op as update_agent_op,
)
from robosystems.routers.extensions.roboledger.operations.ledger import (
  update_event_block_op as update_event_block_op,
)
from robosystems.routers.extensions.roboledger.operations.ledger import (
  update_event_handler_op as update_event_handler_op,
)
from robosystems.routers.extensions.roboledger.operations.ledger import (
  update_journal_entry_op as update_journal_entry_op,
)
from robosystems.routers.extensions.roboledger.operations.reports import (
  DeleteReportOperation as DeleteReportOperation,
)
from robosystems.routers.extensions.roboledger.operations.reports import (
  RegenerateReportOperation as RegenerateReportOperation,
)
from robosystems.routers.extensions.roboledger.operations.reports import (
  create_report_op as create_report_op,
)
from robosystems.routers.extensions.roboledger.operations.reports import (
  delete_report_op as delete_report_op,
)
from robosystems.routers.extensions.roboledger.operations.reports import (
  file_report_op as file_report_op,
)
from robosystems.routers.extensions.roboledger.operations.reports import (
  regenerate_report_op as regenerate_report_op,
)
from robosystems.routers.extensions.roboledger.operations.reports import (
  transition_filing_status_op as transition_filing_status_op,
)

# Handlers and request bodies are re-exported so the surface can still be
# reached as `...roboledger.operations.<name>`, the path it had before the
# split. Tests and tool adapters bind to these names.
from robosystems.routers.extensions.roboledger.operations.setup import (
  change_reporting_style_op as change_reporting_style_op,
)
from robosystems.routers.extensions.roboledger.operations.setup import (
  initialize_chart_of_accounts_op as initialize_chart_of_accounts_op,
)
from robosystems.routers.extensions.roboledger.operations.setup import (
  initialize_op as initialize_op,
)
from robosystems.routers.extensions.roboledger.operations.setup import (
  update_entity_op as update_entity_op,
)
from robosystems.routers.extensions.roboledger.operations.taxonomy import (
  AutoMapElementsOperation as AutoMapElementsOperation,
)
from robosystems.routers.extensions.roboledger.operations.taxonomy import (
  auto_map_elements_op as auto_map_elements_op,
)
from robosystems.routers.extensions.roboledger.operations.taxonomy import (
  create_mapping_association_op as create_mapping_association_op,
)
from robosystems.routers.extensions.roboledger.operations.taxonomy import (
  create_taxonomy_block_op as create_taxonomy_block_op,
)
from robosystems.routers.extensions.roboledger.operations.taxonomy import (
  delete_mapping_association_op as delete_mapping_association_op,
)
from robosystems.routers.extensions.roboledger.operations.taxonomy import (
  delete_taxonomy_block_op as delete_taxonomy_block_op,
)
from robosystems.routers.extensions.roboledger.operations.taxonomy import (
  link_entity_taxonomy_op as link_entity_taxonomy_op,
)
from robosystems.routers.extensions.roboledger.operations.taxonomy import (
  update_taxonomy_block_op as update_taxonomy_block_op,
)

router = APIRouter()

# Order is the reference's reading order within each tag: operations are
# grouped by tag, and within a tag by the order they were registered.
_MODULES = (
  setup,
  taxonomy,
  information_blocks,
  ledger,
  close,
  reports,
  distribution,
)

# Routes are copied onto this router rather than composed with
# `include_router`. As of FastAPI 0.113 an included router is kept as an
# `_IncludedRouter` marker and flattened only when the app mounts it, which
# would leave `router.routes` holding seven markers and no operations. Two
# structural tests walk exactly that list — every hand-written write op
# carries the write-role gate, every op that writes materialized content
# marks its graph stale — and both would have gone quietly vacuous. Copying
# is equivalent here because nothing is layered on at this level: no prefix,
# no tags, no dependencies. Add any of those and use `include_router`
# instead, then give those tests a way to see through it.
for _module in _MODULES:
  router.routes.extend(_module.router.routes)
