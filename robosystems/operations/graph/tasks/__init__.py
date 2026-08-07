"""Worker tasks for graph creation, materialization, and subgraph creation.

Each task extends ``BaseTask`` and registers itself with ``@register_task`` so
the worker consumer loop can dispatch it. The business logic lives here in
``operations/``; the worker supplies only the infrastructure — the same split
as :mod:`robosystems.operations.operators.adapters.worker_task`.
"""

# Imported for side effects: importing each module runs its @register_task.
from robosystems.operations.graph.tasks import (
  extensions_materialize as extensions_materialize,
)
from robosystems.operations.graph.tasks import graph_creation as graph_creation
from robosystems.operations.graph.tasks import (
  graph_materialization as graph_materialization,
)
from robosystems.operations.graph.tasks import (
  graph_tier_upgrade as graph_tier_upgrade,
)
from robosystems.operations.graph.tasks import subgraph_creation as subgraph_creation


def get_worker_components() -> dict[str, list[str]]:
  """Return worker task types registered by this module.

  Follows the get_dagster_components() convention from adapter pipelines.
  Used for introspection — the actual registration happens via
  @register_task decorators on import.
  """
  return {
    "task_types": [
      "extensions_materialize",
      "graph_creation",
      "graph_materialization",
      "graph_tier_upgrade",
      "subgraph_creation",
    ],
  }
