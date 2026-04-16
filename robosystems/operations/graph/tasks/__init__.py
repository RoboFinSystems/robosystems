"""Graph operation worker tasks.

Business logic tasks for graph creation, materialization, and subgraph
creation. Each task extends BaseTask from the worker infrastructure and
registers via @register_task for the consumer loop to dispatch.

This mirrors the agent adapter pattern (operations/agents/adapters/worker_task.py)
where business logic lives in operations/ and the worker provides infrastructure.
"""

# Side-effect imports: trigger @register_task decorators
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
