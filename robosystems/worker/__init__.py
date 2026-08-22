"""Background task worker for executing long-running operations.

Consumes tasks from a Valkey queue and executes them with progress
reporting via the SSE system and observability via Dagster.

Task registration happens via side-effect imports at module load time:
- Platform tasks: worker infrastructure (dagster_monitoring) and
  operations (graph creation/materialization/subgraph, operators)
- Adapter tasks: loaded via load_adapter_tasks() for future adapter extensions
"""

# Import task modules to trigger @register_task decorators.
# These are imported for side effects only (registration).
import robosystems.operations.graph.tasks as graph_tasks  # noqa: F401
import robosystems.operations.operators.adapters.worker_task as worker_task  # noqa: F401
import robosystems.operations.roboledger.tasks as roboledger_tasks  # noqa: F401
import robosystems.worker.tasks.dagster_monitoring as dagster_monitoring  # noqa: F401
from robosystems.worker.tasks import load_adapter_tasks

# Load adapter-contributed tasks (empty for now — extension point)
load_adapter_tasks()
