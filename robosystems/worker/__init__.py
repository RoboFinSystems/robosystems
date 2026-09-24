"""Background task worker: consumes a Valkey queue, reporting progress over
SSE. Task handlers register via the side-effect imports below.
"""

# Side-effect imports: @register_task.
import robosystems.operations.graph.tasks as graph_tasks  # noqa: F401
import robosystems.operations.operators.adapters.worker_task as worker_task  # noqa: F401
import robosystems.operations.roboledger.tasks as roboledger_tasks  # noqa: F401
import robosystems.worker.tasks.dagster_monitoring as dagster_monitoring  # noqa: F401
from robosystems.worker.tasks import load_adapter_tasks

# Load adapter-contributed tasks (empty for now — extension point)
load_adapter_tasks()
