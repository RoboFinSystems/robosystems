"""Background task worker for executing long-running operations.

Consumes tasks from a Valkey queue and executes them with progress
reporting via the SSE system and observability via Dagster.
"""

# Import task modules to trigger @register_task decorators.
# These are imported for side effects only (registration).
import robosystems.operations.agents.adapters.worker_task
import robosystems.worker.tasks.dagster_monitoring
import robosystems.worker.tasks.graph_creation
import robosystems.worker.tasks.graph_materialization
import robosystems.worker.tasks.subgraph_creation  # noqa: F401
