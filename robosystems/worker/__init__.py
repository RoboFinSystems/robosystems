"""Background task worker for executing long-running operations.

Consumes tasks from a Valkey queue and executes them with progress
reporting via the SSE system and observability via Dagster.
"""
