# Operator System

AI executors: Claude-via-MCP running against a graph, for natural-language querying, analysis, and autonomous background work.

## Naming: Operator is not Agent

**"Operator" is the AI-executor concept.** It is deliberately distinct from **`Agent`**, the REA counterparty model in `models/extensions/roboledger/agent.py` — a customer, vendor, or employee. `Agent` is the canonical ontology term and it is not available for the AI layer, so everything in the executor layer is named Operator: this package, `routers/graphs/operator/`, `CypherOperator`, `MappingOperator`, and the endpoint `/v1/graphs/{graph_id}/operator`.

This is the most confusable naming in the repo. When you read `Agent`, it is a business counterparty; when you read `Operator`, it is an AI executor.

## Architecture

Three layers, separating domain logic from execution infrastructure:

| Layer | Answers |
| ----- | ------- |
| `Operator` | What does this operator do? |
| `OperatorContext` | AI, tools, credits, progress — the services it needs |
| Adapters | How is it invoked? |

Operators are **stateless**. No `graph_id`, `user`, or `db_session` in `__init__` — everything arrives on the `OperatorContext` built by the adapter. That means an operator can be instantiated without any context for routing decisions via `operator.spec`, and the operator's own code is identical whether it runs in an API request or a background worker.

## Building an operator

```python
from robosystems.operations.operators.operator_registry import register_operator
from robosystems.operations.operators.base import (
    Operator, OperatorCapability, OperatorMode, OperatorResult, OperatorSpec,
)

@register_operator("my_operator")
class MyOperator(Operator):
    spec = OperatorSpec(
        name="My Operator",
        description="Does something useful",
        capabilities=[OperatorCapability.FINANCIAL_ANALYSIS],
        supported_modes=[OperatorMode.STANDARD],
        requires_credits=True,
        read_only=True,   # see below
    )

    async def run(self, ctx: OperatorContext) -> OperatorResult:
        # AI calls automatically track tokens and consume credits
        response = await ctx.ai.create_message(
            messages=[{"role": "user", "content": ctx.query}],
            system="You are a financial analyst.",
        )

        # MCP tools for graph queries, taxonomy operations, etc.
        schema = await ctx.tools.call_tool("get-graph-schema", {"graph_id": ctx.graph_id})

        # Progress reporting (SSE in worker, callback in API)
        await ctx.progress.report("Analysis complete", percent=100.0)

        return OperatorResult(content=response.content, tools_called=["get-graph-schema"])
```

Import the module in `implementations/__init__.py` so the decorator runs.

`OperatorSpec.read_only` defaults to **`False`** on purpose: an operator is treated as write-capable and gated on the graph write role unless it proves otherwise. An operator that gains a write tool later inherits the gate automatically; one that forgets the flag is over-restricted rather than under-protected. Set `True` only where the tool allowlist is provably read-only.

## Registered operators

| Operator | Context | Purpose |
| -------- | ------- | ------- |
| `cypher` | Worker, `run_operator_worker()` — queued by `POST /v1/graphs/{g}/operator[/cypher]` | Natural language → Cypher graph queries |
| `mapping` | Worker, `run_operator_worker()` — queued by `auto-map-elements` and the QuickBooks first sync | Autonomous CoA → US GAAP taxonomy mapping |

## Module layout

| Module | Contents |
| ------ | -------- |
| `base.py` | `Operator` ABC, `OperatorSpec`, `OperatorResult`, `OperatorCapability`, `OperatorMode`, `GraphScope` + `matches_graph_scope`, `enforce_operator_write_role`, and the legacy `BaseOperator` classes |
| `operator_context.py` | `OperatorContext` dataclass; `ToolAccess` / `ProgressReporter` protocols |
| `operator_registry.py` | `@register_operator`, `get_operator()`, `list_operators()`, adapter loading |
| `ai_client.py` / `tracked_ai.py` | Bedrock wrapper, and the tracking wrapper around it |
| `credit_consumer.py` | `SessionCreditConsumer` (API), `FactoryCreditConsumer` (worker) |
| `credit_preflight.py` | Pre-flight balance check before any Bedrock spend |
| `tool_access.py` | `HttpToolAccess` (MCP over HTTP), `DirectToolAccess` (in-process) |
| `tool_loop.py` | Bounded, model-driven tool-use loop shared by read/analysis operators |
| `progress.py` | `CallbackProgress` (API), `OperationManagerProgress` (worker SSE) |
| `orchestrator.py` | Operator routing and coordination |
| `adapters/api.py` | `run_operator_api()` — builds an in-process context (the orchestrator's `route_query` and tests; no endpoint runs an operator in the API process) |
| `adapters/worker.py` | `run_operator_worker()` — builds the context for worker tasks and returns the response envelope |
| `adapters/worker_task.py` | `@register_task("operator")` bridge into the worker consumer |
| `implementations/` | `cypher.py`, `mapping/` (operator, prompt, constants) |

## Execution paths

Every operator runs on the background worker. The API is a producer: it gates the request (lifecycle, repository limits, write role, graph scope, credits), enqueues, and answers 202 with the operation's stream/status/cancel links — or, under `?mode=sync`, waits up to 50s and answers 200 with the result.

```
POST /v1/graphs/{g}/operator[/{type}]   (or auto-map-elements, or the QB first sync)
  → gates → enqueue_task("operator", params={"operator_type", "query", "mode",
                                              "history", "context"})
  → Valkey queue → worker consumer (marks RUNNING) → OperatorWorkerTask
  → run_operator_worker() → operator.run(ctx) → response envelope
  Context: DirectToolAccess + FactoryCreditConsumer + OperationManagerProgress
```

The envelope (`content`, `operator_used`, `mode_used`, `metadata`, `tokens_used`, `confidence_score`, `execution_time`) is what `/status` returns under `result` and what the SSE `operation_completed` event carries; the operator's own metadata keys are also merged flat for the mapping operation's consumers. The worker re-runs the three gates with its own session before the first model call — a task can sit in the queue past a role change or a spent balance.

`run_operator_api()` still builds an in-process context (`HttpToolAccess` + `SessionCreditConsumer` + `CallbackProgress`) for the orchestrator's `route_query` and for tests; no endpoint reaches it.

**Pausing.** A worker task can stop at a checkpoint for a human decision (`BaseTask.pause_for_input` → status `awaiting_input` → `POST /v1/operations/{id}/resume`); see the worker README. No operator pauses yet — the primitive is there for the ones that will.

## Design decisions

**Credit tracking is unforgettable.** Every `ctx.ai.create_message()` goes through `TrackedAIClient`, which tracks tokens and consumes credits on the call path. `credit_preflight.py` runs before any spend and fails closed — an error resolving the balance denies the run, because credits are consumed only *after* Bedrock returns.

**Services are protocols.** `ToolAccess`, `ProgressReporter`, and `CreditConsumer` are protocols, so tests inject no-op implementations while API and worker contexts inject the real ones. The operator code doesn't change between them.

**The tool loop is model-driven.** `tool_loop.py` lets the model choose and call read-only MCP tools, feeds tool errors back as `is_error` tool results so it can self-correct, and stops at a bounded iteration count.
