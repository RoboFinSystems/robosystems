# Operator System

Unified operator architecture for AI-powered financial analysis, autonomous operations, and natural language interaction with graph databases.

## Architecture

Three-layer design separating domain logic from execution infrastructure:

```
Operator (domain logic)      "what does this operator do?"
OperatorContext (services)    "AI, tools, credits, progress"
Adapters (lifecycle)       "how is this operator invoked?"
```

Operators are stateless. They receive a context with services and return results. The execution context (API request vs background worker) determines how the context is constructed, not what the operator does.

> **Naming**: "Operator" is the AI-executor concept (Claude/MCP). Distinct from the REA `Agent` (counterparty model — customers, vendors, employees) in `models/extensions/roboledger/agent.py`. See [domain naming feedback memory](../../../../.claude/memory/feedback_domain_naming_beats_code_org.md) for the rationale.

## Building a New Operator

```python
from robosystems.operations.operators.operator_registry import register_operator
from robosystems.operations.operators.base import Operator, OperatorCapability, OperatorMode, OperatorResult, OperatorSpec

@register_operator("my_operator")
class MyOperator(Operator):
    spec = OperatorSpec(
        name="My Operator",
        description="Does something useful",
        capabilities=[OperatorCapability.FINANCIAL_ANALYSIS],
        supported_modes=[OperatorMode.STANDARD],
        requires_credits=True,
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

Then import in `implementations/__init__.py` to trigger registration.

## Registered Operators

| Operator | Type | Context | Purpose |
|---|---|---|---|
| `cypher` | API (sync/SSE) | `run_operator_api()` | Natural language → Cypher graph queries |
| `mapping` | Worker (async) | `run_operator_worker()` | Autonomous CoA → US GAAP taxonomy mapping |

## Module Layout

```
operators/
    base.py                 # Operator ABC, OperatorSpec, OperatorResult, OperatorMode, legacy classes
    operator_context.py        # OperatorContext dataclass, ToolAccess/ProgressReporter protocols
    operator_registry.py       # @register_operator decorator, get_operator(), list_operators()
    ai_client.py            # AIClient (Bedrock wrapper)
    tracked_ai.py           # TrackedAIClient (wraps AIClient + auto credit tracking)
    credit_consumer.py      # SessionCreditConsumer (API), FactoryCreditConsumer (worker)
    tool_access.py          # HttpToolAccess (MCP via HTTP), DirectToolAccess (in-process)
    progress.py             # CallbackProgress (API), OperationManagerProgress (worker SSE)
    orchestrator.py         # Operator routing and coordination

    adapters/
        api.py              # run_operator_api() — constructs context for API requests
        worker.py           # run_operator_worker() — constructs context for background tasks
        worker_task.py      # @register_task("operator") bridge to worker consumer

    implementations/
        cypher.py           # CypherOperator — graph query operator
        mapping/
            operator.py        # MappingOperator — taxonomy mapping operator
            prompt.py       # System prompt for mapping operations
```

## Execution Paths

**API path** (CypherOperator — sync/SSE responses):
```
Router → orchestrator.route_query() → run_operator_api() → operator.run(ctx) → OperatorResult
  Context: HttpToolAccess + SessionCreditConsumer + CallbackProgress
```

**Worker path** (MappingOperator — background tasks):
```
enqueue_task("operator", params={"operator_type": "mapping"})
  → Valkey queue → worker consumer → OperatorWorkerTask
  → run_operator_worker() → operator.run(ctx) → result dict
  Context: DirectToolAccess + FactoryCreditConsumer + OperationManagerProgress
```

## Key Design Decisions

**TrackedAIClient**: Every `ctx.ai.create_message()` call automatically tracks tokens and consumes credits. Operators cannot forget credit tracking — it's built into the call path.

**Protocol-based services**: `ToolAccess`, `ProgressReporter`, and `CreditConsumer` are protocols. Tests use `NoOp` implementations. API and worker contexts inject real implementations. Operator code is identical in both contexts.

**Stateless operators**: No `graph_id`, `user`, or `db_session` in `__init__`. All injected via `OperatorContext` by the adapter. Operators can be instantiated without any context for routing decisions via `operator.spec`.
