# Agent System

Unified agent architecture for AI-powered financial analysis, autonomous operations, and natural language interaction with graph databases.

## Architecture

Three-layer design separating domain logic from execution infrastructure:

```
Agent (domain logic)      "what does this agent do?"
AgentContext (services)    "AI, tools, credits, progress"
Adapters (lifecycle)       "how is this agent invoked?"
```

Agents are stateless. They receive a context with services and return results. The execution context (API request vs background worker) determines how the context is constructed, not what the agent does.

## Building a New Agent

```python
from robosystems.operations.agents.agent_registry import register_agent
from robosystems.operations.agents.base import Agent, AgentCapability, AgentMode, AgentResult, AgentSpec

@register_agent("my_agent")
class MyAgent(Agent):
    spec = AgentSpec(
        name="My Agent",
        description="Does something useful",
        capabilities=[AgentCapability.FINANCIAL_ANALYSIS],
        supported_modes=[AgentMode.STANDARD],
        requires_credits=True,
    )

    async def run(self, ctx: AgentContext) -> AgentResult:
        # AI calls automatically track tokens and consume credits
        response = await ctx.ai.create_message(
            messages=[{"role": "user", "content": ctx.query}],
            system="You are a financial analyst.",
        )

        # MCP tools for graph queries, taxonomy operations, etc.
        schema = await ctx.tools.call_tool("get-graph-schema", {"graph_id": ctx.graph_id})

        # Progress reporting (SSE in worker, callback in API)
        await ctx.progress.report("Analysis complete", percent=100.0)

        return AgentResult(content=response.content, tools_called=["get-graph-schema"])
```

Then import in `implementations/__init__.py` to trigger registration.

## Registered Agents

| Agent | Type | Context | Purpose |
|---|---|---|---|
| `cypher` | API (sync/SSE) | `run_agent_api()` | Natural language → Cypher graph queries |
| `mapping` | Worker (async) | `run_agent_worker()` | Autonomous CoA → US GAAP taxonomy mapping |

## Module Layout

```
agents/
    base.py                 # Agent ABC, AgentSpec, AgentResult, AgentMode, legacy classes
    agent_context.py        # AgentContext dataclass, ToolAccess/ProgressReporter protocols
    agent_registry.py       # @register_agent decorator, get_agent(), list_agents()
    ai_client.py            # AIClient (Bedrock wrapper)
    tracked_ai.py           # TrackedAIClient (wraps AIClient + auto credit tracking)
    credit_consumer.py      # SessionCreditConsumer (API), FactoryCreditConsumer (worker)
    tool_access.py          # HttpToolAccess (MCP via HTTP), DirectToolAccess (in-process)
    progress.py             # CallbackProgress (API), OperationManagerProgress (worker SSE)
    orchestrator.py         # Agent routing and coordination

    adapters/
        api.py              # run_agent_api() — constructs context for API requests
        worker.py           # run_agent_worker() — constructs context for background tasks
        worker_task.py      # @register_task("agent") bridge to worker consumer

    implementations/
        cypher.py           # CypherAgent — graph query agent
        mapping/
            agent.py        # MappingAgent — taxonomy mapping agent
            prompt.py       # System prompt for mapping operations
```

## Execution Paths

**API path** (CypherAgent — sync/SSE responses):
```
Router → orchestrator.route_query() → run_agent_api() → agent.run(ctx) → AgentResult
  Context: HttpToolAccess + SessionCreditConsumer + CallbackProgress
```

**Worker path** (MappingAgent — background tasks):
```
enqueue_task("agent", params={"agent_type": "mapping"})
  → Valkey queue → worker consumer → AgentWorkerTask
  → run_agent_worker() → agent.run(ctx) → result dict
  Context: DirectToolAccess + FactoryCreditConsumer + OperationManagerProgress
```

## Key Design Decisions

**TrackedAIClient**: Every `ctx.ai.create_message()` call automatically tracks tokens and consumes credits. Agents cannot forget credit tracking — it's built into the call path.

**Protocol-based services**: `ToolAccess`, `ProgressReporter`, and `CreditConsumer` are protocols. Tests use `NoOp` implementations. API and worker contexts inject real implementations. Agent code is identical in both contexts.

**Stateless agents**: No `graph_id`, `user`, or `db_session` in `__init__`. All injected via `AgentContext` by the adapter. Agents can be instantiated without any context for routing decisions via `agent.spec`.
