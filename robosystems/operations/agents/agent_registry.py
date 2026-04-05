"""Agent registry for the unified Agent protocol.

Simple registration and lookup. Agents register via the @register_agent
decorator and are looked up by type string.
"""

from __future__ import annotations

from typing import Any

from robosystems.logger import logger
from robosystems.operations.agents.base import Agent

_AGENTS: dict[str, type[Agent]] = {}
_adapters_loaded = False


def register_agent(agent_type: str):
  """Decorator to register a new-style Agent.

  Example::

      @register_agent("cypher")
      class CypherAgent(Agent):
          spec = AgentSpec(...)
          async def run(self, ctx): ...
  """

  def decorator(cls: type[Agent]) -> type[Agent]:
    if agent_type in _AGENTS:
      logger.warning(
        f"Overriding existing agent registration '{agent_type}': "
        f"{_AGENTS[agent_type].__name__} -> {cls.__name__}"
      )
    _AGENTS[agent_type] = cls
    logger.info(f"Registered agent '{agent_type}': {cls.__name__}")
    return cls

  return decorator


def get_agent(agent_type: str) -> Agent:
  """Instantiate and return an agent by type.

  Raises:
      KeyError: If agent_type is not registered.
  """
  cls = _AGENTS.get(agent_type)
  if cls is None:
    registered = ", ".join(_AGENTS.keys()) or "(none)"
    raise KeyError(f"Agent '{agent_type}' not registered. Available: {registered}")
  return cls()


def get_agent_class(agent_type: str) -> type[Agent] | None:
  """Get the agent class without instantiation."""
  return _AGENTS.get(agent_type)


def list_agents() -> dict[str, dict[str, Any]]:
  """List all registered agents with their spec metadata."""
  return {
    agent_type: {
      "name": cls.spec.name,
      "description": cls.spec.description,
      "capabilities": [c.value for c in cls.spec.capabilities],
      "supported_modes": [m.value for m in cls.spec.supported_modes],
      "requires_credits": cls.spec.requires_credits,
      "version": cls.spec.version,
    }
    for agent_type, cls in _AGENTS.items()
  }


def is_registered(agent_type: str) -> bool:
  """Check if an agent type is registered."""
  return agent_type in _AGENTS


def load_adapter_agents() -> None:
  """Load agents from enabled adapters.

  Each adapter can expose a get_agent_components() function that
  returns {"agent_types": [...]} after importing its agent modules
  (which triggers @register_agent side effects).

  Called once from operations/agents/__init__.py.
  """
  global _adapters_loaded
  if _adapters_loaded:
    return
  _adapters_loaded = True

  # Future adapters register agents here:
  #
  # from robosystems.config import env
  #
  # if env.SEC_PIPELINE_ENABLED:
  #   from robosystems.adapters.sec.agents import get_agent_components
  #   get_agent_components()  # triggers @register_agent side effects


def clear_registry() -> None:
  """Clear all registrations. For testing only."""
  _AGENTS.clear()
