"""Agent implementations.

Importing this module registers all agents in the agent_registry.
"""

from robosystems.operations.agents.implementations import cypher
from robosystems.operations.agents.implementations.mapping import agent as mapping

__all__ = ["cypher", "mapping"]
