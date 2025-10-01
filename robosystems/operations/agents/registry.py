"""
Agent registry for discovery and lifecycle management.

Provides registration, discovery, and management of agent implementations.
"""

from typing import Dict, Type, Optional, List, Any
import importlib
from robosystems.operations.agents.base import (
  BaseAgent,
  AgentCapability,
  AgentMode,
)
from robosystems.models.iam import User
from robosystems.logger import logger


class AgentRegistrationError(Exception):
  """Raised when agent registration fails."""

  pass


class DuplicateAgentError(AgentRegistrationError):
  """Raised when attempting to register duplicate agent."""

  pass


class AgentNotFoundError(Exception):
  """Raised when agent is not found in registry."""

  pass


class AgentRegistry:
  """
  Singleton registry for managing agent implementations.

  Provides discovery, registration, and lifecycle management.
  """

  _instance = None
  _agents: Dict[str, Type[BaseAgent]] = {}
  _aliases: Dict[str, str] = {}
  _versions: Dict[str, Dict[str, Type[BaseAgent]]] = {}
  _dependencies: Dict[str, List[str]] = {}
  _lazy_imports: Dict[str, str] = {}

  def __new__(cls):
    """Ensure singleton pattern."""
    if cls._instance is None:
      cls._instance = super().__new__(cls)
    return cls._instance

  @classmethod
  def register(cls, agent_type: str, depends_on: Optional[List[str]] = None):
    """
    Decorator for registering agents.

    Args:
        agent_type: Unique identifier for the agent
        depends_on: Optional list of required agent dependencies

    Returns:
        Decorator function
    """

    def decorator(agent_class: Type[BaseAgent]):
      cls._register_agent(agent_type, agent_class, depends_on=depends_on)
      return agent_class

    return decorator

  @classmethod
  def _register_agent(
    cls,
    agent_type: str,
    agent_class: Type[BaseAgent],
    override: bool = False,
    version: Optional[str] = None,
    depends_on: Optional[List[str]] = None,
  ):
    """Internal method to register an agent."""
    # Validate agent class
    if not issubclass(agent_class, BaseAgent):
      raise AgentRegistrationError(
        f"Agent {agent_class.__name__} must inherit from BaseAgent"
      )

    # Check for duplicate
    if agent_type in cls._agents and not override:
      raise DuplicateAgentError(
        f"Agent type '{agent_type}' is already registered. Use override=True to replace."
      )

    # Register main entry
    cls._agents[agent_type] = agent_class

    # Register version if provided
    if version:
      if agent_type not in cls._versions:
        cls._versions[agent_type] = {}
      cls._versions[agent_type][version] = agent_class

    # Register dependencies
    if depends_on:
      cls._dependencies[agent_type] = depends_on

    logger.info(f"Registered agent '{agent_type}': {agent_class.__name__}")

  def register_agent(
    self,
    agent_type: str,
    agent_class: Type[BaseAgent],
    override: bool = False,
    version: Optional[str] = None,
  ):
    """
    Register an agent programmatically.

    Args:
        agent_type: Unique identifier for the agent
        agent_class: The agent class to register
        override: Whether to override existing registration
        version: Optional version string
    """
    self._register_agent(agent_type, agent_class, override, version)

  def unregister_agent(self, agent_type: str):
    """
    Unregister an agent.

    Args:
        agent_type: The agent type to unregister

    Raises:
        AgentNotFoundError: If agent not found
    """
    if agent_type not in self._agents:
      raise AgentNotFoundError(f"Agent type '{agent_type}' not found")

    del self._agents[agent_type]

    # Clean up aliases
    aliases_to_remove = [
      alias for alias, target in self._aliases.items() if target == agent_type
    ]
    for alias in aliases_to_remove:
      del self._aliases[alias]

    # Clean up versions
    if agent_type in self._versions:
      del self._versions[agent_type]

    # Clean up dependencies
    if agent_type in self._dependencies:
      del self._dependencies[agent_type]

    logger.info(f"Unregistered agent '{agent_type}'")

  def get_agent(
    self,
    agent_type: str,
    graph_id: str,
    user: User,
    db_session=None,
    version: Optional[str] = None,
  ) -> Optional[BaseAgent]:
    """
    Get an agent instance.

    Args:
        agent_type: The agent type to retrieve
        graph_id: Graph database identifier
        user: Authenticated user
        db_session: Optional database session
        version: Optional specific version

    Returns:
        Agent instance or None if not found
    """
    # Resolve alias if needed
    if agent_type in self._aliases:
      agent_type = self._aliases[agent_type]

    # Handle lazy loading
    if agent_type in self._lazy_imports:
      self._load_lazy_agent(agent_type)

    # Get agent class
    if version and agent_type in self._versions:
      agent_class = self._versions[agent_type].get(version)
    else:
      agent_class = self._agents.get(agent_type)

    if not agent_class:
      return None

    # Check dependencies
    if agent_type in self._dependencies:
      for dep in self._dependencies[agent_type]:
        if dep not in self._agents:
          raise AgentRegistrationError(
            f"Agent '{agent_type}' requires '{dep}' which is not registered"
          )

    # Create instance
    try:
      agent = agent_class(graph_id, user, db_session)
      return agent
    except Exception as e:
      logger.error(f"Failed to instantiate agent '{agent_type}': {str(e)}")
      raise AgentRegistrationError(f"Failed to create agent: {str(e)}")

  def get_all_agents(
    self, graph_id: str, user: User, db_session=None
  ) -> Dict[str, BaseAgent]:
    """
    Get all registered agents.

    Args:
        graph_id: Graph database identifier
        user: Authenticated user
        db_session: Optional database session

    Returns:
        Dictionary of agent instances
    """
    agents = {}

    for agent_type in self._agents:
      try:
        agent = self.get_agent(agent_type, graph_id, user, db_session)
        if agent:
          agents[agent_type] = agent
      except Exception as e:
        logger.warning(f"Could not instantiate agent '{agent_type}': {str(e)}")

    return agents

  def list_agents(self) -> Dict[str, Dict[str, Any]]:
    """
    List all available agents with metadata.

    Returns:
        Dictionary of agent metadata
    """
    agent_list = {}

    for agent_type, agent_class in self._agents.items():
      try:
        # Create temporary instance to get metadata
        temp_user = type("TempUser", (), {"id": "temp", "email": "temp@temp"})()
        temp_agent = agent_class("temp_graph", temp_user)
        metadata = temp_agent.metadata

        agent_list[agent_type] = {
          "name": metadata.name,
          "description": metadata.description,
          "version": metadata.version,
          "capabilities": [c.value for c in metadata.capabilities],
          "supported_modes": [m.value for m in metadata.supported_modes],
          "requires_credits": metadata.requires_credits,
        }
      except Exception as e:
        logger.warning(f"Could not get metadata for '{agent_type}': {str(e)}")
        agent_list[agent_type] = {
          "name": agent_type,
          "error": "Could not retrieve metadata",
        }

    return agent_list

  def get_agents_by_capability(
    self, capability: AgentCapability
  ) -> Dict[str, Type[BaseAgent]]:
    """
    Get agents that have a specific capability.

    Args:
        capability: The capability to filter by

    Returns:
        Dictionary of matching agent classes
    """
    matching = {}

    for agent_type, agent_class in self._agents.items():
      try:
        # Check if agent has capability
        temp_user = type("TempUser", (), {"id": "temp", "email": "temp@temp"})()
        temp_agent = agent_class("temp_graph", temp_user)

        if capability in temp_agent.metadata.capabilities:
          matching[agent_type] = agent_class
      except Exception:
        pass

    return matching

  def get_agents_by_mode(self, mode: AgentMode) -> Dict[str, Type[BaseAgent]]:
    """
    Get agents that support a specific mode.

    Args:
        mode: The mode to filter by

    Returns:
        Dictionary of matching agent classes
    """
    matching = {}

    for agent_type, agent_class in self._agents.items():
      try:
        temp_user = type("TempUser", (), {"id": "temp", "email": "temp@temp"})()
        temp_agent = agent_class("temp_graph", temp_user)

        if mode in temp_agent.metadata.supported_modes:
          matching[agent_type] = agent_class
      except Exception:
        pass

    return matching

  def discover_agent(
    self, query: str, graph_id: str, user: User, db_session=None
  ) -> Optional[BaseAgent]:
    """
    Discover the best agent for a query.

    Args:
        query: The query to analyze
        graph_id: Graph database identifier
        user: Authenticated user
        db_session: Optional database session

    Returns:
        Best matching agent or None
    """
    best_agent = None
    best_score = 0.0

    agents = self.get_all_agents(graph_id, user, db_session)

    for agent_type, agent in agents.items():
      try:
        score = agent.can_handle(query)
        if score > best_score:
          best_score = score
          best_agent = agent
      except Exception as e:
        logger.warning(f"Error evaluating agent '{agent_type}': {str(e)}")

    return best_agent

  def register_bulk(self, agents: Dict[str, Type[BaseAgent]]):
    """
    Register multiple agents at once.

    Args:
        agents: Dictionary mapping agent types to classes
    """
    for agent_type, agent_class in agents.items():
      self.register_agent(agent_type, agent_class)

  def clear(self):
    """Clear all agent registrations."""
    self._agents.clear()
    self._aliases.clear()
    self._versions.clear()
    self._dependencies.clear()
    self._lazy_imports.clear()
    logger.info("Cleared all agent registrations")

  def add_alias(self, alias: str, agent_type: str):
    """
    Add an alias for an agent type.

    Args:
        alias: The alias name
        agent_type: The actual agent type
    """
    if agent_type not in self._agents:
      raise AgentNotFoundError(f"Agent type '{agent_type}' not found")

    self._aliases[alias] = agent_type
    logger.info(f"Added alias '{alias}' for agent '{agent_type}'")

  def get_agent_metadata(self, agent_type: str) -> Optional[Dict[str, Any]]:
    """
    Get agent metadata without instantiation.

    Args:
        agent_type: The agent type

    Returns:
        Agent metadata dictionary or None
    """
    if agent_type in self._aliases:
      agent_type = self._aliases[agent_type]

    agent_class = self._agents.get(agent_type)
    if not agent_class:
      return None

    try:
      temp_user = type("TempUser", (), {"id": "temp", "email": "temp@temp"})()
      temp_agent = agent_class("temp_graph", temp_user)
      metadata = temp_agent.metadata

      return {
        "name": metadata.name,
        "description": metadata.description,
        "version": metadata.version,
        "capabilities": [c.value for c in metadata.capabilities],
        "supported_modes": [m.value for m in metadata.supported_modes],
        "requires_credits": metadata.requires_credits,
      }
    except Exception as e:
      logger.error(f"Failed to get metadata for '{agent_type}': {str(e)}")
      return None

  def is_registered(self, agent_type: str) -> bool:
    """
    Check if an agent type is registered.

    Args:
        agent_type: The agent type to check

    Returns:
        True if registered, False otherwise
    """
    if agent_type in self._aliases:
      agent_type = self._aliases[agent_type]
    return agent_type in self._agents

  def check_agent_health(
    self, agent_type: str, graph_id: str, user: User, db_session=None
  ) -> Dict[str, Any]:
    """
    Check health of a specific agent.

    Args:
        agent_type: The agent type to check
        graph_id: Graph database identifier
        user: Authenticated user
        db_session: Optional database session

    Returns:
        Health status dictionary
    """
    try:
      agent = self.get_agent(agent_type, graph_id, user, db_session)

      if not agent:
        return {
          "status": "not_found",
          "agent_type": agent_type,
          "error": "Agent not registered",
        }

      # Basic health check - can instantiate and has metadata
      metadata = agent.metadata

      return {
        "status": "healthy",
        "agent_type": agent_type,
        "name": metadata.name,
        "version": metadata.version,
        "capabilities": [c.value for c in metadata.capabilities],
        "metadata": {
          "requires_credits": metadata.requires_credits,
          "supported_modes": [m.value for m in metadata.supported_modes],
        },
      }

    except Exception as e:
      return {
        "status": "unhealthy",
        "agent_type": agent_type,
        "error": str(e),
      }

  def register_lazy(self, agent_type: str, import_path: str):
    """
    Register an agent for lazy loading.

    Args:
        agent_type: The agent type identifier
        import_path: Full import path to the agent class
    """
    self._lazy_imports[agent_type] = import_path
    logger.info(f"Registered lazy import for '{agent_type}': {import_path}")

  def _load_lazy_agent(self, agent_type: str):
    """Load a lazily registered agent."""
    if agent_type not in self._lazy_imports:
      return

    import_path = self._lazy_imports[agent_type]

    try:
      module_path, class_name = import_path.rsplit(".", 1)
      module = importlib.import_module(module_path)
      agent_class = getattr(module, class_name)

      self.register_agent(agent_type, agent_class)
      del self._lazy_imports[agent_type]

      logger.info(f"Lazy loaded agent '{agent_type}' from {import_path}")

    except Exception as e:
      logger.error(f"Failed to lazy load '{agent_type}' from {import_path}: {str(e)}")
      raise AgentRegistrationError(f"Failed to load agent: {str(e)}")
