"""
API models for agent endpoints.

Defines request and response models for the multiagent system.
"""

from pydantic import BaseModel, Field
from typing import List, Dict, Optional, Any
from datetime import datetime
from enum import Enum


class AgentMessage(BaseModel):
  """Message in conversation history."""

  role: str = Field(..., description="Message role (user/assistant)")
  content: str = Field(..., description="Message content")
  timestamp: Optional[datetime] = Field(None, description="Message timestamp")


class AgentMode(str, Enum):
  """Agent execution modes."""

  QUICK = "quick"
  STANDARD = "standard"
  EXTENDED = "extended"
  STREAMING = "streaming"


class SelectionCriteria(BaseModel):
  """Criteria for agent selection."""

  min_confidence: float = Field(0.3, description="Minimum confidence score")
  required_capabilities: List[str] = Field(
    default_factory=list, description="Required agent capabilities"
  )
  preferred_mode: Optional[AgentMode] = Field(
    None, description="Preferred execution mode"
  )
  max_response_time: float = Field(60.0, description="Maximum response time in seconds")
  excluded_agents: List[str] = Field(
    default_factory=list, description="Agents to exclude from selection"
  )


class AgentRequest(BaseModel):
  """Request model for agent interactions."""

  message: str = Field(..., description="The query or message to process")
  history: List[AgentMessage] = Field(
    default_factory=list, description="Conversation history"
  )
  context: Optional[Dict[str, Any]] = Field(
    None,
    description="Additional context for analysis (e.g., enable_rag, include_schema)",
  )
  mode: Optional[AgentMode] = Field(AgentMode.STANDARD, description="Execution mode")
  agent_type: Optional[str] = Field(
    None, description="Specific agent type to use (optional)"
  )
  selection_criteria: Optional[SelectionCriteria] = Field(
    None, description="Criteria for agent selection"
  )
  force_extended_analysis: bool = Field(
    False,
    description="Force extended analysis mode with comprehensive research",
  )
  enable_rag: bool = Field(True, description="Enable RAG context enrichment")
  stream: bool = Field(False, description="Enable streaming response")


class BatchAgentRequest(BaseModel):
  """Request for batch processing multiple queries."""

  queries: List[AgentRequest] = Field(..., description="List of queries to process")
  parallel: bool = Field(False, description="Process queries in parallel")


class AgentResponse(BaseModel):
  """Response model for agent interactions."""

  content: str = Field(..., description="The agent's response content")
  agent_used: str = Field(..., description="The agent type that handled the request")
  mode_used: AgentMode = Field(..., description="The execution mode used")
  metadata: Optional[Dict[str, Any]] = Field(
    None, description="Response metadata including routing info"
  )
  tokens_used: Optional[Dict[str, int]] = Field(
    None, description="Token usage statistics"
  )
  confidence_score: Optional[float] = Field(
    None, description="Confidence score of the response"
  )
  operation_id: Optional[str] = Field(
    None, description="Operation ID for SSE monitoring"
  )
  is_partial: bool = Field(False, description="Whether this is a partial response")
  error_details: Optional[Dict[str, Any]] = Field(
    None, description="Error details if any"
  )
  execution_time: Optional[float] = Field(None, description="Execution time in seconds")
  timestamp: datetime = Field(
    default_factory=datetime.utcnow, description="Response timestamp"
  )


class BatchAgentResponse(BaseModel):
  """Response for batch processing."""

  results: List[AgentResponse] = Field(..., description="List of agent responses")
  total_execution_time: float = Field(..., description="Total execution time")
  parallel_processed: bool = Field(
    ..., description="Whether queries were processed in parallel"
  )


class AgentListResponse(BaseModel):
  """Response for listing available agents."""

  agents: Dict[str, Dict[str, Any]] = Field(
    ..., description="Dictionary of available agents with metadata"
  )
  total: int = Field(..., description="Total number of agents")


class AgentMetadataResponse(BaseModel):
  """Response for agent metadata."""

  name: str = Field(..., description="Agent name")
  description: str = Field(..., description="Agent description")
  version: str = Field(..., description="Agent version")
  capabilities: List[str] = Field(..., description="Agent capabilities")
  supported_modes: List[str] = Field(..., description="Supported execution modes")
  requires_credits: bool = Field(..., description="Whether agent requires credits")
  author: Optional[str] = Field(None, description="Agent author")
  tags: List[str] = Field(default_factory=list, description="Agent tags")


class AgentRecommendationRequest(BaseModel):
  """Request for agent recommendations."""

  query: str = Field(..., description="Query to analyze")
  context: Optional[Dict[str, Any]] = Field(None, description="Additional context")


class AgentRecommendation(BaseModel):
  """Single agent recommendation."""

  agent_type: str = Field(..., description="Agent type identifier")
  agent_name: str = Field(..., description="Agent display name")
  confidence: float = Field(..., description="Confidence score (0-1)")
  capabilities: List[str] = Field(..., description="Agent capabilities")
  reason: Optional[str] = Field(None, description="Reason for recommendation")


class AgentRecommendationResponse(BaseModel):
  """Response for agent recommendations."""

  recommendations: List[AgentRecommendation] = Field(
    ..., description="List of agent recommendations sorted by confidence"
  )
  query: str = Field(..., description="The analyzed query")


class AgentHealthStatus(BaseModel):
  """Health status for a single agent."""

  agent_type: str = Field(..., description="Agent type identifier")
  status: str = Field(..., description="Health status (healthy/unhealthy/not_found)")
  name: Optional[str] = Field(None, description="Agent name")
  version: Optional[str] = Field(None, description="Agent version")
  error: Optional[str] = Field(None, description="Error message if unhealthy")
  metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")


class AgentHealthResponse(BaseModel):
  """Response for agent health check."""

  status: str = Field(..., description="Overall system status")
  agents: Dict[str, AgentHealthStatus] = Field(
    ..., description="Health status for each agent"
  )
  timestamp: datetime = Field(
    default_factory=datetime.utcnow, description="Health check timestamp"
  )


class AgentMetricsResponse(BaseModel):
  """Response for agent metrics."""

  total_queries: int = Field(..., description="Total queries processed")
  agent_usage: Dict[str, Dict[str, Any]] = Field(
    ..., description="Usage statistics per agent"
  )
  average_response_time: float = Field(
    ..., description="Average response time in seconds"
  )
  cache_hits: int = Field(0, description="Number of cache hits")
  cache_misses: int = Field(0, description="Number of cache misses")
  errors: int = Field(0, description="Number of errors")
  timestamp: datetime = Field(
    default_factory=datetime.utcnow, description="Metrics timestamp"
  )
