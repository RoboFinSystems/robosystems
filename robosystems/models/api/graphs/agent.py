"""
API models for agent endpoints.

Defines request and response models for the multiagent system.
"""

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class AgentMessage(BaseModel):
  """Message in conversation history."""

  role: str = Field(..., description="Message role (user/assistant)")
  content: str = Field(..., description="Message content")
  timestamp: datetime | None = Field(None, description="Message timestamp")


class AgentMode(str, Enum):
  """Agent execution modes."""

  QUICK = "quick"
  STANDARD = "standard"
  EXTENDED = "extended"
  STREAMING = "streaming"


class SelectionCriteria(BaseModel):
  """Criteria for agent selection."""

  min_confidence: float = Field(0.3, description="Minimum confidence score")
  required_capabilities: list[str] = Field(
    default_factory=list, description="Required agent capabilities"
  )
  preferred_mode: AgentMode | None = Field(None, description="Preferred execution mode")
  max_response_time: float = Field(60.0, description="Maximum response time in seconds")
  excluded_agents: list[str] = Field(
    default_factory=list, description="Agents to exclude from selection"
  )


class AgentRequest(BaseModel):
  """Request model for agent interactions."""

  message: str = Field(..., description="The query or message to process")
  history: list[AgentMessage] = Field(
    default_factory=list, description="Conversation history"
  )
  context: dict[str, Any] | None = Field(
    None,
    description="Additional context for analysis (e.g., enable_rag, include_schema)",
  )
  mode: AgentMode | None = Field(AgentMode.STANDARD, description="Execution mode")
  agent_type: str | None = Field(
    None, description="Specific agent type to use (optional)"
  )
  selection_criteria: SelectionCriteria | None = Field(
    None, description="Criteria for agent selection"
  )
  force_extended_analysis: bool = Field(
    False,
    description="Force extended analysis mode with comprehensive research",
  )
  enable_rag: bool = Field(True, description="Enable RAG context enrichment")
  stream: bool = Field(False, description="Enable streaming response")

  class Config:
    json_schema_extra = {
      "examples": [
        {
          "summary": "Simple query with auto-selection",
          "description": "Basic query letting the system choose the best agent",
          "value": {
            "message": "What was Apple's revenue in Q4 2023?",
            "enable_rag": True,
          },
        },
        {
          "summary": "Query with conversation history",
          "description": "Follow-up question using conversation context",
          "value": {
            "message": "How did that compare to the previous quarter?",
            "history": [
              {"role": "user", "content": "What was Apple's revenue in Q4 2023?"},
              {
                "role": "assistant",
                "content": "Apple's Q4 2023 revenue was $89.5 billion, representing a 1% year-over-year decline.",
              },
            ],
            "enable_rag": True,
          },
        },
        {
          "summary": "Advanced query with selection criteria",
          "description": "Complex query with specific agent requirements and extended analysis",
          "value": {
            "message": "Analyze the competitive landscape in cloud computing, focusing on market share and growth trends",
            "mode": "extended",
            "enable_rag": True,
            "force_extended_analysis": True,
            "context": {
              "industry": "technology",
              "focus_areas": ["market_share", "growth_trends"],
              "time_period": "2023-2024",
            },
            "selection_criteria": {
              "min_confidence": 0.7,
              "required_capabilities": ["financial_analysis", "market_research"],
              "preferred_mode": "extended",
              "max_response_time": 45.0,
            },
          },
        },
      ]
    }


class BatchAgentRequest(BaseModel):
  """Request for batch processing multiple queries."""

  queries: list[AgentRequest] = Field(
    ..., description="List of queries to process (max 10)"
  )
  parallel: bool = Field(False, description="Process queries in parallel")

  class Config:
    json_schema_extra = {
      "examples": [
        {
          "summary": "Sequential batch processing",
          "description": "Process multiple company queries sequentially",
          "value": {
            "queries": [
              {"message": "What was Apple's Q4 2023 revenue?", "enable_rag": True},
              {"message": "What was Microsoft's Q4 2023 revenue?", "enable_rag": True},
              {"message": "What was Google's Q4 2023 revenue?", "enable_rag": True},
            ],
            "parallel": False,
          },
        },
        {
          "summary": "Parallel batch processing",
          "description": "Process multiple queries in parallel for faster results",
          "value": {
            "queries": [
              {
                "message": "Analyze Tesla's financial performance",
                "mode": "extended",
                "enable_rag": True,
              },
              {
                "message": "Analyze Ford's financial performance",
                "mode": "extended",
                "enable_rag": True,
              },
              {
                "message": "Compare EV market trends",
                "mode": "standard",
                "enable_rag": True,
              },
            ],
            "parallel": True,
          },
        },
      ]
    }


class AgentResponse(BaseModel):
  """Response model for agent interactions."""

  content: str = Field(..., description="The agent's response content")
  agent_used: str = Field(..., description="The agent type that handled the request")
  mode_used: AgentMode = Field(..., description="The execution mode used")
  metadata: dict[str, Any] | None = Field(
    None, description="Response metadata including routing info"
  )
  tokens_used: dict[str, int] | None = Field(None, description="Token usage statistics")
  confidence_score: float | None = Field(
    None, description="Confidence score of the response (0.0-1.0 scale)"
  )
  operation_id: str | None = Field(None, description="Operation ID for SSE monitoring")
  is_partial: bool = Field(False, description="Whether this is a partial response")
  error_details: dict[str, Any] | None = Field(None, description="Error details if any")
  execution_time: float | None = Field(None, description="Execution time in seconds")
  timestamp: datetime = Field(
    default_factory=datetime.utcnow, description="Response timestamp"
  )

  class Config:
    json_schema_extra = {
      "examples": [
        {
          "summary": "Successful financial analysis",
          "description": "Auto-selected financial agent with high confidence",
          "value": {
            "content": "Apple's Q4 2023 revenue was $89.5 billion, representing a 1% year-over-year decline. This was driven by strong iPhone sales offset by weakness in the Mac and iPad segments. Services revenue grew 16% to $22.3 billion, continuing its strong growth trajectory.",
            "agent_used": "financial",
            "mode_used": "standard",
            "metadata": {
              "routing_info": {
                "candidates_evaluated": 3,
                "selection_reason": "Financial query with revenue and metrics keywords",
                "confidence_scores": {"financial": 0.92, "research": 0.45, "rag": 0.23},
              },
              "rag_enrichment": {
                "documents_retrieved": 5,
                "sources": ["Apple 10-Q Q4 2023", "Earnings Call Transcript Oct 2023"],
              },
            },
            "tokens_used": {
              "prompt_tokens": 450,
              "completion_tokens": 320,
              "total_tokens": 770,
            },
            "confidence_score": 0.92,
            "execution_time": 3.45,
            "timestamp": "2024-01-15T10:30:45Z",
          },
        },
        {
          "summary": "Research query with extended analysis",
          "description": "Research agent with comprehensive market analysis",
          "value": {
            "content": "The cloud computing market is dominated by three major players: AWS (32% market share), Microsoft Azure (23%), and Google Cloud (10%). Over the past year, growth rates have been: AWS 12%, Azure 27%, Google Cloud 28%. The market shows clear consolidation trends...",
            "agent_used": "research",
            "mode_used": "extended",
            "metadata": {
              "routing_info": {
                "candidates_evaluated": 3,
                "selection_reason": "Market research query requiring comprehensive analysis",
              },
              "analysis_depth": "comprehensive",
              "sources_consulted": 15,
            },
            "tokens_used": {
              "prompt_tokens": 1200,
              "completion_tokens": 850,
              "total_tokens": 2050,
            },
            "confidence_score": 0.88,
            "execution_time": 12.3,
            "timestamp": "2024-01-15T10:35:22Z",
          },
        },
      ]
    }


class BatchAgentResponse(BaseModel):
  """Response for batch processing."""

  results: list[AgentResponse] = Field(
    ..., description="List of agent responses (includes successes and failures)"
  )
  total_execution_time: float = Field(
    ..., description="Total execution time in seconds"
  )
  parallel_processed: bool = Field(
    ..., description="Whether queries were processed in parallel"
  )

  class Config:
    json_schema_extra = {
      "examples": [
        {
          "summary": "Successful sequential batch",
          "description": "All queries processed successfully in sequence",
          "value": {
            "results": [
              {
                "content": "Apple's Q4 2023 revenue was $89.5 billion...",
                "agent_used": "financial",
                "mode_used": "standard",
                "confidence_score": 0.92,
                "execution_time": 3.2,
                "timestamp": "2024-01-15T10:30:00Z",
              },
              {
                "content": "Microsoft's Q4 2023 revenue was $62.0 billion...",
                "agent_used": "financial",
                "mode_used": "standard",
                "confidence_score": 0.89,
                "execution_time": 3.5,
                "timestamp": "2024-01-15T10:30:03Z",
              },
              {
                "content": "Google's Q4 2023 revenue was $86.3 billion...",
                "agent_used": "financial",
                "mode_used": "standard",
                "confidence_score": 0.91,
                "execution_time": 3.1,
                "timestamp": "2024-01-15T10:30:07Z",
              },
            ],
            "total_execution_time": 9.8,
            "parallel_processed": False,
          },
        },
        {
          "summary": "Parallel batch with partial failure",
          "description": "Some queries succeed, one fails due to insufficient credits",
          "value": {
            "results": [
              {
                "content": "Tesla's financial performance shows strong revenue growth...",
                "agent_used": "financial",
                "mode_used": "extended",
                "confidence_score": 0.88,
                "execution_time": 12.3,
                "timestamp": "2024-01-15T10:35:12Z",
              },
              {
                "content": "",
                "agent_used": "financial",
                "mode_used": "extended",
                "error_details": {
                  "code": "insufficient_credits",
                  "message": "Insufficient credits for extended analysis",
                  "required_credits": 50,
                  "available_credits": 25,
                },
                "execution_time": 0.1,
                "timestamp": "2024-01-15T10:35:00Z",
              },
              {
                "content": "EV market trends show continued growth with 45% YoY increase...",
                "agent_used": "research",
                "mode_used": "standard",
                "confidence_score": 0.85,
                "execution_time": 8.7,
                "timestamp": "2024-01-15T10:35:09Z",
              },
            ],
            "total_execution_time": 12.5,
            "parallel_processed": True,
          },
        },
      ]
    }


class AgentListResponse(BaseModel):
  """Response for listing available agents."""

  agents: dict[str, dict[str, Any]] = Field(
    ..., description="Dictionary of available agents with metadata"
  )
  total: int = Field(..., description="Total number of agents")


class AgentMetadataResponse(BaseModel):
  """Response for agent metadata."""

  name: str = Field(..., description="Agent name")
  description: str = Field(..., description="Agent description")
  version: str = Field(..., description="Agent version")
  capabilities: list[str] = Field(..., description="Agent capabilities")
  supported_modes: list[str] = Field(..., description="Supported execution modes")
  requires_credits: bool = Field(..., description="Whether agent requires credits")
  author: str | None = Field(None, description="Agent author")
  tags: list[str] = Field(default_factory=list, description="Agent tags")


class AgentRecommendationRequest(BaseModel):
  """Request for agent recommendations."""

  query: str = Field(..., description="Query to analyze")
  context: dict[str, Any] | None = Field(None, description="Additional context")


class AgentRecommendation(BaseModel):
  """Single agent recommendation."""

  agent_type: str = Field(..., description="Agent type identifier")
  agent_name: str = Field(..., description="Agent display name")
  confidence: float = Field(..., description="Confidence score (0-1)")
  capabilities: list[str] = Field(..., description="Agent capabilities")
  reason: str | None = Field(None, description="Reason for recommendation")


class AgentRecommendationResponse(BaseModel):
  """Response for agent recommendations."""

  recommendations: list[AgentRecommendation] = Field(
    ..., description="List of agent recommendations sorted by confidence"
  )
  query: str = Field(..., description="The analyzed query")


class AgentHealthStatus(BaseModel):
  """Health status for a single agent."""

  agent_type: str = Field(..., description="Agent type identifier")
  status: str = Field(..., description="Health status (healthy/unhealthy/not_found)")
  name: str | None = Field(None, description="Agent name")
  version: str | None = Field(None, description="Agent version")
  error: str | None = Field(None, description="Error message if unhealthy")
  metadata: dict[str, Any] | None = Field(None, description="Additional metadata")


class AgentHealthResponse(BaseModel):
  """Response for agent health check."""

  status: str = Field(..., description="Overall system status")
  agents: dict[str, AgentHealthStatus] = Field(
    ..., description="Health status for each agent"
  )
  timestamp: datetime = Field(
    default_factory=datetime.utcnow, description="Health check timestamp"
  )


class AgentMetricsResponse(BaseModel):
  """Response for agent metrics."""

  total_queries: int = Field(..., description="Total queries processed")
  agent_usage: dict[str, dict[str, Any]] = Field(
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
