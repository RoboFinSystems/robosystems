"""
API models for AI Operator endpoints.

Defines request and response models for the multi-operator system.
"Operator" is the AI-executor concept (Claude/MCP-driven), distinct from
the REA ``Agent`` (counterparty) modeled in
``models/extensions/roboledger/agent.py``.
"""

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class OperatorMessage(BaseModel):
  """Message in conversation history."""

  role: str = Field(..., description="Message role (user/assistant)")
  content: str = Field(..., description="Message content")
  timestamp: datetime | None = Field(None, description="Message timestamp")


class OperatorMode(str, Enum):
  """Operator execution modes."""

  QUICK = "quick"
  STANDARD = "standard"
  EXTENDED = "extended"
  STREAMING = "streaming"


class SelectionCriteria(BaseModel):
  """Criteria for operator selection."""

  min_confidence: float = Field(0.3, description="Minimum confidence score")
  required_capabilities: list[str] = Field(
    default_factory=list, description="Required operator capabilities"
  )
  preferred_mode: OperatorMode | None = Field(
    None, description="Preferred execution mode"
  )
  max_response_time: float = Field(60.0, description="Maximum response time in seconds")
  excluded_operators: list[str] = Field(
    default_factory=list, description="Operators to exclude from selection"
  )


class OperatorRequest(BaseModel):
  """Request model for operator interactions."""

  message: str = Field(..., description="The query or message to process")
  history: list[OperatorMessage] = Field(
    default_factory=list, description="Conversation history"
  )
  context: dict[str, Any] | None = Field(
    None,
    description="Additional context for analysis (e.g., enable_rag, include_schema)",
  )
  mode: OperatorMode | None = Field(OperatorMode.STANDARD, description="Execution mode")
  operator_type: str | None = Field(
    None, description="Specific operator type to use (optional)"
  )
  selection_criteria: SelectionCriteria | None = Field(
    None, description="Criteria for operator selection"
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
          "description": "Basic query letting the system choose the best operator",
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
          "description": "Complex query with specific operator requirements and extended analysis",
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


class BatchOperatorRequest(BaseModel):
  """Request for batch processing multiple queries."""

  queries: list[OperatorRequest] = Field(
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


class OperatorResponse(BaseModel):
  """Response model for operator interactions."""

  content: str = Field(..., description="The operator's response content")
  operator_used: str = Field(
    ..., description="The operator type that handled the request"
  )
  mode_used: OperatorMode = Field(..., description="The execution mode used")
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
          "description": "Auto-selected financial operator with high confidence",
          "value": {
            "content": "Apple's Q4 2023 revenue was $89.5 billion, representing a 1% year-over-year decline. This was driven by strong iPhone sales offset by weakness in the Mac and iPad segments. Services revenue grew 16% to $22.3 billion, continuing its strong growth trajectory.",
            "operator_used": "financial",
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
          "description": "Research operator with comprehensive market analysis",
          "value": {
            "content": "The cloud computing market is dominated by three major players: AWS (32% market share), Microsoft Azure (23%), and Google Cloud (10%). Over the past year, growth rates have been: AWS 12%, Azure 27%, Google Cloud 28%. The market shows clear consolidation trends...",
            "operator_used": "research",
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


class BatchOperatorResponse(BaseModel):
  """Response for batch processing."""

  results: list[OperatorResponse] = Field(
    ..., description="List of operator responses (includes successes and failures)"
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
                "operator_used": "financial",
                "mode_used": "standard",
                "confidence_score": 0.92,
                "execution_time": 3.2,
                "timestamp": "2024-01-15T10:30:00Z",
              },
              {
                "content": "Microsoft's Q4 2023 revenue was $62.0 billion...",
                "operator_used": "financial",
                "mode_used": "standard",
                "confidence_score": 0.89,
                "execution_time": 3.5,
                "timestamp": "2024-01-15T10:30:03Z",
              },
              {
                "content": "Google's Q4 2023 revenue was $86.3 billion...",
                "operator_used": "financial",
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
                "operator_used": "financial",
                "mode_used": "extended",
                "confidence_score": 0.88,
                "execution_time": 12.3,
                "timestamp": "2024-01-15T10:35:12Z",
              },
              {
                "content": "",
                "operator_used": "financial",
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
                "operator_used": "research",
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


class OperatorListResponse(BaseModel):
  """Response for listing available operators."""

  operators: dict[str, dict[str, Any]] = Field(
    ..., description="Dictionary of available operators with metadata"
  )
  total: int = Field(..., description="Total number of operators")


class OperatorMetadataResponse(BaseModel):
  """Response for operator metadata."""

  name: str = Field(..., description="Operator name")
  description: str = Field(..., description="Operator description")
  version: str = Field(..., description="Operator version")
  capabilities: list[str] = Field(..., description="Operator capabilities")
  supported_modes: list[str] = Field(..., description="Supported execution modes")
  requires_credits: bool = Field(..., description="Whether operator requires credits")
  author: str | None = Field(None, description="Operator author")
  tags: list[str] = Field(default_factory=list, description="Operator tags")


class OperatorRecommendationRequest(BaseModel):
  """Request for operator recommendations."""

  query: str = Field(..., description="Query to analyze")
  context: dict[str, Any] | None = Field(None, description="Additional context")


class OperatorRecommendation(BaseModel):
  """Single operator recommendation."""

  operator_type: str = Field(..., description="Operator type identifier")
  operator_name: str = Field(..., description="Operator display name")
  confidence: float = Field(..., description="Confidence score (0-1)")
  capabilities: list[str] = Field(..., description="Operator capabilities")
  reason: str | None = Field(None, description="Reason for recommendation")


class OperatorRecommendationResponse(BaseModel):
  """Response for operator recommendations."""

  recommendations: list[OperatorRecommendation] = Field(
    ..., description="List of operator recommendations sorted by confidence"
  )
  query: str = Field(..., description="The analyzed query")


class OperatorHealthStatus(BaseModel):
  """Health status for a single operator."""

  operator_type: str = Field(..., description="Operator type identifier")
  status: str = Field(..., description="Health status (healthy/unhealthy/not_found)")
  name: str | None = Field(None, description="Operator name")
  version: str | None = Field(None, description="Operator version")
  error: str | None = Field(None, description="Error message if unhealthy")
  metadata: dict[str, Any] | None = Field(None, description="Additional metadata")


class OperatorHealthResponse(BaseModel):
  """Response for operator health check."""

  status: str = Field(..., description="Overall system status")
  operators: dict[str, OperatorHealthStatus] = Field(
    ..., description="Health status for each operator"
  )
  timestamp: datetime = Field(
    default_factory=datetime.utcnow, description="Health check timestamp"
  )


class OperatorMetricsResponse(BaseModel):
  """Response for operator metrics."""

  total_queries: int = Field(..., description="Total queries processed")
  operator_usage: dict[str, dict[str, Any]] = Field(
    ..., description="Usage statistics per operator"
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
