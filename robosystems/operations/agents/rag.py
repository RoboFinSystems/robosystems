"""
RAG (Retrieval-Augmented Generation) agent implementation.

Provides fast retrieval without AI for cost-effective responses.
"""

from typing import Dict, List, Optional, Any

from robosystems.operations.agents.base import (
  BaseAgent,
  AgentMode,
  AgentCapability,
  AgentMetadata,
  AgentResponse,
)
from robosystems.operations.agents.registry import AgentRegistry
from robosystems.operations.agents.context import ContextEnricher, RAGConfig
from robosystems.logger import logger


@AgentRegistry.register("rag")
class RAGAgent(BaseAgent):
  """
  Pure RAG agent for fast retrieval without AI.

  Provides template-based responses using retrieved context.
  """

  @property
  def metadata(self) -> AgentMetadata:
    """Return agent metadata."""
    return AgentMetadata(
      name="RAG Agent",
      description="Fast retrieval and template-based responses without AI",
      capabilities=[
        AgentCapability.RAG_SEARCH,
        AgentCapability.ENTITY_ANALYSIS,
      ],
      version="1.0.0",
      supported_modes=[
        AgentMode.QUICK,
        AgentMode.STANDARD,
      ],
      requires_credits=False,  # No AI usage
      author="RoboSystems",
      tags=["rag", "retrieval", "search", "cost-effective"],
    )

  def __init__(self, graph_id: str, user, db_session=None):
    """Initialize RAG agent."""
    super().__init__(graph_id, user, db_session)

    # Initialize context enricher
    config = RAGConfig(
      enable_semantic_search=True,
      enable_entity_linking=True,
      enable_pattern_matching=True,
      max_results=10,
    )
    self.context_enricher = ContextEnricher(graph_id, config)

  async def analyze(
    self,
    query: str,
    mode: AgentMode = AgentMode.STANDARD,
    history: Optional[List[Dict[str, Any]]] = None,
    context: Optional[Dict[str, Any]] = None,
    callback: Optional[Any] = None,
  ) -> AgentResponse:
    """
    Perform RAG-based analysis without AI.

    Args:
        query: The query to analyze
        mode: Execution mode
        history: Conversation history
        context: Additional context
        callback: Progress callback

    Returns:
        AgentResponse with retrieved information
    """
    try:
      # Validate mode (RAG only supports QUICK and STANDARD)
      if mode not in [AgentMode.QUICK, AgentMode.STANDARD]:
        mode = AgentMode.STANDARD

      # Initialize tools if needed
      if not self.kuzu_client:
        await self.initialize_tools()

      # Enrich context
      enriched_context = await self.context_enricher.enrich(query, context)

      # Perform retrieval
      if mode == AgentMode.QUICK:
        response_content = await self._quick_retrieval(query, enriched_context)
      else:
        response_content = await self._standard_retrieval(
          query, enriched_context, history
        )

      # No token usage since no AI
      return AgentResponse(
        content=response_content,
        agent_name=self.metadata.name,
        mode_used=mode,
        metadata={
          **enriched_context,
          "ai_used": False,
          "retrieval_based": True,
        },
        tokens_used={"input": 0, "output": 0},
        tools_called=["semantic_search", "template_generation"],
        confidence_score=self._calculate_retrieval_confidence(enriched_context),
      )

    except Exception as e:
      logger.error(f"RAG agent error: {str(e)}")
      return AgentResponse(
        content=f"Retrieval failed: {str(e)}",
        agent_name=self.metadata.name,
        mode_used=mode,
        error_details={
          "code": "RETRIEVAL_ERROR",
          "message": str(e),
        },
      )

  def can_handle(self, query: str, context: Optional[Dict[str, Any]] = None) -> float:
    """
    Calculate confidence for handling queries with RAG.

    Args:
        query: The query to evaluate
        context: Optional context

    Returns:
        Confidence score between 0 and 1
    """
    query_lower = query.lower()

    # RAG is good for factual queries
    factual_keywords = [
      "what is",
      "what are",
      "define",
      "explain",
      "describe",
      "show me",
      "find",
      "search",
      "retrieve",
      "get",
      "list",
      "information about",
      "details on",
      "data for",
    ]

    score = 0.5  # Base score for RAG fallback

    for keyword in factual_keywords:
      if keyword in query_lower:
        score += 0.1

    # Check if query is simple enough for RAG
    if len(query.split()) < 10:
      score += 0.1

    # Check context preferences
    if context:
      if context.get("prefer_no_ai"):
        score += 0.3
      if context.get("cost_sensitive"):
        score += 0.2

    return min(score, 1.0)

  async def _quick_retrieval(self, query: str, context: Dict[str, Any]) -> str:
    """Perform quick retrieval with minimal processing."""
    # Get relevant documents
    documents = context.get("relevant_documents", [])

    if not documents:
      # Try direct graph query
      results = await self._simple_graph_query(query)
      if results:
        return self._format_graph_results(results, query)
      return "No relevant information found. Please try a more specific query."

    # Format retrieved documents
    response = "Based on the available information:\n\n"

    for i, doc in enumerate(documents[:3]):  # Top 3 for quick mode
      response += f"{i + 1}. {doc.get('content', '')[:200]}...\n"
      if doc.get("metadata"):
        response += f"   Source: {doc['metadata'].get('source', 'Unknown')}\n"
      response += "\n"

    return response

  async def _standard_retrieval(
    self, query: str, context: Dict[str, Any], history: Optional[List[Dict]]
  ) -> str:
    """Perform standard retrieval with enhanced processing."""
    # Get all enriched information
    documents = context.get("relevant_documents", [])
    entities = context.get("linked_entities", [])
    patterns = context.get("historical_patterns", [])

    # Perform graph queries based on entities
    graph_results = []
    if entities and self.mcp_tools:
      for entity in entities[:3]:  # Limit queries
        try:
          result = await self._entity_query(entity)
          if result:
            graph_results.append(result)
        except Exception as e:
          logger.error(f"Entity query failed: {str(e)}")

    # Build comprehensive response
    response = self._build_template_response(
      query, documents, entities, patterns, graph_results, history
    )

    return response

  async def _simple_graph_query(self, query: str) -> Optional[List[Dict]]:
    """Execute a simple graph query based on the user query."""
    if not self.mcp_tools:
      return None

    try:
      # Build a simple Cypher query
      cypher = self._build_simple_cypher(query)

      result = await self.mcp_tools.call_tool(
        "read-graph-cypher",
        {"query": cypher, "parameters": {}},
      )

      return result if isinstance(result, list) else [result]

    except Exception as e:
      logger.error(f"Graph query failed: {str(e)}")
      return None

  async def _entity_query(self, entity: Dict[str, Any]) -> Optional[Dict]:
    """Query for specific entity information."""
    if not self.mcp_tools:
      return None

    try:
      entity_name = entity.get("entity", "")
      entity_type = entity.get("type", "")

      # Build entity-specific query with parameterization to prevent injection
      if entity_type == "ORG":
        cypher = """
                MATCH (e:Entity {name: $entity_name})
                RETURN e
                LIMIT 1
                """
        params = {"entity_name": entity_name}
      elif entity_type == "DATE":
        cypher = """
                MATCH (f:Fact)
                WHERE f.period CONTAINS $date_value
                RETURN f
                LIMIT 5
                """
        params = {"date_value": entity_name}
      else:
        return None

      result = await self.mcp_tools.call_tool(
        "read-graph-cypher",
        {"query": cypher, "parameters": params},
      )

      return {"entity": entity_name, "data": result}

    except Exception as e:
      logger.error(f"Entity query failed: {str(e)}")
      return None

  def _build_simple_cypher(self, query: str) -> str:
    """Build a simple Cypher query from user query."""
    query_lower = query.lower()

    # Pattern matching for query types
    if "companies" in query_lower or "entities" in query_lower:
      return "MATCH (e:Entity) RETURN e.name, e.type LIMIT 10"

    elif "facts" in query_lower or "data" in query_lower:
      return """
            MATCH (f:Fact)-[:HAS_ELEMENT]->(e:Element)
            RETURN f.value, f.period, e.name
            ORDER BY f.period DESC
            LIMIT 20
            """

    elif "relationships" in query_lower:
      return """
            MATCH (n)-[r]->(m)
            RETURN type(r) as relationship, count(*) as count
            ORDER BY count DESC
            LIMIT 10
            """

    else:
      # Default query
      return "MATCH (n) RETURN n LIMIT 10"

  def _format_graph_results(self, results: List[Dict], query: str) -> str:
    """Format graph query results."""
    if not results:
      return "No data found."

    response = f"Query Results for: {query}\n\n"

    for i, result in enumerate(results[:5]):
      response += f"Result {i + 1}:\n"

      if isinstance(result, dict):
        for key, value in result.items():
          if key != "embedding":  # Skip embeddings
            response += f"  {key}: {value}\n"
      else:
        response += f"  {result}\n"

      response += "\n"

    if len(results) > 5:
      response += f"... and {len(results) - 5} more results\n"

    return response

  def _build_template_response(
    self,
    query: str,
    documents: List[Dict],
    entities: List[Dict],
    patterns: List[Dict],
    graph_results: List[Dict],
    history: Optional[List[Dict]],
  ) -> str:
    """Build a template-based response from retrieved information."""
    sections = []

    # Header
    sections.append(f"Information Retrieved for: {query}\n")

    # Document section
    if documents:
      sections.append("📄 Relevant Documents:")
      for i, doc in enumerate(documents[:5]):
        content = doc.get("content", "")[:300]
        score = doc.get("score", 0)
        sections.append(f"{i + 1}. {content}...")
        sections.append(f"   Relevance: {score:.2f}")

    # Entity section
    if entities:
      sections.append("\n🏢 Identified Entities:")
      for entity in entities[:5]:
        sections.append(
          f"• {entity['entity']} ({entity['type']}) - "
          f"Confidence: {entity.get('confidence', 0):.2f}"
        )

    # Graph data section
    if graph_results:
      sections.append("\n📊 Graph Data:")
      for result in graph_results:
        if result and "entity" in result:
          sections.append(f"• {result['entity']}:")
          data = result.get("data", [])
          if data:
            sections.append(f"  Found {len(data)} related records")

    # Pattern section
    if patterns:
      sections.append("\n📈 Detected Patterns:")
      for pattern in patterns:
        sections.append(
          f"• {pattern.get('pattern', 'Unknown')}: "
          f"{pattern.get('description', 'No description')}"
        )

    # Context from history
    if history and len(history) > 0:
      sections.append("\n💭 Context: This follows up on previous questions")

    # Join sections
    response = "\n".join(sections)

    # Add summary
    response += "\n\n" + self._generate_summary(query, documents, entities)

    return response

  def _generate_summary(
    self,
    query: str,
    documents: List[Dict],
    entities: List[Dict],
  ) -> str:
    """Generate a summary without AI."""
    if not documents and not entities:
      return "💡 No specific information found. Try rephrasing your query or being more specific."

    summary = "💡 Summary: "

    # Simple template-based summary
    if "what" in query.lower():
      summary += "The information shows "
    elif "how" in query.lower():
      summary += "The process involves "
    elif "why" in query.lower():
      summary += "The reason is "
    else:
      summary += "Based on the retrieved information, "

    # Add entity context
    if entities:
      entity_names = [e["entity"] for e in entities[:2]]
      summary += f"related to {', '.join(entity_names)}, "

    # Add document summary
    if documents:
      summary += f"we found {len(documents)} relevant documents "
      if documents[0].get("score", 0) > 0.8:
        summary += "with high confidence."
      else:
        summary += "that may be relevant."
    else:
      summary += "but no direct documentation was found."

    return summary

  def _calculate_retrieval_confidence(self, context: Dict[str, Any]) -> float:
    """Calculate confidence based on retrieval quality."""
    confidence = 0.3  # Base confidence

    # Check document relevance
    documents = context.get("relevant_documents", [])
    if documents:
      # Average document score
      avg_score = sum(d.get("score", 0) for d in documents) / len(documents)
      confidence += avg_score * 0.4

    # Check entity extraction
    if context.get("linked_entities"):
      confidence += 0.2

    # Check patterns
    if context.get("historical_patterns"):
      confidence += 0.1

    return min(confidence, 1.0)
