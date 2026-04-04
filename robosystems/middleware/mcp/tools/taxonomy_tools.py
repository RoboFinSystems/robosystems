"""Taxonomy mapping MCP tools for CoA → GAAP mapping workflows.

Four tools for taxonomy management:
1. get-unmapped-elements: List CoA elements not yet mapped to reporting taxonomy
2. suggest-mapping: Query reporting taxonomy for matching concepts by classification
3. create-mapping-association: Write a confirmed CoA → reporting concept mapping
4. get-mapping-summary: Get mapping coverage stats
"""

from typing import Any

from robosystems.logger import logger


class GetUnmappedElementsTool:
  """List CoA elements not yet mapped to the reporting taxonomy."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "get-unmapped-elements",
      "description": """List Chart of Accounts elements that are not yet mapped to US GAAP reporting concepts.

**WHEN TO USE:**
- At the start of a CoA → GAAP mapping workflow
- To check which accounts still need mapping
- To track progress during an iterative mapping session

**RETURNS:**
- List of unmapped CoA elements with id, code, name, classification, balance_type
- Classification (asset/liability/equity/revenue/expense) helps narrow the target reporting concept

**WORKFLOW:**
1. Call this tool to see unmapped elements
2. For each element, use suggest-mapping to find matching reporting concepts
3. Use create-mapping-association to write confirmed mappings
4. Call get-mapping-summary to check overall coverage""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "mapping_id": {
            "type": "string",
            "description": "Optional: specific mapping structure ID to check against. If omitted, checks all mapping structures.",
          },
        },
        "required": [],
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    from robosystems.db.extensions import extensions_session
    from robosystems.models.extensions import Association, Element

    graph_id = self.client.graph_id
    mapping_id = arguments.get("mapping_id")

    try:
      with extensions_session(graph_id) as session:
        from sqlalchemy import select

        coa_sources = ("quickbooks", "xero", "plaid", "native", "import")
        coa_elements = (
          session.execute(
            select(Element).where(
              Element.source.in_(coa_sources),
              Element.is_active.is_(True),
              Element.is_abstract.is_(False),
            )
          )
          .scalars()
          .all()
        )

        if mapping_id:
          mapped_query = select(Association.from_element_id).where(
            Association.structure_id == mapping_id,
            Association.association_type == "mapping",
          )
        else:
          mapped_query = select(Association.from_element_id).where(
            Association.association_type == "mapping",
          )
        mapped_ids = set(session.execute(mapped_query).scalars().all())

        unmapped = [e for e in coa_elements if e.id not in mapped_ids]

        return {
          "unmapped_count": len(unmapped),
          "total_coa_elements": len(coa_elements),
          "elements": [
            {
              "id": e.id,
              "code": e.code,
              "name": e.name,
              "classification": e.classification,
              "balance_type": e.balance_type,
              "external_source": e.external_source,
            }
            for e in unmapped
          ],
        }
    except Exception as exc:
      logger.warning(f"get-unmapped-elements failed: {exc}")
      return {"error": str(exc)}


class SuggestMappingTool:
  """Query reporting taxonomy for matching concepts by classification."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "suggest-mapping",
      "description": """Find matching US GAAP reporting concepts for a CoA element.

**WHEN TO USE:**
- When deciding which reporting concept a CoA account should map to
- Narrows by classification (asset→Assets subtree, revenue→Revenues subtree, etc.)

**HOW IT WORKS:**
- Reads the shared US GAAP reporting taxonomy (SFAC 6 → us-gaap hierarchy)
- Filters by the same classification as the source element
- Returns candidate concepts with qname, name, and hierarchy depth

**TIPS:**
- The classification filter (asset/liability/equity/revenue/expense) narrows candidates by ~80%
- Depth 1 = high-level line items (Revenue, Cost of Revenue, Operating Expenses)
- Depth 2 = detail items (R&D, SGA, Depreciation)
- Abstract elements are grouping nodes — map to non-abstract concepts""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "element_id": {
            "type": "string",
            "description": "The CoA element ID to find mapping candidates for",
          },
          "classification": {
            "type": "string",
            "description": "Override classification filter (asset, liability, equity, revenue, expense). Defaults to the element's own classification.",
          },
        },
        "required": ["element_id"],
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    from robosystems.db.extensions import extensions_session
    from robosystems.models.extensions import Element

    graph_id = self.client.graph_id
    element_id = arguments["element_id"]
    classification_override = arguments.get("classification")

    try:
      with extensions_session(graph_id) as session:
        from sqlalchemy import select

        # Get the source element
        source = session.execute(
          select(Element).where(Element.id == element_id)
        ).scalar_one_or_none()
        if not source:
          return {"error": f"Element {element_id} not found"}

        classification = classification_override or source.classification

        # Map classification to SFAC 6 categories
        cls_map = {
          "asset": ["asset"],
          "liability": ["liability"],
          "equity": ["equity"],
          "revenue": ["revenue"],
          "expense": ["expense"],
        }
        target_classifications = cls_map.get(classification, [classification])

        # Get reporting taxonomy candidates
        candidates = (
          session.execute(
            select(Element)
            .where(
              Element.source.in_(("us-gaap", "sfac6")),
              Element.classification.in_(target_classifications),
              Element.is_active.is_(True),
            )
            .order_by(Element.depth, Element.name)
          )
          .scalars()
          .all()
        )

        return {
          "source_element": {
            "id": source.id,
            "code": source.code,
            "name": source.name,
            "classification": source.classification,
          },
          "candidates": [
            {
              "id": c.id,
              "qname": c.qname,
              "name": c.name,
              "classification": c.classification,
              "is_abstract": c.is_abstract,
              "depth": c.depth,
              "source": c.source,
            }
            for c in candidates
          ],
        }
    except Exception as exc:
      logger.warning(f"suggest-mapping failed: {exc}")
      return {"error": str(exc)}


class CreateMappingAssociationTool:
  """Write a confirmed CoA → reporting concept mapping."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "create-mapping-association",
      "description": """Create a mapping association between a CoA element and a US GAAP reporting concept.

**WHEN TO USE:**
- After confirming which reporting concept a CoA account should map to
- Writes the association to the mapping structure in the OLTP database

**REQUIRES:**
- mapping_id: The mapping structure ID (create one first via the API if needed)
- from_element_id: The CoA element being mapped
- to_element_id: The target US GAAP reporting concept

**RETURNS:**
- The created association with IDs and element names""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "mapping_id": {
            "type": "string",
            "description": "The mapping structure ID to add the association to",
          },
          "from_element_id": {
            "type": "string",
            "description": "Source CoA element ID",
          },
          "to_element_id": {
            "type": "string",
            "description": "Target US GAAP reporting concept element ID",
          },
          "confidence": {
            "type": "number",
            "description": "Confidence score (0-1). Omit for manual mappings.",
          },
        },
        "required": ["mapping_id", "from_element_id", "to_element_id"],
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    from robosystems.db.extensions import extensions_session
    from robosystems.models.extensions import Association, Element
    from robosystems.utils.ulid import generate_prefixed_ulid

    graph_id = self.client.graph_id

    try:
      with extensions_session(graph_id) as session:
        from sqlalchemy import select

        from_elem = session.execute(
          select(Element).where(Element.id == arguments["from_element_id"])
        ).scalar_one_or_none()
        to_elem = session.execute(
          select(Element).where(Element.id == arguments["to_element_id"])
        ).scalar_one_or_none()

        if not from_elem:
          return {"error": "Source element not found"}
        if not to_elem:
          return {"error": "Target element not found"}

        assoc = Association(
          id=generate_prefixed_ulid("assoc"),
          structure_id=arguments["mapping_id"],
          from_element_id=arguments["from_element_id"],
          to_element_id=arguments["to_element_id"],
          association_type="mapping",
          confidence=arguments.get("confidence"),
          suggested_by="ai",
          created_by="mcp",
        )
        session.add(assoc)
        session.flush()

        return {
          "created": True,
          "association_id": assoc.id,
          "from_element": from_elem.name,
          "from_code": from_elem.code,
          "to_element": to_elem.name,
          "to_qname": to_elem.qname,
        }
    except Exception as exc:
      logger.warning(f"create-mapping-association failed: {exc}")
      return {"error": str(exc)}


class GetMappingSummaryTool:
  """Get mapping coverage statistics."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "get-mapping-summary",
      "description": """Get coverage statistics for a CoA → GAAP mapping.

**WHEN TO USE:**
- To check progress during a mapping session
- To see how many accounts are mapped vs unmapped
- To review confidence distribution of AI-suggested mappings

**RETURNS:**
- Total CoA elements, mapped count, unmapped count, coverage percentage
- Confidence distribution: high (>0.90), medium (0.70-0.90), low (<0.70)""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "mapping_id": {
            "type": "string",
            "description": "The mapping structure ID to check coverage for",
          },
        },
        "required": ["mapping_id"],
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    from robosystems.db.extensions import extensions_session
    from robosystems.models.extensions import Association, Element

    graph_id = self.client.graph_id
    mapping_id = arguments["mapping_id"]

    try:
      with extensions_session(graph_id) as session:
        from sqlalchemy import func, select

        coa_sources = ("quickbooks", "xero", "plaid", "native", "import")
        total_coa = (
          session.execute(
            select(func.count())
            .select_from(Element)
            .where(
              Element.source.in_(coa_sources),
              Element.is_active.is_(True),
              Element.is_abstract.is_(False),
            )
          ).scalar()
          or 0
        )

        assocs = (
          session.execute(
            select(Association).where(
              Association.structure_id == mapping_id,
              Association.association_type == "mapping",
            )
          )
          .scalars()
          .all()
        )

        mapped_count = len({a.from_element_id for a in assocs})
        unmapped_count = total_coa - mapped_count
        high = sum(1 for a in assocs if a.confidence and a.confidence > 0.90)
        medium = sum(1 for a in assocs if a.confidence and 0.70 <= a.confidence <= 0.90)
        low = sum(1 for a in assocs if a.confidence and a.confidence < 0.70)

        return {
          "mapping_id": mapping_id,
          "total_coa_elements": total_coa,
          "mapped_count": mapped_count,
          "unmapped_count": unmapped_count,
          "coverage_percent": round(
            (mapped_count / total_coa * 100) if total_coa > 0 else 0.0, 1
          ),
          "confidence_distribution": {
            "high": high,
            "medium": medium,
            "low": low,
            "manual": len(assocs) - high - medium - low,
          },
        }
    except Exception as exc:
      logger.warning(f"get-mapping-summary failed: {exc}")
      return {"error": str(exc)}
