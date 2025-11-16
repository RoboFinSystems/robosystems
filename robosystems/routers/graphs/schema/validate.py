"""Schema validation endpoint."""

import yaml
import json
import asyncio
import time
from fastapi import APIRouter, Body, Depends, HTTPException, status, Path
from sqlalchemy.orm import Session

from robosystems.logger import logger
from robosystems.models.iam import User
from robosystems.models.api.graphs.schema import (
  SchemaValidationRequest,
  SchemaValidationResponse,
)
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.rate_limits import (
  subscription_aware_rate_limit_dependency,
)
from robosystems.schemas.custom import (
  CustomSchemaManager,
  SchemaFormat,
)
from robosystems.schemas.manager import SchemaManager
from robosystems.database import get_db_session
from robosystems.models.api.common import ErrorResponse
from robosystems.middleware.robustness import (
  OperationType,
  OperationStatus,
  record_operation_metric,
  get_operation_logger,
)

from .utils import circuit_breaker, timeout_coordinator
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN

router = APIRouter()


@router.post(
  "/schema/validate",
  response_model=SchemaValidationResponse,
  summary="Validate Schema",
  description="""Validate a custom schema definition before deployment.

This endpoint performs comprehensive validation including:
- **Structure Validation**: Ensures proper JSON/YAML format
- **Type Checking**: Validates data types (STRING, INT, DOUBLE, etc.)
- **Constraint Verification**: Checks primary keys and unique constraints
- **Relationship Integrity**: Validates node references in relationships
- **Naming Conventions**: Ensures valid identifiers
- **Compatibility**: Checks against existing extensions if specified

Supported formats:
- JSON schema definitions
- YAML schema definitions
- Direct dictionary format

Validation helps prevent:
- Schema deployment failures
- Data integrity issues
- Performance problems
- Naming conflicts

**Subgraph Support:**
This endpoint accepts both parent graph IDs and subgraph IDs.
- Parent graph: Use `graph_id` like `kg0123456789abcdef`
- Subgraph: Use full subgraph ID like `kg0123456789abcdef_dev`
Schema validation is performed against the specified graph/subgraph's current
schema and data structure.

This operation is included - no credit consumption required.""",
  status_code=status.HTTP_200_OK,
  operation_id="validateSchema",
  responses={
    200: {
      "description": "Schema validation completed",
      "model": SchemaValidationResponse,
    },
    400: {"description": "Invalid schema format", "model": ErrorResponse},
    403: {"description": "Access denied to graph", "model": ErrorResponse},
    422: {"description": "Schema validation failed", "model": ErrorResponse},
    500: {"description": "Validation error", "model": ErrorResponse},
  },
)
async def validate_schema(
  graph_id: str = Path(
    ..., description="Graph database identifier", pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN
  ),
  request: SchemaValidationRequest = Body(
    ...,
    description="Schema definition to validate",
    openapi_examples={
      "valid_schema": {
        "summary": "Valid Schema with Relationships",
        "description": "A complete valid schema with nodes and relationships",
        "value": {
          "schema_definition": {
            "name": "financial_analysis",
            "version": "1.0.0",
            "description": "Schema for SEC financial data",
            "nodes": [
              {
                "name": "Company",
                "properties": [
                  {"name": "cik", "type": "STRING", "is_primary_key": True},
                  {"name": "name", "type": "STRING", "is_required": True},
                  {"name": "ticker", "type": "STRING"},
                ],
              },
              {
                "name": "Filing",
                "properties": [
                  {
                    "name": "accession_number",
                    "type": "STRING",
                    "is_primary_key": True,
                  },
                  {"name": "form_type", "type": "STRING"},
                ],
              },
            ],
            "relationships": [
              {
                "name": "FILED",
                "from_node": "Company",
                "to_node": "Filing",
              }
            ],
          },
          "format": "json",
        },
      },
      "schema_with_warnings": {
        "summary": "Schema with Warnings",
        "description": "Valid schema but with isolated nodes",
        "value": {
          "schema_definition": {
            "name": "warehouse_schema",
            "version": "1.0.0",
            "nodes": [
              {
                "name": "Product",
                "properties": [
                  {"name": "sku", "type": "STRING", "is_primary_key": True}
                ],
              },
              {
                "name": "Location",
                "properties": [
                  {"name": "id", "type": "STRING", "is_primary_key": True}
                ],
              },
            ],
            "relationships": [],
          },
          "format": "json",
        },
      },
      "invalid_schema": {
        "summary": "Invalid Schema",
        "description": "Schema with validation errors (invalid type, missing node)",
        "value": {
          "schema_definition": {
            "name": "invalid_example",
            "version": "1.0.0",
            "nodes": [
              {
                "name": "Company",
                "properties": [{"name": "name", "type": "INVALID_TYPE"}],
              }
            ],
            "relationships": [
              {
                "name": "RELATES_TO",
                "from_node": "Company",
                "to_node": "NonExistentNode",
              }
            ],
          },
          "format": "json",
        },
      },
      "yaml_format": {
        "summary": "YAML Format Schema",
        "description": "Schema validation using YAML format",
        "value": {
          "schema_definition": """name: inventory_schema
version: '1.0.0'
nodes:
  - name: Product
    properties:
      - name: sku
        type: STRING
        is_primary_key: true
      - name: name
        type: STRING
relationships:
  - name: IN_CATEGORY
    from_node: Product
    to_node: Category""",
          "format": "yaml",
        },
      },
      "compatibility_check": {
        "summary": "Compatibility Check",
        "description": "Validate schema and check compatibility with existing extensions",
        "value": {
          "schema_definition": {
            "name": "custom_extension",
            "version": "1.0.0",
            "nodes": [
              {
                "name": "Transaction",
                "properties": [
                  {"name": "id", "type": "STRING", "is_primary_key": True},
                  {"name": "amount", "type": "DOUBLE"},
                ],
              }
            ],
          },
          "format": "json",
          "check_compatibility": ["roboledger"],
        },
      },
    },
  ),
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
  db: Session = Depends(get_db_session),
) -> SchemaValidationResponse:
  """
  Validate a custom schema definition.

  This endpoint checks:
  - Schema structure validity
  - Data type correctness
  - Primary key requirements
  - Node/relationship consistency
  - Optional compatibility with existing extensions
  """
  # Initialize robustness components
  operation_logger = get_operation_logger()

  # Record operation start and get timing
  operation_start_time = time.time()

  # Record operation start metrics
  record_operation_metric(
    operation_type=OperationType.SCHEMA_OPERATION,
    status=OperationStatus.SUCCESS,  # Will be updated on completion
    duration_ms=0.0,  # Will be updated on completion
    endpoint="/v1/graphs/{graph_id}/schema/validate",
    graph_id=graph_id,
    user_id=current_user.id,
    operation_name="validate_schema",
    metadata={
      "format": request.format,
      "check_compatibility": bool(request.check_compatibility),
    },
  )

  # Initialize timeout for error handling
  operation_timeout = None

  try:
    # Check circuit breaker before processing
    circuit_breaker.check_circuit(graph_id, "schema_validation")

    # Set up timeout coordination for schema validation (can be complex)
    operation_timeout = timeout_coordinator.calculate_timeout(
      operation_type="validation",
      complexity_factors={
        "operation": "schema_validation",
        "check_compatibility": bool(request.check_compatibility),
        "expected_complexity": "high" if request.check_compatibility else "medium",
      },
    )

    # Schema operations are included - no credit consumption

    # Log the request with operation logger
    operation_logger.log_external_service_call(
      endpoint="/v1/graphs/{graph_id}/schema/validate",
      service_name="schema_manager",
      operation="validate_schema",
      duration_ms=0.0,  # Will be updated on completion
      status="processing",
      graph_id=graph_id,
      user_id=current_user.id,
      metadata={
        "format": request.format,
      },
    )

    manager = CustomSchemaManager()

    # Parse based on format
    format_map = {
      "json": SchemaFormat.JSON,
      "yaml": SchemaFormat.YAML,
      "dict": SchemaFormat.DICT,
    }

    schema_format = format_map.get(request.format.lower(), SchemaFormat.JSON)

    # Attempt to parse the schema with timeout coordination
    def validate_schema_sync():
      errors = []
      warnings = []
      stats = None
      valid = False

      try:
        # Convert string to dict if needed
        if isinstance(request.schema_definition, str):
          if schema_format == SchemaFormat.YAML:
            schema_dict = yaml.safe_load(request.schema_definition)
          else:
            schema_dict = json.loads(request.schema_definition)
        else:
          schema_dict = request.schema_definition

        # Parse and validate
        schema = manager.create_from_dict(schema_dict)
        valid = True

        # Collect statistics
        stats = {
          "nodes": len(schema.nodes),
          "relationships": len(schema.relationships),
          "total_properties": sum(len(node.properties) for node in schema.nodes),
          "primary_keys": sum(
            1
            for node in schema.nodes
            for prop in node.properties
            if prop.is_primary_key
          ),
        }

        # Check for warnings
        if len(schema.nodes) == 0:
          warnings.append("Schema has no nodes defined")

        if len(schema.relationships) > 0 and len(schema.nodes) == 0:
          warnings.append("Schema has relationships but no nodes")

        # Check node connectivity
        connected_nodes = set()
        for rel in schema.relationships:
          if rel.from_node != "*":
            connected_nodes.add(rel.from_node)
          if rel.to_node != "*":
            connected_nodes.add(rel.to_node)

        isolated_nodes = [
          node.name
          for node in schema.nodes
          if node.name not in connected_nodes and len(schema.relationships) > 0
        ]
        if isolated_nodes:
          warnings.append(f"Isolated nodes with no relationships: {isolated_nodes}")

      except Exception as e:
        errors.append(str(e))

      return valid, errors, warnings, stats

    # Run validation with timeout
    valid, errors, warnings, stats = await asyncio.wait_for(
      asyncio.get_event_loop().run_in_executor(None, validate_schema_sync),
      timeout=operation_timeout,
    )

    # Check compatibility if requested
    compatibility = None
    if request.check_compatibility and valid:
      try:
        schema_manager = SchemaManager()
        compat_result = schema_manager.check_schema_compatibility(
          request.check_compatibility
        )

        compatibility = {
          "compatible": compat_result.compatible,
          "conflicts": compat_result.conflicts,
          "checked_extensions": request.check_compatibility,
        }

        if not compat_result.compatible:
          warnings.extend(compat_result.conflicts)

      except Exception as e:
        warnings.append(f"Compatibility check failed: {str(e)}")

    message = "Schema is valid" if valid else "Schema validation failed"
    if warnings and valid:
      message += f" with {len(warnings)} warning(s)"

    # Record successful operation
    operation_duration_ms = (time.time() - operation_start_time) * 1000
    circuit_breaker.record_success(graph_id, "schema_validation")

    # Record success metrics
    record_operation_metric(
      operation_type=OperationType.SCHEMA_OPERATION,
      status=OperationStatus.SUCCESS,
      duration_ms=operation_duration_ms,
      endpoint="/v1/graphs/{graph_id}/schema/validate",
      graph_id=graph_id,
      user_id=current_user.id,
      operation_name="validate_schema",
      metadata={
        "format": request.format,
        "valid": valid,
        "node_count": stats.get("nodes", 0) if stats else 0,
      },
    )

    return SchemaValidationResponse(
      valid=valid,
      message=message,
      errors=errors if errors else None,
      warnings=warnings if warnings else None,
      stats=stats,
      compatibility=compatibility,
    )

  except asyncio.TimeoutError:
    # Record circuit breaker failure and timeout metrics
    circuit_breaker.record_failure(graph_id, "schema_validation")
    operation_duration_ms = (time.time() - operation_start_time) * 1000

    # Record timeout failure metrics
    record_operation_metric(
      operation_type=OperationType.SCHEMA_OPERATION,
      status=OperationStatus.FAILURE,
      duration_ms=operation_duration_ms,
      endpoint="/v1/graphs/{graph_id}/schema/validate",
      graph_id=graph_id,
      user_id=current_user.id,
      operation_name="validate_schema",
      metadata={
        "format": request.format,
        "error_type": "timeout",
        "timeout_seconds": operation_timeout,
      },
    )

    timeout_str = f" after {operation_timeout}s" if operation_timeout else ""
    logger.error(f"Schema validation timeout{timeout_str} for user {current_user.id}")
    raise HTTPException(
      status_code=status.HTTP_504_GATEWAY_TIMEOUT,
      detail="Schema validation timed out",
    )
  except HTTPException:
    # Record circuit breaker failure for HTTP exceptions
    circuit_breaker.record_failure(graph_id, "schema_validation")
    operation_duration_ms = (time.time() - operation_start_time) * 1000

    # Record failure metrics
    record_operation_metric(
      operation_type=OperationType.SCHEMA_OPERATION,
      status=OperationStatus.FAILURE,
      duration_ms=operation_duration_ms,
      endpoint="/v1/graphs/{graph_id}/schema/validate",
      graph_id=graph_id,
      user_id=current_user.id,
      operation_name="validate_schema",
      metadata={
        "format": request.format,
        "error_type": "http_exception",
      },
    )
    raise
  except Exception as e:
    # Record circuit breaker failure for general exceptions
    circuit_breaker.record_failure(graph_id, "schema_validation")
    operation_duration_ms = (time.time() - operation_start_time) * 1000

    # Record failure metrics
    record_operation_metric(
      operation_type=OperationType.SCHEMA_OPERATION,
      status=OperationStatus.FAILURE,
      duration_ms=operation_duration_ms,
      endpoint="/v1/graphs/{graph_id}/schema/validate",
      graph_id=graph_id,
      user_id=current_user.id,
      operation_name="validate_schema",
      metadata={
        "format": request.format,
        "error_type": type(e).__name__,
        "error_message": str(e),
      },
    )

    logger.error(f"Schema validation error: {e}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Failed to validate schema: {str(e)}",
    )
