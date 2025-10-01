"""
Copy endpoint module for data ingestion from various sources.

This module provides a comprehensive data copying system with:
- Multiple source strategies (S3, DataFrame, URL, etc.)
- Format detection and validation
- Tier-based limits and rate limiting
- Security validation and audit logging
- Extensible architecture for new sources
"""

from fastapi import APIRouter

from .execute import router as execute_router

# Create main copy router without prefix
# The prefix will be added at the main app level as /v1/graphs/{graph_id}/copy
router = APIRouter(
  tags=["Copy"],
)

# Mount execute router for copy operations
# Note: execute_router should not have its own tags to avoid duplication
router.include_router(execute_router)

# Export main router
__all__ = ["router"]
