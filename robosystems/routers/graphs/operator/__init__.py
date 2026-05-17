"""
Operator execution module with modular sub-components.

This module provides a comprehensive AI Operator execution system optimized for
intelligent analysis with:
- Automatic operator selection based on query intent
- Intelligent strategy selection based on execution profiles
- Multiple response formats (JSON, SSE) with transparent handling
- Progress monitoring for long-running operations
- Background queue integration for extended analysis

Note: "Operator" here is the AI-executor concept (Claude/MCP), distinct from
the REA ``Agent`` (counterparty: customer/vendor/employee) in
``models/extensions/roboledger/agent.py``.
"""

from fastapi import APIRouter

from .execute import router as execute_router

# Create main Operator router
router = APIRouter(
  tags=["Operator"],
)

# Mount sub-routers
router.include_router(execute_router)

# Export main router
__all__ = ["router"]
