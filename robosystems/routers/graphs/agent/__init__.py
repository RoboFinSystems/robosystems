"""
Agent execution module with modular sub-components.

This module provides a comprehensive AI agent execution system optimized for intelligent
analysis with:
- Automatic agent selection based on query intent
- Intelligent strategy selection based on execution profiles
- Multiple response formats (JSON, SSE) with transparent handling
- Progress monitoring for long-running operations
- Celery worker integration for extended analysis
"""

from fastapi import APIRouter

from .execute import router as execute_router

# Create main Agent router
router = APIRouter(
  tags=["Agent"],
)

# Mount sub-routers
router.include_router(execute_router)

# Export main router
__all__ = ["router"]
