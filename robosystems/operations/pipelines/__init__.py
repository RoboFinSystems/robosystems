"""
Pipeline Operations

This module contains production-ready data processing pipelines that orchestrate
complex multi-step operations with full visibility and tracking.

Key features:
- First-class pipeline tracking with Redis-based state management
- Distributed execution across workers
- Automatic Kuzu ingestion upon completion
- Production-ready error handling and retry logic
- Full transparency of pipeline progress
"""

from .sec_xbrl_filings import SECXBRLPipeline

__all__ = ["SECXBRLPipeline"]
