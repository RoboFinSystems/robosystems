"""Strawberry types for the extensions GraphQL schema.

Types are organized by domain (ledger, investor, ...) to match the
REST router layout. Each domain module defines Strawberry wrappers over
existing Pydantic response models so the GraphQL schema stays in lockstep
with the REST surface.
"""
