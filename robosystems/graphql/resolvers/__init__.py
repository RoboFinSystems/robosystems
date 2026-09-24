"""Per-domain resolver classes, composed into the `Query` root by schema.py.

A field opens a session through `_common` (which runs the auth and extension
gates), calls `operations/{domain}/reads/`, and wraps the Pydantic result.
"""
