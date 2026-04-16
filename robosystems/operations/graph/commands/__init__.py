"""Graph lifecycle command functions.

Pure business logic for graph-scoped operations. Each command takes
typed inputs, calls existing services, and returns a result dict or
raises HTTPException. The graph operations router wraps these in
OperationEnvelope via the shared dispatch infrastructure.
"""
