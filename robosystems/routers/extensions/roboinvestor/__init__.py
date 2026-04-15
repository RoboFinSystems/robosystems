"""RoboInvestor command operations.

Hosts the `/extensions/roboinvestor/{graph_id}/operations/*` endpoints.
Each operation is a thin route that opens a session, calls into
`operations/roboinvestor/commands/*`, and returns an `OperationEnvelope`
via `execute_operation`.
"""
