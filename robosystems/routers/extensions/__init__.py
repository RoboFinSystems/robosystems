"""Extensions API routers.

Hosts the command-surface REST endpoints for the extensions subsystem
(RoboLedger, RoboInvestor). Reads live on `/extensions/{graph_id}/graphql`
(Strawberry); writes live at `POST /extensions/{domain}/{graph_id}/operations/{operation_name}`
and all share the `OperationEnvelope` shape defined in
`robosystems.middleware.extensions`.
"""
