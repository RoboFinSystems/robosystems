"""RoboLedger domain operations.

All accounting-specific business logic — financial reports, closing
schedules, fiscal calendar state machine — lives here. Platform
infrastructure (OLTP loader, graph materialization, staleness) remains
in `robosystems.operations.extensions`.

Grouped under a `roboledger/` namespace to mirror the existing
`models/extensions/roboledger/` split and to make the eventual extraction
into a standalone `roboledger` package a straightforward git mv.
"""
