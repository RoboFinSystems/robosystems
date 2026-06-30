"""Reusable synthetic-ledger scenario engine for the showcase demos.

Each showcase episode declares a :class:`~examples._scenario.drivers.Scenario`
(a chart of accounts plus a set of economic *flows*), and the engine lowers
those flows to the balanced transaction tuples the RoboLedger demo harness
already consumes. The arc is authored as parameters (recognition curves,
collection lags, inventory pre-buys) rather than hand-written monthly dicts,
so the books articulate by construction and the working-capital story emerges
mechanically from the recognition-vs-settlement gap.

See ``examples/coffee_roaster_demo`` for the first episode (Driftline Coffee).
"""
