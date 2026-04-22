"""Association classifier — cross-domain home for pattern detection.

Phase epsilon lands the data model (``association_classifications``
junction + :class:`ClassificationLite` on the envelope + tenant copy).
The runtime classifier logic that populates the junction — Cypher
pattern-matching for RollUp / RollForward / Hypercube / LineItems /
MemberList / Dimension, plus disclosure-mechanics semantic
classification — still lives inside the SEC adapter at
``robosystems/adapters/sec/processors/classify.py``.

The extraction happens in Phase δ.3's expand pass: a Postgres-backed
classifier that runs over OLTP ``associations`` + ``elements`` directly,
producing ``association_classifications`` rows. Until then, the SEC
adapter keeps its LadybugDB/parquet-based implementation and writes
results to the graph's Classification nodes; tenant OLTP
``association_classifications`` rows arrive via library seed writers
when seed content carries them (none today).

Scaffold reserved so future ``classify(session, structure_id, …)``
calls have a stable import path that handlers can hook into.
"""

from __future__ import annotations

__all__: list[str] = []
