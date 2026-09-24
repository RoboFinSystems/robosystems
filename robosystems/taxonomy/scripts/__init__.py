"""Live taxonomy tooling: idempotent regenerators of committed ``frameworks/``
artifacts, plus a read-only validation tool.

- ``generate_tenant_exclude``: rewrites ``frameworks/rs-gaap/tenant-exclude/v1.json``.
- ``print_library_hierarchy``: rollup trees and a coherence scan for a graph.

``rs-gaap-rollup-rules/v1`` is hand-maintained (one RollUp rule per calc
subtotal parent); tests/taxonomy/test_rollup_rules_seed.py catches drift.
New derivation scripts belong here, where CI keeps them honest, not
developer-local.
"""
