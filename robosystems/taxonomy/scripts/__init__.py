"""Live taxonomy tooling.

This package holds only the *live* scripts — idempotent regenerators that
reproduce a committed ``frameworks/`` artifact from current inputs, plus a
read-only validation tool:

- ``generate_tenant_exclude`` — rewrites ``frameworks/rs-gaap/tenant-exclude/v1.json``
  from the seeded public library; re-run when the catalog or Reporting Styles change.
- ``generate_rollup_rules`` — rewrites the ``rs-gaap-rollup-rules`` package from the
  calc DAG.
- ``print_library_hierarchy`` — renders the rollup trees + a coherence scan for any
  graph_id (curation validation).

The committed artifact is the JSON-LD under ``frameworks/``, not the script run.
**Spent one-shot transforms** that reshaped the hand-authored source once (the
``split_type_subtype`` / ``dedupe_type_subtype_labels`` / ``backfill_base_en_labels`` /
``swap_deprecated_revenue_cogs`` / ``consolidate_labels_to_en`` family) live in
``local/scripts/taxonomy/`` alongside the framework-derivation scripts — their job
is done, their provenance is in the commit history.
"""
