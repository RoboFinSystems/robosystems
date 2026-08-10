"""Live taxonomy tooling.

This package holds only the *live* scripts — idempotent regenerators that
reproduce a committed ``frameworks/`` artifact from current inputs, plus a
read-only validation tool:

- ``generate_tenant_exclude`` — rewrites ``frameworks/rs-gaap/tenant-exclude/v1.json``
  from the seeded public library; re-run when the catalog or Reporting Styles change.
- ``print_library_hierarchy`` — renders the rollup trees + a coherence scan for any
  graph_id (curation validation).

``generate_rollup_rules`` was retired 2026-08-10 (issue #898). Its grouping key
was the per-subtotal calc role URI, which ``rs-gaap-calculations/v1`` collapsed
into three statement-level Networks — so the generator raised rather than
regenerating, and the ``@id`` slugs it produced (``bs-assets``, ``is-cor``,
``cf-netchange``) are hand-abbreviated and not derivable from the parent qname.
``rs-gaap-rollup-rules/v1`` is now hand-maintained: one ``RollUp`` rule per calc
subtotal parent, and ``tests/taxonomy/test_rollup_rules_seed.py`` fails when the
calc DAG gains or loses a parent the package doesn't cover.

The committed artifact is the JSON-LD under ``frameworks/``, not the script run.
**Spent one-shot transforms** that reshaped the hand-authored source once (the
``split_type_subtype`` / ``dedupe_type_subtype_labels`` / ``backfill_base_en_labels`` /
``swap_deprecated_revenue_cogs`` / ``consolidate_labels_to_en`` family) were kept
developer-local and deleted 2026-08-05 once the reshaped JSON-LD became the source
of truth — their job is done, their provenance is in the commit history.

A **new** derivation script belongs here, not developer-local: the one-shots rotted
against three package-path renames precisely because nothing in CI held them honest.
"""
