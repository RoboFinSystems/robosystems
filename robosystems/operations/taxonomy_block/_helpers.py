"""Shared helpers for taxonomy block handlers.

Small utilities shared across the CoA / custom_ontology /
reporting_extension handlers. Factored here so the per-block-type
modules don't drift on the common derivations.
"""

from __future__ import annotations


def qname_for(
  standard: str | None,
  default_namespace: str,
  code: str | None,
  name: str,
) -> str:
  """Derive the envelope-local qname when the tenant didn't supply one.

  Falls back to ``<standard or default_namespace>:<code or name-without-spaces>``
  so qname is always set; the DB column is nullable but the envelope
  treats qname as the canonical identifier.
  """
  ns = standard or default_namespace
  token = code or name.replace(" ", "")
  return f"{ns}:{token}"


__all__ = ["qname_for"]
