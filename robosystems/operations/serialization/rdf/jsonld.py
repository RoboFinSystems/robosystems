"""JSON-LD encoder for ``StatementBundle``.

Output shape matches spec §3 Report mode: a single document with
``@context`` embedded inline (snapshot of ``CANONICAL_CONTEXT`` + the
bundle's framework pins + iso4217 units) and a flat ``@graph`` array
containing the bundle header (``rs:Report`` or ``rs:LiveSnapshot``),
framework slice nodes (``rs:Element``, ``rs:Association``), per-Network
IB envelopes, and instance Facts.

The fidelity bar is at the Fact granularity — every Fact in the source
emits a Fact node with identical (concept @id, period, unit, value,
decimals). Element @ids use the compact qname form (``rs-gaap:Assets``);
all instance @ids use the ``rs:<type>/<row-id>`` scheme (``rs:fact/...``,
``rs:report/...``) so the bundle is fully self-referencing.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any

from pydantic import BaseModel

from robosystems.arelle.context import CANONICAL_CONTEXT, RS_VOCAB
from robosystems.operations.serialization.bundle import (
  BundleAssociation,
  BundleElement,
  BundleFact,
  PeriodMeta,
  StatementBundle,
)

# Units used in bundle Facts. Currency codes resolve under iso4217; the
# bundle context layers this in so consumers see ``"unit": "iso4217:USD"``
# rather than an opaque string.
_ISO4217_NAMESPACE = "http://www.xbrl.org/2003/iso4217#"


def serialize_to_jsonld(bundle: StatementBundle) -> str:
  """Serialize a ``StatementBundle`` to a JSON-LD string.

  Returns the JSON-LD document as a UTF-8 ``str``. The encoder is pure
  (no DB access, no side effects); all data lives on the bundle.
  Output is indented for readability — the artifact is downloaded by
  humans and parsed by tools that don't care about whitespace.
  """
  document: dict[str, Any] = {
    "@context": _build_context(bundle.framework_pins),
    "@graph": _build_graph(bundle),
  }
  return json.dumps(document, indent=2, default=_json_default, sort_keys=False)


# ── @context assembly ─────────────────────────────────────────────────────


def _build_context(framework_pins: dict[str, str]) -> dict[str, Any]:
  """Build the ``@context`` for a bundle.

  Starts from the canonical taxonomy library context (so concepts
  compact to their familiar qnames — ``rs-gaap:Assets``, ``fac:Asset``)
  and layers in iso4217 currency units plus the framework version pin
  metadata. The pins are carried as ``rs:frameworkVersion`` entries on
  the bundle header rather than as @context overrides — namespace URIs
  in our taxonomy already encode the version segment.
  """
  ctx = dict(CANONICAL_CONTEXT)
  ctx["iso4217"] = _ISO4217_NAMESPACE
  ctx.setdefault("frameworkVersion", {"@id": f"{RS_VOCAB}frameworkVersion"})
  return ctx


# ── @graph assembly ───────────────────────────────────────────────────────


def _build_graph(bundle: StatementBundle) -> list[dict[str, Any]]:
  graph: list[dict[str, Any]] = []
  graph.append(_bundle_header_node(bundle))
  graph.extend(_element_node(e) for e in bundle.framework_slice.elements)
  graph.extend(_association_node(a) for a in bundle.framework_slice.associations)
  graph.extend(_ib_envelope_node(env) for env in bundle.ib_envelopes)
  graph.extend(_fact_node(f) for f in bundle.facts)
  return graph


def _bundle_header_node(bundle: StatementBundle) -> dict[str, Any]:
  """The first node in ``@graph`` carries the mode + identity payload.

  Mode-tagged ``@type`` is what the (future) importer pivots on to
  decide whether the document is admissible as a Report — see spec §3.
  """
  entity_node = bundle.entity.model_dump(exclude_none=True)
  framework_pin_entries = [
    {"name": name, "version": version}
    for name, version in bundle.framework_pins.items()
  ]
  periods_node = [_period_node(p) for p in bundle.periods]

  if bundle.mode == "report":
    if bundle.report_meta is None:
      raise ValueError("report-mode bundle missing report_meta")
    rm = bundle.report_meta
    return {
      "@id": f"rs:report/{rm.report_id}",
      "@type": "rs:Report",
      "entity": entity_node,
      "periods": periods_node,
      "reporting_style": bundle.reporting_style,
      "frameworkVersion": framework_pin_entries,
      "filing_status": rm.filing_status,
      "filed_at": rm.filed_at,
      "supersedes_id": (f"rs:report/{rm.supersedes_id}" if rm.supersedes_id else None),
      "source_graph_id": rm.source_graph_id,
      "source_report_id": (
        f"rs:report/{rm.source_report_id}" if rm.source_report_id else None
      ),
      "shared_at": rm.shared_at,
      "generation_count": rm.generation_count,
    }

  if bundle.live_meta is None:
    raise ValueError("live-mode bundle missing live_meta")
  lm = bundle.live_meta
  return {
    "@id": f"rs:snapshot/{lm.snapshot_at.isoformat()}",
    "@type": "rs:LiveSnapshot",
    "entity": entity_node,
    "periods": periods_node,
    "reporting_style": bundle.reporting_style,
    "frameworkVersion": framework_pin_entries,
    "snapshot_at": lm.snapshot_at,
    "non_authoritative": True,
  }


def _period_node(period: PeriodMeta) -> dict[str, Any]:
  if period.period_type == "instant":
    return {"instant": period.end, "label": period.label, "@type": "rs:Period"}
  return {
    "startDate": period.start,
    "endDate": period.end,
    "label": period.label,
    "@type": "rs:Period",
  }


def _element_node(element: BundleElement) -> dict[str, Any]:
  """Project a ``BundleElement`` to an ``rs:Element`` JSON-LD node.

  Uses the compact ``qname`` form for ``@id`` so concepts resolve via
  the @context to their canonical namespace IRIs (``rs-gaap:Assets`` →
  ``https://robosystems.ai/taxonomy/rs-gaap/v1/Assets``). The DB ulid
  is preserved as ``rs:internalId`` for round-trip identity recovery.
  """
  node: dict[str, Any] = {
    "@id": element.qname,
    "@type": "rs:Element",
    "internalId": element.id,
    "name": element.name,
    "periodType": element.period_type,
    "abstract": element.is_abstract,
    "monetary": element.is_monetary,
    "elementType": element.element_type,
    "source": element.source,
  }
  if element.label:
    node["label"] = element.label
  if element.balance_type:
    node["balance"] = element.balance_type
  if element.substitution_group:
    node["substitutionGroup"] = element.substitution_group
  if element.namespace:
    node["namespace"] = element.namespace
  return node


def _association_node(association: BundleAssociation) -> dict[str, Any]:
  """Project a ``BundleAssociation`` to a standalone ``rs:Association``
  node.

  The standalone shape (one node per arc, carrying ``from`` / ``to`` /
  ``arcrole`` / ``role`` / ``order`` / ``weight``) is the round-trip
  form. Compact predicate form (e.g. ``rs:summationOf`` on the parent
  Element pointing at children) is the human-readable form used in
  static framework JSON-LD; the bundle uses the standalone form so the
  arc metadata round-trips losslessly.
  """
  node: dict[str, Any] = {
    "@type": "rs:Association",
    "structure": f"rs:structure/{association.structure_id}",
    "from": f"rs:element/{association.from_element_id}",
    "to": f"rs:element/{association.to_element_id}",
    "associationType": association.association_type,
  }
  if association.arcrole:
    node["arcrole"] = association.arcrole
  if association.role:
    node["role"] = association.role
  if association.order_value is not None:
    node["order"] = association.order_value
  if association.weight is not None:
    node["weight"] = association.weight
  return node


def _ib_envelope_node(envelope: Any) -> dict[str, Any]:
  """Project an ``InformationBlockEnvelope`` to a JSON-LD node.

  The envelope is a Pydantic model — dump it and tag with ``@type``
  plus an ``rs:ib/<structure_id>`` ``@id``. Field shape is preserved
  verbatim so the receiving renderer can rehydrate the envelope
  without lookup work.
  """
  if isinstance(envelope, BaseModel):
    body = envelope.model_dump(exclude_none=True, mode="json")
  elif isinstance(envelope, dict):
    body = dict(envelope)
  else:
    body = dict(envelope.__dict__)
  body["@type"] = "rs:InformationBlock"
  if "id" in body:
    body["@id"] = f"rs:ib/{body['id']}"
  return body


def _fact_node(fact: BundleFact) -> dict[str, Any]:
  """Project a ``BundleFact`` to an ``rs:Fact`` JSON-LD node.

  Carries the round-trip-critical fields: concept (via qname @id),
  period (instant or duration), unit (iso4217:USD shape), value,
  decimals. The internal ulid is preserved as ``rs:internalId``.
  """
  period_node: dict[str, Any]
  if fact.period_type == "instant":
    period_node = {"instant": fact.period_end}
  else:
    period_node = {"startDate": fact.period_start, "endDate": fact.period_end}

  node: dict[str, Any] = {
    "@id": f"rs:fact/{fact.id}",
    "@type": "rs:Fact",
    "internalId": fact.id,
    "concept": fact.element_qname,
    "period": period_node,
    "unit": f"iso4217:{fact.unit}",
    "value": fact.value,
    "decimals": fact.decimals,
  }
  if fact.fact_set_id:
    node["factSet"] = f"rs:factset/{fact.fact_set_id}"
  if fact.structure_id:
    node["structure"] = f"rs:structure/{fact.structure_id}"
  return node


# ── JSON encoder default ──────────────────────────────────────────────────


def _json_default(obj: Any) -> Any:
  """Encode types ``json.dumps`` doesn't know natively.

  Handles ``date`` / ``datetime`` (ISO-8601), Pydantic models
  (``.model_dump(mode='json')`` so nested dates also serialize),
  and anything else by delegating to ``str`` as a last resort.
  """
  if isinstance(obj, datetime):
    return obj.isoformat()
  if isinstance(obj, date):
    return obj.isoformat()
  if isinstance(obj, BaseModel):
    return obj.model_dump(mode="json", exclude_none=True)
  raise TypeError(f"JSON-LD encoder: unsupported type {type(obj).__name__}")
