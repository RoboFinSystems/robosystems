# Cadence Labs — SHACL Ontology Conformance

## Result: ✅ **Conforms to RoboSystems RDF Ontology v1**

- **Bundle**: `saas-startup-demo.jsonld`
- **Graph triples**: 3,786
- **rs:Fact nodes**: 85
- **rs:Association nodes**: 170
- **rs:Element nodes**: 98
- **SHACL shapes checked**: 8 (positive instance shapes + negative shapes banning the retired dialects)

Validated on the host with **pyshacl** against `frameworks/ontology/v1/shapes.ttl` — the *same* shapes that gate the framework seeds and the publish-time bundle validation, run here directly on the on-disk artifact (no API, no database, no container). Conformance means every `rs:Fact` references its aspects directly (`rs:element`/`rs:entity`/`rs:period`/`rs:unit` — no XBRL `context`), every `rs:Association` carries `xlink:from`/`to` + `xlink:arcrole`, and none of the retired dialects (`xbrli:contextRef`, `arcFrom`, direct `summationOf`) appear.

## Violations

_None._ Zero violations.
