"""Shipped chart-of-accounts templates — read-only stencils, loaded from data.

A template lives at ``frameworks/chart-templates/<key>/v1/``: a
framework-free ``chart.jsonld`` (the accounts, anchored only to the
fac-traits substrate) and one ``mappings/<framework>.jsonld`` per framework
the chart knows how to map into. It is a **stencil, not library content**:
never seeded into ``public``, never copied into a tenant at provision —
``initialize-chart-of-accounts`` instantiates it once, as tenant-owned
``coa:*`` elements, and the tenant owns the result from then on.

The directory sits beside the frameworks but is not one (no manifest, so
framework discovery never sees it — the ``ontology/`` precedent). A chart
maps into *many* frameworks — one ledger, many filings — which is why the
accounts and the mapping sets are separate files and why nothing here is
anchored to ``rs-gaap``.

Doctrine: ``specs/taxonomy/chart-templates-as-data.md`` (§2 the ruling, §5
the chart-of-accounts lifecycle) and ``specs/adapters/mercury-adapter.md``
§8.1 — no chart by default, native and synced ledgers never mix,
initializing a chart is an explicit, one-time act.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from robosystems.taxonomy.discovery import FRAMEWORKS_DIR

TEMPLATES_DIR: Path = FRAMEWORKS_DIR / "chart-templates"
TEMPLATE_VERSION = "v1"

# ``hasTrait`` in a chart file is a fac-traits IRI; the CoA envelope takes the
# bare elementsOfFinancialStatements member (asset / liability / …).
EFS_TRAIT_PREFIX = "trait:elementsOfFinancialStatements/"

# Legal forms the equity rows can be mapped for. A mapping set may carry a
# variant per form; unknown or empty forms resolve to the set's default.
LEGAL_FORMS: tuple[str, ...] = ("corporation", "partnership", "llc")
DEFAULT_FORM = "corporation"

# One chart row: (code, name, trait, sub_classification, balance_type,
# description). ``trait`` is the FASB metamodel member (asset / liability /
# equity / revenue / expense); ``balance_type`` is "debit" | "credit".
Account = tuple[str, str, str, str, str, str | None]

# One mapping arc: (chart account code, target concept qname).
Arc = tuple[str, str]


def resolve_form(entity_type: str | None) -> str:
  """The legal form a chart is mapped for — one of `LEGAL_FORMS`, never the
  raw request string. Unknown or empty forms resolve to ``corporation``."""
  form = (entity_type or "").strip().lower()
  return form if form in LEGAL_FORMS else DEFAULT_FORM


@dataclass(frozen=True)
class MappingSet:
  """A chart's mapping into one framework — ``mappings/<framework>.jsonld``.

  ``arcs`` are form-independent; ``variants`` swap rows by the framework's
  Reporting Style family (for rs-gaap: the equity rows by legal form). A
  framework without such a split ships one variant or none.
  """

  framework: str
  display_name: str
  structure_name: str
  arcs: list[Arc]
  variants: dict[str, list[Arc]]
  default_variant: str

  def resolve_variant(self, entity_type: str | None) -> str:
    form = (entity_type or "").strip().lower()
    return form if form in self.variants else self.default_variant

  def arcs_for(self, entity_type: str | None) -> list[Arc]:
    """Every arc for ``entity_type``: the base rows plus that form's variant."""
    variant = self.variants.get(self.resolve_variant(entity_type), [])
    return list(self.arcs) + list(variant)


@dataclass(frozen=True)
class ChartTemplate:
  key: str
  display_name: str
  description: str
  ordinal: int
  accounts: list[Account]
  mappings: dict[str, MappingSet]
  path: Path

  @property
  def account_count(self) -> int:
    return len(self.accounts)

  @property
  def frameworks(self) -> tuple[str, ...]:
    """Frameworks this template ships a mapping set for."""
    return tuple(self.mappings)

  def mapping_set(self, framework: str) -> MappingSet | None:
    return self.mappings.get(framework)


def _arcs(rows: list[dict[str, str]]) -> list[Arc]:
  return [(str(row["from"]), str(row["to"])) for row in rows]


def _load_mapping_set(path: Path) -> MappingSet:
  doc = json.loads(path.read_text(encoding="utf-8"))
  framework = str(doc["framework"])
  if framework != path.stem:
    raise ValueError(
      f"{path}: framework {framework!r} does not match the file name "
      f"{path.stem!r} — the file name is the framework key"
    )
  variants = {
    str(form): _arcs(rows) for form, rows in (doc.get("variants") or {}).items()
  }
  default_variant = str(doc.get("defaultVariant") or DEFAULT_FORM)
  if variants and default_variant not in variants:
    raise ValueError(
      f"{path}: defaultVariant {default_variant!r} is not one of {sorted(variants)}"
    )
  return MappingSet(
    framework=framework,
    display_name=str(doc.get("displayName") or framework),
    structure_name=str(doc.get("structureName") or f"CoA to {framework} Mapping"),
    arcs=_arcs(doc.get("arcs") or []),
    variants=variants,
    default_variant=default_variant,
  )


def _load_template(template_dir: Path) -> ChartTemplate:
  version_dir = template_dir / TEMPLATE_VERSION
  chart = json.loads((version_dir / "chart.jsonld").read_text(encoding="utf-8"))
  key = str(chart["key"])
  if key != template_dir.name:
    raise ValueError(
      f"{version_dir / 'chart.jsonld'}: key {key!r} does not match the "
      f"directory name {template_dir.name!r}"
    )
  accounts: list[Account] = []
  for row in chart["accounts"]:
    trait = str(row["hasTrait"])
    if not trait.startswith(EFS_TRAIT_PREFIX):
      raise ValueError(
        f"{key}: account {row.get('code')!r} trait {trait!r} is not an "
        f"elementsOfFinancialStatements member"
      )
    accounts.append(
      (
        str(row["code"]),
        str(row["label"]),
        trait.removeprefix(EFS_TRAIT_PREFIX),
        str(row["subClassification"]),
        str(row["balance"]),
        row.get("documentation"),
      )
    )
  mappings_dir = version_dir / "mappings"
  mapping_paths = sorted(mappings_dir.glob("*.jsonld")) if mappings_dir.exists() else []
  mapping_sets = [_load_mapping_set(path) for path in mapping_paths]
  return ChartTemplate(
    key=key,
    display_name=str(chart["displayName"]),
    description=str(chart["description"]),
    ordinal=int(chart.get("ordinal", 0)),
    accounts=accounts,
    mappings={ms.framework: ms for ms in mapping_sets},
    path=version_dir,
  )


def _load_catalogue(root: Path = TEMPLATES_DIR) -> dict[str, ChartTemplate]:
  if not root.exists():
    return {}
  templates = [
    _load_template(entry)
    for entry in root.iterdir()
    if entry.is_dir()
    and not entry.name.startswith(".")
    and (entry / TEMPLATE_VERSION / "chart.jsonld").exists()
  ]
  templates.sort(key=lambda t: (t.ordinal, t.key))
  return {t.key: t for t in templates}


CHART_TEMPLATES: dict[str, ChartTemplate] = _load_catalogue()

TEMPLATE_KEYS: tuple[str, ...] = tuple(CHART_TEMPLATES)


def get_template(key: str) -> ChartTemplate | None:
  return CHART_TEMPLATES.get((key or "").strip().lower())


def list_templates() -> list[ChartTemplate]:
  return list(CHART_TEMPLATES.values())


__all__ = [
  "CHART_TEMPLATES",
  "DEFAULT_FORM",
  "LEGAL_FORMS",
  "TEMPLATES_DIR",
  "TEMPLATE_KEYS",
  "Account",
  "Arc",
  "ChartTemplate",
  "MappingSet",
  "get_template",
  "list_templates",
  "resolve_form",
]
