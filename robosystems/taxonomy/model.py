"""Pydantic model for taxonomy packages.

`TaxonomyPackage` is the in-memory intermediate between JSON-LD (canonical
on-disk format) and the database. The loader reads JSON-LD → TaxonomyPackage;
``operations/taxonomy_block/library_creator.py`` takes TaxonomyPackage → ORM
session inserts. The
extractor (in `robosystems/arelle/`) produces rdflib.Graph which is
serialized to JSON-LD via the serializer — these never construct
TaxonomyPackage directly; it's a DB-side DTO.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class LabelSpec(BaseModel):
  """XBRL label linkbase entry."""

  role: str = Field(
    "standard",
    description=(
      "Label role: standard, verbose, terse, documentation, "
      "periodStart, periodEnd, negated, total, commentaryGuidance, other"
    ),
  )
  language: str = Field("en", description="ISO 639-1 language code")
  text: str = Field(..., description="The label text")


class ReferenceSpec(BaseModel):
  """XBRL reference linkbase entry (FASB ASC citation, SEC reg, etc)."""

  ref_type: str | None = Field(
    None, description="'ASC' | 'SEC' | 'SFAC' | 'IFRS' | 'Other'"
  )
  citation: str = Field(
    ..., description="Free-form citation, e.g. 'FASB ASC 842-10-25-1'"
  )
  uri: str | None = Field(None, description="Dereferenceable URL if available")
  attributes: str | None = Field(
    None,
    description="Raw attributes from the source reference linkbase (Publisher, Number, Paragraph)",
  )


class ElementSpec(BaseModel):
  """A single taxonomy element (XBRL concept).

  Only XBRL-intrinsic attributes live here (balance_type, period_type,
  is_abstract, is_monetary, element_type, substitution_group).
  FASB metamodel traits live in TraitSpec + TraitAssignmentSpec.
  """

  qname: str = Field(..., description="Qualified name, e.g. 'fac:Assets'")
  namespace: str = Field(..., description="Namespace prefix, e.g. 'fac'")
  namespace_uri: str = Field(..., description="Full namespace URI")
  name: str = Field(..., description="Local name within the namespace")

  balance_type: str = Field("debit", description="debit | credit")
  period_type: str = Field("duration", description="instant | duration")
  is_abstract: bool = Field(False, description="True for abstract grouping concepts")
  is_monetary: bool = Field(True, description="True for monetary-valued concepts")
  element_type: str = Field(
    "concept", description="concept | abstract | axis | member | hypercube"
  )
  substitution_group: str | None = Field(
    None, description="XBRL substitution group qname, e.g. 'xbrli:item'"
  )

  # Source / origin
  source: str = Field(..., description="fac | us-gaap | ifrs | …")

  # Hierarchy (resolved at write time) — parent_id in OLTP. Independent
  # of classification: this is the element-tree hierarchy, not the
  # class-subclass classification hierarchy (which lives in associations).
  parent_qname: str | None = Field(None, description="Parent element qname if known")

  # Rich metadata
  labels: list[LabelSpec] = Field(default_factory=list)
  references: list[ReferenceSpec] = Field(default_factory=list)


class TraitSpec(BaseModel):
  """A FASB metamodel trait vocabulary entry: (category, identifier) pair.

  Each entry seeds one row in the ``traits`` table. Categories are the
  25 element-side categories: 24 FASB metamodel axes + flowClassification.
  """

  category: str = Field(
    ...,
    description=(
      "Trait axis, e.g. 'elementsOfFinancialStatements', 'liquidity', 'activityType'."
    ),
  )
  identifier: str = Field(
    ..., description="Member name within the category, e.g. 'asset', 'current'."
  )
  source: str = Field(
    ..., description="Provenance, e.g. 'us-gaap-metamodel', 'fac', 'system'."
  )
  name: str | None = Field(None, description="Human-readable display name")
  description: str | None = Field(None)


class TraitAssignmentSpec(BaseModel):
  """Element-to-trait assignment — seeds one element_traits row."""

  element_qname: str = Field(..., description="Element qname being classified")
  category: str = Field(..., description="Trait category")
  identifier: str = Field(..., description="Member identifier within the category")
  source: str = Field(
    "us-gaap-metamodel",
    description=(
      "Provenance of the assignment — which seed/taxonomy declared the "
      "(element → trait) arc. Defaults to us-gaap-metamodel since that's "
      "where nearly all EFS assignments originate; future seeds from other "
      "metamodels (e.g. ifrs-metamodel) should set this explicitly so "
      "row provenance is preserved in element_traits.source."
    ),
  )
  is_primary: bool = Field(
    True,
    description=(
      "Canonical row per (element, category). Most elements carry a "
      "single primary assignment per category; multi-valued axes "
      "(multiple trait members in one category) set is_primary=true "
      "on the first and false on alternates."
    ),
  )
  confidence: float | None = Field(None, description="For AI-suggested assignments")


class AssociationSpec(BaseModel):
  """A single arc in the taxonomy (parent-child, equivalence, etc)."""

  from_qname: str = Field(..., description="Source element qname")
  to_qname: str = Field(..., description="Target element qname")
  association_type: str = Field(
    ...,
    description=(
      "presentation | calculation | mapping | equivalence | "
      "general-special | essence-alias"
    ),
  )
  arcrole: str = Field(..., description="Full XBRL arcrole URI")
  role: str | None = Field(None, description="XBRL extended link role URI (structure)")
  order: float | None = Field(None, description="Presentation order")
  weight: float | None = Field(None, description="Calculation weight (+1/-1)")


class StructureSpec(BaseModel):
  """An extended link role as a named structure (Balance Sheet, etc)."""

  name: str = Field(..., description="Human-readable name")
  role_uri: str = Field(..., description="Full XBRL extended link role URI")
  block_type: str = Field(
    "custom",
    description=(
      "balance_sheet | income_statement | cash_flow_statement | "
      "equity_statement | chart_of_accounts | coa_mapping | schedule | custom"
    ),
  )
  concept_arrangement: str | None = Field(
    None,
    description=(
      "Charlie's Concept Arrangement Pattern declared on this Disclosure. "
      "Drives the renderer's compilation strategy. Values:\n"
      "  arithmetic       — additive identity (Assets = Liabilities + Equity)\n"
      "  roll_up          — sub-totals roll up to a parent total\n"
      "  roll_forward     — beginning + delta = ending balance\n"
      "  variance         — actual vs target / budget\n"
      "  adjustment       — point-in-time correction\n"
      "  set              — flat enumeration with no aggregation\n"
      "  component        — composite Disclosure recursing into other "
      "Disclosures (BalanceSheet → AssetsRollUp + LiabilitiesAndEquityRollUp)\n"
      "  hierarchy        — nested grouping with no arithmetic\n"
      "  level1_textblock — top-level note (e.g. PropertyPlantAndEquipment)\n"
      "  level2_textblock — sub-note within a level-1\n"
      "  level3_textblock — third-level note\n"
      "  level4_textblock — fourth-level note\n"
      "  textblock        — generic note text block (legacy alias)\n"
      "When None, the renderer falls back to a per-block_type default "
      "(income_statement / balance_sheet / cash_flow_statement → arithmetic; "
      "equity_statement → roll_forward; validation_rules → arithmetic)."
    ),
  )


class RuleTargetSpec(BaseModel):
  """Polymorphic pointer from a rule to the atom it scopes.

  `target_ref` is the natural-key identifier for the target: a role URI
  when `target_kind='structure'`, a qname when `target_kind='element'`,
  or a reified-arc IRI when `target_kind='association'`. The writer
  resolves it to the corresponding UUID at seed time. Rules with no
  target (a global report-wide rule) omit this spec entirely.
  """

  target_kind: Literal["structure", "element", "association"] = Field(
    ..., description="Which atom type this rule is scoped to."
  )
  target_ref: str = Field(
    ...,
    description=(
      "Natural-key reference to the target atom. role URI for "
      "structure, qname for element, reified-arc IRI for association."
    ),
  )


class RuleVariableSpec(BaseModel):
  """Binding from a `$Variable` in the rule expression to a concept qname.

  The rule engine evaluates `rule_expression` by looking up each
  variable in the Structure's in-scope fact set and substituting the
  value. This spec is the compile-time binding table.
  """

  variable_name: str = Field(
    ..., description="Local variable name in `rule_expression`, e.g. 'Assets'."
  )
  variable_qname: str = Field(
    ..., description="Concept qname the variable resolves to, e.g. 'fac:Assets'."
  )


class RuleSpec(BaseModel):
  """A single verification rule from the Seattle Method taxonomy.

  Two orthogonal axes: `rule_category` (scope/governance — 8 values from
  `cm_VerificationRule` subclasses) and `rule_pattern` (mechanism — 10
  values from `cm_BusinessRulePattern`). `rule_expression` carries an
  XPath-flavored predicate with `$Variable` placeholders bound via
  `rule_variables`; evaluation is deferred to the engine.
  """

  id: str = Field(
    ...,
    description=(
      "Blank-node local id from the seed, e.g. 'fac-rule-bs-identity'. "
      "Writer rewrites this to a deterministic UUID5 keyed on "
      "(standard, id)."
    ),
  )
  rule_category: str = Field(
    ...,
    description=(
      "cm:VerificationRule subclass — AutomatedAccountingAndReportingChecks, "
      "FundamentalAccountingConceptRelation, PeerConsistencyRule, "
      "PriorPeriodConsistencyRule, ReportLevelModelStructureRule, "
      "ReportingSystemSpecificRule, ToDoManualTask, XBRLTechnicalSyntaxRule."
    ),
  )
  rule_pattern: str = Field(
    ...,
    description=(
      "cm:BusinessRulePattern member — Adjustment, CoExists, EqualTo, "
      "Exists, GreaterThan, GreaterThanOrEqualToZero, LessThan, "
      "RollForward, RollUp, Variance."
    ),
  )
  rule_expression: str = Field(
    ...,
    description=(
      "XPath-flavored predicate (verbatim from Charlie's XBRL Formula "
      "linkbases) or natural-language condition for structural rules. "
      "Evaluation DSL selection is the engine's job."
    ),
  )
  rule_target: RuleTargetSpec | None = Field(
    None,
    description=(
      "Atom the rule is scoped to. Null = global rule that applies report-wide."
    ),
  )
  rule_variables: list[RuleVariableSpec] = Field(default_factory=list)
  rule_message: str | None = Field(
    None,
    description=(
      "Human-readable message emitted when the rule fails. Null for "
      "engine-only rules whose failure output is constructed elsewhere."
    ),
  )
  rule_severity: Literal["info", "warning", "error"] = Field(
    "error", description="Failure severity when the rule does not evaluate true."
  )
  rule_origin: Literal["forked", "native"] = Field(
    "native",
    description=(
      "Provenance. 'forked' = transcribed from an upstream source "
      "(Seattle Method, etc). 'native' = authored in this seed or by a "
      "tenant post-provision."
    ),
  )


class TaxonomyPackage(BaseModel):
  """A fully-loaded taxonomy ready for library persistence."""

  name: str = Field(..., description="Human name, e.g. 'FAC v1'")
  standard: str = Field(..., description="fac | us-gaap | ifrs")
  version: str = Field(..., description="Version identifier, e.g. 'v1' or '2020'")
  namespace_uri: str = Field(..., description="Primary namespace URI for this package")

  elements: list[ElementSpec] = Field(default_factory=list)
  associations: list[AssociationSpec] = Field(default_factory=list)
  structures: list[StructureSpec] = Field(default_factory=list)
  traits: list[TraitSpec] = Field(default_factory=list)
  trait_assignments: list[TraitAssignmentSpec] = Field(default_factory=list)
  rules: list[RuleSpec] = Field(default_factory=list)

  taxonomy_type: str = Field(
    "reporting_standard",
    description=(
      "chart_of_accounts | reporting_standard | reporting_extension | "
      "custom_ontology | mapping | schedule | rules | "
      "trait-vocabulary | trait-assignment | "
      "classification-vocabulary | classification-assignment — shapes "
      "how the library viewer renders this taxonomy. Concept taxonomies "
      "(rs-gaap) are 'reporting_standard'; equivalence + hierarchy "
      "arc packs (fac, rs-gaap-hierarchy) are 'mapping'; the FASB "
      "metamodel seed is 'trait-vocabulary'; rs-gaap-to-metamodel "
      "is 'trait-assignment'; verification-rule packs (fac-rules) "
      "are 'rules'."
    ),
  )
  is_shared: bool = Field(True, description="Shared across tenants (library-origin)")
  description: str | None = Field(None)

  default_block_type: str | None = Field(
    None,
    description=(
      "Package-level default for ``StructureSpec.block_type`` when a "
      "structure has no explicit ``blockType`` and no role-uri "
      "heuristic match. Used by 'mapping' / 'rules' / 'disclosure' "
      "packages to override the role-uri name heuristic that would "
      "otherwise mistake (e.g.) a fac-to-rs-gaap crosswalk role for a "
      "balance_sheet just because the role URI mentions BS."
    ),
  )
