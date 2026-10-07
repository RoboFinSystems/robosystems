"""Initialize an entity's chart of accounts from a shipped template, once, for
an entity with no chart (a QuickBooks-synced one's arrives with the sync).

One transaction, two steps: the Taxonomy Block envelope (the chart plus one
``coa_mapping`` structure per framework the tenant carries) through the CoA
handler, then each framework's mapping arcs resolved by qname against the
tenant's library copy, since the handler resolves refs only within the
envelope. The chart is minted as tenant-owned elements: ``coa:*`` for the
group parent, a prefix of its own for any other entity.
"""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from robosystems.logger import logger
from robosystems.models.api.extensions.chart_of_accounts import (
  InitializeChartOfAccountsRequest,
  InitializeChartOfAccountsResponse,
)
from robosystems.models.api.extensions.taxonomies import (
  CreateMappingAssociationOperation,
)
from robosystems.models.api.taxonomy_block import (
  CreateTaxonomyBlockRequest,
  TaxonomyBlockElementRequest,
  TaxonomyBlockStructureRequest,
)
from robosystems.models.extensions import Element, Entity
from robosystems.operations.roboledger.commands.taxonomies import (
  MappingAssociationExistsError,
  create_mapping_association,
)
from robosystems.operations.roboledger.entity_scope import find_entity_id
from robosystems.operations.taxonomy_block.chart_of_accounts import (
  chart_namespace,
)
from robosystems.operations.taxonomy_block.chart_of_accounts import (
  create as create_chart_block,
)
from robosystems.operations.taxonomy_block.chart_templates import (
  ChartTemplate,
  MappingSet,
  get_template,
  resolve_form,
)
from robosystems.operations.taxonomy_block.coa_mappings import (
  entity_chart_id,
  find_mapping_structure,
  framework_taxonomy_id,
)

COA_TAXONOMY_TYPE = "chart_of_accounts"
DEFAULT_CHART_NAME = "Chart of Accounts"


class ChartAlreadyExistsError(ValueError):
  """The entity already has an active chart of accounts."""

  def __init__(self, taxonomy_id: str) -> None:
    super().__init__(
      "This entity already has a chart of accounts; a chart is never "
      "replaced. Customize it with update-taxonomy-block."
    )
    self.taxonomy_id = taxonomy_id


class ChartTemplateNotFoundError(LookupError):
  def __init__(self, key: str) -> None:
    super().__init__(f"Unknown chart template {key!r}.")
    self.key = key


def active_chart_id(session: Session, entity_id: str | None = None) -> str | None:
  """The entity's active ``chart_of_accounts`` taxonomy id, if any; the group
  parent's when no entity is named.

  Any origin counts — QuickBooks-synced, taxonomy-block-authored, or
  template-initialized. Initialize is one-time; a chart is never replaced.
  """
  return entity_chart_id(session, find_entity_id(session, entity_id))


def initialize_chart_of_accounts(
  session: Session,
  body: InitializeChartOfAccountsRequest,
  created_by: str,
  *,
  entity_id: str | None = None,
) -> InitializeChartOfAccountsResponse:
  """Initialize the entity's chart, default the group parent's. Each entity
  keeps its own; a sibling already having one is no bar."""
  template = get_template(body.template)
  if template is None:
    raise ChartTemplateNotFoundError(body.template)

  owner_id = find_entity_id(session, entity_id or body.entity_id)
  existing = entity_chart_id(session, owner_id)
  if existing is not None:
    raise ChartAlreadyExistsError(existing)

  entity_type = _resolve_entity_type(session, body.entity_type, owner_id)
  name = (body.name or "").strip() or DEFAULT_CHART_NAME
  applicable, skipped = _applicable_mapping_sets(session, template)
  namespace = chart_namespace(session, owner_id)

  payload = CreateTaxonomyBlockRequest(
    name=name,
    taxonomy_type=COA_TAXONOMY_TYPE,
    standard=namespace,
    description=f"{template.display_name} chart, initialized from a template.",
    elements=[
      TaxonomyBlockElementRequest(
        qname=f"{namespace or 'coa'}:{code}",
        name=account_name,
        trait=trait,
        balance_type=balance_type,
        description=description,
        code=code,
        sub_classification=sub_classification,
      )
      for (
        code,
        account_name,
        trait,
        sub_classification,
        balance_type,
        description,
      ) in template.accounts
    ],
    structures=[
      TaxonomyBlockStructureRequest(
        name=mapping_set.structure_name,
        block_type="coa_mapping",
        target_framework=mapping_set.framework,
        description=(
          f"Maps the chart of accounts to {mapping_set.display_name} "
          "reporting concepts."
        ),
      )
      for mapping_set in applicable
    ],
    metadata={
      "template": template.key,
      "template_version": template.path.name,
      "entity_type": entity_type,
      "frameworks": [mapping_set.framework for mapping_set in applicable],
    },
  )
  taxonomy_id = create_chart_block(session, payload, created_by, entity_id=owner_id)

  coa_rows = session.execute(
    select(Element.code, Element.id).where(Element.taxonomy_id == taxonomy_id)
  ).all()
  coa_by_code: dict[str, str] = {str(code): str(eid) for code, eid in coa_rows}

  mappings_created = 0
  unresolved: list[str] = [
    f"{mapping_set.framework}: not in this graph's library" for mapping_set in skipped
  ]
  for mapping_set in applicable:
    created, misses = _create_mapping_set(
      session,
      taxonomy_id=taxonomy_id,
      coa_by_code=coa_by_code,
      mapping_set=mapping_set,
      entity_type=entity_type,
      created_by=created_by,
    )
    mappings_created += created
    unresolved.extend(miss for miss in misses if miss not in unresolved)

  logger.info(
    "Initialized chart of accounts %s from template %s (%d elements, "
    "%d mappings across %s, %d unresolved)",
    taxonomy_id,
    template.key,
    len(template.accounts),
    mappings_created,
    [mapping_set.framework for mapping_set in applicable] or "no framework",
    len(unresolved),
  )
  return InitializeChartOfAccountsResponse(
    taxonomy_id=taxonomy_id,
    name=name,
    template=template.key,  # type: ignore[arg-type]
    entity_type=entity_type,
    elements_created=len(template.accounts),
    mappings_created=mappings_created,
    frameworks=[mapping_set.framework for mapping_set in applicable],
    unresolved=unresolved,
  )


def _resolve_entity_type(
  session: Session, requested: str | None, entity_id: str | None
) -> str:
  """The legal form the equity rows are mapped for — always one of the
  forms the templates know, so the response and the taxonomy metadata record
  what was actually applied rather than the request string."""
  if requested and requested.strip():
    return resolve_form(requested)
  entity = session.get(Entity, entity_id) if entity_id is not None else None
  form = getattr(entity, "entity_type", None) if entity is not None else None
  return resolve_form(form)


def _applicable_mapping_sets(
  session: Session, template: ChartTemplate
) -> tuple[list[MappingSet], list[MappingSet]]:
  """Split the template's mapping sets by whether the tenant carries the
  framework — its taxonomy is in the library copy — in the template's
  declared order."""
  applicable: list[MappingSet] = []
  skipped: list[MappingSet] = []
  for mapping_set in template.mappings.values():
    present = framework_taxonomy_id(session, mapping_set.framework) is not None
    (applicable if present else skipped).append(mapping_set)
  return applicable, skipped


def _create_mapping_set(
  session: Session,
  *,
  taxonomy_id: str,
  coa_by_code: dict[str, str],
  mapping_set: MappingSet,
  entity_type: str,
  created_by: str,
) -> tuple[int, list[str]]:
  """Create one framework's mapping arcs; return (created, unresolved)."""
  mapping = find_mapping_structure(session, mapping_set.framework, chart_id=taxonomy_id)
  if mapping is None:
    raise LookupError(
      f"Chart {taxonomy_id!r} has no mapping into {mapping_set.framework!r}."
    )
  structure_id = str(mapping.id)

  arcs = mapping_set.arcs_for(entity_type)
  targets = sorted({qname for _code, qname in arcs})
  library_rows = session.execute(
    select(Element.qname, Element.id).where(
      Element.source == mapping_set.framework, Element.qname.in_(targets)
    )
  ).all()
  library_by_qname: dict[str, str] = {
    str(qname): str(eid) for qname, eid in library_rows
  }

  created = 0
  unresolved: list[str] = []
  for code, qname in arcs:
    from_id = coa_by_code.get(code)
    to_id = library_by_qname.get(qname)
    if from_id is None:
      # A template bug; report it rather than fail a usable chart.
      unresolved.append(f"{code} -> {qname} (no such account in template)")
      continue
    if to_id is None:
      if qname not in unresolved:
        unresolved.append(qname)
      continue
    try:
      create_mapping_association(
        session,
        CreateMappingAssociationOperation(
          mapping_id=structure_id,
          from_element_id=from_id,
          to_element_id=to_id,
          association_type="mapping",
          suggested_by="template",
        ),
        created_by,
      )
    except MappingAssociationExistsError:
      continue
    created += 1

  return created, unresolved
