"""Initialize a chart of accounts from a shipped template.

The fresh-company half of the native accounting cutover
(``specs/ledger/native-accounting-cutover.md`` §4; the templates themselves
are data — ``specs/taxonomy/chart-templates-as-data.md``). A
QuickBooks-synced tenant never needs this — its chart arrives with the sync
and stays the chart after a sever. A company with no chart initializes one
here, once, and customizes it with ``update-taxonomy-block``.

One transaction, in three steps: the Taxonomy Block envelope (the chart plus
one ``coa_mapping`` structure per framework the template maps into and the
tenant carries) through the declarative CoA handler; then, per framework,
the template's mapping arcs resolved by qname against the tenant's library
copy. The handler resolves association refs only against envelope-local
qnames, so the library targets are a second step — the demo runner does the
same two steps over HTTP; here they are one unit of work.

A template is a stencil, not library content: the file is read, the chart
is minted as tenant-owned ``coa:*`` elements, and the tenant owns it from
then on. The op follows the graph's framework pin through the tenant's
library copy — a mapping set applies when its framework's concepts are
present — so a plural pin yields a chart mapped into every framework it
carries, with no change here.
"""

from __future__ import annotations

from sqlalchemy import exists, select
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
from robosystems.models.extensions import Element, Structure, Taxonomy
from robosystems.operations.roboledger.commands.taxonomies import (
  MappingAssociationExistsError,
  create_mapping_association,
)
from robosystems.operations.roboledger.reads.entity import resolve_parent_entity
from robosystems.operations.taxonomy_block.chart_of_accounts import (
  create as create_chart_block,
)
from robosystems.operations.taxonomy_block.chart_templates import (
  ChartTemplate,
  MappingSet,
  get_template,
  resolve_form,
)

COA_TAXONOMY_TYPE = "chart_of_accounts"
# The rs-gaap mapping set's structure name — the one every tenant has today.
MAPPING_STRUCTURE_NAME = "CoA to US GAAP Mapping"
DEFAULT_CHART_NAME = "Chart of Accounts"


class ChartAlreadyExistsError(ValueError):
  """The graph already has an active chart of accounts."""

  def __init__(self, taxonomy_id: str) -> None:
    super().__init__(
      "This graph already has a chart of accounts; a chart is never "
      "replaced. Customize it with update-taxonomy-block."
    )
    self.taxonomy_id = taxonomy_id


class ChartTemplateNotFoundError(LookupError):
  def __init__(self, key: str) -> None:
    super().__init__(f"Unknown chart template {key!r}.")
    self.key = key


def active_chart_id(session: Session) -> str | None:
  """The graph's active ``chart_of_accounts`` taxonomy id, if any.

  Any origin counts — QuickBooks-synced, taxonomy-block-authored, or
  template-initialized. Initialize is one-time; a chart is never replaced.
  """
  return session.execute(
    select(Taxonomy.id)
    .where(
      Taxonomy.taxonomy_type == COA_TAXONOMY_TYPE,
      Taxonomy.is_active.is_(True),
    )
    .order_by(Taxonomy.created_at)
    .limit(1)
  ).scalar_one_or_none()


def initialize_chart_of_accounts(
  session: Session,
  body: InitializeChartOfAccountsRequest,
  created_by: str,
) -> InitializeChartOfAccountsResponse:
  template = get_template(body.template)
  if template is None:
    raise ChartTemplateNotFoundError(body.template)

  existing = active_chart_id(session)
  if existing is not None:
    raise ChartAlreadyExistsError(existing)

  entity_type = _resolve_entity_type(session, body.entity_type)
  name = (body.name or "").strip() or DEFAULT_CHART_NAME
  applicable, skipped = _applicable_mapping_sets(session, template)

  payload = CreateTaxonomyBlockRequest(
    name=name,
    taxonomy_type=COA_TAXONOMY_TYPE,
    description=f"{template.display_name} chart, initialized from a template.",
    elements=[
      TaxonomyBlockElementRequest(
        qname=f"coa:{code}",
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
  taxonomy_id = create_chart_block(session, payload, created_by)

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


def _resolve_entity_type(session: Session, requested: str | None) -> str:
  """The legal form the equity rows are mapped for — always one of the
  forms the templates know, so the response and the taxonomy metadata record
  what was actually applied rather than the request string."""
  if requested and requested.strip():
    return resolve_form(requested)
  entity = resolve_parent_entity(session)
  form = getattr(entity, "entity_type", None) if entity is not None else None
  return resolve_form(form)


def _applicable_mapping_sets(
  session: Session, template: ChartTemplate
) -> tuple[list[MappingSet], list[MappingSet]]:
  """Split the template's mapping sets by whether the tenant carries the
  framework — its concepts are in the library copy (``Element.source``) —
  in the template's declared order."""
  applicable: list[MappingSet] = []
  skipped: list[MappingSet] = []
  for mapping_set in template.mappings.values():
    present = bool(
      session.execute(
        select(exists().where(Element.source == mapping_set.framework))
      ).scalar()
    )
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
  structure_id = session.execute(
    select(Structure.id).where(
      Structure.taxonomy_id == taxonomy_id,
      Structure.block_type == "coa_mapping",
      Structure.name == mapping_set.structure_name,
    )
  ).scalar_one()

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
      # A template whose mapping names a code it does not declare is a
      # template bug; report it as unresolved rather than failing the
      # chart the customer can already use.
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
