"""Initialize a chart of accounts from a shipped template.

The fresh-company half of the native accounting cutover
(``specs/ledger/native-accounting-cutover.md`` §4). A QuickBooks-synced
tenant never needs this — its chart arrives with the sync and stays the
chart after a sever. A company with no chart initializes one here, once,
and customizes it with ``update-taxonomy-block``.

Two steps, one transaction: the Taxonomy Block envelope (chart + the
``coa_mapping`` structure) through the declarative CoA handler, then the
template's CoA → rs-gaap mapping associations resolved against the library.
The handler resolves association refs only against envelope-local qnames,
so the library targets are a second step — the demo runner does the same
two steps over HTTP; here they are one unit of work.
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
from robosystems.models.extensions import Element, Structure, Taxonomy
from robosystems.operations.roboledger.commands.taxonomies import (
  MappingAssociationExistsError,
  create_mapping_association,
)
from robosystems.operations.roboledger.reads.entity import resolve_parent_entity
from robosystems.operations.taxonomy_block.chart_of_accounts import (
  create as create_chart_block,
)
from robosystems.operations.taxonomy_block.chart_templates import get_template

COA_TAXONOMY_TYPE = "chart_of_accounts"
MAPPING_STRUCTURE_NAME = "CoA to US GAAP Mapping"
DEFAULT_CHART_NAME = "Chart of Accounts"
DEFAULT_ENTITY_TYPE = "corporation"


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
  """The graph's active ``chart_of_accounts`` taxonomy id, if any."""
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
        name=MAPPING_STRUCTURE_NAME,
        block_type="coa_mapping",
        description="Maps the chart of accounts to rs-gaap reporting concepts.",
      )
    ],
    metadata={"template": template.key, "entity_type": entity_type},
  )
  taxonomy_id = create_chart_block(session, payload, created_by)

  mappings_created, unresolved = _create_template_mappings(
    session,
    taxonomy_id=taxonomy_id,
    mappings=template.mappings_for(entity_type),
    created_by=created_by,
  )

  logger.info(
    "Initialized chart of accounts %s from template %s (%d elements, "
    "%d mappings, %d unresolved)",
    taxonomy_id,
    template.key,
    len(template.accounts),
    mappings_created,
    len(unresolved),
  )
  return InitializeChartOfAccountsResponse(
    taxonomy_id=taxonomy_id,
    name=name,
    template=template.key,  # type: ignore[arg-type]
    entity_type=entity_type,
    elements_created=len(template.accounts),
    mappings_created=mappings_created,
    unresolved=unresolved,
  )


def _resolve_entity_type(session: Session, requested: str | None) -> str:
  if requested and requested.strip():
    return requested.strip().lower()
  entity = resolve_parent_entity(session)
  form = getattr(entity, "entity_type", None) if entity is not None else None
  return (form or DEFAULT_ENTITY_TYPE).strip().lower()


def _create_template_mappings(
  session: Session,
  *,
  taxonomy_id: str,
  mappings: list[tuple[str, str]],
  created_by: str,
) -> tuple[int, list[str]]:
  """Map the new chart's elements to the library; return (created, unresolved)."""
  structure_id = session.execute(
    select(Structure.id).where(
      Structure.taxonomy_id == taxonomy_id,
      Structure.block_type == "coa_mapping",
    )
  ).scalar_one()

  coa_rows = session.execute(
    select(Element.code, Element.id).where(Element.taxonomy_id == taxonomy_id)
  ).all()
  coa_by_code: dict[str, str] = {str(code): str(eid) for code, eid in coa_rows}

  targets = sorted({qname for _code, qname in mappings})
  library_rows = session.execute(
    select(Element.qname, Element.id).where(
      Element.source == "rs-gaap", Element.qname.in_(targets)
    )
  ).all()
  library_by_qname: dict[str, str] = {
    str(qname): str(eid) for qname, eid in library_rows
  }

  created = 0
  unresolved: list[str] = []
  for code, qname in mappings:
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
