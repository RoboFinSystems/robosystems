"""Document → text-block-fact binding (the bind-text-block operation).

Binds a platform Document (markdown) — or one of its sections — to a
disclosure Element as a ``Nonnumeric`` text-block Fact. The document
stays the editable source of truth; the fact snapshots its text into a
standing ``factset_type='disclosure'`` FactSet owned by the disclosure
Structure, stamped with ``DocumentProvenance`` (document_id + section +
content_hash — the drift signal). Report builds copy the standing set
into a per-report FactSet (``reports._snapshot_text_block_facts``), so a
filed report is immutable even if the document is later edited; a
re-bind refreshes the standing fact and its hash.

Cross-database by design: Documents live in the PLATFORM database, facts
in the extensions (tenant) database — the linkage is the provenance
reference, never an FK. The Document lookup is graph-scoped
(``get_by_id_and_graph``), so another graph's document id is a plain
miss, not an existence oracle.
"""

from __future__ import annotations

import hashlib

from sqlalchemy import select
from sqlalchemy.orm import Session

from robosystems.models.api.extensions.text_blocks import (
  BindTextBlockRequest,
  BindTextBlockResponse,
)
from robosystems.models.api.fact_provenance import DocumentProvenance
from robosystems.models.core.document.document import Document
from robosystems.models.extensions.element import Element
from robosystems.models.extensions.roboledger.fact import Fact
from robosystems.models.extensions.roboledger.fact_set import FactSet
from robosystems.models.extensions.structure import TEXT_BLOCK_CAPS, Structure
from robosystems.operations.information_block.envelope import DISCLOSURE_BLOCK_TYPE
from robosystems.operations.roboledger.fact_set import create_fact_set
from robosystems.operations.search.markdown_parser import (
  parse_frontmatter,
  section_markdown,
)
from robosystems.utils.ulid import generate_prefixed_ulid

# Mirrors DocumentService's update cap — the bind snapshots at most one
# document's worth of text.
_MAX_TEXT_CHARS = 500_000


class DocumentNotFoundError(Exception):
  """Document id doesn't resolve within this graph."""


class SectionNotFoundError(ValueError):
  """Requested section slug doesn't exist in the document."""


class TextBlockStructureError(ValueError):
  """Target structure isn't a text-block disclosure."""


class TextBlockElementError(ValueError):
  """Target element missing or not taggable (abstract)."""


def _resolve_text(document: Document, section_id: str | None) -> str:
  _, body = parse_frontmatter(document.content)
  if section_id is None:
    text = body.strip()
  else:
    sections = section_markdown(body, document.title)
    by_id = {s["section_id"]: s["content"] for s in sections}
    if section_id not in by_id:
      available = ", ".join(sorted(by_id)) or "(none)"
      raise SectionNotFoundError(
        f"Section '{section_id}' not found in document '{document.id}'. "
        f"Available sections: {available}"
      )
    text = by_id[section_id].strip()
  if not text:
    raise SectionNotFoundError(
      f"Document '{document.id}' has no content to bind"
      + (f" in section '{section_id}'" if section_id else "")
    )
  if len(text) > _MAX_TEXT_CHARS:
    raise ValueError(
      f"Bound text exceeds {_MAX_TEXT_CHARS:,} character limit ({len(text):,} chars)"
    )
  return text


def _resolve_element(session: Session, body: BindTextBlockRequest) -> Element:
  if body.element_id is not None:
    element = session.get(Element, body.element_id)
  else:
    element = session.execute(
      select(Element).where(Element.qname == body.element_qname).limit(1)
    ).scalar_one_or_none()
  if element is None:
    ref = body.element_id or body.element_qname
    raise TextBlockElementError(f"Element '{ref}' not found")
  if element.is_abstract:
    raise TextBlockElementError(
      f"Element '{element.qname or element.id}' is abstract — text binds to "
      "a concrete text-block concept"
    )
  return element


def bind_text_block(
  session: Session,
  platform_db: Session,
  graph_id: str,
  body: BindTextBlockRequest,
  created_by: str,
) -> BindTextBlockResponse:
  """Bind a Document (or section) to a disclosure element as a text-block fact.

  Upsert semantics: one standing ``'disclosure'`` FactSet per
  (structure, entity, period); a re-bind for the same element replaces
  that element's fact and refreshes the FactSet provenance
  (``replaced=True``). Other elements' facts in the same standing set
  are left untouched.
  """
  document = Document.get_by_id_and_graph(body.document_id, graph_id, platform_db)
  if document is None:
    raise DocumentNotFoundError(f"Document '{body.document_id}' not found")

  text_value = _resolve_text(document, body.section_id)
  content_hash = hashlib.sha256(text_value.encode("utf-8")).hexdigest()

  structure = session.get(Structure, body.structure_id)
  if structure is None or structure.block_type != DISCLOSURE_BLOCK_TYPE:
    raise TextBlockStructureError(
      f"Structure '{body.structure_id}' is not a regulatory_disclosure structure"
    )
  if (structure.concept_arrangement or "") not in TEXT_BLOCK_CAPS:
    raise TextBlockStructureError(
      f"Structure '{body.structure_id}' has concept_arrangement="
      f"'{structure.concept_arrangement}' — text binds to a text-block CAP "
      f"({', '.join(sorted(TEXT_BLOCK_CAPS))})"
    )

  element = _resolve_element(session, body)
  if element.item_type is None:
    # The bind is the sprint's only text authoring surface — enrich the
    # element's value domain here rather than widening the taxonomy-block
    # request schema.
    element.item_type = "text_block"

  if body.entity_id is not None:
    entity_id = body.entity_id
  else:
    from robosystems.operations.roboledger.commands.reports import _get_entity_id

    entity_id = _get_entity_id(session, graph_id)

  provenance = DocumentProvenance(
    document_id=document.id,
    section_id=body.section_id,
    content_hash=content_hash,
    asserted_by=created_by,
  )

  standing = session.execute(
    select(FactSet)
    .where(
      FactSet.structure_id == body.structure_id,
      FactSet.factset_type == "disclosure",
      # Actuals pin — disclosures have no scenario slices; defensive so a
      # future scenario producer can never alias the standing bind.
      FactSet.scenario_id.is_(None),
      FactSet.entity_id == entity_id,
      FactSet.period_start == body.period_start,
      FactSet.period_end == body.period_end,
    )
    .order_by(FactSet.created_at.desc())
    .limit(1)
  ).scalar_one_or_none()

  replaced = False
  if standing is None:
    standing = create_fact_set(
      session,
      id=generate_prefixed_ulid("fs"),
      structure_id=body.structure_id,
      period_start=body.period_start,
      period_end=body.period_end,
      factset_type="disclosure",
      entity_id=entity_id,
      provenance=provenance,
      created_by=created_by,
    )
    session.flush()
  else:
    existing = (
      session.execute(
        select(Fact).where(
          Fact.fact_set_id == standing.id,
          Fact.element_id == element.id,
        )
      )
      .scalars()
      .all()
    )
    for fact in existing:
      session.delete(fact)
    replaced = bool(existing)
    standing.provenance = provenance.model_dump(mode="json")

  fact = Fact(
    id=generate_prefixed_ulid("fact"),
    element_id=element.id,
    value=None,
    string_value=text_value,
    fact_type="Nonnumeric",
    value_type="inline",
    content_type="text/markdown",
    period_start=body.period_start,
    period_end=body.period_end,
    period_type="duration",
    entity_id=entity_id,
    structure_id=body.structure_id,
    fact_set_id=standing.id,
  )
  session.add(fact)
  session.flush()

  return BindTextBlockResponse(
    fact_id=fact.id,
    fact_set_id=standing.id,
    structure_id=body.structure_id,
    element_id=element.id,
    document_id=document.id,
    section_id=body.section_id,
    content_hash=content_hash,
    characters=len(text_value),
    period_start=body.period_start,
    period_end=body.period_end,
    replaced=replaced,
  )
