"""One XBRL filing to graph parquet, on xbrlkit's model and projection.

The load and everything that reads the loaded filing are xbrlkit's — the
platform supplies the cache directory and its settings
(``client/arelle.py``); ``to_xbrl_model`` walks the
``ModelXbrl`` into the neutral ``XbrlModel`` and ``to_graph_tables`` projects
it into the property graph's rows with the platform's own ids, so a filing
projected by ``xbrlkit build --format lpg`` and a filing processed here are
the same rows. What runs on top of those rows stays here because it needs
the platform: text-block externalization to the CDN, semantic enrichment of
elements and structures, the schema-aware parquet writer, and association
classification.
"""

import gc
import os
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
from arelle.UrlUtil import IXDS_DOC_SEPARATOR, IXDS_SURROGATE
from xbrlkit.model import EntityIdentity, FilingMeta, XbrlModel
from xbrlkit.parse import to_xbrl_model
from xbrlkit.schema import NODE_TABLES, REL_TABLES
from xbrlkit.serialize.lpg import (
  XBRL_GRAPH_PROCESSOR_VERSION,
  GraphTables,
  to_graph_tables,
)

from robosystems.adapters.sec.client.arelle import close_filing, load_filing
from robosystems.adapters.sec.config import (
  XBRL_COLUMN_STANDARDIZATION,
  XBRL_EXTERNALIZATION_THRESHOLD,
  XBRL_EXTERNALIZE_LARGE_VALUES,
  XBRL_KEEP_TEXTBLOCKS_INLINE,
  XBRL_SEMANTIC_ENRICHMENT,
  XBRL_SKIP_TEXTBLOCK_FACTS,
  XBRL_STANDARDIZED_FILENAMES,
  XBRL_TYPE_PREFIXES,
)
from robosystems.adapters.sec.processors.dataframe import DataFrameManager
from robosystems.adapters.sec.processors.parquet import ParquetWriter
from robosystems.adapters.sec.processors.schema import (
  XBRLSchemaAdapter,
  XBRLSchemaConfigGenerator,
)
from robosystems.adapters.sec.processors.textblock import TextBlockExternalizer
from robosystems.config import env
from robosystems.logger import logger
from robosystems.operations.aws.s3 import S3Client

__all__ = ["XBRL_GRAPH_PROCESSOR_VERSION", "XBRLGraphProcessor"]


class XBRLGraphProcessor:
  """Process one filing into the graph's parquet files under ``output_dir``.

  ``sec_filer`` and ``sec_report`` are the EDGAR submissions header and the
  filing's record from it, as ``SECMetadataLoader`` returns them; they become
  the model's ``EntityIdentity`` and ``FilingMeta``. ``report_uri`` is the
  primary document's EDGAR URL, the stem every report-scoped id is folded on.
  """

  def __init__(
    self,
    report_uri,
    entityId=None,
    sec_filer=None,
    sec_report=None,
    output_dir="./data/output",
    schema_config=None,
    local_file_path=None,
    enricher=None,
  ):
    logger.debug(f"Initializing XBRL processor for report URI: {report_uri}")
    self.report_uri = report_uri
    self.local_file_path = local_file_path
    self.entityId = entityId
    self.sec_filer = sec_filer
    self.sec_report = sec_report
    self.output_dir = Path(output_dir)
    self.version = XBRL_GRAPH_PROCESSOR_VERSION
    self.instance_path: str | None = None
    self.failed = False

    # Set by process(): the parsed model, and the filer's Entity row and the
    # Report row as the projection wrote them (the externalizer and the
    # classifier read the filing's coordinates from these).
    self.model: XbrlModel | None = None
    self.entity_data: dict[str, Any] | None = None
    self.report_data: dict[str, Any] | None = None

    s3_client = None
    if XBRL_EXTERNALIZE_LARGE_VALUES and env.PUBLIC_DATA_BUCKET:
      try:
        s3_client = S3Client()
      except Exception as e:
        logger.warning(f"Failed to initialize S3 client for externalization: {e}")

    self.textblock_externalizer = TextBlockExternalizer(
      s3_client=s3_client,
      bucket=env.PUBLIC_DATA_BUCKET,
      cdn_url=env.PUBLIC_DATA_CDN_URL,
      threshold=XBRL_EXTERNALIZATION_THRESHOLD,
      enabled=XBRL_EXTERNALIZE_LARGE_VALUES,
      keep_inline=XBRL_KEEP_TEXTBLOCKS_INLINE,
    )

    self.enable_standardized_filenames = XBRL_STANDARDIZED_FILENAMES
    self.enable_type_prefixes = XBRL_TYPE_PREFIXES
    self.enable_column_standardization = XBRL_COLUMN_STANDARDIZATION

    # Semantic enrichment (a shared enricher avoids reloading the model per filing)
    self.enable_semantic_enrichment = XBRL_SEMANTIC_ENRICHMENT
    self._enricher = enricher

    if not schema_config:
      raise ValueError(
        "Schema configuration is required for XBRL processing. "
        "Please provide a valid schema_config parameter."
      )

    logger.debug("Initializing schema adapters for schema-driven DataFrame creation")
    self.schema_adapter = XBRLSchemaAdapter(schema_config)
    self.schema_adapter.print_schema_summary()
    self.ingest_adapter = XBRLSchemaConfigGenerator(schema_config)

    self.df_manager = DataFrameManager(
      self.schema_adapter, self.ingest_adapter, self.enable_column_standardization
    )
    # The enrichment, classification and parquet steps read the tables as
    # plain attributes (self.elements_df, self.facts_df, …); bind each one
    # empty here and fill them from the projection in process().
    for df_attr_name, df in self.df_manager.initialize_all_dataframes().items():
      setattr(self, df_attr_name, df)
    self.schema_to_dataframe_mapping = (
      self.df_manager.create_dynamic_dataframe_mapping()
    )

    self.parquet_writer = ParquetWriter(
      self.output_dir,
      self.schema_adapter,
      self.ingest_adapter,
      self.df_manager,
      self.enable_standardized_filenames,
      self.enable_type_prefixes,
      self.enable_column_standardization,
      self.sec_filer,
      self.sec_report,
    )

    logger.debug(
      f"XBRL processor initialized with version {self.version} "
      f"for output directory {self.output_dir}"
    )

  def process(self):
    """Load, parse, project, enrich and write the filing.

    A filing whose instance document cannot be found is marked ``failed`` and
    produces no parquet, so no phantom relationships reach the graph; the
    caller sees the missing ``nodes/Fact`` table and records the error.
    """
    logger.info(f"Starting XBRL processing for report: {self.report_uri}")

    self.instance_path = self._resolve_instance_path()
    if self.instance_path is None:
      self.failed = True
      logger.warning("Not outputting parquet files for failed report")
      return

    model_xbrl = None
    try:
      logger.debug("Loading the filing through Arelle")
      model_xbrl = load_filing(self.instance_path)

      logger.info("Parsing the filing into the xbrlkit model")
      model = to_xbrl_model(
        model_xbrl, self._filing_meta(), entity=self._entity_identity()
      )
      if XBRL_SKIP_TEXTBLOCK_FACTS:
        model = _without_textblock_facts(model)
      self.model = model

      logger.info("Projecting the model into graph tables")
      self._bind_tables(to_graph_tables(model))

      self._externalize_large_values()

      if self.enable_semantic_enrichment:
        logger.info("Computing semantic enrichments")
        self.enrich_dataframes()

      logger.info("Outputting parquet files")
      self.output_parquet_files()

      self.classify_associations()

      logger.info("XBRL processing completed successfully")
    except Exception as e:
      logger.error(f"Error processing XBRL: {e}")
      import traceback

      logger.error(f"Traceback: {traceback.format_exc()}")
      raise
    finally:
      # ModelXbrl and the controller accumulate memory across filings.
      close_filing(model_xbrl)
      # Release Arelle's C extension objects.
      gc.collect()

  async def process_async(self):
    """Async version of process method for use in async contexts."""
    logger.info(f"Starting async XBRL processing for report: {self.report_uri}")

    # Nothing here awaits — the whole pipeline is synchronous DataFrame work.
    self.process()

  def output_parquet_files(self):
    """Write every DataFrame to ``nodes/`` and ``relationships/``."""
    self.parquet_writer.write_all_dataframes(self.schema_to_dataframe_mapping, self)

  def classify_associations(self):
    """Classify associations using Cypher pattern detection on temp embedded LadybugDB.

    Runs after parquet output. Loads the filing's parquets into a temporary
    LadybugDB, detects structural patterns (RollUp, RollForward, etc.),
    and writes Classification nodes + relationships as additional parquets.
    """
    from robosystems.adapters.sec.config import XBRL_ASSOCIATION_CLASSIFICATION

    if not XBRL_ASSOCIATION_CLASSIFICATION:
      return

    try:
      from robosystems.adapters.sec.processors.classify import (
        AssociationClassifier,
        FilingMeta,
      )

      classifier = AssociationClassifier()
      # Source filing coordinates from the enriched report/entity metadata the
      # processor already holds — classify can't read them from its
      # identifier-only Report table. Feeds FactSet `filed` provenance +
      # REPORT_HAS_FACT_SET edges.
      filing_meta = FilingMeta(
        report_id=self.report_data.get("identifier") if self.report_data else None,
        accession=self.report_data.get("accession_number")
        if self.report_data
        else None,
        filing_date=self.report_data.get("filing_date") if self.report_data else None,
        form=self.report_data.get("form") if self.report_data else None,
        filer_cik=self.entity_data.get("cik") if self.entity_data else None,
      )
      result = classifier.classify(self.output_dir, filing_meta)

      if not result.classifications_df.empty:
        self.parquet_writer.write_dataframe(
          result.classifications_df, "nodes/Classification.parquet"
        )
        self.parquet_writer.write_dataframe(
          result.assoc_classifications_df,
          "relationships/ASSOCIATION_HAS_CLASSIFICATION.parquet",
        )
        logger.info(
          f"Wrote {len(result.classifications_df)} association classifications"
        )

      # Write structure-level FactSets
      if not result.factsets_df.empty:
        self.parquet_writer.write_dataframe(result.factsets_df, "nodes/FactSet.parquet")
        self.parquet_writer.write_dataframe(
          result.structure_factset_rels_df,
          "relationships/STRUCTURE_HAS_FACT_SET.parquet",
        )
        self.parquet_writer.write_dataframe(
          result.factset_fact_rels_df,
          "relationships/FACT_SET_CONTAINS_FACT.parquet",
        )
        if not result.report_factset_rels_df.empty:
          self.parquet_writer.write_dataframe(
            result.report_factset_rels_df,
            "relationships/REPORT_HAS_FACT_SET.parquet",
          )
        logger.info(
          f"Wrote {len(result.factsets_df)} structure FactSets "
          f"with {len(result.factset_fact_rels_df)} fact links"
        )

      # Apply disclosure-root canonical hints to elements
      if (
        result.canonical_hints
        and hasattr(self, "elements_df")
        and not self.elements_df.empty
      ):
        upgraded = 0
        for elem_id, (concept_id, confidence) in result.canonical_hints.items():
          mask = self.elements_df["identifier"] == elem_id
          if not mask.any():
            continue
          current_conf = self.elements_df.loc[mask, "canonical_confidence"].iloc[0]
          if current_conf is None or pd.isna(current_conf) or current_conf < confidence:
            self.elements_df.loc[mask, "canonical_concept"] = concept_id
            self.elements_df.loc[mask, "canonical_confidence"] = confidence
            upgraded += 1
        if upgraded:
          self.parquet_writer.write_dataframe(self.elements_df, "nodes/Element.parquet")
          logger.info(
            f"Upgraded {upgraded} elements with disclosure-root canonical concepts"
          )
    except Exception as e:
      # Classification is non-critical — log and continue
      logger.warning(f"Association classification failed (non-critical): {e}")

  def enrich_dataframes(self):
    """Batch canonical enrichment of Element and Structure DataFrames.

    Embeddings are computed transiently to assign canonical concepts / types;
    no embedding vector is persisted. Labels are not enriched — they carry no
    canonical concept, so there is nothing for enrichment to assign."""
    from robosystems.adapters.sec.enrichment import (
      SemanticEnricher,
      camel_case_to_words,
      classify_structure_heuristic,
      compose_element_text,
      compose_structure_text,
      parse_structure_definition,
    )

    if self._enricher is None:
      self._enricher = SemanticEnricher()

    enricher = self._enricher

    # ----- Elements --------------------------------------------------------
    if hasattr(self, "elements_df") and not self.elements_df.empty:
      logger.info(f"Enriching {len(self.elements_df)} elements")

      # Column order must match schema: canonical_concept, canonical_confidence
      # LadybugDB COPY FROM uses positional matching, not column names
      for col in ("canonical_concept", "canonical_confidence"):
        if col not in self.elements_df.columns:
          self.elements_df[col] = None

      # Parse names and compose texts
      texts = []
      for _, row in self.elements_df.iterrows():
        parsed_name = camel_case_to_words(row.get("name", "") or "")
        text = compose_element_text(
          parsed_name,
          {
            "balance": row.get("balance"),
            "period_type": row.get("period_type"),
            "classification": row.get("classification"),
          },
        )
        texts.append(text)

      # Batch embed
      embeddings = enricher.embed_batch(texts)

      # Match canonical for each element
      canonical_concepts = []
      canonical_confidences = []
      for i, row in self.elements_df.iterrows():
        concept_id, confidence = enricher.match_canonical(
          embeddings[len(canonical_concepts)],
          {
            "qname": row.get("qname", ""),
            "period_type": row.get("period_type", ""),
            "balance": row.get("balance", ""),
          },
        )
        canonical_concepts.append(concept_id)
        canonical_confidences.append(confidence if concept_id else None)

      self.elements_df["canonical_concept"] = canonical_concepts
      self.elements_df["canonical_confidence"] = canonical_confidences

      matched = sum(1 for c in canonical_concepts if c is not None)
      logger.info(
        f"Element enrichment complete: {matched}/{len(self.elements_df)} matched to canonical concepts"
      )

    # Labels are deliberately skipped: they carry no canonical concept, and
    # nothing downstream consumes a label embedding.

    # ----- Structures ------------------------------------------------------
    if hasattr(self, "structures_df") and not self.structures_df.empty:
      logger.info(f"Enriching {len(self.structures_df)} structures")

      # Column order must match schema: canonical_type, canonical_confidence
      # LadybugDB COPY FROM uses positional matching, not column names
      for col in ("canonical_type", "canonical_confidence"):
        if col not in self.structures_df.columns:
          self.structures_df[col] = None

      # Re-parse definitions to fix potentially empty names
      texts = []
      parsed_names = []
      for _, row in self.structures_df.iterrows():
        _, _, parsed_name = parse_structure_definition(row.get("definition", ""))
        parsed_names.append(parsed_name)
        text = compose_structure_text(parsed_name, row.get("definition", ""))
        texts.append(text)

      # Fill in names the stored row left empty
      for idx, name in enumerate(parsed_names):
        if name and not self.structures_df.iloc[idx].get("name"):
          self.structures_df.iloc[idx, self.structures_df.columns.get_loc("name")] = (
            name
          )

      # Batch embed
      non_empty_mask = [bool(t.strip()) for t in texts]
      non_empty_texts = [t for t, m in zip(texts, non_empty_mask, strict=True) if m]

      if non_empty_texts:
        struct_embeddings = enricher.embed_batch(non_empty_texts)
        emb_iter = iter(struct_embeddings)
        all_embeddings = []
        for m in non_empty_mask:
          if m:
            all_embeddings.append(next(emb_iter))
          else:
            all_embeddings.append(None)
      else:
        all_embeddings = [None] * len(texts)

      # Pre-compute structure → element qnames mapping for graph refinement
      from robosystems.adapters.sec.config import XBRL_GRAPH_REFINEMENT

      structure_element_map: dict[str, list[str]] = {}
      structure_def_hashes: dict[str, str] = {}
      if XBRL_GRAPH_REFINEMENT and (
        hasattr(self, "structure_associations_df")
        and not self.structure_associations_df.empty
        and hasattr(self, "association_to_elements_df")
        and not self.association_to_elements_df.empty
        and hasattr(self, "elements_df")
        and not self.elements_df.empty
      ):
        import hashlib

        # Build association_id → element_qname mapping
        assoc_to_qname: dict[str, str] = {}
        elem_id_to_qname: dict[str, str] = {}
        for _, erow in self.elements_df.iterrows():
          eid = erow.get("identifier")
          eq = erow.get("qname")
          if eid and eq:
            elem_id_to_qname[eid] = eq

        for _, arow in self.association_to_elements_df.iterrows():
          assoc_id = arow.get("from")
          elem_id = arow.get("to")
          if assoc_id and elem_id and elem_id in elem_id_to_qname:
            assoc_to_qname[assoc_id] = elem_id_to_qname[elem_id]

        # Build structure_id → [element_qnames]
        for _, srow in self.structure_associations_df.iterrows():
          struct_id = srow.get("from")
          assoc_id = srow.get("to")
          if struct_id and assoc_id and assoc_id in assoc_to_qname:
            structure_element_map.setdefault(struct_id, []).append(
              assoc_to_qname[assoc_id]
            )

        # Pre-compute definition hashes for consensus lookup
        for _, row in self.structures_df.iterrows():
          struct_id = row.get("identifier")
          definition = row.get("definition", "") or ""
          if struct_id:
            structure_def_hashes[struct_id] = hashlib.md5(
              definition.encode()
            ).hexdigest()

      # Classify structures (heuristic first, then embedding fallback)
      # Statements use keyword heuristics + embeddings; Disclosures use
      # composition profiles from disclosure_mechanics training data
      canonical_types = []
      canonical_confidences = []

      for idx, row in self.structures_df.iterrows():
        name = parsed_names[len(canonical_types)]
        definition = row.get("definition", "")
        block_type = row.get("type")
        heuristic_type, heuristic_conf = classify_structure_heuristic(
          name, definition, block_type=block_type
        )
        if heuristic_type:
          canonical_types.append(heuristic_type)
          canonical_confidences.append(heuristic_conf)
        elif block_type != "Statement":
          # For Disclosure structures, attempt disclosure composition classification
          disc_type = None
          disc_conf = None
          if block_type == "Disclosure" and XBRL_GRAPH_REFINEMENT:
            struct_id = row.get("identifier")
            elements = structure_element_map.get(struct_id, []) if struct_id else []
            if elements:
              # DEI detection first (deterministic, high confidence)
              dei_type, dei_conf = enricher.detect_dei_structure(elements)
              if dei_type:
                disc_type, disc_conf = dei_type, dei_conf
              else:
                # Balance sheet rollup detection (deterministic)
                bs_type, bs_conf = enricher.detect_balance_sheet_rollup(elements)
                if bs_type:
                  disc_type, disc_conf = bs_type, bs_conf
                else:
                  # Disclosure composition classification (probabilistic fallback)
                  d_type, d_score = enricher.classify_disclosure_by_composition(
                    elements
                  )
                  if d_type and d_score >= 0.3:
                    disc_type, disc_conf = d_type, d_score
          canonical_types.append(disc_type)
          canonical_confidences.append(disc_conf)
        elif all_embeddings[len(canonical_types)] is not None:
          emb_type, emb_conf = enricher.match_structure_canonical(
            all_embeddings[len(canonical_types)]
          )
          canonical_types.append(emb_type)
          canonical_confidences.append(emb_conf if emb_type else None)
        else:
          canonical_types.append(None)
          canonical_confidences.append(None)

      # Apply graph-based refinement
      statement_types = {
        "income_statement",
        "balance_sheet",
        "cash_flow_statement",
        "equity_statement",
        "comprehensive_income",
      }

      if XBRL_GRAPH_REFINEMENT:
        for i, row in enumerate(self.structures_df.itertuples()):
          ct = canonical_types[i]
          cc = canonical_confidences[i]
          if cc is None:
            continue
          struct_id = row.identifier if hasattr(row, "identifier") else None
          if struct_id is None:
            continue
          elements = structure_element_map.get(struct_id, [])
          def_hash = structure_def_hashes.get(struct_id, "")

          if ct in statement_types:
            # Statement refinement (existing pipeline)
            refined_type, refined_conf = enricher.refine_structure_confidence(
              ct, cc, elements, def_hash
            )
          else:
            # Disclosure refinement
            refined_type, refined_conf = enricher.refine_disclosure_confidence(
              ct, cc, elements, def_hash
            )
          canonical_types[i] = refined_type
          canonical_confidences[i] = refined_conf

      # `all_embeddings` is used transiently above for canonical_type matching
      # (match_structure_canonical); the vector itself is not persisted.
      self.structures_df["canonical_type"] = canonical_types
      self.structures_df["canonical_confidence"] = canonical_confidences

      classified = sum(1 for c in canonical_types if c is not None)
      logger.info(
        f"Structure enrichment complete: {classified}/{len(self.structures_df)} classified"
      )

  # ---- the filing's coordinates ------------------------------------------

  def _resolve_instance_path(self) -> str | None:
    """The instance document to load, or None (logged) when it is missing.

    An inline XBRL document set arrives as a surrogate path joining its
    member files; each member is checked rather than the surrogate itself.
    """
    if self.local_file_path:
      path = self.local_file_path
      logger.info(f"Using local file path: {path}")
    elif self.report_uri.startswith("file://"):
      path = self.report_uri.removeprefix("file://")
    else:
      logger.error("No local file path provided and report_uri is not a file:// URL")
      return None

    if IXDS_DOC_SEPARATOR in path:
      members = path.partition(IXDS_SURROGATE)[2].split(IXDS_DOC_SEPARATOR)
      exists = bool(members) and all(os.path.exists(p) for p in members)
    else:
      exists = os.path.exists(path)

    if not exists:
      logger.error(f"XBRL instance file not found: {path}")
      return None
    return path

  def _normalized_cik(self) -> str:
    """The filer's CIK, zero-padded to ten digits.

    ``sec_filer["cik"]`` wins over ``entityId`` — the submissions header is
    the authoritative source — and either form (padded or not) yields the
    same identifier.
    """
    raw = (self.sec_filer or {}).get("cik") or self.entityId
    if raw is None or raw == "":
      raise ValueError(
        "XBRLGraphProcessor needs the filer's CIK (entityId or sec_filer['cik'])"
      )
    return str(raw).lstrip("0").zfill(10)

  def _filing_meta(self) -> FilingMeta:
    """The filing's ``FilingMeta`` from its EDGAR record.

    The fiscal context (``fiscal_year_focus`` and friends) is left for the
    parse to read off the DEI cover-page facts.
    """
    report = self.sec_report or {}
    acceptance = _text(report.get("acceptanceDateTime"))
    if acceptance and _iso_date(acceptance[:10], "acceptanceDateTime") is None:
      acceptance = None
    return FilingMeta(
      accession=_text(report.get("accessionNumber")) or "",
      cik=self._normalized_cik(),
      form=_text(report.get("form")),
      filing_date=_iso_date(report.get("filingDate"), "filingDate"),
      report_date=_iso_date(report.get("reportDate"), "reportDate"),
      acceptance_datetime=acceptance,
      is_inline_xbrl=bool(report.get("isInlineXBRL", False)),
      primary_document=_text(report.get("primaryDocument")),
      report_uri=self.report_uri,
    )

  def _entity_identity(self) -> EntityIdentity:
    """The filer's ``EntityIdentity`` from the EDGAR submissions header.

    Empty header strings are kept as written (the graph stores them as
    ``""``); only the website falls through to ``investorWebsite`` and then
    to null. The EIN is padded to nine digits by the projection.
    """
    filer = self.sec_filer or {}
    name = _text(filer.get("entity_name") or filer.get("name"))
    ein = filer.get("ein")
    return EntityIdentity(
      cik=self._normalized_cik(),
      name=name,
      legal_name=name,
      ein=None if ein is None or ein == "" else str(ein),
      ticker=_text(filer.get("ticker")),
      exchange=_text(filer.get("exchange")),
      sic=_text(filer.get("sic")),
      sic_description=_text(filer.get("sicDescription")),
      category=_text(filer.get("category")),
      state_of_incorporation=_text(filer.get("stateOfIncorporation")),
      fiscal_year_end=_text(filer.get("fiscalYearEnd")),
      entity_type=_text(filer.get("entityType")),
      website=_text(filer.get("website") or filer.get("investorWebsite")) or None,
      phone=_text(filer.get("phone")),
    )

  # ---- the projected rows ---------------------------------------------------

  def _bind_tables(self, tables: GraphTables) -> None:
    """Fill the per-table DataFrames from the projection's rows.

    Columns follow xbrlkit's schema order, which is the platform's (the two
    are asserted equal in ``tests/schemas``); a table the schema config does
    not declare is skipped.
    """
    for spec, rows in (
      *((t, tables.nodes[t.name]) for t in NODE_TABLES),
      *((t, tables.relationships[t.name]) for t in REL_TABLES),
    ):
      attr = self.schema_to_dataframe_mapping.get(spec.name)
      if attr is None:
        logger.debug(f"Schema config declares no {spec.name} table, skipping")
        continue
      setattr(self, attr, pd.DataFrame(rows, columns=list(spec.columns)))

    entities = tables.nodes["Entity"]
    reports = tables.nodes["Report"]
    self.entity_data = dict(entities[0]) if entities else None
    self.report_data = dict(reports[0]) if reports else None
    logger.info(
      "Projected "
      + ", ".join(f"{name}={count}" for name, count in tables.counts().items())
    )

  def _externalize_large_values(self) -> None:
    """Replace oversized or HTML fact values with their CDN URL.

    Uploads are queued while the Fact rows are rewritten and sent in one
    batch at the end; a value the externalizer cannot queue stays inline.
    """
    attr = self.schema_to_dataframe_mapping.get("Fact")
    externalizer = self.textblock_externalizer
    if attr is None or not externalizer.enabled:
      return
    facts: pd.DataFrame = getattr(self, attr)
    if facts.empty:
      return

    values: list[Any] = []
    value_types: list[str] = []
    content_types: list[str | None] = []
    externalized = 0
    for identifier, value in zip(facts["identifier"], facts["value"], strict=True):
      if value and externalizer.should_externalize(value):
        result = externalizer.queue_value_for_s3(
          value, identifier, self.entity_data, self.report_data
        )
        if result:
          values.append(result["stored_value"])
          value_types.append(result["value_type"])
          content_types.append(result["content_type"])
          externalized += 1
          continue
        logger.warning(f"Failed to queue large value for {identifier}, storing inline")
      values.append(value)
      value_types.append("inline")
      content_types.append(None)

    facts["value"] = values
    facts["value_type"] = value_types
    facts["content_type"] = content_types
    if externalized:
      logger.info(f"Externalized {externalized} large fact values")
    externalizer.process_batch_uploads()


def _without_textblock_facts(model: XbrlModel) -> XbrlModel:
  """The model minus its text-block facts (``XBRL_SKIP_TEXTBLOCK_FACTS``)."""
  kept = [
    fact
    for fact in model.facts
    if not (
      (concept := model.concepts.get(fact.concept_qname)) and concept.is_textblock
    )
  ]
  dropped = len(model.facts) - len(kept)
  if dropped:
    logger.info(f"Skipping {dropped} textblock facts (XBRL_SKIP_TEXTBLOCK_FACTS)")
  return model.model_copy(update={"facts": kept})


def _text(value: Any) -> str | None:
  return None if value is None else str(value)


def _iso_date(value: Any, field: str) -> date | None:
  """A ``YYYY-MM-DD`` EDGAR date, or None (logged) when it does not parse."""
  if value is None or value == "":
    return None
  try:
    return datetime.strptime(str(value), "%Y-%m-%d").date()
  except ValueError:
    logger.warning(f"Invalid {field} format: {value}")
    return None
