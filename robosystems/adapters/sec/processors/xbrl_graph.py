import gc
import os
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from arelle import XbrlConst

# Import from specific modules to avoid circular imports
from robosystems.adapters.sec.client.arelle import ArelleClient
from robosystems.adapters.sec.client.edgar import SEC_BASE_URL, SECClient
from robosystems.adapters.sec.config import (
  XBRL_COLUMN_STANDARDIZATION,
  XBRL_EXTERNALIZATION_THRESHOLD,
  XBRL_EXTERNALIZE_LARGE_VALUES,
  XBRL_SEMANTIC_ENRICHMENT,
  XBRL_SKIP_TEXTBLOCK_FACTS,
  XBRL_STANDARDIZED_FILENAMES,
  XBRL_TYPE_PREFIXES,
)
from robosystems.adapters.sec.processors.dataframe import DataFrameManager
from robosystems.adapters.sec.processors.ids import (
  create_dimension_id,
  create_element_id,
  create_entity_id,
  create_fact_id,
  create_label_id,
  create_period_id,
  create_reference_id,
  create_report_id,
  create_structure_id,
  create_taxonomy_id,
  create_unit_id,
  safe_concat,
)
from robosystems.adapters.sec.processors.parquet import ParquetWriter
from robosystems.adapters.sec.processors.schema import (
  XBRLSchemaAdapter,
  XBRLSchemaConfigGenerator,
)
from robosystems.adapters.sec.processors.textblock import TextBlockExternalizer
from robosystems.config import env
from robosystems.logger import logger
from robosystems.operations.aws.s3 import S3Client
from robosystems.utils import (
  ISO_8601_URI,
  ROLES_FILTERED,
)
from robosystems.utils.uuid import generate_uuid7

XBRL_GRAPH_PROCESSOR_VERSION = "1.0.0"


class XBRLGraphProcessor:
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
    self.report_uri = report_uri  # Keep original SEC URL for metadata
    self.local_file_path = local_file_path  # Local file for processing
    self.entityId = entityId
    self.sec_filer = sec_filer
    self.sec_report = sec_report
    self.output_dir = Path(output_dir)
    self.version = XBRL_GRAPH_PROCESSOR_VERSION
    self.instance_path = None
    self.entity_data = None
    self.report_data = None

    # Track which elements have been fully processed to avoid duplicate label/reference creation
    self.processed_elements = set()

    # Initialize TextBlockExternalizer for S3 externalization
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
    )

    # Feature flags for upstream simplification
    self.enable_standardized_filenames = XBRL_STANDARDIZED_FILENAMES
    self.enable_type_prefixes = XBRL_TYPE_PREFIXES
    self.enable_column_standardization = XBRL_COLUMN_STANDARDIZATION

    # Semantic enrichment (shared enricher preferred — avoids reloading model per filing)
    self.enable_semantic_enrichment = XBRL_SEMANTIC_ENRICHMENT
    self._enricher = enricher

    if (
      self.enable_standardized_filenames
      or self.enable_type_prefixes
      or self.enable_column_standardization
    ):
      logger.debug("XBRL Upstream Simplification Features Enabled:")
      logger.debug(f"  - Standardized Filenames: {self.enable_standardized_filenames}")
      logger.debug(f"  - Type Prefixes: {self.enable_type_prefixes}")
      logger.debug(f"  - Column Standardization: {self.enable_column_standardization}")

    # Initialize schema adapters for dynamic DataFrame creation and filename generation
    if schema_config:
      logger.debug("Initializing schema adapters for schema-driven DataFrame creation")
      self.schema_adapter = XBRLSchemaAdapter(schema_config)
      self.schema_adapter.print_schema_summary()

      self.ingest_adapter = XBRLSchemaConfigGenerator(schema_config)

      # Initialize DataFrame manager
      self.df_manager = DataFrameManager(
        self.schema_adapter, self.ingest_adapter, self.enable_column_standardization
      )

      # Initialize all DataFrames through the manager
      dataframes = self.df_manager.initialize_all_dataframes()

      # Set DataFrames as instance attributes for backward compatibility
      for df_attr_name, df in dataframes.items():
        setattr(self, df_attr_name, df)

      # Create dynamic DataFrame mapping
      self.schema_to_dataframe_mapping = (
        self.df_manager.create_dynamic_dataframe_mapping()
      )

      # Initialize Parquet writer
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
    else:
      raise ValueError(
        "Schema configuration is required for XBRL processing. "
        "Please provide a valid schema_config parameter."
      )

    logger.debug(
      f"XBRL processor initialized with version {self.version} for output directory {self.output_dir}"
    )

  def safe_concat(
    self, existing_df: pd.DataFrame, new_df: pd.DataFrame
  ) -> pd.DataFrame:
    """Safely concatenate DataFrames (delegates to xbrl.naming_utils)."""
    return safe_concat(existing_df, new_df)

  def process(self):
    """Process XBRL data and output to parquet files."""
    logger.info(f"Starting XBRL processing for report: {self.report_uri}")

    self.make_entity()
    self.make_report()

    if not self.report_data:
      logger.error("Report not found, skipping further processing")
      return

    # Use the local file path if provided, otherwise try to derive from report_uri
    if self.local_file_path:
      self.instance_path = self.local_file_path
      logger.info(f"Using local file path: {self.instance_path}")
    elif self.report_uri.startswith("file://"):
      self.instance_path = self.report_uri.replace("file://", "")
    else:
      logger.error("No local file path provided and report_uri is not a file:// URL")
      # Mark report as failed
      if hasattr(self, "report_data") and self.report_data:
        self.report_data["failed"] = True
        if hasattr(self, "reports_df") and not self.reports_df.empty:
          self.reports_df.loc[
            self.reports_df["identifier"] == self.report_data["identifier"], "failed"
          ] = True
      # Don't output parquet files for failed reports to avoid phantom relationships
      logger.warning(
        "Not outputting parquet files for failed report - no instance file"
      )
      return

    if not self.instance_path or not os.path.exists(self.instance_path):
      logger.error(f"XBRL instance file not found: {self.instance_path}")
      # Mark report as failed
      if hasattr(self, "report_data") and self.report_data:
        self.report_data["failed"] = True
        if hasattr(self, "reports_df") and not self.reports_df.empty:
          self.reports_df.loc[
            self.reports_df["identifier"] == self.report_data["identifier"], "failed"
          ] = True
      # Don't output parquet files for failed reports to avoid phantom relationships
      logger.warning("Not outputting parquet files for failed report - file not found")
      return

    arelle_client = None
    try:
      logger.debug("Initializing Arelle controller")
      arelle_client = ArelleClient()
      self.arelle_cntlr = arelle_client.controller(self.instance_path)

      logger.info("Extracting DEI fiscal context from cover page")
      self.extract_dei_fiscal_info()

      logger.info("Processing DTS (Discoverable Taxonomy Set)")
      self.make_dts()

      logger.info("Processing facts")
      self.make_facts()

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
      raise e
    finally:
      # Critical: Clean up Arelle resources to prevent memory leaks
      # ModelXbrl and the controller accumulate memory across filings
      if self.arelle_cntlr is not None:
        try:
          self.arelle_cntlr.close()
        except Exception as e:
          logger.warning(f"Error closing ModelXbrl: {e}")
        self.arelle_cntlr = None
      if arelle_client is not None:
        try:
          arelle_client.close()
        except Exception as e:
          logger.warning(f"Error closing ArelleClient: {e}")
      # Force garbage collection to release Arelle's C extension objects
      gc.collect()

  async def process_async(self):
    """Async version of process method for use in async contexts."""
    logger.info(f"Starting async XBRL processing for report: {self.report_uri}")

    # Async version just calls the sync version since we're working with DataFrames
    self.process()

  def output_parquet_files(self):
    """Output all DataFrames to parquet files organized in nodes/ and relationships/ subdirectories."""
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
      from robosystems.adapters.sec.processors.classify import AssociationClassifier

      classifier = AssociationClassifier()
      result = classifier.classify(self.output_dir)

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

    Query embeddings are computed transiently to assign canonical concepts /
    types, but no embedding vector is persisted (the LanceDB semantic-element
    search was retired). Labels are no longer enriched at all — their
    embeddings were computed and then dropped at staging."""
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

    # Labels are intentionally not enriched — they carry no canonical concept,
    # and their embeddings (the only thing enrichment ever produced for them)
    # were dropped at staging. Retiring the vector index removes the last
    # reason to embed them.

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

      # Fix names that were empty from the old parser
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

  def make_entity(self):
    """Create the main entity (formerly entity) for this graph."""
    logger.debug(f"Creating entity data for ID: {self.entityId}")
    if not self.entityId:
      logger.warning("No entity ID provided")
      self.entity_data = None
      return None

    # Determine the authoritative CIK source:
    # 1. sec_filer.cik if available (most reliable source)
    # 2. Otherwise fall back to entityId
    raw_cik = self.entityId
    if self.sec_filer and self.sec_filer.get("cik"):
      raw_cik = self.sec_filer.get("cik")

    # Normalize CIK to 10-digit padded format for consistent identification
    # This ensures the same entity always gets the same identifier regardless of
    # whether the source data uses padded or unpadded CIK format
    # Strip leading zeros first, then pad to 10 digits (e.g., "320193" -> "0000320193")
    normalized_cik = str(raw_cik).lstrip("0").zfill(10)

    # Use canonical URI format for identifier generation - always consistent
    canonical_uri = f"http://www.sec.gov/CIK#{normalized_cik}"
    entity_identifier = create_entity_id(canonical_uri)

    entity_data = {
      "identifier": entity_identifier,  # Primary key - deterministic UUID5
      "uri": canonical_uri,  # Canonical SEC entity URI
      "scheme": "http://www.sec.gov/CIK",  # SEC CIK scheme
      "cik": normalized_cik,  # 10-digit padded CIK for consistent lookups
      "ticker": None,
      "name": None,
      "legal_name": None,
      "industry": None,
      "entity_type": None,
      "sic": None,
      "sic_description": None,
      "category": None,
      "state_of_incorporation": None,
      "fiscal_year_end": None,
      "tax_id": None,
      "website": None,
      "status": "active",
      "is_parent": True,  # This is the top-level entity for this graph
      "parent_entity_id": None,  # No parent for top-level entity
      "created_at": None,
      "updated_at": None,
    }

    if self.sec_filer:
      logger.info("Adding entity information from SEC filer data")
      # Use entity_name (from SEC submissions API) or fallback to name
      entity_name = self.sec_filer.get("entity_name") or self.sec_filer.get("name")
      entity_data["name"] = entity_name
      entity_data["legal_name"] = entity_name  # Use name as legal_name if not provided
      # Note: We keep the normalized CIK set above, don't overwrite with sec_filer's
      # potentially unpadded value to maintain consistent identification
      entity_data["ticker"] = self.sec_filer.get("ticker")
      entity_data["sic"] = self.sec_filer.get("sic")
      entity_data["sic_description"] = self.sec_filer.get("sicDescription")
      entity_data["category"] = self.sec_filer.get("category")
      entity_data["state_of_incorporation"] = self.sec_filer.get("stateOfIncorporation")
      entity_data["fiscal_year_end"] = self.sec_filer.get("fiscalYearEnd")
      # Ensure EIN/tax_id is properly formatted as a string with leading zeros
      ein_value = self.sec_filer.get("ein")
      if ein_value is not None and ein_value != "":
        # Convert to string and pad with zeros if needed (EINs are 9 digits)
        entity_data["tax_id"] = str(ein_value).zfill(9)
      else:
        entity_data["tax_id"] = None

      # Additional fields from submissions data
      entity_data["entity_type"] = self.sec_filer.get("entityType")  # operating, etc.
      entity_data["website"] = self.sec_filer.get("website") or self.sec_filer.get(
        "investorWebsite"
      )

      # Exchange information (if ticker exists)
      if self.sec_filer.get("exchange"):
        entity_data["exchange"] = self.sec_filer.get("exchange")

      # LEI (Legal Entity Identifier) if available
      if self.sec_filer.get("lei"):
        entity_data["lei"] = self.sec_filer.get("lei")

      # Phone number
      if self.sec_filer.get("phone"):
        entity_data["phone"] = self.sec_filer.get("phone")

      # Note: URI and scheme are already set with canonical format above,
      # no need to overwrite here

      # Map SIC to industry if available
      if entity_data["sic"]:
        entity_data["industry"] = entity_data["sic_description"]
      logger.info(
        f"Entity {entity_data['name']} data prepared with {sum(1 for v in entity_data.values() if v is not None)} populated fields"
      )

    # Add to entities DataFrame using schema adapter if available
    if self.schema_adapter:
      new_entity_df = self.schema_adapter.process_dataframe_for_schema(
        "Entity", entity_data
      )
      logger.debug(
        f"Schema adapter created entity DataFrame with {len(new_entity_df.columns)} columns: {list(new_entity_df.columns)}"
      )
    else:
      new_entity_df = pd.DataFrame([entity_data])
    self.entities_df = self.safe_concat(self.entities_df, new_entity_df)

    self.entity_data = entity_data
    return entity_data

  def make_report(self):
    logger.debug(f"Creating report data: {self.report_uri}")

    report_id = create_report_id(self.report_uri)
    logger.debug(f"Creating new report with ID: {report_id}")

    # Include all fields from LadybugDB Report schema (exactly match LadybugSchemaBuilder)
    report_data = {
      "identifier": report_id,  # Primary key - UUIDv7
      "uri": self.report_uri,
      "name": None,
      "accession_number": None,
      "form": None,
      "filing_date": None,
      "report_date": None,  # Period end date for the report
      "acceptance_date": None,
      "is_inline_xbrl": False,
      "xbrl_processor_version": XBRL_GRAPH_PROCESSOR_VERSION,
      "processed": False,
      "failed": False,
      # Fiscal context - populated later from DEI cover page facts
      "fiscal_year_focus": None,
      "fiscal_period_focus": None,
      "fiscal_year_end_month": None,
    }

    if self.sec_report:
      logger.info("Adding report information from SEC report data")
      report_data["name"] = self.sec_report.get("form")
      report_data["accession_number"] = self.sec_report.get("accessionNumber")
      if self.sec_report.get("filingDate"):
        try:
          report_data["filing_date"] = datetime.strptime(
            self.sec_report["filingDate"], "%Y-%m-%d"
          ).strftime("%Y-%m-%d")  # Convert to string format
        except ValueError:
          logger.warning(f"Invalid filingDate format: {self.sec_report['filingDate']}")
          report_data["filing_date"] = None
      if self.sec_report.get("reportDate"):
        try:
          report_data["report_date"] = datetime.strptime(
            self.sec_report["reportDate"], "%Y-%m-%d"
          ).strftime("%Y-%m-%d")  # Convert to string format
        except ValueError:
          logger.warning(f"Invalid reportDate format: {self.sec_report['reportDate']}")
          report_data["report_date"] = None
      report_data["form"] = self.sec_report.get("form")

      # Add acceptance_date if available
      if self.sec_report.get("acceptanceDateTime"):
        try:
          report_data["acceptance_date"] = datetime.strptime(
            self.sec_report["acceptanceDateTime"][:10],
            "%Y-%m-%d",  # Take just date part
          ).strftime("%Y-%m-%d")  # Convert to string format
        except ValueError:
          logger.warning(
            f"Invalid acceptanceDateTime format: {self.sec_report['acceptanceDateTime']}"
          )
          report_data["acceptance_date"] = None

      report_data["is_inline_xbrl"] = self.sec_report.get("isInlineXBRL", False)
      logger.info(f"Report {report_data['name']} data prepared")

    # Add to reports DataFrame using schema adapter if available
    if self.schema_adapter:
      new_report_df = self.schema_adapter.process_dataframe_for_schema(
        "Report", report_data
      )
      logger.debug(
        f"Schema adapter created report DataFrame with {len(new_report_df.columns)} columns: {list(new_report_df.columns)}"
      )
    else:
      new_report_df = pd.DataFrame([report_data])
    self.reports_df = self.safe_concat(self.reports_df, new_report_df)

    # Add entity-report relationship if entity exists
    if self.entity_data:
      logger.debug("Creating entity-report relationship")
      entity_report_rel = {
        "from": self.entity_data["identifier"],
        "to": report_data["identifier"],
      }
      if self.schema_adapter:
        new_entity_report_df = self.schema_adapter.process_dataframe_for_schema(
          "ENTITY_HAS_REPORT", entity_report_rel
        )
      else:
        new_entity_report_df = pd.DataFrame([entity_report_rel])
      self.entity_reports_df = self.safe_concat(
        self.entity_reports_df, new_entity_report_df
      )

    logger.debug("Report data creation completed")
    self.report_data = report_data

    # Note: instance_path will be set from report_uri in the main process method

  def extract_dei_fiscal_info(self):
    """Extract fiscal context from DEI cover page facts in the Arelle model.

    This method parses through the in-memory XBRL facts to find DEI elements
    that provide fiscal year/period context. Must be called after Arelle is initialized.

    Extracts:
    - dei:DocumentFiscalYearFocus -> fiscal_year_focus (e.g., 2024)
    - dei:DocumentFiscalPeriodFocus -> fiscal_period_focus (e.g., "Q3", "FY")
    - dei:CurrentFiscalYearEndDate -> fiscal_year_end_month (e.g., 12 for December)
    """
    if not self.arelle_cntlr:
      logger.warning("Arelle not initialized, cannot extract DEI fiscal info")
      return

    fiscal_year_focus = None
    fiscal_period_focus = None
    fiscal_year_end_month = None

    for xfact in self.arelle_cntlr.facts:
      if xfact.concept is None or xfact.concept.qname is None:
        continue

      qname_str = str(xfact.concept.qname)

      if qname_str == "dei:DocumentFiscalYearFocus":
        try:
          fiscal_year_focus = int(xfact.value)
          logger.debug(f"Extracted fiscal_year_focus: {fiscal_year_focus}")
        except (ValueError, TypeError):
          logger.warning(f"Could not parse fiscal year focus: {xfact.value}")

      elif qname_str == "dei:DocumentFiscalPeriodFocus":
        fiscal_period_focus = str(xfact.value) if xfact.value else None
        logger.debug(f"Extracted fiscal_period_focus: {fiscal_period_focus}")

      elif qname_str == "dei:CurrentFiscalYearEndDate":
        # Format is "--MM-DD" (e.g., "--12-31" for December 31)
        try:
          value = str(xfact.value) if xfact.value else ""
          if value.startswith("--") and len(value) >= 5:
            fiscal_year_end_month = int(value[2:4])
            logger.debug(f"Extracted fiscal_year_end_month: {fiscal_year_end_month}")
        except (ValueError, TypeError):
          logger.warning(f"Could not parse fiscal year end date: {xfact.value}")

    # Update report_data and reports_df with fiscal info
    if self.report_data:
      self.report_data["fiscal_year_focus"] = fiscal_year_focus
      self.report_data["fiscal_period_focus"] = fiscal_period_focus
      self.report_data["fiscal_year_end_month"] = fiscal_year_end_month

      # Update the DataFrame row
      if hasattr(self, "reports_df") and not self.reports_df.empty:
        report_id = self.report_data["identifier"]
        self.reports_df.loc[
          self.reports_df["identifier"] == report_id, "fiscal_year_focus"
        ] = fiscal_year_focus
        self.reports_df.loc[
          self.reports_df["identifier"] == report_id, "fiscal_period_focus"
        ] = fiscal_period_focus
        self.reports_df.loc[
          self.reports_df["identifier"] == report_id, "fiscal_year_end_month"
        ] = fiscal_year_end_month

      logger.info(
        f"Report fiscal context: FY{fiscal_year_focus} {fiscal_period_focus} "
        f"(year-end month: {fiscal_year_end_month})"
      )

  def fetch_filing(self, cik, accno, is_inline_xbrl):
    logger.info(f"Fetching filing for CIK: {cik}, Accession Number: {accno}")
    long_accno = accno
    accno = long_accno.replace("-", "")
    filename = f"{long_accno}-xbrl.zip"
    xbrlzip_url = os.path.join(
      SEC_BASE_URL, "Archives/edgar/data", cik, accno, filename
    )
    logger.debug(f"XBRL zip URL: {xbrlzip_url}")

    s = SECClient(cik=cik)
    xbrl_zip = s.download_xbrlzip(xbrlzip_url)
    if xbrl_zip is None:
      logger.warning("XBRL zip not found, attempting to get largest XML file")
      filing_url = os.path.join(SEC_BASE_URL, "Archives/edgar/data", cik, accno)
      instance_url = s.get_largest_xml_file(filing_url)
      if instance_url is None:
        logger.error("Failed to get largest XML file")
        # Mark report as failed in DataFrame
        if hasattr(self, "report_data") and self.report_data is not None:
          self.report_data["failed"] = True
          # Update the DataFrame
          if "identifier" in self.report_data:
            self.reports_df.loc[
              self.reports_df["identifier"] == self.report_data["identifier"], "failed"
            ] = True
        raise ValueError(f"Failed to fetch a valid filing for {self.report_uri}")
      return instance_url
    else:
      schema_fn = None
      for f in xbrl_zip.namelist():
        if ".xsd" in f:
          schema_fn = f
      extract_dir = f"./data/input/{cik}/{accno}"
      logger.info(f"Extracting XBRL files to: {extract_dir}")
      xbrl_zip.extractall(extract_dir)
      if is_inline_xbrl:
        instance_fn = self.report_uri.split("/")[-1]
      else:
        if schema_fn:
          instance_fn = schema_fn.replace(".xsd", ".xml")
        else:
          raise ValueError("No schema file found in XBRL zip file")
      instance_path = os.path.join(extract_dir, instance_fn)
      logger.info(f"Instance file path: {instance_path}")
      return instance_path

  def make_dts(self):
    logger.info("Processing Discoverable Taxonomy Set (DTS)")
    if not self.arelle_cntlr:
      return
    for _, v in self.arelle_cntlr.namespaceDocs.items():
      if not v:
        continue

      document = v[0]
      document_path = document.filepathdir if document else None
      filing_path = (
        self.arelle_cntlr.modelDocument.filepathdir
        if self.arelle_cntlr and self.arelle_cntlr.modelDocument
        else None
      )

      taxonomy_namespace = document.targetNamespace
      logger.debug(f"Processing taxonomy namespace: {taxonomy_namespace}")

      if document_path == filing_path:
        logger.info(f"Found matching taxonomy URI: {taxonomy_namespace}")
        self.taxonomy_uri = taxonomy_namespace
        self.make_taxonomy()

  def make_facts(self):
    logger.debug("Processing facts")

    fact_count = 0
    for xfact in self.arelle_cntlr.facts:
      self.make_fact(xfact)
      fact_count += 1

    # Process batch S3 uploads if any were queued
    self.textblock_externalizer.process_batch_uploads()

    logger.info(f"Processed {fact_count} facts")

  def make_fact(self, xfact):
    fact_uri = f"{self.report_uri}#fact-{xfact.md5sum.value}"
    identifier = create_fact_id(fact_uri)
    logger.debug(f"Processing fact: {fact_uri}")

    # Skip facts with missing context (malformed XBRL)
    if xfact.context is None:
      logger.warning(f"Skipping fact with missing context: {fact_uri}")
      return

    # Skip textblock facts entirely if configured (saves storage for historical data)
    # This takes precedence over externalization - fact is not created at all
    if XBRL_SKIP_TEXTBLOCK_FACTS and xfact.concept and xfact.concept.isTextBlock:
      logger.debug(
        f"Skipping textblock fact (XBRL_SKIP_TEXTBLOCK_FACTS=true): {fact_uri}"
      )
      return

    # Check if fact already exists to prevent duplicates
    existing_fact = self.facts_df[self.facts_df["identifier"] == identifier]
    if not existing_fact.empty:
      logger.debug(f"Fact already exists, skipping duplicate: {fact_uri}")
      # Return early to avoid creating duplicate relationships
      return

    # Compute numeric value for easier analysis
    # Store the actual reported value (no decimals scaling) — Arelle already provides
    # the real number. The `decimals` attribute indicates precision, not a multiplier.
    numeric_value = None
    if xfact.unit is not None and xfact.value is not None:
      try:
        numeric_value = float(str(xfact.value))
      except (ValueError, TypeError):
        # If conversion fails, leave numeric_value as None
        pass

    # Process fact value - externalize if large
    fact_value = str(xfact.value) if xfact.value is not None else None
    value_type = "inline"  # Default to inline storage
    content_type = None

    # Check if value should be externalized
    if fact_value and self.textblock_externalizer.should_externalize(fact_value):
      logger.debug(
        f"Queueing large value ({len(fact_value)} bytes) for batch upload: {fact_uri}"
      )
      # Queue for batch upload instead of immediate upload
      external_result = self.textblock_externalizer.queue_value_for_s3(
        fact_value, identifier, self.entity_data, self.report_data
      )

      if external_result:
        # Use the expected URL (will be uploaded in batch)
        fact_value = external_result["url"]
        value_type = external_result["value_type"]
        content_type = external_result["content_type"]
        logger.debug(f"Queued for externalization: {fact_value}")
      else:
        logger.warning("Failed to queue large value, storing inline")

    # Determine dimensional qualifiers (segments, geography, etc.)
    # Facts without dimensions represent consolidated totals
    dimension_count = len(xfact.context.qnameDims)
    has_dimensions = dimension_count > 0

    fact_data = {
      "identifier": identifier,
      "uri": fact_uri,
      "value": fact_value,
      "numeric_value": numeric_value,  # NEW: Computed numeric value for calculations
      "fact_type": "Numeric" if xfact.unit is not None else "Nonnumeric",
      "decimals": xfact.decimals if xfact.unit is not None else None,
      "value_type": value_type,  # NEW: Indicates inline vs external storage
      "content_type": content_type,  # NEW: MIME type for externalized content
      "has_dimensions": has_dimensions,  # True if fact has dimensional breakdowns
      "dimension_count": dimension_count,  # Number of dimensions (0=consolidated, 1=single, 2+=complex)
    }

    logger.debug(f"Created new fact: {fact_uri}")

    # Add fact to DataFrame using schema adapter to ensure all columns are populated
    if self.schema_adapter:
      new_fact_df = self.schema_adapter.process_dataframe_for_schema("Fact", fact_data)
    else:
      new_fact_df = pd.DataFrame([fact_data])

    self.facts_df = self.safe_concat(self.facts_df, new_fact_df)

    # Connect fact to report
    if self.report_data:
      report_fact_rel = {
        "from": self.report_data["identifier"],
        "to": identifier,
      }
      new_report_fact_df = pd.DataFrame([report_fact_rel])
      self.report_facts_df = self.safe_concat(self.report_facts_df, new_report_fact_df)

    if xfact.unit is not None:
      logger.debug(f"Processing numeric fact with decimals: {fact_data['decimals']}")
      self.make_units(fact_data, xfact)
    else:
      logger.debug("Processing non-numeric fact")

    self.make_fact_dimensions(fact_data, xfact)
    self.make_entity_from_context(fact_data, xfact)
    self.make_concept(fact_data, xfact)
    self.make_period(fact_data, xfact)
    logger.debug(f"Completed processing fact: {fact_uri}")

  def make_units(self, fact_data, xfact):
    logger.debug("Processing units for fact")

    def make_unit_uri(measure):
      measure = str(measure)
      measure_spt = measure.split(":")
      if len(measure_spt) == 1:
        value = measure
        nsuri = xfact.unit.elementNamespaceURI
      else:
        prefix = measure_spt[0]
        value = measure_spt[1]
        nsuri = xfact.unit.nsmap[prefix]

      uri = f"{nsuri}#{value}"
      return measure, value, uri

    unit_data = None

    if xfact.unit.isSingleMeasure:
      measure, value, uri = make_unit_uri(xfact.unit.measures[0][0])
      logger.debug(f"Processing single measure unit: {uri}")

      # Make unit identifier global/idempotent (remove report-specific prefix)
      unit_identifier = create_unit_id(uri)

      # Check if unit already exists globally
      existing_unit = self.units_df[self.units_df["identifier"] == unit_identifier]
      if existing_unit.empty:
        unit_data = {
          "identifier": unit_identifier,
          "uri": uri,
          "measure": measure,
          "value": value,
          "numerator_uri": None,
          "denominator_uri": None,
        }

        # Use schema adapter to ensure all columns are populated
        if self.schema_adapter:
          new_unit_df = self.schema_adapter.process_dataframe_for_schema(
            "Unit", unit_data
          )
        else:
          new_unit_df = pd.DataFrame([unit_data])

        self.units_df = self.safe_concat(self.units_df, new_unit_df)
        logger.debug(f"Created new unit: {uri}")
      else:
        unit_data = existing_unit.iloc[0].to_dict()

    elif xfact.unit.isDivide:
      nummeasure, numval, numuri = make_unit_uri(xfact.unit.measures[0][0])
      denommeasure, denomval, denomuri = make_unit_uri(xfact.unit.measures[1][0])
      fraction_measure = f"{nummeasure}/{denommeasure}"
      fraction_value = f"{numval}/{denomval}"
      # Generate a proper URI for divided units instead of using None
      fraction_uri = f"{numuri}/{denomuri}"
      logger.debug(
        f"Processing divided unit: {fraction_measure} with URI: {fraction_uri}"
      )

      # Make divided unit identifier global/idempotent (remove report-specific prefix)
      unit_identifier = create_unit_id(fraction_uri)

      # Check if unit already exists globally
      existing_unit = self.units_df[self.units_df["identifier"] == unit_identifier]
      if existing_unit.empty:
        unit_data = {
          "identifier": unit_identifier,
          "uri": fraction_uri,  # Use generated URI instead of None
          "numerator_uri": numuri,
          "denominator_uri": denomuri,
          "measure": fraction_measure,
          "value": fraction_value,
        }

        # Use schema adapter to ensure all columns are populated
        if self.schema_adapter:
          new_unit_df = self.schema_adapter.process_dataframe_for_schema(
            "Unit", unit_data
          )
        else:
          new_unit_df = pd.DataFrame([unit_data])

        self.units_df = self.safe_concat(self.units_df, new_unit_df)
        logger.debug(f"Created new divided unit: {fraction_measure}")
      else:
        unit_data = existing_unit.iloc[0].to_dict()

    # Create fact-unit relationship
    if unit_data:
      # Use the identifier from unit_data which is now global/idempotent
      unit_identifier = unit_data.get("identifier")
      fact_unit_rel = {
        "from": fact_data["identifier"],
        "to": unit_identifier,
      }
      new_fact_unit_df = pd.DataFrame([fact_unit_rel])
      self.fact_units_df = self.safe_concat(self.fact_units_df, new_fact_unit_df)

  def make_fact_dimensions(self, fact_data, xfact):
    logger.debug("Processing fact dimensions")
    if len(xfact.context.qnameDims) == 0:
      logger.debug("No dimensions found for fact")
      return None

    for dim, mem in sorted(xfact.context.qnameDims.items()):
      axis_ns = dim.namespaceURI
      axis_uri = f"{axis_ns}#{dim.localName}"
      logger.debug(f"Processing dimension: {axis_uri}")

      axis_type = (
        "segment"
        if xfact.context.hasSegment
        else "scenario"
        if xfact.context.hasScenario
        else "unknown"
      )

      fact_dim_data = None
      fact_dim_identifier = None

      if mem.isExplicit:
        member_ns = mem.member.document.targetNamespace
        member_uri = f"{member_ns}#{mem.member.name}"
        logger.debug(f"Processing explicit member: {member_uri}")

        # Fact dimensions should be deterministic based on their axis and member
        fact_dim_uri = f"{self.report_uri}#dimension-{axis_uri}-{member_uri}"
        fact_dim_identifier = create_dimension_id(fact_dim_uri)

        # Check if fact dimension already exists
        if (
          hasattr(self, "dimensions_df")
          and not self.dimensions_df.empty
          and "axis_uri" in self.dimensions_df.columns
          and "member_uri" in self.dimensions_df.columns
          and "type" in self.dimensions_df.columns
        ):
          existing_fact_dim = self.dimensions_df[
            (self.dimensions_df["axis_uri"] == axis_uri)
            & (self.dimensions_df["member_uri"] == member_uri)
            & (self.dimensions_df["type"] == axis_type)
          ]
        else:
          existing_fact_dim = pd.DataFrame()  # Empty dataframe

        if existing_fact_dim.empty:
          fact_dim_data = {
            "identifier": fact_dim_identifier,
            "axis": dim.localName,
            "member": mem.member.name,
            "dimension_type": "xbrl_explicit",
            "axis_uri": axis_uri,
            "member_uri": member_uri,
            "type": axis_type,
            "is_explicit": True,
            "is_typed": False,
          }
          new_fact_dim_df = pd.DataFrame([fact_dim_data])
          if hasattr(self, "dimensions_df") and not self.dimensions_df.empty:
            self.dimensions_df = self.safe_concat(self.dimensions_df, new_fact_dim_df)
          else:
            self.dimensions_df = new_fact_dim_df
          logger.debug(f"Created new dimension: {member_uri}")

          # Create axis element if needed
          axis_element_data = self.make_element(mem.dimension)

          # Create member element if needed
          member_element_data = self.make_element(mem.member)

          # Create fact dimension to axis element relationship
          if axis_element_data:
            fact_dim_axis_rel = {
              "from": fact_dim_identifier,
              "to": axis_element_data["identifier"],
            }
            new_fact_dim_elem_df = pd.DataFrame([fact_dim_axis_rel])
            if (
              hasattr(self, "dimension_has_axis_element_rel_df")
              and not self.dimension_has_axis_element_rel_df.empty
            ):
              self.dimension_has_axis_element_rel_df = self.safe_concat(
                self.dimension_has_axis_element_rel_df, new_fact_dim_elem_df
              )
            else:
              self.dimension_has_axis_element_rel_df = new_fact_dim_elem_df

          # Create fact dimension to member element relationship
          if member_element_data:
            fact_dim_member_rel = {
              "from": fact_dim_identifier,
              "to": member_element_data["identifier"],
            }
            new_fact_dim_elem_df = pd.DataFrame([fact_dim_member_rel])
            if (
              hasattr(self, "dimension_has_member_element_rel_df")
              and not self.dimension_has_member_element_rel_df.empty
            ):
              self.dimension_has_member_element_rel_df = self.safe_concat(
                self.dimension_has_member_element_rel_df, new_fact_dim_elem_df
              )
            else:
              self.dimension_has_member_element_rel_df = new_fact_dim_elem_df
        else:
          fact_dim_identifier = existing_fact_dim.iloc[0]["identifier"]

      elif mem.isTyped:
        typed_member = mem.stringValue
        logger.debug(f"Processing typed member: {typed_member}")

        # Fact dimensions should be deterministic based on their axis and member value
        fact_dim_uri = f"{self.report_uri}#dimension-{axis_uri}-typed-{typed_member}"
        fact_dim_identifier = create_dimension_id(fact_dim_uri)

        # Check if fact dimension already exists
        if (
          hasattr(self, "dimensions_df")
          and not self.dimensions_df.empty
          and "axis_uri" in self.dimensions_df.columns
          and "member_uri" in self.dimensions_df.columns
          and "type" in self.dimensions_df.columns
        ):
          existing_fact_dim = self.dimensions_df[
            (self.dimensions_df["axis_uri"] == axis_uri)
            & (self.dimensions_df["member_uri"] == typed_member)
            & (self.dimensions_df["type"] == axis_type)
          ]
        else:
          existing_fact_dim = pd.DataFrame()  # Empty dataframe

        if existing_fact_dim.empty:
          fact_dim_data = {
            "identifier": fact_dim_identifier,
            "axis": dim.localName,
            "member": typed_member,
            "dimension_type": "xbrl_typed",
            "axis_uri": axis_uri,
            "member_uri": typed_member,
            "type": axis_type,
            "is_explicit": False,
            "is_typed": True,
          }
          new_fact_dim_df = pd.DataFrame([fact_dim_data])
          if hasattr(self, "dimensions_df") and not self.dimensions_df.empty:
            self.dimensions_df = self.safe_concat(self.dimensions_df, new_fact_dim_df)
          else:
            self.dimensions_df = new_fact_dim_df
          logger.debug(f"Created new typed dimension: {typed_member}")

          # Create axis element if needed
          axis_element_data = self.make_element(mem.dimension)

          # Create fact dimension to axis element relationship
          if axis_element_data:
            fact_dim_axis_rel = {
              "from": fact_dim_identifier,
              "to": axis_element_data["identifier"],
            }
            new_fact_dim_elem_df = pd.DataFrame([fact_dim_axis_rel])
            if (
              hasattr(self, "dimension_has_axis_element_rel_df")
              and not self.dimension_has_axis_element_rel_df.empty
            ):
              self.dimension_has_axis_element_rel_df = self.safe_concat(
                self.dimension_has_axis_element_rel_df, new_fact_dim_elem_df
              )
            else:
              self.dimension_has_axis_element_rel_df = new_fact_dim_elem_df
        else:
          fact_dim_identifier = existing_fact_dim.iloc[0]["identifier"]

      # Create fact to dimension relationship
      if fact_dim_identifier:
        fact_dim_rel = {
          "from": fact_data["identifier"],
          "to": fact_dim_identifier,
        }
        new_fact_dim_rel_df = pd.DataFrame([fact_dim_rel])

        # Validate columns before concatenation to prevent schema mismatches
        expected_columns = {"from", "to"}
        if (
          hasattr(self, "fact_has_dimension_rel_df")
          and not self.fact_has_dimension_rel_df.empty
        ):
          existing_columns = set(self.fact_has_dimension_rel_df.columns)
          if existing_columns != expected_columns:
            logger.error(
              f"fact_has_dimension_rel_df has wrong columns: {existing_columns}"
            )
            # Reset the DataFrame with correct schema
            self.fact_has_dimension_rel_df = pd.DataFrame(
              columns=list(expected_columns)
            )

        self.fact_has_dimension_rel_df = self.safe_concat(
          self.fact_has_dimension_rel_df, new_fact_dim_rel_df
        )

  def make_entity_from_context(self, fact_data, xfact):
    """Process entity information from XBRL context.

    This creates or links to entities found in XBRL contexts. These could be:
    - The main entity (if CIK matches the top-level entity after normalization)
    - Subsidiary entities (if different from main entity)
    """
    logger.debug("Processing entity from XBRL context for fact")
    entity_ns, entity_id = xfact.context.entityIdentifier
    logger.debug(f"Processing XBRL entity: {entity_ns}#{entity_id}")

    # Normalize entity_id if it looks like a CIK (numeric string)
    # This handles both "320193" and "0000320193" formats
    normalized_entity_id = entity_id
    if entity_id.isdigit():
      normalized_entity_id = entity_id.lstrip("0").zfill(10)

    # Check if this is the main entity or a subsidiary
    is_main_entity = False
    if self.entity_data:
      main_entity_cik = self.entity_data.get("cik")

      # Match by normalized CIK - both are now 10-digit padded
      if main_entity_cik and normalized_entity_id == main_entity_cik:
        is_main_entity = True

    if is_main_entity and self.entity_data:
      # Use the main entity identifier
      entity_identifier = self.entity_data["identifier"]
      logger.debug(f"Using main entity for CIK {normalized_entity_id}")
    else:
      # This is a subsidiary or different entity - create canonical URI
      canonical_uri = f"{entity_ns}#{normalized_entity_id}"
      entity_identifier = create_entity_id(canonical_uri)

      # Check if this subsidiary entity already exists
      existing_entity = self.entities_df[self.entities_df["uri"] == canonical_uri]
      if existing_entity.empty:
        entity_data = {
          "identifier": entity_identifier,  # Primary key - deterministic UUID5
          "uri": canonical_uri,
          "scheme": entity_ns,
          "cik": normalized_entity_id if normalized_entity_id.isdigit() else None,
          "name": entity_id,  # Keep original ID as name
          "is_parent": False,  # This is not the top-level entity
          "parent_entity_id": self.entity_data["identifier"]
          if self.entity_data
          else None,
          "entity_type": "subsidiary",
        }

        # Use schema adapter to ensure all columns are populated
        if self.schema_adapter:
          new_entity_df = self.schema_adapter.process_dataframe_for_schema(
            "Entity", entity_data
          )
        else:
          new_entity_df = pd.DataFrame([entity_data])

        self.entities_df = self.safe_concat(self.entities_df, new_entity_df)
        logger.debug(
          f"Created subsidiary entity: {canonical_uri} with ID: {entity_identifier}"
        )

    # Create fact-entity relationship
    fact_entity_rel = {
      "from": fact_data["identifier"],
      "to": entity_identifier,
    }
    new_fact_entity_df = pd.DataFrame([fact_entity_rel])
    self.fact_entities_df = self.safe_concat(self.fact_entities_df, new_fact_entity_df)

  def make_concept(self, fact_data, xfact):
    logger.debug("Processing concept for fact")
    if xfact.concept is None:
      logger.debug("Fact has no concept, skipping")
      return None
    concept_ns = xfact.concept.document.targetNamespace
    concept_uri = f"{concept_ns}#{xfact.concept.name}"
    logger.debug(f"Processing concept: {concept_uri}")

    element_data = self.make_element(xfact.concept)
    if element_data:
      logger.debug(f"Created element for concept: {concept_uri}")

      if fact_data and "identifier" in fact_data:
        # Create fact-element relationship (fact uses element)
        fact_element_rel = {
          "from": fact_data["identifier"],  # Fact HAS element
          "to": element_data["identifier"],
        }
        new_fact_element_df = pd.DataFrame([fact_element_rel])
        self.fact_elements_df = self.safe_concat(
          self.fact_elements_df, new_fact_element_df
        )

      return element_data

  def _validate_and_format_date(self, dt, description: str) -> str | None:
    """Validate and format a datetime to YYYY-MM-DD string.

    Some XBRL filings have malformed dates (e.g., '202-05-31' instead of '2022-05-31').
    This helper validates the date is reasonable and returns None for invalid dates.
    """
    try:
      date_str = dt.strftime("%Y-%m-%d")
      # Validate the formatted date has a 4-digit year (catches truncated years)
      if len(date_str) < 10 or not date_str[:4].isdigit():
        logger.warning(f"Malformed {description}: {date_str}")
        return None
      # Validate year is reasonable (1900-2100)
      year = int(date_str[:4])
      if year < 1900 or year > 2100:
        logger.warning(f"Unreasonable year in {description}: {date_str}")
        return None
      return date_str
    except Exception as e:
      logger.warning(f"Failed to format {description}: {e}")
      return None

  def make_period(self, fact_data, xfact):
    logger.debug("Processing period for fact")
    period_uri = None
    period_data = None

    if xfact.context.isInstantPeriod:
      instant_date = self._validate_and_format_date(
        xfact.context.instantDatetime - timedelta(1), "instant date"
      )
      if instant_date is None:
        return None  # Skip fact with invalid period
      period_uri = f"{ISO_8601_URI}#{instant_date}"
      logger.debug(f"Processing instant period: {period_uri}")

      # Make period identifier global/idempotent for deduplication
      period_identifier = create_period_id(period_uri)

      # Check if period already exists globally
      existing_period = self.periods_df[
        self.periods_df["identifier"] == period_identifier
      ]
      if existing_period.empty:
        # Compute calendar year and quarter
        instant_dt = datetime.strptime(instant_date, "%Y-%m-%d")
        calendar_year = instant_dt.year

        # Determine calendar quarter based on instant date month
        # Note: This is calendar quarter, NOT entity fiscal quarter
        instant_month = instant_dt.month
        if instant_month in [1, 2, 3]:
          calendar_quarter = "Q1"
        elif instant_month in [4, 5, 6]:
          calendar_quarter = "Q2"
        elif instant_month in [7, 8, 9]:
          calendar_quarter = "Q3"
        else:
          calendar_quarter = "Q4"

        period_data = {
          "identifier": period_identifier,
          "uri": period_uri,
          "start_date": None,  # NULL for instant periods
          "end_date": instant_date,  # Instant date stored in end_date
          "calendar_year": calendar_year,
          "calendar_quarter": calendar_quarter,
          "days_in_period": 0,  # 0 for instant (point-in-time)
          "period_type": "instant",
          "duration_type": None,  # Not applicable for instant periods
          "calendar_period_key": instant_date,  # For instants, just the date
        }
        new_period_df = pd.DataFrame([period_data])
        self.periods_df = self.safe_concat(self.periods_df, new_period_df)
        logger.debug(f"Created new instant period: {period_uri}")

    elif xfact.context.isStartEndPeriod:
      start_date = self._validate_and_format_date(
        xfact.context.startDatetime, "start date"
      )
      end_date = self._validate_and_format_date(
        xfact.context.endDatetime - timedelta(1), "end date"
      )
      if start_date is None or end_date is None:
        return None  # Skip fact with invalid period
      period_uri = f"{ISO_8601_URI}#{start_date}/{end_date}"
      logger.debug(f"Processing start-end period: {period_uri}")

      # Make period identifier global/idempotent for deduplication
      period_identifier = create_period_id(period_uri)

      # Check if period already exists globally
      existing_period = self.periods_df[
        self.periods_df["identifier"] == period_identifier
      ]
      if existing_period.empty:
        # Compute calendar year, quarter and duration analysis
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        calendar_year = end_dt.year
        days_in_period = (end_dt - start_dt).days + 1

        # Determine duration subtype based on day count
        is_quarterly = 80 <= days_in_period <= 100  # ~3 months
        is_semi_annual = 170 <= days_in_period <= 190  # ~6 months (YTD)
        is_nine_months = 260 <= days_in_period <= 280  # ~9 months (YTD)
        is_annual = 350 <= days_in_period <= 380  # ~1 year

        if is_quarterly:
          duration_type = "quarterly"
        elif is_semi_annual:
          duration_type = "semi_annual"
        elif is_nine_months:
          duration_type = "nine_months"
        elif is_annual:
          duration_type = "annual"
        else:
          duration_type = "other"

        # Determine calendar quarter based on end date and period type
        # Note: This is calendar quarter, NOT entity fiscal quarter
        calendar_quarter = None
        end_month = end_dt.month
        if is_quarterly:
          if end_month in [1, 2, 3]:
            calendar_quarter = "Q1"
          elif end_month in [4, 5, 6]:
            calendar_quarter = "Q2"
          elif end_month in [7, 8, 9]:
            calendar_quarter = "Q3"
          else:
            calendar_quarter = "Q4"
        elif is_semi_annual:
          if end_month in [4, 5, 6, 7]:
            calendar_quarter = "H1"
          else:
            calendar_quarter = "H2"
        elif is_nine_months:
          calendar_quarter = "M9"
        elif is_annual:
          calendar_quarter = "FY"

        # Generate calendar_period_key
        if is_annual:
          calendar_period_key = str(calendar_year)
        elif calendar_quarter:
          calendar_period_key = f"{calendar_year}{calendar_quarter}"
        else:
          calendar_period_key = f"{start_date}/{end_date}"

        period_data = {
          "identifier": period_identifier,
          "uri": period_uri,
          "start_date": start_date,
          "end_date": end_date,
          "calendar_year": calendar_year,
          "calendar_quarter": calendar_quarter,
          "days_in_period": days_in_period,
          "period_type": "duration",
          "duration_type": duration_type,
          "calendar_period_key": calendar_period_key,
        }
        new_period_df = pd.DataFrame([period_data])
        self.periods_df = self.safe_concat(self.periods_df, new_period_df)
        logger.debug(f"Created new start-end period: {period_uri}")

    elif xfact.context.isForeverPeriod:
      period_uri = f"{ISO_8601_URI}#Forever"
      logger.debug("Processing forever period")

      # Make period identifier global/idempotent for deduplication
      period_identifier = create_period_id(period_uri)

      # Check if period already exists globally
      existing_period = self.periods_df[
        self.periods_df["identifier"] == period_identifier
      ]
      if existing_period.empty:
        period_data = {
          "identifier": period_identifier,
          "uri": period_uri,
          "start_date": None,
          "end_date": None,
          "calendar_year": None,
          "calendar_quarter": None,
          "days_in_period": None,
          "period_type": "forever",
          "duration_type": None,  # Not applicable for forever periods
          "calendar_period_key": "forever",
        }
        new_period_df = pd.DataFrame([period_data])
        self.periods_df = self.safe_concat(self.periods_df, new_period_df)
        logger.debug("Created new forever period")
    else:
      # Fallback for unknown period types
      period_uri = f"{ISO_8601_URI}#Unknown"
      logger.warning(f"Unknown period type for fact, using fallback: {period_uri}")

      # Make period identifier global/idempotent
      report_id = self.report_data["identifier"] if self.report_data else "unknown"
      period_identifier = create_period_id(f"{report_id}#{period_uri}")

      existing_period = self.periods_df[
        self.periods_df["identifier"] == period_identifier
      ]
      if existing_period.empty:
        period_data = {
          "identifier": period_identifier,
          "uri": period_uri,
          "start_date": None,
          "end_date": None,
          "calendar_year": None,
          "calendar_quarter": None,
          "days_in_period": None,
          "period_type": "duration",
          "duration_type": "other",
          "calendar_period_key": "unknown",
        }
        new_period_df = pd.DataFrame([period_data])
        self.periods_df = self.safe_concat(self.periods_df, new_period_df)
        logger.debug("Created fallback unknown period")

    # Create fact-period relationship
    if period_uri:
      # Get the period identifier (global/idempotent)
      if period_data:
        period_identifier = period_data["identifier"]
      else:
        # For existing periods - use global identifier
        period_identifier = create_period_id(period_uri)

      fact_period_rel = {
        "from": fact_data["identifier"],
        "to": period_identifier,
      }
      new_fact_period_df = pd.DataFrame([fact_period_rel])
      self.fact_periods_df = self.safe_concat(self.fact_periods_df, new_fact_period_df)

  def make_taxonomy(self):
    if not hasattr(self, "taxonomy_uri") or not self.taxonomy_uri:
      logger.error("No taxonomy URI available")
      return

    logger.debug(f"Creating taxonomy for URI: {self.taxonomy_uri}")

    taxonomy_identifier = create_taxonomy_id(self.taxonomy_uri)

    # Check if taxonomy already exists
    existing_taxonomy = self.taxonomies_df[
      self.taxonomies_df["uri"] == self.taxonomy_uri
    ]
    if existing_taxonomy.empty:
      taxonomy_data = {"identifier": taxonomy_identifier, "uri": self.taxonomy_uri}
      new_taxonomy_df = pd.DataFrame([taxonomy_data])
      self.taxonomies_df = self.safe_concat(self.taxonomies_df, new_taxonomy_df)
      logger.debug(f"Created new taxonomy: {self.taxonomy_uri}")
      self.taxonomy_data = taxonomy_data
    else:
      # Use existing taxonomy data
      self.taxonomy_data = existing_taxonomy.iloc[0].to_dict()

    # Connect taxonomy to report
    if self.report_data:
      logger.debug("Connecting taxonomy to report")
      report_taxonomy_rel = {
        "from": self.report_data["identifier"],
        "to": taxonomy_identifier,
      }
      new_report_taxonomy_df = pd.DataFrame([report_taxonomy_rel])
      if (
        hasattr(self, "report_uses_taxonomy_df")
        and not self.report_uses_taxonomy_df.empty
      ):
        self.report_uses_taxonomy_df = self.safe_concat(
          self.report_uses_taxonomy_df, new_report_taxonomy_df
        )
      else:
        self.report_uses_taxonomy_df = new_report_taxonomy_df

    self.make_structures()
    logger.debug("Taxonomy creation completed")
    return self.taxonomy_data

  def make_structures(self):
    logger.info("Processing taxonomy structures")
    filing_roles = pd.DataFrame(
      data=[(k[0], k[1]) for k in self.arelle_cntlr.baseSets],
      columns=["arcrole", "linkrole"],
    )

    filing_roles = filing_roles[~filing_roles["linkrole"].isin(ROLES_FILTERED)]
    filing_roles = filing_roles.drop_duplicates(keep="first").dropna()
    logger.debug(f"Found {len(filing_roles)} filing roles to process")

    for _, r in filing_roles.iterrows():
      role_uri = r.linkrole
      arcrole = r.arcrole
      role_types = self.arelle_cntlr.roleTypes.get(role_uri)
      if not role_types:
        logger.warning(f"No role types found for {role_uri}, skipping")
        continue
      role = role_types[0]
      logger.debug(f"Processing role: {role_uri} with arcrole: {arcrole}")

      structure_uri = f"{self.taxonomy_uri}#{role.id}"

      # Check if structure already exists
      existing_structure = self.structures_df[
        self.structures_df["uri"] == structure_uri
      ]
      if existing_structure.empty:
        # Make structure identifier filing-specific using accession number to avoid cross-filing conflicts
        accession_number = (
          self.report_data.get("accession_number", "unknown")
          if self.report_data
          else "unknown"
        )
        # Use deterministic UUID for structures to allow deduplication
        # Structures are specific to each filing (accession number)
        structure_id = create_structure_id(
          f"structure:{accession_number}#{structure_uri}"
        )
        network_uri = role_uri
        definition = (
          role.definition if hasattr(role, "definition") and role.definition else ""
        )
        from robosystems.adapters.sec.enrichment import parse_structure_definition

        network_number, network_type, network_name = parse_structure_definition(
          definition
        )

        structure_data = {
          "identifier": structure_id,  # Put identifier first since it's the primary key
          "uri": structure_uri,
          "network_uri": network_uri,
          "definition": definition,
          "number": network_number,
          "type": network_type,
          "name": network_name,
        }
        new_structure_df = pd.DataFrame([structure_data])
        self.structures_df = self.safe_concat(self.structures_df, new_structure_df)
        logger.debug(f"Created new structure: {structure_uri} with ID: {structure_id}")

        # Connect structure to taxonomy
        if hasattr(self, "taxonomy_data") and self.taxonomy_data:
          structure_taxonomy_rel = {
            "from": structure_id,
            "to": self.taxonomy_data["identifier"],
          }
          new_structure_taxonomy_df = pd.DataFrame([structure_taxonomy_rel])
          self.structure_taxonomies_df = self.safe_concat(
            self.structure_taxonomies_df, new_structure_taxonomy_df
          )
      else:
        structure_data = existing_structure.iloc[0].to_dict()
        logger.debug(
          f"Using existing structure: {structure_uri} with ID: {structure_data.get('identifier', 'unknown')}"
        )

      self.make_associations(role_uri, arcrole, structure_data)

  def make_associations(self, role_uri, arcrole, structure_data):
    logger.debug(f"Processing associations for role: {role_uri}")
    role_rels = self.arelle_cntlr.relationshipSet(arcrole, role_uri, None, None)

    if not role_rels.rootConcepts:
      logger.debug("No root concepts found")
      return

    if (
      not hasattr(role_rels, "modelRelationshipsFrom")
      or not role_rels.modelRelationshipsFrom
    ):
      logger.debug("No model relationships found")
      return

    for ele, rel in role_rels.modelRelationshipsFrom.items():
      to_ele = ele.viewConcept
      parent_element_data = self.make_element(to_ele)
      logger.debug(
        f"Processing parent element: {getattr(to_ele, 'name', 'unknown') if to_ele is not None else 'unknown'}"
      )

      for r in rel:
        from_ele = r.viewConcept
        child_element_data = self.make_element(from_ele)
        logger.debug(
          f"Processing child element: {getattr(from_ele, 'name', 'unknown') if from_ele is not None else 'unknown'}"
        )

        # Create association data - random UUID (associations are snapshots, created once per processing)
        association_id = generate_uuid7()
        association_data = {
          "identifier": association_id,
          "arcrole": arcrole,
          "order_value": r.order * 1,
          "association_type": "Presentation"
          if arcrole == XbrlConst.parentChild
          else "Calculation"
          if arcrole == XbrlConst.summationItem
          else "Other",
          "weight": r.weight if arcrole == XbrlConst.summationItem else None,
          "root": to_ele in role_rels.rootConcepts,
          "preferred_label": r.preferredLabel if r.preferredLabel is not None else None,
        }

        new_association_df = pd.DataFrame([association_data])
        self.associations_df = self.safe_concat(
          self.associations_df, new_association_df
        )
        logger.debug(
          f"Created association between {getattr(to_ele, 'name', 'unknown') if to_ele is not None else 'unknown'} and {getattr(from_ele, 'name', 'unknown') if from_ele is not None else 'unknown'}"
        )

        # Create association relationships (from parent to child)
        if parent_element_data and child_element_data:
          # Association FROM element (parent in hierarchy)
          assoc_from_rel = {
            "from": association_id,
            "to": parent_element_data["identifier"],
          }
          new_assoc_from_df = pd.DataFrame([assoc_from_rel])
          self.association_from_elements_df = self.safe_concat(
            self.association_from_elements_df, new_assoc_from_df
          )

          # Association TO element (child in hierarchy)
          assoc_to_rel = {
            "from": association_id,
            "to": child_element_data["identifier"],
          }
          new_assoc_to_df = pd.DataFrame([assoc_to_rel])
          self.association_to_elements_df = self.safe_concat(
            self.association_to_elements_df, new_assoc_to_df
          )

        # Connect association to structure
        structure_assoc_rel = {
          "from": structure_data["identifier"],
          "to": association_id,
        }
        new_structure_assoc_df = pd.DataFrame([structure_assoc_rel])
        self.structure_associations_df = self.safe_concat(
          self.structure_associations_df, new_structure_assoc_df
        )

  def make_element(self, xconcept):
    if xconcept is None:
      logger.debug("Concept is None, skipping element creation")
      return None
    if xconcept.qname is None:
      logger.debug("Concept has no qname, skipping element creation")
      return None
    concept_ns = xconcept.document.targetNamespace
    concept_uri = f"{concept_ns}#{xconcept.name}"
    logger.debug(f"Processing element: {concept_uri}")

    # Make element identifier global/idempotent for deduplication
    # This allows the same element to be shared across all reports
    element_identifier = create_element_id(concept_uri)

    # Check if we've already fully processed this element (including labels and references)
    if element_identifier in self.processed_elements:
      logger.debug(
        f"Element already processed: {concept_uri}, skipping label/reference creation"
      )
      # Return the element data without creating duplicate labels/references
      # We need to reconstruct the element_data to return it
      qname_str = str(xconcept.qname)
      element_name = qname_str.split(":")[-1] if ":" in qname_str else qname_str
      return {
        "identifier": element_identifier,
        "uri": concept_uri,
        "qname": qname_str,
        "name": element_name,
      }

    # Extract element information for deduplication via COPY with IGNORE_ERRORS
    qname_str = str(xconcept.qname)
    # Extract simple name from qname for easier querying (Claude Opus recommendation)
    element_name = qname_str.split(":")[-1] if ":" in qname_str else qname_str

    element_data = {
      "identifier": element_identifier,
      "uri": concept_uri,
      "qname": qname_str,
      "name": element_name,  # NEW: Simple name for easier querying
      "period_type": xconcept.periodType,
      "type": xconcept.niceType,
      "balance": xconcept.balance,
      "is_abstract": xconcept.isAbstract,
      "is_dimension_item": xconcept.isDimensionItem,
      "is_domain_member": xconcept.isDomainMember,
      "is_hypercube_item": xconcept.isHypercubeItem,
      "is_integer": xconcept.isInteger,
      "is_numeric": xconcept.isNumeric,
      "is_shares": xconcept.isShares,
      "is_fraction": xconcept.isFraction,
      "is_textblock": xconcept.isTextBlock,
      "substitution_group": None,
      "item_type": None,
      "classification": None,
    }

    # Add element classification
    element_data = self.make_element_classification(element_data, xconcept)

    # Use schema adapter to ensure all columns are populated
    if self.schema_adapter:
      new_element_df = self.schema_adapter.process_dataframe_for_schema(
        "Element", element_data
      )
    else:
      new_element_df = pd.DataFrame([element_data])

    self.elements_df = self.safe_concat(self.elements_df, new_element_df)
    logger.debug(
      f"Created new element: {concept_uri} with global ID: {element_identifier}"
    )

    self.make_element_labels(element_data, xconcept)
    self.make_element_references(element_data, xconcept)

    # Mark this element as fully processed to avoid duplicate label/reference creation
    self.processed_elements.add(element_identifier)

    return element_data

  def make_element_classification(self, element_data, xconcept):
    logger.debug(f"Classifying element: {element_data['uri']}")
    subgrp_qname = None
    type_name = None

    if hasattr(xconcept, "substitutionGroupQname"):
      subgrp_qname = str(xconcept.substitutionGroupQname)
      subgrp_name = xconcept.substitutionGroupQname.localName
      subgrp_ns = xconcept.substitutionGroupQname.namespaceURI
      subgrp_uri = f"{subgrp_ns}#{subgrp_name}"
      element_data["substitution_group"] = subgrp_uri
      logger.debug(f"Set substitution group: {subgrp_uri}")

    if hasattr(xconcept, "typeQname"):
      type_name = xconcept.typeQname.localName
      type_ns = xconcept.typeQname.namespaceURI
      type_uri = f"{type_ns}#{type_name}"
      element_data["item_type"] = type_uri
      logger.debug(f"Set item type: {type_uri}")

    classification = None
    if (
      subgrp_qname == "xbrldt:hypercubeItem"
      and xconcept.periodType == "duration"
      and xconcept.abstract == "true"
    ):
      classification = "hypercubeElement"
    elif (
      subgrp_qname == "xbrldt:hypercubeItem"
      and xconcept.periodType == "instant"
      and xconcept.abstract == "true"
    ) or (
      subgrp_qname == "xbrldt:hypercubeItem"
      and xconcept.periodType == "duration"
      and xconcept.abstract == "true"
    ):
      classification = "dimensionElement"
    elif (
      subgrp_qname == "xbrli:item"
      and xconcept.periodType == "duration"
      and xconcept.abstract == "true"
    ):
      if type_name == "domainItemType" and xconcept.nillable == "true":
        classification = "domainElement"
      elif type_name == "domainItemType" and xconcept.nillable == "false":
        classification = "memberElement"
      elif str(xconcept.name)[-9:] == "LineItems":
        classification = "lineItemsElement"
      else:
        classification = "listItemsElement"

    if classification:
      element_data["classification"] = classification
      logger.debug(f"Set classification: {classification}")

    return element_data

  def make_element_labels(self, element_data, xconcept):
    logger.debug(f"Processing labels for element: {element_data['uri']}")
    label_rels = self.arelle_cntlr.relationshipSet(
      XbrlConst.conceptLabel
    ).fromModelObject(xconcept)
    for rel in label_rels:
      label_obj = rel.toModelObject
      label_lang = label_obj.xmlLang
      label_type = label_obj.role
      label_value = label_obj.text
      logger.debug(f"Processing label: {label_type} ({label_lang})")

      # Create label data with global/idempotent identifier
      label_identifier = create_label_id(label_value, label_type, label_lang)
      label_data = {
        "identifier": label_identifier,
        "value": label_value,
        "type": label_type,
        "language": label_lang,
      }

      # With global identifiers, labels can be deduplicated across reports
      new_label_df = pd.DataFrame([label_data])
      self.labels_df = self.safe_concat(self.labels_df, new_label_df)

      # Create element-label relationship
      element_label_rel = {
        "from": element_data["identifier"],
        "to": label_identifier,
      }
      new_element_label_df = pd.DataFrame([element_label_rel])
      self.element_labels_df = self.safe_concat(
        self.element_labels_df, new_element_label_df
      )

      # Create taxonomy-label relationship. On the SEC shared repo the taxonomy
      # is the filer's per-report extension taxonomy, so this edge is
      # report-scoped. Carry element_uri so "this report's label for element X"
      # is an exact lookup — the shared, content-addressed Label pool alone can't
      # distinguish which element a label belongs to. URI (not qname) keeps the
      # join exact for filer-extension elements. See sec-label-scoping spec.
      if hasattr(self, "taxonomy_data"):
        taxonomy_label_rel = {
          "from": self.taxonomy_data["identifier"],
          "to": label_identifier,
          "element_uri": element_data.get("uri"),
        }
        new_taxonomy_label_df = pd.DataFrame([taxonomy_label_rel])
        self.taxonomy_labels_df = self.safe_concat(
          self.taxonomy_labels_df, new_taxonomy_label_df
        )

  def make_element_references(self, element_data, xconcept):
    logger.debug(f"Processing references for element: {element_data['uri']}")
    ref_rels = self.arelle_cntlr.relationshipSet(
      XbrlConst.conceptReference
    ).fromModelObject(xconcept)
    for rel in ref_rels:
      ref_obj = rel.toModelObject
      ref_type = ref_obj.role
      for elt in ref_obj.iterchildren():
        ref_value = elt.stringValue
        logger.debug(f"Processing reference: {ref_type}")

        # Create reference data with global/idempotent identifier
        reference_identifier = create_reference_id(ref_value, ref_type)
        reference_data = {
          "identifier": reference_identifier,
          "value": ref_value,
          "type": ref_type,
        }

        # With global identifiers, references can be deduplicated across reports
        new_reference_df = pd.DataFrame([reference_data])
        self.references_df = self.safe_concat(self.references_df, new_reference_df)

        # Create element-reference relationship
        element_ref_rel = {
          "from": element_data["identifier"],
          "to": reference_identifier,
        }
        new_element_ref_df = pd.DataFrame([element_ref_rel])
        self.element_references_df = self.safe_concat(
          self.element_references_df, new_element_ref_df
        )

        # Create taxonomy-reference relationship
        if hasattr(self, "taxonomy_data"):
          taxonomy_ref_rel = {
            "from": self.taxonomy_data["identifier"],
            "to": reference_identifier,
          }
          new_taxonomy_ref_df = pd.DataFrame([taxonomy_ref_rel])
          self.taxonomy_references_df = self.safe_concat(
            self.taxonomy_references_df, new_taxonomy_ref_df
          )
