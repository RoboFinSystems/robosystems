from datetime import datetime, timedelta
import hashlib
import os
import re
import pandas as pd
from pathlib import Path
from arelle import XbrlConst
from robosystems.logger import logger
from robosystems.adapters.arelle import ArelleClient
from robosystems.adapters.sec import SEC_BASE_URL, SECClient
from robosystems.adapters.s3 import S3Client
from robosystems.utils.uuid import (
  generate_uuid7,
  generate_deterministic_uuid7,
)
from robosystems.utils import (
  ISO_8601_URI,
  ROLES_FILTERED,
)
from robosystems.processors.schema_processor import SchemaProcessor
from robosystems.processors.schema_ingestion import SchemaIngestionProcessor

XBRL_GRAPH_PROCESSOR_VERSION = "1.0.0"


# ============================================================================
# XBRL-Specific ID Creation Functions
# These use UUIDv7 for optimal database performance
# ============================================================================


def create_element_id(uri: str) -> str:
  """Create an Element identifier from URI."""
  return generate_deterministic_uuid7(uri, namespace="element")


def create_label_id(value: str, label_type: str, language: str) -> str:
  """Create a Label identifier from content."""
  content = f"{value}#{label_type}#{language}"
  return generate_deterministic_uuid7(content, namespace="label")


def create_taxonomy_id(uri: str) -> str:
  """Create a Taxonomy identifier from URI."""
  return generate_deterministic_uuid7(uri, namespace="taxonomy")


def create_reference_id(value: str, ref_type: str) -> str:
  """Create a Reference identifier."""
  content = f"{value}#{ref_type}"
  return generate_deterministic_uuid7(content, namespace="reference")


def create_report_id(uri: str) -> str:
  """Create a Report identifier from URI."""
  # Reports must have deterministic IDs based on URI for consistency across pipeline runs
  return generate_deterministic_uuid7(uri, namespace="report")


def create_fact_id(fact_uri: str) -> str:
  """Create a Fact identifier."""
  # Facts must have deterministic IDs based on URI for consistency across pipeline runs
  return generate_deterministic_uuid7(fact_uri, namespace="fact")


def create_entity_id(entity_uri: str) -> str:
  """Create an Entity identifier."""
  return generate_deterministic_uuid7(entity_uri, namespace="entity")


def create_period_id(period_uri: str) -> str:
  """Create a Period identifier."""
  return generate_deterministic_uuid7(period_uri, namespace="period")


def create_unit_id(unit_uri: str) -> str:
  """Create a Unit identifier."""
  return generate_deterministic_uuid7(unit_uri, namespace="unit")


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
    self.entity_data = None  # Backward compatibility
    self.report_data = None

    # Track which elements have been fully processed to avoid duplicate label/reference creation
    self.processed_elements = set()

    # Initialize S3 client for textblock externalization
    from robosystems.config import env

    self.s3_client = None
    self.sec_textblocks_bucket = env.PUBLIC_DATA_BUCKET
    self.sec_textblocks_cdn_url = env.PUBLIC_DATA_CDN_URL
    self.externalize_large_values = env.XBRL_EXTERNALIZE_LARGE_VALUES
    self.externalization_threshold = env.XBRL_EXTERNALIZATION_THRESHOLD

    if self.externalize_large_values and self.sec_textblocks_bucket:
      try:
        self.s3_client = S3Client()
        logger.info(
          f"S3 externalization enabled: bucket={self.sec_textblocks_bucket}, "
          f"threshold={self.externalization_threshold} bytes"
        )
      except Exception as e:
        logger.warning(f"Failed to initialize S3 client for externalization: {e}")
        self.externalize_large_values = False

    # Feature flags for upstream simplification
    self.enable_standardized_filenames = env.XBRL_STANDARDIZED_FILENAMES
    self.enable_type_prefixes = env.XBRL_TYPE_PREFIXES
    self.enable_column_standardization = env.XBRL_COLUMN_STANDARDIZATION

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
      self.schema_adapter = SchemaProcessor(schema_config)
      self.schema_adapter.print_schema_summary()  # Already uses debug logging

      # Also initialize ingestion adapter for filename generation
      self.ingest_adapter = SchemaIngestionProcessor(schema_config)
    else:
      logger.warning(
        "No schema config provided - falling back to manual DataFrame definitions"
      )
      self.schema_adapter = None
      self.ingest_adapter = None

    # Initialize DataFrames using schema adapter or fallback to manual definitions
    self._initialize_dataframes()

    # Create dynamic DataFrame mapping for schema-driven file saving
    if self.schema_adapter and self.ingest_adapter:
      self._create_dynamic_dataframe_mapping()

    logger.debug(
      f"XBRL processor initialized with version {self.version} for output directory {self.output_dir}"
    )

  def _initialize_dataframes(self):
    """Initialize all DataFrames using schema adapter dynamically from schema."""
    if self.schema_adapter:
      logger.debug("Initializing DataFrames dynamically using SchemaProcessor")

      try:
        # Use schema from the adapter which was initialized with the correct config
        schema_builder = self.schema_adapter.schema_builder
        schema = schema_builder.schema

        if not schema or not schema.nodes:
          logger.warning("No schema or nodes found in schema builder")
          return

        # Get node and relationship types from the configured schema
        node_types = [node.name for node in schema.nodes]
        for node_type in node_types:
          df_attr_name = self._convert_schema_name_to_dataframe_attr(
            node_type, is_node=True
          )
          try:
            df = self.schema_adapter.create_schema_compatible_dataframe(node_type)
            setattr(self, df_attr_name, df)
            logger.debug(f"Initialized DataFrame: {df_attr_name} for node {node_type}")
          except Exception as e:
            logger.error(f"Failed to create DataFrame for node {node_type}: {e}")
            raise ValueError(
              f"Failed to initialize DataFrame for node type '{node_type}': {e}. "
              f"Schema-based initialization is required."
            )

        # Initialize DataFrames for all relationship types
        if schema.relationships:
          relationship_types = [rel.name for rel in schema.relationships]
          for rel_type in relationship_types:
            df_attr_name = self._convert_schema_name_to_dataframe_attr(
              rel_type, is_node=False
            )
            try:
              df = self.schema_adapter.create_schema_compatible_dataframe(rel_type)
              setattr(self, df_attr_name, df)
              logger.debug(
                f"Initialized DataFrame: {df_attr_name} for relationship {rel_type}"
              )
            except Exception as e:
              logger.error(
                f"Failed to create DataFrame for relationship {rel_type}: {e}"
              )
              raise ValueError(
                f"Failed to initialize DataFrame for relationship type '{rel_type}': {e}. "
                f"Schema-based initialization is required."
              )

          logger.debug(
            f"All DataFrames initialized dynamically: {len(node_types)} nodes, {len(relationship_types)} relationships"
          )

        # Initialize additional DataFrames that represent specialized relationships
        # These may not follow standard schema naming patterns but are needed for XBRL dimension handling
        additional_relationship_dfs = {
          "fact_has_dimension_rel_df": "Relationship: fact -> dimension",
          "fact_dimension_axis_element_rel_df": "Relationship: dimension -> axis element",
          "fact_dimension_member_element_rel_df": "Relationship: dimension -> member element",
          "fact_set_contains_facts_df": "Relationship: fact set -> facts",
        }

        for df_name, description in additional_relationship_dfs.items():
          if not hasattr(self, df_name):
            # Try to create a proper DataFrame with schema
            try:
              # Extract the relationship type from the df_name and map to correct schema name
              # fact_has_dimension_rel_df -> FACT_HAS_DIMENSION
              if df_name == "fact_has_dimension_rel_df":
                schema_name = "FACT_HAS_DIMENSION"
              elif df_name == "fact_dimension_axis_element_rel_df":
                schema_name = "FACT_DIMENSION_AXIS_ELEMENT"
              elif df_name == "fact_dimension_member_element_rel_df":
                schema_name = "FACT_DIMENSION_MEMBER_ELEMENT"
              elif df_name == "fact_set_contains_facts_df":
                schema_name = "FACT_SET_CONTAINS_FACT"
              else:
                # Fallback to original logic for other cases
                rel_type = df_name.replace("_df", "").replace("_rel", "")
                parts = rel_type.split("_")
                schema_name = "".join(p.capitalize() for p in parts) + "Rel"

              df = self.schema_adapter.create_schema_compatible_dataframe(schema_name)

              setattr(self, df_name, df)
              logger.debug(
                f"Initialized additional relationship DataFrame: {df_name} ({description})"
              )
            except Exception:
              logger.warning(
                f"Could not create schema-based DataFrame for {df_name}, trying empty with proper columns"
              )
              # Create with minimal columns that are expected
              import pandas as pd

              if "fact_has_dimension_rel" in df_name:
                # FACT_HAS_DIMENSION relationship needs these exact columns
                df = pd.DataFrame(columns=["from", "to"])
              elif "axis_element" in df_name:
                df = pd.DataFrame(columns=["from", "to"])
              elif "member_element" in df_name:
                df = pd.DataFrame(columns=["from", "to"])
              elif "fact_set_contains" in df_name:
                df = pd.DataFrame(columns=["from", "to"])
              else:
                df = pd.DataFrame(columns=["from", "to"])
              setattr(self, df_name, df)
              logger.debug(
                f"Initialized {df_name} with basic columns: {list(df.columns)}"
              )

      except Exception as e:
        logger.error(f"Failed to initialize DataFrames dynamically: {e}")
        raise ValueError(
          f"Failed to initialize DataFrames with schema adapter: {e}. "
          f"Schema-based initialization is required for XBRL processing."
        )
    else:
      # No fallback - schema config is required
      raise ValueError(
        "Schema configuration is required for XBRL processing. "
        "No manual DataFrame fallback is supported. "
        "Please provide a valid schema_config parameter."
      )

  def _create_dynamic_dataframe_mapping(self):
    """Create mapping from schema names to DataFrame attributes for dynamic file saving."""
    # Generate mapping dynamically from schema
    self.schema_to_dataframe_mapping = {}

    if not self.schema_adapter:
      logger.warning("No schema adapter available for dynamic mapping")
      return

    try:
      # Use schema from the adapter which was initialized with the correct config
      schema_builder = self.schema_adapter.schema_builder
      schema = schema_builder.schema

      if not schema:
        logger.warning("No schema available for dynamic mapping")
        return

      # Map all node types dynamically
      if schema.nodes:
        for node in schema.nodes:
          node_type = node.name
          # Convert node name to DataFrame attribute name (PascalCase -> snake_case + _df)
          df_attr_name = self._convert_schema_name_to_dataframe_attr(
            node_type, is_node=True
          )

          # Check if we actually have this DataFrame attribute
          if hasattr(self, df_attr_name):
            self.schema_to_dataframe_mapping[node_type] = df_attr_name
            logger.debug(f"Mapped node {node_type} -> {df_attr_name}")
          else:
            logger.debug(
              f"DataFrame attribute {df_attr_name} not found for node {node_type}"
            )

      # Map all relationship types dynamically
      if schema.relationships:
        for rel in schema.relationships:
          rel_type = rel.name
          # Convert relationship name to DataFrame attribute name
          df_attr_name = self._convert_schema_name_to_dataframe_attr(
            rel_type, is_node=False
          )

          # Check if we actually have this DataFrame attribute
          if hasattr(self, df_attr_name):
            self.schema_to_dataframe_mapping[rel_type] = df_attr_name
            logger.debug(f"Mapped relationship {rel_type} -> {df_attr_name}")
          else:
            logger.debug(
              f"DataFrame attribute {df_attr_name} not found for relationship {rel_type}"
            )

      # Add explicit mappings for fact dimension relationships that may not be in the base schema
      additional_mappings = {
        "FACT_DIMENSION_AXIS_ELEMENT": "fact_dimension_axis_element_rel_df",
        "FACT_DIMENSION_MEMBER_ELEMENT": "fact_dimension_member_element_rel_df",
        "FACT_SET_CONTAINS_FACT": "fact_set_contains_facts_df",
      }

      for schema_name, df_attr in additional_mappings.items():
        if hasattr(self, df_attr):
          self.schema_to_dataframe_mapping[schema_name] = df_attr
          logger.debug(f"Added explicit mapping for {schema_name} -> {df_attr}")

      logger.debug(
        f"Dynamic DataFrame mapping created: {len(self.schema_to_dataframe_mapping)} mappings"
      )

    except Exception as e:
      logger.error(f"Failed to create dynamic DataFrame mapping: {e}")
      # Create minimal fallback mapping for core types only
      self.schema_to_dataframe_mapping = {
        "Entity": "entities_df",
        "Report": "reports_df",
        "Fact": "facts_df",
        "ENTITY_HAS_REPORT": "entity_reports_df",
        "REPORT_HAS_FACT": "report_facts_df",
      }
      logger.warning("Using fallback minimal DataFrame mapping")

  def _convert_schema_name_to_dataframe_attr(
    self, schema_name: str, is_node: bool
  ) -> str:
    """Convert schema name to DataFrame attribute name following current naming conventions."""
    if is_node:
      # Node naming: Entity -> entities_df, FactSet -> fact_sets_df
      # Convert PascalCase to snake_case, add plural and _df suffix
      snake_case = self._camel_to_snake(schema_name)
      plural = self._make_plural(snake_case)
      return f"{plural}_df"
    else:
      # Relationship naming: FACT_HAS_ELEMENT -> fact_elements_df
      # Special case for FACT_HAS_DIMENSION to avoid conflict with FactDimension node
      if schema_name == "FACT_HAS_DIMENSION":
        return "fact_has_dimension_rel_df"  # Use explicit relationship suffix to distinguish from node

      # Convert to snake_case and create descriptive name
      parts = schema_name.lower().split("_")
      if len(parts) >= 3 and parts[1] == "has":
        # Pattern: ENTITY_HAS_PROPERTY -> entity_properties_df
        entity = parts[0]
        property_name = "_".join(parts[2:])
        plural_property = self._make_plural(property_name)
        return f"{entity}_{plural_property}_df"
      else:
        # General pattern: convert to snake_case + _df
        snake_case = schema_name.lower()
        return f"{snake_case}_df"

  def _camel_to_snake(self, name: str) -> str:
    """Convert PascalCase to snake_case."""

    # Insert underscore before uppercase letters (except first)
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()

  def _make_plural(self, word: str) -> str:
    """Convert word to plural form following simple English rules."""
    if word.endswith("y"):
      return word[:-1] + "ies"
    elif word.endswith(("s", "x", "z", "ch", "sh")):
      return word + "es"
    else:
      return word + "s"

  def _convert_schema_name_to_filename(self, schema_name: str) -> str:
    """Convert schema name to appropriate filename using exact table names."""
    # Use exact schema names for file naming - no conversion to snake_case
    # This ensures directories and files match exact table names like Entity, FactDimension, etc.
    return f"{schema_name}.parquet"

  def safe_concat(
    self, existing_df: pd.DataFrame, new_df: pd.DataFrame
  ) -> pd.DataFrame:
    """Safely concatenate DataFrames, handling empty DataFrame warnings"""
    if new_df.empty:
      return existing_df
    if existing_df.empty:
      return new_df.copy()

    # Fix for pandas FutureWarning: explicitly handle dtypes when concatenating
    # Ensure consistent dtypes between DataFrames before concatenation
    for col in new_df.columns:
      if col in existing_df.columns:
        # Convert to common dtype if they differ
        if existing_df[col].dtype != new_df[col].dtype:
          # Use object dtype as fallback for mixed types
          common_dtype = (
            "object"
            if existing_df[col].dtype == "object" or new_df[col].dtype == "object"
            else existing_df[col].dtype
          )
          existing_df[col] = existing_df[col].astype(common_dtype)
          new_df[col] = new_df[col].astype(common_dtype)

    return pd.concat([existing_df, new_df], ignore_index=True, sort=False)

  def _should_externalize_value(self, value):
    """Determine if a value should be externalized to S3."""
    if not self.externalize_large_values or not value:
      return False

    value_str = str(value)

    # Check if it's HTML content (most important criteria)
    if "<" in value_str and ">" in value_str:
      return True

    # Check if it exceeds size threshold
    if len(value_str) > self.externalization_threshold:
      return True

    return False

  def _queue_value_for_s3(self, value, fact_id, report_data):
    """
    Queue large value for batch upload to S3 with content-based caching.

    Args:
      value: The value to externalize
      fact_id: The fact identifier (for unique S3 key)
      report_data: Report metadata for S3 path organization

    Returns:
      dict: Contains the expected external URL and metadata
    """
    if not self.s3_client or not self.sec_textblocks_bucket:
      logger.warning("S3 client not initialized, cannot externalize value")
      return None

    try:
      value_str = str(value)

      # Calculate content hash for caching
      content_hash = hashlib.sha256(value_str.encode()).hexdigest()

      # Check if this content was already uploaded (cache hit)
      if content_hash in self.s3_content_cache:
        cached_result = self.s3_content_cache[content_hash]
        logger.debug(
          f"Cache hit for content hash {content_hash[:8]}, reusing URL: {cached_result['url']}"
        )
        return cached_result

      # Determine content type
      content_type = (
        "text/html" if "<" in value_str and ">" in value_str else "text/plain"
      )
      file_extension = "html" if content_type == "text/html" else "txt"

      # Build S3 key using content hash for deduplication
      # Use content hash in filename to enable content-based deduplication
      s3_key = self._generate_s3_key_with_hash(
        content_hash, report_data, file_extension
      )

      # Check if this key already exists in S3 (persistent cache)
      if self._check_s3_object_exists(s3_key):
        logger.debug(
          f"S3 object already exists for content hash {content_hash[:8]}: {s3_key}"
        )
        # Object exists, just return the URL without re-uploading
        if self.sec_textblocks_cdn_url:
          external_url = f"{self.sec_textblocks_cdn_url}/{s3_key}"
        else:
          external_url = (
            f"https://{self.sec_textblocks_bucket}.s3.amazonaws.com/{s3_key}"
          )

        result = {
          "url": external_url,
          "value_type": "external",
          "content_type": content_type,
        }

        # Cache for future use in this session
        self.s3_content_cache[content_hash] = result
        return result

      # Queue for batch upload (new content)
      self.s3_upload_queue.append((value_str, self.sec_textblocks_bucket, s3_key))

      # Generate expected URL
      if self.sec_textblocks_cdn_url:
        external_url = f"{self.sec_textblocks_cdn_url}/{s3_key}"
      else:
        external_url = f"https://{self.sec_textblocks_bucket}.s3.amazonaws.com/{s3_key}"

      # Store mapping for later validation
      self.s3_upload_map[fact_id] = {
        "url": external_url,
        "key": s3_key,
        "content_type": content_type,
      }

      result = {
        "url": external_url,
        "value_type": "external",
        "content_type": content_type,
      }

      # Cache for future use in this session
      self.s3_content_cache[content_hash] = result

      return result

    except Exception as e:
      logger.error(f"Error queueing value for S3: {e}")
      return None

  def _process_batch_s3_uploads(self):
    """Process all queued S3 uploads in batch."""
    if not self.s3_upload_queue:
      return

    if not self.s3_client:
      logger.warning("S3 client not initialized, cannot process batch uploads")
      return

    logger.info(f"Starting batch upload of {len(self.s3_upload_queue)} items to S3")

    # Use batch upload method from S3 client
    results = self.s3_client.batch_upload_strings(
      items=self.s3_upload_queue,
      content_type=None,  # Content type varies per item
      max_workers=10,  # Parallel uploads
      max_retries=3,
    )

    # Log results
    successful = sum(1 for success in results.values() if success)
    failed = len(results) - successful

    if failed > 0:
      logger.warning(
        f"Batch S3 upload completed with {failed} failures out of {len(results)} total"
      )
      # Log failed keys for debugging
      for key, success in results.items():
        if not success:
          logger.error(f"Failed to upload: {key}")
    else:
      logger.info(f"Successfully uploaded all {successful} items to S3")

    # Clear the queue
    self.s3_upload_queue = []

  def _generate_s3_key(self, fact_id, report_data, file_extension):
    """Generate S3 key for externalized content."""
    # Extract metadata for S3 path
    year = None
    cik = None
    accession = None

    if report_data:
      # Extract year from filing date
      filing_date = report_data.get("filing_date")
      if filing_date:
        year = filing_date[:4]

      # Extract CIK from entity data if available
      if self.entity_data:
        cik = self.entity_data.get("cik")

      # Extract accession number
      accession = report_data.get("accession_number")

    # Fallback to generic path if metadata missing
    if not year:
      year = datetime.now().strftime("%Y")
    if not cik:
      cik = "unknown"
    if not accession:
      accession = "unknown"

    # Build S3 key
    # Shorten fact_id for readability
    fact_id_short = fact_id[:8] if len(fact_id) > 8 else fact_id
    s3_key = f"{year}/{cik}/{accession}/fact_{fact_id_short}.{file_extension}"

    return s3_key

  def _generate_s3_key_with_hash(self, content_hash, report_data, file_extension):
    """Generate S3 key using content hash for deduplication."""
    # Extract metadata for S3 path
    year = None
    cik = None
    accession = None

    if report_data:
      # Extract year from filing date
      filing_date = report_data.get("filing_date")
      if filing_date:
        year = filing_date[:4]

      # Extract CIK from entity data if available
      if self.entity_data:
        cik = self.entity_data.get("cik")

      # Extract accession number
      accession = report_data.get("accession_number")

    # Fallback to generic path if metadata missing
    if not year:
      year = datetime.now().strftime("%Y")
    if not cik:
      cik = "unknown"
    if not accession:
      accession = "unknown"

    # Build S3 key using content hash
    # Use first 12 chars of hash for reasonable uniqueness + readability
    content_hash_short = content_hash[:12]
    s3_key = f"{year}/{cik}/{accession}/fact_{content_hash_short}.{file_extension}"

    return s3_key

  def _check_s3_object_exists(self, s3_key):
    """Check if an S3 object already exists."""
    if not self.s3_client or not self.sec_textblocks_bucket:
      return False

    try:
      return self.s3_client.object_exists(self.sec_textblocks_bucket, s3_key)
    except Exception as e:
      logger.debug(f"Error checking S3 object existence: {e}")
      return False

  def _externalize_value_to_s3(self, value, fact_id, report_data):
    """
    Upload large value to S3 and return the CDN URL.
    (Legacy method - kept for backward compatibility)

    Args:
      value: The value to externalize
      fact_id: The fact identifier (for unique S3 key)
      report_data: Report metadata for S3 path organization

    Returns:
      dict: Contains the external URL and metadata
    """
    if not self.s3_client or not self.sec_textblocks_bucket:
      logger.warning("S3 client not initialized, cannot externalize value")
      return None

    try:
      value_str = str(value)

      # Determine content type
      content_type = (
        "text/html" if "<" in value_str and ">" in value_str else "text/plain"
      )
      file_extension = "html" if content_type == "text/html" else "txt"

      # Build S3 key using report metadata
      # Format: year/cik/accession/fact_{fact_id_short}.{ext}
      year = None
      cik = None
      accession = None

      if report_data:
        # Extract year from filing date
        filing_date = report_data.get("filing_date")
        if filing_date:
          year = filing_date[:4]

        # Extract CIK from entity data if available
        if self.entity_data:
          cik = self.entity_data.get("cik")

        # Extract accession number
        accession = report_data.get("accession_number")

      # Fallback to generic path if metadata missing
      if not year:
        year = datetime.now().strftime("%Y")
      if not cik:
        cik = "unknown"
      if not accession:
        accession = "unknown"

      # Use first 8 chars of fact_id for filename
      fact_id_short = fact_id[:8] if len(fact_id) > 8 else fact_id
      s3_key = f"{year}/{cik}/{accession}/fact_{fact_id_short}.{file_extension}"

      # Upload to S3
      logger.debug(
        f"Uploading large value to S3: s3://{self.sec_textblocks_bucket}/{s3_key}"
      )
      self.s3_client.upload_string(
        content=value_str,
        bucket=self.sec_textblocks_bucket,
        key=s3_key,
        content_type=content_type,
      )

      # Return the CDN URL if available, otherwise S3 URL
      if self.sec_textblocks_cdn_url:
        external_url = f"{self.sec_textblocks_cdn_url}/{s3_key}"
      else:
        external_url = f"s3://{self.sec_textblocks_bucket}/{s3_key}"

      return {
        "url": external_url,
        "value_type": "external_resource",
        "value_size": len(value_str),
        "content_type": content_type,
      }

    except Exception as e:
      logger.error(f"Failed to externalize value to S3: {e}")
      return None

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

    try:
      logger.debug("Initializing Arelle controller")
      self.arelle_cntlr = ArelleClient().controller(self.instance_path)

      logger.info("Processing DTS (Discoverable Taxonomy Set)")
      self.make_dts()

      logger.info("Processing facts")
      self.make_facts()

      logger.info("Outputting parquet files")
      self.output_parquet_files()
      logger.info("XBRL processing completed successfully")
    except Exception as e:
      logger.error(f"Error processing XBRL: {e}")
      import traceback

      logger.error(f"Traceback: {traceback.format_exc()}")
      raise e

  async def process_async(self):
    """Async version of process method for use in async contexts."""
    logger.info(f"Starting async XBRL processing for report: {self.report_uri}")

    # Async version just calls the sync version since we're working with DataFrames
    self.process()

  def output_parquet_files(self):
    """Output all DataFrames to parquet files organized in nodes/ and relationships/ subdirectories."""
    logger.debug(f"Creating output directory: {self.output_dir}")
    self.output_dir.mkdir(parents=True, exist_ok=True)

    # Create subdirectories for nodes and relationships
    nodes_dir = self.output_dir / "nodes"
    relationships_dir = self.output_dir / "relationships"
    nodes_dir.mkdir(parents=True, exist_ok=True)
    relationships_dir.mkdir(parents=True, exist_ok=True)

    logger.debug("Created nodes/ and relationships/ subdirectories")

    if self.schema_adapter and self.ingest_adapter:
      # Schema-driven approach: save all DataFrames using schema names and patterns
      logger.debug("Using schema-driven file output with proper directory organization")

      for schema_name, dataframe_attr in self.schema_to_dataframe_mapping.items():
        if hasattr(self, dataframe_attr):
          df = getattr(self, dataframe_attr)
          # Convert schema name to lowercase with underscores for consistent naming
          filename = self._convert_schema_name_to_filename(schema_name)
          # Use schema-driven save with correct table name and directory
          self._save_df_to_parquet_schema_driven(df, filename, schema_name)
          logger.debug(f"Saved {schema_name} -> {filename} ({len(df)} rows)")

    else:
      # Fallback approach (should not be reached due to required schema config)
      logger.warning(
        "Schema adapter not available, using fallback file output with directory organization"
      )

      # Node tables - save to nodes/ subdirectory with exact table names
      # Use exact schema table names as directory names
      self._save_df_to_parquet(self.entities_df, "nodes/Entity.parquet")
      self._save_df_to_parquet(self.reports_df, "nodes/Report.parquet")
      self._save_df_to_parquet(self.facts_df, "nodes/Fact.parquet")
      self._save_df_to_parquet(self.units_df, "nodes/Unit.parquet")
      self._save_df_to_parquet(self.fact_dimensions_df, "nodes/FactDimension.parquet")
      self._save_df_to_parquet(self.elements_df, "nodes/Element.parquet")
      self._save_df_to_parquet(self.labels_df, "nodes/Label.parquet")
      self._save_df_to_parquet(self.references_df, "nodes/Reference.parquet")
      self._save_df_to_parquet(self.structures_df, "nodes/Structure.parquet")
      self._save_df_to_parquet(self.associations_df, "nodes/Association.parquet")
      self._save_df_to_parquet(self.periods_df, "nodes/Period.parquet")
      self._save_df_to_parquet(self.taxonomies_df, "nodes/Taxonomy.parquet")
      self._save_df_to_parquet(self.fact_sets_df, "nodes/FactSet.parquet")
      # Note: TaxonomyLabel and TaxonomyReference are stored as Label and Reference
      # with relationships to Taxonomy nodes
      self._save_df_to_parquet(self.taxonomy_labels_df, "nodes/Label.parquet")
      self._save_df_to_parquet(self.taxonomy_references_df, "nodes/Reference.parquet")

      # Relationship tables - save to relationships/ subdirectory with exact table names
      self._save_df_to_parquet(
        self.entity_reports_df, "relationships/ENTITY_HAS_REPORT.parquet"
      )
      self._save_df_to_parquet(
        self.report_facts_df, "relationships/REPORT_HAS_FACT.parquet"
      )
      self._save_df_to_parquet(
        self.report_fact_sets_df, "relationships/REPORT_HAS_FACT_SET.parquet"
      )
      self._save_df_to_parquet(
        self.report_uses_taxonomy_df, "relationships/REPORT_USES_TAXONOMY.parquet"
      )
      self._save_df_to_parquet(
        self.fact_units_df, "relationships/FACT_HAS_UNIT.parquet"
      )

      # Save fact_has_dimension relationship
      if (
        hasattr(self, "fact_has_dimension_rel_df")
        and not self.fact_has_dimension_rel_df.empty
      ):
        self._save_df_to_parquet(
          self.fact_has_dimension_rel_df, "relationships/FACT_HAS_DIMENSION.parquet"
        )

      self._save_df_to_parquet(
        self.fact_entities_df, "relationships/FACT_HAS_ENTITY.parquet"
      )
      self._save_df_to_parquet(
        self.fact_elements_df, "relationships/FACT_HAS_ELEMENT.parquet"
      )
      self._save_df_to_parquet(
        self.fact_periods_df, "relationships/FACT_HAS_PERIOD.parquet"
      )
      self._save_df_to_parquet(
        self.fact_set_contains_facts_df, "relationships/FACT_SET_CONTAINS_FACT.parquet"
      )
      self._save_df_to_parquet(
        self.element_labels_df, "relationships/ELEMENT_HAS_LABEL.parquet"
      )
      self._save_df_to_parquet(
        self.element_references_df, "relationships/ELEMENT_HAS_REFERENCE.parquet"
      )
      self._save_df_to_parquet(
        self.structure_taxonomies_df, "relationships/STRUCTURE_HAS_TAXONOMY.parquet"
      )
      self._save_df_to_parquet(
        self.taxonomy_labels_df, "relationships/TAXONOMY_HAS_LABEL.parquet"
      )
      self._save_df_to_parquet(
        self.taxonomy_references_df, "relationships/TAXONOMY_HAS_REFERENCE.parquet"
      )
      self._save_df_to_parquet(
        self.structure_associations_df,
        "relationships/STRUCTURE_HAS_ASSOCIATION.parquet",
      )
      self._save_df_to_parquet(
        self.association_from_elements_df,
        "relationships/ASSOCIATION_HAS_FROM_ELEMENT.parquet",
      )
      self._save_df_to_parquet(
        self.association_to_elements_df,
        "relationships/ASSOCIATION_HAS_TO_ELEMENT.parquet",
      )
      self._save_df_to_parquet(
        self.fact_dimension_axis_element_rel_df,
        "relationships/FACT_DIMENSION_AXIS_ELEMENT.parquet",
      )
      self._save_df_to_parquet(
        self.fact_dimension_member_element_rel_df,
        "relationships/FACT_DIMENSION_MEMBER_ELEMENT.parquet",
      )

    logger.info("All parquet files have been saved successfully")

  def _save_df_to_parquet_schema_driven(self, df, filename, schema_name):
    """Save a DataFrame to parquet using schema-driven approach with correct table name and directory."""
    if not df.empty:
      # Fix column types for specific tables with known type issues
      # These columns should always be STRING even when all values are null
      if schema_name == "Entity":
        # List of string columns in Entity that may be null
        string_columns = [
          "ein",
          "tax_id",
          "ticker",
          "exchange",  # Stock exchange
          "name",
          "legal_name",
          "industry",
          "entity_type",
          "sic",
          "sic_description",
          "category",
          "state_of_incorporation",
          "fiscal_year_end",
          "lei",  # Legal Entity Identifier
          "phone",  # Main phone number
          "website",
          "uri",
          "scheme",
          "cik",
          "status",
          "parent_entity_id",
        ]
        for col in string_columns:
          if col in df.columns:
            # Special handling for EIN/tax_id to preserve leading zeros
            if col in ["ein", "tax_id"]:
              # Convert numeric EINs to string with proper zero-padding
              # EINs are 9 digits and may have leading zeros
              df[col] = df[col].apply(
                lambda x: str(int(x)).zfill(9)
                if pd.notna(x) and str(x).strip() != ""
                else None
              )
            # Ensure all string columns are object type for parquet
            df[col] = df[col].astype("object")
      elif schema_name == "Unit":
        # List of string columns in Unit that may be null
        string_columns = ["numerator_uri", "denominator_uri", "uri", "measure", "value"]
        for col in string_columns:
          if col in df.columns:
            df[col] = df[col].astype("object")
      elif schema_name == "Report":
        # List of string columns in Report that may be null
        string_columns = [
          "name",
          "uri",
          "accession_number",
          "form",
          "xbrl_processor_version",
        ]
        for col in string_columns:
          if col in df.columns:
            df[col] = df[col].astype("object")
      elif schema_name == "Association":
        # Ensure weight is always DOUBLE type
        if "weight" in df.columns:
          df["weight"] = df["weight"].astype(
            "float64"
          )  # float64 maps to DOUBLE in parquet

      # Use the actual schema name for schema completeness check
      if self.schema_adapter:
        logger.debug(
          f"Ensuring schema completeness for {schema_name} (from {filename})"
        )
        logger.debug(
          f"DataFrame before schema completeness: {len(df.columns)} columns: {list(df.columns)}"
        )
        df = self._ensure_schema_completeness(df, schema_name)
        logger.debug(
          f"DataFrame after schema completeness: {len(df.columns)} columns: {list(df.columns)}"
        )

      # Apply column standardization if enabled
      if self.enable_column_standardization:
        df = self._standardize_dataframe_columns(df, schema_name)

      # Determine if this is a relationship table
      is_relationship = (
        schema_name in self.ingest_adapter.get_all_relationship_tables()
        if self.ingest_adapter
        else False
      )

      # Apply deduplication for all node types to prevent primary key violations
      # Only deduplicate if the dataframe has an 'identifier' column and it's not a relationship
      if "identifier" in df.columns and not is_relationship:
        original_count = len(df)
        df = df.drop_duplicates(subset=["identifier"], keep="first")
        deduped_count = len(df)
        if original_count != deduped_count:
          logger.info(
            f"Deduplicated {filename}: {original_count} -> {deduped_count} rows"
          )

      # Determine the appropriate subdirectory
      subdir = "relationships" if is_relationship else "nodes"

      # Generate filename using exact table name (schema_name)
      # Use schema_name directly instead of converting through filename
      final_filename = self._generate_standardized_filename(
        schema_name, is_relationship
      )

      # Include subdirectory in filepath
      filepath = self.output_dir / subdir / final_filename

      # Debug: Log DataFrame info before saving to parquet
      logger.debug(
        f"Saving {schema_name} to {final_filename}: {len(df)} rows, {len(df.columns)} columns"
      )
      logger.debug(f"Columns in {schema_name}: {list(df.columns)}")

      df.to_parquet(filepath, index=False)

      if final_filename != filename:
        logger.info(f"📝 Standardized filename: {filename} -> {final_filename}")
      logger.debug(f"Saved {len(df)} rows to {filepath}")
    else:
      logger.debug(f"Skipping empty DataFrame for {filename}")

  def _generate_standardized_filename(
    self, base_name: str, is_relationship: bool = False
  ) -> str:
    """
    Generate standardized filename based on filing information.

    Args:
        base_name: Base filename (e.g., "fact_dimensions")
        is_relationship: Whether this is relationship data

    Returns:
        Standardized filename
    """
    if not self.enable_standardized_filenames and not self.enable_type_prefixes:
      return f"{base_name}.parquet"

    # Build filename components
    components = []

    # Add type prefix if enabled
    if self.enable_type_prefixes:
      prefix = "rel_" if is_relationship else "node_"
      components.append(prefix)

    # Add base name
    components.append(base_name)

    # Add filing metadata if standardized filenames are enabled
    if self.enable_standardized_filenames and self.sec_filer and self.sec_report:
      # Use filing date and CIK for uniqueness
      date_str = getattr(self.sec_report, "filing_date", "").replace("-", "")
      cik = getattr(self.sec_filer, "cik", "unknown")
      if date_str and cik != "unknown":
        components.extend([date_str, cik])

    return "_".join(components) + ".parquet"

  def _standardize_dataframe_columns(
    self, df: pd.DataFrame, table_name: str
  ) -> pd.DataFrame:
    """
    Standardize DataFrame column names to match target schema.

    Args:
        df: Input DataFrame
        table_name: Target table name for column mapping

    Returns:
        DataFrame with standardized column names
    """
    if not self.enable_column_standardization or df.empty:
      return df

    # Column mappings for known problematic columns
    column_mappings = {
      # Column mappings would go here if needed
      # Example: "Association": {"old_name": "new_name"}
    }

    if table_name in column_mappings:
      mapping = column_mappings[table_name]
      df = df.rename(columns=mapping)
      if mapping:
        logger.debug(f"Standardized columns for {table_name}: {mapping}")

    return df

  def _ensure_schema_completeness(self, df, table_name):
    """
    Ensure DataFrame has all schema-defined columns before saving to parquet.
    This prevents missing columns during Kuzu ingestion.
    """
    try:
      logger.debug(f"Checking schema completeness for table: {table_name}")
      # Get schema information for this table
      if not self.schema_adapter:
        logger.warning(f"No schema adapter for table {table_name}")
        return df
      table_info = self.schema_adapter.get_schema_info(table_name)
      logger.debug(f"Schema lookup result for {table_name}: {table_info['type']}")
      if table_info["type"] == "unknown":
        logger.warning(f"No schema found for table {table_name}, saving as-is")
        return df

      schema_info = table_info["schema"]

      # Get expected columns from schema
      expected_columns = set()

      # For relationships, add foreign key columns
      if table_info["type"] == "relationship":
        expected_columns.update(["from", "to"])

      # Add all schema property columns
      for prop in schema_info["properties"]:
        expected_columns.add(prop.name)

      # Check which columns are missing
      current_columns = set(df.columns)
      missing_columns = expected_columns - current_columns

      if missing_columns:
        logger.debug(
          f"Adding missing schema columns to {table_name}: {missing_columns}"
        )

        # Add missing columns with appropriate default values to prevent column dropping
        for col in missing_columns:
          # Find the property definition to get its type
          prop_type = None
          for prop in schema_info["properties"]:
            if prop.name == col:
              prop_type = prop.type.upper()
              break

          # Use non-null defaults based on type to prevent parquet from dropping columns
          if prop_type in ["STRING", "VARCHAR", "TEXT"]:
            df[col] = ""  # Empty string instead of None
          elif prop_type in ["INT", "INT64", "INTEGER", "INT32"]:
            df[col] = 0  # Zero instead of None
          elif prop_type in ["DOUBLE", "FLOAT", "DECIMAL"]:
            df[col] = 0.0  # Zero float instead of None
          elif prop_type in ["BOOLEAN", "BOOL"]:
            df[col] = False  # False instead of None
          else:
            # For dates/timestamps and unknown types, use None (they handle nulls better)
            df[col] = None

        # Reorder columns to match schema order (put foreign keys first for relationships)
        ordered_columns = []
        if table_info["type"] == "relationship":
          if "from" in expected_columns:
            ordered_columns.append("from")
          if "to" in expected_columns:
            ordered_columns.append("to")

        # Add property columns in schema order
        for prop in schema_info["properties"]:
          if prop.name in expected_columns:
            ordered_columns.append(prop.name)

        # Add any extra columns that aren't in schema
        extra_columns = current_columns - expected_columns
        ordered_columns.extend(sorted(extra_columns))

        # Reorder DataFrame columns
        df = df[ordered_columns]

        logger.debug(
          f"Schema-complete {table_name} now has {len(df.columns)} columns: {list(df.columns)}"
        )

      return df

    except Exception as e:
      logger.warning(f"Failed to ensure schema completeness for {table_name}: {e}")
      return df

  def _save_df_to_parquet(self, df, filename):
    """Save a DataFrame to parquet if it's not empty."""
    if not df.empty:
      # Fix column types for specific tables with known type issues
      # These columns should always be STRING even when all values are null
      if "Entity" in filename:
        # List of string columns in Entity that may be null
        string_columns = [
          "ein",
          "tax_id",
          "ticker",
          "exchange",  # Stock exchange
          "name",
          "legal_name",
          "industry",
          "entity_type",
          "sic",
          "sic_description",
          "category",
          "state_of_incorporation",
          "fiscal_year_end",
          "lei",  # Legal Entity Identifier
          "phone",  # Main phone number
          "website",
          "uri",
          "scheme",
          "cik",
          "status",
          "parent_entity_id",
          "created_at",  # Add timestamp fields
          "updated_at",  # Add timestamp fields
        ]
        for col in string_columns:
          if col in df.columns:
            # Special handling for EIN/tax_id to preserve leading zeros
            if col in ["ein", "tax_id"]:
              # Convert numeric EINs to string with proper zero-padding
              # EINs are 9 digits and may have leading zeros
              df[col] = df[col].apply(
                lambda x: str(int(x)).zfill(9)
                if pd.notna(x) and str(x).strip() != ""
                else ""  # Use empty string instead of None to preserve string type
              )
            else:
              # For all other string columns, use empty string for null values
              # This preserves string type in PyArrow even when all values would be null
              df[col] = df[col].fillna("").astype("object")
      elif "Unit" in filename:
        # List of string columns in Unit that may be null
        string_columns = ["numerator_uri", "denominator_uri", "uri", "measure", "value"]
        for col in string_columns:
          if col in df.columns:
            # Ensure string type even when all values are null
            df[col] = df[col].fillna("").astype("object")
            df.loc[df[col] == "", col] = None
      elif "Report" in filename:
        # List of string columns in Report that may be null
        string_columns = [
          "name",
          "uri",
          "accession_number",
          "form",
          "xbrl_processor_version",
          "filing_date",
          "report_date",
          "acceptance_date",
          "period_end_date",
          "entity_identifier",
          "updated_at",
        ]
        for col in string_columns:
          if col in df.columns:
            # Ensure string type even when all values are null
            df[col] = df[col].fillna("").astype("object")
            df.loc[df[col] == "", col] = None
      elif "Fact" in filename:
        # List of string columns in Fact that may be null
        string_columns = [
          "identifier",
          "uri",
          "value",
          "fact_type",
          "decimals",
          "value_type",
          "content_type",  # This was causing issues
        ]
        for col in string_columns:
          if col in df.columns:
            # Ensure string type even when all values are null
            df[col] = df[col].fillna("").astype("object")
            df.loc[df[col] == "", col] = None
      elif "Association" in filename:
        # Ensure weight is always DOUBLE type
        if "weight" in df.columns:
          df["weight"] = df["weight"].astype(
            "float64"
          )  # float64 maps to DOUBLE in parquet
      # Handle path components for subdirectory support
      # If filename contains a path separator, extract the base name
      if "/" in filename:
        # Extract subdirectory and base filename
        parts = filename.rsplit("/", 1)
        subdir = parts[0]
        base_filename = parts[1]
      else:
        subdir = ""
        base_filename = filename

      # Determine table name from filename for column standardization
      base_name = base_filename.replace(".parquet", "")

      # Handle relationship table name mapping for schema resolution
      # Convert fact_has_dimension -> FactDimensionsRel to match schema mapping
      if base_name == "fact_has_dimension":
        table_name = "FactDimensionsRel"  # Maps to FACT_HAS_DIMENSION relationship
      else:
        # Use the original title case conversion for other tables
        table_name = base_name.replace("_", " ").title().replace(" ", "")

      # Ensure schema completeness if schema adapter is available
      if self.schema_adapter:
        logger.info(
          f"Ensuring schema completeness for {table_name} (from {base_filename})"
        )
        logger.debug(
          f"DataFrame before schema completeness: {len(df.columns)} columns: {list(df.columns)}"
        )
        df = self._ensure_schema_completeness(df, table_name)
        logger.debug(
          f"DataFrame after schema completeness: {len(df.columns)} columns: {list(df.columns)}"
        )

      # Apply column standardization if enabled
      if self.enable_column_standardization:
        df = self._standardize_dataframe_columns(df, table_name)

      # Determine if this is a relationship table
      is_relationship = (
        table_name in self.ingest_adapter.get_all_relationship_tables()
        if self.ingest_adapter
        else False
      )

      # Apply deduplication for all node types to prevent primary key violations
      # Only deduplicate if the dataframe has an 'identifier' column and it's not a relationship
      if "identifier" in df.columns and not is_relationship:
        original_count = len(df)
        df = df.drop_duplicates(subset=["identifier"], keep="first")
        deduped_count = len(df)
        if original_count != deduped_count:
          logger.info(
            f"Deduplicated {base_filename}: {original_count} -> {deduped_count} rows"
          )

      # Generate filename (may be different if standardization is enabled)
      base_name_clean = base_filename.replace(".parquet", "")
      is_relationship = self._is_relationship_filename(base_name_clean)
      final_filename = self._generate_standardized_filename(
        base_name_clean, is_relationship
      )

      # Construct filepath with subdirectory if provided
      if subdir:
        filepath = self.output_dir / subdir / final_filename
      else:
        filepath = self.output_dir / final_filename

      # Debug: Log DataFrame info before saving to parquet
      logger.info(
        f"Saving {table_name} to {final_filename}: {len(df)} rows, {len(df.columns)} columns"
      )
      logger.debug(f"Columns in {table_name}: {list(df.columns)}")

      df.to_parquet(filepath, index=False)

      if final_filename != filename:
        logger.info(f"📝 Standardized filename: {filename} -> {final_filename}")
      logger.debug(f"Saved {len(df)} rows to {filepath}")
    else:
      logger.debug(f"Skipping empty DataFrame for {filename}")

  def _is_relationship_filename(self, base_name: str) -> bool:
    """Determine if a filename represents relationship data."""
    relationship_patterns = [
      "entity_reports",
      "report_facts",
      "report_fact_sets",
      "report_taxonomies",
      "fact_units",
      "fact_has_dimension_rel",
      "fact_entities",
      "fact_elements",
      "fact_periods",
      "fact_set_facts",
      "element_labels",
      "element_references",
      "structure_taxonomies",
      "taxonomy_labels",
      "taxonomy_references",
      "structure_associations",
      "association_from_elements",
      "association_to_elements",
      "fact_dimension_elements",
    ]
    return base_name in relationship_patterns

  def make_entity(self):
    """Create the main entity (formerly entity) for this graph."""
    logger.debug(f"Creating entity data for ID: {self.entityId}")
    if not self.entityId:
      logger.warning("No entity ID provided")
      self.entity_data = None
      return None

    # Include all fields from Kuzu Entity schema
    # Generate a deterministic UUID for the identifier while keeping CIK for the cik field
    entity_uri = f"https://www.sec.gov/CIK{self.entityId.zfill(10)}"
    entity_identifier = create_entity_id(entity_uri)

    entity_data = {
      "identifier": entity_identifier,  # Primary key - UUIDv7 for optimal indexing
      "uri": entity_uri,  # SEC entity URI
      "scheme": "https://www.sec.gov/",  # SEC scheme
      "cik": self.entityId.zfill(10),  # Keep CIK for reference (10-digit padded)
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
      "ein": None,
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
      entity_data["cik"] = self.sec_filer.get("cik")
      entity_data["ticker"] = self.sec_filer.get("ticker")
      entity_data["sic"] = self.sec_filer.get("sic")
      entity_data["sic_description"] = self.sec_filer.get("sicDescription")
      entity_data["category"] = self.sec_filer.get("category")
      entity_data["state_of_incorporation"] = self.sec_filer.get("stateOfIncorporation")
      entity_data["fiscal_year_end"] = self.sec_filer.get("fiscalYearEnd")
      # Ensure EIN is properly formatted as a string with leading zeros
      ein_value = self.sec_filer.get("ein")
      if ein_value is not None and ein_value != "":
        # Convert to string and pad with zeros if needed (EINs are 9 digits)
        entity_data["ein"] = str(ein_value).zfill(9)
      else:
        entity_data["ein"] = None
      entity_data["tax_id"] = entity_data["ein"]  # EIN is the tax ID

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

      # Set XBRL entity URI and scheme if available
      if entity_data["cik"]:
        entity_data["scheme"] = "http://www.sec.gov/CIK"
        entity_data["uri"] = f"http://www.sec.gov/CIK#{entity_data['cik']}"

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

    # Include all fields from Kuzu Report schema (exactly match KuzuSchemaBuilder)
    report_data = {
      "identifier": report_id,  # Primary key - UUIDv7
      "uri": self.report_uri,
      "name": None,
      "accession_number": None,
      "form": None,
      "filing_date": None,  # Use None for null dates
      "report_date": None,
      "acceptance_date": None,
      "period_of_report": None,
      "period_start_date": None,
      "period_end_date": None,
      "is_inline_xbrl": False,
      "xbrl_processor_version": XBRL_GRAPH_PROCESSOR_VERSION,
      "processed": False,
      "failed": False,
    }

    if self.sec_report:
      logger.info("Adding report information from SEC report data")
      report_data["name"] = self.sec_report.get("form")
      report_data["accession_number"] = self.sec_report.get("accessionNumber")
      if "filingDate" in self.sec_report and self.sec_report["filingDate"]:
        try:
          report_data["filing_date"] = datetime.strptime(
            self.sec_report["filingDate"], "%Y-%m-%d"
          ).strftime("%Y-%m-%d")  # Convert to string format
        except ValueError:
          logger.warning(f"Invalid filingDate format: {self.sec_report['filingDate']}")
          report_data["filing_date"] = None
      if "reportDate" in self.sec_report and self.sec_report["reportDate"]:
        try:
          report_data["report_date"] = datetime.strptime(
            self.sec_report["reportDate"], "%Y-%m-%d"
          ).strftime("%Y-%m-%d")  # Convert to string format
        except ValueError:
          logger.warning(f"Invalid reportDate format: {self.sec_report['reportDate']}")
          report_data["report_date"] = None
      report_data["form"] = self.sec_report.get("form")

      # Add acceptance_date if available
      if (
        "acceptanceDateTime" in self.sec_report
        and self.sec_report["acceptanceDateTime"]
      ):
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

      # Add period_end_date if available
      if "periodOfReport" in self.sec_report and self.sec_report["periodOfReport"]:
        try:
          report_data["period_end_date"] = datetime.strptime(
            self.sec_report["periodOfReport"], "%Y-%m-%d"
          ).strftime("%Y-%m-%d")  # Convert to string format
        except ValueError:
          logger.warning(
            f"Invalid periodOfReport format: {self.sec_report['periodOfReport']}"
          )
          report_data["period_end_date"] = None
      report_data["is_inline_xbrl"] = self.sec_report.get("isInlineXBRL", False)
      logger.info(f"Report {report_data['name']} data prepared")

    # Set entity_identifier to link report to entity
    if self.entity_data and self.entity_data.get("identifier"):
      report_data["entity_identifier"] = self.entity_data["identifier"]
      logger.info(
        f"Set report entity_identifier to UUID: {self.entity_data['identifier']} (CIK: {self.entityId})"
      )
    elif self.entityId:
      # Fallback: Generate the entity UUID if entity_data not available
      entity_uri = f"https://www.sec.gov/CIK{self.entityId.zfill(10)}"
      report_data["entity_identifier"] = create_entity_id(entity_uri)
      logger.info(
        f"Generated entity_identifier UUID for CIK {self.entityId}: {report_data['entity_identifier']}"
      )

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
        "report_context": f"Filing: {report_data.get('form', 'Unknown')}",
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
    logger.info("Processing Document Type System (DTS)")
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
    logger.debug("Creating fact set and processing facts")

    # Create fact set - deterministic based on report URI
    factset_uri = f"{self.report_uri}#factset"
    factset_id = generate_deterministic_uuid7(factset_uri, namespace="factset")
    factset_data = {"identifier": factset_id}
    new_factset_df = pd.DataFrame([factset_data])
    self.fact_sets_df = self.safe_concat(self.fact_sets_df, new_factset_df)

    # Connect fact set to report
    if self.report_data:
      report_factset_rel = {
        "from": self.report_data["identifier"],
        "to": factset_id,
        "fact_set_context": f"Report facts for {self.report_data.get('form', 'filing')}",
      }
      new_report_factset_df = pd.DataFrame([report_factset_rel])
      self.report_fact_sets_df = self.safe_concat(
        self.report_fact_sets_df, new_report_factset_df
      )

    self.report_factset_id = factset_id
    logger.debug(f"Created fact set with ID: {factset_id}")

    # Initialize batch upload queue and cache for S3 externalization
    self.s3_upload_queue = []  # List of (content, bucket, key) tuples
    self.s3_upload_map = {}  # Map fact_id to expected S3 URL
    self.s3_content_cache = {}  # Map content hash to S3 URL (avoids duplicate uploads)

    fact_count = 0
    for xfact in self.arelle_cntlr.facts:
      self.make_fact(xfact)
      fact_count += 1

    # Process batch S3 uploads if any were queued
    if self.s3_upload_queue:
      logger.info(
        f"Processing batch upload of {len(self.s3_upload_queue)} large values to S3"
      )
      self._process_batch_s3_uploads()

    logger.info(f"Processed {fact_count} facts")

  def make_fact(self, xfact):
    fact_uri = f"{self.report_uri}#fact-{xfact.md5sum.value}"
    identifier = create_fact_id(fact_uri)
    logger.debug(f"Processing fact: {fact_uri}")

    # Check if fact already exists to prevent duplicates
    existing_fact = self.facts_df[self.facts_df["identifier"] == identifier]
    if not existing_fact.empty:
      logger.debug(f"Fact already exists, skipping duplicate: {fact_uri}")
      # Return early to avoid creating duplicate relationships
      return

    # Compute numeric value for easier analysis (Claude Opus recommendation)
    numeric_value = None
    if xfact.unit is not None and xfact.value is not None:
      try:
        # Convert string value to float and apply decimal scaling
        raw_value = float(str(xfact.value))
        if xfact.decimals is not None:
          # XBRL decimals are powers of 10 (e.g., -6 means divide by 1,000,000)
          numeric_value = raw_value * (10 ** int(xfact.decimals))
        else:
          numeric_value = raw_value
      except (ValueError, TypeError):
        # If conversion fails, leave numeric_value as None
        pass

    # Process fact value - externalize if large
    fact_value = str(xfact.value) if xfact.value is not None else None
    value_type = "inline"  # Default to inline storage
    content_type = None

    # Check if value should be externalized
    if fact_value and self._should_externalize_value(fact_value):
      logger.debug(
        f"Queueing large value ({len(fact_value)} bytes) for batch upload: {fact_uri}"
      )
      # Queue for batch upload instead of immediate upload
      external_result = self._queue_value_for_s3(
        fact_value, identifier, self.report_data
      )

      if external_result:
        # Use the expected URL (will be uploaded in batch)
        fact_value = external_result["url"]
        value_type = external_result["value_type"]
        content_type = external_result["content_type"]
        logger.debug(f"Queued for externalization: {fact_value}")
      else:
        logger.warning("Failed to queue large value, storing inline")

    fact_data = {
      "identifier": identifier,
      "uri": fact_uri,
      "value": fact_value,
      "numeric_value": numeric_value,  # NEW: Computed numeric value for calculations
      "fact_type": "Numeric" if xfact.unit is not None else "Nonnumeric",
      "decimals": xfact.decimals if xfact.unit is not None else None,
      "value_type": value_type,  # NEW: Indicates inline vs external storage
      "content_type": content_type,  # NEW: MIME type for externalized content
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
        "fact_context": f"Fact from {fact_data.get('type', 'unknown')} fact",
      }
      new_report_fact_df = pd.DataFrame([report_fact_rel])
      self.report_facts_df = self.safe_concat(self.report_facts_df, new_report_fact_df)

    # Connect fact to fact set
    factset_fact_rel = {
      "from": self.report_factset_id,
      "to": identifier,
    }
    new_factset_fact_df = pd.DataFrame([factset_fact_rel])
    if hasattr(self, "fact_set_contains_facts_df"):
      self.fact_set_contains_facts_df = self.safe_concat(
        self.fact_set_contains_facts_df, new_factset_fact_df
      )
    else:
      self.fact_set_contains_facts_df = new_factset_fact_df

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
        "unit_context": f"Unit: {unit_data.get('measure', 'unknown')}",
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
        fact_dim_identifier = generate_deterministic_uuid7(
          fact_dim_uri, namespace="dimension"
        )

        # Check if fact dimension already exists
        if (
          hasattr(self, "fact_dimensions_df")
          and not self.fact_dimensions_df.empty
          and "axis_uri" in self.fact_dimensions_df.columns
          and "member_uri" in self.fact_dimensions_df.columns
          and "type" in self.fact_dimensions_df.columns
        ):
          existing_fact_dim = self.fact_dimensions_df[
            (self.fact_dimensions_df["axis_uri"] == axis_uri)
            & (self.fact_dimensions_df["member_uri"] == member_uri)
            & (self.fact_dimensions_df["type"] == axis_type)
          ]
        else:
          existing_fact_dim = pd.DataFrame()  # Empty dataframe

        if existing_fact_dim.empty:
          fact_dim_data = {
            "identifier": fact_dim_identifier,
            "axis_uri": axis_uri,
            "member_uri": member_uri,
            "type": axis_type,
            "is_explicit": True,
            "is_typed": False,
          }
          new_fact_dim_df = pd.DataFrame([fact_dim_data])
          if hasattr(self, "fact_dimensions_df") and not self.fact_dimensions_df.empty:
            self.fact_dimensions_df = self.safe_concat(
              self.fact_dimensions_df, new_fact_dim_df
            )
          else:
            self.fact_dimensions_df = new_fact_dim_df
          logger.debug(f"Created new fact dimension: {member_uri}")

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
              hasattr(self, "fact_dimension_axis_element_rel_df")
              and not self.fact_dimension_axis_element_rel_df.empty
            ):
              self.fact_dimension_axis_element_rel_df = self.safe_concat(
                self.fact_dimension_axis_element_rel_df, new_fact_dim_elem_df
              )
            else:
              self.fact_dimension_axis_element_rel_df = new_fact_dim_elem_df

          # Create fact dimension to member element relationship
          if member_element_data:
            fact_dim_member_rel = {
              "from": fact_dim_identifier,
              "to": member_element_data["identifier"],
            }
            new_fact_dim_elem_df = pd.DataFrame([fact_dim_member_rel])
            if (
              hasattr(self, "fact_dimension_member_element_rel_df")
              and not self.fact_dimension_member_element_rel_df.empty
            ):
              self.fact_dimension_member_element_rel_df = self.safe_concat(
                self.fact_dimension_member_element_rel_df, new_fact_dim_elem_df
              )
            else:
              self.fact_dimension_member_element_rel_df = new_fact_dim_elem_df
        else:
          fact_dim_identifier = existing_fact_dim.iloc[0]["identifier"]

      elif mem.isTyped:
        typed_member = mem.stringValue
        logger.debug(f"Processing typed member: {typed_member}")

        # Fact dimensions should be deterministic based on their axis and member value
        fact_dim_uri = f"{self.report_uri}#dimension-{axis_uri}-typed-{typed_member}"
        fact_dim_identifier = generate_deterministic_uuid7(
          fact_dim_uri, namespace="dimension"
        )

        # Check if fact dimension already exists
        if (
          hasattr(self, "fact_dimensions_df")
          and not self.fact_dimensions_df.empty
          and "axis_uri" in self.fact_dimensions_df.columns
          and "member_uri" in self.fact_dimensions_df.columns
          and "type" in self.fact_dimensions_df.columns
        ):
          existing_fact_dim = self.fact_dimensions_df[
            (self.fact_dimensions_df["axis_uri"] == axis_uri)
            & (self.fact_dimensions_df["member_uri"] == typed_member)
            & (self.fact_dimensions_df["type"] == axis_type)
          ]
        else:
          existing_fact_dim = pd.DataFrame()  # Empty dataframe

        if existing_fact_dim.empty:
          fact_dim_data = {
            "identifier": fact_dim_identifier,
            "axis_uri": axis_uri,
            "member_uri": typed_member,
            "type": axis_type,
            "is_explicit": False,
            "is_typed": True,
          }
          new_fact_dim_df = pd.DataFrame([fact_dim_data])
          if hasattr(self, "fact_dimensions_df") and not self.fact_dimensions_df.empty:
            self.fact_dimensions_df = self.safe_concat(
              self.fact_dimensions_df, new_fact_dim_df
            )
          else:
            self.fact_dimensions_df = new_fact_dim_df
          logger.debug(f"Created new typed fact dimension: {typed_member}")

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
              hasattr(self, "fact_dimension_axis_element_rel_df")
              and not self.fact_dimension_axis_element_rel_df.empty
            ):
              self.fact_dimension_axis_element_rel_df = self.safe_concat(
                self.fact_dimension_axis_element_rel_df, new_fact_dim_elem_df
              )
            else:
              self.fact_dimension_axis_element_rel_df = new_fact_dim_elem_df
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
    - The main entity (if URI matches the top-level entity)
    - Subsidiary entities (if different from main entity)
    """
    logger.debug("Processing entity from XBRL context for fact")
    entity_ns, entity_id = xfact.context.entityIdentifier
    entity_uri = f"{entity_ns}#{entity_id}"
    logger.debug(f"Processing XBRL entity: {entity_uri}")

    # Check if this is the main entity or a subsidiary
    is_main_entity = False
    if self.entity_data:
      # Check if this entity URI matches our main entity
      main_entity_uri = self.entity_data.get("uri")
      main_entity_cik = self.entity_data.get("cik")

      # Match by URI or by CIK in the entity ID
      if main_entity_uri == entity_uri:
        is_main_entity = True
      elif main_entity_cik and entity_id == main_entity_cik:
        is_main_entity = True
        # Update main entity's URI if not set
        if not main_entity_uri:
          self.entity_data["uri"] = entity_uri
          self.entity_data["scheme"] = entity_ns

    if is_main_entity and self.entity_data:
      # Use the main entity identifier
      entity_identifier = self.entity_data["identifier"]
      logger.debug(f"Using main entity for {entity_uri}")
    else:
      # This is a subsidiary or different entity - create it
      entity_identifier = create_entity_id(entity_uri)

      # Check if this subsidiary entity already exists
      existing_entity = self.entities_df[self.entities_df["uri"] == entity_uri]
      if existing_entity.empty:
        entity_data = {
          "identifier": entity_identifier,  # Primary key - UUIDv7
          "uri": entity_uri,
          "scheme": entity_ns,
          "name": entity_id,  # Use entity ID as name for now
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
          f"Created subsidiary entity: {entity_uri} with ID: {entity_identifier}"
        )

    # Create fact-entity relationship
    fact_entity_rel = {
      "from": fact_data["identifier"],
      "to": entity_identifier,
      "entity_context": f"Entity: {entity_id}",
    }
    new_fact_entity_df = pd.DataFrame([fact_entity_rel])
    self.fact_entities_df = self.safe_concat(self.fact_entities_df, new_fact_entity_df)

  def make_concept(self, fact_data, xfact):
    logger.debug("Processing concept for fact")
    concept_ns = xfact.concept.document.targetNamespace
    concept_uri = f"{concept_ns}#{xfact.concept.name}"
    logger.debug(f"Processing concept: {concept_uri}")

    element_data = self.make_element(xfact.concept)
    if element_data:
      logger.debug(f"Created element for concept: {concept_uri}")

      # Create fact-element relationship (fact uses element)
      fact_element_rel = {
        "from": fact_data["identifier"],  # Fact HAS element
        "to": element_data["identifier"],
      }
      new_fact_element_df = pd.DataFrame([fact_element_rel])
      self.fact_elements_df = self.safe_concat(
        self.fact_elements_df, new_fact_element_df
      )

  def make_period(self, fact_data, xfact):
    logger.debug("Processing period for fact")
    period_uri = None
    period_data = None

    if xfact.context.isInstantPeriod:
      instant_date = (xfact.context.instantDatetime - timedelta(1)).strftime("%Y-%m-%d")
      period_uri = f"{ISO_8601_URI}#{instant_date}"
      logger.debug(f"Processing instant period: {period_uri}")

      # Make period identifier global/idempotent for deduplication
      period_identifier = create_period_id(period_uri)

      # Check if period already exists globally
      existing_period = self.periods_df[
        self.periods_df["identifier"] == period_identifier
      ]
      if existing_period.empty:
        # Compute fiscal year and other time series fields (Claude Opus recommendation)
        instant_dt = datetime.strptime(instant_date, "%Y-%m-%d")
        fiscal_year = instant_dt.year

        # Determine fiscal quarter based on instant date
        instant_month = instant_dt.month
        fiscal_quarter = None
        if instant_month in [1, 2, 3]:
          fiscal_quarter = "Q1"
        elif instant_month in [4, 5, 6]:
          fiscal_quarter = "Q2"
        elif instant_month in [7, 8, 9]:
          fiscal_quarter = "Q3"
        elif instant_month in [10, 11, 12]:
          fiscal_quarter = "Q4"

        # Determine if this is a typical reporting date
        # Common quarter-end dates: 3/31, 6/30, 9/30, 12/31 (±a few days for weekends)
        is_quarter_end = instant_dt.day >= 28 and instant_month in [3, 6, 9, 12]
        is_year_end = instant_dt.day >= 28 and instant_month == 12

        period_data = {
          "identifier": period_identifier,
          "uri": period_uri,
          "instant_date": instant_date,  # Keep for backward compatibility (deprecated)
          "start_date": None,  # NULL for instant periods
          "end_date": instant_date,  # Use end_date for instant values
          "forever_date": False,
          "fiscal_year": fiscal_year,  # For easier time series queries
          "fiscal_quarter": fiscal_quarter,  # Q1-Q4 based on instant date
          "is_annual": is_year_end,  # True if Dec 28-31 (typical year-end)
          "is_quarterly": is_quarter_end,  # True if quarter-end date
          "days_in_period": 0,  # 0 for instant (point-in-time)
          "period_type": "instant",  # Clearly identify as instant
          "is_ytd": False,  # Instant values are not cumulative
        }
        new_period_df = pd.DataFrame([period_data])
        self.periods_df = self.safe_concat(self.periods_df, new_period_df)
        logger.debug(f"Created new instant period: {period_uri}")

    elif xfact.context.isStartEndPeriod:
      start_date = xfact.context.startDatetime.strftime("%Y-%m-%d")
      end_date = (xfact.context.endDatetime - timedelta(1)).strftime("%Y-%m-%d")
      period_uri = f"{ISO_8601_URI}#{start_date}/{end_date}"
      logger.debug(f"Processing start-end period: {period_uri}")

      # Make period identifier global/idempotent for deduplication
      period_identifier = create_period_id(period_uri)

      # Check if period already exists globally
      existing_period = self.periods_df[
        self.periods_df["identifier"] == period_identifier
      ]
      if existing_period.empty:
        # Compute fiscal year, quarter and duration analysis (Claude Opus recommendation)
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        fiscal_year = end_dt.year  # Use end date for fiscal year
        days_in_period = (end_dt - start_dt).days + 1

        # Determine period type - handle variable fiscal periods
        is_quarterly = 80 <= days_in_period <= 100  # ~3 months (Q1, Q2, Q3, Q4)
        is_semi_annual = 170 <= days_in_period <= 190  # ~6 months (H1, H2)
        is_nine_months = 260 <= days_in_period <= 280  # ~9 months (YTD through Q3)
        is_annual = 350 <= days_in_period <= 380  # ~1 year (FY)

        # Determine period type for aggregation
        period_type = None
        is_ytd = False  # Year-to-date flag for cumulative periods

        if is_quarterly:
          period_type = "quarterly"
        elif is_semi_annual:
          period_type = "semi_annual"
          is_ytd = True  # 6-month periods are typically YTD
        elif is_nine_months:
          period_type = "nine_months"
          is_ytd = True  # 9-month periods are always YTD
        elif is_annual:
          period_type = "annual"
        else:
          period_type = "other"

        # Estimate fiscal quarter/period based on end date and duration
        fiscal_quarter = None
        if is_quarterly:
          end_month = end_dt.month
          if end_month in [1, 2, 3]:
            fiscal_quarter = "Q1"
          elif end_month in [4, 5, 6]:
            fiscal_quarter = "Q2"
          elif end_month in [7, 8, 9]:
            fiscal_quarter = "Q3"
          elif end_month in [10, 11, 12]:
            fiscal_quarter = "Q4"
        elif is_semi_annual:
          # For 6-month periods, determine if H1 or H2
          end_month = end_dt.month
          if end_month in [4, 5, 6, 7]:
            fiscal_quarter = "H1"  # First half
          else:
            fiscal_quarter = "H2"  # Second half
        elif is_nine_months:
          fiscal_quarter = "M9"  # 9 months (through Q3)

        period_data = {
          "identifier": period_identifier,
          "uri": period_uri,
          "instant_date": None,
          "start_date": start_date,
          "end_date": end_date,
          "forever_date": False,
          "fiscal_year": fiscal_year,  # For easier time series queries
          "fiscal_quarter": fiscal_quarter,  # Q1-Q4, H1-H2, M9, etc.
          "is_annual": is_annual,  # True for ~1 year periods
          "is_quarterly": is_quarterly,  # True for ~3 month periods
          "days_in_period": days_in_period,  # Actual duration
          "period_type": period_type,  # NEW: quarterly, semi_annual, nine_months, annual, other
          "is_ytd": is_ytd,  # NEW: True for cumulative YTD periods
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
          "instant_date": None,
          "start_date": None,
          "end_date": None,
          "forever_date": True,
          "fiscal_year": None,  # Forever has no specific year
          "fiscal_quarter": None,  # Forever has no quarter
          "is_annual": False,  # Forever is not annual
          "is_quarterly": False,  # Forever is not quarterly
          "days_in_period": None,  # Forever has infinite duration
          "period_type": "forever",  # NEW: Clearly identify as forever
          "is_ytd": False,  # Forever is not YTD
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
      # Use deterministic period ID for the report context
      period_identifier = create_period_id(f"{report_id}#{period_uri}")

      existing_period = self.periods_df[
        self.periods_df["identifier"] == period_identifier
      ]
      if existing_period.empty:
        period_data = {
          "identifier": period_identifier,
          "uri": period_uri,
          "instant_date": None,
          "start_date": None,
          "end_date": None,
          "forever_date": False,
        }
        new_period_df = pd.DataFrame([period_data])
        self.periods_df = self.safe_concat(self.periods_df, new_period_df)
        logger.debug("Created fallback unknown period")

    # Create fact-period relationship
    if period_uri:
      # Get the period identifier (global/idempotent)
      if "period_data" in locals() and period_data:
        period_identifier = period_data["identifier"]
      else:
        # For existing periods - use global identifier
        period_identifier = create_period_id(period_uri)

      fact_period_rel = {
        "from": fact_data["identifier"],
        "to": period_identifier,
        "period_context": f"Period: {period_uri.split('#')[-1] if '#' in period_uri else 'unknown'}",
      }
      new_fact_period_df = pd.DataFrame([fact_period_rel])
      self.fact_periods_df = self.safe_concat(self.fact_periods_df, new_fact_period_df)

  def make_taxonomy(self):
    logger.debug(f"Creating taxonomy for URI: {self.taxonomy_uri}")

    if not hasattr(self, "taxonomy_uri") or not self.taxonomy_uri:
      logger.error("No taxonomy URI available")
      return

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
        "taxonomy_context": f"Uses taxonomy: {self.taxonomy_uri.split('/')[-1] if '/' in self.taxonomy_uri else 'unknown'}",
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

    # taxonomy_data is already set above
    self.make_structures()
    logger.debug("Taxonomy creation completed")

  def make_structures(self):
    logger.info("Processing taxonomy structures")
    filing_roles = pd.DataFrame(
      data=[(k[0], k[1]) for k in self.arelle_cntlr.baseSets.keys()],
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
        structure_id = generate_deterministic_uuid7(
          f"structure:{accession_number}#{structure_uri}"
        )
        network_uri = role_uri
        definition = (
          role.definition if hasattr(role, "definition") and role.definition else ""
        )
        def_split = definition.split("-") if definition else []
        if self.sec_report is not None and len(def_split) >= 2:
          network_number = def_split[0].strip()
          network_type = def_split[1].strip()
          network_name = (
            definition.split(def_split[1] + "-")[1].strip()
            if definition and def_split[1] + "-" in definition
            else None
          )
        else:
          network_number = None
          network_type = None
          network_name = None

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
            "taxonomy_context": f"Taxonomy: {self.taxonomy_data.get('uri', 'unknown')}",
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

      if arcrole == XbrlConst.parentChild:
        logger.debug("Processing parent-child associations")
        self.make_association_metadata(structure_data["identifier"])

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
          "association_context": f"Association: {association_data.get('type', 'unknown')}",
        }
        new_structure_assoc_df = pd.DataFrame([structure_assoc_rel])
        self.structure_associations_df = self.safe_concat(
          self.structure_associations_df, new_structure_assoc_df
        )

  def make_element(self, xconcept):
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
    ):
      classification = "dimensionElement"
    elif (
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
        "label_context": f"Label: {label_data.get('type', 'unknown')}",
      }
      new_element_label_df = pd.DataFrame([element_label_rel])
      self.element_labels_df = self.safe_concat(
        self.element_labels_df, new_element_label_df
      )

      # Create taxonomy-label relationship
      if hasattr(self, "taxonomy_data"):
        taxonomy_label_rel = {
          "from": self.taxonomy_data["identifier"],
          "to": label_identifier,
          "label_context": f"Taxonomy label: {label_data.get('type', 'unknown')}",
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
          "reference_context": f"Reference: {reference_data.get('type', 'unknown')}",
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
            "reference_context": f"Taxonomy reference: {reference_data.get('type', 'unknown')}",
          }
          new_taxonomy_ref_df = pd.DataFrame([taxonomy_ref_rel])
          self.taxonomy_references_df = self.safe_concat(
            self.taxonomy_references_df, new_taxonomy_ref_df
          )

  def make_association_metadata(self, structure_id):
    logger.debug(f"Processing association metadata for structure: {structure_id}")
    # Note: Association metadata classification will be handled during Kuzu import
    # This method is simplified for parquet output
    logger.debug("Association metadata processing completed (deferred to Kuzu import)")
