# Processors Directory

This directory contains specialized processors that handle complex data transformations, calculations, and business logic for the RoboSystems platform. Processors are responsible for taking raw data and transforming it into structured formats suitable for storage, analysis, or further processing.

## 📂 Directory Structure

```
processors/
├── README.md                          # This file
├── __init__.py                        # Exports all processors
├── xbrl_graph.py                      # XBRLGraphProcessor - XBRL to graph data model conversion
├── xbrl/                              # XBRL utility components (extracted for cleaner code)
│   ├── __init__.py                    # XBRL module exports
│   ├── dataframe_manager.py           # DataFrame validation and processing
│   ├── duckdb_graph_ingestion.py      # DuckDB → Graph ingestion
│   ├── parquet_writer.py              # Parquet file generation
│   ├── id_utils.py                    # Identifier generation utilities
│   ├── naming_utils.py                # Naming convention utilities
│   ├── textblock_externalizer.py      # Text block extraction
│   ├── schema_adapter.py              # XBRLSchemaAdapter - DataFrame schema adaptation
│   └── schema_config_generator.py     # XBRLSchemaConfigGenerator - Schema config generation
└── qb_transactions.py                 # QBTransactionsProcessor - QuickBooks processing
```

## 🎯 Purpose

Processors handle the "heavy lifting" of data transformation and business logic processing:

- **Data Transformation** - Converting raw data into structured formats
- **Business Logic** - Implementing complex financial calculations and rules
- **Schema Processing** - Ensuring data compatibility with database schemas
- **File Processing** - Handling ingestion of external data formats
- **Financial Calculations** - Trial balances, schedules, and accounting operations

## 🔧 Key Processors

### **Data Processing & Transformation**

- **`XBRLGraphProcessor`** (`xbrl_graph.py`) - Core XBRL to graph data model conversion

  Converts SEC XBRL filings from the Arelle/XBRL data model into RoboSystems graph data model:

  - **Data Model Transformation** - Maps XBRL concepts to graph nodes and relationships
  - **Entity Processing** - Extracts company information and identifiers
  - **Fact Processing** - Converts financial facts with contexts, units, and dimensions
  - **Taxonomy Processing** - Handles us-gaap and custom taxonomies
  - **Report Processing** - Structures 10-K and 10-Q filing data
  - **Schema Integration** - Uses XBRLSchemaAdapter for DataFrame compatibility

  The main conversion logic resides here, with utility components extracted to `/xbrl/` for cleaner code organization.

- **XBRL Utility Components** (`/processors/xbrl/`) - Supporting utilities extracted for code clarity

  **Core Utilities:**

  - **`DataFrameManager`** - DataFrame validation, cleaning, and transformation

    - Schema-driven validation and type conversion
    - Null handling and data normalization
    - Duplicate detection and resolution

  - **`XBRLDuckDBGraphProcessor`** - Direct DuckDB staging to graph database ingestion

    - Native DuckDB → LadybugDB via database extensions
    - Batch processing with comprehensive error handling
    - Progress tracking and monitoring integration

  - **`ParquetWriter`** - Optimized Parquet file generation

    - Schema-based file writing with compression
    - S3 upload integration and optimization
    - Memory-efficient processing for large datasets

  - **`TextBlockExternalizer`** - Large text block extraction and storage

    - Separates narrative content from structured data
    - Reduces graph database size and improves performance

  - **`XBRLSchemaAdapter`** - DataFrame schema adaptation for XBRL data

    - Validates DataFrame structures against schema definitions
    - Creates schema-compatible DataFrames with proper columns
    - Handles XBRL table mappings and data validation

  - **`XBRLSchemaConfigGenerator`** - Dynamic schema configuration generation

    - Creates file pattern recognition for parquet files
    - Maps relationship structures for graph ingestion
    - Produces column structures from schema properties

  - **Helper Modules**
    - `id_utils.py` - Standardized identifier generation
    - `naming_utils.py` - Consistent naming conventions

### **External Data Integration**

- **`QBTransactionsProcessor`** - Processes QuickBooks transaction data
  - Normalizes QuickBooks API responses
  - Maps QuickBooks entities to graph structures
  - Handles transaction categorization and processing

## 🏗️ Architecture Principles

### 1. **Separation from Adapters**

- **Processors** (this directory): Transform and process data
- **Adapters** (`/adapters/`): Connect to external services (APIs, databases)
- **Operations** (`/operations/`): Orchestrate business workflows

### 2. **Single Responsibility**

Each processor focuses on one specific type of data transformation:

- Clear input/output contracts
- Focused business logic
- Testable components

### 3. **Schema-Driven Design**

Processors work with schema definitions to ensure:

- Data compatibility across the system
- Dynamic configuration based on schema changes
- Consistent data structures

### 4. **Reusability**

Processors are designed to be:

- Reusable across different workflows
- Configurable for different use cases
- Independent of specific data sources

## 🔄 Usage Patterns

### Importing Processors

```python
from robosystems.processors import XBRLGraphProcessor
from robosystems.processors.xbrl import (
    DataFrameManager,
    ParquetWriter,
    XBRLDuckDBGraphProcessor
)

# Main XBRL to graph conversion
processor = XBRLGraphProcessor(
    report_uri="https://www.sec.gov/...",
    output_dir="./data/output"
)
processor.process()  # Converts XBRL/Arelle data model to graph data model

# Use XBRL utility components directly if needed
df_manager = DataFrameManager()
validated_df = df_manager.validate_and_process(raw_data)

parquet_writer = ParquetWriter()
parquet_writer.write_to_s3(validated_df, s3_path)

# Note: Graph ingestion is typically handled by the Graph API endpoint
# but the utility can be used programmatically
ingestion_processor = XBRLDuckDBGraphProcessor()
ingestion_processor.ingest_to_graph(graph_id, table_name)
```

### Processing Pipeline

**XBRL Filing Processing Pipeline:**

```
SEC XBRL Filing
       ↓
XBRLGraphProcessor (xbrl_graph.py)
  - Arelle/XBRL → Graph Data Model Conversion
  - Uses: DataFrameManager, ParquetWriter, XBRLSchemaAdapter
       ↓
Parquet Files → S3 Storage
       ↓
DuckDB Staging Table (Upload)
       ↓
XBRLDuckDBGraphProcessor → Graph Database (LadybugDB/Neo4j)
```

### Configuration

Processors accept configuration objects that define:

- Schema definitions
- Processing parameters
- Output formats
- Validation rules

## 🚀 Benefits

### For Data Processing

- **Consistency** - Standardized data transformation patterns
- **Validation** - Built-in data validation and error handling
- **Performance** - Optimized for large-scale data processing
- **Reliability** - Comprehensive error handling and logging

### For Development

- **Modularity** - Each processor handles one specific concern
- **Testability** - Isolated components easy to unit test
- **Maintainability** - Clear separation of processing logic
- **Extensibility** - Easy to add new processors for new data types

### For Business Logic

- **Accuracy** - Financial calculations follow accounting standards
- **Compliance** - Handles regulatory requirements (SEC, GAAP)
- **Flexibility** - Configurable for different business scenarios
- **Scalability** - Designed for high-volume data processing

## 📚 Related Components

- **Adapters** (`/adapters/`) - External service connections
- **Operations** (`/operations/`) - Business workflow orchestration
- **Models** (`/models/`) - Data structure definitions
- **Schema** (`/schema/`) - Schema definitions and validation
- **Tasks** (`/tasks/`) - Background processing jobs

## 🔧 Development Guidelines

When creating new processors:

1. **Focus on Transformation** - Pure data processing logic
2. **Accept Configuration** - Make processors configurable
3. **Handle Errors Gracefully** - Comprehensive error handling
4. **Add Logging** - Detailed processing logs for debugging
5. **Write Tests** - Unit tests for all processing logic
6. **Document Thoroughly** - Clear docstrings and examples
