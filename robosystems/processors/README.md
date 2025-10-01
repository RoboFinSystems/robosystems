# Processors Directory

This directory contains specialized processors that handle complex data transformations, calculations, and business logic for the RoboSystems platform. Processors are responsible for taking raw data and transforming it into structured formats suitable for storage, analysis, or further processing.

## 📂 Directory Structure

```
processors/
├── README.md                          # This file
├── __init__.py                        # Exports all processors
├── xbrl_graph.py                      # XBRLGraphProcessor - SEC XBRL filing processing
├── schema_processor.py                # SchemaProcessor - DataFrame schema compatibility
├── schema_ingestion_processor.py      # SchemaIngestionProcessor - Ingestion config generation
├── trial_balance.py                   # TrialBalanceProcessor - Financial calculations
├── schedule.py                        # ScheduleProcessor - Financial schedule generation
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

- **`XBRLGraphProcessor`** - Processes SEC XBRL filings into graph database format
  - Handles complex XBRL taxonomy structures
  - Converts financial data into nodes and relationships
  - Manages entity relationships and fact processing

### **Schema Management**

- **`SchemaProcessor`** - Ensures DataFrame compatibility with Kuzu schemas

  - Validates DataFrame structures against schema definitions
  - Creates schema-compatible DataFrames with proper columns
  - Handles XBRL table mappings and data validation

- **`SchemaIngestionProcessor`** - Generates dynamic ingestion configurations
  - Creates file pattern recognition for parquet files
  - Maps relationship structures for graph ingestion
  - Produces column structures from schema properties

### **Financial Processing**

- **`TrialBalanceProcessor`** - Generates financial trial balances

  - Processes chart of accounts and transaction data
  - Calculates account balances and financial positions
  - Handles retained earnings and current year calculations

- **`ScheduleProcessor`** - Creates financial schedules and reports
  - Generates structured financial reporting schedules
  - Handles transaction scheduling and amortization
  - Creates timeline-based financial projections

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
from robosystems.processors import (
    XBRLGraphProcessor,
    SchemaProcessor,
    TrialBalanceProcessor
)

# Use in business logic
processor = XBRLGraphProcessor(config)
result = processor.process_filing(filing_data)
```

### Processing Pipeline

Processors often work together in pipelines:

```
Raw Data → SchemaIngestionProcessor → SchemaProcessor → XBRLGraphProcessor → Database
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

## 🎯 Examples

### Financial Data Processing

```python
# Process trial balance
tb_processor = TrialBalanceProcessor(entity_id="123")
tb_processor.generate()

# Generate financial schedule
schedule_processor = ScheduleProcessor(
    transaction_id="tx_456",
    start_date="2024-01-01",
    months=12
)
schedule = schedule_processor.create()
```

### Schema Processing

```python
# Ensure data fits schema
schema_processor = SchemaProcessor(config)
df = schema_processor.create_schema_compatible_dataframe("Entity")
validated_df = schema_processor.process_dataframe_for_schema("Entity", data)
```

This directory represents the core data processing engine of the RoboSystems platform, handling the complex transformations needed to convert raw financial data into structured, queryable information.
