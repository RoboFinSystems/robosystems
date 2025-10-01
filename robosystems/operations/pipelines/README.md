# Pipeline Operations

Production-ready data processing pipelines with full visibility and tracking.

## Overview

This module contains orchestrated multi-step data processing pipelines designed for distributed execution across workers. Each pipeline provides:

- **First-class tracking**: Redis-based state management with real-time progress
- **Distributed execution**: Work distribution across Celery workers
- **Automatic ingestion**: Kuzu graph database ingestion upon completion
- **Production resilience**: Retry logic, error handling, and timeout management
- **Full transparency**: Detailed progress tracking and status reporting

## Architecture

```
Pipeline Flow:
1. Pipeline Start (Celery Task)
   → Initialize tracker
   → Record pipeline metadata

2. Discovery Phase
   → Discover entities (companies, filings, etc.)
   → Update expected task count

3. Processing Phase
   → Process individual items in parallel
   → Track completion/failure per item
   → Update progress in real-time

4. Completion Phase
   → Detect pipeline completion automatically
   → Trigger Kuzu ingestion
   → Clean up resources
```

## SEC XBRL Pipeline

The SEC XBRL pipeline (`sec_xbrl_filings.py`) processes SEC financial filings:

### Pipeline Steps

1. **Entity Discovery**

   - Fetches S&P 500 companies with recent XBRL filings
   - Updates tracker with entity count

2. **Filing Discovery**

   - Discovers 10-K and 10-Q filings for each entity
   - Updates tracker with expected filing count

3. **XBRL Processing**

   - Downloads XBRL documents from SEC EDGAR
   - Processes with XBRLGraphProcessor
   - Converts to graph-ready parquet files
   - Uploads to S3 with date partitioning

4. **Kuzu Ingestion**
   - Automatically triggered on pipeline completion
   - Ingests all parquet files into Kuzu graph database
   - Runs asynchronously on Kuzu shared master

### Usage

```python
from robosystems.operations.pipelines import SECXBRLPipeline

# Initialize pipeline
pipeline = SECXBRLPipeline("sec_pipeline_12345")

# Start pipeline with limits
pipeline.start(
    max_companies=10,
    max_filings_per_entity=5
)

# Check status
status = pipeline.get_status()
print(f"Progress: {status['progress_percent']}%")
print(f"Completed: {status['completed_tasks']}/{status['expected_tasks']}")
```

### Celery Task Integration

The pipeline is designed to be orchestrated via Celery tasks:

```python
# In tasks/data_sync/sec_filings.py
@celery_app.task
def orchestrate_sec_pipeline(max_companies=None, max_filings=None):
    pipeline_id = f"sec_pipeline_{int(time.time())}"
    pipeline = SECXBRLPipeline(pipeline_id)

    # Start pipeline
    pipeline.start(max_companies, max_filings)

    # Queue discovery task
    discover_and_process_companies.delay(
        pipeline_id, max_companies, max_filings
    )
```

## Pipeline Tracker Integration

All pipelines use the `PipelineTracker` for state management:

```python
from robosystems.utils.pipeline_tracker import PipelineTracker

class MyPipeline:
    def __init__(self, pipeline_id):
        self.tracker = PipelineTracker("my_pipeline", pipeline_id)

    def process_item(self, item):
        try:
            # Process item
            result = do_processing(item)

            # Track completion
            is_complete = self.tracker.task_completed({"item": item})

            if is_complete:
                # Pipeline complete - trigger next phase
                self.trigger_next_phase()

        except Exception as e:
            # Track failure
            self.tracker.task_failed({"item": item, "error": str(e)})
```

## Best Practices

1. **Always use tracker**: Every pipeline must use PipelineTracker for visibility
2. **Atomic updates**: Update tracker immediately after each task completion
3. **Error handling**: Always track failures with meaningful error messages
4. **Idempotency**: Design pipelines to be safely re-runnable
5. **Resource cleanup**: Clean up temporary files and resources
6. **Distributed design**: Assume tasks run on different workers

## Production Considerations

- **Timeouts**: Set appropriate timeouts for each pipeline phase
- **Retries**: Configure retry logic for transient failures
- **Monitoring**: Use tracker status for monitoring and alerting
- **Scaling**: Design for horizontal scaling across workers
- **Cost**: Consider S3 storage and Kuzu compute costs
