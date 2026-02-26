"""SEC Entity Sync Pipeline Utilities."""

import tempfile
from pathlib import Path


def get_pipeline_work_dir(graph_id: str) -> Path:
  """Get a deterministic work directory for a pipeline run.

  All assets in the same pipeline run share this directory so they
  can pass data between extract → transform → load without needing
  Dagster IO managers.
  """
  base = Path(tempfile.gettempdir()) / "sec_entity_pipeline" / graph_id
  base.mkdir(parents=True, exist_ok=True)
  return base
