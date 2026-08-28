"""SEC Hugging Face Publish Asset.

Copies the SEC R2 snapshot (the same .lbug.zst subscribers download) to the
public Hugging Face dataset. Manual only: no sensor or schedule launches it,
so the public dump stays a deliberate act while the hosted graph stays
always-current.

The lineage chain:
  sec_lbug_r2_published -> sec_lbug_hf_published

This is a thin wrapper around the shared publish_to_huggingface() helper.
"""

from dagster import AssetExecutionContext, MaterializeResult, asset

from robosystems.adapters.sec.pipeline.configs import SECHFPublishConfig
from robosystems.config import env
from robosystems.dagster.assets.shared_repositories.huggingface import (
  publish_to_huggingface,
)

# Fixed path so the download URL on the dataset card never changes.
SEC_HF_PATH_IN_REPO = "sec.lbug.zst"


@asset(
  group_name="sec_pipeline",
  description="Copy the SEC R2 snapshot to the public Hugging Face dataset (manual)",
  kinds={"huggingface", "ladybug"},
  deps=["sec_lbug_r2_published"],
  metadata={
    "pipeline": "sec",
    "stage": "hf_publish",
    "manual_only": True,
  },
)
def sec_lbug_hf_published(
  context: AssetExecutionContext,
  config: SECHFPublishConfig,
) -> MaterializeResult:
  """Copy the SEC R2 snapshot to Hugging Face via a Hub-side Job.

  Delegates to the shared publish_to_huggingface() helper which handles:
  - Presigning the R2 object (no AWS egress: the bytes move R2 -> HF Job -> Hub)
  - Launching and polling the Hugging Face Job
  - Verifying the published size against R2
  - Pruning superseded snapshots' LFS objects
  """
  return publish_to_huggingface(
    context,
    graph_id="sec",
    repo_id=env.HF_SEC_DATASET_REPO,
    path_in_repo=SEC_HF_PATH_IN_REPO,
    job_flavor=config.job_flavor,
    job_timeout=config.job_timeout,
    prune_previous=config.prune_previous,
  )
