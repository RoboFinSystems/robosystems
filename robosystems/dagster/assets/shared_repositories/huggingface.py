"""Shared Repository Publish Helper (Hugging Face).

The snapshot reaches Hugging Face without its bytes touching AWS again: the R2
publish already paid the one egress, and R2 downloads are free. So the copy
runs as a Hugging Face Job (compute on the Hub's side) that pulls a presigned
R2 URL onto its local disk and uploads to the dataset repo. Dagster only
presigns, launches, polls, verifies, and prunes.
"""

import time
from datetime import UTC, datetime
from importlib.metadata import PackageNotFoundError, version
from typing import Any

from dagster import AssetExecutionContext, MaterializeResult, MetadataValue
from huggingface_hub import HfApi

from robosystems.config import env
from robosystems.dagster.assets.shared_repositories.publish import (
  _build_r2_destination,
  _validate_shared_graph_id,
)

HF_JOB_IMAGE = "python:3.12"
HF_JOB_POLL_SECONDS = 30
HF_JOB_MAX_WAIT_SECONDS = 24 * 60 * 60
HF_PRESIGN_TTL_SECONDS = 12 * 60 * 60
HF_TERMINAL_STAGES = frozenset({"COMPLETED", "ERROR", "CANCELED", "DELETED"})

# Runs inside the Hugging Face Job container: pull from R2 (free egress) onto
# the job's local disk, then push to the Hub. The presigned URL and the write
# token arrive as job secrets; everything else as plain env.
HF_JOB_SCRIPT = """\
set -euo pipefail
pip install --quiet --upgrade 'huggingface_hub>=1.19'
curl --fail --silent --show-error --location --retry 5 --retry-all-errors \\
  --output /tmp/payload "$SRC_URL"
hf upload "$REPO_ID" /tmp/payload "$PATH_IN_REPO" --repo-type dataset \\
  --commit-message "$COMMIT_MESSAGE"
"""


def publish_to_huggingface(
  context: AssetExecutionContext,
  graph_id: str,
  repo_id: str,
  path_in_repo: str,
  *,
  job_flavor: str = "cpu-basic",
  job_timeout: str = "6h",
  prune_previous: bool = True,
) -> MaterializeResult:
  """Copy a shared repository's R2 snapshot to a public Hugging Face dataset.

  Presigns the R2 object, runs a Hugging Face Job that downloads it and uploads
  it to ``repo_id`` at ``path_in_repo`` (a fixed path, overwritten each
  snapshot so the download URL stays stable), waits for the job, verifies the
  published size against R2, and optionally deletes the LFS objects of earlier
  snapshots at the same path so storage stays one snapshot deep.
  """
  context.log.info(f"Publishing {graph_id} R2 snapshot to Hugging Face {repo_id}")

  if env.ENVIRONMENT == "dev":
    context.log.info("Skipping Hugging Face publish in dev environment")
    return MaterializeResult(
      metadata={
        "status": "skipped",
        "reason": "dev_environment",
        "graph_id": graph_id,
      }
    )

  import boto3

  _validate_shared_graph_id(graph_id)

  token = env.HF_TOKEN
  if not token:
    raise RuntimeError("HF_TOKEN not configured")
  r2_config = env.get_r2_config()
  if not r2_config:
    raise RuntimeError("R2 not configured")

  r2_info = _build_r2_destination(graph_id)
  bucket = r2_info["bucket"]
  r2_key = r2_info["r2_key"]

  r2_client = boto3.client("s3", **r2_config)
  head = r2_client.head_object(Bucket=bucket, Key=r2_key)
  source_size = head["ContentLength"]
  snapshot_at = head["LastModified"]
  source_url = r2_client.generate_presigned_url(
    "get_object",
    Params={"Bucket": bucket, "Key": r2_key},
    ExpiresIn=HF_PRESIGN_TTL_SECONDS,
  )
  context.log.info(
    f"Source: {r2_info['r2_uri']} "
    f"({source_size / (1024**3):.2f} GiB, modified {snapshot_at})"
  )

  engine_version = _engine_version()
  commit_message = (
    f"{path_in_repo}: snapshot {snapshot_at:%Y-%m-%d} (ladybug {engine_version})"
  )

  api = HfApi(token=token)
  namespace = repo_id.split("/", 1)[0]
  job = api.run_job(
    image=HF_JOB_IMAGE,
    command=["bash", "-c", HF_JOB_SCRIPT],
    env={
      "REPO_ID": repo_id,
      "PATH_IN_REPO": path_in_repo,
      "COMMIT_MESSAGE": commit_message,
      "HF_XET_HIGH_PERFORMANCE": "1",
    },
    secrets={"HF_TOKEN": token, "SRC_URL": source_url},
    flavor=job_flavor,
    timeout=job_timeout,
    namespace=namespace,
    labels={"pipeline": "shared_repositories", "graph_id": graph_id},
  )
  context.log.info(
    f"Hugging Face job {job.id} launched ({job_flavor}, timeout {job_timeout}): "
    f"{job.url}"
  )

  final_stage, final_message = _wait_for_job(context, api, job.id, namespace)
  if final_stage != "COMPLETED":
    log_tail = "\n".join(
      list(api.fetch_job_logs(job_id=job.id, namespace=namespace))[-40:]
    )
    raise RuntimeError(
      f"Hugging Face job {job.id} ended {final_stage}: {final_message}\n{log_tail}"
    )

  published = _published_file(api, repo_id, path_in_repo)
  if published["size"] != source_size:
    raise RuntimeError(
      f"Size mismatch for {repo_id}/{path_in_repo}: "
      f"R2 {source_size} bytes vs Hub {published['size']} bytes"
    )

  pruned = (
    _prune_previous(api, repo_id, path_in_repo, published["sha256"])
    if prune_previous
    else 0
  )
  context.log.info(
    f"Published {repo_id}/{path_in_repo} ({source_size / (1024**3):.2f} GiB, "
    f"sha256 {published['sha256']}); pruned {pruned} previous snapshot(s)"
  )

  return MaterializeResult(
    metadata={
      "hf_repo": repo_id,
      "hf_path": path_in_repo,
      "hf_url": MetadataValue.url(f"https://huggingface.co/datasets/{repo_id}"),
      "hf_job_id": job.id,
      "hf_job_url": MetadataValue.url(job.url or ""),
      "source_r2_uri": r2_info["r2_uri"],
      "snapshot_at": snapshot_at.isoformat(),
      "compressed_size_bytes": source_size,
      "file_size_gb": round(source_size / (1024**3), 2),
      "sha256": published["sha256"] or "",
      "engine_version": engine_version,
      "commit_message": commit_message,
      "pruned_previous": pruned,
      "graph_id": graph_id,
      "published_at": datetime.now(UTC).isoformat(),
    }
  )


def _engine_version() -> str:
  """The LadybugDB version pinned in this deployment — the one that wrote the file."""
  try:
    return version("ladybug")
  except PackageNotFoundError:
    return "unknown"


def _wait_for_job(
  context: AssetExecutionContext, api: HfApi, job_id: str, namespace: str
) -> tuple[str, str | None]:
  """Poll the job until a terminal stage; returns (stage, message)."""
  deadline = time.monotonic() + HF_JOB_MAX_WAIT_SECONDS
  last_stage = None
  while True:
    info = api.inspect_job(job_id=job_id, namespace=namespace)
    stage = str(getattr(info.status.stage, "value", info.status.stage))
    if stage != last_stage:
      context.log.info(f"Hugging Face job {job_id}: {stage}")
      last_stage = stage
    if stage in HF_TERMINAL_STAGES:
      return stage, info.status.message
    if time.monotonic() > deadline:
      raise RuntimeError(
        f"Hugging Face job {job_id} still {stage} after "
        f"{HF_JOB_MAX_WAIT_SECONDS}s; giving up"
      )
    time.sleep(HF_JOB_POLL_SECONDS)


def _published_file(api: HfApi, repo_id: str, path_in_repo: str) -> dict[str, Any]:
  """Size and LFS sha256 of ``path_in_repo`` as the Hub now serves it."""
  info = api.dataset_info(repo_id, files_metadata=True)
  for sibling in info.siblings or []:
    if sibling.rfilename == path_in_repo:
      lfs = sibling.lfs
      return {
        "size": sibling.size,
        "sha256": lfs["sha256"] if lfs else None,
      }
  raise RuntimeError(f"{path_in_repo} not found in {repo_id} after upload")


def _prune_previous(
  api: HfApi, repo_id: str, path_in_repo: str, current_sha256: str | None
) -> int:
  """Delete LFS objects of earlier snapshots at the same path; returns the count.

  Rewrites history so no commit still points at the deleted objects. Storage
  is what this buys: each snapshot is tens of GB, and a superseded one has no
  audience — the current file is the product.
  """
  if not current_sha256:
    return 0
  stale = [
    lfs_file
    for lfs_file in api.list_lfs_files(repo_id, repo_type="dataset")
    if lfs_file.filename == path_in_repo and lfs_file.file_oid != current_sha256
  ]
  if stale:
    api.permanently_delete_lfs_files(
      repo_id, stale, repo_type="dataset", rewrite_history=True
    )
  return len(stale)
