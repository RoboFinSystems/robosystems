"""Tests for the SEC Hugging Face publish asset and its shared helper."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from dagster import MaterializeResult, build_asset_context

from robosystems.adapters.sec.pipeline.configs import SECHFPublishConfig
from robosystems.adapters.sec.pipeline.hf_publish import (
  SEC_HF_PATH_IN_REPO,
  sec_lbug_hf_published,
)
from robosystems.dagster.assets.shared_repositories import huggingface as hf_module
from robosystems.dagster.assets.shared_repositories.huggingface import (
  HF_JOB_IMAGE,
  publish_to_huggingface,
)

HELPER = "robosystems.dagster.assets.shared_repositories.huggingface"
REPO = "robosystems/sec-xbrl-knowledge-graphs"
PATH = "sec.lbug.zst"
SIZE = 37_639_013_896
SHA = "a" * 64


def _job(stage, message=None):
  return SimpleNamespace(
    id="job123",
    url="https://huggingface.co/jobs/robosystems/job123",
    status=SimpleNamespace(stage=stage, message=message),
  )


def _sibling(size=SIZE, sha256=SHA):
  return SimpleNamespace(
    rfilename=PATH,
    size=size,
    lfs={"size": size, "sha256": sha256, "pointer_size": 134},
  )


def _lfs(filename, file_oid):
  return SimpleNamespace(filename=filename, file_oid=file_oid)


@pytest.fixture
def prod_env():
  with patch(f"{HELPER}.env") as env:
    env.ENVIRONMENT = "prod"
    env.HF_TOKEN = "hf_test_token"
    env.get_r2_config.return_value = {
      "endpoint_url": "https://r2.example",
      "aws_access_key_id": "k",
      "aws_secret_access_key": "s",
      "region_name": "auto",
    }
    yield env


@pytest.fixture
def r2_client():
  with patch("boto3.client") as client_factory:
    client = MagicMock()
    from datetime import UTC, datetime

    client.head_object.return_value = {
      "ContentLength": SIZE,
      "LastModified": datetime(2026, 7, 16, 6, 51, 25, tzinfo=UTC),
    }
    client.generate_presigned_url.return_value = "https://r2.example/presigned"
    client_factory.return_value = client
    yield client


@pytest.fixture
def r2_destination():
  with patch(f"{HELPER}._build_r2_destination") as build:
    build.return_value = {
      "bucket": "robosystems-downloads",
      "r2_key": "downloads/sec/sec.lbug.zst",
      "r2_uri": "r2://robosystems-downloads/downloads/sec/sec.lbug.zst",
    }
    yield build


@pytest.fixture
def hf_api():
  with patch(f"{HELPER}.HfApi") as api_cls, patch(f"{HELPER}.time.sleep"):
    api = MagicMock()
    api.run_job.return_value = _job("SCHEDULING")
    api.inspect_job.side_effect = [_job("RUNNING"), _job("COMPLETED")]
    api.dataset_info.return_value = SimpleNamespace(siblings=[_sibling()])
    api.list_lfs_files.return_value = [
      _lfs(PATH, "b" * 64),
      _lfs(PATH, SHA),
      _lfs("README.md", "c" * 64),
    ]
    api_cls.return_value = api
    yield api


@pytest.mark.unit
class TestSecLbugHfPublished:
  """Tests for the sec_lbug_hf_published asset."""

  @patch("robosystems.adapters.sec.pipeline.hf_publish.env")
  @patch("robosystems.adapters.sec.pipeline.hf_publish.publish_to_huggingface")
  def test_delegates_with_config(self, mock_publish, mock_env):
    mock_env.HF_SEC_DATASET_REPO = REPO
    mock_publish.return_value = MaterializeResult(metadata={"graph_id": "sec"})

    context = build_asset_context()
    config = SECHFPublishConfig(
      job_flavor="cpu-xl", job_timeout="8h", prune_previous=False
    )
    result = sec_lbug_hf_published(context, config)

    mock_publish.assert_called_once_with(
      context,
      graph_id="sec",
      repo_id=REPO,
      path_in_repo=SEC_HF_PATH_IN_REPO,
      job_flavor="cpu-xl",
      job_timeout="8h",
      prune_previous=False,
    )
    assert isinstance(result, MaterializeResult)

  def test_defaults_target_cpu_basic(self):
    config = SECHFPublishConfig()
    assert config.job_flavor == "cpu-basic"
    assert config.job_timeout == "6h"
    assert config.prune_previous is True
    assert SEC_HF_PATH_IN_REPO == "sec.lbug.zst"


@pytest.mark.unit
class TestPublishToHuggingface:
  """Tests for the shared publish_to_huggingface helper."""

  def test_skips_in_dev(self):
    with patch(f"{HELPER}.env") as env:
      env.ENVIRONMENT = "dev"
      result = publish_to_huggingface(
        build_asset_context(), graph_id="sec", repo_id=REPO, path_in_repo=PATH
      )
    assert result.metadata["status"] == "skipped"
    assert result.metadata["reason"] == "dev_environment"

  def test_requires_hf_token(self, prod_env):
    prod_env.HF_TOKEN = ""
    with pytest.raises(RuntimeError, match="HF_TOKEN"):
      publish_to_huggingface(
        build_asset_context(), graph_id="sec", repo_id=REPO, path_in_repo=PATH
      )

  def test_requires_r2(self, prod_env):
    prod_env.get_r2_config.return_value = {}
    with pytest.raises(RuntimeError, match="R2"):
      publish_to_huggingface(
        build_asset_context(), graph_id="sec", repo_id=REPO, path_in_repo=PATH
      )

  def test_rejects_non_shared_graph(self, prod_env, r2_client, r2_destination):
    with pytest.raises(ValueError, match="not a shared repository"):
      publish_to_huggingface(
        build_asset_context(),
        graph_id="kg1a2b3c4d5e6f7a8b9c0d",
        repo_id=REPO,
        path_in_repo=PATH,
      )

  def test_happy_path_launches_job_verifies_and_prunes(
    self, prod_env, r2_client, r2_destination, hf_api
  ):
    result = publish_to_huggingface(
      build_asset_context(),
      graph_id="sec",
      repo_id=REPO,
      path_in_repo=PATH,
      job_flavor="cpu-basic",
      job_timeout="6h",
    )

    r2_client.head_object.assert_called_once_with(
      Bucket="robosystems-downloads", Key="downloads/sec/sec.lbug.zst"
    )
    r2_client.generate_presigned_url.assert_called_once()
    presign_kwargs = r2_client.generate_presigned_url.call_args.kwargs
    assert presign_kwargs["Params"] == {
      "Bucket": "robosystems-downloads",
      "Key": "downloads/sec/sec.lbug.zst",
    }
    assert presign_kwargs["ExpiresIn"] == hf_module.HF_PRESIGN_TTL_SECONDS

    hf_api.run_job.assert_called_once()
    job_kwargs = hf_api.run_job.call_args.kwargs
    assert job_kwargs["image"] == HF_JOB_IMAGE
    assert job_kwargs["command"][:2] == ["bash", "-c"]
    assert "hf upload" in job_kwargs["command"][2]
    assert job_kwargs["flavor"] == "cpu-basic"
    assert job_kwargs["timeout"] == "6h"
    assert job_kwargs["namespace"] == "robosystems"
    # Credentials travel as job secrets, never plain env
    assert job_kwargs["secrets"] == {
      "HF_TOKEN": "hf_test_token",
      "SRC_URL": "https://r2.example/presigned",
    }
    assert job_kwargs["env"]["REPO_ID"] == REPO
    assert job_kwargs["env"]["PATH_IN_REPO"] == PATH
    assert job_kwargs["env"]["COMMIT_MESSAGE"].startswith(
      "sec.lbug.zst: snapshot 2026-07-16 (ladybug "
    )
    assert "HF_TOKEN" not in job_kwargs["env"]
    assert "SRC_URL" not in job_kwargs["env"]

    assert hf_api.inspect_job.call_count == 2
    hf_api.dataset_info.assert_called_once_with(REPO, files_metadata=True)

    # Only the superseded object at the same path is pruned
    hf_api.permanently_delete_lfs_files.assert_called_once()
    (stale,) = hf_api.permanently_delete_lfs_files.call_args.args[1:]
    assert [f.file_oid for f in stale] == ["b" * 64]
    assert hf_api.permanently_delete_lfs_files.call_args.kwargs == {
      "repo_type": "dataset",
      "rewrite_history": True,
    }

    assert result.metadata["hf_repo"] == REPO
    assert result.metadata["hf_path"] == PATH
    assert result.metadata["hf_job_id"] == "job123"
    assert result.metadata["compressed_size_bytes"] == SIZE
    assert result.metadata["sha256"] == SHA
    assert result.metadata["pruned_previous"] == 1
    assert result.metadata["snapshot_at"] == "2026-07-16T06:51:25+00:00"

  def test_prune_can_be_disabled(self, prod_env, r2_client, r2_destination, hf_api):
    result = publish_to_huggingface(
      build_asset_context(),
      graph_id="sec",
      repo_id=REPO,
      path_in_repo=PATH,
      prune_previous=False,
    )
    hf_api.list_lfs_files.assert_not_called()
    hf_api.permanently_delete_lfs_files.assert_not_called()
    assert result.metadata["pruned_previous"] == 0

  def test_nothing_to_prune_when_only_current_exists(
    self, prod_env, r2_client, r2_destination, hf_api
  ):
    hf_api.list_lfs_files.return_value = [_lfs(PATH, SHA)]
    result = publish_to_huggingface(
      build_asset_context(), graph_id="sec", repo_id=REPO, path_in_repo=PATH
    )
    hf_api.permanently_delete_lfs_files.assert_not_called()
    assert result.metadata["pruned_previous"] == 0

  def test_job_failure_raises_with_log_tail(
    self, prod_env, r2_client, r2_destination, hf_api
  ):
    hf_api.inspect_job.side_effect = [_job("RUNNING"), _job("ERROR", "exit 22")]
    hf_api.fetch_job_logs.return_value = iter(["curl: (22) HTTP 403"])

    with pytest.raises(RuntimeError, match=r"(?s)ended ERROR: exit 22.*HTTP 403"):
      publish_to_huggingface(
        build_asset_context(), graph_id="sec", repo_id=REPO, path_in_repo=PATH
      )
    hf_api.dataset_info.assert_not_called()
    hf_api.permanently_delete_lfs_files.assert_not_called()

  def test_size_mismatch_raises_before_prune(
    self, prod_env, r2_client, r2_destination, hf_api
  ):
    hf_api.dataset_info.return_value = SimpleNamespace(
      siblings=[_sibling(size=SIZE - 1)]
    )
    with pytest.raises(RuntimeError, match="Size mismatch"):
      publish_to_huggingface(
        build_asset_context(), graph_id="sec", repo_id=REPO, path_in_repo=PATH
      )
    hf_api.permanently_delete_lfs_files.assert_not_called()

  def test_missing_file_after_upload_raises_after_retries(
    self, prod_env, r2_client, r2_destination, hf_api
  ):
    hf_api.dataset_info.return_value = SimpleNamespace(siblings=[])
    with pytest.raises(RuntimeError, match="not found"):
      publish_to_huggingface(
        build_asset_context(), graph_id="sec", repo_id=REPO, path_in_repo=PATH
      )
    assert hf_api.dataset_info.call_count == hf_module.HF_VERIFY_ATTEMPTS
    hf_api.permanently_delete_lfs_files.assert_not_called()

  def test_lagging_listing_is_retried(
    self, prod_env, r2_client, r2_destination, hf_api
  ):
    hf_api.dataset_info.side_effect = [
      SimpleNamespace(siblings=[]),
      SimpleNamespace(siblings=[_sibling()]),
    ]
    result = publish_to_huggingface(
      build_asset_context(), graph_id="sec", repo_id=REPO, path_in_repo=PATH
    )
    assert hf_api.dataset_info.call_count == 2
    assert result.metadata["sha256"] == SHA

  def test_enum_stage_values_are_normalised(
    self, prod_env, r2_client, r2_destination, hf_api
  ):
    from enum import Enum

    class Stage(str, Enum):
      RUNNING = "RUNNING"
      COMPLETED = "COMPLETED"

    hf_api.inspect_job.side_effect = [
      _job(Stage.RUNNING),
      _job(Stage.COMPLETED),
    ]
    result = publish_to_huggingface(
      build_asset_context(), graph_id="sec", repo_id=REPO, path_in_repo=PATH
    )
    assert result.metadata["hf_job_id"] == "job123"
