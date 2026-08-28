"""Tests for the Hugging Face SEC dump downloader."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
import zstandard

from robosystems.scripts import sec_dump

SCRIPT = "robosystems.scripts.sec_dump"
PAYLOAD = b"LBUG" + bytes(range(256)) * 512


def _archive(path, payload=PAYLOAD, content_size=True):
  cctx = zstandard.ZstdCompressor(write_content_size=content_size, write_checksum=True)
  path.write_bytes(cctx.compress(payload))
  return path


@pytest.mark.unit
class TestGraphIdFor:
  def test_strips_archive_suffixes(self):
    assert sec_dump.graph_id_for("sec.lbug.zst") == "sec"
    assert sec_dump.graph_id_for("sec_historical.lbug") == "sec_historical"
    assert sec_dump.graph_id_for("snapshots/sec.lbug.zst") == "sec"


@pytest.mark.unit
class TestPublishedEngineVersion:
  def test_parses_version_from_archive_commit_title(self):
    api = MagicMock()
    api.list_repo_commits.return_value = [
      SimpleNamespace(title="Update README.md"),
      SimpleNamespace(title="sec.lbug.zst: snapshot 2026-07-16 (ladybug 0.18.1)"),
      SimpleNamespace(title="sec.lbug.zst: snapshot 2026-06-01 (ladybug 0.17.0)"),
    ]
    assert (
      sec_dump.published_engine_version(api, "r/s", "sec.lbug.zst", None) == "0.18.1"
    )
    api.list_repo_commits.assert_called_once_with(
      "r/s", repo_type="dataset", revision=None
    )

  def test_none_when_title_carries_no_version(self):
    api = MagicMock()
    api.list_repo_commits.return_value = [SimpleNamespace(title="sec.lbug.zst: manual")]
    assert sec_dump.published_engine_version(api, "r/s", "sec.lbug.zst", None) is None

  def test_none_when_listing_fails(self):
    api = MagicMock()
    api.list_repo_commits.side_effect = RuntimeError("offline")
    assert sec_dump.published_engine_version(api, "r/s", "sec.lbug.zst", None) is None


@pytest.mark.unit
class TestArchiveContentSize:
  def test_reads_recorded_size(self, tmp_path):
    archive = _archive(tmp_path / "a.zst")
    assert sec_dump.archive_content_size(archive) == len(PAYLOAD)

  def test_none_when_size_not_recorded(self, tmp_path):
    archive = _archive(tmp_path / "a.zst", content_size=False)
    assert sec_dump.archive_content_size(archive) is None


@pytest.mark.unit
class TestDecompress:
  def test_roundtrip_lands_file_and_cleans_partial(self, tmp_path):
    archive = _archive(tmp_path / "sec.lbug.zst")
    dest = tmp_path / "lbug-dbs" / "sec.lbug"

    written = sec_dump.decompress(archive, dest, len(PAYLOAD))

    assert written == len(PAYLOAD)
    assert dest.read_bytes() == PAYLOAD
    assert not (tmp_path / "lbug-dbs" / "sec.lbug.partial").exists()

  def test_corrupt_archive_fails_checksum_and_leaves_no_file(self, tmp_path):
    archive = _archive(tmp_path / "sec.lbug.zst")
    blob = bytearray(archive.read_bytes())
    blob[len(blob) // 2] ^= 0xFF
    archive.write_bytes(bytes(blob))
    dest = tmp_path / "sec.lbug"

    with pytest.raises(zstandard.ZstdError):
      sec_dump.decompress(archive, dest, len(PAYLOAD))
    assert not dest.exists()
    assert not (tmp_path / "sec.lbug.partial").exists()

  def test_rejects_payload_without_lbug_magic(self, tmp_path):
    archive = _archive(tmp_path / "x.zst", payload=b"KUZU" + b"\x00" * 100)
    with pytest.raises(RuntimeError, match="LadybugDB"):
      sec_dump.decompress(archive, tmp_path / "x.lbug", None)
    assert not (tmp_path / "x.lbug").exists()

  def test_rejects_size_mismatch(self, tmp_path):
    archive = _archive(tmp_path / "sec.lbug.zst")
    with pytest.raises(RuntimeError, match="Decompressed size"):
      sec_dump.decompress(archive, tmp_path / "sec.lbug", len(PAYLOAD) + 1)
    assert not (tmp_path / "sec.lbug").exists()


@pytest.fixture
def hub(tmp_path):
  """Fake Hub: download materialises a valid archive into the requested dir."""

  def fake_download(repo_id, filename, *, repo_type, revision, local_dir):
    assert repo_type == "dataset"
    return str(_archive(local_dir / filename))

  api = MagicMock()
  api.dataset_info.return_value = SimpleNamespace(
    siblings=[SimpleNamespace(rfilename="sec.lbug.zst", size=1234)]
  )
  api.list_repo_commits.return_value = [
    SimpleNamespace(title="sec.lbug.zst: snapshot 2026-07-16 (ladybug 0.18.1)")
  ]
  with (
    patch("huggingface_hub.hf_hub_download", side_effect=fake_download) as download,
    patch("huggingface_hub.HfApi", return_value=api),
    patch(f"{SCRIPT}.graph_api_running", return_value=False),
    patch(f"{SCRIPT}.local_engine_version", return_value="0.18.1"),
  ):
    yield SimpleNamespace(download=download, api=api)


def _run(tmp_path, *flags):
  dest_dir = tmp_path / "lbug-dbs"
  download_dir = tmp_path / "downloads"
  return (
    sec_dump.main(
      ["--dest-dir", str(dest_dir), "--download-dir", str(download_dir), *flags]
    ),
    dest_dir,
    download_dir,
  )


@pytest.mark.unit
class TestMain:
  def test_downloads_decompresses_and_removes_archive(self, tmp_path, hub):
    code, dest_dir, download_dir = _run(tmp_path)

    assert code == 0
    assert (dest_dir / "sec.lbug").read_bytes() == PAYLOAD
    assert not (download_dir / "sec.lbug.zst").exists()
    hub.download.assert_called_once()
    assert hub.download.call_args.args[:2] == (
      "robosystems/sec-xbrl-knowledge-graphs",
      "sec.lbug.zst",
    )

  def test_keep_archive(self, tmp_path, hub):
    code, _, download_dir = _run(tmp_path, "--keep-archive")
    assert code == 0
    assert (download_dir / "sec.lbug.zst").exists()

  def test_refuses_existing_without_force(self, tmp_path, hub):
    dest_dir = tmp_path / "lbug-dbs"
    dest_dir.mkdir()
    (dest_dir / "sec.lbug").write_bytes(b"LBUG old")

    code, _, _ = _run(tmp_path)

    assert code == 1
    assert (dest_dir / "sec.lbug").read_bytes() == b"LBUG old"
    hub.download.assert_not_called()

  def test_force_replaces_and_drops_stale_wal(self, tmp_path, hub):
    dest_dir = tmp_path / "lbug-dbs"
    dest_dir.mkdir()
    (dest_dir / "sec.lbug").write_bytes(b"LBUG old")
    (dest_dir / "sec.lbug.wal").write_bytes(b"wal")

    code, _, _ = _run(tmp_path, "--force")

    assert code == 0
    assert (dest_dir / "sec.lbug").read_bytes() == PAYLOAD
    assert not (dest_dir / "sec.lbug.wal").exists()

  def test_warns_on_engine_mismatch_but_proceeds(self, tmp_path, hub, caplog):
    with patch(f"{SCRIPT}.local_engine_version", return_value="0.19.0"):
      code, dest_dir, _ = _run(tmp_path)
    assert code == 0
    assert (dest_dir / "sec.lbug").exists()
    assert "written by ladybug 0.18.1; this checkout pins 0.19.0" in caplog.text

  def test_graph_id_and_repo_overrides(self, tmp_path, hub):
    code, dest_dir, _ = _run(
      tmp_path, "--repo", "acme/dump", "--graph-id", "secx", "--force"
    )
    assert code == 0
    assert (dest_dir / "secx.lbug").exists()
    assert hub.download.call_args.args[0] == "acme/dump"
