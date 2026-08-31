#!/usr/bin/env python3
"""Pull the public SEC .lbug dump from Hugging Face into data/lbug-dbs.

The dump is the SEC shared repository as one embedded LadybugDB file, published
monthly as ``sec.lbug.zst`` on the ``robosystems/sec-xbrl-knowledge-graphs``
dataset. This downloads it, streams the decompression (the archive's XXH64
checksum is verified on the way through), lands ``sec.lbug`` in the graph
directory the local stack mounts, and warns if the engine that wrote the file
differs from the ``ladybug`` version pinned in this checkout.

Both sizes — the archive and what it expands to — are read from the dataset at
run time and checked against free space before each step, so no figure is
repeated here; the dataset card carries the current ones. Expect tens of GiB to
download and well over 100 GiB on disk.

Usage:
    just sec-dump                    # see the dataset card for current sizes
    just sec-dump --force            # replace an existing data/lbug-dbs/sec.lbug
    just sec-dump --keep-archive     # keep the .zst after decompression
    just sec-dump-no-restart         # same, without the graph-api restart afterwards

The `just` recipe restarts the graph-api container after a successful run (it
holds a replaced file open until restarted); this script never touches the
stack, so run standalone it only warns.

Then:
    just lbug-query sec "MATCH (e:Entity) RETURN count(e)"
"""

import argparse
import logging
import os
import re
import shutil
import subprocess
import sys
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

logger = logging.getLogger("sec_dump")

DEFAULT_FILENAME = "sec.lbug.zst"
DEFAULT_DEST_DIR = Path("data/lbug-dbs")
DEFAULT_DOWNLOAD_DIR = Path("data/downloads/huggingface")
LBUG_MAGIC = b"LBUG"
ZSTD_HEADER_BYTES = 18
READ_CHUNK_BYTES = 16 * 1024 * 1024
PROGRESS_EVERY_BYTES = 8 * 1024**3
GIB = 1024**3
# The publish job stamps the engine into the commit title:
#   "sec.lbug.zst: snapshot 2026-07-16 (ladybug 0.18.1)"
ENGINE_VERSION_PATTERN = re.compile(r"\(ladybug ([0-9][^)\s]*)\)")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
  from robosystems.config import env

  parser = argparse.ArgumentParser(
    description="Download the public SEC .lbug dump from Hugging Face",
    formatter_class=argparse.RawDescriptionHelpFormatter,
  )
  parser.add_argument(
    "--repo",
    default=env.HF_SEC_DATASET_REPO,
    help="Hugging Face dataset repo (default: %(default)s)",
  )
  parser.add_argument(
    "--filename",
    default=DEFAULT_FILENAME,
    help="Archive to fetch from the dataset (default: %(default)s)",
  )
  parser.add_argument(
    "--revision", default=None, help="Dataset revision (branch, tag, or commit)"
  )
  parser.add_argument(
    "--graph-id",
    default=None,
    help="Local graph id, i.e. the .lbug basename (default: derived from --filename)",
  )
  parser.add_argument(
    "--dest-dir",
    type=Path,
    default=DEFAULT_DEST_DIR,
    help="Graph directory the stack mounts (default: %(default)s)",
  )
  parser.add_argument(
    "--download-dir",
    type=Path,
    default=DEFAULT_DOWNLOAD_DIR,
    help="Where the archive lands before decompression (default: %(default)s)",
  )
  parser.add_argument(
    "--force", action="store_true", help="Replace an existing .lbug at the destination"
  )
  parser.add_argument(
    "--keep-archive",
    action="store_true",
    help="Keep the .zst after decompression (default: delete it)",
  )
  return parser.parse_args(argv)


def graph_id_for(filename: str) -> str:
  """``sec.lbug.zst`` -> ``sec``."""
  name = Path(filename).name
  for suffix in (".lbug.zst", ".zst", ".lbug"):
    if name.endswith(suffix):
      return name[: -len(suffix)]
  return name


def published_engine_version(api, repo_id: str, filename: str, revision) -> str | None:
  """Engine version stamped on the archive's latest commit title, if any."""
  try:
    commits = api.list_repo_commits(repo_id, repo_type="dataset", revision=revision)
  except Exception as e:
    logger.debug(f"Could not list commits for {repo_id}: {e}")
    return None
  for commit in commits:
    if commit.title.startswith(f"{filename}:"):
      match = ENGINE_VERSION_PATTERN.search(commit.title)
      return match.group(1) if match else None
  return None


def local_engine_version() -> str | None:
  try:
    return version("ladybug")
  except PackageNotFoundError:
    return None


def published_archive_size(api, repo_id: str, filename: str, revision) -> int | None:
  try:
    info = api.dataset_info(repo_id, revision=revision, files_metadata=True)
  except Exception as e:
    logger.debug(f"Could not read dataset info for {repo_id}: {e}")
    return None
  for sibling in info.siblings or []:
    if sibling.rfilename == filename:
      return sibling.size
  return None


def archive_content_size(archive: Path) -> int | None:
  """Uncompressed size recorded in the zstd frame header, or None if absent."""
  import zstandard

  with archive.open("rb") as f:
    header = f.read(ZSTD_HEADER_BYTES)
  params = zstandard.get_frame_parameters(header)
  if params.content_size in (
    zstandard.CONTENTSIZE_UNKNOWN,
    zstandard.CONTENTSIZE_ERROR,
  ):
    return None
  return params.content_size


def require_free_space(directory: Path, needed: int, purpose: str) -> None:
  free = shutil.disk_usage(directory).free
  if free < needed:
    raise SystemExit(
      f"Not enough free space in {directory} to {purpose}: "
      f"need {needed / GIB:.1f} GiB, have {free / GIB:.1f} GiB"
    )


def download_archive(
  repo_id: str, filename: str, download_dir: Path, revision: str | None
) -> Path:
  from huggingface_hub import hf_hub_download

  download_dir.mkdir(parents=True, exist_ok=True)
  logger.info(f"Downloading {repo_id}/{filename} -> {download_dir}/")
  return Path(
    hf_hub_download(
      repo_id,
      filename,
      repo_type="dataset",
      revision=revision,
      local_dir=download_dir,
    )
  )


def decompress(archive: Path, dest: Path, expected_size: int | None) -> int:
  """Stream ``archive`` into ``dest`` via a ``.partial`` file; returns bytes written.

  The zstd checksum is verified as the stream is read, the first bytes must
  carry the LadybugDB magic, and the total must match the frame's recorded
  size. Only then is the partial file moved into place.
  """
  import zstandard

  dest.parent.mkdir(parents=True, exist_ok=True)
  partial = dest.with_name(dest.name + ".partial")
  written = 0
  next_report = PROGRESS_EVERY_BYTES
  try:
    with (
      archive.open("rb") as src,
      zstandard.ZstdDecompressor().stream_reader(src) as reader,
      partial.open("wb") as out,
    ):
      first = reader.read(READ_CHUNK_BYTES)
      if not first.startswith(LBUG_MAGIC):
        raise RuntimeError(
          f"{archive.name} does not decompress to a LadybugDB file "
          f"(expected {LBUG_MAGIC!r} header, got {first[:4]!r})"
        )
      chunk = first
      while chunk:
        out.write(chunk)
        written += len(chunk)
        if written >= next_report:
          logger.info(f"  decompressed {written / GIB:.0f} GiB")
          next_report += PROGRESS_EVERY_BYTES
        chunk = reader.read(READ_CHUNK_BYTES)
    if expected_size is not None and written != expected_size:
      raise RuntimeError(
        f"Decompressed size {written} != {expected_size} recorded in the archive"
      )
    os.replace(partial, dest)
  finally:
    partial.unlink(missing_ok=True)
  return written


def remove_stale_wal(dest: Path) -> None:
  """A WAL left by a previous file at this path must not replay onto the new one."""
  wal = dest.with_name(dest.name + ".wal")
  if wal.exists():
    logger.info(f"Removing stale {wal}")
    wal.unlink()


def graph_api_running() -> bool:
  try:
    result = subprocess.run(
      ["docker", "ps", "--filter", "name=graph-api", "--format", "{{.Names}}"],
      capture_output=True,
      text=True,
      timeout=10,
      check=False,
    )
  except (OSError, subprocess.SubprocessError):
    return False
  return bool(result.stdout.strip())


def main(argv: list[str] | None = None) -> int:
  logging.basicConfig(level=logging.INFO, format="%(message)s")
  # Must be set before huggingface_hub initialises hf_xet; saturates the link.
  os.environ.setdefault("HF_XET_HIGH_PERFORMANCE", "1")

  args = parse_args(argv)
  graph_id = args.graph_id or graph_id_for(args.filename)
  dest = args.dest_dir / f"{graph_id}.lbug"

  if dest.exists() and not args.force:
    logger.error(
      f"{dest} already exists ({dest.stat().st_size / GIB:.2f} GiB). "
      "Re-run with --force to replace it."
    )
    return 1

  from huggingface_hub import HfApi

  api = HfApi()
  archive_size = published_archive_size(api, args.repo, args.filename, args.revision)
  if archive_size is not None:
    logger.info(f"{args.repo}/{args.filename}: {archive_size / GIB:.2f} GiB compressed")
    require_free_space(
      _existing_parent(args.download_dir), archive_size, "download the archive"
    )

  published = published_engine_version(api, args.repo, args.filename, args.revision)
  local = local_engine_version()
  if published and local and published != local:
    logger.warning(
      f"The dump was written by ladybug {published}; this checkout pins {local}. "
      "Storage formats can differ across versions - if the graph fails to open, "
      f"check out the release that pins ladybug {published}."
    )
  elif published:
    logger.info(f"Engine: ladybug {published} (matches this checkout)")

  archive = download_archive(args.repo, args.filename, args.download_dir, args.revision)
  expected = archive_content_size(archive)
  if expected is not None:
    logger.info(f"Archive expands to {expected / GIB:.1f} GiB")
    require_free_space(_existing_parent(dest), expected, "decompress the dump")

  if dest.exists() and graph_api_running():
    logger.warning(
      f"The graph API container is running and holds the current {dest.name} "
      "open; it serves the old file until restarted (`just sec-dump` restarts it; "
      "standalone, run `just restart`)."
    )

  logger.info(f"Decompressing {archive.name} -> {dest}")
  written = decompress(archive, dest, expected)
  remove_stale_wal(dest)
  if not args.keep_archive:
    archive.unlink()
    logger.info(f"Removed {archive}")

  logger.info(f"Ready: {dest} ({written / GIB:.1f} GiB)")
  logger.info("Next:")
  logger.info(f'  just lbug-query {graph_id} "MATCH (e:Entity) RETURN count(e)"')
  return 0


def _existing_parent(path: Path) -> Path:
  """Nearest existing ancestor, for disk-usage checks before mkdir."""
  candidate = path.resolve()
  while not candidate.exists():
    candidate = candidate.parent
  return candidate


if __name__ == "__main__":
  sys.exit(main())
