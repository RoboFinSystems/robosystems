"""Graph storage configuration tests."""

from datetime import UTC, datetime

from robosystems.config.storage.graph import (
  GRAPH_STORAGE,
  GraphStorageConfig,
  GraphStorageType,
  get_backup_key,
  get_backup_metadata_key,
  get_backup_prefix,
  get_backup_uri,
  get_download_extension,
  get_instance_backup_key,
  get_instance_backup_prefix,
  get_instance_backup_uri,
  get_shared_repo_backup_key,
  get_shared_repo_backup_prefix,
  get_shared_repo_database_key,
  get_shared_repo_database_prefix,
  get_staging_key,
  get_staging_prefix,
  get_staging_uri,
)


class TestGraphStorageType:
  """Tests for GraphStorageType enum and registry."""

  def test_all_types_in_registry(self):
    for storage_type in GraphStorageType:
      assert storage_type in GRAPH_STORAGE

  def test_registry_values_are_configs(self):
    for config in GRAPH_STORAGE.values():
      assert isinstance(config, GraphStorageConfig)

  def test_prefixes_end_with_slash(self):
    for config in GRAPH_STORAGE.values():
      assert config.prefix.endswith("/")

  def test_storage_type_values(self):
    assert GraphStorageType.USER_STAGING.value == "user-staging"
    assert GraphStorageType.BACKUPS.value == "graph-backups"
    assert GraphStorageType.DATABASES.value == "graph-databases"


class TestStagingKeys:
  """Tests for user staging S3 key builders."""

  def test_get_staging_key(self):
    result = get_staging_key("user123", "kg456", "Entity", "f789", "data.parquet")
    assert result == "user-staging/user123/kg456/Entity/f789/data.parquet"

  def test_get_staging_prefix_no_filters(self):
    result = get_staging_prefix()
    assert result == "user-staging/"

  def test_get_staging_prefix_user_only(self):
    result = get_staging_prefix(user_id="user123")
    assert result == "user-staging/user123/"

  def test_get_staging_prefix_user_and_graph(self):
    result = get_staging_prefix(user_id="user123", graph_id="kg456")
    assert result == "user-staging/user123/kg456/"

  def test_get_staging_prefix_all_filters(self):
    result = get_staging_prefix(
      user_id="user123", graph_id="kg456", table_name="Entity"
    )
    assert result == "user-staging/user123/kg456/Entity/"

  def test_get_staging_prefix_graph_without_user_ignored(self):
    result = get_staging_prefix(graph_id="kg456")
    assert result == "user-staging/"

  def test_get_staging_prefix_table_without_graph_ignored(self):
    result = get_staging_prefix(user_id="user123", table_name="Entity")
    assert result == "user-staging/user123/"


class TestBackupKeys:
  """Tests for application backup S3 key builders."""

  def test_get_backup_key(self):
    ts = datetime(2024, 1, 15, 12, 30, 45, tzinfo=UTC)
    result = get_backup_key("kg456", "full", ts)
    assert result == "graph-backups/databases/kg456/full/backup-20240115_123045.lbug.gz"

  def test_get_backup_key_custom_extension(self):
    ts = datetime(2024, 6, 1, 0, 0, 0, tzinfo=UTC)
    result = get_backup_key("kg789", "incremental", ts, extension=".tar.gz")
    assert result.endswith(".tar.gz")

  def test_get_backup_metadata_key(self):
    ts = datetime(2024, 1, 15, 12, 30, 45, tzinfo=UTC)
    result = get_backup_metadata_key("kg456", ts)
    assert result == "graph-backups/metadata/kg456/backup-20240115_123045.json"

  def test_get_backup_prefix_no_filters(self):
    result = get_backup_prefix()
    assert result == "graph-backups/databases/"

  def test_get_backup_prefix_graph_only(self):
    result = get_backup_prefix(graph_id="kg456")
    assert result == "graph-backups/databases/kg456/"

  def test_get_backup_prefix_graph_and_type(self):
    result = get_backup_prefix(graph_id="kg456", backup_type="full")
    assert result == "graph-backups/databases/kg456/full/"

  def test_get_backup_prefix_type_without_graph_ignored(self):
    result = get_backup_prefix(backup_type="full")
    assert result == "graph-backups/databases/"


class TestInstanceBackupKeys:
  """Tests for instance-level database backup S3 key builders."""

  def test_get_instance_backup_key(self):
    ts = datetime(2024, 1, 15, 12, 30, 45, tzinfo=UTC)
    result = get_instance_backup_key("prod", "kg456", ts)
    assert result == "graph-databases/prod/kg456/kg456_20240115_123045.tar.gz"

  def test_get_instance_backup_prefix_env_only(self):
    result = get_instance_backup_prefix("staging")
    assert result == "graph-databases/staging/"

  def test_get_instance_backup_prefix_with_graph(self):
    result = get_instance_backup_prefix("prod", graph_id="kg456")
    assert result == "graph-databases/prod/kg456/"


class TestSharedRepoKeys:
  """Tests for shared repository S3 key builders."""

  def test_get_shared_repo_database_key_default_extension(self):
    result = get_shared_repo_database_key("sec")
    assert result == "shared-repositories/databases/sec.lbug"

  def test_get_shared_repo_database_key_duckdb(self):
    result = get_shared_repo_database_key("sec", ".duckdb")
    assert result == "shared-repositories/databases/sec.duckdb"

  def test_get_shared_repo_database_prefix(self):
    result = get_shared_repo_database_prefix()
    assert result == "shared-repositories/databases/"

  def test_get_shared_repo_backup_key(self):
    ts = datetime(2024, 1, 15, 12, 30, 45, tzinfo=UTC)
    result = get_shared_repo_backup_key("sec", ts)
    assert result == "shared-repositories/backups/sec/backup-20240115_123045.tar.gz"

  def test_get_shared_repo_backup_prefix_no_filter(self):
    result = get_shared_repo_backup_prefix()
    assert result == "shared-repositories/backups/"

  def test_get_shared_repo_backup_prefix_with_graph(self):
    result = get_shared_repo_backup_prefix("sec")
    assert result == "shared-repositories/backups/sec/"


class TestDownloadExtension:
  """Tests for deriving the served extension from a stored object key."""

  def test_full_dump_zip(self):
    key = "graph-backups/databases/kg1/full/backup-20240115_123045.lbug.zip"
    assert get_download_extension(key) == ".lbug.zip"

  def test_r2_zstd_snapshot(self):
    assert get_download_extension("downloads/sec/sec.lbug.zst") == ".lbug.zst"

  def test_raw_lbug(self):
    assert get_download_extension("downloads/sec/sec.lbug") == ".lbug"

  def test_compound_extension_wins_over_its_own_suffix(self):
    """ ".lbug.zip" must not be truncated to ".zip" by an earlier match."""
    for key, expected in (
      ("a/b/backup.csv.zip", ".csv.zip"),
      ("a/b/backup.json.zip", ".json.zip"),
      ("a/b/backup.parquet.zip", ".parquet.zip"),
      ("a/b/backup.lbug.gz", ".lbug.gz"),
    ):
      assert get_download_extension(key) == expected

  def test_uppercase_key_matches(self):
    assert get_download_extension("A/B/BACKUP.LBUG.ZST") == ".lbug.zst"

  def test_unknown_key_falls_back_to_default(self):
    assert get_download_extension("a/b/backup.bin") == ".zip"
    assert get_download_extension("a/b/backup.bin", default=".lbug") == ".lbug"


class TestURIBuilders:
  """Tests for full S3 URI builder functions."""

  def test_get_staging_uri(self):
    result = get_staging_uri("my-bucket", "user1", "kg1", "Entity", "f1", "file.csv")
    assert result == "s3://my-bucket/user-staging/user1/kg1/Entity/f1/file.csv"

  def test_get_backup_uri(self):
    ts = datetime(2024, 3, 1, 10, 0, 0, tzinfo=UTC)
    result = get_backup_uri("my-bucket", "kg1", "full", ts)
    assert result.startswith("s3://my-bucket/graph-backups/")

  def test_get_instance_backup_uri(self):
    ts = datetime(2024, 3, 1, 10, 0, 0, tzinfo=UTC)
    result = get_instance_backup_uri("my-bucket", "prod", "kg1", ts)
    assert result.startswith("s3://my-bucket/graph-databases/")
