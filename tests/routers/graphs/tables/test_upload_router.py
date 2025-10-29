import pytest
from unittest.mock import Mock, AsyncMock, patch

from robosystems.routers.graphs.tables.upload import get_upload_url
from robosystems.models.api.table import FileUploadRequest


@pytest.mark.unit
class TestUploadRouterAutoTableCreation:
  @pytest.mark.asyncio
  async def test_auto_creates_node_table_for_pascal_case_name(self):
    graph_id = "kg_test_123"
    table_name = "Company"
    file_request = FileUploadRequest(
      file_name="companies.parquet", content_type="application/x-parquet"
    )

    mock_user = Mock()
    mock_user.id = "user_123"

    mock_db = Mock()
    mock_created_table = Mock()
    mock_created_table.id = "table_123"
    mock_created_table.table_name = "Company"

    with patch(
      "robosystems.routers.graphs.tables.upload.get_universal_repository",
      new_callable=AsyncMock,
    ) as mock_get_repo:
      mock_get_repo.return_value = Mock()

      with patch(
        "robosystems.models.iam.graph_table.GraphTable.get_by_name",
        return_value=None,
      ):
        with patch(
          "robosystems.models.iam.graph_table.GraphTable.create",
          return_value=mock_created_table,
        ) as mock_create:
          with patch("robosystems.adapters.s3.S3Client") as mock_s3_client_class:
            mock_s3 = Mock()
            mock_s3.s3_client.generate_presigned_url = Mock(
              return_value="https://s3.url"
            )
            mock_s3_client_class.return_value = mock_s3

            with patch(
              "robosystems.models.iam.graph_file.GraphFile.create"
            ) as mock_file_create:
              mock_file = Mock()
              mock_file.id = "file_123"
              mock_file_create.return_value = mock_file

              await get_upload_url(
                graph_id=graph_id,
                table_name=table_name,
                request=file_request,
                current_user=mock_user,
                _rate_limit=None,
                db=mock_db,
              )

              mock_create.assert_called_once()
              call_kwargs = mock_create.call_args[1]
              assert call_kwargs["graph_id"] == graph_id
              assert call_kwargs["table_name"] == "Company"
              assert call_kwargs["table_type"] == "node"

  @pytest.mark.asyncio
  async def test_auto_creates_relationship_table_for_screaming_snake_case(self):
    graph_id = "kg_test_456"
    table_name = "PERSON_WORKS_FOR_COMPANY"
    file_request = FileUploadRequest(
      file_name="relationships.parquet", content_type="application/x-parquet"
    )

    mock_user = Mock()
    mock_user.id = "user_456"

    mock_db = Mock()
    mock_created_table = Mock()
    mock_created_table.id = "table_456"

    with patch(
      "robosystems.routers.graphs.tables.upload.get_universal_repository",
      new_callable=AsyncMock,
    ) as mock_get_repo:
      mock_get_repo.return_value = Mock()

      with patch(
        "robosystems.models.iam.graph_table.GraphTable.get_by_name",
        return_value=None,
      ):
        with patch(
          "robosystems.models.iam.graph_table.GraphTable.create",
          return_value=mock_created_table,
        ) as mock_create:
          with patch("robosystems.adapters.s3.S3Client") as mock_s3_client_class:
            mock_s3 = Mock()
            mock_s3.s3_client.generate_presigned_url = Mock(
              return_value="https://s3.url"
            )
            mock_s3_client_class.return_value = mock_s3

            with patch(
              "robosystems.models.iam.graph_file.GraphFile.create"
            ) as mock_file_create:
              mock_file = Mock()
              mock_file.id = "file_456"
              mock_file_create.return_value = mock_file

              await get_upload_url(
                graph_id=graph_id,
                table_name=table_name,
                request=file_request,
                current_user=mock_user,
                _rate_limit=None,
                db=mock_db,
              )

              mock_create.assert_called_once()
              call_kwargs = mock_create.call_args[1]
              assert call_kwargs["graph_id"] == graph_id
              assert call_kwargs["table_name"] == "PERSON_WORKS_FOR_COMPANY"
              assert call_kwargs["table_type"] == "relationship"

  @pytest.mark.asyncio
  async def test_does_not_create_table_if_exists(self):
    graph_id = "kg_test_789"
    table_name = "Person"
    file_request = FileUploadRequest(
      file_name="people.parquet", content_type="application/x-parquet"
    )

    mock_user = Mock()
    mock_user.id = "user_789"

    mock_db = Mock()
    existing_table = Mock()
    existing_table.id = "existing_table_123"

    with patch(
      "robosystems.routers.graphs.tables.upload.get_universal_repository",
      new_callable=AsyncMock,
    ) as mock_get_repo:
      mock_get_repo.return_value = Mock()

      with patch(
        "robosystems.models.iam.graph_table.GraphTable.get_by_name",
        return_value=existing_table,
      ):
        with patch(
          "robosystems.models.iam.graph_table.GraphTable.create"
        ) as mock_create:
          with patch("robosystems.adapters.s3.S3Client") as mock_s3_client_class:
            mock_s3 = Mock()
            mock_s3.s3_client.generate_presigned_url = Mock(
              return_value="https://s3.url"
            )
            mock_s3_client_class.return_value = mock_s3

            with patch(
              "robosystems.models.iam.graph_file.GraphFile.create"
            ) as mock_file_create:
              mock_file = Mock()
              mock_file.id = "file_789"
              mock_file_create.return_value = mock_file

              await get_upload_url(
                graph_id=graph_id,
                table_name=table_name,
                request=file_request,
                current_user=mock_user,
                _rate_limit=None,
                db=mock_db,
              )

              mock_create.assert_not_called()
