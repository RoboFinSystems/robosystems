from unittest.mock import AsyncMock, Mock, patch

import pytest
from sqlalchemy.orm import Session

from robosystems.operations.graph.entity_graph_service import EntityGraphService


@pytest.mark.unit
class TestEntityCreationFlag:
  @pytest.fixture
  def mock_session(self):
    session = Mock(spec=Session)
    session.query = Mock()
    session.commit = Mock()
    session.add = Mock()
    session.refresh = Mock()
    return session

  @pytest.fixture
  def entity_service(self, mock_session):
    return EntityGraphService(mock_session)

  @pytest.mark.asyncio
  async def test_create_entity_true_creates_entity_node(
    self, entity_service, mock_session
  ):
    entity_data = {
      "name": "Test Company",
      "uri": "https://test.com",
      "cik": "0001234567",
      "extensions": [],
      "create_entity": True,
    }

    with patch.object(entity_service, "_generate_graph_id", return_value="kg_test_123"):
      with patch(
        "robosystems.models.iam.org_limits.OrgLimits.get_or_create_for_org"
      ) as mock_user_limits:
        mock_limits = Mock()
        mock_limits.can_create_graph = Mock(
          return_value=(True, "User can create graph")
        )
        mock_user_limits.return_value = mock_limits

        with patch(
          "robosystems.models.iam.org_user.OrgUser.get_user_orgs"
        ) as mock_get_user_orgs:
          mock_org_user = Mock()
          mock_org_user.org_id = "org_test_123"
          mock_get_user_orgs.return_value = [mock_org_user]

          with patch(
            "robosystems.operations.graph.entity_graph_service.LadybugAllocationManager"
          ) as mock_allocation_mgr:
            mock_mgr_instance = AsyncMock()
            mock_location = Mock()
            mock_location.instance_id = "instance_123"
            mock_location.private_ip = "10.0.0.1"
            mock_mgr_instance.allocate_database = AsyncMock(return_value=mock_location)
            mock_allocation_mgr.return_value = mock_mgr_instance

            with patch(
              "robosystems.operations.graph.entity_graph_service.get_graph_client_for_instance"
            ) as mock_get_client:
              mock_lbug_client = AsyncMock()

              # Create a mock exception that looks like a 404
              not_found_error = Exception("Not found")
              not_found_error.status_code = 404

              mock_lbug_client.get_database = AsyncMock(side_effect=not_found_error)
              mock_lbug_client.create_database = AsyncMock()
              mock_lbug_client.install_schema = AsyncMock(
                return_value={"status": "success"}
              )
              mock_lbug_client.close = AsyncMock()
              mock_get_client.return_value = mock_lbug_client

              with patch(
                "robosystems.models.iam.graph.Graph.create"
              ) as mock_graph_create:
                mock_graph = Mock()
                mock_graph.graph_id = "kg_test_123"
                mock_graph_create.return_value = mock_graph

                with patch("robosystems.models.iam.graph_schema.GraphSchema.create"):
                  with patch(
                    "robosystems.operations.graph.table_service.TableService.create_tables_from_schema",
                    return_value=[],
                  ):
                    with patch.object(
                      entity_service,
                      "_install_entity_schema",
                      new_callable=AsyncMock,
                      return_value="CREATE NODE TABLE Entity(...);",
                    ):
                      with patch.object(
                        entity_service,
                        "_create_entity_in_graph_lbug",
                        new_callable=AsyncMock,
                      ) as mock_create_entity:
                        mock_entity_response = Mock()
                        mock_entity_response.name = "Test Company"
                        mock_entity_response.model_dump = Mock(
                          return_value={
                            "name": "Test Company",
                            "uri": "https://test.com",
                          }
                        )
                        mock_create_entity.return_value = mock_entity_response

                        with patch(
                          "robosystems.operations.graph.credit_service.CreditService"
                        ):
                          result = await entity_service.create_entity_with_new_graph(
                            entity_data_dict=entity_data,
                            user_id="user_123",
                            tier="ladybug-standard",
                          )

                          mock_create_entity.assert_called_once()
                          assert result["graph_id"] == "kg_test_123"
                          assert result["entity"] is not None
                          assert result["entity"]["name"] == "Test Company"

  @pytest.mark.asyncio
  async def test_create_entity_false_skips_entity_node(
    self, entity_service, mock_session
  ):
    entity_data = {
      "name": "Test Company",
      "uri": "https://test.com",
      "cik": "0001234567",
      "extensions": [],
      "create_entity": False,
    }

    with patch.object(entity_service, "_generate_graph_id", return_value="kg_test_456"):
      with patch(
        "robosystems.models.iam.org_limits.OrgLimits.get_or_create_for_org"
      ) as mock_user_limits:
        mock_limits = Mock()
        mock_limits.can_create_graph = Mock(
          return_value=(True, "User can create graph")
        )
        mock_user_limits.return_value = mock_limits

        with patch(
          "robosystems.models.iam.org_user.OrgUser.get_user_orgs"
        ) as mock_get_user_orgs:
          mock_org_user = Mock()
          mock_org_user.org_id = "org_test_456"
          mock_get_user_orgs.return_value = [mock_org_user]

          with patch(
            "robosystems.operations.graph.entity_graph_service.LadybugAllocationManager"
          ) as mock_allocation_mgr:
            mock_mgr_instance = AsyncMock()
            mock_location = Mock()
            mock_location.instance_id = "instance_456"
            mock_location.private_ip = "10.0.0.2"
            mock_mgr_instance.allocate_database = AsyncMock(return_value=mock_location)
            mock_allocation_mgr.return_value = mock_mgr_instance

            with patch(
              "robosystems.operations.graph.entity_graph_service.get_graph_client_for_instance"
            ) as mock_get_client:
              mock_lbug_client = AsyncMock()

              # Create a mock exception that looks like a 404
              not_found_error = Exception("Not found")
              not_found_error.status_code = 404

              mock_lbug_client.get_database = AsyncMock(side_effect=not_found_error)
              mock_lbug_client.create_database = AsyncMock()
              mock_lbug_client.install_schema = AsyncMock(
                return_value={"status": "success"}
              )
              mock_lbug_client.close = AsyncMock()
              mock_get_client.return_value = mock_lbug_client

              with patch(
                "robosystems.models.iam.graph.Graph.create"
              ) as mock_graph_create:
                mock_graph = Mock()
                mock_graph.graph_id = "kg_test_456"
                mock_graph_create.return_value = mock_graph

                with patch("robosystems.models.iam.graph_schema.GraphSchema.create"):
                  with patch(
                    "robosystems.operations.graph.table_service.TableService.create_tables_from_schema",
                    return_value=[],
                  ):
                    with patch.object(
                      entity_service,
                      "_install_entity_schema",
                      new_callable=AsyncMock,
                      return_value="CREATE NODE TABLE Entity(...);",
                    ):
                      with patch.object(
                        entity_service,
                        "_create_entity_in_graph_lbug",
                        new_callable=AsyncMock,
                      ) as mock_create_entity:
                        with patch(
                          "robosystems.operations.graph.credit_service.CreditService"
                        ):
                          result = await entity_service.create_entity_with_new_graph(
                            entity_data_dict=entity_data,
                            user_id="user_123",
                            tier="ladybug-standard",
                          )

                          mock_create_entity.assert_not_called()
                          assert result["graph_id"] == "kg_test_456"
                          assert result["entity"] is None

  @pytest.mark.asyncio
  async def test_create_entity_defaults_to_true_when_not_specified(
    self, entity_service, mock_session
  ):
    entity_data = {
      "name": "Test Company",
      "uri": "https://test.com",
      "cik": "0001234567",
      "extensions": [],
    }

    with patch.object(entity_service, "_generate_graph_id", return_value="kg_test_789"):
      with patch(
        "robosystems.models.iam.org_limits.OrgLimits.get_or_create_for_org"
      ) as mock_user_limits:
        mock_limits = Mock()
        mock_limits.can_create_graph = Mock(
          return_value=(True, "User can create graph")
        )
        mock_user_limits.return_value = mock_limits

        with patch(
          "robosystems.models.iam.org_user.OrgUser.get_user_orgs"
        ) as mock_get_user_orgs:
          mock_org_user = Mock()
          mock_org_user.org_id = "org_test_789"
          mock_get_user_orgs.return_value = [mock_org_user]

          with patch(
            "robosystems.operations.graph.entity_graph_service.LadybugAllocationManager"
          ) as mock_allocation_mgr:
            mock_mgr_instance = AsyncMock()
            mock_location = Mock()
            mock_location.instance_id = "instance_789"
            mock_location.private_ip = "10.0.0.3"
            mock_mgr_instance.allocate_database = AsyncMock(return_value=mock_location)
            mock_allocation_mgr.return_value = mock_mgr_instance

            with patch(
              "robosystems.operations.graph.entity_graph_service.get_graph_client_for_instance"
            ) as mock_get_client:
              mock_lbug_client = AsyncMock()

              # Create a mock exception that looks like a 404
              not_found_error = Exception("Not found")
              not_found_error.status_code = 404

              mock_lbug_client.get_database = AsyncMock(side_effect=not_found_error)
              mock_lbug_client.create_database = AsyncMock()
              mock_lbug_client.install_schema = AsyncMock(
                return_value={"status": "success"}
              )
              mock_lbug_client.close = AsyncMock()
              mock_get_client.return_value = mock_lbug_client

              with patch(
                "robosystems.models.iam.graph.Graph.create"
              ) as mock_graph_create:
                mock_graph = Mock()
                mock_graph.graph_id = "kg_test_789"
                mock_graph_create.return_value = mock_graph

                with patch("robosystems.models.iam.graph_schema.GraphSchema.create"):
                  with patch(
                    "robosystems.operations.graph.table_service.TableService.create_tables_from_schema",
                    return_value=[],
                  ):
                    with patch.object(
                      entity_service,
                      "_install_entity_schema",
                      new_callable=AsyncMock,
                      return_value="CREATE NODE TABLE Entity(...);",
                    ):
                      with patch.object(
                        entity_service,
                        "_create_entity_in_graph_lbug",
                        new_callable=AsyncMock,
                      ) as mock_create_entity:
                        mock_entity_response = Mock()
                        mock_entity_response.name = "Test Company"
                        mock_entity_response.model_dump = Mock(
                          return_value={
                            "name": "Test Company",
                            "uri": "https://test.com",
                          }
                        )
                        mock_create_entity.return_value = mock_entity_response

                        with patch(
                          "robosystems.operations.graph.credit_service.CreditService"
                        ):
                          result = await entity_service.create_entity_with_new_graph(
                            entity_data_dict=entity_data,
                            user_id="user_123",
                            tier="ladybug-standard",
                          )

                          mock_create_entity.assert_called_once()
                          assert result["graph_id"] == "kg_test_789"
                          assert result["entity"] is not None
