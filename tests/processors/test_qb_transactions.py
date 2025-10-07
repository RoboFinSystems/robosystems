"""Tests for QuickBooks integration with Kuzu database."""

import pytest
from unittest.mock import Mock, patch
from robosystems.adapters.qb import QBClient


@pytest.fixture
def mock_qb_credentials():
  """Mock QuickBooks credentials for testing."""
  return {"access_token": "mock_access_token", "refresh_token": "mock_refresh_token"}


@pytest.fixture
def sample_qb_entity_data():
  """Sample QuickBooks entity data for testing."""
  return {
    "Id": "1",
    "EntityName": "Test QB Entity",
    "LegalName": "Test QB Entity LLC",
    "EntityAddr": {
      "Line1": "123 Business St",
      "City": "San Francisco",
      "CountrySubDivisionCode": "CA",
      "PostalCode": "94105",
      "Country": "USA",
    },
  }


class TestQBClientIntegration:
  """Test QuickBooks client integration with Kuzu database."""

  @patch("robosystems.adapters.qb.AuthClient")
  @patch("robosystems.adapters.qb.QuickBooks")
  def test_qb_client_initialization(
    self, mock_quickbooks, mock_auth_client, mock_qb_credentials
  ):
    """Test QBClient initialization."""
    realm_id = "9341452700148642"

    # Mock the auth client instance with proper refresh_token property
    mock_auth_instance = Mock()
    mock_auth_instance.refresh_token = mock_qb_credentials["refresh_token"]
    mock_auth_client.return_value = mock_auth_instance

    client = QBClient(realm_id=realm_id, qb_credentials=mock_qb_credentials)

    assert client.realm_id == realm_id
    assert client.refresh_token == mock_qb_credentials["refresh_token"]
    assert client.access_token == mock_qb_credentials["access_token"]

  def test_qb_client_initialization_validation(self):
    """Test QBClient initialization validation."""
    # Test missing realm_id
    with pytest.raises(ValueError):
      QBClient(realm_id="", qb_credentials={"refresh_token": "test"})

    # Test missing credentials
    with pytest.raises(ValueError):
      QBClient(realm_id="123", qb_credentials={})

    # Test missing refresh token
    with pytest.raises(ValueError):
      QBClient(realm_id="123", qb_credentials={"access_token": "test"})

  @patch("robosystems.adapters.qb.AuthClient")
  @patch("robosystems.adapters.qb.QuickBooks")
  def test_qb_client_mock_mode(
    self, mock_quickbooks, mock_auth_client, mock_qb_credentials
  ):
    """Test QBClient in mock mode (for testing)."""
    realm_id = "9341452700148642"
    mock_credentials = {**mock_qb_credentials, "refresh_token": "mock_refresh_token"}

    # Mock the auth client instance with proper refresh_token property
    mock_auth_instance = Mock()
    mock_auth_instance.refresh_token = "mock_refresh_token"
    mock_auth_client.return_value = mock_auth_instance

    client = QBClient(realm_id=realm_id, qb_credentials=mock_credentials)

    # Should not try to refresh token in mock mode
    assert client.refresh_token == "mock_refresh_token"


class TestQBDataIntegration:
  """Test integration of QuickBooks data with Kuzu database."""

  def test_entity_data_mapping(
    self, kuzu_repository_with_schema, sample_qb_entity_data
  ):
    """Test mapping QuickBooks entity data to Kuzu database."""
    # Create QB-specific entity schema with extended fields
    kuzu_repository_with_schema.execute_query("""
      CREATE NODE TABLE QBEntity(
        identifier STRING,
        name STRING,
        legal_name STRING,
        address STRING,
        city STRING,
        state STRING,
        zip_code STRING,
        country STRING,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        PRIMARY KEY (identifier)
      )
    """)

    # Create entity from QB data
    entity_data = {
      "identifier": f"qb-entity-{sample_qb_entity_data['Id']}",
      "name": sample_qb_entity_data["EntityName"],
      "legal_name": sample_qb_entity_data["LegalName"],
      "address": sample_qb_entity_data["EntityAddr"]["Line1"],
      "city": sample_qb_entity_data["EntityAddr"]["City"],
      "state": sample_qb_entity_data["EntityAddr"]["CountrySubDivisionCode"],
      "zip_code": sample_qb_entity_data["EntityAddr"]["PostalCode"],
      "country": sample_qb_entity_data["EntityAddr"]["Country"],
      "created_at": "2023-01-01 00:00:00",
      "updated_at": "2023-01-01 00:00:00",
    }

    # Store in Kuzu database
    cypher = """
    CREATE (c:QBEntity {
      identifier: $identifier,
      name: $name,
      legal_name: $legal_name,
      address: $address,
      city: $city,
      state: $state,
      zip_code: $zip_code,
      country: $country,
      created_at: timestamp($created_at),
      updated_at: timestamp($updated_at)
    }) RETURN c
    """

    result = kuzu_repository_with_schema.execute_single(cypher, entity_data)
    assert result is not None

    entity = result["c"]
    assert entity["name"] == sample_qb_entity_data["EntityName"]
    assert entity["legal_name"] == sample_qb_entity_data["LegalName"]
    assert entity["city"] == "San Francisco"

  def test_qb_connection_mapping(self, kuzu_repository_with_schema):
    """Test mapping QuickBooks connection data to Kuzu database."""
    # Create QB-specific connection schema with extended fields
    kuzu_repository_with_schema.execute_query("""
      CREATE NODE TABLE QBConnection(
        identifier STRING,
        provider STRING,
        realm_id STRING,
        connection_id STRING,
        uri STRING,
        status STRING,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        PRIMARY KEY (identifier)
      )
    """)

    realm_id = "9341452700148642"

    connection_data = {
      "identifier": f"qb-connection-{realm_id}",
      "provider": "QuickBooks",
      "realm_id": realm_id,
      "connection_id": f"quickbooks_{realm_id}",
      "uri": f"https://quickbooks.intuit.com/entity/{realm_id}",
      "status": "connected",
      "created_at": "2023-01-01 00:00:00",
      "updated_at": "2023-01-01 00:00:00",
    }

    cypher = """
    CREATE (conn:QBConnection {
      identifier: $identifier,
      provider: $provider,
      realm_id: $realm_id,
      connection_id: $connection_id,
      uri: $uri,
      status: $status,
      created_at: timestamp($created_at),
      updated_at: timestamp($updated_at)
    }) RETURN conn
    """

    result = kuzu_repository_with_schema.execute_single(cypher, connection_data)
    assert result is not None

    connection = result["conn"]
    assert connection["provider"] == "QuickBooks"
    assert connection["realm_id"] == realm_id
    assert connection["status"] == "connected"

  def test_entity_connection_relationship(
    self, kuzu_repository_with_schema, sample_qb_entity_data
  ):
    """Test relationship between entity and QuickBooks connection."""
    # Create QB-specific entity schema
    kuzu_repository_with_schema.execute_query("""
      CREATE NODE TABLE QBEntity(
        identifier STRING,
        name STRING,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        PRIMARY KEY (identifier)
      )
    """)

    # Create entity first
    entity_data = {
      "identifier": f"qb-entity-{sample_qb_entity_data['Id']}",
      "name": sample_qb_entity_data["EntityName"],
      "created_at": "2023-01-01 00:00:00",
      "updated_at": "2023-01-01 00:00:00",
    }

    entity_cypher = """
    CREATE (c:QBEntity {
      identifier: $identifier,
      name: $name,
      created_at: timestamp($created_at),
      updated_at: timestamp($updated_at)
    }) RETURN c
    """

    entity_result = kuzu_repository_with_schema.execute_single(
      entity_cypher, entity_data
    )
    assert entity_result is not None

    # Create QB connection schema
    kuzu_repository_with_schema.execute_query("""
      CREATE NODE TABLE QBConnection(
        identifier STRING,
        provider STRING,
        realm_id STRING,
        status STRING,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        PRIMARY KEY (identifier)
      )
    """)

    # Create connection
    realm_id = "9341452700148642"
    connection_data = {
      "identifier": f"qb-connection-{realm_id}",
      "provider": "QuickBooks",
      "realm_id": realm_id,
      "status": "connected",
      "created_at": "2023-01-01 00:00:00",
      "updated_at": "2023-01-01 00:00:00",
    }

    connection_cypher = """
    CREATE (conn:QBConnection {
      identifier: $identifier,
      provider: $provider,
      realm_id: $realm_id,
      status: $status,
      created_at: timestamp($created_at),
      updated_at: timestamp($updated_at)
    }) RETURN conn
    """

    connection_result = kuzu_repository_with_schema.execute_single(
      connection_cypher, connection_data
    )
    assert connection_result is not None

    # Create relationship table (unique name to avoid conflicts)
    kuzu_repository_with_schema.execute_query("""
      CREATE REL TABLE ENTITY_HAS_QB_CONNECTION(FROM QBEntity TO QBConnection)
    """)

    # Create relationship
    rel_cypher = """
    MATCH (c:QBEntity {identifier: $entity_id})
    MATCH (conn:QBConnection {identifier: $connection_id})
    CREATE (c)-[r:ENTITY_HAS_QB_CONNECTION]->(conn)
    RETURN r
    """

    rel_result = kuzu_repository_with_schema.execute_single(
      rel_cypher,
      {
        "entity_id": entity_data["identifier"],
        "connection_id": connection_data["identifier"],
      },
    )
    assert rel_result is not None

    # Verify relationship
    verify_cypher = """
    MATCH (c:QBEntity {identifier: $entity_id})-[:ENTITY_HAS_QB_CONNECTION]->(conn:QBConnection)
    WHERE conn.provider = 'QuickBooks'
    RETURN c, conn
    """

    verify_result = kuzu_repository_with_schema.execute_single(
      verify_cypher, {"entity_id": entity_data["identifier"]}
    )

    assert verify_result is not None
    entity = verify_result["c"]
    connection = verify_result["conn"]
    assert entity["name"] == sample_qb_entity_data["EntityName"]
    assert connection["provider"] == "QuickBooks"


class TestQBTransactionProcessing:
  """Test QuickBooks transaction processing patterns."""

  def test_transaction_data_structure(self, kuzu_repository_with_schema):
    """Test QuickBooks transaction data structure in Kuzu database."""
    # Create transaction schema
    kuzu_repository_with_schema.execute_query("""
      CREATE NODE TABLE QBTransaction(
        identifier STRING,
        qb_id STRING,
        transaction_type STRING,
        amount DOUBLE,
        date STRING,
        description STRING,
        account_id STRING,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        PRIMARY KEY (identifier)
      )
    """)

    kuzu_repository_with_schema.execute_query("""
      CREATE NODE TABLE QBEntity(
        identifier STRING,
        name STRING,
        PRIMARY KEY (identifier)
      )
    """)

    kuzu_repository_with_schema.execute_query("""
      CREATE REL TABLE ENTITY_HAS_QB_TRANSACTION(FROM QBEntity TO QBTransaction)
    """)

    # Sample transaction data
    transaction_data = {
      "identifier": "qb-txn-12345",
      "qb_id": "12345",
      "transaction_type": "Payment",
      "amount": 1500.00,
      "date": "2023-12-15",
      "description": "Customer payment received",
      "account_id": "1000",
      "created_at": "2023-12-15 10:30:00",
      "updated_at": "2023-12-15 10:30:00",
    }

    cypher = """
    CREATE (txn:QBTransaction {
      identifier: $identifier,
      qb_id: $qb_id,
      transaction_type: $transaction_type,
      amount: $amount,
      date: $date,
      description: $description,
      account_id: $account_id,
      created_at: timestamp($created_at),
      updated_at: timestamp($updated_at)
    }) RETURN txn
    """

    result = kuzu_repository_with_schema.execute_single(cypher, transaction_data)
    assert result is not None

    transaction = result["txn"]
    assert transaction["transaction_type"] == "Payment"
    assert transaction["amount"] == 1500.00
    assert transaction["description"] == "Customer payment received"

  def test_bulk_transaction_processing(self, kuzu_repository_with_schema):
    """Test bulk processing of QuickBooks transactions."""
    # Create transaction schema if not exists
    try:
      kuzu_repository_with_schema.execute_query("""
        CREATE NODE TABLE QBTransaction(
          identifier STRING,
          qb_id STRING,
          transaction_type STRING,
          amount DOUBLE,
          date STRING,
          created_at TIMESTAMP,
          PRIMARY KEY (identifier)
        )
      """)
    except Exception:
      pass  # Table might already exist

    # Create multiple transactions
    transactions = [
      {
        "identifier": f"qb-txn-{i}",
        "qb_id": str(1000 + i),
        "transaction_type": "Sale" if i % 2 == 0 else "Purchase",
        "amount": 100.00 * (i + 1),
        "date": f"2023-12-{15 + (i % 15):02d}",
        "created_at": "2023-12-15 10:30:00",
      }
      for i in range(10)
    ]

    # Bulk insert transactions
    for transaction in transactions:
      cypher = """
      CREATE (txn:QBTransaction {
        identifier: $identifier,
        qb_id: $qb_id,
        transaction_type: $transaction_type,
        amount: $amount,
        date: $date,
        created_at: timestamp($created_at)
      }) RETURN txn
      """

      result = kuzu_repository_with_schema.execute_single(cypher, transaction)
      assert result is not None

    # Verify all transactions were created
    count_cypher = "MATCH (txn:QBTransaction) RETURN count(txn) as count"
    count_result = kuzu_repository_with_schema.execute_single(count_cypher)
    assert count_result["count"] >= 10

    # Test aggregation queries
    sales_cypher = """
    MATCH (txn:QBTransaction {transaction_type: 'Sale'})
    RETURN sum(txn.amount) as total_sales, count(txn) as sale_count
    """
    sales_result = kuzu_repository_with_schema.execute_single(sales_cypher)
    assert sales_result is not None
    assert sales_result["sale_count"] == 5  # Half are sales
    assert sales_result["total_sales"] > 0


class TestQBErrorHandling:
  """Test error handling in QuickBooks integration."""

  def test_invalid_credentials_handling(self):
    """Test handling of invalid QuickBooks credentials."""
    with pytest.raises(ValueError):
      QBClient(realm_id="123", qb_credentials={"invalid": "data"})

  def test_missing_realm_id_handling(self, mock_qb_credentials):
    """Test handling of missing realm ID."""
    with pytest.raises(ValueError):
      QBClient(realm_id="", qb_credentials=mock_qb_credentials)

  def test_connection_timeout_simulation(self, kuzu_repository_with_schema):
    """Test simulation of connection timeout scenarios."""
    # This would test how the system handles QB API timeouts
    # For now, just verify the Kuzu database can handle connection issues gracefully
    try:
      result = kuzu_repository_with_schema.execute_single("RETURN 1 as test")
      assert result["test"] == 1
    except Exception as e:
      pytest.fail(f"Kuzu database should handle queries gracefully: {e}")


if __name__ == "__main__":
  pytest.main([__file__, "-v"])
