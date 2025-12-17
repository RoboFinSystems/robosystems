import pytest
import os
from fastapi.testclient import TestClient
from httpx import AsyncClient, ASGITransport
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from unittest.mock import Mock, patch
import pandas as pd
from robosystems.database import Model as Base
from main import app

# Speed up password hashing for tests by reducing bcrypt rounds
# Production uses 14 rounds, but that's too slow for tests
from robosystems.security import password as password_module

password_module.PasswordSecurity.BCRYPT_ROUNDS = 4  # Fast for tests

# Valid test graph IDs matching the GRAPH_ID_PATTERN
# Pattern: kg + 16+ hex chars (lowercase, 0-9 and a-f only)
# Actual formats: kg + 16 (generic) or kg + 18 (entity with 14 ULID + 4 hash)
VALID_TEST_GRAPH_ID = "kg01234567890abcdef"  # 18 hex chars (entity format)
VALID_TEST_GRAPH_ID_2 = "kg11111111111111111"  # 17 hex chars
VALID_TEST_GRAPH_ID_3 = "kg22222222222222222"  # 17 hex chars


@pytest.fixture(scope="session")
def test_db():
  """Create a test database."""
  database_url = os.environ.get("TEST_DATABASE_URL")

  engine = create_engine(database_url)

  # Create all tables
  # Drop tables with CASCADE to handle foreign key dependencies
  from sqlalchemy import text

  with engine.begin() as conn:
    conn.execute(text("DROP SCHEMA public CASCADE"))
    conn.execute(text("CREATE SCHEMA public"))
  Base.metadata.create_all(bind=engine)

  TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
  return TestingSessionLocal()


@pytest.fixture(scope="module")
def mock_get_current_user():
  """Mock the get_current_user dependency for testing."""
  with patch(
    "robosystems.middleware.auth.dependencies.get_current_user", autospec=True
  ) as mock:
    # Create a mock user
    mock_user = Mock()
    mock_user.id = "test-user-id"
    mock_user.name = "Test User"
    mock_user.email = "test@example.com"
    mock_user.accounts = []

    # Set the return value for the async function
    mock.return_value = mock_user
    yield mock


@pytest.fixture(scope="module")
def client(test_db):
  """Create a test client."""
  # Import the dependency directly
  from robosystems.middleware.rate_limits import (
    auth_rate_limit_dependency,
    rate_limit_dependency,
    user_management_rate_limit_dependency,
    sync_operations_rate_limit_dependency,
    connection_management_rate_limit_dependency,
    analytics_rate_limit_dependency,
    backup_operations_rate_limit_dependency,
    sensitive_auth_rate_limit_dependency,
    tasks_management_rate_limit_dependency,
    general_api_rate_limit_dependency,
    subscription_aware_rate_limit_dependency,
    auth_status_rate_limit_dependency,
    sso_rate_limit_dependency,
    graph_scoped_rate_limit_dependency,
    sse_connection_rate_limit_dependency,
  )

  # Disable rate limiting during tests (but keep authentication functional)
  app.dependency_overrides[auth_rate_limit_dependency] = lambda: None
  app.dependency_overrides[rate_limit_dependency] = lambda: None
  app.dependency_overrides[user_management_rate_limit_dependency] = lambda: None
  app.dependency_overrides[sync_operations_rate_limit_dependency] = lambda: None
  app.dependency_overrides[connection_management_rate_limit_dependency] = lambda: None
  app.dependency_overrides[analytics_rate_limit_dependency] = lambda: None
  app.dependency_overrides[backup_operations_rate_limit_dependency] = lambda: None
  app.dependency_overrides[sensitive_auth_rate_limit_dependency] = lambda: None
  app.dependency_overrides[tasks_management_rate_limit_dependency] = lambda: None
  app.dependency_overrides[general_api_rate_limit_dependency] = lambda: None
  app.dependency_overrides[subscription_aware_rate_limit_dependency] = lambda: None
  app.dependency_overrides[auth_status_rate_limit_dependency] = lambda: None
  app.dependency_overrides[sso_rate_limit_dependency] = lambda: None
  app.dependency_overrides[graph_scoped_rate_limit_dependency] = lambda: None
  app.dependency_overrides[sse_connection_rate_limit_dependency] = lambda: None

  # Override the get_db_session dependency to use test database
  from robosystems.database import get_db_session, get_async_db_session

  def override_get_db():
    yield test_db

  async def override_get_async_db():
    yield test_db

  app.dependency_overrides[get_db_session] = override_get_db
  app.dependency_overrides[get_async_db_session] = override_get_async_db

  # Override the database session to use test database across all modules
  with (
    patch("robosystems.database.session", test_db),
    patch("robosystems.middleware.auth.dependencies.session", test_db),
    patch("robosystems.middleware.auth.utils.session", test_db),
  ):
    client = TestClient(app)
    yield client

  # Reset the dependency overrides
  app.dependency_overrides = {}


@pytest.fixture(scope="function")  # Changed from module to function scope
def client_with_mocked_auth(test_db, test_user):
  """Create a test client with mocked authentication for unit tests."""
  # Import the dependency directly
  from robosystems.middleware.auth.dependencies import (
    get_current_user,
    get_current_user_with_graph,
  )
  from robosystems.middleware.rate_limits import (
    auth_rate_limit_dependency,
    rate_limit_dependency,
    user_management_rate_limit_dependency,
    sync_operations_rate_limit_dependency,
    connection_management_rate_limit_dependency,
    analytics_rate_limit_dependency,
    backup_operations_rate_limit_dependency,
    sensitive_auth_rate_limit_dependency,
    tasks_management_rate_limit_dependency,
    general_api_rate_limit_dependency,
    subscription_aware_rate_limit_dependency,
  )

  # Use the test_user from the fixture (which has an org)
  mock_user = Mock()
  mock_user.id = test_user.id
  mock_user.name = test_user.name
  mock_user.email = test_user.email
  mock_user.password_hash = test_user.password_hash
  mock_user.accounts = []

  # Override the dependencies
  app.dependency_overrides[get_current_user] = lambda: mock_user
  app.dependency_overrides[get_current_user_with_graph] = lambda: mock_user
  # Disable rate limiting during tests
  app.dependency_overrides[auth_rate_limit_dependency] = lambda: None
  app.dependency_overrides[rate_limit_dependency] = lambda: None
  app.dependency_overrides[user_management_rate_limit_dependency] = lambda: None
  app.dependency_overrides[sync_operations_rate_limit_dependency] = lambda: None
  app.dependency_overrides[connection_management_rate_limit_dependency] = lambda: None
  app.dependency_overrides[analytics_rate_limit_dependency] = lambda: None
  app.dependency_overrides[backup_operations_rate_limit_dependency] = lambda: None
  app.dependency_overrides[sensitive_auth_rate_limit_dependency] = lambda: None
  app.dependency_overrides[tasks_management_rate_limit_dependency] = lambda: None
  app.dependency_overrides[general_api_rate_limit_dependency] = lambda: None
  app.dependency_overrides[subscription_aware_rate_limit_dependency] = lambda: None

  # Override the database session dependency
  from robosystems.database import get_db_session

  def override_get_db():
    yield test_db

  app.dependency_overrides[get_db_session] = override_get_db

  client = TestClient(app)
  # Store mock_user in client for access in tests
  client.mock_user = mock_user
  yield client

  # Reset the dependency overrides
  app.dependency_overrides = {}


@pytest.fixture
async def async_client(test_db, test_user):
  """Create an async test client."""
  # Import the dependency directly
  from robosystems.middleware.auth.dependencies import (
    get_current_user,
    get_current_user_with_graph,
  )
  from robosystems.middleware.rate_limits import (
    auth_rate_limit_dependency,
    rate_limit_dependency,
    user_management_rate_limit_dependency,
    sync_operations_rate_limit_dependency,
    connection_management_rate_limit_dependency,
    analytics_rate_limit_dependency,
    backup_operations_rate_limit_dependency,
    sensitive_auth_rate_limit_dependency,
    tasks_management_rate_limit_dependency,
    general_api_rate_limit_dependency,
    subscription_aware_rate_limit_dependency,
    auth_status_rate_limit_dependency,
    sso_rate_limit_dependency,
    graph_scoped_rate_limit_dependency,
    sse_connection_rate_limit_dependency,
  )

  # Use the test_user from the fixture
  mock_user = Mock()
  mock_user.id = test_user.id
  mock_user.name = test_user.name
  mock_user.email = test_user.email
  mock_user.accounts = []

  # Override the dependencies
  app.dependency_overrides[get_current_user] = lambda: mock_user
  app.dependency_overrides[get_current_user_with_graph] = lambda: mock_user

  # Disable ALL rate limiting during tests
  app.dependency_overrides[auth_rate_limit_dependency] = lambda: None
  app.dependency_overrides[rate_limit_dependency] = lambda: None
  app.dependency_overrides[user_management_rate_limit_dependency] = lambda: None
  app.dependency_overrides[sync_operations_rate_limit_dependency] = lambda: None
  app.dependency_overrides[connection_management_rate_limit_dependency] = lambda: None
  app.dependency_overrides[analytics_rate_limit_dependency] = lambda: None
  app.dependency_overrides[backup_operations_rate_limit_dependency] = lambda: None
  app.dependency_overrides[sensitive_auth_rate_limit_dependency] = lambda: None
  app.dependency_overrides[tasks_management_rate_limit_dependency] = lambda: None
  app.dependency_overrides[general_api_rate_limit_dependency] = lambda: None
  app.dependency_overrides[subscription_aware_rate_limit_dependency] = lambda: None
  app.dependency_overrides[auth_status_rate_limit_dependency] = lambda: None
  app.dependency_overrides[sso_rate_limit_dependency] = lambda: None
  app.dependency_overrides[graph_scoped_rate_limit_dependency] = lambda: None
  app.dependency_overrides[sse_connection_rate_limit_dependency] = lambda: None

  # Override the database session dependency
  from robosystems.database import get_db_session

  def override_get_db():
    yield test_db

  app.dependency_overrides[get_db_session] = override_get_db

  transport = ASGITransport(app=app)
  async with AsyncClient(transport=transport, base_url="http://test") as ac:
    # Store mock_user in client for access in tests
    ac.mock_user = mock_user
    yield ac

  # Reset the dependency overrides
  app.dependency_overrides = {}


@pytest.fixture
async def auth_integration_client(test_db):
  """
  Create an async test client for authentication integration tests.

  Unlike async_client, this fixture does NOT override authentication dependencies,
  allowing real JWT/API key validation to occur. It DOES mock:
  - Rate limiting (disabled)
  - Database session (uses test_db)
  - GraphClient/GraphClientFactory (to avoid LadybugDB access)
  """
  from robosystems.middleware.rate_limits import (
    auth_rate_limit_dependency,
    rate_limit_dependency,
    user_management_rate_limit_dependency,
    sync_operations_rate_limit_dependency,
    connection_management_rate_limit_dependency,
    analytics_rate_limit_dependency,
    backup_operations_rate_limit_dependency,
    sensitive_auth_rate_limit_dependency,
    tasks_management_rate_limit_dependency,
    general_api_rate_limit_dependency,
    subscription_aware_rate_limit_dependency,
    auth_status_rate_limit_dependency,
    sso_rate_limit_dependency,
    graph_scoped_rate_limit_dependency,
    sse_connection_rate_limit_dependency,
  )
  from robosystems.database import get_db_session, get_async_db_session
  from unittest.mock import AsyncMock, patch

  # Disable ALL rate limiting during tests
  app.dependency_overrides[auth_rate_limit_dependency] = lambda: None
  app.dependency_overrides[rate_limit_dependency] = lambda: None
  app.dependency_overrides[user_management_rate_limit_dependency] = lambda: None
  app.dependency_overrides[sync_operations_rate_limit_dependency] = lambda: None
  app.dependency_overrides[connection_management_rate_limit_dependency] = lambda: None
  app.dependency_overrides[analytics_rate_limit_dependency] = lambda: None
  app.dependency_overrides[backup_operations_rate_limit_dependency] = lambda: None
  app.dependency_overrides[sensitive_auth_rate_limit_dependency] = lambda: None
  app.dependency_overrides[tasks_management_rate_limit_dependency] = lambda: None
  app.dependency_overrides[general_api_rate_limit_dependency] = lambda: None
  app.dependency_overrides[subscription_aware_rate_limit_dependency] = lambda: None
  app.dependency_overrides[auth_status_rate_limit_dependency] = lambda: None
  app.dependency_overrides[sso_rate_limit_dependency] = lambda: None
  app.dependency_overrides[graph_scoped_rate_limit_dependency] = lambda: None
  app.dependency_overrides[sse_connection_rate_limit_dependency] = lambda: None

  # Override database session dependencies to use test_db
  def override_get_db():
    yield test_db

  async def override_get_async_db():
    yield test_db

  app.dependency_overrides[get_db_session] = override_get_db
  app.dependency_overrides[get_async_db_session] = override_get_async_db

  # Mock GraphClientFactory to avoid LadybugDB database access
  with patch(
    "robosystems.graph_api.client.factory.GraphClientFactory.create_client"
  ) as mock_factory:
    mock_client = AsyncMock()
    mock_client.get_database_info = AsyncMock(
      return_value={
        "database_name": "test_graph",
        "database_size_bytes": 1024,
        "node_count": 0,
        "relationship_count": 0,
        "node_labels": [],
        "relationship_types": [],
      }
    )
    mock_client.close = AsyncMock()
    mock_factory.return_value = mock_client

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
      yield ac

  # Reset the dependency overrides
  app.dependency_overrides = {}


@pytest.fixture
def test_user(test_db):
  """Create a test user with associated org."""
  from robosystems.models.iam import User, Org, OrgUser, OrgRole, OrgType
  import uuid
  import bcrypt

  unique_id = str(uuid.uuid4())[:8]
  password = "T3stP@ssw0rd!"
  salt = bcrypt.gensalt()
  password_hash = bcrypt.hashpw(password.encode("utf-8"), salt).decode("utf-8")

  org = Org(
    id=f"test-org-{unique_id}",
    name=f"Test Org {unique_id}",
    org_type=OrgType.PERSONAL,
  )
  test_db.add(org)
  test_db.flush()

  user = User(
    id=f"test-user-{unique_id}",
    email=f"test+{unique_id}@example.com",
    name="Test User",
    password_hash=password_hash,
  )
  test_db.add(user)
  test_db.flush()

  org_user = OrgUser(
    org_id=org.id,
    user_id=user.id,
    role=OrgRole.OWNER,
  )
  test_db.add(org_user)
  test_db.commit()
  return user


@pytest.fixture
def test_org(test_db, test_user):
  """Get the org for the test user."""
  from robosystems.models.iam import OrgUser

  org_users = OrgUser.get_user_orgs(test_user.id, test_db)
  if not org_users:
    raise ValueError(f"Test user {test_user.id} has no organization")
  return org_users[0].org


@pytest.fixture
def sample_graph(test_db, test_org):
  """Create a sample graph for testing."""
  from robosystems.models.iam import Graph
  from robosystems.config.graph_tier import GraphTier
  import uuid

  unique_id = str(uuid.uuid4().hex)[:8]
  graph = Graph.create(
    graph_id=f"kg{unique_id}{unique_id}",
    graph_name="Test Graph Fixture",
    graph_type="entity",
    org_id=test_org.id,
    session=test_db,
    base_schema="base",
    schema_extensions=["roboledger"],
    graph_tier=GraphTier.LADYBUG_STANDARD,
    graph_instance_id="test-instance",
    graph_metadata={
      "purpose": "testing",
      "fixture": True,
    },
  )
  return graph


@pytest.fixture
def test_user_graph(test_db, test_user, sample_graph):
  """Create a test user-graph relationship."""
  from robosystems.models.iam import GraphUser

  # Create GraphUser relationship
  user_graph = GraphUser.create(
    user_id=test_user.id,
    graph_id=sample_graph.graph_id,
    role="admin",
    is_selected=True,
    session=test_db,
  )
  return user_graph


@pytest.fixture
def test_graph_with_credits(test_db, test_user, sample_graph):
  """Create a graph with credits setup for testing."""
  from robosystems.models.iam import GraphUser, GraphCredits
  from decimal import Decimal
  from datetime import datetime, timezone
  import uuid

  # Create GraphUser relationship
  user_graph = GraphUser.create(
    user_id=test_user.id,
    graph_id=sample_graph.graph_id,
    role="admin",
    is_selected=True,
    session=test_db,
  )

  # Create GraphCredits for the test graph
  graph_credits = GraphCredits(
    id=f"gc_{sample_graph.graph_id}_{str(uuid.uuid4())[:8]}",
    graph_id=sample_graph.graph_id,
    user_id=test_user.id,
    billing_admin_id=test_user.id,
    current_balance=Decimal("1000.0"),
    monthly_allocation=Decimal("1000.0"),
    last_allocation_date=datetime.now(timezone.utc),
  )
  test_db.add(graph_credits)
  test_db.commit()

  return {
    "graph": sample_graph,
    "user_graph": user_graph,
    "credits": graph_credits,
  }


@pytest.fixture
def db_session(test_db):
  """Alias for test_db to match test expectations."""
  return test_db


@pytest.fixture(autouse=True)
def setup_database(test_db):
  """Setup and teardown for each test."""
  # Start a transaction
  test_db.begin()
  yield test_db
  # Clean up all data after each test
  test_db.rollback()
  # Also clean any committed data by truncating tables
  from robosystems.models.iam import (
    User,
    UserAPIKey,
    GraphUser,
    GraphCredits,
    Graph,
    Org,
    OrgUser,
  )

  try:
    # Delete in reverse dependency order to avoid foreign key constraints
    test_db.query(UserAPIKey).delete()
    test_db.query(GraphCredits).delete()
    test_db.query(GraphUser).delete()
    test_db.query(Graph).delete()  # Delete graphs before orgs
    test_db.query(OrgUser).delete()  # Delete org memberships before users/orgs
    test_db.query(User).delete()
    test_db.query(Org).delete()
    test_db.commit()
  except Exception:
    test_db.rollback()


# SEC XBRL Testing Fixtures
@pytest.fixture
def mock_sec_client():
  """Mock SEC client for testing."""
  with patch("robosystems.adapters.sec.SECClient") as mock_client:
    client_instance = Mock()
    mock_client.return_value = client_instance
    # Ensure get_report_url returns a string
    client_instance.get_report_url.return_value = "http://example.com/report.xml"
    yield client_instance


@pytest.fixture
def mock_xbrl():
  """Mock XBRL class for testing."""
  with patch("robosystems.tasks.sec_filings.XBRLGraphProcessor") as mock_xbrl_class:
    xbrl_instance = Mock()
    mock_xbrl_class.return_value = xbrl_instance
    yield mock_xbrl_class


@pytest.fixture
def sample_sec_submissions():
  """Sample SEC submissions data for testing."""
  data = {
    "isXBRL": [True, True, False, True],
    "form": ["10-K", "10-Q", "8-K", "10-K"],
    "accessionNumber": ["0001", "0002", "0003", "0004"],
    "reportDate": ["2023-01-01", "2023-02-01", "2023-03-01", "2023-04-01"],
    "isInlineXBRL": [False, False, False, False],
    "primaryDocument": ["test1.xml", "test2.xml", "test3.xml", "test4.xml"],
  }
  return pd.DataFrame(data)


# LadybugDB Database Testing Fixtures
@pytest.fixture
def temp_lbug_db():
  """Create a temporary LadybugDB database for testing."""
  import tempfile

  with tempfile.TemporaryDirectory() as temp_dir:
    db_path = os.path.join(temp_dir, "test.db")
    yield db_path


@pytest.fixture
def lbug_repository(temp_lbug_db):
  """Create a LadybugDB Repository instance for testing."""
  from robosystems.middleware.graph import Repository

  repo = Repository(temp_lbug_db)
  yield repo
  repo.close()


@pytest.fixture
def lbug_repository_with_schema(temp_lbug_db):
  """Create a LadybugDB Repository with the entity schema already applied."""
  from robosystems.middleware.graph import Repository

  repo = Repository(temp_lbug_db)

  # Apply entity schema
  schema_statements = [
    """
    CREATE NODE TABLE Entity(
        identifier STRING,
        name STRING,
        uri STRING,
        description STRING,
        cik STRING,
        ticker STRING,
        scheme STRING,
        is_parent BOOLEAN,
        parent_entity_id STRING,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        PRIMARY KEY (identifier)
    )
    """,
    """
    CREATE NODE TABLE Connection(
        identifier STRING,
        provider STRING,
        uri STRING,
        status STRING,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        PRIMARY KEY (identifier)
    )
    """,
    """
    CREATE NODE TABLE User(
        identifier STRING,
        name STRING,
        email STRING,
        role STRING,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        PRIMARY KEY (identifier)
    )
    """,
    """
    CREATE NODE TABLE Report(
        identifier STRING,
        entity_cik STRING,
        form STRING,
        report_date STRING,
        filing_date STRING,
        status STRING,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        PRIMARY KEY (identifier)
    )
    """,
    """
    CREATE NODE TABLE Fact(
        identifier STRING,
        report_id STRING,
        element_qname STRING,
        value STRING,
        unit_measure STRING,
        period_type STRING,
        start_date STRING,
        end_date STRING,
        created_at TIMESTAMP,
        PRIMARY KEY (identifier)
    )
    """,
    """
    CREATE NODE TABLE Transaction(
        identifier STRING,
        uri STRING,
        date STRING,
        type STRING,
        name STRING,
        amount DOUBLE,
        status STRING,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        PRIMARY KEY (identifier)
    )
    """,
    """
    CREATE NODE TABLE Process(
        identifier STRING,
        name STRING,
        type STRING,
        status STRING,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        PRIMARY KEY (identifier)
    )
    """,
    """
    CREATE NODE TABLE Security(
        identifier STRING,
        ticker STRING,
        name STRING,
        figi STRING,
        uri STRING,
        status STRING,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        PRIMARY KEY (identifier)
    )
    """,
    """
    CREATE NODE TABLE Element(
        identifier STRING,
        qname STRING,
        uri STRING,
        name STRING,
        type STRING,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        PRIMARY KEY (identifier)
    )
    """,
    """
    CREATE NODE TABLE Unit(
        identifier STRING,
        measure STRING,
        uri STRING,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        PRIMARY KEY (identifier)
    )
    """,
    """
    CREATE NODE TABLE Period(
        identifier STRING,
        start_date STRING,
        end_date STRING,
        instant STRING,
        type STRING,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        PRIMARY KEY (identifier)
    )
    """,
    "CREATE REL TABLE HAS_REPORT(FROM Entity TO Report)",
    "CREATE REL TABLE REPORTED_IN(FROM Report TO Fact)",
    "CREATE REL TABLE HAS_USER(FROM Entity TO User)",
    "CREATE REL TABLE HAS_CONNECTION(FROM Entity TO Connection)",
    "CREATE REL TABLE HAS_TRANSACTION(FROM Entity TO Transaction)",
    "CREATE REL TABLE HAS_PROCESS(FROM Entity TO Process)",
    "CREATE REL TABLE HAS_SECURITY(FROM Entity TO Security)",
    "CREATE REL TABLE HAS_ELEMENT(FROM Fact TO Element)",
    "CREATE REL TABLE HAS_UNIT(FROM Fact TO Unit)",
    "CREATE REL TABLE HAS_PERIOD(FROM Fact TO Period)",
    "CREATE REL TABLE HAS_ENTITY(FROM Fact TO Entity)",
  ]

  for statement in schema_statements:
    repo.execute_query(statement.strip())

  yield repo
  repo.close()


@pytest.fixture
def mock_lbug_database_context():
  """Mock lbug database context manager for testing."""
  from unittest.mock import Mock, patch

  with patch(
    "robosystems.operations.multitenant_utils.lbug_database_context"
  ) as mock_context:
    mock_repo = Mock()
    mock_repo.execute_query.return_value = []
    mock_repo.execute_single.return_value = None
    mock_context.return_value.__enter__.return_value = mock_repo
    yield mock_context


@pytest.fixture
def sample_entity_data():
  """Sample entity data for testing."""
  return {
    "identifier": "test-entity-123",
    "name": "Test Entity Inc",
    "uri": "https://test-entity.com",
    "description": "Test entity for unit testing",
    "created_at": "2023-01-01 00:00:00",
    "updated_at": "2023-01-01 00:00:00",
  }


@pytest.fixture
def sample_report_data():
  """Sample report data for testing."""
  return {
    "identifier": "test-report-123",
    "entity_cik": "test-entity-123",
    "form": "10-K",
    "report_date": "2023-12-31",
    "filing_date": "2024-03-15",
    "status": "active",
    "created_at": "2024-03-15 00:00:00",
    "updated_at": "2024-03-15 00:00:00",
  }


@pytest.fixture
def sample_fact_data():
  """Sample fact data for testing."""
  return {
    "identifier": "test-fact-123",
    "report_id": "test-report-123",
    "element_qname": "us-gaap:Revenue",
    "value": "1000000",
    "unit_measure": "USD",
    "period_type": "duration",
    "start_date": "2023-01-01",
    "end_date": "2023-12-31",
    "created_at": "2024-03-15 00:00:00",
  }


def create_lbug_node(repo, node_type: str, **properties):
  """Helper function to create a node in LadybugDB."""
  props_str = ", ".join([f"{k}: '{v}'" for k, v in properties.items()])
  cypher = f"CREATE (n:{node_type} {{{props_str}}}) RETURN n"
  result = repo.execute_single(cypher)
  return result


def create_lbug_relationship(
  repo,
  from_node_id: str,
  to_node_id: str,
  rel_type: str,
  from_label: str = None,
  to_label: str = None,
):
  """Helper function to create a relationship in LadybugDB."""
  from_match = f"(a:{from_label})" if from_label else "(a)"
  to_match = f"(b:{to_label})" if to_label else "(b)"

  cypher = f"""
  MATCH {from_match} WHERE a.identifier = '{from_node_id}'
  MATCH {to_match} WHERE b.identifier = '{to_node_id}'
  CREATE (a)-[r:{rel_type}]->(b)
  RETURN r
  """
  result = repo.execute_single(cypher)
  return result


@pytest.fixture
def lbug_helpers():
  """Helper functions for LadybugDB testing."""
  return {
    "create_node": create_lbug_node,
    "create_relationship": create_lbug_relationship,
  }


@pytest.fixture
def other_user(test_db):
  """Create another test user without access to sample_graph."""
  from robosystems.models.iam import User
  import uuid
  import bcrypt

  unique_id = str(uuid.uuid4())[:8]
  password = "0th3rP@ssw0rd!"
  salt = bcrypt.gensalt()
  password_hash = bcrypt.hashpw(password.encode("utf-8"), salt).decode("utf-8")

  user = User(
    id=f"other-user-{unique_id}",
    email=f"other+{unique_id}@example.com",
    name="Other Test User",
    password_hash=password_hash,
  )
  test_db.add(user)
  test_db.commit()
  return user


@pytest.fixture
def test_user_token(test_user, sample_graph, test_db):
  """Create a JWT token for test_user with access to sample_graph."""
  from robosystems.middleware.auth.jwt import create_jwt_token
  from robosystems.models.iam import GraphUser

  # Ensure test_user has access to sample_graph
  existing_access = (
    test_db.query(GraphUser)
    .filter(
      GraphUser.user_id == test_user.id, GraphUser.graph_id == sample_graph.graph_id
    )
    .first()
  )

  if not existing_access:
    GraphUser.create(
      user_id=test_user.id,
      graph_id=sample_graph.graph_id,
      role="admin",
      session=test_db,
    )

  # Generate JWT token for test_user
  return create_jwt_token(test_user.id)


@pytest.fixture
def other_user_token(other_user):
  """Create a JWT token for other_user without access to sample_graph."""
  from robosystems.middleware.auth.jwt import create_jwt_token

  # Generate JWT token for other_user (no graph access)
  return create_jwt_token(other_user.id)
