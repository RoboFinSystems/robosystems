import pytest
import pandas as pd


@pytest.fixture
def mock_entity_df(mocker):
  """Create a realistic pandas DataFrame with entity data."""
  data = {
    "cik_str": ["0000123456", "0000654321", "0000789012"],
    "ticker": ["TEST", "EXMP", "SMPL"],
    "title": ["Test Corp", "Example Inc", "Sample Entity"],
  }
  df = pd.DataFrame(data)
  return df


@pytest.fixture
def mock_entity_setup(mocker):
  """Common setup for entity-related tests."""
  # Mock entity and save methods
  mock_entity = mocker.MagicMock()
  mock_entity.element_id = "test_entity_id"
  mock_entity.securities.connect = mocker.MagicMock()

  # Mock entity constructor and get_by methods
  mock_get_entity = mocker.patch(
    "robosystems.models.base.entity.Entity.get_entity_by_cik", return_value=None
  )
  mock_entity_init = mocker.patch(
    "robosystems.models.base.entity.Entity.__new__", return_value=mock_entity
  )

  # Mock UUID generation for consistent testing
  mocker.patch(
    "robosystems.utils.uuid.generate_deterministic_uuid7", return_value="test_uuid_v7"
  )

  return {
    "entity": mock_entity,
    "get_entity": mock_get_entity,
    "entity_init": mock_entity_init,
  }


@pytest.fixture
def mock_security_setup(mocker):
  """Common setup for security-related tests."""
  # Mock security and methods
  mock_security = mocker.MagicMock()
  mock_security.element_id = "test_security_id"

  # Mock security constructor and get_by methods
  mock_get_security = mocker.patch(
    "robosystems.models.security.Security.get_security_by_uri", return_value=None
  )
  mock_security_init = mocker.patch(
    "robosystems.models.security.Security.__new__", return_value=mock_security
  )

  return {
    "security": mock_security,
    "get_security": mock_get_security,
    "security_init": mock_security_init,
  }


@pytest.fixture
def mock_openfigi_success_response():
  """Standard successful OpenFIGI API response."""
  return [
    {
      "data": [
        {
          "figi": "BBG000BLNNH6",
          "compositeFIGI": "BBG000BLNNH6",
          "securityType": "Common Stock",
          "marketSector": "Equity",
          "shareClassFIGI": "BBG001S5N8V8",
          "securityType2": "Equity",
          "securityDescription": "Test Corp Common Stock",
          "ticker": "TEST",
          "exchCode": "US",
        }
      ]
    }
  ]


@pytest.fixture
def mock_openfigi_not_found_response():
  """OpenFIGI API response when no match is found."""
  return [{"warning": "No identifier found."}]


@pytest.fixture
def mock_openfigi_error_response():
  """OpenFIGI API error response."""
  return "429"  # Rate limit error


@pytest.fixture
def mock_sec_report():
  """Common SEC report data structure."""
  return {
    "accessionNumber": "0001",
    "form": "10-K",
    "isInlineXBRL": False,
    "primaryDocument": "test.xml",
    "filingDate": "2024-01-01",
    "reportDate": "2023-12-31",
  }


@pytest.fixture
def mock_sec_filer():
  """Common SEC filer data structure."""
  return {"name": "Test Corp", "filings": {"recent": [], "files": []}}


@pytest.fixture
def mock_xbrl(monkeypatch):
  """Mock XBRLGraphProcessor class and its methods."""
  from unittest.mock import MagicMock

  mock_xbrl = MagicMock()
  mock_xbrl.process = MagicMock()
  mock_xbrl.make_entity = MagicMock()
  mock_xbrl.make_report = MagicMock()
  mock_xbrl.make_dts = MagicMock()
  mock_xbrl.make_facts = MagicMock()
  return mock_xbrl


@pytest.fixture
def mock_arelle_controller(mocker):
  """Mock Arelle controller with basic functionality."""
  mock_controller = mocker.MagicMock()
  mock_controller.facts = []
  mock_controller.namespaceDocs = {}
  mock_controller.baseSets = {}
  mock_controller.roleTypes = {}
  return mock_controller


@pytest.fixture
def mock_arelle_client(mocker):
  """Mock Arelle client that returns the mock controller."""
  mock_client = mocker.MagicMock()
  mock_client.controller.return_value = mock_arelle_controller()
  return mock_client


@pytest.fixture
def sample_sec_submissions():
  """Sample SEC submissions data."""
  return pd.DataFrame(
    {
      "accessionNumber": ["0001", "0002", "0003"],
      "form": ["10-K", "10-Q", "8-K"],
      "filingDate": ["2024-01-01", "2024-02-01", "2024-03-01"],
      "reportDate": ["2023-12-31", "2023-09-30", "2024-03-01"],
      "isInlineXBRL": [True, True, False],
      "primaryDocument": ["test1.xml", "test2.xml", "test3.xml"],
      "isXBRL": [True, True, False],
    }
  )
