"""Unit tests for entity graph API models."""

import pytest
from pydantic import ValidationError

from robosystems.models.api.entity_graph import (
  AvailableExtension,
  AvailableExtensionsResponse,
  EntityCreate,
  EntityListResponse,
  EntityResponse,
  EntityUpdate,
  EntityWithGraphResponse,
)


@pytest.mark.unit
class TestEntityCreate:
  def test_minimal_valid_entity(self):
    model = EntityCreate(name="Test Company")
    assert model.name == "Test Company"
    assert model.uri is None
    assert model.cik is None
    assert model.tier is None
    assert model.extensions is None

  def test_full_entity(self):
    model = EntityCreate(
      name="Apple Inc.",
      uri="https://apple.com",
      cik="0000320193",
      sic="3571",
      sic_description="Electronic Computers",
      category="Technology",
      state_of_incorporation="California",
      fiscal_year_end="0930",
      ein="94-2404110",
      tier="ladybug-standard",
      extensions=["roboledger"],
    )
    assert model.name == "Apple Inc."
    assert model.uri == "https://apple.com"
    assert model.cik == "0000320193"
    assert model.extensions == ["roboledger"]

  def test_name_required(self):
    with pytest.raises(ValidationError):
      EntityCreate()  # type: ignore[call-arg]

  def test_name_min_length(self):
    with pytest.raises(ValidationError) as exc_info:
      EntityCreate(name="")
    errors = exc_info.value.errors()
    assert any(e["loc"] == ("name",) for e in errors)

  def test_name_max_length(self):
    with pytest.raises(ValidationError) as exc_info:
      EntityCreate(name="x" * 256)
    errors = exc_info.value.errors()
    assert any(e["loc"] == ("name",) for e in errors)

  def test_name_exactly_max_length(self):
    model = EntityCreate(name="x" * 255)
    assert len(model.name) == 255

  def test_uri_cannot_be_empty_string(self):
    with pytest.raises(ValidationError) as exc_info:
      EntityCreate(name="Test", uri="")
    errors = exc_info.value.errors()
    assert any(e["loc"] == ("uri",) for e in errors)

  def test_uri_cannot_be_whitespace_only(self):
    with pytest.raises(ValidationError) as exc_info:
      EntityCreate(name="Test", uri="   ")
    errors = exc_info.value.errors()
    assert any(e["loc"] == ("uri",) for e in errors)

  def test_uri_valid_value(self):
    model = EntityCreate(name="Test", uri="https://example.com")
    assert model.uri == "https://example.com"

  def test_valid_extensions(self):
    model = EntityCreate(
      name="Test",
      extensions=["roboledger", "roboinvestor"],
    )
    assert model.extensions == ["roboledger", "roboinvestor"]

  def test_unknown_extension_rejected(self):
    with pytest.raises(ValidationError) as exc_info:
      EntityCreate(name="Test", extensions=["roboledger", "unknown_ext"])
    errors = exc_info.value.errors()
    assert any(e["loc"] == ("extensions",) for e in errors)
    assert any("Unknown extensions" in str(e.get("msg", "")) for e in errors)

  @pytest.mark.parametrize(
    "removed",
    ["roboscm", "robofo", "robohrm", "roboepm", "roboreport"],
  )
  def test_removed_extensions_rejected(self, removed: str):
    with pytest.raises(ValidationError):
      EntityCreate(name="Test", extensions=[removed])

  def test_empty_extensions_list(self):
    model = EntityCreate(name="Test", extensions=[])
    assert model.extensions == []

  def test_none_extensions(self):
    model = EntityCreate(name="Test", extensions=None)
    assert model.extensions is None

  def test_model_dump(self):
    model = EntityCreate(name="Test Co", uri="https://test.co")
    dumped = model.model_dump()
    assert dumped["name"] == "Test Co"
    assert dumped["uri"] == "https://test.co"
    assert dumped["cik"] is None

  def test_json_round_trip(self):
    model = EntityCreate(
      name="Test",
      uri="https://test.com",
      extensions=["roboledger"],
    )
    json_str = model.model_dump_json()
    restored = EntityCreate.model_validate_json(json_str)
    assert restored.name == model.name
    assert restored.extensions == model.extensions


@pytest.mark.unit
class TestEntityUpdate:
  def test_all_optional(self):
    model = EntityUpdate()
    assert model.name is None
    assert model.uri is None

  def test_partial_update(self):
    model = EntityUpdate(name="Updated Name")
    assert model.name == "Updated Name"
    assert model.uri is None

  def test_uri_cannot_be_empty_string(self):
    with pytest.raises(ValidationError) as exc_info:
      EntityUpdate(uri="")
    errors = exc_info.value.errors()
    assert any(e["loc"] == ("uri",) for e in errors)

  def test_uri_cannot_be_whitespace(self):
    with pytest.raises(ValidationError) as exc_info:
      EntityUpdate(uri="   ")
    errors = exc_info.value.errors()
    assert any(e["loc"] == ("uri",) for e in errors)

  def test_name_min_length(self):
    with pytest.raises(ValidationError) as exc_info:
      EntityUpdate(name="")
    errors = exc_info.value.errors()
    assert any(e["loc"] == ("name",) for e in errors)

  def test_name_max_length(self):
    with pytest.raises(ValidationError) as exc_info:
      EntityUpdate(name="x" * 256)
    errors = exc_info.value.errors()
    assert any(e["loc"] == ("name",) for e in errors)


@pytest.mark.unit
class TestEntityResponse:
  def test_minimal_response(self):
    model = EntityResponse(
      id="ent_123",
      name="Test Entity",
      uri="https://test.com",
      created_at="2024-01-01T00:00:00Z",
      updated_at="2024-01-01T00:00:00Z",
    )
    assert model.id == "ent_123"
    assert model.cik is None
    assert model.tier is None

  def test_full_response(self):
    model = EntityResponse(
      id="ent_123",
      name="Apple Inc.",
      uri="https://apple.com",
      cik="0000320193",
      database="test_db",
      sic="3571",
      sic_description="Electronic Computers",
      category="Technology",
      state_of_incorporation="CA",
      fiscal_year_end="0930",
      ein="94-2404110",
      created_at="2024-01-01T00:00:00Z",
      updated_at="2024-01-02T00:00:00Z",
      tier="ladybug-standard",
      extensions=["roboledger"],
    )
    assert model.name == "Apple Inc."
    assert model.extensions == ["roboledger"]


@pytest.mark.unit
class TestEntityListResponse:
  def test_empty_list(self):
    model = EntityListResponse(entities=[], total=0)
    assert model.entities == []
    assert model.total == 0

  def test_with_entities(self):
    entity = EntityResponse(
      id="ent_1",
      name="Test",
      uri="https://test.com",
      created_at="2024-01-01T00:00:00Z",
      updated_at="2024-01-01T00:00:00Z",
    )
    model = EntityListResponse(entities=[entity], total=1)
    assert len(model.entities) == 1
    assert model.total == 1


@pytest.mark.unit
class TestEntityWithGraphResponse:
  def test_valid_response(self):
    entity = EntityResponse(
      id="ent_1",
      name="Test",
      uri="https://test.com",
      created_at="2024-01-01T00:00:00Z",
      updated_at="2024-01-01T00:00:00Z",
    )
    model = EntityWithGraphResponse(
      entity=entity,
      graph_id="kg1234567890abcdef",
      graph_status="active",
      graph_tier="ladybug-standard",
      graph_extensions=["roboledger"],
    )
    assert model.graph_id == "kg1234567890abcdef"
    assert model.graph_extensions == ["roboledger"]


@pytest.mark.unit
class TestAvailableExtension:
  def test_default_enabled(self):
    model = AvailableExtension(name="roboledger", description="Accounting")
    assert model.enabled is False

  def test_enabled_extension(self):
    model = AvailableExtension(
      name="roboledger", description="Accounting", enabled=True
    )
    assert model.enabled is True


@pytest.mark.unit
class TestAvailableExtensionsResponse:
  def test_extensions_list(self):
    ext = AvailableExtension(name="roboledger", description="Accounting")
    model = AvailableExtensionsResponse(extensions=[ext])
    assert len(model.extensions) == 1
    assert model.extensions[0].name == "roboledger"
