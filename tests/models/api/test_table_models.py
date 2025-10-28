import pytest
from pydantic import ValidationError

from robosystems.models.api.table import FileStatusUpdate


class TestFileStatusUpdate:
    def test_valid_uploaded_status(self):
        model = FileStatusUpdate(status="uploaded")
        assert model.status == "uploaded"

    def test_valid_disabled_status(self):
        model = FileStatusUpdate(status="disabled")
        assert model.status == "disabled"

    def test_valid_archived_status(self):
        model = FileStatusUpdate(status="archived")
        assert model.status == "archived"

    def test_status_field_required(self):
        with pytest.raises(ValidationError) as exc_info:
            FileStatusUpdate()  # type: ignore[call-arg]

        errors = exc_info.value.errors()
        assert any(error["loc"] == ("status",) for error in errors)
        assert any(error["type"] == "missing" for error in errors)

    def test_extra_fields_forbidden(self):
        with pytest.raises(ValidationError) as exc_info:
            FileStatusUpdate(status="uploaded", extra_field="not_allowed")  # type: ignore[call-arg]

        errors = exc_info.value.errors()
        assert any("extra" in error["type"] for error in errors)

    def test_model_dump(self):
        model = FileStatusUpdate(status="uploaded")
        dumped = model.model_dump()

        assert dumped == {"status": "uploaded"}

    def test_model_json_schema(self):
        schema = FileStatusUpdate.model_json_schema()

        assert "properties" in schema
        assert "status" in schema["properties"]
        assert schema["properties"]["status"]["type"] == "string"
        assert (
            "File status: 'uploaded' (ready for ingest), 'disabled' (exclude from ingest), 'archived' (soft deleted)"
            in schema["properties"]["status"]["description"]
        )

    def test_from_json(self):
        json_data = '{"status": "disabled"}'
        model = FileStatusUpdate.model_validate_json(json_data)

        assert model.status == "disabled"

    def test_to_json(self):
        model = FileStatusUpdate(status="archived")
        json_str = model.model_dump_json()

        assert '"status":"archived"' in json_str
