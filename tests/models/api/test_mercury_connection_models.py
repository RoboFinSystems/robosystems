"""Connection request models: the Mercury config and the reserved source name."""

from __future__ import annotations

from datetime import date

import pytest
from pydantic import ValidationError

from robosystems.models.api.graphs.connections import (
  RESERVED_SOURCE_NAMES,
  CreateConnectionRequest,
  ExternalConnectionConfig,
  MercuryConnectionConfig,
)


@pytest.mark.unit
class TestMercuryConnectionRequest:
  def test_config_is_optional_for_mercury(self):
    request = CreateConnectionRequest(provider="mercury")
    assert request.mercury_config is None
    assert request.entity_id is None

  def test_config_is_carried(self):
    request = CreateConnectionRequest(
      provider="mercury",
      mercury_config=MercuryConnectionConfig(
        since_date=date(2026, 1, 1), include_treasury=False, api_key="secret-token-123"
      ),
    )
    assert request.mercury_config is not None
    assert request.mercury_config.since_date == date(2026, 1, 1)
    assert request.mercury_config.include_treasury is False
    assert request.mercury_config.api_key == "secret-token-123"

  def test_mercury_config_on_another_provider_is_refused(self):
    with pytest.raises(ValidationError, match="should not be provided"):
      CreateConnectionRequest(
        provider="external",
        external_config=ExternalConnectionConfig(source_name="salesforce"),
        mercury_config=MercuryConnectionConfig(),
      )

  def test_quickbooks_config_on_mercury_is_refused(self):
    from robosystems.models.api.graphs.connections import QuickBooksConnectionConfig

    with pytest.raises(ValidationError, match="should not be provided"):
      CreateConnectionRequest(
        provider="mercury", quickbooks_config=QuickBooksConnectionConfig()
      )

  def test_short_api_key_is_refused(self):
    with pytest.raises(ValidationError):
      MercuryConnectionConfig(api_key="short")

  def test_mercury_is_a_reserved_source_name(self):
    assert "mercury" in RESERVED_SOURCE_NAMES
    with pytest.raises(ValidationError, match="reserved"):
      ExternalConnectionConfig(source_name="mercury")
