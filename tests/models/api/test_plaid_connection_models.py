"""Connection request models for Plaid, and the Link-shaped OAuth init response."""

from __future__ import annotations

from datetime import UTC, date, datetime

import pytest
from pydantic import ValidationError

from robosystems.models.api.graphs.connections import (
  CreateConnectionRequest,
  PlaidConnectionConfig,
)
from robosystems.models.api.oauth import OAuthInitResponse


@pytest.mark.unit
class TestPlaidConnectionRequest:
  def test_config_is_optional_for_plaid(self):
    request = CreateConnectionRequest(provider="plaid")
    assert request.plaid_config is None

  def test_config_is_carried(self):
    request = CreateConnectionRequest(
      provider="plaid", plaid_config=PlaidConnectionConfig(since_date=date(2026, 1, 1))
    )
    assert request.plaid_config is not None
    assert request.plaid_config.since_date == date(2026, 1, 1)

  def test_plaid_config_on_another_provider_is_refused(self):
    with pytest.raises(ValidationError, match="should not be provided"):
      CreateConnectionRequest(provider="mercury", plaid_config=PlaidConnectionConfig())


@pytest.mark.unit
class TestOAuthInitResponse:
  def test_a_link_flow_carries_a_token_and_no_url(self):
    response = OAuthInitResponse(
      link_token="link-sandbox-1", state="state_1", expires_at=datetime.now(UTC)
    )
    assert response.auth_url is None
    assert response.link_token == "link-sandbox-1"

  def test_a_redirect_flow_carries_a_url_and_no_token(self):
    response = OAuthInitResponse(
      auth_url="https://example.com/auth", state="state_1", expires_at=datetime.now(UTC)
    )
    assert response.link_token is None
