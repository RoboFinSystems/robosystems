"""The Stripe webhook is absent on a deployment without Stripe billing, and a
seeded placeholder secret never boots a deployed environment."""

import logging
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from robosystems.config import env
from robosystems.config.validation import ConfigValidationError, EnvValidator
from robosystems.database import get_db_session
from robosystems.middleware.rate_limits import webhook_rate_limit_dependency
from robosystems.routers.admin.webhooks import router
from tests.config.test_validation import MockEnvConfig


@pytest.mark.unit
def test_webhook_is_absent_when_billing_is_off():
  app = FastAPI()
  app.include_router(router)
  app.dependency_overrides[get_db_session] = lambda: MagicMock()
  app.dependency_overrides[webhook_rate_limit_dependency] = lambda: None
  with patch.object(env, "BILLING_ENABLED", False):
    response = TestClient(app).post(
      "/admin/v1/webhooks/stripe",
      content=b"{}",
      headers={
        "stripe-signature": "t=1,v1=deadbeef",
        "content-type": "application/json",
      },
    )
  assert response.status_code == 404


def _deployed_config(**overrides):
  cfg = MockEnvConfig()
  cfg.ENVIRONMENT = "prod"
  cfg.GRAPH_API_KEY = "k"
  for name, value in overrides.items():
    setattr(cfg, name, value)
  return cfg


PLACEHOLDER_ERROR = "STRIPE_WEBHOOK_SECRET: placeholder"


def _validation_log(config, caplog) -> str:
  # The exception carries only a count; each error is logged on its own line.
  with caplog.at_level(logging.ERROR, logger="robosystems.config.validation"):
    try:
      EnvValidator.validate_required_vars(config)
    except ConfigValidationError:
      pass
  return "\n".join(record.getMessage() for record in caplog.records)


@pytest.mark.unit
def test_placeholder_webhook_secret_is_refused_even_with_billing_off(caplog):
  config = _deployed_config(
    STRIPE_WEBHOOK_SECRET="whsec_your_stripe_webhook_secret_here"
  )
  assert PLACEHOLDER_ERROR in _validation_log(config, caplog)


@pytest.mark.unit
def test_a_real_looking_secret_is_not_flagged(caplog):
  config = _deployed_config(STRIPE_WEBHOOK_SECRET="whsec_" + "k" * 40)
  assert PLACEHOLDER_ERROR not in _validation_log(config, caplog)
