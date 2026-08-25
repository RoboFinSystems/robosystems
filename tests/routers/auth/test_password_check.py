"""Public password-strength check mounts the same limiter as login."""

from inspect import signature

import pytest

from robosystems.middleware.rate_limits import auth_rate_limit_dependency
from robosystems.routers.auth.password import (
  check_password_strength,
  get_password_policy,
)


@pytest.mark.unit
def test_password_check_mounts_the_auth_limiter():
  dep = signature(check_password_strength).parameters["_rate_limit"].default
  assert dep.dependency is auth_rate_limit_dependency


@pytest.mark.unit
def test_password_policy_stays_unlimited():
  assert "_rate_limit" not in signature(get_password_policy).parameters
