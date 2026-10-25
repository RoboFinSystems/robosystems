"""A cross-app handoff completes only in the browser that started it."""

import hashlib
from unittest.mock import patch

import pytest
from fastapi import HTTPException

from robosystems.models.api.auth import SSOCompleteRequest
from robosystems.routers.auth.sso import _assert_browser_binding

NONCE = "a" * 32
BOUND = {
  "target_app": "roboledger",
  "browser_nonce_hash": hashlib.sha256(NONCE.encode()).hexdigest(),
}
UNBOUND = {"target_app": "roboledger", "browser_nonce_hash": None}


def _complete(session_data, nonce):
  body = SSOCompleteRequest(session_id="sess", browser_nonce=nonce)
  with patch("robosystems.routers.auth.sso.SecurityAuditLogger.log_security_event"):
    _assert_browser_binding(body, session_data)


@pytest.mark.unit
def test_the_browser_that_minted_the_nonce_completes():
  _complete(BOUND, NONCE)


@pytest.mark.unit
@pytest.mark.parametrize("nonce", [None, "b" * 32])
def test_any_other_browser_is_refused(nonce):
  with pytest.raises(HTTPException) as exc:
    _complete(BOUND, nonce)
  assert exc.value.status_code == 401


@pytest.mark.unit
def test_a_client_presenting_a_nonce_refuses_an_unbound_handoff():
  with pytest.raises(HTTPException):
    _complete(UNBOUND, NONCE)


@pytest.mark.unit
def test_clients_predating_the_binding_still_complete_unbound_handoffs():
  _complete(UNBOUND, None)
