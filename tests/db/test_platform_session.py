"""`platform_session()` must be independent of the request-scoped session.

`platform_session()` previously drove
`get_db_session()`, which returns the request-scoped Session (keyed by
the request contextvar → task → thread). Opened *inside* a request — a GraphQL
resolver, an MCP tool, a runner thread that inherited the request's
contextvars — it resolved to the endpoint's own Session, and its `finally`
close tore that session down mid-flight (objects expunged, pending adds
silently discarded). It now opens an independent SessionFactory() session.
"""

from __future__ import annotations

import pytest

from robosystems.db import platform as P


@pytest.mark.unit
def test_platform_session_does_not_touch_the_request_scoped_session():
  token = P.activate_request_scope()
  try:
    # The endpoint's session for this request scope.
    request_session = P.session()

    with P.platform_session() as nested:
      # It must be a distinct Session, not the request's own.
      assert nested is not request_session

    # After the nested session closes, the request's session must be untouched
    # — the same object, still registered in the scoped registry (pre-fix,
    # platform_session's finally called session.remove(), dropping it).
    assert P.session() is request_session
  finally:
    P.session.remove()
    P.deactivate_request_scope(token)


@pytest.mark.unit
def test_two_platform_sessions_are_distinct():
  with P.platform_session() as a, P.platform_session() as b:
    assert a is not b
