"""Rate limit header middleware tests."""

import pytest
from starlette.testclient import TestClient
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from robosystems.middleware.rate_limits.headers import RateLimitHeaderMiddleware


def _make_app(state_attrs: dict | None = None):
    """Create a minimal Starlette app with the middleware."""

    async def homepage(request: Request):
        if state_attrs:
            for key, value in state_attrs.items():
                setattr(request.state, key, value)
        return JSONResponse({"ok": True})

    app = Starlette(routes=[Route("/", homepage)])
    app.add_middleware(RateLimitHeaderMiddleware)
    return app


class TestRateLimitHeaders:
    """Tests for RateLimitHeaderMiddleware."""

    def test_no_headers_when_no_state(self):
        app = _make_app()
        client = TestClient(app)
        response = client.get("/")
        assert "X-RateLimit-Remaining" not in response.headers
        assert "X-RateLimit-Limit" not in response.headers

    def test_general_rate_limit_headers(self):
        app = _make_app({"rate_limit_remaining": 95, "rate_limit_limit": 100})
        client = TestClient(app)
        response = client.get("/")
        assert response.headers["X-RateLimit-Remaining"] == "95"
        assert response.headers["X-RateLimit-Limit"] == "100"

    def test_subscription_headers(self):
        app = _make_app({"rate_limit_tier": "standard", "rate_limit_category": "query"})
        client = TestClient(app)
        response = client.get("/")
        assert response.headers["X-RateLimit-Tier"] == "standard"
        assert response.headers["X-RateLimit-Category"] == "query"

    def test_auth_rate_limit_headers(self):
        app = _make_app({"auth_rate_limit_remaining": 5, "auth_rate_limit_limit": 10})
        client = TestClient(app)
        response = client.get("/")
        assert response.headers["X-Auth-RateLimit-Remaining"] == "5"
        assert response.headers["X-Auth-RateLimit-Limit"] == "10"

    def test_mcp_rate_limit_headers(self):
        app = _make_app({"mcp_rate_limit_remaining": 48, "mcp_rate_limit_limit": 50})
        client = TestClient(app)
        response = client.get("/")
        assert response.headers["X-MCP-RateLimit-Remaining"] == "48"
        assert response.headers["X-MCP-RateLimit-Limit"] == "50"

    def test_agent_rate_limit_headers(self):
        app = _make_app({"agent_rate_limit_remaining": 8, "agent_rate_limit_limit": 10})
        client = TestClient(app)
        response = client.get("/")
        assert response.headers["X-Agent-RateLimit-Remaining"] == "8"
        assert response.headers["X-Agent-RateLimit-Limit"] == "10"

    def test_mixed_headers(self):
        app = _make_app({
            "rate_limit_remaining": 90,
            "rate_limit_limit": 100,
            "rate_limit_tier": "premium",
        })
        client = TestClient(app)
        response = client.get("/")
        assert response.headers["X-RateLimit-Remaining"] == "90"
        assert response.headers["X-RateLimit-Tier"] == "premium"
