"""Rate limit cache tests."""

from unittest.mock import MagicMock, patch

import pytest

from robosystems.middleware.rate_limits.cache import RateLimitCache


@pytest.fixture
def cache():
    """Create RateLimitCache with mocked Redis."""
    with patch("robosystems.middleware.rate_limits.cache.env") as mock_env:
        mock_env.RATE_LIMIT_ENABLED = True
        c = RateLimitCache.__new__(RateLimitCache)
        c.enabled = True
        c._redis = MagicMock()
        return c


class TestGetRateLimitKey:
    def test_key_format(self, cache):
        key = cache._get_rate_limit_key("user:123")
        assert key == "rate_limit:user:123"

    def test_prefix(self, cache):
        assert cache.RATE_LIMIT_PREFIX == "rate_limit:"


class TestCheckRateLimit:
    def test_disabled_always_allows(self):
        with patch("robosystems.middleware.rate_limits.cache.env") as mock_env:
            mock_env.RATE_LIMIT_ENABLED = False
            c = RateLimitCache.__new__(RateLimitCache)
            c.enabled = False
            c._redis = None
            allowed, remaining = c.check_rate_limit("user:1", 100, 60)
            assert allowed is True
            assert remaining == 100

    def test_allowed_request(self, cache):
        cache._redis.eval.return_value = [1, 99]
        allowed, remaining = cache.check_rate_limit("user:1", 100, 60)
        assert allowed is True
        assert remaining == 99

    def test_blocked_request(self, cache):
        cache._redis.eval.return_value = [0, 0]
        allowed, remaining = cache.check_rate_limit("user:1", 100, 60)
        assert allowed is False
        assert remaining == 0

    def test_redis_failure_fails_open(self, cache):
        cache._redis.eval.side_effect = Exception("Redis down")
        allowed, remaining = cache.check_rate_limit("user:1", 100, 60)
        assert allowed is True
        assert remaining == 100


class TestGetRateLimitStats:
    def test_disabled(self):
        with patch("robosystems.middleware.rate_limits.cache.env") as mock_env:
            mock_env.RATE_LIMIT_ENABLED = False
            c = RateLimitCache.__new__(RateLimitCache)
            c.enabled = False
            stats = c.get_rate_limit_stats()
            assert stats["enabled"] is False

    def test_stats_returned(self, cache):
        cache._redis.keys.return_value = [
            "rate_limit:user:1",
            "rate_limit:ip:2",
            "rate_limit:apikey:3",
            "rate_limit:jwt:4",
        ]
        stats = cache.get_rate_limit_stats()
        assert stats["enabled"] is True
        assert stats["active_limits"] == 4

    def test_redis_failure(self, cache):
        cache._redis.keys.side_effect = Exception("Redis down")
        stats = cache.get_rate_limit_stats()
        assert "error" in stats


class TestClearRateLimit:
    def test_clears_successfully(self, cache):
        cache._redis.delete.return_value = 1
        result = cache.clear_rate_limit("user:1")
        assert result is True

    def test_nothing_to_clear(self, cache):
        cache._redis.delete.return_value = 0
        result = cache.clear_rate_limit("nonexistent")
        assert result is False

    def test_redis_failure(self, cache):
        cache._redis.delete.side_effect = Exception("Redis down")
        result = cache.clear_rate_limit("user:1")
        assert result is False


class TestGetSet:
    def test_get_disabled(self):
        with patch("robosystems.middleware.rate_limits.cache.env") as mock_env:
            mock_env.RATE_LIMIT_ENABLED = False
            c = RateLimitCache.__new__(RateLimitCache)
            c.enabled = False
            assert c.get("key") is None

    def test_get_success(self, cache):
        cache._redis.get.return_value = b"value"
        assert cache.get("key") == b"value"

    def test_get_failure(self, cache):
        cache._redis.get.side_effect = Exception("fail")
        assert cache.get("key") is None

    def test_set_disabled(self):
        with patch("robosystems.middleware.rate_limits.cache.env") as mock_env:
            mock_env.RATE_LIMIT_ENABLED = False
            c = RateLimitCache.__new__(RateLimitCache)
            c.enabled = False
            assert c.set("key", "val") is False

    def test_set_success(self, cache):
        cache._redis.set.return_value = True
        assert cache.set("key", "val", expire=60) is True

    def test_set_failure(self, cache):
        cache._redis.set.side_effect = Exception("fail")
        assert cache.set("key", "val") is False
