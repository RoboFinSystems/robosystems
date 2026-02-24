"""Device fingerprinting tests."""

from unittest.mock import MagicMock

from robosystems.security.device_fingerprinting import (
    create_device_hash,
    extract_device_fingerprint,
    is_fingerprint_suspicious,
    validate_device_fingerprint,
)


class TestExtractDeviceFingerprint:
    """Tests for extract_device_fingerprint."""

    def _make_request(self, headers: dict | None = None):
        request = MagicMock()
        _headers = headers or {}
        request.headers.get = lambda key, default="": _headers.get(key, default)
        return request

    def test_extracts_user_agent(self):
        request = self._make_request({"user-agent": "Mozilla/5.0"})
        fp = extract_device_fingerprint(request)
        assert fp["user_agent"] == "Mozilla/5.0"

    def test_extracts_accept_language(self):
        request = self._make_request({"accept-language": "en-US,en;q=0.9"})
        fp = extract_device_fingerprint(request)
        assert fp["accept_language"] == "en-US,en;q=0.9"

    def test_extracts_accept_encoding(self):
        request = self._make_request({"accept-encoding": "gzip, deflate, br"})
        fp = extract_device_fingerprint(request)
        assert fp["accept_encoding"] == "gzip, deflate, br"

    def test_extracts_client_hints(self):
        request = self._make_request(
            {
                "sec-ch-ua": '"Chromium";v="124"',
                "sec-ch-ua-platform": '"macOS"',
            }
        )
        fp = extract_device_fingerprint(request)
        assert fp["sec_ch_ua"] == '"Chromium";v="124"'
        assert fp["sec_ch_ua_platform"] == '"macOS"'

    def test_defaults_to_empty_strings(self):
        request = self._make_request()
        fp = extract_device_fingerprint(request)
        assert fp["user_agent"] == ""
        assert fp["accept_language"] == ""
        assert fp["accept_encoding"] == ""
        assert fp["sec_ch_ua"] == ""
        assert fp["sec_ch_ua_platform"] == ""

    def test_returns_all_expected_keys(self):
        request = self._make_request()
        fp = extract_device_fingerprint(request)
        expected_keys = {
            "user_agent",
            "accept_language",
            "accept_encoding",
            "sec_ch_ua",
            "sec_ch_ua_platform",
        }
        assert set(fp.keys()) == expected_keys


class TestCreateDeviceHash:
    """Tests for create_device_hash."""

    def test_returns_hex_string(self):
        fp = {"user_agent": "test", "accept_language": "en"}
        result = create_device_hash(fp)
        assert isinstance(result, str)
        assert len(result) == 64  # SHA256 hex digest

    def test_deterministic(self):
        fp = {"user_agent": "Mozilla/5.0", "accept_language": "en"}
        assert create_device_hash(fp) == create_device_hash(fp)

    def test_order_independent(self):
        fp1 = {"b": "2", "a": "1"}
        fp2 = {"a": "1", "b": "2"}
        assert create_device_hash(fp1) == create_device_hash(fp2)

    def test_different_fingerprints_different_hashes(self):
        fp1 = {"user_agent": "Chrome"}
        fp2 = {"user_agent": "Firefox"}
        assert create_device_hash(fp1) != create_device_hash(fp2)


class TestValidateDeviceFingerprint:
    """Tests for validate_device_fingerprint."""

    def test_matching_fingerprint(self):
        fp = {"user_agent": "Chrome", "accept_language": "en"}
        stored_hash = create_device_hash(fp)
        assert validate_device_fingerprint(stored_hash, fp) is True

    def test_mismatched_fingerprint(self):
        fp_original = {"user_agent": "Chrome"}
        fp_current = {"user_agent": "Firefox"}
        stored_hash = create_device_hash(fp_original)
        assert validate_device_fingerprint(stored_hash, fp_current) is False


class TestIsFingerprintSuspicious:
    """Tests for is_fingerprint_suspicious."""

    def test_identical_fingerprints_not_suspicious(self):
        fp = {
            "user_agent": "Chrome",
            "sec_ch_ua": '"Chromium";v="124"',
            "accept_language": "en",
            "accept_encoding": "gzip",
        }
        suspicious, changes = is_fingerprint_suspicious(fp, fp)
        assert suspicious is False
        assert changes == []

    def test_user_agent_change_is_suspicious(self):
        stored = {"user_agent": "Chrome", "sec_ch_ua": "same"}
        current = {"user_agent": "Firefox", "sec_ch_ua": "same"}
        suspicious, changes = is_fingerprint_suspicious(stored, current)
        assert suspicious is True
        assert "user_agent_changed" in changes

    def test_browser_hints_change_is_suspicious(self):
        stored = {"user_agent": "same", "sec_ch_ua": '"Chromium"'}
        current = {"user_agent": "same", "sec_ch_ua": '"Firefox"'}
        suspicious, changes = is_fingerprint_suspicious(stored, current)
        assert suspicious is True
        assert "browser_hints_changed" in changes

    def test_language_change_not_suspicious(self):
        stored = {
            "user_agent": "same",
            "sec_ch_ua": "same",
            "accept_language": "en",
        }
        current = {
            "user_agent": "same",
            "sec_ch_ua": "same",
            "accept_language": "fr",
        }
        suspicious, changes = is_fingerprint_suspicious(stored, current)
        assert suspicious is False
        assert "language_changed" in changes

    def test_encoding_change_not_suspicious(self):
        stored = {
            "user_agent": "same",
            "sec_ch_ua": "same",
            "accept_encoding": "gzip",
        }
        current = {
            "user_agent": "same",
            "sec_ch_ua": "same",
            "accept_encoding": "br",
        }
        suspicious, changes = is_fingerprint_suspicious(stored, current)
        assert suspicious is False
        assert "encoding_changed" in changes

    def test_multiple_changes(self):
        stored = {
            "user_agent": "Chrome",
            "sec_ch_ua": '"Chromium"',
            "accept_language": "en",
            "accept_encoding": "gzip",
        }
        current = {
            "user_agent": "Firefox",
            "sec_ch_ua": '"Firefox"',
            "accept_language": "fr",
            "accept_encoding": "br",
        }
        suspicious, changes = is_fingerprint_suspicious(stored, current)
        assert suspicious is True
        assert len(changes) == 4
