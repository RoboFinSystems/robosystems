"""
Cloudflare Turnstile CAPTCHA verification service.

Provides server-side verification of Turnstile CAPTCHA tokens to prevent bot
registrations and other automated attacks.
"""

import logging
from dataclasses import dataclass

import aiohttp

from ..config import env

logger = logging.getLogger(__name__)

# Cloudflare Turnstile verification endpoint
TURNSTILE_VERIFY_URL = "https://challenges.cloudflare.com/turnstile/v0/siteverify"


@dataclass
class CaptchaVerificationResult:
  """Result of CAPTCHA verification."""

  success: bool
  error_codes: list[str]
  challenge_ts: str | None = None
  hostname: str | None = None
  action: str | None = None
  cdata: str | None = None


class CaptchaService:
  """Service for verifying CAPTCHA tokens."""

  def __init__(self):
    self.secret_key = env.TURNSTILE_SECRET_KEY
    self.site_key = env.TURNSTILE_SITE_KEY

  async def verify_turnstile_token(
    self,
    token: str,
    remote_ip: str | None = None,
    idempotency_key: str | None = None,
  ) -> CaptchaVerificationResult:
    """
    Verify a Cloudflare Turnstile CAPTCHA token.

    Args:
        token: The CAPTCHA token from the frontend
        remote_ip: Optional client IP address for additional validation
        idempotency_key: Optional idempotency key for duplicate prevention

    Returns:
        CaptchaVerificationResult with verification status and details
    """
    if not token:
      # Only log warning in non-test environments to reduce test noise
      if not env.is_test():
        logger.warning("Empty CAPTCHA token provided")
      return CaptchaVerificationResult(
        success=False, error_codes=["missing-input-response"]
      )

    if not self.secret_key:
      logger.warning(
        "TURNSTILE_SECRET_KEY not configured - CAPTCHA verification disabled"
      )
      return CaptchaVerificationResult(success=True, error_codes=["missing-secret-key"])

    # Prepare verification request
    data = {
      "secret": self.secret_key,
      "response": token,
    }

    # Add optional parameters
    if remote_ip:
      data["remoteip"] = remote_ip
    if idempotency_key:
      data["idempotency_key"] = idempotency_key

    try:
      async with aiohttp.ClientSession() as session, session.post(
        TURNSTILE_VERIFY_URL, data=data, timeout=aiohttp.ClientTimeout(total=10)
      ) as response:
        if response.status != 200:
          logger.error(f"Turnstile API returned status {response.status}")
          return CaptchaVerificationResult(success=False, error_codes=["api-error"])

        result_data = await response.json()

        return CaptchaVerificationResult(
          success=result_data.get("success", False),
          error_codes=result_data.get("error-codes", []),
          challenge_ts=result_data.get("challenge_ts"),
          hostname=result_data.get("hostname"),
          action=result_data.get("action"),
          cdata=result_data.get("cdata"),
        )

    except (TimeoutError, aiohttp.ClientError) as e:
      logger.error(f"HTTP error during CAPTCHA verification: {e}")
      return CaptchaVerificationResult(success=False, error_codes=["network-error"])
    except Exception as e:
      logger.error(f"Unexpected error during CAPTCHA verification: {e}")
      return CaptchaVerificationResult(success=False, error_codes=["internal-error"])

  def is_captcha_required(self) -> bool:
    """Check if CAPTCHA verification is required in current environment."""
    return env.CAPTCHA_ENABLED

  def get_site_key(self) -> str:
    """Get the Turnstile site key for frontend integration."""
    return self.site_key

  async def verify_captcha_or_skip(
    self, token: str | None, remote_ip: str | None = None
  ) -> CaptchaVerificationResult:
    """
    Verify CAPTCHA token if required, or skip if in development mode.

    This is the main method that should be used in auth endpoints.

    Args:
        token: CAPTCHA token (can be None in development)
        remote_ip: Client IP for additional validation

    Returns:
        CaptchaVerificationResult indicating success or failure
    """
    if not self.is_captcha_required():
      # Development mode - skip CAPTCHA verification
      logger.info("CAPTCHA verification skipped (development mode)")
      return CaptchaVerificationResult(success=True, error_codes=["dev-mode-skip"])

    if not token:
      logger.warning("CAPTCHA token required in production but not provided")
      return CaptchaVerificationResult(
        success=False, error_codes=["missing-input-response"]
      )

    return await self.verify_turnstile_token(token, remote_ip)


# Global instance for easy import
captcha_service = CaptchaService()


# Error code descriptions for debugging
TURNSTILE_ERROR_DESCRIPTIONS = {
  "missing-input-secret": "The secret parameter is missing",
  "invalid-input-secret": "The secret parameter is invalid or malformed",
  "missing-input-response": "The response parameter is missing",
  "invalid-input-response": "The response parameter is invalid or malformed",
  "bad-request": "The request is invalid or malformed",
  "timeout-or-duplicate": "The response is no longer valid (either timeout or duplicate)",
  "internal-error": "An internal error happened while validating the response",
  "api-error": "HTTP error from Turnstile API",
  "network-error": "Network error during verification",
  "missing-secret-key": "TURNSTILE_SECRET_KEY not configured",
  "dev-mode-skip": "CAPTCHA verification skipped in development mode",
}


def get_error_description(error_code: str) -> str:
  """Get human-readable description for CAPTCHA error code."""
  return TURNSTILE_ERROR_DESCRIPTIONS.get(error_code, f"Unknown error: {error_code}")
