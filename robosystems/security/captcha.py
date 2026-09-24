"""Server-side Cloudflare Turnstile CAPTCHA verification."""

import logging
from dataclasses import dataclass

import aiohttp

from ..config import env

logger = logging.getLogger(__name__)

TURNSTILE_VERIFY_URL = "https://challenges.cloudflare.com/turnstile/v0/siteverify"


@dataclass
class CaptchaVerificationResult:
  success: bool
  error_codes: list[str]
  challenge_ts: str | None = None
  hostname: str | None = None
  action: str | None = None
  cdata: str | None = None


class CaptchaService:
  def __init__(self):
    self.secret_key = env.TURNSTILE_SECRET_KEY
    self.site_key = env.TURNSTILE_SITE_KEY

  async def verify_turnstile_token(
    self,
    token: str,
    remote_ip: str | None = None,
    idempotency_key: str | None = None,
  ) -> CaptchaVerificationResult:
    """Verify a token against the siteverify API. Errors resolve to an
    unsuccessful result, never an exception."""
    if not token:
      if not env.is_test():
        logger.warning("Empty CAPTCHA token provided")
      return CaptchaVerificationResult(
        success=False, error_codes=["missing-input-response"]
      )

    if not self.secret_key:
      # Fail closed in deployed environments; dev/test fall open.
      if env.is_production() or env.is_staging():
        logger.error(
          "TURNSTILE_SECRET_KEY not configured while CAPTCHA is enabled - "
          "failing closed"
        )
        return CaptchaVerificationResult(
          success=False, error_codes=["missing-secret-key"]
        )
      logger.warning(
        "TURNSTILE_SECRET_KEY not configured - CAPTCHA verification disabled"
      )
      return CaptchaVerificationResult(success=True, error_codes=["missing-secret-key"])

    data = {
      "secret": self.secret_key,
      "response": token,
    }

    if remote_ip:
      data["remoteip"] = remote_ip
    if idempotency_key:
      data["idempotency_key"] = idempotency_key

    try:
      async with (
        aiohttp.ClientSession() as session,
        session.post(
          TURNSTILE_VERIFY_URL, data=data, timeout=aiohttp.ClientTimeout(total=10)
        ) as response,
      ):
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
    return env.CAPTCHA_ENABLED

  def get_site_key(self) -> str:
    return self.site_key

  async def verify_captcha_or_skip(
    self, token: str | None, remote_ip: str | None = None
  ) -> CaptchaVerificationResult:
    """The entry point for auth endpoints: verifies when CAPTCHA is enabled,
    otherwise succeeds (``token`` may then be None)."""
    if not self.is_captcha_required():
      logger.info("CAPTCHA verification skipped (development mode)")
      return CaptchaVerificationResult(success=True, error_codes=["dev-mode-skip"])

    if not token:
      logger.warning("CAPTCHA token required in production but not provided")
      return CaptchaVerificationResult(
        success=False, error_codes=["missing-input-response"]
      )

    return await self.verify_turnstile_token(token, remote_ip)


captcha_service = CaptchaService()


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
  return TURNSTILE_ERROR_DESCRIPTIONS.get(error_code, f"Unknown error: {error_code}")
