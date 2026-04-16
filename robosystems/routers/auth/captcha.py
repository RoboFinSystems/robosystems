"""CAPTCHA configuration endpoint."""

from fastapi import APIRouter

from ...models.api.common import COMMON_ERROR_RESPONSES
from ...security.captcha import captcha_service

router = APIRouter()


@router.get(
  "/captcha/config",
  summary="Get CAPTCHA Configuration",
  description="Returns site key and whether CAPTCHA is required. Site key is null when CAPTCHA is disabled.",
  operation_id="getCaptchaConfig",
  responses={**COMMON_ERROR_RESPONSES},
)
async def get_captcha_config():
  return {
    "required": captcha_service.is_captcha_required(),
    "site_key": captcha_service.get_site_key()
    if captcha_service.is_captcha_required()
    else None,
    "provider": "turnstile",
  }
