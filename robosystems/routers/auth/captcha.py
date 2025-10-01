"""CAPTCHA configuration endpoint."""

from fastapi import APIRouter

from ...security.captcha import captcha_service

# Create router for captcha endpoint
router = APIRouter()


@router.get(
  "/captcha/config",
  summary="Get CAPTCHA Configuration",
  description="Get CAPTCHA configuration including site key and whether CAPTCHA is required.",
  operation_id="getCaptchaConfig",
  responses={
    200: {
      "description": "CAPTCHA configuration",
      "content": {
        "application/json": {
          "example": {
            "required": True,
            "site_key": "0x4AAAAAAA...",
            "provider": "turnstile",
          }
        }
      },
    }
  },
)
async def get_captcha_config():
  """
  Get CAPTCHA configuration for frontend integration.

  Returns:
      Dict with CAPTCHA configuration including whether it's required and site key
  """
  return {
    "required": captcha_service.is_captcha_required(),
    "site_key": captcha_service.get_site_key()
    if captcha_service.is_captcha_required()
    else None,
    "provider": "turnstile",
  }
