"""Password policy and validation endpoints."""

from fastapi import APIRouter

from ...models.api.auth import (
  PasswordCheckRequest,
  PasswordCheckResponse,
  PasswordPolicyResponse,
)
from ...security.password import PasswordSecurity

# Create router for password endpoints
router = APIRouter()


@router.get(
  "/password/policy",
  response_model=PasswordPolicyResponse,
  summary="Get Password Policy",
  description="Get current password policy requirements for frontend validation",
  operation_id="getPasswordPolicy",
  responses={
    200: {
      "description": "Password policy requirements",
      "content": {
        "application/json": {
          "example": {
            "policy": {
              "min_length": 12,
              "require_uppercase": True,
              "require_lowercase": True,
              "require_digits": True,
              "require_special": True,
            }
          }
        }
      },
    }
  },
)
async def get_password_policy():
  """
  Get current password policy requirements.

  Returns:
      Dict with password policy including complexity requirements
  """
  return PasswordPolicyResponse(policy=PasswordSecurity.get_password_policy())


@router.post(
  "/password/check",
  response_model=PasswordCheckResponse,
  summary="Check Password Strength",
  description="Check password strength and get validation feedback",
  operation_id="checkPasswordStrength",
  responses={
    200: {
      "description": "Password strength analysis",
      "content": {
        "application/json": {
          "example": {
            "is_valid": True,
            "strength": "good",
            "score": 75,
            "errors": [],
            "suggestions": ["Consider adding more special characters"],
            "character_types": {
              "uppercase": True,
              "lowercase": True,
              "digits": True,
              "special": True,
            },
          }
        }
      },
    }
  },
)
async def check_password_strength(request: PasswordCheckRequest):
  """
  Check password strength and provide feedback.

  Args:
      request: Password check request with password and optional email

  Returns:
      Password strength analysis and improvement suggestions
  """
  result = PasswordSecurity.validate_password(request.password, request.email)

  return PasswordCheckResponse(
    is_valid=result.is_valid,
    strength=result.strength.value,
    score=result.score,
    errors=result.errors,
    suggestions=result.suggestions,
    character_types=result.character_types,
  )
