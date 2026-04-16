"""Password policy and validation endpoints."""

from fastapi import APIRouter

from ...models.api.auth import (
  PasswordCheckRequest,
  PasswordCheckResponse,
  PasswordPolicyResponse,
)
from ...models.api.common import COMMON_ERROR_RESPONSES
from ...security.password import PasswordSecurity

router = APIRouter()


@router.get(
  "/password/policy",
  response_model=PasswordPolicyResponse,
  summary="Get Password Policy",
  operation_id="getPasswordPolicy",
  responses={**COMMON_ERROR_RESPONSES},
)
async def get_password_policy():
  return PasswordPolicyResponse(policy=PasswordSecurity.get_password_policy())


@router.post(
  "/password/check",
  response_model=PasswordCheckResponse,
  summary="Check Password Strength",
  operation_id="checkPasswordStrength",
  responses={**COMMON_ERROR_RESPONSES},
)
async def check_password_strength(request: PasswordCheckRequest):
  result = PasswordSecurity.validate_password(request.password, request.email)

  return PasswordCheckResponse(
    is_valid=result.is_valid,
    strength=result.strength.value,
    score=result.score,
    errors=result.errors,
    suggestions=result.suggestions,
    character_types=result.character_types,
  )
