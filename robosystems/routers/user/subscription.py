"""
User subscription management endpoints for shared repository access.

Provides endpoints for:
- Checking user subscription status
- Viewing subscription features
- Managing shared repository add-ons (SEC, industry data, etc.)
- Subscribing to and managing shared repository access

Note: Graph billing is now handled per-graph via the credit system.
"""

import logging
from datetime import datetime, timezone
from typing import Dict, Any

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from ...database import get_db_session
from ...middleware.auth.dependencies import get_current_user
from robosystems.middleware.rate_limits import user_management_rate_limit_dependency
from ...models.iam import User
from ...models.iam.user_repository import (
  UserRepository,
  RepositoryAccessLevel,
)
from ...models.iam.user_repository_credits import UserRepositoryCredits
from ...models.api.subscription import (
  UserSubscriptionsResponse,
  SubscriptionRequest,
  SubscriptionResponse,
  TierUpgradeRequest,
  AddOnCreditInfo,
  CreditsSummaryResponse,
  RepositoryCreditsResponse,
  CancellationResponse,
)


# Subscription API models moved to robosystems.models.api.subscription


logger = logging.getLogger(__name__)

router = APIRouter(
  tags=["User Subscriptions"],
  dependencies=[Depends(get_current_user)],
)


# Shared Repository Add-on Management


@router.get(
  "/shared-repositories",
  response_model=UserSubscriptionsResponse,
  operation_id="getUserSharedSubscriptions",
  summary="Get User Subscriptions",
  description="Retrieve user's current shared repository subscriptions with detailed information",
  responses={
    200: {
      "description": "Successfully retrieved user subscriptions",
      "content": {
        "application/json": {
          "example": {
            "subscriptions": [
              {
                "id": "addon_abc123",
                "user_id": "user_xyz789",
                "addon_type": "sec_data",
                "addon_tier": "basic",
                "is_active": True,
                "activated_at": "2024-01-15T10:30:00Z",
                "expires_at": None,
                "monthly_price_cents": 2999,
                "features": ["Access to SEC XBRL filings", "5,000 credits per month"],
                "metadata": {"subscription_method": "api"},
              }
            ],
            "total_count": 1,
            "active_count": 1,
          }
        }
      },
    },
    401: {"description": "Authentication required"},
    500: {"description": "Internal server error"},
  },
)
async def get_user_shared_subscriptions(
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  active_only: bool = Query(True, description="Only return active subscriptions"),
  _rate_limit: None = Depends(user_management_rate_limit_dependency),
):
  """Get user's current shared repository subscriptions."""
  try:
    access_records = UserRepository.get_user_repositories(
      user_id=current_user.id, session=db, active_only=active_only
    )

    subscriptions = []
    for access_record in access_records:
      # Map the fields to match the expected response model
      subscription_dict = access_record.to_dict()

      # Get the repository config to extract features
      config = subscription_dict.get("config", {})
      features = []
      if config:
        features.append(
          f"{subscription_dict.get('monthly_credit_allocation', 0):,} credits per month"
        )
        features.append(
          f"${subscription_dict.get('monthly_price_cents', 0) / 100:.2f}/month"
        )
        features.append(
          f"{subscription_dict.get('access_level', 'read').title()} access"
        )

      subscription_info = {
        "id": subscription_dict["id"],
        "user_id": subscription_dict["user_id"],
        "addon_type": subscription_dict[
          "repository_type"
        ],  # Map repository_type to addon_type
        "addon_tier": subscription_dict[
          "repository_plan"
        ],  # Map repository_plan to addon_tier
        "is_active": subscription_dict["is_active"],
        "activated_at": subscription_dict["activated_at"],
        "expires_at": subscription_dict["expires_at"],
        "monthly_price_cents": subscription_dict["monthly_price_cents"],
        "features": features,
        "metadata": subscription_dict.get("metadata", {}),
      }
      subscriptions.append(subscription_info)

    return {
      "subscriptions": subscriptions,
      "total_count": len(subscriptions),
      "active_count": len([s for s in subscriptions if s["is_active"]]),
    }

  except Exception as e:
    logger.error(f"Failed to get user shared subscriptions: {e}")
    raise HTTPException(status_code=500, detail="Failed to retrieve user subscriptions")


@router.post(
  "/shared-repositories/subscribe",
  response_model=SubscriptionResponse,
  status_code=status.HTTP_201_CREATED,
  operation_id="subscribeToSharedRepository",
  summary="Subscribe to Shared Repository",
  description="Create a new subscription to a shared repository add-on with specified tier",
  responses={
    201: {
      "description": "Successfully subscribed to shared repository",
      "content": {
        "application/json": {
          "example": {
            "message": "Successfully subscribed to shared repository",
            "subscription": {
              "id": "addon_abc123",
              "user_id": "user_xyz789",
              "addon_type": "sec_data",
              "addon_tier": "basic",
              "is_active": True,
              "activated_at": "2024-01-15T10:30:00Z",
              "expires_at": "2024-01-22T10:30:00Z",
              "monthly_price_cents": 0,
              "features": ["Access to SEC XBRL filings", "5,000 credits per month"],
              "metadata": {"subscription_method": "api"},
            },
          }
        }
      },
    },
    400: {
      "description": "Invalid add-on type or tier, or user already has active subscription"
    },
    401: {"description": "Authentication required"},
    500: {"description": "Internal server error"},
  },
)
async def subscribe_to_shared_repository(
  subscription_request: SubscriptionRequest,
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(user_management_rate_limit_dependency),
):
  """Subscribe to a shared repository add-on."""
  try:
    # Check if repository is enabled for subscriptions
    if not UserRepository.is_repository_enabled(subscription_request.repository_type):
      raise HTTPException(
        status_code=400,
        detail=f"Repository type {subscription_request.repository_type.value} is not available for subscription at this time",
      )

    # Get pricing information
    subscription_config = _get_repository_plans_config()
    tier_config = subscription_config.get(subscription_request.repository_type, {}).get(
      subscription_request.repository_plan, {}
    )

    if not tier_config:
      raise HTTPException(
        status_code=400,
        detail=f"Invalid repository type {subscription_request.repository_type} or plan {subscription_request.repository_plan}",
      )

    # Calculate price
    monthly_price_cents = int(tier_config["price_monthly"] * 100)

    # Create the subscription
    access_record = UserRepository.create_access(
      user_id=current_user.id,
      repository_type=subscription_request.repository_type,
      repository_name=subscription_request.repository_type.value,
      access_level=RepositoryAccessLevel.READ,  # Default to read access
      repository_plan=subscription_request.repository_plan,
      session=db,
      monthly_price_cents=monthly_price_cents,
      monthly_credits=tier_config["monthly_credits"],
      metadata={
        "subscribed_at": datetime.now(timezone.utc).isoformat(),
        "subscription_method": "api",
      },
    )

    # Map the subscription to match expected response format
    subscription_dict = access_record.to_dict()

    # Get features for the subscription
    features = []
    if tier_config:
      features.append(f"{tier_config.get('monthly_credits', 0):,} credits per month")
      features.append(f"${tier_config.get('price_monthly', 0)}/month")
      features.append(
        f"{tier_config.get('access_level', RepositoryAccessLevel.READ).value.title()} access"
      )

    subscription_response = {
      "id": subscription_dict["id"],
      "user_id": subscription_dict["user_id"],
      "addon_type": subscription_dict["repository_type"],
      "addon_tier": subscription_dict["repository_plan"],
      "is_active": subscription_dict["is_active"],
      "activated_at": subscription_dict["activated_at"],
      "expires_at": subscription_dict["expires_at"],
      "monthly_price_cents": subscription_dict["monthly_price_cents"],
      "features": features,
      "metadata": subscription_dict.get("metadata", {}),
    }

    return {
      "message": "Successfully subscribed to shared repository",
      "subscription": subscription_response,
    }

  except ValueError as e:
    logger.warning(f"Invalid subscription request: {e}")
    from robosystems.security import raise_secure_error, ErrorType

    raise_secure_error(
      ErrorType.VALIDATION_ERROR,
      original_error=e,
      additional_context={"operation": "create_subscription"},
    )
  except Exception as e:
    logger.error(f"Failed to subscribe to shared repository: {e}")
    raise HTTPException(status_code=500, detail="Failed to create subscription")


@router.put(
  "/shared-repositories/{subscription_id}/upgrade",
  operation_id="upgradeSharedRepositorySubscription",
  summary="Upgrade Subscription Tier",
  description="Upgrade a subscription to a higher tier with immediate credit adjustment",
  responses={
    200: {
      "description": "Successfully upgraded subscription tier",
      "content": {
        "application/json": {
          "example": {
            "message": "Successfully upgraded subscription tier",
            "subscription": {
              "id": "addon_abc123",
              "addon_type": "sec_data",
              "addon_tier": "professional",
              "monthly_price_cents": 9999,
              "features": [
                "Access to SEC XBRL filings",
                "25,000 credits per month",
                "Advanced analytics",
              ],
            },
          }
        }
      },
    },
    400: {"description": "Invalid tier for add-on type"},
    404: {"description": "Subscription not found or not owned by user"},
    401: {"description": "Authentication required"},
    500: {"description": "Internal server error"},
  },
)
async def upgrade_repository_plan(
  subscription_id: str,
  upgrade_request: TierUpgradeRequest,
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(user_management_rate_limit_dependency),
):
  """Upgrade a subscription to a higher tier."""
  try:
    # Get the access record
    access_record = (
      db.query(UserRepository)
      .filter(
        UserRepository.id == subscription_id,
        UserRepository.user_id == current_user.id,
        UserRepository.is_active,
      )
      .first()
    )

    if not access_record:
      raise HTTPException(
        status_code=404, detail="Subscription not found or not owned by user"
      )

    # Check if repository is still enabled
    if not UserRepository.is_repository_enabled(access_record.repository_type):
      raise HTTPException(
        status_code=400,
        detail=f"Repository type {access_record.repository_type.value} is no longer available for subscription changes",
      )

    # Get pricing for new plan
    subscription_config = _get_repository_plans_config()
    tier_config = subscription_config.get(access_record.repository_type.value, {}).get(
      upgrade_request.new_plan, {}
    )

    if not tier_config:
      raise HTTPException(
        status_code=400,
        detail=f"Invalid plan {upgrade_request.new_plan} for repository type {access_record.repository_type}",
      )

    # Calculate new price
    new_price_cents = int(tier_config["price_monthly"] * 100)

    # Upgrade the plan
    access_record.upgrade_tier(
      new_plan=upgrade_request.new_plan, session=db, new_price_cents=new_price_cents
    )

    # Map the subscription to match expected response format
    subscription_dict = access_record.to_dict()

    # Get features for the upgraded subscription
    features = []
    if tier_config:
      features.append(f"{tier_config.get('monthly_credits', 0):,} credits per month")
      features.append(f"${tier_config.get('price_monthly', 0)}/month")
      features.append(
        f"{tier_config.get('access_level', RepositoryAccessLevel.READ).value.title()} access"
      )

    subscription_response = {
      "id": subscription_dict["id"],
      "user_id": subscription_dict["user_id"],
      "addon_type": subscription_dict["repository_type"],
      "addon_tier": subscription_dict["repository_plan"],
      "is_active": subscription_dict["is_active"],
      "activated_at": subscription_dict["activated_at"],
      "expires_at": subscription_dict["expires_at"],
      "monthly_price_cents": subscription_dict["monthly_price_cents"],
      "features": features,
      "metadata": subscription_dict.get("metadata", {}),
    }

    return {
      "message": "Successfully upgraded subscription tier",
      "subscription": subscription_response,
    }

  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"Failed to upgrade subscription tier: {e}")
    raise HTTPException(status_code=500, detail="Failed to upgrade subscription")


@router.delete(
  "/shared-repositories/{subscription_id}",
  response_model=CancellationResponse,
  operation_id="cancelSharedRepositorySubscription",
  summary="Cancel Subscription",
  description="Cancel a shared repository subscription and disable associated credit pool",
  responses={
    200: {
      "description": "Successfully cancelled subscription",
      "content": {
        "application/json": {
          "example": {
            "message": "Successfully cancelled subscription",
            "subscription_id": "addon_abc123",
            "cancelled_at": "2024-01-15T10:30:00Z",
          }
        }
      },
    },
    404: {"description": "Subscription not found or not owned by user"},
    401: {"description": "Authentication required"},
    500: {"description": "Internal server error"},
  },
)
async def cancel_subscription(
  subscription_id: str,
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(user_management_rate_limit_dependency),
):
  """Cancel a shared repository subscription."""
  try:
    # Get the access record
    access_record = (
      db.query(UserRepository)
      .filter(
        UserRepository.id == subscription_id,
        UserRepository.user_id == current_user.id,
        UserRepository.is_active,
      )
      .first()
    )

    if not access_record:
      raise HTTPException(
        status_code=404, detail="Subscription not found or not owned by user"
      )

    # Cancel the subscription
    access_record.cancel_subscription(session=db)

    return {
      "message": "Successfully cancelled subscription",
      "subscription_id": subscription_id,
      "cancelled_at": datetime.now(timezone.utc).isoformat(),
    }

  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"Failed to cancel subscription: {e}")
    raise HTTPException(status_code=500, detail="Failed to cancel subscription")


@router.get(
  "/shared-repositories/credits",
  response_model=CreditsSummaryResponse,
  operation_id="getSharedRepositoryCredits",
  summary="Get Credit Balances",
  description="Retrieve credit balances for all shared repository subscriptions",
  responses={
    200: {
      "description": "Successfully retrieved credit balances",
      "content": {
        "application/json": {
          "example": {
            "add_ons": [
              {
                "subscription_id": "sub_abc123",
                "addon_type": "sec_data",
                "name": "Sec Data",
                "tier": "basic",
                "credits_remaining": 4500.0,
                "credits_allocated": 5000.0,
                "credits_consumed": 500.0,
                "rollover_amount": 0.0,
              }
            ],
            "total_credits": 4500.0,
            "credits_by_addon": [
              {
                "addon_type": "sec_data",
                "addon_tier": "basic",
                "subscription_id": "sub_abc123",
                "current_balance": 4500.0,
                "monthly_allocation": 5000.0,
                "consumed_this_month": 500.0,
                "usage_percentage": 10.0,
                "rollover_credits": 0.0,
                "allows_rollover": False,
              }
            ],
            "addon_count": 1,
          }
        }
      },
    },
    401: {"description": "Authentication required"},
    500: {"description": "Internal server error"},
  },
)
async def get_shared_repository_credits(
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(user_management_rate_limit_dependency),
):
  """Get credit balances for all shared repository subscriptions."""
  try:
    access_records = UserRepository.get_user_repositories(
      user_id=current_user.id, session=db, active_only=True
    )

    credits_summary = []
    subscriptions_list = []
    total_credits = 0

    for access_record in access_records:
      if access_record.shared_credits:
        credit_info = access_record.shared_credits.get_summary()
        credit_info["repository_type"] = access_record.repository_type
        credit_info["repository_plan"] = access_record.repository_plan
        credit_info["access_id"] = access_record.id
        credits_summary.append(credit_info)
        total_credits += credit_info["current_balance"]

        # Create SubscriptionCreditInfo for new format
        subscription_info = AddOnCreditInfo(
          subscription_id=access_record.id,
          addon_type=access_record.repository_type,
          name=access_record.repository_type.replace("_", " ").title(),
          tier=access_record.repository_plan,
          credits_remaining=credit_info["current_balance"],
          credits_allocated=credit_info["monthly_allocation"],
          credits_consumed=credit_info["consumed_this_month"],
          rollover_amount=credit_info.get("rollover_credits", 0),
        )
        subscriptions_list.append(subscription_info)

    return {
      "add_ons": subscriptions_list,
      "total_credits": total_credits,
      "credits_by_addon": credits_summary,  # Keep for backward compatibility
      "addon_count": len(credits_summary),
    }

  except Exception as e:
    logger.error(f"Failed to get shared repository credits: {e}")
    raise HTTPException(status_code=500, detail="Failed to retrieve credit information")


@router.get(
  "/shared-repositories/credits/{repository}",
  response_model=RepositoryCreditsResponse,
  operation_id="getRepositoryCredits",
  summary="Get Repository Credits",
  description="Get credit balance for a specific shared repository",
  responses={
    200: {
      "description": "Successfully retrieved repository credits",
      "content": {
        "application/json": {
          "examples": {
            "has_access": {
              "summary": "User has access to repository",
              "value": {
                "repository": "sec",
                "has_access": True,
                "credits": {
                  "current_balance": 4500.0,
                  "monthly_allocation": 5000.0,
                  "consumed_this_month": 500.0,
                  "usage_percentage": 10.0,
                  "is_active": True,
                },
              },
            },
            "no_access": {
              "summary": "User has no access to repository",
              "value": {
                "repository": "sec",
                "has_access": False,
                "message": "No active subscription for sec repository",
              },
            },
          }
        }
      },
    },
    401: {"description": "Authentication required"},
    500: {"description": "Internal server error"},
  },
)
async def get_repository_credits(
  repository: str,
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(user_management_rate_limit_dependency),
):
  """Get credit balance for a specific shared repository."""
  try:
    credits = UserRepositoryCredits.get_user_repository_credits(
      user_id=current_user.id, repository_type=repository, session=db
    )

    if not credits:
      return {
        "repository": repository,
        "has_access": False,
        "message": f"No active subscription for {repository} repository",
      }

    return {
      "repository": repository,
      "has_access": True,
      "credits": credits.get_summary(),
    }

  except Exception as e:
    logger.error(f"Failed to get repository credits: {e}")
    raise HTTPException(status_code=500, detail="Failed to retrieve repository credits")


def _get_repository_plans_config() -> Dict[str, Dict[str, Any]]:
  """Get repository plan configuration for all repository types."""
  configs = UserRepository.get_all_repository_configs()

  # Extract just the plans for backward compatibility
  result = {}
  for repo_type, repo_config in configs.items():
    if "plans" in repo_config:
      result[repo_type] = repo_config["plans"]

  return result
