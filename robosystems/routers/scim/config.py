"""SCIM service-discovery endpoints (RFC 7643 §5-8).

Okta/Entra probe these before provisioning to learn what the server supports.
Static JSON — the values describe the minimal Users-only surface this build
implements (no Groups, PATCH supported, one filter).
"""

from fastapi import APIRouter

from ...config import env
from ...models.api.scim import USER_SCHEMA

router = APIRouter()


def _base_url() -> str:
  return f"{env.ROBOSYSTEMS_API_URL}/scim/v2"


@router.get("/ServiceProviderConfig", include_in_schema=False)
async def service_provider_config() -> dict:
  return {
    "schemas": ["urn:ietf:params:scim:schemas:core:2.0:ServiceProviderConfig"],
    "documentationUri": "https://docs.robosystems.ai/scim",
    "patch": {"supported": True},
    "bulk": {"supported": False, "maxOperations": 0, "maxPayloadSize": 0},
    "filter": {"supported": True, "maxResults": 200},
    "changePassword": {"supported": False},
    "sort": {"supported": False},
    "etag": {"supported": False},
    "authenticationSchemes": [
      {
        "type": "oauthbearertoken",
        "name": "OAuth Bearer Token",
        "description": "Authentication via a per-org SCIM bearer token.",
        "primary": True,
      }
    ],
    "meta": {
      "location": f"{_base_url()}/ServiceProviderConfig",
      "resourceType": "ServiceProviderConfig",
    },
  }


@router.get("/ResourceTypes", include_in_schema=False)
async def resource_types() -> dict:
  user_type = {
    "schemas": ["urn:ietf:params:scim:schemas:core:2.0:ResourceType"],
    "id": "User",
    "name": "User",
    "endpoint": "/Users",
    "description": "User Account",
    "schema": USER_SCHEMA,
    "meta": {
      "location": f"{_base_url()}/ResourceTypes/User",
      "resourceType": "ResourceType",
    },
  }
  return {
    "schemas": ["urn:ietf:params:scim:api:messages:2.0:ListResponse"],
    "totalResults": 1,
    "startIndex": 1,
    "itemsPerPage": 1,
    "Resources": [user_type],
  }


@router.get("/Schemas", include_in_schema=False)
async def schemas() -> dict:
  user_schema = {
    "id": USER_SCHEMA,
    "name": "User",
    "description": "User Account",
    "attributes": [
      {
        "name": "userName",
        "type": "string",
        "multiValued": False,
        "required": True,
        "caseExact": False,
        "mutability": "readWrite",
        "returned": "default",
        "uniqueness": "server",
      },
      {
        "name": "active",
        "type": "boolean",
        "multiValued": False,
        "required": False,
        "mutability": "readWrite",
        "returned": "default",
      },
    ],
    "meta": {
      "resourceType": "Schema",
      "location": f"{_base_url()}/Schemas/{USER_SCHEMA}",
    },
  }
  return {
    "schemas": ["urn:ietf:params:scim:api:messages:2.0:ListResponse"],
    "totalResults": 1,
    "startIndex": 1,
    "itemsPerPage": 1,
    "Resources": [user_schema],
  }
