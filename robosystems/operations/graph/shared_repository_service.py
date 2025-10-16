"""
Shared Repository Service for creating and managing shared graph repositories.

This service handles the creation of shared repositories (SEC, industry, economic, etc.)
that are accessible across multiple companies. These repositories contain public data
and are created on dedicated instances in production.

Key features:
- Shared repository creation through normal API flow
- Automatic DynamoDB registration via allocation manager
- Support for different repository types (SEC, industry, economic, etc.)
- Proper schema installation for each repository type
"""

from typing import Dict, Any, Optional
from datetime import datetime, timezone

from ...logger import logger
from ...graph_api.client import get_kuzu_client_for_instance
from ...config import env


class SharedRepositoryService:
  """Service for creating and managing shared graph repositories."""

  async def create_shared_repository(
    self,
    repository_name: str,
    created_by: Optional[str] = None,
  ) -> Dict[str, Any]:
    """
    Create a shared repository on the shared master instance.

    For shared repositories, we connect directly to the shared master
    instead of allocating a new database slot.

    Args:
        repository_name: Name of the repository (e.g., 'sec', 'industry')
        created_by: Optional user ID who initiated creation

    Returns:
        Dictionary containing repository creation details
    """
    logger.info(f"Creating shared repository: {repository_name}")

    # Validate repository name
    valid_repositories = ["sec", "industry", "economic", "regulatory", "market", "esg"]
    if repository_name not in valid_repositories:
      raise ValueError(
        f"Invalid repository name: {repository_name}. "
        f"Must be one of: {', '.join(valid_repositories)}"
      )

    try:
      # For shared repositories, connect to the shared master directly
      # The shared master is already running and registered in DynamoDB
      from ...graph_api.client.factory import KuzuClientFactory

      # Get the shared master URL from DynamoDB discovery
      shared_master_url = await KuzuClientFactory._get_shared_master_url()
      logger.info(f"Found shared master at {shared_master_url}")

      # Extract IP from URL (format: http://IP:PORT)
      import re

      match = re.match(r"http://([^:]+):(\d+)", shared_master_url)
      if not match:
        raise ValueError(f"Invalid shared master URL format: {shared_master_url}")

      master_ip = match.group(1)

      # Create database on the shared master
      kuzu_client = await get_kuzu_client_for_instance(master_ip)

      try:
        # Create the database with shared schema
        logger.info(f"Creating database for repository: {repository_name}")
        create_result = await kuzu_client.create_database(
          graph_id=repository_name,
          schema_type="shared",
          repository_name=repository_name,
        )

        logger.info(f"Database created successfully: {create_result.get('status')}")

        # Step 3: Verify database is healthy
        db_info = await kuzu_client.get_database_info(repository_name)
        if not db_info.get("is_healthy", False):
          raise RuntimeError(f"Database {repository_name} created but not healthy")

        logger.info(f"Shared repository '{repository_name}' created successfully!")

        # For shared master, we don't have instance_id from allocation
        # Get it from DynamoDB if needed
        instance_id = "shared-master"  # Default identifier

        return {
          "repository_name": repository_name,
          "graph_id": repository_name,
          "instance_id": instance_id,
          "status": "created",
          "created_at": datetime.now(timezone.utc).isoformat(),
          "created_by": created_by or "system",
          "database_info": db_info,
        }

      finally:
        await kuzu_client.close()

    except Exception as e:
      logger.error(f"Failed to create shared repository on shared master: {e}")
      # No allocation to clean up since we're using the shared master directly
      raise


async def ensure_shared_repository_exists(
  repository_name: str,
  kuzu_url: Optional[str] = None,
) -> Dict[str, Any]:
  """
  Ensure a shared repository exists, creating it if necessary.

  This is a convenience function that checks if a repository exists
  and creates it if not.

  Args:
      repository_name: Name of the repository (e.g., 'sec')
      kuzu_url: Optional Kuzu URL override

  Returns:
      Dictionary with repository status
  """
  if not kuzu_url:
    kuzu_url = env.KUZU_API_URL

  # Try to get database info first
  try:
    from ...graph_api.client.factory import KuzuClientFactory

    # Use factory to get proper client for shared repository
    # Shared repositories automatically route to shared_master/shared_replica infrastructure
    client = await KuzuClientFactory.create_client(
      graph_id=repository_name, operation_type="read"
    )

    try:
      db_info = await client.get_database_info(repository_name)
      if db_info.get("is_healthy", False):
        logger.info(f"Repository {repository_name} already exists")
        return {
          "status": "exists",
          "repository_name": repository_name,
          "database_info": db_info,
        }
    finally:
      await client.close()
  except Exception as e:
    logger.info(f"Repository {repository_name} not found, will create: {e}")

  # Create the repository
  service = SharedRepositoryService()
  return await service.create_shared_repository(repository_name)
