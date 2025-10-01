"""
Utility functions for consistent Kuzu database path handling.

This module ensures consistent path construction for Kuzu databases
using the Kuzu 0.11.0 single-file format with .kuzu extension.
"""

from pathlib import Path
from typing import Union, Optional

from ...config import env


def get_kuzu_database_path(db_name: str, base_path: Optional[str] = None) -> Path:
  """
  Get the correct path for a Kuzu database, handling shared repositories.

  This function ensures consistent path construction across the codebase,
  using the new Kuzu 0.11.0 single-file format with .kuzu extension.

  Args:
      db_name: Name of the database (e.g., 'sec', 'kg1a2b3c')
      base_path: Optional base path override (defaults to KUZU_DATABASE_PATH env var)

  Returns:
      Path object for the database file (.kuzu extension)
  """
  # Get base path from environment or parameter
  if base_path is None:
    base_path_str = env.KUZU_DATABASE_PATH
  else:
    base_path_str = base_path

  base_path_obj = Path(base_path_str)

  db_path = base_path_obj / f"{db_name}.kuzu"

  return db_path


def ensure_kuzu_directory(db_path: Union[str, Path]) -> None:
  """
  Ensure the parent directory for a Kuzu database exists.

  Args:
      db_path: Path to the database file
  """
  db_path = Path(db_path)
  db_path.parent.mkdir(parents=True, exist_ok=True)
