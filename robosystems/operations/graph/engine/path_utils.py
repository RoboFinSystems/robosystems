"""Path construction for LadybugDB databases (single ``.lbdb`` file per graph)."""

from pathlib import Path

from ....config import env
from ....config.storage.graph import graph_db_filename


def get_lbug_database_path(db_name: str, base_path: str | None = None) -> Path:
  """``{base}/{db_name}.lbdb``, defaulting the base to ``LBUG_DATABASE_PATH``."""
  if base_path is None:
    base_path_str = env.LBUG_DATABASE_PATH
  else:
    base_path_str = base_path

  base_path_obj = Path(base_path_str)

  db_path = base_path_obj / graph_db_filename(db_name)

  return db_path


def ensure_lbug_directory(db_path: str | Path) -> None:
  """Create the database file's parent directory if it is missing."""
  db_path = Path(db_path)
  db_path.parent.mkdir(parents=True, exist_ok=True)
