"""The demos' direct-DB reset must prove the database is local before deleting.

`examples/_common/local_db.assert_local_extensions_db` is the one guard in
front of every demo `_reset` module. The API-URL check in the demos is not
enough on its own: an SSM tunnel makes production RDS answer on
`localhost:5432`, exactly where the local URL points.
"""

from unittest.mock import MagicMock, patch

import pytest

from examples._common.local_db import (
  NonLocalDatabaseError,
  assert_local_extensions_db,
)

MOD = "examples._common.local_db"
URL_ACCESSOR = "robosystems.db.extensions.get_extensions_database_url"


def _engine_answering(rdsadmin_present: bool):
  engine = MagicMock()
  conn = engine.connect.return_value.__enter__.return_value
  conn.execute.return_value.first.return_value = (1,) if rdsadmin_present else None
  return engine


@pytest.mark.unit
def test_local_host_and_plain_postgres_passes():
  engine = _engine_answering(rdsadmin_present=False)
  with (
    patch(
      URL_ACCESSOR,
      return_value="postgresql://postgres:postgres@localhost:5432/extensions",
    ),
    patch(f"{MOD}.create_engine", return_value=engine),
  ):
    assert_local_extensions_db()
  engine.dispose.assert_called_once()


@pytest.mark.unit
@pytest.mark.parametrize(
  "url",
  [
    "postgresql://postgres:x@robosystems-prod.cluster-abc.us-east-1.rds.amazonaws.com:5432/extensions",
    "postgresql://postgres:x@10.0.4.12:5432/extensions",
    "postgresql://postgres:x@extensions.internal:5432/extensions",
  ],
)
def test_non_local_host_is_refused_before_connecting(url):
  with (
    patch(URL_ACCESSOR, return_value=url),
    patch(f"{MOD}.create_engine") as create_engine,
  ):
    with pytest.raises(NonLocalDatabaseError, match="not a local Postgres"):
      assert_local_extensions_db()
  create_engine.assert_not_called()


@pytest.mark.unit
def test_tunnelled_rds_on_localhost_is_refused():
  """The scenario that motivates the guard: a local-looking URL whose other
  end is RDS. The host check passes; the database itself gives it away."""
  engine = _engine_answering(rdsadmin_present=True)
  with (
    patch(
      URL_ACCESSOR,
      return_value="postgresql://postgres:postgres@127.0.0.1:5432/extensions",
    ),
    patch(f"{MOD}.create_engine", return_value=engine),
  ):
    with pytest.raises(NonLocalDatabaseError, match="Amazon RDS"):
      assert_local_extensions_db()
  engine.dispose.assert_called_once()
