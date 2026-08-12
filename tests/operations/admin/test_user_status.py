"""Tests for administrative activate/deactivate.

Deactivation is the escalation above a credential rotation: a password change
invalidates sessions but leaves API keys working, so this is the only path that
cuts off programmatic access short of deleting the account. What matters here is
that it revokes *both* halves, and that re-running it finishes a partial job.
"""

from uuid import uuid4

import pytest

from robosystems.models.core import User
from robosystems.models.core.user.user_api_key import UserAPIKey
from robosystems.operations.admin import UserNotFound, set_user_active


def _create_user(session, password_hash: str) -> User:
  suffix = uuid4().hex[:8]
  user = User(
    email=f"suspendable+{suffix}@example.com",
    name="Suspendable User",
    password_hash=password_hash,
  )
  session.add(user)
  session.commit()
  session.refresh(user)
  return user


class TestDeactivate:
  def test_revokes_sessions_and_keys(self, test_db, test_user):
    user = _create_user(test_db, test_user.password_hash)
    UserAPIKey.create(user_id=user.id, name="key one", session=test_db)
    UserAPIKey.create(user_id=user.id, name="key two", session=test_db)
    before = user.session_version or 0

    change = set_user_active(user.id, False, test_db)

    assert change.is_active is False
    assert change.changed is True
    assert change.api_keys_revoked == 2

    test_db.refresh(user)
    assert user.is_active is False
    # Both halves of access are cut: JWTs die via the version bump, keys via
    # their own flag. Neither alone is sufficient.
    assert (user.session_version or 0) > before
    assert UserAPIKey.get_active_by_user_id(str(user.id), test_db) == []

  def test_rerun_finishes_a_partial_revocation(self, test_db, test_user):
    """A retry during an incident must not short-circuit on the is_active flag.

    Key revocation is best-effort per key, so a key can survive a failed first
    pass. If the second call no-opped because the user was already inactive,
    that key would stay live — the exact opposite of what the operator wants.
    """
    user = _create_user(test_db, test_user.password_hash)
    set_user_active(user.id, False, test_db)

    # Simulate a key that outlived the first pass.
    UserAPIKey.create(user_id=user.id, name="straggler", session=test_db)
    assert len(UserAPIKey.get_active_by_user_id(str(user.id), test_db)) == 1

    change = set_user_active(user.id, False, test_db)

    assert change.changed is False  # flag was already off...
    assert change.api_keys_revoked == 1  # ...but the straggler was still killed
    assert UserAPIKey.get_active_by_user_id(str(user.id), test_db) == []

  def test_reports_zero_keys_when_there_are_none(self, test_db, test_user):
    user = _create_user(test_db, test_user.password_hash)

    change = set_user_active(user.id, False, test_db)

    assert change.api_keys_revoked == 0
    assert change.changed is True


class TestActivate:
  def test_restores_sign_in_but_not_keys(self, test_db, test_user):
    user = _create_user(test_db, test_user.password_hash)
    UserAPIKey.create(user_id=user.id, name="doomed", session=test_db)
    set_user_active(user.id, False, test_db)

    change = set_user_active(user.id, True, test_db)

    assert change.is_active is True
    assert change.changed is True
    assert change.api_keys_revoked == 0

    test_db.refresh(user)
    assert user.is_active is True
    # Reactivation deliberately does not resurrect revoked keys.
    assert UserAPIKey.get_active_by_user_id(str(user.id), test_db) == []

  def test_activating_an_active_user_reports_no_change(self, test_db, test_user):
    user = _create_user(test_db, test_user.password_hash)

    change = set_user_active(user.id, True, test_db)

    assert change.is_active is True
    assert change.changed is False


class TestMissingUser:
  def test_unknown_user_raises(self, test_db):
    with pytest.raises(UserNotFound):
      set_user_active(f"user_{uuid4().hex}", False, test_db)


class TestRevocationCountIsWhatActuallyHappened:
  """The reported number must be revocations, not attempts.

  It used to be counted before the attempt, so a partial revocation — the case
  an operator most needs to see, mid-incident — was indistinguishable from a
  clean one. "3 keys revoked" while one is still live is worse than no number.
  """

  def test_partial_revocation_is_reported(self, test_db, test_user, monkeypatch):
    user = _create_user(test_db, test_user.password_hash)
    UserAPIKey.create(user_id=user.id, name="key one", session=test_db)
    UserAPIKey.create(user_id=user.id, name="key two", session=test_db)
    UserAPIKey.create(user_id=user.id, name="key three", session=test_db)

    original = UserAPIKey.deactivate
    calls = {"n": 0}

    def _flaky(self, session):
      calls["n"] += 1
      if calls["n"] == 2:
        raise RuntimeError("redis down")
      return original(self, session)

    monkeypatch.setattr(UserAPIKey, "deactivate", _flaky)

    change = set_user_active(user.id, False, test_db)

    assert change.api_keys_revoked == 2
    assert change.api_keys_failed == 1
    # And the survivor is genuinely still live in the key table, which is what
    # the count is warning about.
    assert len(UserAPIKey.get_active_by_user_id(str(user.id), test_db)) == 1

  def test_clean_revocation_reports_no_failures(self, test_db, test_user):
    user = _create_user(test_db, test_user.password_hash)
    UserAPIKey.create(user_id=user.id, name="key one", session=test_db)

    change = set_user_active(user.id, False, test_db)

    assert change.api_keys_revoked == 1
    assert change.api_keys_failed == 0

  def test_activate_reports_zero(self, test_db, test_user):
    user = _create_user(test_db, test_user.password_hash)

    change = set_user_active(user.id, True, test_db)

    assert change.api_keys_revoked == 0
    assert change.api_keys_failed == 0


class TestFullyApplied:
  """The response must say whether the kill switch fully took, not just
  whether the key rows flipped — a surviving cache entry authenticates
  until its TTL."""

  def test_cache_failure_is_not_fully_applied(self, test_db, test_user, monkeypatch):
    user = _create_user(test_db, test_user.password_hash)
    monkeypatch.setattr(User, "_invalidate_auth_cache", lambda self: False)

    change = set_user_active(user.id, False, test_db)

    assert change.fully_applied is False
    # The key side was clean; the session cache wasn't.
    assert change.api_keys_failed == 0

  def test_key_cache_failure_is_not_fully_applied(
    self, test_db, test_user, monkeypatch
  ):
    user = _create_user(test_db, test_user.password_hash)
    UserAPIKey.create(user_id=user.id, name="k", session=test_db)
    monkeypatch.setattr(UserAPIKey, "invalidate_cache", lambda self: False)

    change = set_user_active(user.id, False, test_db)

    assert change.fully_applied is False
    # The row itself revoked fine — only its cache entry survived.
    assert change.api_keys_revoked == 1
    assert change.api_keys_failed == 0

  def test_clean_deactivation_is_fully_applied(self, test_db, test_user):
    user = _create_user(test_db, test_user.password_hash)
    UserAPIKey.create(user_id=user.id, name="k", session=test_db)

    change = set_user_active(user.id, False, test_db)

    assert change.fully_applied is True
