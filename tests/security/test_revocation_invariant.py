"""The revocation invariant: after any revocation, nothing still authorizes.

Every grant in the platform is fronted by an auth cache with a TTL of minutes,
so a revocation is only real once both halves are gone — the grant row in the
database and every cache entry that was derived from it. Three findings
(spec: security/org-multi-user-hardening.md, the "revocation cluster") were
each an instance of one half being withdrawn while the other kept authorizing.

Each test below seeds the cache in the exact shape the production write paths
produce, revokes through one of the platform's revocation surfaces, and then
asserts the same thing: no grant row and no cache entry still authorizes the
revoked user. The revocation surfaces are the ones the spec enumerates:

- graph membership removal (router)
- org membership removal, including the F15 grant a period-end cancel leaves
  alive (router)
- immediate repository-subscription cancellation (F8, operation)
- administrative account deletion (F7, operation)
"""

from __future__ import annotations

import fnmatch
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from robosystems.middleware.auth.cache import APIKeyCache
from robosystems.models.core import (
  BillingSubscription,
  Graph,
  GraphRole,
  GraphUser,
  Org,
  OrgRole,
  OrgType,
  OrgUser,
  User,
  UserAPIKey,
)
from robosystems.models.core.user.user_repository import (
  RepositoryAccessLevel,
  RepositoryType,
  UserRepository,
)
from robosystems.operations.admin import execute_user_deletion
from robosystems.operations.billing import (
  cancel_repository_subscription,
  cancel_user_repository_subscriptions,
)

PROVIDER = "robosystems.operations.providers.payment_provider.get_payment_provider"

pytestmark = pytest.mark.asyncio


class FakeRedis:
  """The subset of the Redis API the auth cache uses, over a dict."""

  def __init__(self) -> None:
    self.store: dict[str, str] = {}

  def ping(self) -> bool:
    return True

  def keys(self, pattern: str) -> list[str]:
    return [k for k in self.store if fnmatch.fnmatchcase(k, pattern)]

  def get(self, key: str) -> str | None:
    return self.store.get(key)

  def setex(self, key: str, _ttl: int, value: str) -> None:
    self.store[key] = value

  def delete(self, *keys: str) -> int:
    removed = 0
    for key in keys:
      if key in self.store:
        del self.store[key]
        removed += 1
    return removed

  def pipeline(self) -> FakePipeline:
    return FakePipeline(self)


class FakePipeline:
  """Buffers commands and returns their results from ``execute``."""

  def __init__(self, redis: FakeRedis) -> None:
    self._redis = redis
    self._results: list[object] = []

  def get(self, key: str) -> None:
    self._results.append(self._redis.get(key))

  def setex(self, key: str, ttl: int, value: str) -> None:
    self._results.append(self._redis.setex(key, ttl, value))

  def execute(self) -> list[object]:
    results, self._results = self._results, []
    return results


@pytest.fixture
def cache():
  """A real APIKeyCache over a fake Redis, installed everywhere the code
  under test resolves the cache singleton — the operations and models go
  through ``importlib`` to the module attribute, the routers bind it at
  import time.
  """
  with patch("robosystems.middleware.auth.cache.redis.Redis"):
    instance = APIKeyCache()
  instance._redis = FakeRedis()
  with (
    patch("robosystems.middleware.auth.cache.api_key_cache", instance),
    patch("robosystems.routers.graphs.members.api_key_cache", instance),
    patch("robosystems.routers.orgs.members.api_key_cache", instance),
  ):
    yield instance


def _create_user(session, password_hash: str) -> User:
  user = User(
    email=f"revoked+{uuid4().hex[:8]}@example.com",
    name="Soon Revoked",
    password_hash=password_hash,
  )
  session.add(user)
  session.commit()
  session.refresh(user)
  return user


def _ensure_sec(session) -> None:
  if Graph.get_by_id("sec", session) is None:
    Graph.create(
      graph_id="sec",
      org_id=None,
      graph_name="SEC",
      graph_type="repository",
      session=session,
    )


def _seed_authorizing_entries(
  cache: APIKeyCache, user: User, fingerprint: str, graph_ids: list[str]
) -> None:
  """Write every kind of entry that would let this user through, using the
  production write paths so the shapes are exact."""
  user_data = {
    "id": user.id,
    "email": user.email,
    "name": user.name,
    "is_active": True,
    "session_version": 0,
  }
  cache.cache_api_key_validation(fingerprint, user_data, is_active=True)
  cache.cache_jwt_user_data(user.id, user_data, session_version=0)
  for graph_id in graph_ids:
    cache.cache_graph_access(fingerprint, graph_id, has_access=True)
    cache.cache_jwt_graph_access(user.id, graph_id, has_access=True)


def _still_authorizing(cache: APIKeyCache, user: User, fingerprint: str) -> list[str]:
  """Cache keys whose presence would still admit the user on some path."""
  store = cache._redis.store  # type: ignore[union-attr]
  return sorted(
    key
    for key in store
    if key == f"{cache.CACHE_KEY_PREFIX}{fingerprint}"
    or key.startswith(f"{cache.GRAPH_CACHE_KEY_PREFIX}{fingerprint}:")
    or key == f"{cache.USER_DATA_PREFIX}{user.id}"
    or key.startswith(f"{cache.JWT_GRAPH_CACHE_KEY_PREFIX}{user.id}:")
  )


class TestSeedIsRealistic:
  """If the seed did not authorize, the invariant tests would pass vacuously."""

  def test_seeded_entries_read_back_as_grants(self, cache, test_db, test_user):
    user = _create_user(test_db, test_user.password_hash)
    _seed_authorizing_entries(cache, user, "fp_seed", ["kg1", "sec"])

    assert cache.get_cached_api_key_validation("fp_seed")["user_data"]["id"] == user.id
    assert cache.get_cached_graph_access("fp_seed", "kg1") is True
    assert cache.get_cached_jwt_graph_access(user.id, "sec") is True
    assert cache.get_cached_jwt_user_data(user.id, 0)["user_data"]["id"] == user.id
    assert len(_still_authorizing(cache, user, "fp_seed")) == 6


class TestGraphMembershipRevocation:
  async def test_removing_a_graph_member_leaves_nothing_authorizing(
    self, cache, async_client, test_db, test_user
  ):
    org = Org.create(
      name=f"Org {uuid4().hex[:6]}", org_type=OrgType.TEAM, session=test_db
    )
    OrgUser.create(
      org_id=org.id, user_id=test_user.id, role=OrgRole.OWNER, session=test_db
    )
    graph = Graph.create(
      graph_id=f"kg{uuid4().hex[:16]}",
      org_id=org.id,
      graph_name="Team Graph",
      graph_type="generic",
      session=test_db,
    )
    member = _create_user(test_db, test_user.password_hash)
    OrgUser.create(
      org_id=org.id, user_id=member.id, role=OrgRole.MEMBER, session=test_db
    )
    GraphUser.create(
      user_id=member.id, graph_id=graph.graph_id, role=GraphRole.MEMBER, session=test_db
    )
    _seed_authorizing_entries(cache, member, "fp_graph", [graph.graph_id])

    response = await async_client.delete(
      f"/v1/graphs/{graph.graph_id}/members/{member.id}"
    )

    assert response.status_code == 204
    assert not GraphUser.user_has_access(member.id, graph.graph_id, test_db)
    assert _still_authorizing(cache, member, "fp_graph") == []


class TestOrgMembershipRevocation:
  async def test_removing_an_org_member_revokes_period_end_repository_grant(
    self, cache, async_client, test_db, test_user
  ):
    """F15: a period-end cancel leaves the grant alive to ``expires_at`` while
    the billing row is already terminal. Off-boarding must revoke it anyway —
    the grant, not the billing row, is what authorization reads."""
    _ensure_sec(test_db)
    org = Org.create(
      name=f"Org {uuid4().hex[:6]}", org_type=OrgType.TEAM, session=test_db
    )
    OrgUser.create(
      org_id=org.id, user_id=test_user.id, role=OrgRole.OWNER, session=test_db
    )
    graph = Graph.create(
      graph_id=f"kg{uuid4().hex[:16]}",
      org_id=org.id,
      graph_name="Team Graph",
      graph_type="generic",
      session=test_db,
    )
    member = _create_user(test_db, test_user.password_hash)
    OrgUser.create(
      org_id=org.id, user_id=member.id, role=OrgRole.MEMBER, session=test_db
    )
    GraphUser.create(
      user_id=member.id, graph_id=graph.graph_id, role=GraphRole.MEMBER, session=test_db
    )

    subscription = BillingSubscription.create_subscription(
      org_id=org.id,
      resource_type="repository",
      resource_id="sec",
      plan_name="sec-starter",
      base_price_cents=4900,
      session=test_db,
      user_id=member.id,
    )
    subscription.activate(test_db)
    subscription.stripe_subscription_id = "sub_period_end"
    test_db.commit()
    grant = UserRepository.create_access(
      user_id=member.id,
      repository_type=RepositoryType.SEC,
      repository_name="sec",
      access_level=RepositoryAccessLevel.READ,
      repository_plan="sec-starter",
      session=test_db,
    )
    with patch(PROVIDER, return_value=MagicMock()):
      cancel_repository_subscription(
        subscription, session=test_db, actor_user_id=member.id
      )
    test_db.refresh(grant)
    assert grant.is_active is True, "precondition: period-end cancel keeps the grant"
    assert (
      BillingSubscription.get_live_subscriptions_for_user(
        member.id, test_db, resource_type="repository"
      )
      == []
    ), "precondition: the billing row is already terminal"

    _seed_authorizing_entries(cache, member, "fp_org", [graph.graph_id, "sec"])

    with patch(PROVIDER, return_value=MagicMock()):
      response = await async_client.delete(f"/v1/orgs/{org.id}/members/{member.id}")

    assert response.status_code == 204
    test_db.refresh(grant)
    assert grant.is_active is False
    assert not UserRepository.user_has_access(member.id, "sec", test_db)
    assert not GraphUser.user_has_access(member.id, graph.graph_id, test_db)
    assert _still_authorizing(cache, member, "fp_org") == []


class TestRepositorySubscriptionRevocation:
  def test_immediate_cancel_leaves_nothing_authorizing(
    self, cache, test_db, test_user, test_org
  ):
    """F8: the endpoint promises ``immediate=true`` stops access immediately;
    that is only true once the cached decisions go with the grant."""
    _ensure_sec(test_db)
    member = _create_user(test_db, test_user.password_hash)
    OrgUser.create(
      org_id=test_org.id, user_id=member.id, role=OrgRole.MEMBER, session=test_db
    )
    subscription = BillingSubscription.create_subscription(
      org_id=test_org.id,
      resource_type="repository",
      resource_id="sec",
      plan_name="sec-starter",
      base_price_cents=4900,
      session=test_db,
      user_id=member.id,
    )
    subscription.activate(test_db)
    grant = UserRepository.create_access(
      user_id=member.id,
      repository_type=RepositoryType.SEC,
      repository_name="sec",
      access_level=RepositoryAccessLevel.READ,
      repository_plan="sec-starter",
      session=test_db,
    )
    _seed_authorizing_entries(cache, member, "fp_repo", ["sec", "sec_historical"])

    with patch(PROVIDER, return_value=MagicMock()):
      cancel_repository_subscription(
        subscription, session=test_db, actor_user_id=member.id, immediate=True
      )

    test_db.refresh(grant)
    assert grant.is_active is False
    assert _still_authorizing(cache, member, "fp_repo") == []

  def test_offboarding_sweep_revokes_grants_without_live_subscriptions(
    self, cache, test_db, test_user, test_org
  ):
    """F15 at the operation level: a grant with no billing row behind it at
    all (a comped or legacy grant) is still revoked by the off-boarding sweep."""
    _ensure_sec(test_db)
    member = _create_user(test_db, test_user.password_hash)
    grant = UserRepository.create_access(
      user_id=member.id,
      repository_type=RepositoryType.SEC,
      repository_name="sec",
      access_level=RepositoryAccessLevel.READ,
      repository_plan="sec-starter",
      session=test_db,
    )
    _seed_authorizing_entries(cache, member, "fp_comped", ["sec"])

    canceled = cancel_user_repository_subscriptions(
      member.id, session=test_db, actor_user_id=test_user.id
    )

    assert canceled == []
    test_db.refresh(grant)
    assert grant.is_active is False
    assert _still_authorizing(cache, member, "fp_comped") == []


class TestAccountDeletionRevocation:
  def test_deleting_an_account_leaves_nothing_authenticating(
    self, cache, test_db, test_user
  ):
    """F7: the bulk delete bypasses ``UserAPIKey.delete``, so without an
    explicit sweep a deleted account keeps authenticating from cache."""
    user = _create_user(test_db, test_user.password_hash)
    org = Org.create(
      name=f"Personal {uuid4().hex[:6]}", org_type=OrgType.PERSONAL, session=test_db
    )
    OrgUser.create(org_id=org.id, user_id=user.id, role=OrgRole.OWNER, session=test_db)
    key_row, _plain = UserAPIKey.create(user_id=user.id, name="ci", session=test_db)
    fingerprint = key_row.key_fingerprint
    assert fingerprint
    _seed_authorizing_entries(cache, user, fingerprint, ["kg1", "sec"])

    execute_user_deletion(user.id, test_db)

    assert User.get_by_id(user.id, test_db) is None
    assert test_db.query(UserAPIKey).filter_by(user_id=user.id).count() == 0
    assert _still_authorizing(cache, user, fingerprint) == []
