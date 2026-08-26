"""Tests for the delete-graph operation.

Covers:
- Shared-repo rejection (defense-in-depth, mirrors delete-subgraph/change-tier)
- Confirm-token validation (must equal the URL graph_id)
- Admin-role check on the graph
- Subscription preconditions (must exist, must not already be canceled)
- Happy path: subscription canceled with immediate=True, audit trail emitted
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from robosystems.models.api.graphs.operations import DeleteGraphOp
from robosystems.routers.graphs.operations import delete_graph_op


class _FakeCache:
  """In-memory idempotency cache matching the real signature."""

  async def reserve(self, *args, **kwargs):
    return True

  async def release(self, *args, **kwargs):
    return None

  def __init__(self) -> None:
    self.store: dict = {}

  async def get(
    self, user_id, graph_id, operation_name, idempotency_key, body_fingerprint
  ):
    return None

  async def put(
    self,
    user_id,
    graph_id,
    operation_name,
    idempotency_key,
    envelope,
    body_fingerprint,
    ttl_seconds=86400,
  ):
    self.store[(user_id, graph_id, operation_name, idempotency_key)] = envelope


def _user(user_id: str = "usr_test") -> MagicMock:
  u = MagicMock()
  u.id = user_id
  return u


class TestDeleteGraphSharedRepoGate:
  @pytest.mark.asyncio
  @pytest.mark.parametrize("graph_id", ["sec", "sec_historical"])
  async def test_rejects_shared_repo(self, graph_id: str) -> None:
    body = DeleteGraphOp(confirm=graph_id)
    with pytest.raises(HTTPException) as exc:
      await delete_graph_op(
        body=body,
        graph_id=graph_id,
        user=_user(),
        idempotency_key=None,
        cache=_FakeCache(),
        db=MagicMock(),
      )
    assert exc.value.status_code == 403
    assert "shared repository" in exc.value.detail
    assert graph_id in exc.value.detail


class TestDeleteGraphConfirmToken:
  @pytest.mark.asyncio
  async def test_rejects_when_confirm_does_not_match_graph_id(self) -> None:
    body = DeleteGraphOp(confirm="kg_other")
    with pytest.raises(HTTPException) as exc:
      await delete_graph_op(
        body=body,
        graph_id="kg_target",
        user=_user(),
        idempotency_key=None,
        cache=_FakeCache(),
        db=MagicMock(),
      )
    assert exc.value.status_code == 400
    assert "confirm" in exc.value.detail.lower()


class TestDeleteGraphRunner:
  """Tests for the inner runner: admin role, subscription state, happy path.

  The runner is invoked through `_dispatch -> execute_operation`. We mock
  `GraphUser`, `BillingSubscription`, and `BillingAuditLog` so the runner
  exercises real branching.
  """

  def _setup_admin_membership(self, db: MagicMock, role: str = "admin") -> MagicMock:
    """Wire the two queries `get_effective_role` runs against `db`:

    - the graph-liveness query (`.all()`) returns one live Graph row, so the
      resolver gets past the "graph is gone" guard; and
    - the membership query (`.first()`) returns a GraphUser with the given
      role (None means no membership).
    """
    live_graph = MagicMock()
    live_graph.graph_id = "kg_x"
    live_graph.org_id = "org_1"
    live_graph.status = "active"
    live_graph.deleted_at = None
    db.query.return_value.filter.return_value.all.return_value = [live_graph]

    membership = None
    if role is not None:
      membership = MagicMock()
      membership.role = role
    db.query.return_value.filter.return_value.first.return_value = membership
    return membership

  @staticmethod
  def _patch_org_owner(role: str | None = "OWNER"):
    """Context-manager helper: patches `OrgUser.get_by_org_and_user` to
    return a membership with the requested role. Pass `role=None` to
    simulate "user is not in this org"."""
    from robosystems.models.core import OrgRole

    org_role_map = {"OWNER": OrgRole.OWNER, "ADMIN": OrgRole.ADMIN}
    org_user = None
    if role is not None:
      org_user = MagicMock()
      org_user.role = org_role_map.get(role, role)
    return patch(
      "robosystems.models.core.OrgUser.get_by_org_and_user",
      return_value=org_user,
    )

  @pytest.mark.asyncio
  @patch("robosystems.models.core.billing.BillingSubscription.get_by_resource")
  async def test_rejects_non_admin(self, mock_get_sub: MagicMock) -> None:
    db = MagicMock()
    self._setup_admin_membership(db, role="member")

    body = DeleteGraphOp(confirm="kg_x")
    with pytest.raises(HTTPException) as exc:
      await delete_graph_op(
        body=body,
        graph_id="kg_x",
        user=_user(),
        idempotency_key=None,
        cache=_FakeCache(),
        db=db,
      )
    assert exc.value.status_code == 403
    assert "admin" in exc.value.detail.lower()
    mock_get_sub.assert_not_called()

  @pytest.mark.asyncio
  @patch("robosystems.models.core.billing.BillingSubscription.get_by_resource")
  async def test_rejects_when_no_subscription(self, mock_get_sub: MagicMock) -> None:
    db = MagicMock()
    self._setup_admin_membership(db, role="admin")
    mock_get_sub.return_value = None

    body = DeleteGraphOp(confirm="kg_x")
    with pytest.raises(HTTPException) as exc:
      await delete_graph_op(
        body=body,
        graph_id="kg_x",
        user=_user(),
        idempotency_key=None,
        cache=_FakeCache(),
        db=db,
      )
    assert exc.value.status_code == 404
    assert "subscription" in exc.value.detail.lower()

  @pytest.mark.asyncio
  @patch("robosystems.models.core.billing.BillingSubscription.get_by_resource")
  async def test_rejects_when_subscription_already_canceled(
    self, mock_get_sub: MagicMock
  ) -> None:
    db = MagicMock()
    self._setup_admin_membership(db, role="admin")

    sub = MagicMock()
    sub.status = "canceled"
    sub.org_id = "org_1"
    mock_get_sub.return_value = sub

    body = DeleteGraphOp(confirm="kg_x")
    with self._patch_org_owner("OWNER"):
      with pytest.raises(HTTPException) as exc:
        await delete_graph_op(
          body=body,
          graph_id="kg_x",
          user=_user(),
          idempotency_key=None,
          cache=_FakeCache(),
          db=db,
        )
    assert exc.value.status_code == 400
    assert "already canceled" in exc.value.detail.lower()
    sub.cancel.assert_not_called()

  @pytest.mark.asyncio
  @patch("robosystems.models.core.billing.BillingSubscription.get_by_resource")
  async def test_rejects_non_org_owner(self, mock_get_sub: MagicMock) -> None:
    """Graph admin alone is not enough — org owner is required since this
    triggers a billing event. Mirrors repo cancel + prior billing cancel."""
    db = MagicMock()
    self._setup_admin_membership(db, role="admin")

    sub = MagicMock()
    sub.id = "sub_abc"
    sub.status = "active"
    sub.org_id = "org_1"
    mock_get_sub.return_value = sub

    body = DeleteGraphOp(confirm="kg_x")
    with self._patch_org_owner("ADMIN"):  # not OWNER
      with pytest.raises(HTTPException) as exc:
        await delete_graph_op(
          body=body,
          graph_id="kg_x",
          user=_user(),
          idempotency_key=None,
          cache=_FakeCache(),
          db=db,
        )
    assert exc.value.status_code == 403
    assert "owner" in exc.value.detail.lower()
    sub.cancel.assert_not_called()

  @pytest.mark.asyncio
  @patch("robosystems.models.core.billing.BillingSubscription.get_by_resource")
  async def test_rejects_at_period_end_for_non_active_subscription(
    self, mock_get_sub: MagicMock
  ) -> None:
    """`at_period_end=True` on a `pending` (or paused etc) sub must be
    rejected — its `current_period_end` may be None, and the cancel()
    guard would raise ValueError. Better to fail fast with a clear
    message and route the user to immediate teardown."""
    db = MagicMock()
    self._setup_admin_membership(db, role="admin")

    sub = MagicMock()
    sub.id = "sub_abc"
    sub.status = "pending"
    sub.org_id = "org_1"
    mock_get_sub.return_value = sub

    body = DeleteGraphOp(confirm="kg_x", at_period_end=True)
    with self._patch_org_owner("OWNER"):
      with pytest.raises(HTTPException) as exc:
        await delete_graph_op(
          body=body,
          graph_id="kg_x",
          user=_user(),
          idempotency_key=None,
          cache=_FakeCache(),
          db=db,
        )
    assert exc.value.status_code == 400
    assert "period end" in exc.value.detail.lower()
    assert "pending" in exc.value.detail.lower()
    sub.cancel.assert_not_called()

  @pytest.mark.asyncio
  @patch("robosystems.security.SecurityAuditLogger")
  @patch("robosystems.models.core.billing.BillingAuditLog.log_event")
  @patch("robosystems.models.core.billing.BillingSubscription.get_by_resource")
  async def test_happy_path_immediate_default_cancels_subscription(
    self,
    mock_get_sub: MagicMock,
    mock_log_event: MagicMock,
    _mock_security: MagicMock,
  ) -> None:
    db = MagicMock()
    self._setup_admin_membership(db, role="admin")

    sub = MagicMock()
    sub.id = "sub_abc"
    sub.status = "active"
    sub.org_id = "org_1"
    sub.stripe_subscription_id = None
    mock_get_sub.return_value = sub

    body = DeleteGraphOp(confirm="kg_x")  # at_period_end defaults to False
    with self._patch_org_owner("OWNER"):
      envelope = await delete_graph_op(
        body=body,
        graph_id="kg_x",
        user=_user(user_id="usr_admin"),
        idempotency_key=None,
        cache=_FakeCache(),
        db=db,
      )

    sub.cancel.assert_called_once_with(db, immediate=True)
    assert envelope.status == "completed"
    assert envelope.operation == "delete-graph"
    assert envelope.result["graph_id"] == "kg_x"
    assert envelope.result["status"] == "deprovisioning_queued"
    mock_log_event.assert_called_once()
    audit_kwargs = mock_log_event.call_args.kwargs
    assert audit_kwargs["event_data"]["immediate"] is True
    assert audit_kwargs["event_data"]["via"] == "graph_ops.delete_graph"

  @pytest.mark.asyncio
  @patch("robosystems.security.SecurityAuditLogger")
  @patch("robosystems.models.core.billing.BillingAuditLog.log_event")
  @patch("robosystems.models.core.billing.BillingSubscription.get_by_resource")
  async def test_happy_path_at_period_end_defers_cancellation(
    self,
    mock_get_sub: MagicMock,
    mock_log_event: MagicMock,
    _mock_security: MagicMock,
  ) -> None:
    """`at_period_end=True` calls subscription.cancel(immediate=False) so the
    graph stays usable until period_end, then suspend→deprovision picks up."""
    from datetime import UTC, datetime

    db = MagicMock()
    self._setup_admin_membership(db, role="admin")

    sub = MagicMock()
    sub.id = "sub_abc"
    sub.status = "active"
    sub.org_id = "org_1"
    sub.stripe_subscription_id = None
    sub.ends_at = datetime(2026, 6, 1, tzinfo=UTC)
    mock_get_sub.return_value = sub

    body = DeleteGraphOp(confirm="kg_x", at_period_end=True)
    with self._patch_org_owner("OWNER"):
      envelope = await delete_graph_op(
        body=body,
        graph_id="kg_x",
        user=_user(user_id="usr_admin"),
        idempotency_key=None,
        cache=_FakeCache(),
        db=db,
      )

    sub.cancel.assert_called_once_with(db, immediate=False)
    assert envelope.result["status"] == "scheduled_for_deprovision"
    assert envelope.result["ends_at"] == "2026-06-01T00:00:00+00:00"
    audit_kwargs = mock_log_event.call_args.kwargs
    assert audit_kwargs["event_data"]["immediate"] is False

  @pytest.mark.asyncio
  @patch("robosystems.security.SecurityAuditLogger")
  @patch("robosystems.operations.providers.payment_provider.get_payment_provider")
  @patch("robosystems.models.core.billing.BillingAuditLog.log_event")
  @patch("robosystems.models.core.billing.BillingSubscription.get_by_resource")
  async def test_happy_path_immediate_cancels_stripe_outright(
    self,
    mock_get_sub: MagicMock,
    _mock_log_event: MagicMock,
    mock_get_provider: MagicMock,
    _mock_security: MagicMock,
  ) -> None:
    db = MagicMock()
    self._setup_admin_membership(db, role="admin")

    sub = MagicMock()
    sub.id = "sub_abc"
    sub.status = "active"
    sub.org_id = "org_1"
    sub.stripe_subscription_id = "sub_stripe_xyz"
    mock_get_sub.return_value = sub

    provider = MagicMock()
    mock_get_provider.return_value = provider

    body = DeleteGraphOp(confirm="kg_x")
    with self._patch_org_owner("OWNER"):
      await delete_graph_op(
        body=body,
        graph_id="kg_x",
        user=_user(),
        idempotency_key=None,
        cache=_FakeCache(),
        db=db,
      )

    provider.cancel_subscription.assert_called_once_with("sub_stripe_xyz")
    provider.stripe.Subscription.modify.assert_not_called()
    sub.cancel.assert_called_once_with(db, immediate=True)

  @pytest.mark.asyncio
  @patch("robosystems.security.SecurityAuditLogger")
  @patch("robosystems.operations.providers.payment_provider.get_payment_provider")
  @patch("robosystems.models.core.billing.BillingAuditLog.log_event")
  @patch("robosystems.models.core.billing.BillingSubscription.get_by_resource")
  async def test_happy_path_at_period_end_uses_stripe_modify(
    self,
    mock_get_sub: MagicMock,
    _mock_log_event: MagicMock,
    mock_get_provider: MagicMock,
    _mock_security: MagicMock,
  ) -> None:
    """`at_period_end=True` on a Stripe-linked sub uses Stripe.Subscription.modify
    with cancel_at_period_end=True (NOT the immediate Subscription.cancel path)."""
    from datetime import UTC, datetime

    db = MagicMock()
    self._setup_admin_membership(db, role="admin")

    sub = MagicMock()
    sub.id = "sub_abc"
    sub.status = "active"
    sub.org_id = "org_1"
    sub.stripe_subscription_id = "sub_stripe_xyz"
    sub.ends_at = datetime(2026, 6, 1, tzinfo=UTC)
    mock_get_sub.return_value = sub

    provider = MagicMock()
    mock_get_provider.return_value = provider

    body = DeleteGraphOp(confirm="kg_x", at_period_end=True)
    with self._patch_org_owner("OWNER"):
      await delete_graph_op(
        body=body,
        graph_id="kg_x",
        user=_user(),
        idempotency_key=None,
        cache=_FakeCache(),
        db=db,
      )

    provider.stripe.Subscription.modify.assert_called_once_with(
      "sub_stripe_xyz", cancel_at_period_end=True
    )
    provider.cancel_subscription.assert_not_called()
    sub.cancel.assert_called_once_with(db, immediate=False)

  @pytest.mark.asyncio
  @patch("robosystems.security.SecurityAuditLogger")
  @patch("robosystems.operations.providers.payment_provider.get_payment_provider")
  @patch("robosystems.models.core.billing.BillingAuditLog.log_event")
  @patch("robosystems.models.core.billing.BillingSubscription.get_by_resource")
  async def test_stripe_cancel_failure_leaves_local_state_unchanged(
    self,
    mock_get_sub: MagicMock,
    mock_log_event: MagicMock,
    mock_get_provider: MagicMock,
    _mock_security: MagicMock,
  ) -> None:
    """Provider first, fail closed. A Stripe cancel that raises means the
    customer would keep paying for a graph that no longer exists if the local
    cancel proceeded — so nothing local moves, and the caller sees a 502 to
    retry. Same contract as the repository cancel path."""
    db = MagicMock()
    self._setup_admin_membership(db, role="admin")

    sub = MagicMock()
    sub.id = "sub_abc"
    sub.status = "active"
    sub.org_id = "org_1"
    sub.stripe_subscription_id = "sub_stripe_xyz"
    mock_get_sub.return_value = sub

    provider = MagicMock()
    provider.cancel_subscription.side_effect = Exception("stripe is down")
    mock_get_provider.return_value = provider

    body = DeleteGraphOp(confirm="kg_x")
    with self._patch_org_owner("OWNER"):
      with pytest.raises(HTTPException) as exc:
        await delete_graph_op(
          body=body,
          graph_id="kg_x",
          user=_user(),
          idempotency_key=None,
          cache=_FakeCache(),
          db=db,
        )

    assert exc.value.status_code == 502
    sub.cancel.assert_not_called()
    mock_log_event.assert_not_called()
