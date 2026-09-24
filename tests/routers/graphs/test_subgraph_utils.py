"""Tests for the subgraph router's parent-graph access check."""

from unittest.mock import MagicMock, patch

import pytest

from robosystems.routers.graphs.subgraphs.utils import verify_parent_graph_access

UTILS = "robosystems.routers.graphs.subgraphs.utils"


@pytest.mark.unit
@pytest.mark.parametrize(
  ("required_role", "require_write"), [("read", False), ("admin", True)]
)
def test_write_gate_follows_required_role(required_role, require_write):
  user = MagicMock()
  with (
    patch(
      "robosystems.middleware.billing.enforcement.require_graph_access"
    ) as require_access,
    patch(f"{UTILS}.GraphUser") as graph_user,
  ):
    graph_user.user_has_access.return_value = True
    graph_user.user_has_admin_access.return_value = True
    verify_parent_graph_access("kg0123456789abcdef", user, MagicMock(), required_role)

  assert require_access.call_args.kwargs["require_write"] is require_write
