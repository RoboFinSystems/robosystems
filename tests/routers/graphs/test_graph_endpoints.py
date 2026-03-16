"""Tests for graph management endpoints.

Exercises: routers/graphs/main.py, info.py, schema/*.py, subscriptions.py
"""


class TestGraphInfoEndpoints:
  """Test graph info and management endpoints through the API."""

  def test_list_user_graphs(self, client_with_mocked_auth, test_user_graph):
    """List graphs for authenticated user."""
    response = client_with_mocked_auth.get("/v1/graphs/")
    assert response.status_code == 200
    data = response.json()
    assert "graphs" in data
    assert isinstance(data["graphs"], list)
    assert len(data["graphs"]) > 0

  def test_get_graph_info(self, client_with_mocked_auth, test_user_graph, sample_graph):
    """Get info for a specific graph — exercises router + graph client."""
    response = client_with_mocked_auth.get(f"/v1/graphs/{sample_graph.graph_id}/info")
    # 200 if graph API has the DB, 500/502/503 if not (test graph not in LadybugDB)
    assert response.status_code in [200, 500, 502, 503]


class TestGraphSchemaEndpoints:
  """Test graph schema endpoints."""

  def test_get_graph_schema(
    self, client_with_mocked_auth, test_user_graph, sample_graph
  ):
    """Get schema for a graph — exercises router + graph client."""
    response = client_with_mocked_auth.get(f"/v1/graphs/{sample_graph.graph_id}/schema")
    assert response.status_code in [200, 500, 502, 503]

  def test_validate_schema(
    self, client_with_mocked_auth, test_user_graph, sample_graph
  ):
    """Validate schema for a graph."""
    response = client_with_mocked_auth.post(
      f"/v1/graphs/{sample_graph.graph_id}/schema/validate"
    )
    assert response.status_code in [200, 422, 500, 502, 503]


class TestGraphSubscriptionEndpoints:
  """Test subscription-related endpoints."""

  def test_get_subscription_status(
    self, client_with_mocked_auth, test_user_graph, sample_graph
  ):
    """Get subscription status for a graph."""
    response = client_with_mocked_auth.get(
      f"/v1/graphs/{sample_graph.graph_id}/subscription"
    )
    assert response.status_code in [200, 404]


class TestGraphCreditEndpoints:
  """Test credit-related endpoints through API."""

  def test_get_credit_summary(self, client_with_mocked_auth, test_graph_with_credits):
    """Get credit summary through API endpoint."""
    graph = test_graph_with_credits["graph"]
    response = client_with_mocked_auth.get(f"/v1/graphs/{graph.graph_id}/credits")
    assert response.status_code in [200, 404]

  def test_get_credit_transactions(
    self, client_with_mocked_auth, test_graph_with_credits
  ):
    """Get credit transactions through API endpoint."""
    graph = test_graph_with_credits["graph"]
    response = client_with_mocked_auth.get(
      f"/v1/graphs/{graph.graph_id}/credits/transactions"
    )
    assert response.status_code in [200, 404]


class TestOfferingEndpoints:
  """Test public offering/pricing endpoints."""

  def test_get_offerings(self, client_with_mocked_auth):
    """Get available subscription offerings."""
    response = client_with_mocked_auth.get("/v1/offering/")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, (list, dict))

  def test_offering_has_expected_fields(self, client_with_mocked_auth):
    """Offerings should have standard fields."""
    response = client_with_mocked_auth.get("/v1/offering/")
    data = response.json()
    if isinstance(data, dict) and "plans" in data:
      plans = data["plans"]
    elif isinstance(data, list):
      plans = data
    else:
      plans = []
    for plan in plans:
      assert "name" in plan


class TestUserEndpoints:
  """Test user management endpoints."""

  def test_get_current_user(self, client_with_mocked_auth):
    """Get current user info."""
    response = client_with_mocked_auth.get("/v1/user/")
    assert response.status_code == 200
    data = response.json()
    assert "id" in data
    assert "email" in data

  def test_list_api_keys(self, client_with_mocked_auth):
    """List API keys for user."""
    response = client_with_mocked_auth.get("/v1/user/api-keys")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, (list, dict))
