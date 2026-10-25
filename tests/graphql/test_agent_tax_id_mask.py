"""Counterparty tax IDs reach the MCP surface as the last four only, whatever
the client calls the field."""

import pytest
import strawberry

from robosystems.graphql.types.ledger import Agent
from robosystems.models.api.extensions.agent import LedgerAgentResponse

ROW = LedgerAgentResponse(
  id="agt_dummy",
  agent_type="vendor",
  name="Dummy Vendor",
  source="app",
  is_active=True,
  is_1099_recipient=False,
  tax_id="123-45-6789",
)


@strawberry.type
class Query:
  @strawberry.field
  def agent(self) -> Agent:
    return Agent.from_pydantic(ROW)


schema = strawberry.Schema(query=Query)


@pytest.mark.unit
def test_mcp_surface_is_masked_through_aliases_and_fragments():
  result = schema.execute_sync(
    "{ agent { id ssn: taxId ...F } } fragment F on Agent { t: taxId }",
    context_value={"surface": "mcp"},
  )
  assert result.errors is None
  assert result.data["agent"] == {"id": "agt_dummy", "ssn": "***6789", "t": "***6789"}


@pytest.mark.unit
def test_other_surfaces_read_the_stored_value():
  result = schema.execute_sync("{ agent { taxId } }", context_value={})
  assert result.errors is None
  assert result.data["agent"]["taxId"] == "123-45-6789"
