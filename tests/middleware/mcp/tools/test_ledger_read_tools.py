"""The typed ledger reads: each GraphQL field a tool text names, as an MCP tool
of its own. Each test runs the field and the tool over the same operations
fixture and checks the tool answers what the field answers."""

from __future__ import annotations

import re
from contextlib import contextmanager
from datetime import UTC, date, datetime
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from graphql import get_named_type, is_object_type

from robosystems.graphql.schema import schema
from robosystems.middleware.mcp.tools.annotations import tool_annotations
from robosystems.middleware.mcp.tools.ledger_read_tools import (
  GetAccountRegisterTool,
  GetClassificationStatsTool,
  ListBankAccountsTool,
  ListDischargeCandidatesTool,
  ListFlowCandidatesTool,
)
from robosystems.middleware.mcp.tools.report_tools import ListReportsTool
from robosystems.middleware.mcp.tools.taxonomy_tools import ListElementArcsTool
from robosystems.models.api.extensions.account_register import (
  AccountRegisterResponse,
  RegisterAccount,
  RegisterLine,
)
from robosystems.models.api.extensions.agent import (
  ClassificationSourceStats,
  ClassificationStatsResponse,
)
from robosystems.models.api.extensions.ar_ap import DischargeCandidates, OpenItem
from robosystems.models.api.extensions.bank_accounts import (
  BankAccountListResponse,
  BankAccountResponse,
)
from robosystems.models.api.extensions.dimensions import LineDimension
from robosystems.models.api.extensions.reports import (
  PeriodSpec,
  ReportListResponse,
  ReportResponse,
  StructureSummary,
)
from robosystems.models.api.extensions.taxonomies import FlowCandidate
from robosystems.models.api.library import (
  LibraryElementArcResponse,
  LibraryElementResponse,
)
from robosystems.operations.roboledger.entity_scope import EntityNotInGraphError

GRAPH_ID = "kg01234567890abcdef"
_READS = "robosystems.operations.roboledger.reads"
_TOOLS = "robosystems.middleware.mcp.tools"


@contextmanager
def _sessions():
  """One mock session behind both surfaces' `extensions_session`."""
  session = MagicMock()
  cm = MagicMock()
  cm.__enter__ = MagicMock(return_value=session)
  cm.__exit__ = MagicMock(return_value=False)
  with (
    patch("robosystems.db.extensions.extensions_session", return_value=cm),
    patch(f"{_TOOLS}.ledger_read_tools.extensions_session", return_value=cm),
    patch(f"{_TOOLS}.report_tools.extensions_session", return_value=cm),
    patch(f"{_TOOLS}.taxonomy_tools.extensions_session", return_value=cm),
  ):
    yield session


def _graphql(query: str) -> Any:
  user = MagicMock()
  user.id = "usr_test"
  result = schema.execute_sync(
    query,
    context_value={
      "request": MagicMock(),
      "user": user,
      "graph_id": GRAPH_ID,
      "schema_extensions": ("roboledger",),
      "graph_type": "entity",
    },
  )
  assert result.errors is None, result.errors
  assert result.data is not None
  return result.data


def _selection(type_name: str, skip: frozenset[str] = frozenset(), depth: int = 0):
  """Every field of a GraphQL type, nested object types included."""
  gql_type = schema._schema.type_map[type_name]
  parts = []
  for name, field in gql_type.fields.items():
    if name in skip:
      continue
    inner = get_named_type(field.type)
    if is_object_type(inner):
      if depth < 3:
        parts.append(f"{name} {{ {_selection(inner.name, skip, depth + 1)} }}")
    else:
      parts.append(name)
  return " ".join(parts)


def _snake(value: Any) -> Any:
  if isinstance(value, dict):
    return {
      re.sub(r"(?<!^)(?=[A-Z])", "_", key).lower(): _snake(item)
      for key, item in value.items()
    }
  if isinstance(value, list):
    return [_snake(item) for item in value]
  return value


def _same_instant(left: Any, right: Any) -> bool:
  try:
    return datetime.fromisoformat(left) == datetime.fromisoformat(right)
  except TypeError, ValueError:
    return False


def _assert_answers(field: Any, tool: Any, path: str = "") -> None:
  """Everything the GraphQL field answered, the tool answered alike."""
  if isinstance(field, dict):
    assert isinstance(tool, dict), path
    for key, value in field.items():
      assert key in tool, f"{path}.{key} missing from the tool's answer"
      _assert_answers(value, tool[key], f"{path}.{key}")
  elif isinstance(field, list):
    assert isinstance(tool, list), path
    assert len(field) == len(tool), path
    for index, (left, right) in enumerate(zip(field, tool, strict=True)):
      _assert_answers(left, right, f"{path}[{index}]")
  elif field != tool:
    assert isinstance(field, str) and _same_instant(field, tool), (
      f"{path}: GraphQL {field!r} != tool {tool!r}"
    )


def _client() -> MagicMock:
  client = MagicMock()
  client.graph_id = GRAPH_ID
  return client


def _line(line_id: str, day: int, debit: float, balance: float) -> RegisterLine:
  return RegisterLine(
    line_id=line_id,
    entry_id=f"je_{line_id}",
    entry_number=f"JE-{day}",
    posting_date=date(2026, 9, day),
    entry_type="standard",
    provenance="native",
    status="posted",
    memo="Rent",
    description="September rent",
    debit=debit,
    credit=0.0,
    balance=balance,
    triggered_by_event_id=f"evt_{line_id}",
    triggered_by_event_type="bill_received",
    agent_id="agt_landlord",
    agent_name="Harbor Properties",
    dimensions=[LineDimension(id="dim_ops", dimension_type="department", name="Ops")],
  )


_REGISTER = AccountRegisterResponse(
  account=RegisterAccount(
    id="el_rent",
    code="6100",
    name="Rent",
    balance_type="debit",
    trait="expense",
    temporary=True,
  ),
  entity_id="ent_parent",
  start_date=date(2026, 9, 1),
  end_date=date(2026, 9, 30),
  opening_from=date(2026, 1, 1),
  opening_balance=16000.0,
  total_debits=2000.0,
  total_credits=0.0,
  closing_balance=18000.0,
  line_count=1,
  page_opening_balance=16000.0,
  lines=[_line("li_1", 1, 2000.0, 18000.0)],
  next_cursor="cur_2",
)


@pytest.mark.unit
class TestGetAccountRegister:
  def test_answers_what_the_field_answers(self):
    with (
      _sessions() as session,
      patch(
        f"{_READS}.account_register.get_account_register", return_value=_REGISTER
      ) as read,
    ):
      field = _graphql(
        '{ accountRegister(accountId: "el_rent", startDate: "2026-09-01", '
        'endDate: "2026-09-30", entityId: "ent_parent") '
        f"{{ {_selection('AccountRegister')} }} }}"
      )["accountRegister"]
      from_field = read.call_args
      tool = GetAccountRegisterTool(_client())._execute_sync(
        {
          "account_id": "el_rent",
          "start_date": "2026-09-01",
          "end_date": "2026-09-30",
          "entity_id": "ent_parent",
        }
      )
      from_tool = read.call_args

    _assert_answers(_snake(field), tool)
    assert from_tool.args == (session, "el_rent")
    assert from_tool.kwargs == from_field.kwargs
    assert from_tool.kwargs["start_date"] == date(2026, 9, 1)
    assert from_tool.kwargs["entity_id"] == "ent_parent"

  def test_a_member_slice_reads_the_members_lines(self):
    with (
      _sessions(),
      patch(
        f"{_READS}.account_register.get_account_register", return_value=_REGISTER
      ) as read,
    ):
      GetAccountRegisterTool(_client())._execute_sync(
        {
          "account_id": "el_rent",
          "dimension_type": "department",
          "dimension_member_id": "unallocated",
        }
      )
    line_slice = read.call_args.kwargs["line_slice"]
    assert line_slice.dimension_type == "department"
    assert line_slice.member_id is None

  @pytest.mark.parametrize(
    ("arguments", "message"),
    [
      ({}, "account_id is required"),
      ({"account_id": "el_rent", "start_date": "Sept"}, "start_date"),
      ({"account_id": "el_rent", "dimension_type": "department"}, "go together"),
    ],
  )
  def test_bad_arguments_are_named(self, arguments, message):
    result = GetAccountRegisterTool(_client())._execute_sync(arguments)
    assert result["error"] == "invalid_arguments"
    assert message in result["message"]

  def test_an_unknown_account_is_not_found(self):
    from robosystems.operations.roboledger.reads.account_register import (
      AccountNotFoundError,
    )

    with (
      _sessions(),
      patch(
        f"{_READS}.account_register.get_account_register",
        side_effect=AccountNotFoundError("Account 'x' not found in this graph."),
      ),
    ):
      result = GetAccountRegisterTool(_client())._execute_sync({"account_id": "x"})
    assert result["error"] == "not_found"


_FLOWS = [
  FlowCandidate(
    element_id="el_ppe",
    qname="rs-gaap:PaymentsToAcquirePropertyPlantAndEquipment",
    name="Purchases of property and equipment",
    activity="investing",
  ),
  FlowCandidate(
    element_id="el_debt",
    qname="rs-gaap:ProceedsFromIssuanceOfLongTermDebt",
    name="Proceeds from long-term debt",
    activity="financing",
  ),
]


@pytest.mark.unit
class TestListFlowCandidates:
  def test_answers_what_the_field_answers(self):
    with (
      _sessions() as session,
      patch(
        f"{_READS}.taxonomies.list_entity_flow_candidates", return_value=_FLOWS
      ) as read,
    ):
      field = _graphql(
        f'{{ flowCandidates(entityId: "ent_sub") {{ {_selection("FlowCandidate")} }} }}'
      )["flowCandidates"]
      tool = ListFlowCandidatesTool(_client())._execute_sync({"entity_id": "ent_sub"})

    _assert_answers(_snake(field), tool["flows"])
    assert read.call_args_list[0].args == read.call_args_list[1].args
    assert read.call_args.args == (session, "ent_sub")

  def test_an_entity_outside_the_graph_is_named(self):
    with (
      _sessions(),
      patch(
        f"{_READS}.taxonomies.list_entity_flow_candidates",
        side_effect=EntityNotInGraphError("Entity 'x' not found in this graph."),
      ),
    ):
      result = ListFlowCandidatesTool(_client())._execute_sync({"entity_id": "x"})
    assert result["error"] == "entity_not_found"


_STATS = ClassificationStatsResponse(
  period="2026-09",
  entity_id="ent_parent",
  lines=10,
  accepted=7,
  overridden=2,
  unsuggested=1,
  split=0,
  unstamped=3,
  human_choice_share=0.3,
  open_lines=4,
  by_source=[
    ClassificationSourceStats(
      source="agent_default",
      lines=8,
      accepted=7,
      overridden=1,
      unsuggested=0,
      split=0,
    ),
    ClassificationSourceStats(
      source="none", lines=2, accepted=0, overridden=1, unsuggested=1, split=0
    ),
  ],
)


@pytest.mark.unit
class TestGetClassificationStats:
  def test_answers_what_the_field_answers(self):
    with (
      _sessions() as session,
      patch(
        f"{_READS}.classification.classification_stats", return_value=_STATS
      ) as read,
    ):
      field = _graphql(
        '{ classificationStats(period: "2026-09") '
        f"{{ {_selection('ClassificationStats')} }} }}"
      )["classificationStats"]
      tool = GetClassificationStatsTool(_client())._execute_sync({"period": "2026-09"})

    _assert_answers(_snake(field), tool)
    assert read.call_args_list[0].args == read.call_args_list[1].args
    assert read.call_args.args == (session, "2026-09", None)

  @pytest.mark.parametrize("period", [None, "", "2026-9", "September"])
  def test_a_bad_period_is_refused_before_the_read(self, period):
    with patch(f"{_READS}.classification.classification_stats") as read:
      result = GetClassificationStatsTool(_client())._execute_sync({"period": period})
    assert result["error"] == "invalid_arguments"
    read.assert_not_called()


_DISCHARGE = DischargeCandidates(
  event_id="evt_payment",
  kind="payables",
  amount_cents=250000,
  classified_to_control=True,
  candidates=[
    OpenItem(
      event_id="evt_bill",
      entity_id="ent_parent",
      agent_id="agt_landlord",
      occurred_on=date(2026, 8, 1),
      document_number="INV-8",
      description="August rent",
      original_cents=250000,
      settled_cents=0,
      open_cents=250000,
      due_date=date(2026, 8, 31),
      due_date_basis="stated",
      days_past_due=15,
      bucket="1-30",
    )
  ],
)


@pytest.mark.unit
class TestListDischargeCandidates:
  def test_answers_what_the_field_answers(self):
    with (
      _sessions() as session,
      patch(
        f"{_READS}.ar_ap.list_discharge_candidates", return_value=_DISCHARGE
      ) as read,
    ):
      field = _graphql(
        '{ dischargeCandidates(eventId: "evt_payment") '
        f"{{ {_selection('DischargeCandidates')} }} }}"
      )["dischargeCandidates"]
      tool = ListDischargeCandidatesTool(_client())._execute_sync(
        {"event_id": "evt_payment"}
      )

    _assert_answers(_snake(field), tool)
    assert read.call_args.args == (session, "evt_payment")

  def test_an_unknown_event_is_not_found(self):
    with (
      _sessions(),
      patch(f"{_READS}.ar_ap.list_discharge_candidates", return_value=None),
    ):
      result = ListDischargeCandidatesTool(_client())._execute_sync(
        {"event_id": "evt_missing"}
      )
    assert result["error"] == "not_found"


_BANK_ACCOUNTS = BankAccountListResponse(
  accounts=[
    BankAccountResponse(
      id="el_checking",
      code="1010",
      name="Operating checking",
      kind="bank",
      balance_type="debit",
      is_active=True,
      entity_id="ent_parent",
      entity_name="Harbor Holdings",
      source="mercury",
      connection_id="conn_1",
      institution="Mercury",
      feed_account_id="acct_9",
      feed_account_name="Checking ••4421",
      feed_account_kind="checking",
      connection_status="active",
      last_sync_at=datetime(2026, 11, 15, 8, 30, tzinfo=UTC),
      last_sync_status="success",
    )
  ],
  total=1,
)


@pytest.mark.unit
class TestListBankAccounts:
  def test_answers_what_the_field_answers(self):
    health = {"conn_1": MagicMock()}
    with (
      _sessions() as session,
      patch(
        f"{_READS}.bank_accounts.load_connection_health", return_value=health
      ) as load,
      patch(
        f"{_READS}.bank_accounts.list_bank_accounts", return_value=_BANK_ACCOUNTS
      ) as read,
    ):
      field = _graphql(f"{{ bankAccounts {{ {_selection('BankAccountList')} }} }}")[
        "bankAccounts"
      ]
      tool = ListBankAccountsTool(_client())._execute_sync({})

    _assert_answers(_snake(field), tool)
    assert [c.args for c in load.call_args_list] == [(GRAPH_ID,), (GRAPH_ID,)]
    assert read.call_args.args == (session,)
    assert read.call_args.kwargs == {"entity_id": None, "connections": health}


_REPORTS = ReportListResponse(
  reports=[
    ReportResponse(
      id="rpt_q3",
      name="Q3 2026",
      taxonomy_id="tax_rs_gaap",
      generation_status="published",
      period_type="quarterly",
      period_start=date(2026, 7, 1),
      period_end=date(2026, 9, 30),
      comparative=True,
      periods=[
        PeriodSpec(start=date(2026, 7, 1), end=date(2026, 9, 30), label="Q3 2026")
      ],
      mapping_id="map_1",
      created_at=datetime(2026, 10, 5, 12, 0, tzinfo=UTC),
      last_generated=datetime(2026, 10, 6, 9, 15, tzinfo=UTC),
      structures=[
        StructureSummary(id="str_bs", name="Balance Sheet", block_type="statement")
      ],
      entity_id="ent_parent",
      entity_name="Harbor Holdings",
      filing_status="draft",
    )
  ]
)


@pytest.mark.unit
class TestListReports:
  def test_answers_what_the_field_answers_but_the_structures(self):
    with (
      _sessions() as session,
      patch(f"{_READS}.reports.list_reports", return_value=_REPORTS) as read,
    ):
      field = _graphql(
        "{ reports(lifecycle: ALL) "
        f"{{ {_selection('ReportList', skip=frozenset({'structures'}))} }} }}"
      )["reports"]
      tool = ListReportsTool(_client())._execute_sync({"lifecycle": "all"})

    _assert_answers(_snake(field), tool)
    assert "structures" not in tool["reports"][0]
    assert read.call_args_list[0].kwargs == read.call_args_list[1].kwargs
    assert read.call_args.args == (session,)

  def test_an_unknown_lifecycle_is_refused(self):
    result = ListReportsTool(_client())._execute_sync({"lifecycle": "filed"})
    assert result["error"] == "invalid_input"


_ARCS = [
  LibraryElementArcResponse(
    id="asc_rent",
    direction="outgoing",
    association_type="mapping",
    arcrole="coa-mapping",
    taxonomy_id="tax_coa",
    taxonomy_standard="coa",
    taxonomy_name="Chart of accounts",
    structure_id="map_1",
    structure_name="CoA mapping",
    peer=LibraryElementResponse(
      id="el_rsgaap_rent",
      qname="rs-gaap:OperatingLeaseExpense",
      name="Operating lease expense",
      trait="expense",
      balance_type="debit",
      period_type="duration",
      element_type="concept",
      source="rs-gaap",
    ),
  )
]


@pytest.mark.unit
class TestListElementArcs:
  def test_answers_what_the_field_answers(self):
    with (
      _sessions() as session,
      patch(
        "robosystems.graphql.resolvers.library.get_element_arcs", return_value=_ARCS
      ) as field_read,
      patch(
        "robosystems.operations.library.reads.get_element_arcs", return_value=_ARCS
      ) as tool_read,
    ):
      field = _graphql(
        '{ libraryElementArcs(id: "el_rent") '
        f"{{ {_selection('LibraryElementArc')} }} }}"
      )["libraryElementArcs"]
      tool = ListElementArcsTool(_client())._execute_sync({"element_id": "el_rent"})

    _assert_answers(_snake(field), tool["arcs"])
    assert field_read.call_args.kwargs == tool_read.call_args.kwargs
    assert tool_read.call_args.args == (session,)


_TOOLS_UNDER_TEST = (
  GetAccountRegisterTool,
  ListFlowCandidatesTool,
  GetClassificationStatsTool,
  ListDischargeCandidatesTool,
  ListBankAccountsTool,
  ListReportsTool,
  ListElementArcsTool,
)


@pytest.mark.unit
@pytest.mark.parametrize("tool_class", _TOOLS_UNDER_TEST)
def test_each_is_an_explicit_read(tool_class):
  definition = tool_class(_client()).get_tool_definition()
  assert definition["inputSchema"]["type"] == "object"
  assert "graphql" not in definition["description"].lower()
  assert tool_annotations(definition["name"], "T") == {
    "title": "T",
    "readOnlyHint": True,
    "destructiveHint": False,
    "idempotentHint": True,
    "openWorldHint": False,
  }


@pytest.mark.unit
class TestFaultsAndBounds:
  @pytest.mark.parametrize("limit", [0, -5, 1001, "many", True, 2.5, 2.5e9])
  def test_a_limit_outside_the_schema_is_refused(self, limit):
    with patch(f"{_READS}.account_register.get_account_register") as read:
      result = GetAccountRegisterTool(_client())._execute_sync(
        {"account_id": "el_rent", "limit": limit}
      )
    assert result == {
      "error": "invalid_arguments",
      "message": "limit must be between 1 and 1000",
    }
    read.assert_not_called()

  @pytest.mark.parametrize("limit", [1, 1000, "50"])
  def test_a_limit_inside_it_is_passed_on(self, limit):
    with (
      _sessions(),
      patch(
        f"{_READS}.account_register.get_account_register", return_value=_REGISTER
      ) as read,
    ):
      GetAccountRegisterTool(_client())._execute_sync(
        {"account_id": "el_rent", "limit": limit}
      )
    assert read.call_args.kwargs["limit"] == int(limit)

  def test_a_window_that_ends_before_it_starts_is_passed_on(self):
    """The read answers an inverted window as an empty one (see
    `test_account_register_db`); the tool does not refuse it first."""
    with (
      _sessions(),
      patch(
        f"{_READS}.account_register.get_account_register", return_value=_REGISTER
      ) as read,
    ):
      result = GetAccountRegisterTool(_client())._execute_sync(
        {
          "account_id": "el_rent",
          "start_date": "2026-09-30",
          "end_date": "2026-09-01",
        }
      )
    assert "error" not in result
    assert read.call_args.kwargs["start_date"] == date(2026, 9, 30)
    assert read.call_args.kwargs["end_date"] == date(2026, 9, 1)

  @pytest.mark.parametrize(
    ("tool_class", "target", "arguments"),
    [
      (
        GetAccountRegisterTool,
        f"{_READS}.account_register.get_account_register",
        {"account_id": "el_rent"},
      ),
      (ListBankAccountsTool, f"{_READS}.bank_accounts.list_bank_accounts", {}),
      (ListFlowCandidatesTool, f"{_READS}.taxonomies.list_entity_flow_candidates", {}),
      (
        GetClassificationStatsTool,
        f"{_READS}.classification.classification_stats",
        {"period": "2026-09"},
      ),
      (
        ListDischargeCandidatesTool,
        f"{_READS}.ar_ap.list_discharge_candidates",
        {"event_id": "evt_1"},
      ),
      (ListReportsTool, f"{_READS}.reports.list_reports", {}),
      (
        ListElementArcsTool,
        "robosystems.operations.library.reads.get_element_arcs",
        {"element_id": "el_rent"},
      ),
    ],
  )
  def test_an_unexpected_fault_answers_a_fixed_message(
    self, tool_class, target, arguments
  ):
    secret = "connection to 10.0.0.12:5432 refused for role tenant_kg01"
    with (
      _sessions(),
      patch(f"{_READS}.bank_accounts.load_connection_health", return_value={}),
      patch(target, side_effect=RuntimeError(secret)),
    ):
      tool = tool_class(_client())
      result = tool._execute_sync(arguments)
    name = tool.get_tool_definition()["name"]
    assert result == {"error": "command_failed", "message": f"{name} failed"}
