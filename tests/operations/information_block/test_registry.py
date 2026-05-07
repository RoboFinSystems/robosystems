"""Registry tests — Schedule + statement family wired, get/list work."""

from __future__ import annotations

import pytest

from robosystems.models.api.extensions.schedules import CreateScheduleRequest
from robosystems.models.api.information_block import (
  ScheduleMechanics,
  StatementMechanics,
)
from robosystems.operations.information_block.registry import (
  BALANCE_SHEET_BLOCK,
  CASH_FLOW_STATEMENT_BLOCK,
  EQUITY_STATEMENT_BLOCK,
  INCOME_STATEMENT_BLOCK,
  REGISTRY,
  SCHEDULE_BLOCK,
  get,
  list_registered,
)

STATEMENT_BLOCK_IDS = [
  "balance_sheet",
  "income_statement",
  "cash_flow_statement",
  "equity_statement",
]


class TestRegistry:
  def test_schedule_block_registered(self) -> None:
    assert "schedule" in REGISTRY
    assert REGISTRY["schedule"] is SCHEDULE_BLOCK

  def test_schedule_block_shape(self) -> None:
    assert SCHEDULE_BLOCK.id == "schedule"
    assert SCHEDULE_BLOCK.display_name == "Schedule"
    assert SCHEDULE_BLOCK.display_plural == "Schedules"
    assert SCHEDULE_BLOCK.category == "Close"
    assert SCHEDULE_BLOCK.construction_mode == "declarative"
    assert SCHEDULE_BLOCK.concept_arrangement_default == "roll_forward"
    assert SCHEDULE_BLOCK.member_arrangement_default is None
    # Mechanics + create request models wired to the right Pydantic classes
    assert SCHEDULE_BLOCK.mechanics_schema is ScheduleMechanics
    assert SCHEDULE_BLOCK.create_request_model is CreateScheduleRequest
    # Schedules are tenant-only; not surfaced on the library sentinel
    assert SCHEDULE_BLOCK.surfaces_in_library is False

  def test_get_returns_entry(self) -> None:
    entry = get("schedule")
    assert entry is SCHEDULE_BLOCK

  def test_get_unknown_raises_keyerror(self) -> None:
    with pytest.raises(KeyError) as exc:
      get("nonexistent_block_type")
    assert "nonexistent_block_type" in str(exc.value)

  def test_list_registered_returns_all_entries(self) -> None:
    entries = list_registered()
    # Schedule + 4 statement block types + Phase η metric.
    assert [e.id for e in entries] == [
      "schedule",
      *STATEMENT_BLOCK_IDS,
      "metric",
    ]

  def test_entry_is_frozen(self) -> None:
    """Registry entries are immutable — a typo in handler wiring can't be
    patched out by a caller at runtime."""
    with pytest.raises(Exception):  # FrozenInstanceError (subclass of AttributeError)
      SCHEDULE_BLOCK.category = "Mutated"  # type: ignore[misc]


class TestStatementRegistration:
  @pytest.mark.parametrize(
    "block_type,entry",
    [
      ("balance_sheet", BALANCE_SHEET_BLOCK),
      ("income_statement", INCOME_STATEMENT_BLOCK),
      ("cash_flow_statement", CASH_FLOW_STATEMENT_BLOCK),
      ("equity_statement", EQUITY_STATEMENT_BLOCK),
    ],
  )
  def test_statement_block_registered(self, block_type: str, entry) -> None:
    assert block_type in REGISTRY
    assert REGISTRY[block_type] is entry
    assert get(block_type) is entry

  @pytest.mark.parametrize("block_type", STATEMENT_BLOCK_IDS)
  def test_statement_block_shape(self, block_type: str) -> None:
    entry = get(block_type)
    assert entry.id == block_type
    assert entry.category == "Reporting"
    assert entry.construction_mode == "compositional"
    assert entry.concept_arrangement_default == "roll_up"
    assert entry.member_arrangement_default == "aggregation"
    assert entry.mechanics_schema is StatementMechanics
    # All four statement block types surface on the library sentinel
    # because their Structures live in public.structures.
    assert entry.surfaces_in_library is True


def _arm_block_types(union_alias) -> set[str]:
  """Return every ``block_type`` Literal value reachable through a
  discriminated-union alias.

  ``Annotated[A | B, Discriminator(...)]`` exposes the union arms via
  ``__args__`` on the inner ``Union``. Each arm's ``block_type`` field
  is a ``Literal[...]`` whose options live in ``Field.annotation.__args__``.
  We unpack both layers and union the resulting strings.
  """
  from typing import get_args

  inner_union, _discriminator = get_args(union_alias)
  arms = get_args(inner_union)
  block_types: set[str] = set()
  for arm in arms:
    field_annotation = arm.model_fields["block_type"].annotation
    block_types.update(get_args(field_annotation))
  return block_types


def _arm_payload_for_block_type(union_alias, block_type: str) -> type:
  """Return the ``payload`` field's type annotation on the union arm
  matching ``block_type``.

  Used to assert that the arm carrying a given block type uses the
  same Pydantic request model the registry has wired into
  ``create_request_model`` / ``update_request_model`` /
  ``delete_request_model``. When they drift, the wire shape and the
  dispatch handler stop agreeing on the payload.
  """
  from typing import get_args

  inner_union, _discriminator = get_args(union_alias)
  arms = get_args(inner_union)
  for arm in arms:
    arm_block_types = set(get_args(arm.model_fields["block_type"].annotation))
    if block_type in arm_block_types:
      return arm.model_fields["payload"].annotation
  raise AssertionError(f"No arm carries block_type={block_type!r}")


class TestRequestUnionRegistryDrift:
  """The discriminated-union request models must cover every registered
  block type. When a new entry is added to ``REGISTRY`` without a
  matching union arm, callers see a generic ``ValidationError`` from
  the API layer instead of the registry's intended dispatch — these
  tests fail loudly to make the omission obvious."""

  def test_create_arm_union_covers_registry(self) -> None:
    from robosystems.models.api.information_block import (
      _CreateInformationBlockArms,
    )

    assert _arm_block_types(_CreateInformationBlockArms) == set(REGISTRY.keys())

  def test_update_arm_union_covers_registry(self) -> None:
    from robosystems.models.api.information_block import (
      _UpdateInformationBlockArms,
    )

    assert _arm_block_types(_UpdateInformationBlockArms) == set(REGISTRY.keys())

  def test_delete_arm_union_covers_registry(self) -> None:
    from robosystems.models.api.information_block import (
      _DeleteInformationBlockArms,
    )

    assert _arm_block_types(_DeleteInformationBlockArms) == set(REGISTRY.keys())


class TestRequestUnionPayloadAlignment:
  """When a registry entry has a real (non-sentinel) request model, the
  matching union arm must use *the same* model as its ``payload`` type.

  Why this matters: the registry's request model is the shape the
  dispatch handler validates against. The arm's payload type is the
  shape OpenAPI / SDK callers see at the wire. If they diverge, the
  schema lies — callers serialize one shape, the handler expects
  another, and the failure surfaces as an opaque validation error
  inside dispatch instead of a clean 422 at the boundary.

  Statement-family + metric blocks intentionally use the
  ``_EmptyPayload`` sentinel in the registry (their handlers raise 501
  today). Those land in the legacy arm whose ``payload: dict[str, Any]``
  is loose by design — the dispatcher rejects the call with 501 before
  any payload validation runs.
  """

  def _check_alignment(
    self,
    union_alias,
    operation: str,  # "create" | "update" | "delete"
  ) -> None:
    from robosystems.operations.information_block.registry import (
      _EmptyPayload,
    )

    field_name = f"{operation}_request_model"
    for block_type, entry in REGISTRY.items():
      registry_model = getattr(entry, field_name)
      arm_payload_type = _arm_payload_for_block_type(union_alias, block_type)

      if registry_model is _EmptyPayload:
        # Sentinel registry entries land in the legacy arm whose
        # payload type is the loose `dict[str, Any]` (or `dict`).
        assert arm_payload_type in (dict, dict[str, object]) or (
          getattr(arm_payload_type, "__origin__", None) is dict
        ), (
          f"Registry entry {block_type!r} uses _EmptyPayload but its "
          f"{operation} arm payload is {arm_payload_type!r} — expected "
          "the legacy arm's loose dict shape."
        )
      else:
        assert arm_payload_type is registry_model, (
          f"Registry entry {block_type!r}.{field_name} is "
          f"{registry_model!r} but the {operation} arm's payload is "
          f"{arm_payload_type!r}. Update the typed arm's payload "
          "annotation to match the registry's request model."
        )

  def test_create_arm_payloads_match_registry(self) -> None:
    from robosystems.models.api.information_block import (
      _CreateInformationBlockArms,
    )

    self._check_alignment(_CreateInformationBlockArms, "create")

  def test_update_arm_payloads_match_registry(self) -> None:
    from robosystems.models.api.information_block import (
      _UpdateInformationBlockArms,
    )

    self._check_alignment(_UpdateInformationBlockArms, "update")

  def test_delete_arm_payloads_match_registry(self) -> None:
    from robosystems.models.api.information_block import (
      _DeleteInformationBlockArms,
    )

    self._check_alignment(_DeleteInformationBlockArms, "delete")
