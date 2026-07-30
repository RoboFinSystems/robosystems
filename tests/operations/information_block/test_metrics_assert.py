"""Tests for ``cmd_assert_metrics`` — observed values → standing metric FactSet."""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from robosystems.models.api.information_block import AssertMetricsRequest
from robosystems.operations.information_block import metrics as metrics_mod

PERIOD_START = date(2026, 7, 1)
PERIOD_END = date(2026, 7, 31)


def _scalars(items: list[Any]) -> MagicMock:
  result = MagicMock()
  result.scalars.return_value.all.return_value = items
  return result


def _scalar_one(value: Any) -> MagicMock:
  result = MagicMock()
  result.scalar_one_or_none.return_value = value
  return result


def _fetchone(row: Any) -> MagicMock:
  result = MagicMock()
  result.fetchone.return_value = row
  return result


def _structure(block_type: str = "metric") -> MagicMock:
  s = MagicMock()
  s.id = "str_growth"
  s.block_type = block_type
  return s


def _element(
  element_id: str,
  qname: str,
  name: str,
  *,
  monetary: bool = False,
  period_type: str = "instant",
  item_type: str | None = None,
  source: str = "native",
  is_abstract: bool = False,
) -> SimpleNamespace:
  return SimpleNamespace(
    id=element_id,
    qname=qname,
    name=name,
    is_monetary=monetary,
    period_type=period_type,
    item_type=item_type,
    source=source,
    is_abstract=is_abstract,
  )


EL_STARS = _element("el_stars", "rsx:GithubStars", "GitHub Stars")
EL_DOWNLOADS = _element(
  "el_dl", "rsx:NpmDownloads", "npm Downloads", period_type="duration"
)
EL_ABSTRACT = _element(
  "el_root", "rsx:GrowthMetricsAbstract", "Growth Metrics", is_abstract=True
)
EL_LIBRARY = _element(
  "el_cr", "rs-metric:CurrentRatio", "Current Ratio", source="rs-metric"
)
EL_LEVER = _element(
  "el_lev", "rs-driver:RevenueGrowth", "Revenue Growth", source="rs-driver"
)
EL_STRAY = _element("el_stray", "rsx:Unarced", "Not On Structure")


def _arc(to_element_id: str, order: float) -> SimpleNamespace:
  return SimpleNamespace(
    from_element_id="el_root", to_element_id=to_element_id, order_value=order
  )


ARCS = [_arc("el_stars", 1.0), _arc("el_dl", 2.0)]


def _rule() -> SimpleNamespace:
  return SimpleNamespace(id="rule_1", target_element_id="el_stars")


def _session(structure: MagicMock) -> MagicMock:
  session = MagicMock()
  session.get.side_effect = lambda model, pk: structure if pk == structure.id else None
  return session


def _body(**overrides: Any) -> AssertMetricsRequest:
  kwargs: dict[str, Any] = {
    "structure_id": "str_growth",
    "period_start": PERIOD_START,
    "period_end": PERIOD_END,
    "entity_id": "ent_1",
    "source_system": "content-machine",
    "observations": [{"qname": "rsx:GithubStars", "value": 1240}],
  }
  kwargs.update(overrides)
  return AssertMetricsRequest(**kwargs)


def _added_facts(session: MagicMock) -> list[Any]:
  return [call.args[0] for call in session.add.call_args_list]


class TestAssertMetrics:
  def test_asserts_into_new_standing_set_in_arc_order(self) -> None:
    structure = _structure()
    session = _session(structure)
    session.execute.side_effect = [
      _scalars(ARCS),
      _scalars([]),  # no Derive rules
      _scalar_one(EL_DOWNLOADS),  # qname lookup (observation order)
      _scalar_one(EL_STARS),
      _scalar_one(None),  # standing FactSet lookup
    ]
    fact_set = MagicMock()
    fact_set.id = "fs_new"

    with patch.object(
      metrics_mod, "create_fact_set", return_value=fact_set
    ) as create_fs:
      result = metrics_mod.cmd_assert_metrics(
        session,
        _body(
          basis_note="July batch",
          observations=[
            {"qname": "rsx:NpmDownloads", "value": 3811},
            {"qname": "rsx:GithubStars", "value": 1240},
          ],
        ),
        created_by="user_1",
      )

    create_fs.assert_called_once()
    fs_kwargs = create_fs.call_args.kwargs
    assert fs_kwargs["factset_type"] == "metric"
    assert fs_kwargs["entity_id"] == "ent_1"
    assert fs_kwargs["period_end"] == PERIOD_END
    provenance = fs_kwargs["provenance"]
    assert provenance.origin == "asserted"
    assert provenance.source_system == "content-machine"
    assert provenance.asserted_by == "user_1"
    assert provenance.basis_note == "July batch"

    assert result.replaced is False
    assert result.fact_set_id == "fs_new"
    # Observations were given in reverse arc order — response re-sorts.
    assert [m.element_qname for m in result.asserted] == [
      "rsx:GithubStars",
      "rsx:NpmDownloads",
    ]
    facts = _added_facts(session)
    assert len(facts) == 2
    assert {f.fact_set_id for f in facts} == {"fs_new"}
    assert all(f.fact_type == "Numeric" for f in facts)

  def test_reassert_replaces_prior_facts_and_restamps_provenance(self) -> None:
    structure = _structure()
    session = _session(structure)
    standing = MagicMock()
    standing.id = "fs_standing"
    old_facts = [MagicMock(), MagicMock()]
    session.execute.side_effect = [
      _scalars(ARCS),
      _scalars([]),
      _scalar_one(EL_STARS),
      _scalar_one(standing),
      _scalars(old_facts),  # prior facts on the standing set
    ]

    with patch.object(metrics_mod, "create_fact_set") as create_fs:
      result = metrics_mod.cmd_assert_metrics(session, _body(), created_by="user_1")

    create_fs.assert_not_called()
    for fact in old_facts:
      session.delete.assert_any_call(fact)
    assert standing.provenance["origin"] == "asserted"
    assert standing.provenance["source_system"] == "content-machine"
    assert result.replaced is True
    assert result.fact_set_id == "fs_standing"

  def test_missing_structure_rejected(self) -> None:
    session = MagicMock()
    session.get.return_value = None
    session.get.side_effect = None

    with pytest.raises(ValueError, match="Structure not found"):
      metrics_mod.cmd_assert_metrics(session, _body(), created_by="user_1")

  def test_wrong_block_type_rejected(self) -> None:
    session = _session(_structure(block_type="schedule"))

    with pytest.raises(ValueError, match="block_type='metric'"):
      metrics_mod.cmd_assert_metrics(session, _body(), created_by="user_1")

  def test_structure_with_derive_rules_is_compute_owned(self) -> None:
    session = _session(_structure())
    session.execute.side_effect = [
      _scalars(ARCS),
      _scalars([_rule()]),
    ]

    with pytest.raises(ValueError, match="compute-owned"):
      metrics_mod.cmd_assert_metrics(session, _body(), created_by="user_1")
    assert session.execute.call_count == 2

  def test_duplicate_qnames_rejected_before_lookups(self) -> None:
    session = _session(_structure())
    session.execute.side_effect = [
      _scalars(ARCS),
      _scalars([]),
    ]

    with pytest.raises(ValueError, match="Duplicate observation qname"):
      metrics_mod.cmd_assert_metrics(
        session,
        _body(
          observations=[
            {"qname": "rsx:GithubStars", "value": 1.0},
            {"qname": "rsx:GithubStars", "value": 2.0},
          ]
        ),
        created_by="user_1",
      )
    assert session.execute.call_count == 2

  def test_unknown_qnames_aggregated_into_one_error(self) -> None:
    session = _session(_structure())
    session.execute.side_effect = [
      _scalars(ARCS),
      _scalars([]),
      _scalar_one(None),
      _scalar_one(None),
    ]

    with pytest.raises(ValueError) as exc:
      metrics_mod.cmd_assert_metrics(
        session,
        _body(
          observations=[
            {"qname": "rsx:Nope", "value": 1.0},
            {"qname": "rsx:AlsoNope", "value": 2.0},
          ]
        ),
        created_by="user_1",
      )
    assert "rsx:Nope" in str(exc.value)
    assert "rsx:AlsoNope" in str(exc.value)

  def test_abstract_element_rejected(self) -> None:
    session = _session(_structure())
    session.execute.side_effect = [
      _scalars(ARCS),
      _scalars([]),
      _scalar_one(EL_ABSTRACT),
    ]

    with pytest.raises(ValueError, match="abstract"):
      metrics_mod.cmd_assert_metrics(
        session,
        _body(observations=[{"qname": "rsx:GrowthMetricsAbstract", "value": 1.0}]),
        created_by="user_1",
      )

  @pytest.mark.parametrize(
    ("element", "doctrine"),
    [
      (EL_LIBRARY, "computed by compute-metrics"),
      (EL_LEVER, "forecast levers"),
    ],
  )
  def test_blocked_element_sources_rejected(
    self, element: SimpleNamespace, doctrine: str
  ) -> None:
    session = _session(_structure())
    session.execute.side_effect = [
      _scalars(ARCS),
      _scalars([]),
      _scalar_one(element),
    ]

    with pytest.raises(ValueError, match=doctrine):
      metrics_mod.cmd_assert_metrics(
        session,
        _body(observations=[{"qname": element.qname, "value": 1.0}]),
        created_by="user_1",
      )

  def test_element_outside_structure_catalog_rejected(self) -> None:
    session = _session(_structure())
    session.execute.side_effect = [
      _scalars(ARCS),
      _scalars([]),
      _scalar_one(EL_STRAY),
    ]

    with pytest.raises(ValueError, match="presentation catalog"):
      metrics_mod.cmd_assert_metrics(
        session,
        _body(observations=[{"qname": "rsx:Unarced", "value": 1.0}]),
        created_by="user_1",
      )

  def test_duration_and_instant_period_shapes(self) -> None:
    structure = _structure()
    session = _session(structure)
    session.execute.side_effect = [
      _scalars(ARCS),
      _scalars([]),
      _scalar_one(EL_STARS),
      _scalar_one(EL_DOWNLOADS),
      _scalar_one(None),
    ]
    fact_set = MagicMock()
    fact_set.id = "fs_new"

    with patch.object(metrics_mod, "create_fact_set", return_value=fact_set):
      metrics_mod.cmd_assert_metrics(
        session,
        _body(
          observations=[
            {"qname": "rsx:GithubStars", "value": 1240},
            {"qname": "rsx:NpmDownloads", "value": 3811},
          ]
        ),
        created_by="user_1",
      )

    by_element = {f.element_id: f for f in _added_facts(session)}
    assert by_element["el_stars"].period_type == "instant"
    assert by_element["el_stars"].period_start is None
    assert by_element["el_dl"].period_type == "duration"
    assert by_element["el_dl"].period_start == PERIOD_START
    assert by_element["el_dl"].period_end == PERIOD_END

  def test_default_entity_resolved_when_omitted(self) -> None:
    structure = _structure()
    session = _session(structure)
    session.execute.side_effect = [
      _fetchone(SimpleNamespace(id="ent_first")),
      _scalars(ARCS),
      _scalars([]),
      _scalar_one(EL_STARS),
      _scalar_one(None),
    ]
    fact_set = MagicMock()
    fact_set.id = "fs_new"

    with patch.object(
      metrics_mod, "create_fact_set", return_value=fact_set
    ) as create_fs:
      result = metrics_mod.cmd_assert_metrics(
        session, _body(entity_id=None), created_by="user_1"
      )

    assert result.entity_id == "ent_first"
    assert create_fs.call_args.kwargs["entity_id"] == "ent_first"
