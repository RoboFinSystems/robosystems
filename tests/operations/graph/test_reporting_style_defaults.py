"""Unit tests for entity-driven Reporting Style derivation at provision.

``default_style_for`` maps an entity legal form to a seeded Style id;
``resolve_reporting_style_id`` applies the precedence used in
``GraphCreationService._persist_metadata`` (explicit override > derived >
corporate Default). Pure mapping, no DB.
"""

from __future__ import annotations

import pytest

from robosystems.config.constants import ReportingStyleConstants as RS
from robosystems.operations.graph.reporting_style_defaults import (
  default_style_for,
  resolve_reporting_style_id,
)


class TestDefaultStyleFor:
  @pytest.mark.parametrize(
    "entity_type,expected",
    [
      ("partnership", RS.PARTNERSHIP_STYLE_ID),
      ("Partnership", RS.PARTNERSHIP_STYLE_ID),
      ("  PARTNERSHIP  ", RS.PARTNERSHIP_STYLE_ID),
      ("llc", RS.LLC_STYLE_ID),
      ("LLC", RS.LLC_STYLE_ID),
      ("limited_liability_company", RS.LLC_STYLE_ID),
      ("corporation", RS.DEFAULT_STYLE_ID),
      ("subsidiary", RS.DEFAULT_STYLE_ID),
      ("sole_proprietorship", RS.DEFAULT_STYLE_ID),
      ("non_profit", RS.DEFAULT_STYLE_ID),
      ("something_unknown", RS.DEFAULT_STYLE_ID),
      ("", RS.DEFAULT_STYLE_ID),
      (None, RS.DEFAULT_STYLE_ID),
    ],
  )
  def test_maps_legal_form_to_style(self, entity_type, expected) -> None:
    assert default_style_for(entity_type) == expected


class TestResolveReportingStyleId:
  def test_none_or_empty_payload_is_corporate_default(self) -> None:
    assert resolve_reporting_style_id(None) == RS.DEFAULT_STYLE_ID
    assert resolve_reporting_style_id({}) == RS.DEFAULT_STYLE_ID

  def test_derives_from_entity_type(self) -> None:
    assert (
      resolve_reporting_style_id({"entity_type": "partnership"})
      == RS.PARTNERSHIP_STYLE_ID
    )
    assert resolve_reporting_style_id({"entity_type": "llc"}) == RS.LLC_STYLE_ID
    assert (
      resolve_reporting_style_id({"entity_type": "corporation"}) == RS.DEFAULT_STYLE_ID
    )

  def test_explicit_override_beats_derived_default(self) -> None:
    # An explicit reporting_style_id wins even when entity_type would map
    # to a different (or the default) style.
    assert (
      resolve_reporting_style_id(
        {"entity_type": "partnership", "reporting_style_id": "custom-style-id"}
      )
      == "custom-style-id"
    )
    assert (
      resolve_reporting_style_id({"reporting_style_id": RS.LLC_STYLE_ID})
      == RS.LLC_STYLE_ID
    )
