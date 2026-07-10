"""Pin ``ReportingStyleConstants`` UUIDs to their deterministic derivation.

The library-seeded Reporting Style ids in ``config/constants.py`` are
hardcoded for use as the default on ``entities.reporting_style_id`` (stamped
from the entity's legal form at creation).
They MUST equal ``generate_deterministic_uuid(role_uri, namespace="structure")``
or migration 0008's INSERT will write a different id than the platform
default points at, breaking provisioning silently.

This test pins each constant to its derivation so a typo in either side
fails CI instead of producing wrong ids that pass validation but never
resolve to a real row.
"""

from __future__ import annotations

import pytest

from robosystems.config.constants import ReportingStyleConstants
from robosystems.utils.uuid import generate_deterministic_uuid

_DEFAULT_STYLE_ROLE = "https://robosystems.ai/seattle/cm-roles/roles/styles/Default"
_PARTNERSHIP_STYLE_ROLE = (
  "https://robosystems.ai/seattle/cm-roles/roles/styles/Partnership"
)
_LLC_STYLE_ROLE = (
  "https://robosystems.ai/seattle/cm-roles/roles/styles/LimitedLiabilityCompany"
)


@pytest.mark.unit
class TestReportingStyleConstants:
  def test_default_style_id_matches_derivation(self) -> None:
    assert (
      generate_deterministic_uuid(_DEFAULT_STYLE_ROLE, namespace="structure")
      == ReportingStyleConstants.DEFAULT_STYLE_ID
    )

  def test_partnership_style_id_matches_derivation(self) -> None:
    assert (
      generate_deterministic_uuid(_PARTNERSHIP_STYLE_ROLE, namespace="structure")
      == ReportingStyleConstants.PARTNERSHIP_STYLE_ID
    )

  def test_llc_style_id_matches_derivation(self) -> None:
    assert (
      generate_deterministic_uuid(_LLC_STYLE_ROLE, namespace="structure")
      == ReportingStyleConstants.LLC_STYLE_ID
    )
