"""Derive an entity's initial Reporting Style from its legal form, at provision
time. Later changes go through the ``change-reporting-style`` operation."""

from __future__ import annotations

from typing import Any

from robosystems.config.constants import ReportingStyleConstants as _RS

# Entity legal form (lowercased) -> seeded Style Structure id. Only the
# forms with a dedicated equity-form Style are listed; corporation /
# subsidiary / sole_proprietorship / non_profit / blank / unknown all fall
# back to the corporate Default below.
_ENTITY_TYPE_STYLE: dict[str, str] = {
  "partnership": _RS.PARTNERSHIP_STYLE_ID,
  "llc": _RS.LLC_STYLE_ID,
  "limited_liability_company": _RS.LLC_STYLE_ID,
}


def default_style_for(entity_type: str | None) -> str:
  """Map an entity legal form to its default Reporting Style id."""
  if not entity_type:
    return _RS.DEFAULT_STYLE_ID
  return _ENTITY_TYPE_STYLE.get(entity_type.strip().lower(), _RS.DEFAULT_STYLE_ID)


def resolve_reporting_style_id(entity_data: dict[str, Any] | None) -> str:
  """Reporting Style id for a new entity: an explicit ``reporting_style_id``,
  else one derived from ``entity_type``, else the corporate Default.

  Reads only the raw create payload; never touches the database.
  """
  if not entity_data:
    return _RS.DEFAULT_STYLE_ID
  explicit = entity_data.get("reporting_style_id")
  if explicit:
    return str(explicit)
  return default_style_for(entity_data.get("entity_type"))
