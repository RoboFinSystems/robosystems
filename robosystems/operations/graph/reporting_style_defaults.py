"""Derive an entity's initial Reporting Style from its legal form.

Used at entity-provision time (``_provision_entity``). The mapping is
intentionally small and DB-free: it points entity legal forms at the
library-seeded Style Structure ids. An explicit ``reporting_style_id`` on
the create request overrides the derived default; everything unknown falls
back to the corporate Default. Post-creation, the entity's Style is changed
through the ``change-reporting-style`` operation instead.
"""

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
  """Resolve the Reporting Style id to pin on a new graph.

  Precedence: an explicit ``reporting_style_id`` on the create request wins;
  otherwise it's derived from the entity's ``entity_type``; otherwise the
  corporate Default. ``entity_data`` is the raw create payload dict (read
  before the tenant schema exists), so this never touches the database.
  """
  if not entity_data:
    return _RS.DEFAULT_STYLE_ID
  explicit = entity_data.get("reporting_style_id")
  if explicit:
    return str(explicit)
  return default_style_for(entity_data.get("entity_type"))
