"""Tax identifiers stay out of the MCP surface.

A counterparty's tax ID can be a person's SSN. The app collects it through
its own forms; a chat client neither asks for it (the field is withheld from
every generated input schema and dropped from arguments) nor reads it back in
full (answers carry the last four characters, as the graph stores it).
"""

from __future__ import annotations

from typing import Any

WITHHELD_INPUT_FIELDS: frozenset[str] = frozenset({"tax_id"})

_MASKED_KEYS = frozenset({"tax_id", "taxId"})


def withhold_input_fields(schema: dict[str, Any]) -> dict[str, Any]:
  properties = schema.get("properties")
  if not isinstance(properties, dict) or not WITHHELD_INPUT_FIELDS & properties.keys():
    return schema
  out = dict(schema)
  out["properties"] = {
    k: v for k, v in properties.items() if k not in WITHHELD_INPUT_FIELDS
  }
  if isinstance(schema.get("required"), list):
    out["required"] = [k for k in schema["required"] if k not in WITHHELD_INPUT_FIELDS]
  return out


def drop_withheld_arguments(arguments: dict[str, Any]) -> dict[str, Any]:
  return {k: v for k, v in arguments.items() if k not in WITHHELD_INPUT_FIELDS}


def _mask(value: str) -> str:
  # Same shape as the graph's Agent.tax_id (materialize.py), so an
  # already-masked value is unchanged.
  return "***" + value[-4:]


def mask_tax_ids(value: Any) -> Any:
  if isinstance(value, dict):
    return {
      k: _mask(v) if k in _MASKED_KEYS and isinstance(v, str) and v else mask_tax_ids(v)
      for k, v in value.items()
    }
  if isinstance(value, list):
    return [mask_tax_ids(item) for item in value]
  return value
