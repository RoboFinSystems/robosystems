"""Identifier rules for user-supplied graph schema names.

A custom schema's node, relationship and property names become DDL text. They
are bare identifiers, never anything that could end a statement or start a
new one; the API model and the parser both refuse anything else.
"""

import re

IDENTIFIER_PATTERN = r"^[A-Za-z_][A-Za-z0-9_]{0,127}$"
TYPE_PATTERN = r"^[A-Z0-9_]+(\[[0-9]{1,5}\])?$"

_IDENTIFIER_RE = re.compile(IDENTIFIER_PATTERN)
_TYPE_RE = re.compile(TYPE_PATTERN)


def require_identifier(name: object, kind: str, *, allow_wildcard: bool = False) -> str:
  """Return ``name`` when it is a bare identifier; raise ``ValueError`` otherwise.

  ``allow_wildcard`` admits the literal ``*`` a relationship uses for any
  node table.
  """
  if allow_wildcard and name == "*":
    return "*"
  if not isinstance(name, str) or not _IDENTIFIER_RE.fullmatch(name):
    raise ValueError(
      f"Invalid {kind} name {name!r}: must start with a letter or underscore and "
      "contain only letters, digits and underscores (at most 128 characters)"
    )
  return name


def require_type(type_name: object, property_name: str) -> str:
  """Return the upper-cased type when it is a plain or sized type token."""
  if not isinstance(type_name, str) or not _TYPE_RE.fullmatch(type_name.upper()):
    raise ValueError(
      f"Invalid type {type_name!r} for property '{property_name}': expected a "
      "type name, optionally sized like FLOAT[384]"
    )
  return type_name.upper()
