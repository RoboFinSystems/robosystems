"""Text-level Cypher helpers applied before a query reaches the Graph API."""

import re

_LITERAL_RE = re.compile(
  r"'(?:[^'\\]|\\[\s\S])*+(?:'|\\?\Z)"
  r'|"(?:[^"\\]|\\[\s\S])*+(?:"|\\?\Z)'
  r"|`[^`]*+(?:`|\Z)"
)
_COMMENT_RE = re.compile(r"//[^\n]*+|/\*[\s\S]*?(?:\*/|\Z)")
_MASKED_RE = re.compile(f"{_LITERAL_RE.pattern}|{_COMMENT_RE.pattern}")


def mask_literals(query: str) -> str:
  """Keep every offset; fill literals with ``#`` and comments with spaces.

  A filled literal still ends the text it sits in, so an insertion point
  found by trimming whitespace never lands in front of it.
  """

  def fill(m: re.Match[str]) -> str:
    return " " * len(m.group()) if m.group()[0] == "/" else "#" * len(m.group())

  return _MASKED_RE.sub(fill, query)
