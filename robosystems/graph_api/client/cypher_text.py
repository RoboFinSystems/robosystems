"""Text-level Cypher helpers applied before a query reaches the Graph API."""

import re

_LITERAL_RE = re.compile(
  r"'(?:[^'\\]|\\[\s\S])*+(?:'|\\?\Z)"
  r'|"(?:[^"\\]|\\[\s\S])*+(?:"|\\?\Z)'
  r"|`[^`]*+(?:`|\Z)"
  r"|//[^\n]*+"
  r"|/\*[\s\S]*?(?:\*/|\Z)"
)
_ORDER_BY_RE = re.compile(r"(?<![\w.$])ORDER\s+BY\b", re.IGNORECASE)
_SORT_END_RE = re.compile(r"[()\[\]{};]|(?<![\w.$])[A-Za-z_]\w*")
_CLAUSE_WORDS = frozenset(
  {
    "SKIP",
    "LIMIT",
    "RETURN",
    "WITH",
    "WHERE",
    "MATCH",
    "OPTIONAL",
    "UNWIND",
    "CALL",
    "UNION",
    "CREATE",
    "MERGE",
    "SET",
    "DELETE",
    "DETACH",
    "REMOVE",
    "FOREACH",
    "LOAD",
    "COPY",
  }
)


def mask_literals(query: str) -> str:
  """Blank strings, quoted names and comments, keeping every offset."""
  return _LITERAL_RE.sub(lambda m: " " * len(m.group()), query)


def add_sort_tiebreaker(query: str) -> str:
  """End every ORDER BY with a constant string key.

  LadybugDB #1067: a sort whose last key follows a STRING key drops and
  misorders rows once the scan passes a few thousand rows, at any LIMIT. A
  trailing string key avoids it, and a constant one only breaks ties the
  query left open.
  """
  masked = mask_literals(query)
  ends = [_sort_end(masked, m.end()) for m in _ORDER_BY_RE.finditer(masked)]
  for end in sorted(ends, reverse=True):
    query = f"{query[:end]}, ''{query[end:]}"
  return query


def _sort_end(masked: str, start: int) -> int:
  depth = 0
  end = len(masked)
  for token in _SORT_END_RE.finditer(masked, start):
    text = token.group()
    if text in "([{":
      depth += 1
    elif text in ")]}":
      if depth == 0:
        end = token.start()
        break
      depth -= 1
    elif depth == 0 and (text == ";" or text.upper() in _CLAUSE_WORDS):
      end = token.start()
      break
  return len(masked[:end].rstrip())
