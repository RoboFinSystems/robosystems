"""Text-level Cypher helpers applied before a query reaches the Graph API."""

import re

_LITERAL_RE = re.compile(
  r"'(?:[^'\\]|\\[\s\S])*+(?:'|\\?\Z)"
  r'|"(?:[^"\\]|\\[\s\S])*+(?:"|\\?\Z)'
  r"|`[^`]*+(?:`|\Z)"
)
_COMMENT_RE = re.compile(r"//[^\n]*+|/\*[\s\S]*?(?:\*/|\Z)")
_MASKED_RE = re.compile(f"{_LITERAL_RE.pattern}|{_COMMENT_RE.pattern}")
_TOKEN_RE = re.compile(
  r"(?P<sort>(?<![\w.$])ORDER\s+BY\b)"
  r"|(?P<open>[(\[{])|(?P<close>[)\]}])|(?P<end>;)|(?P<comma>,)"
  r"|(?P<word>(?<![\w.$])[A-Za-z_]\w*)|(?P<other>\S)",
  re.IGNORECASE,
)
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
  """Keep every offset; fill literals with ``#`` and comments with spaces.

  A filled literal still ends the text it sits in, so an insertion point
  found by trimming whitespace never lands in front of it.
  """

  def fill(m: re.Match[str]) -> str:
    return " " * len(m.group()) if m.group()[0] == "/" else "#" * len(m.group())

  return _MASKED_RE.sub(fill, query)


def add_sort_tiebreaker(query: str) -> str:
  """End every multi-key ORDER BY with a constant string key.

  LadybugDB #1067: a sort whose last key follows a STRING key drops and
  misorders rows once the scan passes a few thousand rows, at any LIMIT. A
  trailing string key avoids it on NULL-free keys (NULLs still misorder),
  and a constant one only breaks ties the query left open. A single key is
  unaffected, and the extra key would cost it the engine's top-k path.
  """
  ends = _multi_key_sort_ends(mask_literals(query))
  if not ends:
    return query
  pieces: list[str] = []
  start = 0
  for end in sorted(ends):
    pieces += [query[start:end], ", ''"]
    start = end
  pieces.append(query[start:])
  return "".join(pieces)


def _multi_key_sort_ends(masked: str) -> list[int]:
  """Where each ORDER BY with two or more keys ends, in one pass.

  Each bracket level holds at most one open sort: ``[keys, expecting_key,
  previous_word]``.
  """
  ends: list[int] = []
  trimmed: dict[int, int] = {}

  def close(sort: list | None, pos: int) -> None:
    if sort and sort[0] > 1:
      if pos not in trimmed:
        end = pos
        while end > 0 and masked[end - 1].isspace():
          end -= 1
        trimmed[pos] = end
      ends.append(trimmed[pos])

  levels: list[list | None] = [None]
  for token in _TOKEN_RE.finditer(masked):
    kind = token.lastgroup
    sort = levels[-1]
    if kind == "sort":
      close(sort, token.start())
      levels[-1] = [1, True, ""]
    elif kind == "open":
      if sort:
        sort[1] = False
      levels.append(None)
    elif kind == "close":
      if len(levels) > 1:
        close(levels.pop(), token.start())
      else:
        close(sort, token.start())
        levels[-1] = None
    elif kind == "end":
      close(sort, token.start())
      levels[-1] = None
    elif sort is None:
      continue
    elif kind == "comma":
      sort[0] += 1
      sort[1] = True
    elif kind == "other":
      sort[1] = False
    else:
      word = token.group().upper()
      after_string_op = word == "WITH" and sort[2] in ("STARTS", "ENDS")
      if word in _CLAUSE_WORDS and not sort[1] and not after_string_op:
        close(sort, token.start())
        levels[-1] = None
      else:
        sort[1] = False
        sort[2] = word
  for sort in levels:
    close(sort, len(masked))
  return ends
