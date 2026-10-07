"""The ticker an entity gets when nobody names one, shared by graph creation
and ``create-entity`` so a derived ticker is the same either way."""

from __future__ import annotations

import re


def derive_ticker(name: str) -> str:
  """The initials of the name's words (at most six), or the first four
  characters of a one-word name."""
  words = re.sub(r"[^a-zA-Z0-9\s]", "", name).split()
  if len(words) >= 2:
    return "".join(word[0].upper() for word in words if word)[:6]
  return name[:4].upper().replace(" ", "") or "ENT"
