"""YAML that arrives from a caller. Every parse of user input goes through here."""

from typing import Any

import yaml


class _NoAliasLoader(yaml.SafeLoader):
  """Refuses YAML aliases: a repeated reference is cheap to parse and
  expands on every later copy or serialization."""

  def compose_node(self, parent, index):  # type: ignore[override]
    if self.check_event(yaml.AliasEvent):
      raise yaml.YAMLError("YAML aliases are not allowed")
    return super().compose_node(parent, index)


def load_untrusted_yaml(text: str) -> Any:
  return yaml.load(text, Loader=_NoAliasLoader)  # noqa: S506
