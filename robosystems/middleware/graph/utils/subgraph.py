"""Subgraph ID parsing and validation (`{parent}_{name}`)."""

import re
from enum import Enum
from typing import NamedTuple

from ..types import SUBGRAPH_NAME_PATTERN as SUBGRAPH_NAME_PATTERN_STR


class SubgraphType(Enum):
  """Unused; the live SubgraphType is in models/api/graphs/subgraphs.py."""

  STATIC = "static"


class SubgraphInfo(NamedTuple):
  graph_id: str  # kg123_dev
  parent_graph_id: str  # kg123
  subgraph_name: str  # dev
  database_name: str  # on disk; same as graph_id
  subgraph_index: int | None = None


# User-graph parents; shared repos can also be parents (platform-managed).
PARENT_GRAPH_PATTERN = re.compile(r"^kg[a-f0-9]{16,}$")

SUBGRAPH_NAME_PATTERN = re.compile(SUBGRAPH_NAME_PATTERN_STR)

FULL_SUBGRAPH_PATTERN = re.compile(r"^(kg[a-f0-9]{16,})_([a-zA-Z0-9]{1,20})$")


def _is_shared_repo(graph_id: str) -> bool:
  from robosystems.config.shared_repositories import is_shared_repository

  return is_shared_repository(graph_id)


def parse_subgraph_id(graph_id: str) -> SubgraphInfo | None:
  """SubgraphInfo for a user or shared-repo subgraph ID, else None."""
  match = FULL_SUBGRAPH_PATTERN.match(graph_id)
  if match:
    parent_id = match.group(1)
    subgraph_name = match.group(2)

    return SubgraphInfo(
      graph_id=graph_id,
      parent_graph_id=parent_id,
      subgraph_name=subgraph_name,
      database_name=graph_id,
    )

  # Shared-repo subgraph, e.g. "sec_historical".
  if "_" in graph_id:
    parts = graph_id.split("_", 1)
    parent_part = parts[0]
    subgraph_part = parts[1] if len(parts) > 1 else ""

    if parent_part and subgraph_part and _is_shared_repo(parent_part):
      if SUBGRAPH_NAME_PATTERN.match(subgraph_part):
        return SubgraphInfo(
          graph_id=graph_id,
          parent_graph_id=parent_part,
          subgraph_name=subgraph_part,
          database_name=graph_id,
        )

  return None


def validate_subgraph_name(name: str) -> bool:
  """1-20 ASCII alphanumerics."""
  return bool(SUBGRAPH_NAME_PATTERN.match(name))


def validate_parent_graph_id(graph_id: str) -> bool:
  """A user graph or shared repository ID that is not itself a subgraph."""
  if parse_subgraph_id(graph_id):
    return False

  if PARENT_GRAPH_PATTERN.match(graph_id):
    return True

  return _is_shared_repo(graph_id)


def construct_subgraph_id(parent_graph_id: str, subgraph_name: str) -> str:
  if not validate_parent_graph_id(parent_graph_id):
    raise ValueError(f"Invalid parent graph ID: {parent_graph_id}")

  if not validate_subgraph_name(subgraph_name):
    raise ValueError(
      f"Invalid subgraph name: {subgraph_name}. Must be alphanumeric and 1-20 characters."
    )

  return f"{parent_graph_id}_{subgraph_name}"


def get_database_name(graph_id: str) -> str:
  """The on-disk database name, which is the graph ID for graphs and subgraphs."""
  return graph_id


def split_graph_hierarchy(graph_id: str) -> tuple[str, str | None]:
  subgraph_info = parse_subgraph_id(graph_id)
  if subgraph_info:
    return subgraph_info.parent_graph_id, subgraph_info.subgraph_name

  return graph_id, None


def is_subgraph(graph_id: str) -> bool:
  return parse_subgraph_id(graph_id) is not None


def is_parent_graph(graph_id: str) -> bool:
  return validate_parent_graph_id(graph_id)


def generate_unique_subgraph_name(
  parent_graph_id: str, base_name: str, existing_names: list[str]
) -> str:
  """Append a number when the cleaned name is taken."""
  clean_name = re.sub(r"[^a-zA-Z0-9]", "", base_name)[:17]  # room for a suffix

  if not clean_name:
    clean_name = "subgraph"

  if clean_name not in existing_names and validate_subgraph_name(clean_name):
    return clean_name

  for i in range(1, 100):
    candidate = f"{clean_name}{i}"
    if len(candidate) <= 20 and candidate not in existing_names:
      return candidate

  raise ValueError(f"Unable to generate unique subgraph name for base: {base_name}")
