"""Every structure that holds library arcs is written by exactly one package.

``create_library_arcs(retire_stale=True)`` retires the arcs in a package's own
structures that its seed no longer emits. If two packages ever wrote arcs into
one structure, reseeding either would delete the other's.
"""

from collections import defaultdict

import pytest

from robosystems.operations.taxonomy_block.library_creator import (
  _default_role_uri,
  _structure_id,
)
from robosystems.taxonomy import load_taxonomy_package
from robosystems.taxonomy.discovery import FRAMEWORKS_DIR

pytestmark = pytest.mark.unit


def test_no_two_packages_write_arcs_into_one_structure():
  writers: dict[str, set[str]] = defaultdict(set)
  paths = sorted(FRAMEWORKS_DIR.glob("*/packages/*/*/taxonomy.jsonld"))
  assert paths

  for path in paths:
    package = load_taxonomy_package(path)
    if not package.associations:
      continue
    own = {_structure_id(s.role_uri) for s in package.structures}
    own.add(_structure_id(_default_role_uri(package)))
    for assoc in package.associations:
      # Unroled arcs route to the package's own structures or its default.
      targets = {_structure_id(assoc.role)} if assoc.role else own
      for structure_id in targets:
        writers[structure_id].add(f"{package.standard}/{package.version}")

  shared = {sid: sorted(pkgs) for sid, pkgs in writers.items() if len(pkgs) > 1}
  assert shared == {}
