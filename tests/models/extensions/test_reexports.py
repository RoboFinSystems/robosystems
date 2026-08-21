"""The extensions package must re-export everything its extension packages do.

``robosystems.models.extensions`` exists so a caller does not have to know which
extension a model belongs to. That only holds if the re-export list keeps pace,
and it is the kind of list nobody rereads: four names had drifted out of it, and
the omission stayed invisible because the sibling import path still worked.

Asserted as a set comparison rather than a checklist, so a model added to
``roboledger`` or ``roboinvestor`` tomorrow fails here instead of being quietly
unreachable from the parent.
"""

from __future__ import annotations

import pytest

import robosystems.models.extensions as extensions
from robosystems.models.extensions import roboinvestor, roboledger


@pytest.mark.unit
@pytest.mark.parametrize(
  "module", [roboledger, roboinvestor], ids=["roboledger", "roboinvestor"]
)
def test_extension_exports_are_reachable_from_the_parent(module):
  declared = set(module.__all__)
  assert declared, f"{module.__name__} declares no __all__"
  missing = sorted(name for name in declared if not hasattr(extensions, name))
  assert not missing, (
    f"{module.__name__} exports {missing}, which do not resolve from "
    "robosystems.models.extensions. Add them to its imports and __all__."
  )


@pytest.mark.unit
@pytest.mark.parametrize(
  "module", [roboledger, roboinvestor], ids=["roboledger", "roboinvestor"]
)
def test_reexports_are_advertised_in_all(module):
  """Reachable is not enough — an import the parent does not advertise in
  ``__all__`` is absent from ``from ... import *`` and from tooling."""
  undeclared = sorted(
    name for name in module.__all__ if name not in set(extensions.__all__)
  )
  assert not undeclared, (
    f"{module.__name__} exports {undeclared}, which resolve from the parent but "
    "are missing from its __all__."
  )
