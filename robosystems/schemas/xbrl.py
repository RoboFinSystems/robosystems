"""The XBRL tables' columns, as xbrlkit declares them.

One asset, two consumers. ``xbrlkit.schema`` is the ordered declaration of
the property-graph tables a filing projects into, and both ``xbrlkit build
--format lpg`` and this platform's compiled schemas create those tables from
it, so Cypher written against a single-filing ``.lbug`` runs unchanged on the
shared ``sec`` graph. The platform keeps its own ``Node`` and ``Relationship``
objects — they carry the descriptions the schema surface shows and compose
with the non-XBRL tables — but the columns, their types, their order and the
primary key come from xbrlkit, so the two DDLs cannot drift
(``tests/schemas/test_xbrlkit_parity.py`` asserts they are equal). Column
order is load-bearing: LadybugDB's ``COPY FROM`` is positional.
"""

from xbrlkit.schema import node_table, rel_table

from .models import Property


def xbrl_node_properties(name: str) -> list[Property]:
  """The properties of the XBRL node table ``name``, in xbrlkit's order."""
  spec = node_table(name)
  return [
    Property(name=p.name, type=p.type, is_primary_key=p.name == spec.primary_key)
    for p in spec.properties
  ]


def xbrl_relationship_properties(name: str) -> list[Property]:
  """The properties of the XBRL relationship table ``name`` (often none)."""
  return [Property(name=p.name, type=p.type) for p in rel_table(name).properties]
