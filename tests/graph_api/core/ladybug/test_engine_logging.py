"""Query parameter values must never reach a log line.

The Operations Security Policy states that logs shall not contain sensitive
data or payloads. Cypher parameter values are customer data by construction —
entity names, email addresses, amounts — so the failed-query log records the
shape of the bound parameters and never their values.
"""

from robosystems.graph_api.core.ladybug.engine import describe_param_shape


class TestDescribeParamShape:
  def test_returns_keys_and_types_never_values(self):
    """The whole point: a caller reading the log learns which parameters were
    bound and what they were, without learning who the customer is."""
    shape = describe_param_shape(
      {
        "entity_name": "Northwind Traders LLC",
        "email": "cfo@northwind.example",
        "amount": 148250.75,
        "period_count": 12,
        "is_closed": True,
      }
    )

    assert shape == {
      "entity_name": "str",
      "email": "str",
      "amount": "float",
      "period_count": "int",
      "is_closed": "bool",
    }

    # No value, in any form, survives into the output.
    rendered = str(shape)
    for value in ("Northwind", "northwind.example", "148250", "cfo@"):
      assert value not in rendered

  def test_none_and_empty_are_empty_shapes(self):
    """An unparameterized query logs an empty shape rather than a null that a
    reader has to interpret."""
    assert describe_param_shape(None) == {}
    assert describe_param_shape({}) == {}

  def test_none_valued_parameter_is_reported_as_such(self):
    """A parameter bound to None is one of the failures this diagnostic exists
    to catch, so it must be distinguishable from an absent key."""
    shape = describe_param_shape({"cik": None, "year": 2026})
    assert shape == {"cik": "NoneType", "year": "int"}

  def test_nested_structures_do_not_leak_their_contents(self):
    """A dict or list parameter is described by its container type only —
    recursing would reintroduce exactly the values this exists to keep out."""
    shape = describe_param_shape(
      {
        "rows": [{"email": "leak@example.com"}],
        "filters": {"tax_id": "12-3456789"},
      }
    )

    assert shape == {"rows": "list", "filters": "dict"}
    rendered = str(shape)
    assert "leak@example.com" not in rendered
    assert "12-3456789" not in rendered
