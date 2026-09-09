"""Tests for ``result_rows``, the single reader for a LadybugDB QueryResult.

Deliberately no ``MagicMock`` here. A mock answers ``hasattr`` for every
attribute, and that is precisely how the defect this helper replaces stayed
invisible: the old callers branched on ``hasattr(result, "get_as_list")``,
the tests mocked ``get_as_list`` into existence, and the branch that ran in
production — the one that returned nothing — was never the branch under
test. These fakes expose only what ``ladybug.QueryResult`` actually has.
"""

import pytest

from robosystems.graph_api.core.ladybug.results import result_rows


class FakeQueryResult:
  """The engine's cursor surface: ``has_next`` / ``get_next``, nothing else.

  Mirrors ladybug 0.18.1, which offers ``get_all``, ``rows_as_dict``,
  ``get_as_arrow`` and this cursor pair — and no ``get_as_list``.
  """

  def __init__(self, rows):
    self._rows = list(rows)
    self._i = 0

  def has_next(self):
    return self._i < len(self._rows)

  def get_next(self):
    row = self._rows[self._i]
    self._i += 1
    return row


@pytest.mark.unit
class TestResultRows:
  def test_reads_every_row_in_order(self):
    result = FakeQueryResult([["a", 1], ["b", 2], ["c", 3]])

    assert result_rows(result) == [["a", 1], ["b", 2], ["c", 3]]

  def test_empty_result_is_an_empty_list(self):
    assert result_rows(FakeQueryResult([])) == []

  def test_exhausts_the_cursor(self):
    """A partial read would silently truncate a caller's row count."""
    result = FakeQueryResult([[1], [2], [3]])

    result_rows(result)

    assert not result.has_next()

  def test_returns_rows_unnormalized(self):
    """Callers distinguish tuple rows from dict rows (TABLE_INFO does), so
    the helper must not coerce either into the other."""
    rows = [("tuple", 1), {"dict": 2}, ["list", 3]]

    assert result_rows(FakeQueryResult(rows)) == rows


@pytest.mark.unit
class TestDoesNotDependOnGetAsList:
  """The regression. ``get_as_list`` does not exist on the engine's result;
  reading through it — or branching on it — yields nothing at all."""

  def test_reads_a_result_that_has_no_get_as_list(self):
    result = FakeQueryResult([["only-row"]])
    assert not hasattr(result, "get_as_list")

    assert result_rows(result) == [["only-row"]]

  def test_never_reaches_for_get_as_list(self):
    """Locks the guard out: touching that name at all is the old bug."""

    class TrapResult(FakeQueryResult):
      def __getattr__(self, name):
        if name == "get_as_list":
          raise AssertionError(
            "result_rows must not branch on get_as_list — the engine has no "
            "such method, so the branch never fires and the fallback is what "
            "actually runs"
          )
        raise AttributeError(name)

    assert result_rows(TrapResult([["row"]])) == [["row"]]
