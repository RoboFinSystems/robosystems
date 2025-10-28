import json

import pytest
from rich.console import Console as RichConsole

from robosystems.utils import query_output


class ConsoleFactory:
  """Helper to capture Rich console output created by query_output."""

  def __init__(self):
    self.consoles = []

  def __call__(self, *args, **kwargs):
    console = RichConsole(
      record=True,
      force_terminal=True,
      width=120,
    )
    self.consoles.append(console)
    return console


def capture_console(monkeypatch):
  factory = ConsoleFactory()
  monkeypatch.setattr(query_output, "Console", factory)
  return factory


def test_format_value_handles_special_types():
  assert query_output.format_value(None) == "NULL"
  assert query_output.format_value({"a": 1}) == json.dumps({"a": 1})
  assert query_output.format_value(["x", "y"]) == json.dumps(["x", "y"])
  assert query_output.format_value(42) == "42"


def test_print_table_renders_rows(monkeypatch):
  factory = capture_console(monkeypatch)
  results = [
    {"name": "Alice", "score": 10},
    {"name": "Bob", "score": 12},
  ]

  query_output.print_table(results, title="Scores", row_count_label="Rows total")

  console_output = factory.consoles[0].export_text()
  assert "Scores" in console_output
  assert "Alice" in console_output and "Bob" in console_output
  assert "name" in console_output and "score" in console_output
  assert "Rows total: 2" in console_output


def test_print_table_handles_empty_results(monkeypatch):
  factory = capture_console(monkeypatch)

  query_output.print_table([], title="Empty Results", row_count_label="Rows total")

  console_output = factory.consoles[0].export_text()
  assert "Empty Results" in console_output
  assert "Rows total: 0" in console_output


def test_print_json_outputs_pretty(capsys):
  data = [{"id": 1, "value": "x"}]
  query_output.print_json(data)

  output = capsys.readouterr().out
  assert output.strip().startswith("[")
  assert '"id": 1' in output
  assert output.strip().endswith("]")


def test_print_csv_outputs_rows(capsys):
  rows = [{"col1": "value1", "col2": "value2"}, {"col1": "value3", "col2": "value4"}]

  query_output.print_csv(rows)

  output = capsys.readouterr().out.strip().splitlines()
  assert output[0] == "col1,col2"
  assert "value1,value2" in output
  assert "value3,value4" in output


def test_print_csv_with_no_rows(capsys):
  query_output.print_csv([])
  assert capsys.readouterr().out == "\n"


def test_info_section_and_field_helpers(monkeypatch):
  factory = capture_console(monkeypatch)

  query_output.print_info_section("Section Title", width=10)
  query_output.print_info_field("Field", ["a", "b"], indent=4)

  # Two consoles were created (one per helper call)
  section_output = factory.consoles[0].export_text()
  field_output = factory.consoles[1].export_text()

  assert "=" * 10 in section_output
  assert "Section Title" in section_output
  assert "Field:" in field_output
  assert "    " in field_output  # indent
  assert json.dumps(["a", "b"]) in field_output


@pytest.mark.parametrize(
  "func,icon,text",
  [
    (query_output.print_success, "✓", "All good"),
    (query_output.print_error, "✗", "Something failed"),
    (query_output.print_warning, "⚠", "Heads up"),
  ],
)
def test_status_helpers_render_icons(monkeypatch, func, icon, text):
  factory = capture_console(monkeypatch)

  func(text)

  console_output = factory.consoles[0].export_text()
  assert icon in console_output
  assert text in console_output
