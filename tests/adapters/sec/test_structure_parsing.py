"""Tests for structure definition parsing."""

from robosystems.adapters.sec.enrichment import parse_structure_definition


class TestParseStructureDefinition:
  def test_standard_format(self):
    number, stype, name = parse_structure_definition(
      "0001001 - Statement - CONSOLIDATED BALANCE SHEETS"
    )
    assert number == "0001001"
    assert stype == "Statement"
    assert name == "CONSOLIDATED BALANCE SHEETS"

  def test_disclosure(self):
    number, stype, name = parse_structure_definition(
      "0002001 - Disclosure - Organization"
    )
    assert number == "0002001"
    assert stype == "Disclosure"
    assert name == "Organization"

  def test_doubled_type(self):
    """Some filings have the type repeated: '995410 - Disclosure - Disclosure - ...'"""
    number, stype, name = parse_structure_definition(
      "995410 - Disclosure - Disclosure - Supplemental Balance Sheet Information"
    )
    assert number == "995410"
    assert stype == "Disclosure"
    assert name == "Supplemental Balance Sheet Information"

  def test_parenthetical(self):
    number, stype, name = parse_structure_definition(
      "0001002 - Statement - CONSOLIDATED BALANCE SHEETS [Parenthetical]"
    )
    assert number == "0001002"
    assert stype == "Statement"
    assert name == "CONSOLIDATED BALANCE SHEETS [Parenthetical]"

  def test_empty_string(self):
    number, stype, name = parse_structure_definition("")
    assert number is None
    assert stype is None
    assert name is None

  def test_none_input(self):
    number, stype, name = parse_structure_definition(None)
    assert number is None
    assert stype is None
    assert name is None

  def test_no_dash_separator(self):
    """Definition without ' - ' separators."""
    number, stype, name = parse_structure_definition("SomeRole")
    assert number is None
    assert stype is None
    assert name == "SomeRole"

  def test_two_parts_only(self):
    """Only two ' - ' separated parts (no name)."""
    number, stype, name = parse_structure_definition("001 - Statement")
    assert number is None
    assert stype is None
    assert name == "001 - Statement"

  def test_name_with_hyphens(self):
    """Names containing hyphens should be preserved."""
    number, stype, name = parse_structure_definition(
      "0003001 - Disclosure - Income Tax - Related Items"
    )
    assert number == "0003001"
    assert stype == "Disclosure"
    assert name == "Income Tax - Related Items"
