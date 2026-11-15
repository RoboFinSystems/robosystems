"""Seals on records kept in Valkey, which graph instances can write."""

import pytest

from robosystems.security.sealed_records import is_sealed, seal, unseal


@pytest.mark.unit
class TestSealedRecords:
  def test_a_sealed_record_verifies_and_unseals_to_itself(self):
    record = {"b": [1, 2], "a": {"x": "y"}}
    sealed = seal("p", record)
    assert is_sealed("p", sealed)
    assert unseal("p", sealed) == record

  def test_a_changed_field_breaks_the_seal(self):
    sealed = seal("p", {"graph_id": "kg1"})
    sealed["graph_id"] = "kg2"
    assert not is_sealed("p", sealed)
    assert unseal("p", sealed) is None

  def test_a_seal_holds_for_one_purpose_only(self):
    assert not is_sealed("other", seal("p", {"a": 1}))

  @pytest.mark.parametrize("record", [{"a": 1}, {"a": 1, "seal": "00"}, None, "x"])
  def test_a_record_without_a_valid_seal_is_refused(self, record):
    assert not is_sealed("p", record)

  def test_resealing_replaces_the_earlier_seal(self):
    sealed = seal("p", {"attempt": 1})
    sealed["attempt"] = 2
    assert is_sealed("p", seal("p", sealed))
