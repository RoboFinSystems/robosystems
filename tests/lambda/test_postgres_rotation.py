"""Tests for the PostgreSQL rotation Lambda's instance resolution."""

import pytest

SECRET_ARN = "arn:aws:secretsmanager:us-east-1:123456789012:secret:robosystems/prod/postgres-AbCdEf"


@pytest.mark.unit
def test_resolves_instance_by_exact_identifier(pgrot):
  info = pgrot.get_database_connection_info(SECRET_ARN, "prod")

  assert info["instance_id"] == "robosystems-prod"
  assert info["database"] == "robosystems"
  assert info["port"] == 5432
  assert info["host"]


@pytest.mark.unit
def test_refuses_secret_from_another_environment(pgrot):
  staging_arn = SECRET_ARN.replace("/prod/", "/staging/")

  with pytest.raises(ValueError, match="does not match"):
    pgrot.get_database_connection_info(staging_arn, "prod")


@pytest.mark.unit
def test_missing_instance_is_an_error(pgrot, monkeypatch):
  monkeypatch.setenv("DB_INSTANCE_IDENTIFIER", "robosystems-missing")

  with pytest.raises(ValueError, match="robosystems-missing"):
    pgrot.get_database_connection_info(SECRET_ARN, "prod")


@pytest.mark.unit
def test_unset_identifier_is_an_error(pgrot, monkeypatch):
  monkeypatch.delenv("DB_INSTANCE_IDENTIFIER")

  with pytest.raises(ValueError, match="DB_INSTANCE_IDENTIFIER"):
    pgrot.get_database_connection_info(SECRET_ARN, "prod")
