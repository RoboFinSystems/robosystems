"""Credentials must never reach a tenant-visible error or the logs.

Regression for the tier-B robustness review §2.8: the extensions materializer
interpolates a libpq connstr (`password=...`) into every postgres_scan()
statement; a DuckDB error that echoes the failing statement would otherwise
carry the RDS master credential into `result.errors` and the graph_api logs.
"""

from __future__ import annotations

import pytest

from robosystems.security.error_handling import redact_connection_secrets


@pytest.mark.unit
@pytest.mark.security
class TestRedactConnectionSecrets:
  def test_redacts_libpq_password_keeps_diagnostics(self):
    raw = (
      "Failed to stage Entity: Parser Error near "
      "postgres_scan('dbname=extensions user=postgres password=hunter2 "
      "host=pg.internal port=5432', 'kg1a2b', 'entities')"
    )
    out = redact_connection_secrets(raw)
    assert "hunter2" not in out
    assert "password=***" in out
    # The useful diagnostic text survives.
    assert "Failed to stage Entity" in out
    assert "dbname=extensions" in out

  def test_redacts_url_form_credentials(self):
    raw = "connect failed: postgresql://postgres:s3cr3t@rds.aws:5432/extensions"
    out = redact_connection_secrets(raw)
    assert "s3cr3t" not in out
    assert "postgresql://postgres:***@rds.aws:5432/extensions" in out

  def test_empty_and_secretless_text_pass_through(self):
    assert redact_connection_secrets("") == ""
    assert redact_connection_secrets("plain error, no creds") == "plain error, no creds"

  def test_multiple_occurrences_all_redacted(self):
    raw = "password=a failed; retry with password=b also failed"
    out = redact_connection_secrets(raw)
    assert "password=a" not in out
    assert "password=b" not in out
    assert out.count("password=***") == 2
