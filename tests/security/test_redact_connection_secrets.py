"""Credentials must never reach a tenant-visible error or the logs.

Statements the materializer builds can carry connection credentials, so any
error text derived from them is scrubbed before it reaches `result.errors` or
the logs.
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

  def test_max_length_rds_password_still_redacted(self):
    # RDS master passwords max out at 128 chars; the bounded quantifiers
    # (256) must keep redacting the longest credential we can hold.
    secret = "x" * 128
    raw = (
      f"error in postgres_scan('user=postgres password={secret} host=h') "
      f"and postgresql://postgres:{secret}@rds.aws:5432/extensions"
    )
    out = redact_connection_secrets(raw)
    assert secret not in out
    assert "password=***" in out
    assert "postgresql://postgres:***@rds.aws" in out

  @pytest.mark.timeout(10)
  def test_adversarial_repetition_stays_fast(self):
    # Bounded quantifiers keep the URL regex linear-ish on input that never
    # terminates a match.
    raw = "postgres://!:" * 20_000
    assert redact_connection_secrets(raw) == raw
