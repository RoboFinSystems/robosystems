"""The platform's Arelle load: the cache directory it chooses and the
settings it hands xbrlkit. The load itself is xbrlkit's and tested there."""

import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from robosystems.adapters.sec.client import arelle

MODULE = "robosystems.adapters.sec.client.arelle"


@pytest.mark.unit
class TestCacheDir:
  def test_env_directory_when_set_and_writable(self, tmp_path, monkeypatch):
    chosen = tmp_path / "cache"
    monkeypatch.setattr(arelle.env, "ARELLE_CACHE_DIR", str(chosen))
    assert arelle.arelle_cache_dir() == chosen
    assert chosen.is_dir()

  def test_repo_cache_when_env_unset(self, tmp_path, monkeypatch):
    repo_cache = tmp_path / "repo-cache"
    monkeypatch.setattr(arelle.env, "ARELLE_CACHE_DIR", "")
    monkeypatch.setattr(arelle, "DEFAULT_CACHE_DIR", repo_cache)
    assert arelle.arelle_cache_dir() == repo_cache
    assert repo_cache.is_dir()

  def test_unwritable_directory_falls_back_and_is_seeded(self, tmp_path, monkeypatch):
    baked = tmp_path / "baked"
    schema = baked / "https" / "www.xbrl.org" / "2003" / "xbrl-instance-2003-12-31.xsd"
    schema.parent.mkdir(parents=True)
    schema.write_bytes(b"<xs:schema/>")
    fallback = tmp_path / "fallback"
    monkeypatch.setattr(arelle.env, "ARELLE_CACHE_DIR", str(baked))
    monkeypatch.setattr(arelle, "FALLBACK_CACHE_DIR", fallback)

    with patch(f"{MODULE}.os.access", return_value=False):
      chosen = arelle.arelle_cache_dir()

    assert chosen == fallback
    seeded = (
      fallback / "https" / "www.xbrl.org" / "2003" / "xbrl-instance-2003-12-31.xsd"
    )
    assert seeded.read_bytes() == b"<xs:schema/>"

  def test_uncreatable_directory_falls_back(self, tmp_path, monkeypatch):
    fallback = tmp_path / "fallback"
    monkeypatch.setattr(arelle.env, "ARELLE_CACHE_DIR", str(tmp_path / "nope"))
    monkeypatch.setattr(arelle, "FALLBACK_CACHE_DIR", fallback)
    real_mkdir = Path.mkdir

    def mkdir(self, *args, **kwargs):
      if self.name == "nope":
        raise OSError("read-only file system")
      return real_mkdir(self, *args, **kwargs)

    with patch(f"{MODULE}.Path.mkdir", mkdir):
      assert arelle.arelle_cache_dir() == fallback


@pytest.mark.unit
class TestLoadFiling:
  def test_passes_the_platforms_settings_to_xbrlkit(self, tmp_path, monkeypatch):
    monkeypatch.setattr(arelle.env, "ARELLE_CACHE_DIR", str(tmp_path / "cache"))
    monkeypatch.setattr(arelle, "ARELLE_WORK_OFFLINE", True)
    monkeypatch.setattr(arelle, "ARELLE_TIMEOUT", 12)
    model = MagicMock(name="ModelXbrl")
    with patch(f"{MODULE}.load_model", return_value=model) as load_model:
      assert arelle.load_filing("/tmp/filing.htm") is model

    kwargs = load_model.call_args.kwargs
    assert load_model.call_args.args == ("/tmp/filing.htm",)
    assert kwargs["cache_dir"] == tmp_path / "cache"
    assert kwargs["offline"] is True and kwargs["timeout"] == 12
    assert "User-Agent" in kwargs["config"].headers

  def test_dts_resolution_error_propagates(self, tmp_path, monkeypatch):
    from xbrlkit.parse import DtsResolutionError

    monkeypatch.setattr(arelle.env, "ARELLE_CACHE_DIR", str(tmp_path / "cache"))
    error = DtsResolutionError("/tmp/filing.htm", ["https://www.xbrl.org/gone.xsd"])
    with patch(f"{MODULE}.load_model", side_effect=error):
      with pytest.raises(DtsResolutionError) as excinfo:
        arelle.load_filing("/tmp/filing.htm")
    assert excinfo.value.unresolved == ["https://www.xbrl.org/gone.xsd"]


@pytest.mark.unit
class TestCloseFiling:
  def test_closes_the_model_and_its_controller(self):
    model = MagicMock(name="ModelXbrl")
    with patch(f"{MODULE}.close") as close:
      arelle.close_filing(model)
    model.close.assert_called_once()
    close.assert_called_once_with(model.modelManager.cntlr)

  def test_none_is_a_no_op(self):
    with patch(f"{MODULE}.close") as close:
      arelle.close_filing(None)
    close.assert_not_called()

  def test_a_failing_model_close_still_releases_the_controller(self):
    model = MagicMock(name="ModelXbrl")
    model.close.side_effect = RuntimeError("already closed")
    with patch(f"{MODULE}.close") as close:
      arelle.close_filing(model)
    close.assert_called_once()


def test_fallback_dir_is_under_tmp():
  assert str(arelle.FALLBACK_CACHE_DIR).startswith(os.sep + "tmp")
  assert isinstance(arelle.DEFAULT_CACHE_DIR, Path)
