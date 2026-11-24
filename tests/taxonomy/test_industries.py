"""The industry classification files hold their own rules."""

import csv
from collections import Counter

import pytest

from robosystems.taxonomy import industries


@pytest.mark.unit
def test_files_are_clean():
  assert industries.check() == []


@pytest.mark.unit
def test_every_level_is_populated():
  levels = Counter(r["level"] for r in industries.load_industries().values())
  assert set(levels) == set(industries.LEVELS)


@pytest.mark.unit
def test_lineage_and_labels():
  node = "health-care/biotechnology/clinical-stage"
  assert industries.lineage(node) == [
    "health-care",
    "health-care/biotechnology",
    node,
  ]
  assert industries.labels(node) == ["Health Care", "Biotechnology", "Clinical Stage"]


@pytest.mark.unit
def test_a_carried_industry_is_shown_once():
  carried = next(
    r for r in industries.load_industries().values() if r["carried"] == "yes"
  )
  assert industries.labels(carried["id"]) == industries.labels(carried["parent"])


@pytest.mark.unit
def test_resolve():
  assert industries.resolve("health-care") == "health-care"
  assert industries.resolve("not/a/node") is None


@pytest.mark.unit
def test_resolve_follows_a_retired_node(monkeypatch):
  rows = {
    "a": {"id": "a", "replaced_by": ""},
    "a/old": {"id": "a/old", "replaced_by": "a/new"},
    "a/new": {"id": "a/new", "replaced_by": ""},
  }
  monkeypatch.setattr(industries, "load_industries", lambda: rows)
  assert industries.resolve("a/old") == "a/new"


@pytest.mark.unit
def test_check_reports_a_bad_row(tmp_path, monkeypatch):
  nodes = tmp_path / "industries.csv"
  nodes.write_text(
    "id,parent,level,label,carried,replaced_by\n"
    "a,,sector,A,,\n"
    "a/b,a,subsector,B,,\n"
    "a/b/c,a/b,industry,C,,\n"
    "a/b/gone,a/b,industry,Gone,,a/b/missing\n"
  )
  companies = tmp_path / "companies.csv"
  companies.write_text(
    "key,cik,name,industry,primary,method,confidence,review\n"
    "ONE,0000000001,One,a/b/c,yes,filing,high,\n"
    "TWO,0000000002,Two,a,yes,filing,high,\n"
    "THREE,0000000003,Three,a/b/c,,guess,high,\n"
  )
  monkeypatch.setattr(industries, "INDUSTRIES_CSV", nodes)
  monkeypatch.setattr(industries, "COMPANIES_CSV", companies)
  problems = "\n".join(industries.check())
  assert "a/b/gone: replaced_by must name an active node" in problems
  assert "TWO: attach at an industry or deeper" in problems
  assert "THREE: method 'guess'" in problems
  assert "THREE: needs exactly one primary industry, has 0" in problems
  assert "ONE" not in problems


@pytest.mark.unit
def test_the_published_file_carries_no_notes():
  with industries.COMPANIES_CSV.open(encoding="utf-8", newline="") as f:
    header = next(csv.reader(f))
  assert header == [
    "key",
    "cik",
    "name",
    "industry",
    "primary",
    "method",
    "confidence",
    "review",
  ]
