#!/usr/bin/env python3
"""Guard uv.lock against the two traps of the release cooldown.

Re-locking under `exclude-newer` filters out a fresh version already in the lock,
so a security fix can roll back without anyone asking for it. A package whose
locked version went down fails this check, unless its own constraint changed,
which makes the move deliberate.

An `exclude-newer-package` entry is only needed until its release ages past the
cooldown. Entries that have are reported so they can be dropped, and never fail
the check, so the passage of time alone cannot turn a build red.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import tomllib
from datetime import UTC, datetime, timedelta
from pathlib import Path

from packaging.version import Version

LOCK_FILE = Path(__file__).resolve().parents[2] / "uv.lock"
PROJECT = "robosystems"
PERMANENT_EXEMPTIONS = {"ladybug", "xbrlkit", "robosystems-client"}


def locked_versions(lock: dict) -> dict[str, Version]:
  """The highest locked version of each package, since a fork can lock several."""
  versions: dict[str, Version] = {}
  for package in lock.get("package", []):
    if "version" in package:
      version = Version(package["version"])
      versions[package["name"]] = max(version, versions.get(package["name"], version))
  return versions


def declared_constraints(lock: dict) -> dict[str, set[str]]:
  """Specifiers this project declares per package, overrides included."""
  entries = list(lock.get("manifest", {}).get("overrides", []))
  for package in lock.get("package", []):
    if package["name"] == PROJECT:
      metadata = package.get("metadata", {})
      entries += metadata.get("requires-dist", [])
      for group in metadata.get("requires-dev", {}).values():
        entries += group
  constraints: dict[str, set[str]] = {}
  for entry in entries:
    constraints.setdefault(entry["name"], set()).add(entry.get("specifier", ""))
  return constraints


def find_downgrades(base: dict, head: dict) -> list[tuple[str, Version, Version]]:
  """Packages whose version fell while their declared constraint stayed the same."""
  before, after = locked_versions(base), locked_versions(head)
  base_constraints = declared_constraints(base)
  head_constraints = declared_constraints(head)
  return [
    (name, before[name], after[name])
    for name in sorted(before.keys() & after.keys())
    if after[name] < before[name]
    and base_constraints.get(name) == head_constraints.get(name)
  ]


def _newest_upload(package: dict) -> datetime | None:
  files = list(package.get("wheels", []))
  if "sdist" in package:
    files.append(package["sdist"])
  uploads = [
    datetime.fromisoformat(file["upload-time"])
    for file in files
    if "upload-time" in file
  ]
  return max(uploads, default=None)


def spent_exemptions(lock: dict, now: datetime) -> list[str]:
  """Exempted packages whose locked release has aged past the cooldown."""
  options = lock.get("options", {})
  span = re.fullmatch(r"P(\d+)D", options.get("exclude-newer-span", ""))
  if span is None:
    return []
  cooldown = timedelta(days=int(span.group(1)))
  exempt = set(options.get("exclude-newer-package", {})) - PERMANENT_EXEMPTIONS
  spent = set()
  for package in lock.get("package", []):
    if package["name"] in exempt:
      uploaded = _newest_upload(package)
      if uploaded is not None and now - uploaded >= cooldown:
        spent.add(package["name"])
  return sorted(spent)


def _read_base(ref: str) -> dict:
  shown = subprocess.run(
    ["git", "show", f"{ref}:uv.lock"],
    capture_output=True,
    text=True,
    check=True,
    cwd=LOCK_FILE.parent,
  )
  return tomllib.loads(shown.stdout)


def main(argv: list[str] | None = None) -> int:
  parser = argparse.ArgumentParser(description=__doc__)
  parser.add_argument("--base", default="HEAD", help="git ref to compare uv.lock with")
  parser.add_argument(
    "--annotate", action="store_true", help="emit GitHub Actions annotations"
  )
  args = parser.parse_args(argv)
  error, notice = ("::error::", "::notice::") if args.annotate else ("", "")

  head = tomllib.loads(LOCK_FILE.read_text())
  for name in spent_exemptions(head, datetime.now(UTC)):
    print(f"{notice}{name}: past the cooldown, drop its exclude-newer-package entry")

  downgrades = find_downgrades(_read_base(args.base), head)
  for name, before, after in downgrades:
    print(
      f"{error}{name} fell from {before} to {after} with no constraint change. "
      f"If the cooldown filtered it, add `{name} = false` to exclude-newer-package, "
      f"then run `uv lock --upgrade-package {name}`.",
    )
  return 1 if downgrades else 0


if __name__ == "__main__":
  sys.exit(main())
