"""Report collection for the isolation harness.

Accumulates one finding per (surface, attacker, target) probe and renders a
machine-readable JSON file plus a human summary. The JSON is the artifact
attached to SOC 2 evidence and handed to the paid gray-box tester as the
scope fixture; the summary is what a run prints.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field

from ._oracle import Verdict


@dataclass
class Finding:
  axis: str  # "horizontal" | "vertical"
  surface: str  # "rest-read" | "rest-write" | "cypher" | "mcp" | "graphql"
  method: str
  path: str
  attacker: str  # principal label
  target_graph: str
  verdict: Verdict
  status: int
  note: str = ""


@dataclass
class Report:
  findings: list[Finding] = field(default_factory=list)

  def add(self, f: Finding) -> None:
    self.findings.append(f)

  def by_verdict(self, v: Verdict) -> list[Finding]:
    return [f for f in self.findings if f.verdict == v]

  @property
  def leaks(self) -> list[Finding]:
    return self.by_verdict(Verdict.LEAK)

  @property
  def invalid(self) -> list[Finding]:
    return self.by_verdict(Verdict.INVALID)

  @property
  def inconclusive(self) -> list[Finding]:
    return self.by_verdict(Verdict.INCONCLUSIVE)

  def counts(self) -> dict[str, int]:
    out = {v.value: 0 for v in Verdict}
    for f in self.findings:
      out[f.verdict.value] += 1
    return out

  def summary(self) -> str:
    c = self.counts()
    lines = [
      "",
      "══════════ tenant-isolation harness ══════════",
      f"  probes: {len(self.findings)}   "
      + "   ".join(f"{k}={v}" for k, v in c.items() if v),
    ]
    for label, items in (
      ("LEAK", self.leaks),
      ("INVALID", self.invalid),
      ("INCONCLUSIVE", self.inconclusive),
    ):
      for f in items:
        lines.append(
          f"  [{label}] {f.axis}/{f.surface} {f.method} {f.path} "
          f"as {f.attacker} → {f.status}: {f.note}"
        )
    lines.append("═══════════════════════════════════════════════")
    return "\n".join(lines)

  def write_json(self, path: str) -> None:
    payload = {
      "counts": self.counts(),
      "findings": [{**asdict(f), "verdict": f.verdict.value} for f in self.findings],
    }
    with open(path, "w") as fh:
      json.dump(payload, fh, indent=2)
