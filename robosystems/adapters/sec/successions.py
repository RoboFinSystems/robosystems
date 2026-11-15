"""Registrants that took over a ticker from another: who succeeded whom, and
who merely holds a symbol someone else gave up.

A CIK names a registrant, and a company can change registrant while staying
the same company: a holding-company reorganization, a redomicile, a
second-step conversion. EDGAR's ticker map then lists the symbol under the new
CIK and carries no link to the old one. The successor says so itself, on Form
8-K12B or 8-K12G3 under Exchange Act Rule 12g-3, but the form's metadata does
not name the predecessor, so the link is recorded here by hand, each row with
the filing that states it.

A `Succession` joins the two registrants' filings under the one ticker. A
`Reassignment` records the opposite finding, that the new holder is another
company (a spin-off, a new shell on an old symbol, a symbol reused), so the
former holder gives the ticker up and nothing is joined. A pair in neither
table is left as it is and reported.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Succession:
  """``successor`` is the successor issuer to ``predecessor`` from
  ``effective``; ``evidence`` is the accession of the filing that says so."""

  ticker: str
  predecessor: str
  successor: str
  effective: str
  evidence: str


@dataclass(frozen=True)
class Reassignment:
  """``former`` no longer holds ``ticker``, and ``holder`` is not its
  successor; ``basis`` says what happened."""

  ticker: str
  former: str
  holder: str
  basis: str


SUCCESSIONS: tuple[Succession, ...] = (
  Succession("XOM", "0000034088", "0002115436", "2026-07-01", "0001193125-26-291990"),
  Succession("NAVI", "0001593538", "0002152623", "2026-10-29", "0001193125-26-436899"),
  Succession("DMRC", "0001438231", "0002119322", "2026-05-18", "0001437749-26-017527"),
  Succession("CLBK", "0001723596", "0002115119", "2026-07-21", "0001193125-26-309602"),
  Succession("XPRO", "0001575828", "0002126198", "2026-07-13", "0001104659-26-083106"),
  # Successor to Real Brokerage and to RE/MAX; the symbol carries Real's history.
  Succession("REAX", "0001862461", "0002136387", "2026-08-24", "0001104659-26-100334"),
  # The successor is the registrant that filed as Brookfield Business Partners.
  Succession("BBUC", "0001871130", "0001654795", "2026-03-27", "0001104659-26-035594"),
  Succession("CBAT", "0001117171", "0002086841", "2026-06-23", "0001213900-26-071131"),
  Succession("CDLR", "0001978867", "0002063345", "2026-11-05", "0001104659-26-122941"),
  Succession("GORO", "0001160791", "0001515964", "2026-07-20", "0001104659-26-085075"),
  Succession("UROY", "0001711570", "0002143673", "2026-07-28", "0001493152-26-034947"),
  # No 8-K12B: a Rule 414 post-effective amendment, "as successor issuer".
  Succession("DTSS", "0001631282", "0002110423", "2026-04-15", "0001213900-26-089077"),
)

REASSIGNMENTS: tuple[Reassignment, ...] = (
  Reassignment("NVRI", "0000045876", "0002104052", "spin-off registered on Form 10"),
  Reassignment("RNA", "0001599901", "0002093101", "spin-off registered on Form 10"),
  Reassignment("MTAL", "0001950246", "0002107724", "a new shell on the old symbol"),
  Reassignment("HYAC", "0001970509", "0002111838", "a new shell on the old symbol"),
  Reassignment("FTW", "0002021042", "0002083125", "the shell's merger partner"),
  Reassignment("XRPN", "0002044009", "0002092592", "the shell's merger partner"),
  Reassignment("ARIS", "0001865187", "0001964504", "unrelated; symbol reused"),
  Reassignment("KUST", "0001849058", "0001342958", "unrelated; symbol reused"),
  Reassignment("SUPN", "0001356576", "0001625297", "merged into the holder"),
  Reassignment("CD", "0001807192", "0001527762", "unrelated; symbol reused"),
)


def predecessors_of(
  cik: str, successions: tuple[Succession, ...] = SUCCESSIONS
) -> list[str]:
  """The registrants ``cik`` succeeded, nearest first, through every link."""
  by_successor: dict[str, list[str]] = {}
  for row in successions:
    by_successor.setdefault(row.successor, []).append(row.predecessor)
  out: list[str] = []
  queue = [cik]
  while queue:
    for predecessor in by_successor.get(queue.pop(0), []):
      if predecessor not in out and predecessor != cik:
        out.append(predecessor)
        queue.append(predecessor)
  return out


def recorded_pairs() -> set[tuple[str, str]]:
  """Every ``(old, new)`` pair either table has a finding for."""
  return {(r.predecessor, r.successor) for r in SUCCESSIONS} | {
    (r.former, r.holder) for r in REASSIGNMENTS
  }
