"""The platform's JSON-LD context and xbrlkit's kernel context bind the same
IRIs to the same terms.

Two documents call themselves holons: the one this platform writes for a
report (``bundle.py`` compacts concept namespaces through
``CANONICAL_CONTEXT``) and the one xbrlkit writes for a filing. A reader
holds one context in mind for both, so a prefix the two bind differently
would make the same qname name two things. Nothing else detects the two
contexts drifting apart; this does, term by term.

xbrlkit's context carries terms the platform's never needed (label roles, the
filing's cover, the fact-level flags), and the platform's carries three of its
own. Those are listed, not asserted away, so a new one shows up here.
"""

from __future__ import annotations

from xbrlkit.serialize._kernel.context import CANONICAL_CONTEXT as XBRLKIT_CONTEXT

from robosystems.arelle.context import CANONICAL_CONTEXT as PLATFORM_CONTEXT

# Terms only the platform binds: its own catalogues, and the text-block
# content type xbrlkit carries on the fact instead.
PLATFORM_ONLY = {"contentType", "rs-driver", "rs-metric"}

# Shared terms whose IRI agrees but whose JSON-LD typing annotation differs:
# xbrlkit types `axis` / `member` as IRIs and leaves `stringValue` untyped.
# Harmless to a reader (the IRI is the meaning); recorded so a fourth is a
# decision, not a surprise.
ANNOTATION_DIFFERS = {"axis", "member", "stringValue"}


def _iri(term: object) -> object:
  return term["@id"] if isinstance(term, dict) else term


def test_every_shared_term_names_the_same_iri() -> None:
  shared = sorted(set(PLATFORM_CONTEXT) & set(XBRLKIT_CONTEXT))
  assert len(shared) > 100, "the two contexts no longer overlap as expected"
  drifted = {
    term: (PLATFORM_CONTEXT[term], XBRLKIT_CONTEXT[term])
    for term in shared
    if _iri(PLATFORM_CONTEXT[term]) != _iri(XBRLKIT_CONTEXT[term])
  }
  assert drifted == {}, f"terms bound to different IRIs: {drifted}"


def test_shared_terms_agree_beyond_the_recorded_annotations() -> None:
  shared = set(PLATFORM_CONTEXT) & set(XBRLKIT_CONTEXT)
  differing = {
    term for term in shared if PLATFORM_CONTEXT[term] != XBRLKIT_CONTEXT[term]
  }
  assert differing == ANNOTATION_DIFFERS


def test_the_platform_binds_nothing_xbrlkit_lacks_beyond_its_own_catalogues() -> None:
  assert set(PLATFORM_CONTEXT) - set(XBRLKIT_CONTEXT) == PLATFORM_ONLY
