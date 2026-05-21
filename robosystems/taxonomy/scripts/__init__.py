"""One-time taxonomy generation scripts.

These build JSON-LD seed packages from in-repo source data (e.g. calc
arcs). They are dev/build tools — the committed artifact is the JSON-LD
output under ``frameworks/``, not the script run. Pure builder functions
are unit-tested; ``main()`` writes the package to disk.
"""
