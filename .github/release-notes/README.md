# Curated release notes

`tag-release.yml` generates the GitHub release body from a Claude-written
changelog of the changes since the last tag. That framing suits routine
releases but reads poorly for a milestone, where the story is what the version
*is* rather than what changed since last Tuesday.

To override it, commit the notes here as `v<version>.md` **before** dispatching
`create-release.yml`. The file has to exist at the tagged ref, and
`create-release.yml` bumps the version on `main`, cuts `release/<version>` from
the result, and tags it in the same run — so the notes must be merged to `main`
before the dispatch, not added to the release branch afterwards. When the file
is present the workflow uses it verbatim and skips the generated changelog, the
release-statistics section, and the generated-with footer.

No file, no change: the release falls back to the generated changelog. Skipping
a release is a normal outcome, not a failure.

## Writing a new one

Write the body only. The workflow supplies the `# RoboSystems Service v<version>`
heading and the links section, so don't repeat them here — start at the first
line of prose.

The filename is version-specific on purpose: a leftover file can never be picked
up by a later release.

## Archived notes

Every minor release from `v1.0.0` through `v1.6.0` predates this mechanism.
Their files are the release bodies as published on GitHub, copied here verbatim
so the writing lives in the repo rather than only in GitHub's database
(`v1.4.0`/`v1.5.0` were archived first; the rest were backfilled 2026-08-04).
They keep their original headings and trailing links, so treat them as records
rather than as templates for the format above. None of them can affect a
release — their tags already exist, so the workflow short-circuits before
reading them.
