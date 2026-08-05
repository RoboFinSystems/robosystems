Draft curated release notes for an upcoming milestone release, following the convention in `.github/release-notes/README.md`.

## Why this command exists

`tag-release.yml` generates release bodies from the changes since the last tag. That suits routine releases but reads poorly for milestones, where the story is what the version *is*. The curated-notes override has non-obvious rules (body-only format, the file must exist at the tagged ref), and release notes are the repo's most-read public text — this command encodes the review and hygiene checks that keep them accurate and safe to publish.

## Instructions

### 1. Decide whether to curate at all

Not every release deserves curated notes. Routine patch releases should keep the generated changelog — skipping is a normal outcome, not a failure. Curate when the release is a milestone: a minor, a headline capability, or a version the documentation will reference. If the user invoked this command for a plain patch, say so and confirm they still want curated notes.

### 2. Establish the version and the range

- The target version comes from the argument (e.g. `/release-notes 1.7.0`). If none was given, ask what version the user intends to tag — the filename must match the eventual tag exactly, and a mismatched file is silently ignored.
- **Never bump the version yourself.** `pyproject.toml` stays untouched; the user bumps during release prep.
- **The range depends on the release kind.** A minor memorializes the whole series since the *previous minor* (`vX.(Y-1).0..origin/main`) — patches got generated changelogs; the minor is the digest nobody gets from reading thirty of them. A curated patch or hotfix covers only the span since the last tag:

```bash
LAST=$(git tag --sort=-creatordate | head -1)          # patch: last tag
# minor: previous minor tag, e.g. v1.6.0 when cutting v1.7.0
git log "$RANGE_START"..origin/main --merges --format='%s'
gh pr list --state merged --limit 30 --json number,title,mergedAt
```

Note the generated links section will still compare against the last tag; the prose should state the span it covers (e.g. "since v1.6.0") explicitly.

### 3. Review the changes for real

Do not write notes from commit subjects alone. Read the PR bodies (`gh pr view <n>`) and spot-check diffs where the description is thin. For a series-scale minor (a hundred-plus PRs), group the merge subjects into themes first, then read the bodies of the load-bearing PRs per theme rather than all of them. Classify everything into features, fixes, infrastructure, and chores, then check specifically:

- **SDK contract impact.** `robosystems-python-client` and `robosystems-typescript-client` are post-1.0 semver contracts. Any breaking change to the public API surface (GraphQL schema, operations envelope, REST shapes) must be prominent in the notes and should already have been coordinated as a client major — if you find an uncoordinated break, stop and raise it. Additive API changes are worth a line noting the SDK regen opportunity.
- **Migrations.** New Alembic migrations (platform or extensions) mean the deploy has a migration step — note it.
- **CloudFormation changes.** Template changes mean a stack update must ride the deploy — note which stack.
- **OpenAPI surface.** New or changed endpoints, and whether they're schema-visible.

### 4. Security disclosure review

This repo is public, and release publication is decoupled from deployment — the notes are world-readable immediately. For any security-adjacent change:

- Keep the line at PR-title neutrality: what area was hardened, never how or against what.
- No exploit mechanics, no affected-endpoint enumerations, no detection signatures or thresholds, no "previously protected only by X" tells.
- Never paste content from private analysis documents into the notes.
- When in doubt, terser.

### 5. Write the file

Write `.github/release-notes/v<version>.md` — **body only**:

- No `# RoboSystems Service v<version>` heading, no release-statistics section, no links section, no generated-with footer. The workflow supplies all of those. Start at the first line of prose.
- The archived `v1.0.0`–`v1.6.0` files keep their original headings and footers — they are records, **not** templates for this format.
- Lead with one or two sentences saying what the version is. Then sections as warranted: key features, breaking changes (only if any truly exist), bug fixes, infrastructure. Ground every line in a change you actually reviewed.

### 6. Hand off — sequencing matters

The file must exist **at the tagged ref**, so it belongs in release prep alongside the version bump, committed before `create-release.yml` is dispatched. Never commit on `main` — the notes ride a feature branch (created via `just create-feature`) or the user's existing release-prep branch. Present the draft for review and leave the bump, merge, and dispatch to the user.
