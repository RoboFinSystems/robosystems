# Contributing to RoboSystems

Thanks for your interest in contributing. This repository is the API and backend service for the RoboSystems platform.

- **[Discussions](https://github.com/orgs/RoboFinSystems/discussions)** — questions, ideas, and general conversation
- **[Project Board](https://github.com/orgs/RoboFinSystems/projects/3)** — work tracked across all RoboSystems repositories
- **[Wiki](https://github.com/RoboFinSystems/robosystems/wiki)** — architecture docs and guides

## Table of Contents

- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [How to Contribute](#how-to-contribute)
- [Licensing and Certification](#licensing-and-certification)
- [Branching](#branching)
- [Coding Standards](#coding-standards)
- [Testing](#testing)
- [Documentation](#documentation)
- [Pull Requests](#pull-requests)
- [CloudFormation](#cloudformation)
- [Security](#security)

## Getting Started

1. **Fork the repository** on GitHub.
2. **Clone your fork**:
   ```bash
   git clone https://github.com/YOUR-USERNAME/robosystems.git
   cd robosystems
   ```
3. **Add the upstream remote**:
   ```bash
   git remote add upstream https://github.com/RoboFinSystems/robosystems.git
   ```

## Development Setup

### Prerequisites

- Docker and Docker Compose
- [`uv`](https://docs.astral.sh/uv/) for Python packages and versions
- [`just`](https://just.systems) as the command runner

```bash
brew install uv just jq
```

### Bring up the stack

```bash
just init     # sets up the local Python environment (uv reads .python-version)
just start    # starts the stack on the `robosystems` Docker profile
just test     # confirm the setup works
```

`just start` creates `.env` and `.env.local` from the `.example` templates if they are missing. `.env` holds container hostnames for services talking to each other; `.env.local` holds localhost URLs for commands run on the host (justfile recipes, migrations, scripts). Adding a secret or changing a port usually means editing both.

### Conventions that trip people up

- **Every Python command runs through `uv`** — `uv run pytest`, `uv run ruff check`. A bare `python`/`pytest` may pick up the system interpreter.
- **Use the `robosystems` Docker profile**, which is what `just start` defaults to. Starting an individual service profile leaves you with a partial stack.
- **Never hand-write a migration.** Update the SQLAlchemy model first, then autogenerate with `just migrate-create "description"`, then review the generated file — autogenerate misses enum changes, CHECK constraints, and some index changes.
- **Two databases, two migration histories.** The platform database is the default; pass `extensions` as the second argument to operate on the extensions database (`just migrate-up extensions`).
- **Local API testing uses `X-API-Key`**, not a bearer token. `just demo-user` writes a key to `.local/config.json`.
- **Read the README in a directory before working in it** — each package documents its own patterns.

## How to Contribute

### Issue types

| Type        | When to use                                           |
| ----------- | ----------------------------------------------------- |
| **Bug**     | Defects or unexpected behavior                        |
| **Task**    | Specific, bounded work that fits in one PR            |
| **Feature** | Request a new capability (no design required)         |
| **RFC**     | Propose a design for discussion before implementation |
| **Spec**    | Approved implementation plan ready for execution      |

For a larger change, the path is Feature (capture the need) → RFC (propose and discuss the design) → Spec (record the approved plan).

### Reporting bugs

Check existing issues first, then include steps to reproduce, expected versus actual behavior, environment details (OS, Python version), and any relevant logs.

### First-time contributors

Issues labeled `good first issue` or `help wanted` are good starting points.

## Licensing and Certification

### Licensing

RoboSystems is licensed under Apache 2.0 and will stay that way. Contributions are accepted under the same license — as Apache 2.0 section 5 puts it, any contribution you intentionally submit for inclusion is licensed under those terms unless you state otherwise. Inbound equals outbound.

There is no contributor license agreement to sign and we do not ask you to assign copyright. You keep it.

### Certification

The pull request template carries a certification checkbox. Tick it when you open the PR:

> I have the right to submit this work under the Apache 2.0 license, and do so. Where any part of it is owned by my employer, I have their permission.

That is an assertion about **provenance** — that the work is yours to give — not a transfer of rights. If part of what you are contributing is owned by your employer, or came from a project under a different license, resolve that before opening the PR rather than after.

We ask for this once per pull request rather than as a `Signed-off-by` trailer on every commit. The obligation is identical; the ceremony is lighter, and it does not send you back through a rebase to satisfy a bot.

### Commit signing

Merges into `main` are performed by a maintainer and land as signed merge commits, so every change reaching the default branch carries a verified signature and an identified approver.

Signing your own commits is welcome but not required. If you want them to show as Verified, register your signing key on your GitHub account — see [commit signature verification](https://docs.github.com/en/authentication/managing-commit-signature-verification).

## Branching

**Contributing from a fork.** External contributors work from a fork and open a pull request against this repository. You do not need push access here, and we do not grant it — a fork gives you everything required to contribute, and your commits keep your authorship when they merge. Branch in your own fork:

```bash
git switch -c feature/add-user-auth
```

**With push access.** Maintainers create branches with the project tooling, which branches from `origin/<base>` and sets the upstream correctly:

```bash
just create-feature feature add-user-auth main
just create-feature bugfix fix-connection-timeout main
```

Either way, the first path segment is the branch type: `feature` (new capability), `bugfix` (fix to existing behavior), `hotfix` (urgent fix), `chore` (dependencies, config), or `refactor` (no functional change). All pull requests target `main`.

## Coding Standards

- **Python 3.13**, with dependencies managed by uv
- **Ruff** for formatting and linting (88-character lines, double quotes) and import sorting
- **basedpyright** for type checking; add type hints to every function signature
- **Self-documenting code** — prefer clear names over comments, and comment only non-obvious logic
- **No emojis** in production code or logs; they belong only in the interactive scripts under `examples/`

```bash
just lint fix    # autofix what Ruff can
just lint        # verify linting
just format      # verify formatting
just typecheck   # basedpyright
just test-code   # all of the above, matching the git hooks
```

### Commit messages

Follow [Conventional Commits](https://www.conventionalcommits.org/): `type(scope): subject`, with an optional body and footer. Types: `feat`, `fix`, `docs`, `style`, `refactor`, `test`, `chore`, `perf`.

```
feat(api): add portfolio analysis endpoint
fix(graph): resolve connection pooling issue
docs(readme): update deployment instructions
```

## Testing

New features need tests; bug fixes need a regression test. See [tests/README.md](/tests/README.md) for fixtures, markers, and how to write a test in this repo.

```bash
just test                   # unit tests (excludes slow tests)
just test adapters          # tests under tests/adapters/
just test-all               # tests plus lint, format, typecheck, CloudFormation lint
just test-cov               # tests with a coverage report

uv run pytest -m unit                              # by marker
uv run pytest tests/middleware/billing/test_enforcement.py   # one file
```

`just test <module>` takes a path *relative to `tests/`*, so use `just test adapters`, not `just test tests/adapters`. To run an arbitrary path, call pytest directly.

Mark tests with `@pytest.mark.unit`, `@pytest.mark.integration`, `@pytest.mark.slow`, or `@pytest.mark.security` as appropriate, mock external dependencies, and give tests names that state the behavior being asserted.

## Documentation

- Update the README for user-visible changes, and the relevant package README for changes to its patterns
- Add docstrings to new functions and classes
- Keep configuration examples current when adding environment variables

API documentation is generated from the FastAPI routes, so accurate type hints, response models, and route docstrings are what make it useful.

## Pull Requests

### Before opening one

```bash
git add <files>
git commit -m "feat: descriptive message"

git fetch origin
git rebase origin/main

just test-all

git push origin your-branch-name
```

### Opening the PR

```bash
gh pr create --base main --title "Your PR title" --body "Your PR description"
```

From a fork, push the branch to your own remote first and target this repository as the base:

```bash
git push origin your-branch-name
gh pr create --repo RoboFinSystems/robosystems --base main \
  --title "Your PR title" --body "Your PR description"
```

From a Claude Code session, the `/create-pr` slash command does the same thing, writing the description from the work in the session and cross-checking each claim against `git diff <target>...<branch>`.

### CI on fork pull requests

A first-time contributor's workflow runs need maintainer approval before they start, so expect CI to sit idle until someone approves it. Fork pull requests always run on GitHub-hosted runners and receive no repository secrets, so any job that depends on credentials will not run for them.

### Requirements

- Tests pass, and linting, formatting, and type checking are clean
- Coverage does not regress meaningfully
- Documentation is updated alongside behavior changes
- One feature or fix per PR
- The certification checkbox is ticked — see [Licensing and Certification](#licensing-and-certification)
- At least one maintainer approval before merge, and a maintainer performs the merge

Address review feedback with new commits rather than force-pushing, so reviewers can follow what changed.

## CloudFormation

Templates live in [`cloudformation/`](/cloudformation/README.md). Use YAML, describe every parameter, supply sensible defaults, use conditions for environment-specific resources, and tag all resources.

```bash
just cf-lint api        # lint and validate one template by name
just cf-lint-all        # lint every template (no AWS credentials needed)
```

`just cf-lint <name>` also calls `aws cloudformation validate-template`, which requires AWS credentials. Without them, use `just cf-lint-all`, which is what CI runs.

## Security

**Do not open a public issue for a security vulnerability.** Email security@robosystems.ai with the details and steps to reproduce, and allow time for a fix before public disclosure.

When contributing:

- Never commit secrets or credentials
- Keep sensitive configuration in environment variables
- Validate and sanitize all user input
- Keep dependencies current

## Questions

- **[GitHub Discussions](https://github.com/orgs/RoboFinSystems/discussions)** — questions and community conversation
- **[GitHub Issues](https://github.com/RoboFinSystems/robosystems/issues)** — bug reports and feature requests
- **security@robosystems.ai** — security issues only

Contributors are credited on the [contributors page](https://github.com/RoboFinSystems/robosystems/graphs/contributors).
