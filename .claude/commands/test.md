---
description: Run the full test suite and code-quality gate, fixing failures to green.
argument-hint: "[module]"
---

Run `just test-all` and fix failures to 100% completion.

## Strategy

1. **Run the full gate**: `just test-all 2>&1 | grep -E "[0-9]+ passed|[0-9]+ failed|^FAILED |^error:|Recipe .* failed|warnings summary" | tail -20` — use `timeout: 600000` on the Bash call (the suite takes 5–6 min; the default 2-min Bash timeout kills it).
2. **Iterate per-module**: when fixing, use `just test <module>` (e.g. `just test routers`) for faster turnaround. Plain `| tail -20` works fine here — output is short.
3. **Stop when green**: don't re-run the full suite to "confirm" — once it passes, you're done.

## What `just test-all` actually runs

In order: `just test` (pytest) → `just test-dbt quickbooks` → `just lint fix` → `just lint` → `just format` → `just typecheck` → `just cf-lint-all` → `just lint-actions`.

That matters for reading the output: **only the first stage produces a pytest summary.** A dbt build failure, a ruff/basedpyright failure, or a CloudFormation/workflow lint failure will not produce a `failed` count — it surfaces as a recipe failure line instead. So a green pytest count alone is *not* proof the gate passed.

For the code-quality half without the ~6-minute test run, use `just test-code` — that's the combination the git hooks enforce.

## Why the grep

With plain `| tail -N` the pytest summary scrolls away behind the later stages' output. Each alternative anchors something specific:

- `[0-9]+ passed` — the summary count (`9785 passed,`) but NOT test names like `test_passed_validation`
- `[0-9]+ failed` — the failure count (`1 failed,`) but NOT names like `test_logs_failed_revocation`
- `^FAILED ` — line-start anchor catches failure headers without matching `PASSED` lines whose name contains `failed`
- `^error:` and `Recipe .* failed` — just's recipe-failure lines, which is how the non-pytest stages report
- `warnings summary` — pytest's warnings-section header

**Success = a `[0-9]+ passed` line, no `failed`/`FAILED`, AND no recipe-failure line.** All three conditions, not just the first.
