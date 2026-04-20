Run `just test-all` and systematically fix all failures to achieve 100% completion.

## Timeouts

Always use `timeout: 600000` (10 minutes) on Bash calls for `just test-all` and `just test`. The default 2-minute Bash timeout is too short for the full suite. CI has a 10-minute limit for the test step.

## Strategy

1. **Run full suite first**: `just test-all 2>&1 | grep -E "passed|failed|error:|FAILED|^= " | tail -20` to see pytest summary + any failures
2. **Fix by module**: When errors exist, use `just test <module>` (e.g., `just test routers`) to iterate faster on that module before re-running the full suite
3. **Fix in order**: Linting/formatting → Type errors → Test failures
4. **Stop when done**: Once `just test-all` passes completely, stop immediately. Do NOT run it again to "confirm".

## Output Handling

**CRITICAL: `just test-all` runs pytest FIRST, then lint, then typecheck.** With `| tail -N`, you only see the end (typecheck output) — the pytest summary scrolls away. Always use the grep pattern below:

```
just test-all 2>&1 | grep -E "passed|failed|error:|FAILED|warnings summary|^= " | tail -20
```

This captures: pytest result line ("X passed, Y failed"), any FAILED test names, and recipe error lines. The absence of "failed" or "FAILED" lines AND presence of "passed" means success — stop there.

For `just test <module>`, the output is short enough that `| tail -20` still works.

## Key Commands

- `just test-all` - Full suite (tests + lint + format + typecheck)
- `just test <module>` - Run tests in `/tests/<module>/` (e.g., `just test routers`)
- `just test` - Run all unit tests (excludes slow/integration)
- `just lint fix` - Auto-fix linting issues
- `just format` - Auto-fix formatting
- `just typecheck` - Run basedpyright

## Goal

100% pass rate on `just test-all` with no errors of any kind. Efficiency matters - don't re-run the full suite until you've fixed all known issues in a module.
