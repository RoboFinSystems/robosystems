Run `just test-all` and systematically fix all failures to achieve 100% completion.

## Strategy

1. **Run full suite first**: `just test-all` to see the full picture
2. **Fix by module**: When errors exist, use `just test <module>` (e.g., `just test routers`) to iterate faster on that module before re-running the full suite
3. **Fix in order**: Linting/formatting → Type errors → Test failures
4. **Stop when done**: Once `just test-all` passes completely, stop immediately

## Key Commands

- `just test-all` - Full suite (tests + lint + format + typecheck)
- `just test <module>` - Run tests in `/tests/<module>/` (e.g., `just test routers`)
- `just test` - Run all unit tests (excludes slow/integration)
- `just lint fix` - Auto-fix linting issues
- `just format` - Auto-fix formatting
- `just typecheck` - Run basedpyright

## Goal

100% pass rate on `just test-all` with no errors of any kind. Efficiency matters - don't re-run the full suite until you've fixed all known issues in a module.
