Run `just test-all` and fix failures to 100% completion.

## Strategy

1. **Run full suite**: `just test-all 2>&1 | grep -E "passed|failed|error:|FAILED|warnings summary|^= " | tail -20` — use `timeout: 600000` on the Bash call (full suite takes 5–6 min; the default 2-min Bash timeout kills it).
2. **Iterate per-module**: when fixing, use `just test <module>` (e.g. `just test routers`) for faster turnaround. Plain `| tail -20` works fine here — output is short.
3. **Stop when green**: don't re-run the full suite to "confirm" — once it passes, you're done.

## Why the grep

`just test-all` runs pytest → lint → typecheck in order. With plain `| tail -N` the pytest summary scrolls away behind typecheck output. The grep filter captures: pytest result line ("X passed, Y failed"), any FAILED test names, and recipe error lines. Absence of `failed`/`FAILED` plus presence of `passed` = success.
