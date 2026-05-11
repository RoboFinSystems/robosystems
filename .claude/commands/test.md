Run `just test-all` and fix failures to 100% completion.

## Strategy

1. **Run full suite**: `just test-all 2>&1 | grep -E "[0-9]+ passed|[0-9]+ failed|^FAILED |^error:|warnings summary" | tail -20` — use `timeout: 600000` on the Bash call (full suite takes 5–6 min; the default 2-min Bash timeout kills it).
2. **Iterate per-module**: when fixing, use `just test <module>` (e.g. `just test routers`) for faster turnaround. Plain `| tail -20` works fine here — output is short.
3. **Stop when green**: don't re-run the full suite to "confirm" — once it passes, you're done.

## Why the grep

`just test-all` runs pytest → lint → typecheck in order. With plain `| tail -N` the pytest summary scrolls away behind typecheck output.

Each alternative in the grep pattern anchors something specific:

- `[0-9]+ passed` — matches the summary count (`9785 passed,`) but NOT test names like `test_passed_validation`
- `[0-9]+ failed` — matches the failure count (`1 failed,`) but NOT test names like `test_logs_failed_revocation`
- `^FAILED ` — line-start anchor catches failure headers (`FAILED tests/foo::test_bar`) without matching `PASSED` lines whose test name contains `failed`
- `^error:` — recipe failure lines
- `warnings summary` — pytest's warnings-section header

Absence of `failed`/`FAILED` plus a `[0-9]+ passed` line = success.
