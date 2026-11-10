# Ledger operations benchmark

Measures whether a model can do a month of bookkeeping on RoboLedger through the
MCP tools, and what it costs. A company with a year of history is loaded, new
activity is planted in the open month, the model works it, and the ledger is
compared with an answer key written before the run.

This is a product evaluation: the company, the answer key and the tools are all
ours. It is not part of the Docker image.

**Status: a shakedown harness.** It runs and scores; nothing here is a published
result. The tasks, rows and prices are provisional.

## How a trial works

1. **Load.** `just demo-roboledger` reloads Cascade Advisory (the synthetic
   company in `examples/roboledger_demo/`), closed through the prior month.
2. **Plant.** The harness enters the task's bills and lands its bank lines
   unclassified, through the same MCP endpoint the model uses.
3. **Run.** The model gets the server's own instructions, a short brief, and a
   tool list. It works until it stops, runs out of turns, or passes the cost cap.
4. **Score.** Code reads the ledger and each planted line and compares them with
   the key. No model judges anything.

## Outcomes

| Outcome | Meaning |
|---|---|
| `correct` | every planted line ended where the key says, and the books match to the cent |
| `harmful` | something was committed, voided or closed that the key says should not have been |
| `abstained` | the model finished and left work open |
| `incomplete` | the model was cut off (turns, cost cap, error) with work open |

`harmful` is reported on its own and never netted against `correct`: a wrong
posting costs more than an open one.

## Rows

| Row | Tools the model holds |
|---|---|
| `C` | the full surface as shipped |
| `D` | the full surface without the preview and review tools |
| `F` | a fixed list of the tools a close uses |

## Running it

Needs the local stack (`just start`) and Anthropic credentials in the
environment (`ANTHROPIC_API_KEY`, or an `ant auth login` profile). The harness
refuses any target that is not local, because it writes to the ledger.

```bash
just bench-ledger tools                      # size each row's tool list, dump what the model receives
just bench-ledger run --dry-run              # load, plant and score with no model (free)
just bench-ledger run --task known-vendor --row F
just bench-ledger run --model claude-haiku-5-5 --task all --row C,F --k 3
```

Every trial reloads the company, about a minute each, and replaces whatever is
in the local Cascade demo graph. After a stack reset, wait for the extensions
database migrations to finish before the first run; the health check goes green
earlier.

Each trial writes a full record (prompt, tool list size, every tool call, token
usage per turn, the score) under `runs/`, which is git-ignored.

## Layout

| File | Holds |
|---|---|
| `tasks.py` | the planted items, their expected results, the brief |
| `score.py` | the comparison of ledger and key |
| `rows.py` | the tool lists |
| `fixture.py` | loading the company and planting a task |
| `loop.py`, `providers.py` | the model loop and the provider route |
| `mcp.py` | the MCP client |
| `prices.json` | list prices; rows marked `verified: false` need checking before a published run |

Model refusals are recorded as a trial's stop reason. Server-side fallback to
another model is deliberately off, since the model is the thing being measured.
