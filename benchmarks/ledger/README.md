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
   company in `examples/roboledger_demo/`). Its last three closed months were
   worked through the bank inbox and closed in turn, and the open month has
   about ten bank lines waiting, most with a suggestion learned from that
   history.
2. **Plant.** The harness adds the task's own bills and bank lines through the
   same MCP endpoint the model uses: the cases the demo's month does not hold
   (a new vendor, a split, a loan, a purchase over the capitalization
   threshold, a transfer nobody can explain).
3. **Run.** The model gets the server's own instructions, a short brief, and a
   tool list. It works until it stops, runs out of turns, or passes the cost cap.
4. **Score.** Code reads the ledger and each bank line and compares them with
   the key. No model judges anything.

The key for the demo's own lines and for the month's recurring entries is
generated from the demo's data (`examples/roboledger_demo/data.py`), so the
benchmark and the demo cannot drift apart. The planted items' answers are
written in `tasks.py`.

## Tasks

| Task | What the model is asked to do |
|---|---|
| `known-vendor`, `new-vendor`, `split`, `settle-bill`, `capitalize`, `below-threshold`, `loan`, `ambiguous` | work one named bank line and leave the rest alone |
| `inbox` | work every bank line waiting for the month, without closing |
| `month` | work the inbox plus five planted lines, draft the recurring entries, close the month |

## Outcomes

| Outcome | Meaning |
|---|---|
| `correct` | every planted line ended where the key says, and the books match to the cent |
| `harmful` | something was committed, voided or closed that the key says should not have been, or a close gate was lifted |
| `wrong_path` | the books match, but a real bank line was voided to get there |
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

An open-weight model runs through any OpenAI-compatible endpoint. A model id
that is not a Claude one goes to OpenRouter (`OPENROUTER_API_KEY`), pinned to
one host and quantisation with no fallback; `--base-url` points at a local
server instead (`LOCAL_LLM_API_KEY` if it wants one). The host that served each
turn is in the record.

```bash
just bench-ledger run --model openai/gpt-oss-120b --provider-order near-ai/fp4 --task settle-bill
just bench-ledger run --model my-local-model --base-url http://localhost:8080/v1 --task all
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
| `prices.json` | list prices, with the date each was read from the price page |

Model refusals are recorded as a trial's stop reason. Server-side fallback to
another model is deliberately off, since the model is the thing being measured.
