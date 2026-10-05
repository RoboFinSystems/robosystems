# Self-Hosted AI Models

The server-side AI operators can run on an open-weight model through any OpenAI-compatible server, so the platform's AI does not depend on one vendor. On a stack you run, both AI lanes can be pointed at open-weight models: the MCP clients that call your tools bring their own model, and the server-side operators can run on any server that implements the OpenAI Chat Completions API with tool calling.

> **This is for a deployment you run** — locally, in your own AWS account, or a dedicated deployment that asks for it. Hosted RoboSystems runs the operators on Claude through AWS Bedrock; `OPENAI_COMPAT_ENABLED` is off there and there is no model selector in the app.

## The Two Lanes

| | Who chooses the model | Where it runs |
| --- | --- | --- |
| **MCP clients** | The client does | Wherever the client runs. Any MCP client — including one driving an open-weight model — connects to your stack's MCP routes and calls the same tool surface. See [AI Operators & MCP](AI-Operators-and-MCP#connecting-from-mcp-clients). |
| **Operators** | Your deployment does | Your stack calls the model. By default that is Claude through Bedrock; this page covers pointing it somewhere else. |

Operators are the server-side AI: the chat assistant that answers questions by composing graph reads, the author operator behind the console's `/do` that makes a requested change through a narrow set of writes, and the chart-of-accounts mapping operator. The author operator always runs on the `quality` tier, so map that tier to move it. Everything else the platform does — ingestion, reporting, close, search — runs without a model at all.

## Configuring the Operators

Set these in `.env`, then run `just rebuild` (a plain `just restart` does not pick up environment changes):

```bash
OPENAI_COMPAT_ENABLED=true
OPENAI_COMPAT_BASE_URL=https://integrate.api.nvidia.com/v1   # or any OpenAI-compatible server
OPENAI_COMPAT_MODEL=z-ai/glm-5.3-flash
OPENAI_COMPAT_API_KEY=...                                    # blank if the server needs none
OPERATOR_PROFILE_ECONOMY=openai-compat
OPERATOR_PROFILE_BALANCED=openai-compat
OPERATOR_PROFILE_QUALITY=openai-compat
```

Turning the flag on registers one extra model, under the short name `openai-compat`. The three `OPERATOR_PROFILE_*` settings decide which of the platform's model tiers it backs. A tier you leave unmapped stays on Bedrock, so you can run the cheap tier on your own model and leave the rest alone, or map all three and never call Bedrock for a model.

| Setting | What it does |
| --- | --- |
| `OPENAI_COMPAT_ENABLED` | Registers the model. With it off, the `openai-compat` row does not exist, nothing can resolve to it, and the meter refuses it. In staging and production it is also readable as the SSM feature flag `features/OPENAI_COMPAT_ENABLED`. |
| `OPENAI_COMPAT_BASE_URL` | The API root. The client posts to `{base_url}/chat/completions`, so include the version segment (`…/v1`). |
| `OPENAI_COMPAT_MODEL` | The model id exactly as your server names it. It becomes the wire id and must not collide with a built-in model or tier name. |
| `OPENAI_COMPAT_API_KEY` | Sent as `Authorization: Bearer`. Leave it blank for a server that takes no key. |
| `OPENAI_COMPAT_MAX_OUTPUT_TOKENS` | Caps output tokens per call when your model's own limit is below what the platform asks for (the extended execution mode asks for 8,000). `0` means no cap of ours. |
| `OPENAI_COMPAT_TIMEOUT_SECONDS` | Per-call timeout, default 300. A model on modest hardware can take minutes for a question that runs several tool calls. |
| `OPENAI_COMPAT_CREDITS_PER_1K_INPUT` / `_OUTPUT` | What a call costs the caller's credit balance, per 1,000 tokens. Both default to `0`. |
| `OPERATOR_PROFILE_ECONOMY` / `_BALANCED` / `_QUALITY` | Which registered model backs each tier. Deployment-scoped; callers name a tier, never a model. |

### Where the Endpoint Can Be

Anything that speaks Chat Completions with tool calling works. Three shapes, in rough order of how much of it you run yourself:

- **A hosted catalog of open weights** — NVIDIA's API catalog (`https://integrate.api.nvidia.com/v1`) and OpenRouter (`https://openrouter.ai/api/v1`) both serve current open-weight models behind one key, which makes them the fastest way to try the lane. Your prompts leave your machine, so treat this as evaluation unless the terms say otherwise.
- **An inference server you run** — vLLM, or an NVIDIA NIM container on your own GPU. Nothing leaves your infrastructure; this is the configuration a sovereignty or data-residency requirement wants.
- **A model server on your workstation** — Ollama, LM Studio. From inside Docker your host is `host.docker.internal`, not `localhost`, so Ollama is `http://host.docker.internal:11434/v1`. Workstation-size models are the weakest option for this workload — see [Which Models Work](#which-models-work).

### Validation at Startup

Misconfiguration stops the stack at boot rather than failing on someone's first question:

- `OPENAI_COMPAT_ENABLED` on with an empty base URL or model.
- A tier pointed at a model name that is not registered, or at `openai-compat` while the flag is off.
- An `OPENAI_COMPAT_MODEL` that collides with a built-in model or tier name — model names resolve by short name, tier name, then wire id, so a collision would silently run and bill something else.

One thing that does *not* change: outside a `dev` stack the platform still checks its AWS credentials at startup, because the rest of it (object storage, the graph registries) runs on AWS regardless of which model answers.

## What the Client Sends

Internally the platform speaks one model-agnostic transcript format and translates at the edge, so the operators and the tool loop never know which provider answered. Useful if you are debugging your own endpoint:

- **Request** — `POST {base_url}/chat/completions` with `model`, `messages`, `max_tokens`, `tools` as `type: "function"` entries, and `tool_choice: "auto"`. `temperature` is included. Nothing else: no streaming, and no provider-specific body fields, so routing preferences that a catalog exposes through the request body cannot be set from here. Encode what you need in the model id instead.
- **Response** — the first choice is read. Text and `tool_calls` both carry over; a `<think>…</think>` block in the content is stripped, and a separate reasoning field is ignored rather than replayed. A tool call with no id gets one generated so the follow-up turn is well formed.
- **Usage** — `prompt_tokens`, `completion_tokens`, and `prompt_tokens_details.cached_tokens` when your server reports them. Cached tokens are counted separately from fresh input, the same way the Bedrock path counts them.
- **Prompt caching** — the platform sends no cache directives on this lane. A server that caches prefixes on its own still does, and its reported cache reads still land in usage.

The client is plain HTTP; it does not use the OpenAI SDK, and no vendor SDK is added to the stack by turning it on.

## Which Models Work

In September 2026 we ran the chat assistant against a demo ledger with twelve questions whose answers were computed independently from the graph, served through NVIDIA's API catalog, with Claude Sonnet 5 on Bedrock — the `balanced` tier's model at the time; it is Claude Sonnet 5.5 now — as the control:

| Model | Correct | Note |
| --- | --- | --- |
| Kimi K3 | 12 / 12 | |
| GLM 5.3 Flash | 12 / 12 | |
| GLM 5.3 | 11 / 11 answered | One question timed out at the endpoint's gateway |
| gpt-oss-20b | 10 / 12 | Missed a multi-step aggregation, and counted a cash account as an expense |
| Claude Sonnet 5 (control, Bedrock) | 12 / 12 | |

What to take from it: **frontier-size open weights, served from a GPU server or a hosted endpoint, are the tested configuration.** A model small enough to fit comfortably on a single workstation GPU got multi-step questions wrong, which on a ledger is the failure that matters — a fluent answer with a wrong number. Other endpoints serving the same models should behave the same way, but we have run this set only through the catalog above. The author and chart-of-accounts mapping operators have not been tested on open weights.

## Before You Point It at Real Books

- **Credits read zero by default.** The credit rate is a cost passthrough, and a GPU you already own has no per-token cost, so both rates default to `0`. If you point the lane at a paid endpoint and have billing on, set `OPENAI_COMPAT_CREDITS_PER_1K_INPUT` / `_OUTPUT` or your meter will under-report real spend. See [Credits & Billing](Credits-and-Billing).
- **A hosted catalog sees your prompts.** Operator prompts carry ledger data — account names, balances, question text. Check the endpoint's data-retention and training terms before using anything but demo data, and prefer a server you run for real books.
- **Free tiers are for evaluation.** They are rate-limited and slow — minutes per question is normal — and they support no latency claim.
- **No caching discount.** The Bedrock path caches the large system-and-tools prefix between calls. This lane does not send cache directives, so unless your server caches on its own, every call pays for the full prefix.
- **Tool calling is required.** A model that cannot call tools cannot drive an operator; it has no other way to reach the graph.

## Troubleshooting

### The Stack Will Not Start After Enabling It

**Solution:** Read the startup error — it names the setting. The usual causes are an empty `OPENAI_COMPAT_BASE_URL` or `OPENAI_COMPAT_MODEL`, or a tier mapped to a name that is not registered. The valid tier value for this lane is exactly `openai-compat`.

### Every Question Fails With a Connection Error

**Solution:** Check that `OPENAI_COMPAT_BASE_URL` ends at the version segment (the client appends `/chat/completions`), and that the address is reachable *from inside the container* — a server on your own machine is `host.docker.internal`, not `localhost`.

### The Model Answers but Never Calls a Tool

**Solution:** Confirm the model supports function calling on that endpoint; catalogs list it per model. An answer with no tool call is a model that cannot see the graph, so its numbers are invented.

### Long Questions Time Out

**Solution:** Raise `OPENAI_COMPAT_TIMEOUT_SECONDS`. If the timeout is upstream — a gateway 504 from a hosted catalog rather than our client giving up — the question needs a faster endpoint, not a longer timeout.

### Changes to `.env` Did Nothing

**Solution:** Run `just rebuild`. `just restart` picks up Python code changes only.

## Related Documentation

**Wiki Guides:**

- **[AI Operators & MCP](AI-Operators-and-MCP)** - The operator endpoint, the MCP tool surface, and connecting your own client
- **[Local Development](Local-Development)** - Getting the stack running in the first place
- **[Credits & Billing](Credits-and-Billing)** - How AI calls are metered
- **[Bootstrap Guide](Bootstrap-Guide)** - Deploy the platform into your own AWS account
- **[Architecture Overview](Architecture-Overview)** - Where operators sit in the platform

**Codebase Documentation:**

- **[Configuration](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/config/README.md)** - The model registry, tier mapping, and every setting on this page
- **[Operations](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/operations/README.md)** - The operator framework

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Wiki](https://github.com/RoboFinSystems/robosystems/wiki)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)
