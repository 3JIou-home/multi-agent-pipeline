# Multi-Agent Pipeline

`multi-agent-pipeline` is a Rust runtime for staged agent execution. It turns one user request into a resumable, file-based workflow driven by the `agpipe` binary and a run directory on disk.

The default flow is:

1. interview and task finalization
2. intake and brief construction
3. one or more independent research or solver stages
4. review and winner or hybrid selection
5. execution in the real workspace
6. verification against the actual result

The service is designed for iterative operator control, cache reuse, and auditable artifacts. The normal operator entry point is the TUI. The low-level CLI still exists, but it is intended for CI, automation, tests, and debugging rather than day-to-day manual use.

## Build

```bash
cargo build --release
./target/release/agpipe --help
```

## Validate

```bash
cargo clippy --all-targets --all-features -- -D warnings
cargo test
cargo build --release
```

## Local Env

The service remains usable without a repo-local env file, but some optional features depend on environment variables.

Current behavior:

- plain Codex-backed TUI flow works as long as `codex` is available on `PATH`
- the OpenAI Responses backend requires `OPENAI_API_KEY` or `AGPIPE_OPENAI_API_KEY`
- `context7` is only auto-selected when `CONTEXT7_API_KEY` is available
- repo-local `.env` and `.env.local` are loaded automatically from the `agpipe` repo root
- existing shell variables always win over values from `.env` files

For a personal machine-specific setup, copy [.env.local.example](/Users/admin/.codex/skills/multi-agent-pipeline/.env.local.example) to `.env.local`. The file is ignored by git.

## TUI Black-Box Smoke

For external terminal-level validation you can drive the TUI with `pexpect` and record the same flow with VHS.

Install the optional tooling:

```bash
python3 -m venv /tmp/agpipe-pexpect-venv
/tmp/agpipe-pexpect-venv/bin/pip install -r scripts/requirements-tui.txt
brew install vhs
```

Run the smoke harness directly:

```bash
/tmp/agpipe-pexpect-venv/bin/python scripts/tui_pexpect_smoke.py \
  --agpipe ./target/release/agpipe \
  --mode run-all
```

Record a deterministic TUI wizard demo with VHS:

```bash
chmod +x examples/mock-codex-vhs.zsh
vhs examples/tui_wizard_demo.tape
```

The `pexpect` harness is the assertive black-box check. The VHS tape is the recorder/demo path. Both use a mock `AGPIPE_CODEX_BIN` plus `AGPIPE_STAGE0_BACKEND=local`, so the wizard always goes through interview, final prompt review, run creation, and a deterministic full pipeline completion.

## Quick Start

Open the TUI:

```bash
./target/release/agpipe
./target/release/agpipe ui --root /path/to/agent-runs
```

Normal UI flow:

- press `c` to create a run
- let the wizard handle interview and final task review when needed
- press `n` for the next safe stage or `r` to resume the whole pipeline
- inspect logs, artifacts, findings, and doctor state from the same screen

Run state semantics:

- `health=healthy` means there is no known consistency problem in the current run graph
- `outcome=follow-up-needed` means verification finished cleanly, but the result is intentionally `partial` and a narrow rerun is recommended
- `status=stalled` means the current stage is still alive but has not produced fresh output for a while; use `i` to interrupt it and inspect the logs before retrying
- `doctor --fix` and the TUI `f` action do not own the run job anymore; they trigger repair actions without overwriting the active stage owner in `runtime/job.json`

Automation and low-level debugging still exist, but they are intentionally moved under `agpipe internal ...`.

## Workflow Model

Every run is file-based. The runtime keeps prompts, stage outputs, logs, review verdicts, execution reports, verification findings, cache metadata, and host evidence in the run directory.

Core guarantees:

- stages are resumable
- solver outputs stay isolated until review
- execution edits the real workspace only after review
- verification reads the real workspace and records goal coverage
- `goal-status.json` is the completion gate, not just "all stages ran"

## YAML Pipeline Configuration

The runtime supports a configurable ordered pipeline. Put `agpipe.pipeline.yml` in the workspace or pass `--pipeline-file`.

Auto-detected paths:

- `/path/to/workspace/agpipe.pipeline.yml`
- `/path/to/workspace/.agpipe/pipeline.yml`

Explicit path:

```bash
./target/release/agpipe internal create-run \
  --task "Research and implement a CLI." \
  --workspace /path/to/workspace \
  --output-dir /path/to/agent-runs \
  --pipeline-file /path/to/pipeline.yml
```

Minimal example:

```yaml
pipeline:
  stages:
    - id: intake
      kind: intake

    - id: research-a
      kind: research

    - id: research-b
      kind: research

    - id: synthesis
      kind: review

    - id: implement
      kind: execution

    - id: audit
      kind: verification
```

Example for research only:

```yaml
pipeline:
  stages:
    - id: intake
      kind: intake
    - id: researcher-a
      kind: research
    - id: researcher-b
      kind: research
    - id: review
      kind: review
    - id: audit
      kind: verification
```

Supported stage kinds:

- `intake`, `brief`, `planning`
- `solver`, `research`, `analysis`, `researcher`
- `review`, `compare`, `synthesis`
- `execution`, `implement`, `implementation`, `apply`
- `verification`, `verify`, `audit`, `check`

Supported fields:

- `id`: stable stage identifier used in status, logs, prompts, and cache keys
- `kind`: behavioral stage type
- `role`: optional explicit agent role override
- `angle`: optional strategy hint for research or solver stages
- `description`: optional metadata
- `depends_on` or `needs`: accepted syntactically for future DAG use

Current scheduler model:

- pipeline order is the order in `stages`
- `depends_on` and `needs` are parsed and preserved, but scheduling is still linear
- this is not yet a full GitLab CI style DAG

## Automatic `role` And `angle`

For research or solver stages you usually do not need to specify `role` or `angle`.

Current behavior:

- if `role` is omitted, intake assigns it automatically from stage `kind` and inferred `task_kind`
- if `role` is explicitly provided in YAML, it is treated as an override and is preserved
- if `angle` is omitted, the runtime assigns a default diversification sequence across solver-like stages

This keeps YAML compact while still producing differentiated researchers or solvers.

## Local `agency-agents` Catalog

When a local `agency-agents` checkout is available, `agpipe` uses it as a real role catalog instead of treating role names as labels only.

Current behavior:

- intake receives the built-in role map plus a generated catalog summary when the Responses backend is used
- solver prompts resolve the assigned `role` to a concrete markdown file when possible
- review prompts resolve reviewer stack entries to concrete markdown files when possible
- Responses prompts embed resolved role documents directly in the local context bundle
- Codex stages add the catalog directory to `codex exec` via `--add-dir`, so stage agents can inspect the actual role files

Lookup order:

- `AGENCY_AGENTS_DIR`
- a sibling `agency-agents` checkout near the current `repo_root`
- `~/agency-agents`

Example without explicit roles:

```yaml
pipeline:
  stages:
    - id: intake
      kind: intake
    - id: research-a
      kind: research
    - id: research-b
      kind: research
    - id: implement
      kind: execution
    - id: audit
      kind: verification
```

## MCP Auto Selection

`agpipe` now does two separate MCP things during `create-run` and later plan saves:

- it auto-selects MCP servers for the task and records that in the run plan
- it auto-provisions missing Codex MCP entries in `~/.codex/config.toml` so downstream stages can actually use them

Current behavior:

- the planner auto-selects `context7` for docs/version-sensitive work only when `CONTEXT7_API_KEY` is available
- otherwise docs/version-sensitive work falls back to official `fetch` plus normal browsing/local context
- the planner auto-selects official `fetch` as a direct-page fallback for docs/research shaped runs
- the planner auto-selects official `git` for review runs on Git workspaces
- the planner also auto-selects `memory` for multi-stage runs that benefit from durable handoffs
- selected MCP guidance is written to `references/mcp-plan.md`
- provisioning status is written to `runtime/mcp-provision.json` and summarized in `references/mcp-plan.md`
- `plan.json` stores the selected MCP metadata and each solver gets its own `mcp_servers` list
- solver prompts include memory isolation guidance so parallel solvers do not leak through a shared memory namespace

Provisioning details:

- `context7` is provisioned as a streamable HTTP MCP server at `https://mcp.context7.com/mcp`
- `exa` is provisioned as a streamable HTTP MCP server at `https://mcp.exa.ai/mcp`
- official `fetch` is provisioned as `docker run -i --rm mcp/fetch`
- official `git` is provisioned as `docker run --rm -i --mount type=bind,src=$HOME,dst=$HOME mcp/git`
- the provisioned official `git` server is constrained to read-only review tools via `enabled_tools`
- `memory` is provisioned automatically when possible:
  - prefer Docker-backed `mcp/memory` when `docker` is available
  - otherwise fall back to `npx -y @modelcontextprotocol/server-memory`
- `context7` uses `env_http_headers` for `CONTEXT7_API_KEY` and is only auto-selected when that environment variable exists
- provisioning is idempotent: existing `[mcp_servers.<name>]` blocks are left alone
- set `AGPIPE_DISABLE_MCP_PROVISION=1` to disable automatic Codex config changes

Important constraints:

- MCP selection is still stage-aware orchestration; provisioning only registers servers in Codex, it does not guarantee credentials or connectivity
- `memory` is scoped per solver namespace to preserve solver independence until review
- when MCP is unavailable, stages fall back to local context and normal browsing rules

Examples:

- docs / SDK / framework / version-sensitive tasks usually select `fetch + memory`, and upgrade to `context7 + fetch + memory` when `CONTEXT7_API_KEY` is available
- research / comparison / source-discovery tasks usually select `exa + fetch + memory`
- code-review tasks on a Git workspace usually select `git + memory`

## Backends

`execution` uses the local `codex` binary. Stage0 and non-execution stages can use the built-in OpenAI Responses backend.

Codex path:

```bash
export AGPIPE_CODEX_BIN=/path/to/codex
```

Enable Responses for stage0:

```bash
export OPENAI_API_KEY=...
export AGPIPE_STAGE0_BACKEND=responses
```

Enable Responses for all non-execution stages:

```bash
export OPENAI_API_KEY=...
export AGPIPE_STAGE0_BACKEND=responses
export AGPIPE_STAGE_BACKEND=responses-readonly
```

Force the deterministic local stage0 fallback:

```bash
export AGPIPE_STAGE0_BACKEND=local
```

Important runtime behavior:

- `execution` stays on Codex because it needs live tool use in the real workspace
- `responses-readonly` is intended for intake, research, review, and some verification work
- stage0 keeps the internal automation contract even when the configured backend is unavailable: `interview-questions` and `interview-finalize` write real artifacts plus `*.fallback.json` provenance before handing off to `create-run`
- Responses requests use structured outputs instead of best-effort embedded JSON parsing
- terminal response states are fail-closed: `failed`, `incomplete`, `cancelled`, and timeout states are surfaced as errors
- create requests use idempotency keys
- polling uses retry or backoff behavior and honors `Retry-After` when present

Privacy-related switches:

```bash
export AGPIPE_OPENAI_BACKGROUND=0
export AGPIPE_OPENAI_STORE=0
```

`doctor` reports stored backend configuration from the run metadata, so diagnostics do not depend only on the current shell environment.

## Cache

`agpipe` uses a real runtime cache, not only advisory prompt hints.

Cache areas include:

- `research`
- `downloads`
- `wheelhouse`
- `models`
- `verification`
- `stage-results`

Local stage cache is content-addressed. The cache key includes the effective prompt inputs and backend fingerprint, not just timestamps.

Important properties:

- repeated `start`, `start-next`, `start-solvers`, and `resume` calls can restore stage outputs from cache
- cache reuse restores both artifacts and logs, so the run stays auditable
- stale lock directories are detected and recovered automatically
- cache manifests track token usage and estimated savings
- `runtime/token-ledger.json` stores local and provider-side token accounting

Readonly backend cache behavior:

- `responses-readonly` stages such as intake, research, and review do not invalidate on unrelated workspace edits
- verification is different: verification cache includes workspace fingerprinting because it must reason about the actual implementation

Prompt cache behavior:

- prompt cache keys are based on stable prefix hashes instead of `workdir`
- static prefix content is separated from dynamic stage suffix content to improve remote cache locality

Inspect cache state:

```bash
./target/release/agpipe internal cache-status /path/to/run --refresh
./target/release/agpipe internal cache-prune /path/to/run --max-age-days 14 --dry-run
```

## Internal Stage0 Flow

Interview mode helps normalize the task before creating a run. The TUI uses this automatically; the commands below are for internal automation and debugging.

Commands:

```bash
./target/release/agpipe internal interview --task "..." --workspace /path/to/workspace --output-dir /tmp/agpipe-interview
./target/release/agpipe internal interview-questions --task "..." --workspace /path/to/workspace --output-dir /tmp/agpipe-interview
./target/release/agpipe internal interview-finalize --task "..." --workspace /path/to/workspace --session-dir /path/to/session --answers-file /path/to/answers.json
```

Behavior:

- preserves the original goal
- asks only the domain questions that matter
- fails closed when the selected backend fails
- keeps diagnostics instead of fabricating success artifacts

## Internal CLI

The TUI is the normal operator surface. The commands below remain available for scripting, tests, and low-level debugging.

```bash
./target/release/agpipe internal create-run ...
./target/release/agpipe internal run ...
./target/release/agpipe internal runs /path/to/agent-runs
./target/release/agpipe internal status /path/to/run
./target/release/agpipe internal doctor /path/to/run
./target/release/agpipe internal next /path/to/run
./target/release/agpipe internal show /path/to/run <stage>
./target/release/agpipe internal copy /path/to/run <stage> --raw
./target/release/agpipe internal start-next /path/to/run
./target/release/agpipe internal start-solvers /path/to/run
./target/release/agpipe internal resume /path/to/run --until verification
./target/release/agpipe internal amend /path/to/run --note "..." --rewind intake
./target/release/agpipe internal recheck /path/to/run verification
./target/release/agpipe internal step-back /path/to/run verification
./target/release/agpipe internal refresh-prompt /path/to/run verification
./target/release/agpipe internal refresh-prompts /path/to/run
./target/release/agpipe internal host-probe /path/to/run --refresh
./target/release/agpipe internal cache-status /path/to/run --refresh
./target/release/agpipe internal cache-prune /path/to/run --max-age-days 14 --dry-run
./target/release/agpipe internal rerun /path/to/run
./target/release/agpipe internal runtime-check /path/to/run --phase execution
```

## TUI

Open the terminal UI:

```bash
./target/release/agpipe
./target/release/agpipe ui --root /path/to/agent-runs
```

Important hotkeys:

- `q`: quit
- `j` or `k`: move between runs
- `g`: refresh run list
- `o` or `Enter`: open the selected artifact
- `c`: create a new run
- `n`: run `start-next`
- `r` or `w`: run the whole stack until verification
- `a`: append an amendment and rewind
- `y`: create a follow-up rerun
- `h`: capture a fresh host probe
- `u`: refresh prompts
- `b`: `step-back review`
- `v`: `recheck verification`

## Run Layout

Each run contains:

- `request.md`
- `brief.md`
- `plan.json`
- `prompts/`
- `solutions/`
- `review/`
- `execution/`
- `verification/`
- `logs/`
- `host/`
- `runtime/`

Key artifacts:

- `plan.json`: normalized run plan, backend configuration, cache config, and inferred task metadata
- `review/summary.md`: review verdict and user-facing summary
- `execution/report.md`: implementation report and validation notes
- `verification/findings.md`: ordered verification findings
- `verification/goal-status.json`: completion gate and goal coverage
- `runtime/token-ledger.json`: accumulated token accounting

## Validation And Smoke Coverage

The current repository is validated with:

- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo test`
- `cargo build --release`
- CLI smoke for hello-world generation
- TUI integration tests
- cache reuse and verification regressions
- Responses backend regressions
- custom YAML pipeline regressions

## Current Limitations

- the configurable pipeline is ordered, not a true DAG scheduler yet
- `depends_on` and `needs` are metadata today, not execution graph edges
- `execution` is still the only stage that edits the workspace directly
- large-repository verification still uses bounded workspace embedding and cache tradeoffs rather than a full repository semantic index
