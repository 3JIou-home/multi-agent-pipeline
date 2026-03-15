# Multi-Agent Pipeline

`multi-agent-pipeline` is a Rust runtime for staged agent execution. It turns one user request into a resumable, file-based workflow driven by the `agpipe` binary and a run directory on disk.

The default flow is:

1. interview and task finalization
2. intake and brief construction
3. one or more independent research or solver stages
4. review and winner or hybrid selection
5. execution in the real workspace
6. verification against the actual result

The service is designed for iterative operator control, cache reuse, and auditable artifacts. The normal operator entry point is the TUI. The CLI is also fully usable for CI, automation, and debugging.

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

## Quick Start

Create and inspect a run:

```bash
./target/release/agpipe create-run \
  --task "Build a Python hello world program." \
  --workspace /path/to/workspace \
  --output-dir /path/to/agent-runs

./target/release/agpipe status /path/to/run
./target/release/agpipe start-next /path/to/run
./target/release/agpipe resume /path/to/run --until verification
./target/release/agpipe doctor /path/to/run
```

Create and automate in one command:

```bash
./target/release/agpipe run \
  --task "Build a Python hello world program." \
  --workspace /path/to/workspace \
  --output-dir /path/to/agent-runs \
  --until verification
```

Open the TUI:

```bash
./target/release/agpipe
./target/release/agpipe ui --root /path/to/agent-runs
```

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
./target/release/agpipe create-run \
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

Important runtime behavior:

- `execution` stays on Codex because it needs live tool use in the real workspace
- `responses-readonly` is intended for intake, research, review, and some verification work
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
./target/release/agpipe cache-status /path/to/run --refresh
./target/release/agpipe cache-prune /path/to/run --max-age-days 14 --dry-run
```

## Interview Flow

Interview mode helps normalize the task before creating a run.

Commands:

```bash
./target/release/agpipe interview --task "..." --workspace /path/to/workspace --output-dir /tmp/agpipe-interview
./target/release/agpipe interview-questions --task "..." --workspace /path/to/workspace --output-dir /tmp/agpipe-interview
./target/release/agpipe interview-finalize --task "..." --workspace /path/to/workspace --session-dir /path/to/session --answers-file /path/to/answers.json
```

Behavior:

- preserves the original goal
- asks only the domain questions that matter
- fails closed when the selected backend fails
- keeps diagnostics instead of fabricating success artifacts

## CLI Surface

Primary commands:

```bash
./target/release/agpipe create-run ...
./target/release/agpipe run ...
./target/release/agpipe runs /path/to/agent-runs
./target/release/agpipe status /path/to/run
./target/release/agpipe doctor /path/to/run
./target/release/agpipe next /path/to/run
./target/release/agpipe show /path/to/run <stage>
./target/release/agpipe copy /path/to/run <stage> --raw
./target/release/agpipe start-next /path/to/run
./target/release/agpipe start-solvers /path/to/run
./target/release/agpipe resume /path/to/run --until verification
./target/release/agpipe amend /path/to/run --note "..." --rewind intake
./target/release/agpipe recheck /path/to/run verification
./target/release/agpipe step-back /path/to/run verification
./target/release/agpipe refresh-prompt /path/to/run verification
./target/release/agpipe refresh-prompts /path/to/run
./target/release/agpipe host-probe /path/to/run --refresh
./target/release/agpipe cache-status /path/to/run --refresh
./target/release/agpipe cache-prune /path/to/run --max-age-days 14 --dry-run
./target/release/agpipe rerun /path/to/run
./target/release/agpipe ui --root /path/to/agent-runs
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
- `r`: resume until verification
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

## Local Fast Paths

The runtime has deterministic local paths for trivial requests when a model call would be wasteful.

Current example:

- Python hello-world tasks can complete through a local template path and direct validation of `main.py`

This is used for smoke tests and for cheap deterministic handling of trivial tasks.

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

## Repository Notes

This repository now represents the main Rust implementation at `multi-agent-pipeline`. The previous split between `multi-agent-pipeline` and `multi-agent-pipeline-rust` is no longer the intended operator-facing distinction.
