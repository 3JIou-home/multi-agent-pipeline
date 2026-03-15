---
name: multi-agent-pipeline
description: "Build and run a five-stage agent pipeline for a single user task: (1) intake/prompt-builder, (2) one to three independent solver agents, (3) a censor/reviewer that compares outputs and writes user-facing summaries, (4) an execution stage that implements the recommended winner or hybrid, and (5) a verification stage that audits the resulting code, checks goal coverage, writes findings, and seeds the next improvement run."
---

# Multi Agent Pipeline

This skill turns a raw user request into a staged, file-based workflow with explicit handoffs. The runtime surface is the `agpipe` binary, supports configurable YAML pipelines, and uses native Rust CLI and TUI entry points.

## Quick Start

1. Build the binary:
   - `cargo build --release`
2. Create a run:
   - `./target/release/agpipe create-run --task "<user request>" --workspace <repo-or-dir> --output-dir <agent-runs-dir> --prompt-format compact`
3. Or create and automate in one step:
   - `./target/release/agpipe run --task "<user request>" --workspace <repo-or-dir> --output-dir <agent-runs-dir> --until review`
4. Inspect and control runs:
   - `./target/release/agpipe runs <agent-runs-dir>`
   - `./target/release/agpipe status <run_dir>`
   - `./target/release/agpipe doctor <run_dir>`
   - `./target/release/agpipe next <run_dir>`
   - `./target/release/agpipe show <run_dir> intake`
   - `./target/release/agpipe copy <run_dir> solver-a --raw`
   - `./target/release/agpipe start-next <run_dir>`
   - `./target/release/agpipe start-solvers <run_dir>`
   - `./target/release/agpipe resume <run_dir> --until verification`
   - `./target/release/agpipe amend <run_dir> --note "<new user correction>" --rewind intake`
   - `./target/release/agpipe host-probe <run_dir> --refresh`
   - `./target/release/agpipe cache-status <run_dir> --refresh`
   - `./target/release/agpipe recheck <run_dir> verification`
   - `./target/release/agpipe step-back <run_dir> verification`
   - `./target/release/agpipe refresh-prompt <run_dir> verification`
   - `./target/release/agpipe refresh-prompts <run_dir>`
   - `./target/release/agpipe rerun <run_dir>`
5. Open the TUI if needed:
   - `./target/release/agpipe`
   - `./target/release/agpipe ui --root <agent-runs-dir>`

`AGPIPE_CODEX_BIN` can be used to point the runtime at a specific `codex` executable.

## Workflow

### 0. Interview And Task Finalization

Use:

- `./target/release/agpipe interview ...`
- `./target/release/agpipe interview-questions ...`
- `./target/release/agpipe interview-finalize ...`

The interview flow should preserve the original goal, ask only the domain questions that matter, and produce a final task prompt that is ready for downstream execution.

### 1. Intake And Prompt Builder

The intake stage should:

- preserve the requested outcome as the top-level goal
- refine the brief and `goal_checks`
- keep `host_facts` and cache settings in `plan.json`
- decompose compound work into concrete workstreams
- avoid solving the task directly

### 2. Independent Solver Stage

Use one to three independent solver stages based on complexity:

- `low`: 1 solver
- `medium`: 2 solvers
- `high`: 3 solvers

Keep solver outputs isolated until review.

### 3. Review

The reviewer must compare solver outputs against the brief and `goal_checks`, penalize silent scope reduction, and recommend a winner, backup, or explicit hybrid.

### 4. Execution

The execution stage must:

- read the review verdict before editing the workspace
- implement the selected winner or hybrid
- treat workspace edits as the main deliverable
- use cache and host facts from `plan.json`
- run the cheapest relevant validation
- write `execution/report.md`

### 5. Verification

The verification stage must inspect the actual workspace, produce ordered findings, update `verification/goal-status.json`, and seed the next rerun when needed.

## Operating Rules

- Keep stages file-based and resumable.
- Preserve solver independence until review.
- Prefer targeted validation over broad expensive suites.
- Do not treat "all stages ran" as equivalent to "the original goal is complete".
- Use `goal_checks` and `verification/goal-status.json` as the completion gate.

## Artifacts

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

## Validation Notes

- `cargo build --release`
- `cargo test`
- `./target/release/agpipe --help`
- `AGPIPE_CODEX_BIN=/usr/bin/false ./target/release/agpipe interview-questions ...`
- `AGPIPE_CODEX_BIN=/usr/bin/false ./target/release/agpipe interview-finalize ...`
- `./target/release/agpipe create-run ...`
- `./target/release/agpipe host-probe <run_dir> --refresh --history`

Treat live parity evidence as valid only when a real local `codex` run succeeds without fallback artifacts.
