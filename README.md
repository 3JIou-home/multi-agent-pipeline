# Multi-Agent Pipeline

`multi-agent-pipeline` is a Codex skill for turning one user request into a staged workflow:

1. Intake and prompt builder
2. One to three independent solver stages
3. A final reviewer that compares solutions and writes a verdict
4. An execution stage that implements the selected winner or hybrid
5. A verification stage that audits the implementation and seeds the next improvement run

The intake stage is expected to preserve the user's requested outcome and decompose compound requests into workstreams. It should not silently replace "build the service" with "make a scaffold" unless that is recorded as an explicit interim milestone.
Every run also carries `goal_checks` in `plan.json`, and verification writes `verification/goal-status.json` so the launcher can distinguish "all stages ran" from "the original goal is actually complete".
Every run also carries `host_facts` in `plan.json`, so device-sensitive stages can use detected platform, architecture, and preferred torch device instead of guessing from prose.

## Install

Clone or copy this directory into your Codex skills directory:

```bash
mkdir -p ~/.codex/skills
cp -R multi-agent-pipeline ~/.codex/skills/
```

If you want the pipeline to use the `agency-agents` role catalog, clone:

```bash
git clone https://github.com/msitarzewski/agency-agents.git
```

Then point the skill at that checkout:

```bash
export AGENCY_AGENTS_DIR=/path/to/agency-agents
```

The pipeline works without `agency-agents`, but when it is available it can reuse the role library from `msitarzewski/agency-agents` for intake, solver selection, and review guidance.

## Create A Run

```bash
python3 scripts/init_run.py \
  --task 'Build a staged pipeline for this request' \
  --workspace /path/to/workspace \
  --output-dir /path/to/agent-runs
```

Compact packet mode:

```bash
python3 scripts/init_run.py \
  --task 'Build a staged pipeline for this request' \
  --workspace /path/to/workspace \
  --output-dir /path/to/agent-runs \
  --prompt-format compact
```

Choose the language for the user-facing review summary:

```bash
python3 scripts/init_run.py \
  --task 'Build a staged pipeline for this request' \
  --workspace /path/to/workspace \
  --output-dir /path/to/agent-runs \
  --summary-language ru
```

Allow execution to fetch missing dependencies or artifacts when needed:

```bash
python3 scripts/init_run.py \
  --task 'Build a staged pipeline for this request' \
  --workspace /path/to/workspace \
  --output-dir /path/to/agent-runs \
  --execution-network fetch-if-needed
```

Modes:

- `fetch-if-needed`: execution may install or download genuinely required tools, packages, repos, weights, adapters, or datasets
- `local-only`: execution must stay offline and treat missing external artifacts as blockers

Allow solver, review, execution, and verification stages to use web research:

```bash
python3 scripts/init_run.py \
  --task 'Build a staged pipeline for this request' \
  --workspace /path/to/workspace \
  --output-dir /path/to/agent-runs \
  --stage-research local-first
```

Modes:

- `research-first`: non-intake stages may browse early for official docs, examples, issue threads, and similar solutions when that materially affects the stage outcome
- `local-first`: non-intake stages inspect the workspace first and browse only when external guidance is needed
- `local-only`: non-intake stages stay local unless the user explicitly asked for web research

Configure a shared cache for research notes, downloads, wheels, models, and verification artifacts:

```bash
python3 scripts/init_run.py \
  --task 'Build a staged pipeline for this request' \
  --workspace /path/to/workspace \
  --output-dir /path/to/agent-runs \
  --cache-root ~/.cache/multi-agent-pipeline \
  --cache-policy reuse
```

Cache policies:

- `reuse`: stages may reuse and update shared cache entries
- `refresh`: stages should ignore existing cache hits and repopulate cache
- `off`: do not use shared cache

Host facts:

- `init_run.py` detects local platform, architecture, whether `torch` is installed, whether `cuda` or `mps` appears available, and the preferred torch device.
- `host_facts` also records `source` and `captured_at`, so later stages know where the hardware facts came from.
- Intake, execution, and verification should treat `host_facts` as authoritative local execution evidence instead of reinventing hardware support from prose or sandbox assumptions.

Choose how intake gathers context before it finalizes stages:

```bash
python3 scripts/init_run.py \
  --task 'Build a staged pipeline for this request' \
  --workspace /path/to/workspace \
  --output-dir /path/to/agent-runs \
  --intake-research research-first
```

Modes:

- `research-first`: intake should browse first, then normalize the brief and stages
- `local-first`: intake should inspect the workspace first and browse only when local context is insufficient
- `local-only`: intake should stay local unless the user explicitly asks for web research

This creates a run directory with:

- `request.md`
- `brief.md`
- `plan.json`
- `prompts/`
- `solutions/`
- `review/`
- `execution/`
- `verification/`

## Run Stages

Primary control-plane is now the Rust binary `agpipe`. Build it from the repo root:

```bash
cargo build --release
```

Default usage opens the terminal interface:

```bash
./target/release/agpipe
./target/release/agpipe ui --root /Users/admin/agent-runs
```

Useful non-interactive commands:

```bash
./target/release/agpipe runs /Users/admin/agent-runs --limit 10
./target/release/agpipe doctor /path/to/agent-runs/<run-id>
./target/release/agpipe resume /path/to/agent-runs/<run-id> --until verification
./target/release/agpipe amend /path/to/agent-runs/<run-id> --note 'Use the photo as a real analysis input.'
./target/release/agpipe rm /path/to/agent-runs/<run-id>
./target/release/agpipe prune-runs /Users/admin/agent-runs --keep 20 --older-than-days 14 --dry-run
```

`agpipe` is a Rust-first control-plane over the Python engine. It gives you:

- one compiled entrypoint for day-to-day use
- a `ratatui` interface for run selection, status, previews, and logs
- native `amend`, `rm`, `prune-runs`, `runs`, and `resume`
- JSON-backed integration with the Python engine for status and doctor data
- delegation to the Python engine for stage execution and fallback flows

The Python tools remain as internal/fallback engine layers:

- `scripts/run_stage.py` stays the stage engine
- `scripts/map.py` still exists for interview/run flows that are not fully ported yet
- user-facing day-to-day operation should go through `agpipe`

Typical high-level flow:

```bash
./target/release/agpipe run \
  --task-file /tmp/llm-freecad-next.txt \
  --workspace /Users/admin/llm-for-freecad \
  --until review

./target/release/agpipe resume /Users/admin/agent-runs/<run-id> --until verification

./target/release/agpipe amend /Users/admin/agent-runs/<run-id> \
  --note 'Сейчас оно работает, но игнорирует фото и строит только похожую по форме деталь. Нужно использовать фото как реальный вход анализа, а не только размеры и template-like fallback.' \
  --rewind intake
```

The default `run` flow is:

1. stage0 interview agent inspects the task and asks the domain questions it still needs
2. stage0 prompt builder generates the final normalized task prompt
3. `init_run.py` creates the run
4. autopilot executes stages until the chosen stop point
5. at the review boundary it pauses for confirmation before execution unless you pass `--auto-approve`

Typical amendment flow:

1. add the new user correction with `agpipe amend` or from the TUI with `a`
2. the amendment is written to `amendments.md`
3. `doctor` will mark the run as stale for `intake`
4. resume the run so the new correction is folded into the brief and downstream stages

Binary-first examples:

```bash
./target/release/agpipe
./target/release/agpipe runs /Users/admin/agent-runs --limit 10
./target/release/agpipe status /Users/admin/agent-runs/<run-id>
./target/release/agpipe doctor /Users/admin/agent-runs/<run-id>
./target/release/agpipe resume /Users/admin/agent-runs/<run-id> --until verification
./target/release/agpipe amend /Users/admin/agent-runs/<run-id> --note 'Фото должно использоваться как реальный вход анализа.'
./target/release/agpipe rm /Users/admin/agent-runs/<run-id>
./target/release/agpipe prune-runs /Users/admin/agent-runs --keep 20 --older-than-days 14 --dry-run
```

TUI hotkeys:

- `q`: quit
- `j` / `k`: move between runs
- `g`: refresh the run list
- `s`: run the selected run's `safe-next-action`
- `n`: `start-next`
- `r`: autopilot resume until verification
- `a`: add an amendment and rewind to `intake`
- `y`: `rerun`
- `h`: `host-probe --refresh`
- `x`: delete the selected run
- `p`: prune old runs with the default policy
- `u`: refresh all prompts
- `b`: `step-back review`
- `v`: `recheck verification`

Stage0 interview only:

```bash
./target/release/agpipe interview \
  --task 'Build a working Telegram to FreeCAD service' \
  --workspace /path/to/workspace \
  --output-dir /path/to/agent-runs
```

Check progress:

```bash
python3 scripts/run_stage.py /path/to/agent-runs/<run-id> status
python3 scripts/run_stage.py /path/to/agent-runs/<run-id> doctor
python3 scripts/run_stage.py /path/to/agent-runs/<run-id> next
python3 scripts/run_stage.py /path/to/agent-runs/<run-id> summary
python3 scripts/run_stage.py /path/to/agent-runs/<run-id> findings
python3 scripts/run_stage.py /path/to/agent-runs/<run-id> augmented-task
python3 scripts/run_stage.py /path/to/agent-runs/<run-id> host-probe --refresh --history
python3 scripts/run_stage.py /path/to/agent-runs/<run-id> cache-status --refresh
python3 scripts/run_stage.py /path/to/agent-runs/<run-id> recheck verification --dry-run
python3 scripts/run_stage.py /path/to/agent-runs/<run-id> step-back verification --dry-run
python3 scripts/run_stage.py /path/to/agent-runs/<run-id> refresh-prompt verification
python3 scripts/run_stage.py /path/to/agent-runs/<run-id> refresh-prompts
```

Run the next stage:

```bash
python3 scripts/run_stage.py /path/to/agent-runs/<run-id> start-next
```

When the next work is the solver batch, `start-next` launches all pending solver stages in parallel and prints the updated status after each solver finishes.

Run a specific stage:

```bash
python3 scripts/run_stage.py /path/to/agent-runs/<run-id> start intake
python3 scripts/run_stage.py /path/to/agent-runs/<run-id> start solver-a
python3 scripts/run_stage.py /path/to/agent-runs/<run-id> start-solvers
python3 scripts/run_stage.py /path/to/agent-runs/<run-id> start review
python3 scripts/run_stage.py /path/to/agent-runs/<run-id> start execution
python3 scripts/run_stage.py /path/to/agent-runs/<run-id> start verification
```

Repeat a stage without creating a new run:

```bash
python3 scripts/run_stage.py /path/to/agent-runs/<run-id> refresh-prompt verification
python3 scripts/run_stage.py /path/to/agent-runs/<run-id> recheck verification
python3 scripts/run_stage.py /path/to/agent-runs/<run-id> start verification
```

Use `recheck verification` when `execution` is still valid and you only want a clean verification rerun. Use `step-back execution` or `step-back verification` when you intentionally want to rewind stage state.

`step-back` resets the selected stage and dependent downstream stages to `pending`. Examples:

- `step-back verification` resets only verification artifacts
- `step-back execution` resets execution and verification
- `step-back review` resets review, execution, and verification
- `step-back solver-a` resets `solver-a`, review, execution, and verification

Manage shared cache:

```bash
python3 scripts/run_stage.py /path/to/agent-runs/<run-id> cache-status --refresh --limit 10
python3 scripts/run_stage.py /path/to/agent-runs/<run-id> cache-prune --max-age-days 14 --dry-run
python3 scripts/run_stage.py /path/to/agent-runs/<run-id> cache-prune --max-age-days 30 --area research
```

Create the next improvement run from verification findings:

```bash
python3 scripts/run_stage.py /path/to/agent-runs/<run-id> rerun
python3 scripts/run_stage.py /path/to/agent-runs/<run-id> rerun --prompt-source augmented
```

Inspect or copy a prompt without running it:

```bash
python3 scripts/run_stage.py /path/to/agent-runs/<run-id> show intake
python3 scripts/run_stage.py /path/to/agent-runs/<run-id> copy solver-a
```

## Notes

- Intake preserves the original requested outcome and records any phase-1 scaffold only as an interim milestone.
- Intake can be configured with `--intake-research`; the default is `research-first`.
- Solver, review, execution, and verification can be configured together with `--stage-research`; the default is `local-first`.
- Execution can be configured with `--execution-network`; the default is `fetch-if-needed`.
- Shared cache can be configured with `--cache-root` and `--cache-policy`; the default policy is `reuse`.
- Compact mode emits JSON-like stage packets to reduce prompt overhead.
- Intake may reuse research cache, and execution may reuse cached downloads, wheels, repos, and models when appropriate.
- Solver stages are intended to stay independent until review.
- Review writes both a machine-oriented verdict and `review/user-summary.md` in the selected language. The default is Russian via `--summary-language ru`.
- Inspect the localized review summary before running execution if you want to adjust the plan or ask for corrections.
- Verification audits the actual implementation, writes `verification/findings.md`, `verification/goal-status.json`, `verification/improvement-request.md`, and `verification/augmented-task.md` for the next run.
- If verification says the critical goal is still incomplete, `status` shows `next: rerun` instead of `next: none`.
- The launcher uses `codex exec` under the hood.
- The launcher syncs missing solver artifacts if intake changes solver count or roles in `plan.json`.
- The launcher also syncs execution and verification artifacts, can print the localized review summary with `run_stage.py <run_dir> summary`, can print post-execution findings with `run_stage.py <run_dir> findings`, and prints `status` automatically after stage runs.
- `run_stage.py <run_dir> rerun` creates a follow-up run from `verification/improvement-request.md` against the same workspace.
- `run_stage.py <run_dir> host-probe --refresh` captures launcher-side host/runtime facts with the current local Python, stores the latest copy in `host/probe.json`, and appends a timestamped snapshot under `host/probes/`.
- `run_stage.py <run_dir> recheck verification` resets only verification artifacts, so you can rerun verification without `--force` while preserving a completed execution stage.
- `run_stage.py <run_dir> step-back <stage>` rewinds a stage to `pending` without creating a new run.
- `run_stage.py <run_dir> refresh-prompt <stage>` regenerates the raw stage prompt from the current skill logic for an existing run.
- `run_stage.py <run_dir> refresh-prompts` regenerates all current stage prompt files in one pass.
- `status` now shows `host-probe: captured (...)` and `host-drift: plan=... probe=...` when the current launcher environment disagrees with `plan.json`.
- Execution and verification automatically refresh `host/probe.json` before they launch, so device-sensitive stages see current host evidence instead of only the original `host_facts`.
- `host/probe.json` records visible environment variable names for common ML prefixes, but not their secret values.
- `run_stage.py <run_dir> cache-status` prints the shared cache index, per-area size, and largest files.
- `run_stage.py <run_dir> cache-prune` removes stale cache files by age and area, then rebuilds the cache index.
- `run_stage.py <run_dir> doctor` catches stale downstream stages such as `verification: done` while `execution: pending`, warns when review/execution/verification artifacts are older than their upstream evidence, and recommends the next safe action.
- `run_stage.py <run_dir> rerun --prompt-source augmented` prefers the fuller verification-generated follow-up prompt when available.
- `agpipe run` still uses the separate Codex-based stage0 interview agent under the hood to gather missing domain clarifications before the actual pipeline run is created.
- `agpipe resume` continues an existing run automatically and uses `start-solvers` when the next work is the solver batch.
- `agpipe amend <run_dir> --note ... --rewind intake` is the fast path when the user wants to refine the current run instead of creating a new one.
- `amendments.md` is treated as authoritative latest user input during later stage runs.
- `agpipe rm <run_dir>` deletes one run, and `agpipe prune-runs <root> --keep N --older-than-days D` deletes old runs in bulk.
- The skill can update downstream prompts and solver count after intake if the brief changes.
