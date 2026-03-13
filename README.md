# Multi-Agent Pipeline

`multi-agent-pipeline` is a Codex skill for turning one user request into a staged workflow:

1. Intake and prompt builder
2. One to three independent solver stages
3. A final reviewer that compares solutions and writes a verdict
4. An execution stage that implements the selected winner or hybrid
5. A verification stage that audits the implementation and seeds the next improvement run

The intake stage is expected to preserve the user's requested outcome and decompose compound requests into workstreams. It should not silently replace "build the service" with "make a scaffold" unless that is recorded as an explicit interim milestone.
Every run also carries `goal_checks` in `plan.json`, and verification writes `verification/goal-status.json` so the launcher can distinguish "all stages ran" from "the original goal is actually complete".

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

Check progress:

```bash
python3 scripts/run_stage.py /path/to/agent-runs/<run-id> status
python3 scripts/run_stage.py /path/to/agent-runs/<run-id> next
python3 scripts/run_stage.py /path/to/agent-runs/<run-id> summary
python3 scripts/run_stage.py /path/to/agent-runs/<run-id> findings
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

Create the next improvement run from verification findings:

```bash
python3 scripts/run_stage.py /path/to/agent-runs/<run-id> rerun
```

Inspect or copy a prompt without running it:

```bash
python3 scripts/run_stage.py /path/to/agent-runs/<run-id> show intake
python3 scripts/run_stage.py /path/to/agent-runs/<run-id> copy solver-a
```

## Notes

- Intake preserves the original requested outcome and records any phase-1 scaffold only as an interim milestone.
- Intake can be configured with `--intake-research`; the default is `research-first`.
- Execution can be configured with `--execution-network`; the default is `fetch-if-needed`.
- Shared cache can be configured with `--cache-root` and `--cache-policy`; the default policy is `reuse`.
- Compact mode emits JSON-like stage packets to reduce prompt overhead.
- Intake may reuse research cache, and execution may reuse cached downloads, wheels, repos, and models when appropriate.
- Solver stages are intended to stay independent until review.
- Review writes both a machine-oriented verdict and `review/user-summary.md` in the selected language. The default is Russian via `--summary-language ru`.
- Inspect the localized review summary before running execution if you want to adjust the plan or ask for corrections.
- Verification audits the actual implementation, writes `verification/findings.md`, `verification/goal-status.json`, and generates `verification/improvement-request.md` for the next run.
- If verification says the critical goal is still incomplete, `status` shows `next: rerun` instead of `next: none`.
- The launcher uses `codex exec` under the hood.
- The launcher syncs missing solver artifacts if intake changes solver count or roles in `plan.json`.
- The launcher also syncs execution and verification artifacts, can print the localized review summary with `run_stage.py <run_dir> summary`, can print post-execution findings with `run_stage.py <run_dir> findings`, and prints `status` automatically after stage runs.
- `run_stage.py <run_dir> rerun` creates a follow-up run from `verification/improvement-request.md` against the same workspace.
- The skill can update downstream prompts and solver count after intake if the brief changes.
