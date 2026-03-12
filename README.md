# Multi-Agent Pipeline

`multi-agent-pipeline` is a Codex skill for turning one user request into a staged workflow:

1. Intake and prompt builder
2. One to three independent solver stages
3. A final reviewer that compares solutions and writes a verdict

The intake stage is expected to preserve the user's requested outcome and decompose compound requests into workstreams. It should not silently replace "build the service" with "make a scaffold" unless that is recorded as an explicit interim milestone.

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

This creates a run directory with:

- `request.md`
- `brief.md`
- `plan.json`
- `prompts/`
- `solutions/`
- `review/`

## Run Stages

Check progress:

```bash
python3 scripts/run_stage.py /path/to/agent-runs/<run-id> status
python3 scripts/run_stage.py /path/to/agent-runs/<run-id> next
```

Run the next stage:

```bash
python3 scripts/run_stage.py /path/to/agent-runs/<run-id> start-next
```

Run a specific stage:

```bash
python3 scripts/run_stage.py /path/to/agent-runs/<run-id> start intake
python3 scripts/run_stage.py /path/to/agent-runs/<run-id> start solver-a
python3 scripts/run_stage.py /path/to/agent-runs/<run-id> start review
```

Inspect or copy a prompt without running it:

```bash
python3 scripts/run_stage.py /path/to/agent-runs/<run-id> show intake
python3 scripts/run_stage.py /path/to/agent-runs/<run-id> copy solver-a
```

## Notes

- Intake preserves the original requested outcome and records any phase-1 scaffold only as an interim milestone.
- Compact mode emits JSON-like stage packets to reduce prompt overhead.
- Solver stages are intended to stay independent until review.
- The launcher uses `codex exec` under the hood.
- The launcher syncs missing solver artifacts if intake changes solver count or roles in `plan.json`.
- The skill can update downstream prompts and solver count after intake if the brief changes.
