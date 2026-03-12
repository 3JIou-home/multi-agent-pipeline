---
name: multi-agent-pipeline
description: "Build and run a three-level agent pipeline for a single user task: (1) intake/prompt-builder, (2) one to three independent solver agents, and (3) a censor/reviewer that compares outputs, runs targeted validation, and writes short summaries. Use when a task benefits from role specialization, multiple candidate solutions, explicit handoffs, or evidence-based review. Especially useful for coding, architecture, infra, research, documentation, and Codex skill/prompt orchestration work."
---

# Multi Agent Pipeline

Turn a raw user request into a staged, file-based workflow with explicit handoffs. This skill assumes one orchestrator controls the pipeline and either:

- launches truly separate agents when the runtime supports it, or
- simulates separation by generating prompt packets and keeping solver outputs isolated until review.

## Quick Start

1. Run `python3 scripts/init_run.py --task "<user request>" --workspace <repo-or-dir>`.
2. Open the generated run directory and read `plan.json`.
3. Use the launcher:
   - `python3 scripts/run_stage.py <run_dir> status`
   - `python3 scripts/run_stage.py <run_dir> next`
   - `python3 scripts/run_stage.py <run_dir> start-next`
4. If you want manual control, use:
   - `python3 scripts/run_stage.py <run_dir> show intake`
   - `python3 scripts/run_stage.py <run_dir> copy solver-a`
   - `python3 scripts/run_stage.py <run_dir> start review`
5. If the runtime cannot spawn multiple agents, run solver passes sequentially but do not read sibling solver outputs until the review stage.

## Workflow

### 1. Intake And Prompt Builder

Use the first-level agent to normalize the request before any solving begins.

Do all of the following:

- Rewrite the raw task into a precise execution brief.
- Verify the workspace path. If it is missing, either correct it or explicitly treat the run as greenfield planning.
- Confirm or correct task kind, complexity, and solver count from `plan.json`.
- Choose the minimal additional skills needed for downstream work.
- Finalize the stage prompts without solving the task itself.

The intake brief must contain:

- objective
- deliverable
- scope boundaries
- repo or workspace path
- constraints
- definition of done
- validation expectations
- open questions that can be answered from local context

Use `references/agency-role-map.md` only for the relevant task kind. If the task is about Codex skills, use `skill-creator`. If the task is about listing or installing skills, use `skill-installer`.

### 2. Independent Solver Stage

Assign one to three solver agents based on complexity:

- `low`: 1 solver
- `medium`: 2 solvers
- `high`: 3 solvers

Keep solutions intentionally different. Prefer these solution angles in order:

- `implementation-first`
- `architecture-first`
- `risk-first`

Use `speed-first` only when rapid delivery matters more than long-term design.

Each solver must receive:

- the same normalized brief
- a specific specialist role
- a distinct solution angle
- explicit deliverables
- the instruction to avoid sibling solution files until review

Each solver output should include:

- assumptions
- approach
- proposed edits or implementation summary
- validation performed
- unresolved risks

### 3. Censor And Reviewer Stage

Open all solver outputs only after all solvers finish. Use `references/review-rubric.md` for scoring and evidence requirements.

The reviewer must:

- compare every solution against the original brief
- run the cheapest relevant validation when code or config changed
- mark evidence gaps when tests could not run
- produce a short summary for each solution
- recommend one winner, one backup, or a compatible hybrid

Adopt the skeptical stance from `agency-agents/testing/testing-reality-checker.md` when the local `agency-agents` repo exists. Use the concise reporting style from `agency-agents/support/support-executive-summary-generator.md` for the final summary.

## Operating Rules

- Keep every stage file-based so the pipeline can be resumed or audited later.
- Preserve solver independence until the reviewer stage.
- Prefer complementary solver roles over duplicates.
- Do not merge incompatible fragments from different solvers without stating the hybrid explicitly.
- Run targeted validation before broad or expensive test suites.
- If tests cannot run, say exactly why and treat that as a review penalty.
- Keep the final reviewer summary short and decision-oriented.

## Artifacts

The scaffold script creates this layout:

```text
agent-runs/<timestamp>-<slug>/
  request.md
  brief.md
  plan.json
  prompts/
    level1-intake.md
    level2-solver-a.md
    level2-solver-b.md
    level2-solver-c.md
    level3-review.md
  solutions/
    solver-a/RESULT.md
    solver-b/RESULT.md
    solver-c/RESULT.md
  review/
    report.md
    scorecard.json
```

Read `plan.json` first. It contains the normalized metadata, role assignments, stack signals, and suggested validation commands.

## Resources

### `scripts/init_run.py`

Generate a reusable run directory with prompts, role assignments, and review hints.

### `scripts/run_stage.py`

Inspect run status, compile stage prompts with absolute references, copy prompts to the clipboard, and launch `codex exec` for a chosen stage.

### `references/agency-role-map.md`

Map task kinds to recommended roles from the local `agency-agents` catalog and Codex skills.

### `references/review-rubric.md`

Define the reviewer scorecard, summary format, and validation heuristics.
