---
name: multi-agent-pipeline
description: "Build and run a five-stage agent pipeline for a single user task: (1) intake/prompt-builder, (2) one to three independent solver agents, (3) a censor/reviewer that compares outputs and writes user-facing summaries, (4) an execution stage that implements the recommended winner or hybrid, and (5) a verification stage that audits the resulting code, checks goal coverage, writes findings, and seeds the next improvement run."
---

# Multi Agent Pipeline

Turn a raw user request into a staged, file-based workflow with explicit handoffs. This skill assumes one orchestrator controls the pipeline and either:

- launches truly separate agents when the runtime supports it, or
- simulates separation by generating prompt packets and keeping solver outputs isolated until review.

Default principle: preserve the user's requested outcome. Intake may decompose, sequence, and clarify the work, but it must not silently downgrade a request for a working system into "architecture only" or "scaffold only" unless the brief records that as an explicit constraint or interim milestone.

## Quick Start

1. Run `python3 scripts/init_run.py --task "<user request>" --workspace <repo-or-dir>`.
   Optional: use `--prompt-format compact` to generate JSON-like stage packets with lower prompt overhead.
   Optional: use `--intake-research research-first|local-first|local-only` to control whether intake performs web research before finalizing stages.
   Optional: use `--stage-research research-first|local-first|local-only` to control whether solver, review, execution, and verification may browse for external guidance during their work.
   Optional: use `--execution-network fetch-if-needed|local-only` to control whether execution may download missing dependencies or artifacts.
   Optional: use `--cache-root` and `--cache-policy reuse|refresh|off` to control shared cache for research notes, downloads, wheels, models, and verification artifacts.
2. Open the generated run directory and read `plan.json`.
3. Build and use the control-plane binary:
   - `cargo build --release`
   - `./target/release/agpipe`
   - `./target/release/agpipe runs <agent-runs-dir>`
   - `./target/release/agpipe resume <run_dir> --until verification`
   - `./target/release/agpipe amend <run_dir> --note "<new user correction>" --rewind intake`
   - `./target/release/agpipe rm <run_dir>`
   - `./target/release/agpipe prune-runs <agent-runs-dir> --keep 20 --older-than-days 14`
4. Python fallback commands still exist:
   - `./target/release/agpipe run --task "<user request>" --workspace <repo-or-dir> --until review`
   - `./target/release/agpipe resume <run_dir> --until verification`
   - `./target/release/agpipe interview --task "<user request>" --workspace <repo-or-dir>`
   - `python3 scripts/run_stage.py <run_dir> status`
   - `python3 scripts/run_stage.py <run_dir> doctor`
   - `python3 scripts/run_stage.py <run_dir> next`
   - `python3 scripts/run_stage.py <run_dir> start-next`
   - `python3 scripts/run_stage.py <run_dir> start-solvers`
   - `python3 scripts/run_stage.py <run_dir> host-probe --refresh`
   - `python3 scripts/run_stage.py <run_dir> cache-status --refresh`
   - `python3 scripts/run_stage.py <run_dir> recheck verification`
   - `python3 scripts/run_stage.py <run_dir> step-back verification`
   - `python3 scripts/run_stage.py <run_dir> refresh-prompt verification`
   - `python3 scripts/run_stage.py <run_dir> refresh-prompts`
   - or open the TUI directly: `./target/release/agpipe ui --root <agent-runs-dir>`
4. If you want manual control, use:
   - `python3 scripts/run_stage.py <run_dir> show intake`
   - `python3 scripts/run_stage.py <run_dir> copy solver-a`
   - `python3 scripts/run_stage.py <run_dir> start review`
5. `start-next` launches all pending solver stages in parallel when the next work is the solver batch. If the runtime cannot spawn multiple agents, run solver passes sequentially but do not read sibling solver outputs until the review stage.

## Workflow

### 0. Interview And Task Finalization

When the raw request is underspecified, use the stage0 interview flow before `init_run.py`.

The interview flow should:

- preserve the original goal
- inspect the workspace when that materially changes the questions
- ask the domain-specific questions actually needed for decomposition, implementation, and goal verification
- avoid generic filler questions
- generate a final normalized task prompt that becomes the real input to `init_run.py`

Use `./target/release/agpipe interview ...` when you want the questions without creating a run yet.
Use `./target/release/agpipe run ...` when you want the interview, run creation, and autopilot execution in one command.

### 1. Intake And Prompt Builder

Use the first-level agent to normalize the request before any solving begins.

Do all of the following:

- Preserve the original requested outcome as the top-level objective.
- Rewrite the raw task into a precise execution brief.
- Verify the workspace path. If it is missing, either correct it or explicitly treat the run as greenfield planning.
- Confirm or correct task kind, complexity, and solver count from `plan.json`.
- Decompose compound requests into concrete workstreams instead of replacing the goal with a smaller safe deliverable.
- Maintain `goal_checks` in `plan.json` so later stages can tell whether the original goal is actually complete.
- Record `host_facts` in `plan.json` at run creation time, including source and capture time, and treat them as authoritative local execution facts for device-aware work.
- Use `run_stage.py <run_dir> host-probe --refresh` when device-sensitive work depends on the current launcher environment. This writes the latest probe to `host/probe.json` and appends a timestamped snapshot under `host/probes/` with the actual local Python, torch, and visible ML-related env keys.
- Follow `intake_research_mode` from `plan.json`. Default behavior is `research-first`: gather external solution context before finalizing the brief and stage decomposition.
- Record and preserve `stage_research_mode` in `plan.json` so downstream stages know whether they may browse externally while solving, reviewing, implementing, or verifying.
- When cache policy is `reuse`, consult and update shared research cache before duplicating external research.
- Choose the minimal additional skills needed for downstream work.
- Finalize the stage prompts without solving the task itself.

The intake brief must contain:

- original requested outcome
- objective
- deliverable
- goal coverage matrix
- workstream decomposition
- scope boundaries
- repo or workspace path
- constraints
- definition of done
- validation expectations
- open questions that can be answered from local context

Use `references/agency-role-map.md` only for the relevant task kind. If the task is about Codex skills, use `skill-creator`. If the task is about listing or installing skills, use `skill-installer`.

If the task contains several subsystems, produce named workstreams such as:

- API and ingress
- model or LLM stage
- renderer or executor
- persistence
- evaluation and safety

If you introduce milestones such as "phase 1" or "first iteration", state them as an execution plan under the preserved top-level goal. Do not let the milestone become the new goal unless the user explicitly asked for that reduction.

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
- the current `stage_research_mode` from `plan.json`

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
- write a short user-facing review summary in the selected language, default `ru`
- recommend one winner, one backup, or a compatible hybrid
- follow `stage_research_mode` from `plan.json` when deciding whether to browse for external fact-checking or comparable solution patterns

Treat "good architecture only" as insufficient when the user asked for a working service or runnable MVP. A scaffold can be part of the recommendation, but only as an intermediate step or partial solution unless the brief explicitly narrows the deliverable.

Adopt the skeptical stance from `agency-agents/testing/testing-reality-checker.md` when the local `agency-agents` repo exists. Use the concise reporting style from `agency-agents/support/support-executive-summary-generator.md` for the final summary.

Pause after review when human input is needed. The user-facing summary exists so a human can accept the winner, request corrections, or adjust the brief before execution continues.

### 4. Execution Stage

Run execution only after review is complete.

The execution stage must:

- read the review verdict before changing the workspace
- implement the recommended winner or explicit hybrid in the primary workspace
- follow `execution_network_mode` from `plan.json`; default behavior is `fetch-if-needed`
- follow `stage_research_mode` from `plan.json`; default behavior is `local-first`
- obey `host_facts.preferred_torch_device` from `plan.json` for torch-based training and inference unless a validated blocker forces a different device
- compare `host_facts` with the latest `host/probe.json` from the launcher when device-sensitive validation matters, and explicitly record drift such as `plan=mps` vs `probe=cpu`
- when cache policy is `reuse`, prefer cached downloads, wheels, repos, and model artifacts before fetching again
- when fetches are needed, record exact install/download commands, sources, versions, and fetched artifacts
- prefer the cheapest working slice that still preserves the top-level goal
- run targeted validation after edits
- write an execution report with changed files, validation, blockers, and next steps

If local constraints force a deviation from the review winner, state the reason directly in the execution report.

### 5. Verification And Improvement Seed

Run verification only after execution is complete.

The verification stage must:

- inspect the actual workspace implementation
- follow `stage_research_mode` from `plan.json` when deciding whether external docs or primary sources are needed to validate a blocker or claim
- review it in code-review mode, with findings ordered by severity
- run the cheapest relevant validation it can
- write a localized verification summary for the user
- write `verification/goal-status.json` so the launcher can distinguish stage completion from actual goal completion
- generate an improvement request that can seed the next run against the existing codebase
- generate `verification/augmented-task.md` so `rerun` can reuse a fuller follow-up prompt without manual reconstruction

Use `references/verification-rubric.md` for the findings format and improvement-request rules.

## Operating Rules

- Keep every stage file-based so the pipeline can be resumed or audited later.
- Preserve solver independence until the reviewer stage.
- Prefer complementary solver roles over duplicates.
- Do not merge incompatible fragments from different solvers without stating the hybrid explicitly.
- Run targeted validation before broad or expensive test suites.
- If tests cannot run, say exactly why and treat that as a review penalty.
- Keep the final reviewer summary short and decision-oriented.
- Keep the user-facing review summary concise and in the language requested by `plan.json`.
- Keep verification findings evidence-based and scoped to actual implemented code.
- Do not treat "all stages ran" as equivalent to "the user's goal is complete". Use the goal coverage gate.

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
    user-summary.md
  execution/
    report.md
  verification/
    findings.md
    goal-status.json
    user-summary.md
    improvement-request.md
    augmented-task.md
```

Read `plan.json` first. It contains the normalized metadata, role assignments, stack signals, summary language, and suggested validation commands.

## Resources

### `scripts/init_run.py`

Generate a reusable run directory with prompts, role assignments, and review hints.

### `scripts/run_stage.py`

Inspect run status, run `doctor` consistency and staleness checks, print localized summaries and verification findings, print the augmented follow-up task, capture launcher-side host probes with history, inspect and prune shared cache, regenerate one prompt with `refresh-prompt` or all prompts with `refresh-prompts`, rerun verification safely with `recheck verification`, rewind a stage back to pending with `step-back`, compile stage prompts with absolute references, copy prompts to the clipboard, launch `codex exec` for a chosen stage, run pending solvers in parallel, use the Rust `agpipe` binary as the primary TUI-first control-plane, keep `scripts/map.py` only as an internal fallback for flows that are not fully ported yet, and surface `next: rerun` when verification says the goal is still incomplete.

When the user wants to refine an existing run instead of starting a new one, record the correction in `amendments.md` and treat it as the latest authoritative user input. By default `agpipe amend` should rewind to `intake`, because new product corrections usually need to update the brief and downstream prompts before solving continues.

### `references/agency-role-map.md`

Map task kinds to recommended roles from the local `agency-agents` catalog and Codex skills.

### `references/decomposition-rules.md`

Guide intake to preserve the requested outcome while splitting multi-part tasks into workstreams and milestones.

### `references/review-rubric.md`

Define the reviewer scorecard, summary format, and validation heuristics.

### `references/verification-rubric.md`

Define the post-execution audit format, goal completion gate, findings expectations, localized verification summary, and the follow-up improvement request.
