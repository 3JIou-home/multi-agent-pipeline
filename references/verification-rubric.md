# Verification Rubric

Use this rubric in the post-execution verification stage. This stage reviews the actual implementation in the workspace, not the pre-execution solver proposals.

## Primary Goal

Answer these questions:

- did execution implement a meaningful slice of the requested system
- did execution cover the critical `goal_checks` recorded in `plan.json`
- what verified defects or regressions remain
- is a follow-up improvement run justified

## Review Mode

Default to code-review mindset:

- findings first
- order by severity
- emphasize bugs, regressions, unsafe behavior, missing validation, and overclaims
- reference files and lines when possible

If there are no meaningful findings, say that explicitly.

## Evidence Rules

Prefer the cheapest relevant validation first:

- targeted unit tests
- syntax or compile checks
- direct reproduction commands
- file inspection

Scope discipline:

- start from `execution/report.md` and review artifacts before reading additional code
- prefer files explicitly named in execution evidence and files directly implicated by failing validation
- do not read solver stdout logs unless direct evidence makes them necessary
- avoid broad repo scans when a narrower evidence path can answer the question

If validation cannot run, record:

- the exact blocked command
- why it was blocked
- how that uncertainty affects confidence

## Findings Format

Write findings as short standalone items:

```markdown
1. High: <problem summary>. [file.py](/abs/path/file.py#L10)
```

After findings, include:

- open questions or assumptions
- short change summary or residual risks

## Goal Status

Generate `verification/goal-status.json`.

It should be machine-readable and must answer whether the original goal is actually complete, not just whether all stages ran.

Required keys:

```json
{
  "goal_complete": false,
  "goal_verdict": "partial",
  "covered_checks": ["example_check"],
  "missing_checks": ["critical_missing_check"],
  "rerun_recommended": true,
  "recommended_next_action": "rerun",
  "reason": "Critical photo-aware analysis is still missing."
}
```

Rules:

- set `goal_complete` to `false` when any critical `goal_checks` item remains missing, unverified, mocked, or replaced by a placeholder flow
- use `goal_verdict=complete` only when the critical user-visible goal is actually implemented and verified to the level allowed by the environment
- if the stage sequence finished but the product goal is still incomplete, the verdict should still be `partial` or `blocked`

## User Summary

Write a short human-facing summary in the language requested by `plan.json`.

Keep it brief and decision-oriented:

- overall result
- top issues
- whether a follow-up run is recommended
- what to do next

## Improvement Request

Generate `verification/improvement-request.md` as the narrow seed task for the next run.

Generate `verification/augmented-task.md` as the fuller follow-up prompt for the next run.

The improvement request should:

- preserve the original product goal
- target the verified defects only
- mention the current workspace and existing implementation
- be implementation-oriented, not architecture-oriented
- avoid repeating defects that were not actually verified

The augmented task should:

- preserve the original user goal verbatim or as close as possible
- summarize verified progress that must not regress
- list only verified blockers and evidence-backed next fixes
- record do-not-regress constraints
- define the next done state clearly enough that `rerun` can use it without manual prompt rewriting
