# Verification Rubric

Use this rubric in the post-execution verification stage. This stage reviews the actual implementation in the workspace, not the pre-execution solver proposals.

## Primary Goal

Answer these questions:

- did execution implement a meaningful slice of the requested system
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

## User Summary

Write a short human-facing summary in the language requested by `plan.json`.

Keep it brief and decision-oriented:

- overall result
- top issues
- whether a follow-up run is recommended
- what to do next

## Improvement Request

Generate `verification/improvement-request.md` as the seed task for the next run.

The improvement request should:

- preserve the original product goal
- target the verified defects only
- mention the current workspace and existing implementation
- be implementation-oriented, not architecture-oriented
- avoid repeating defects that were not actually verified
