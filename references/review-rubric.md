# Review Rubric

Use this rubric in the level 3 censor/reviewer stage. Default to skepticism until evidence supports a stronger verdict.

## Score Dimensions

Score each solution from `0` to `5` on every dimension:

- `task_fit`: how closely the solution matches the normalized brief
- `correctness`: whether the reasoning and implementation appear technically sound
- `evidence`: whether tests, commands, or direct inspection support the claims
- `maintainability`: how easy the solution is to merge, extend, and support
- `risk`: lower operational or product risk earns a higher score
- `clarity`: how easy it is to understand the approach and tradeoffs

Use short written justification for every score below `4`.

## Validation Rules

Run the cheapest relevant validation first. Prefer narrow checks before full suites.

Common signals and default commands:

- `package.json`: run `npm test`, `npm run lint`, or `npm run build` only if the script exists
- `pyproject.toml`, `pytest.ini`, or `tests/`: run `pytest`
- `go.mod`: run `go test ./...`
- `Cargo.toml`: run `cargo test`
- `Makefile`: run `make test` only if the target exists
- `*.tf`: run `terraform validate` if Terraform is available

If none of the above apply, use direct evidence:

- file inspection
- static validation
- syntax checks
- targeted reproduction steps

If validation cannot run, record:

- the exact blocked command
- why it was blocked
- how that uncertainty affects the verdict

## Summary Format

Write one short block per solution:

```markdown
## Solver A
Summary: <one sentence>
Strongest point: <one sentence>
Main risk: <one sentence>
Verdict: <merge as-is | merge with fixes | keep as backup | reject>
```

After the per-solution summaries, write:

- the recommended winner
- the backup option if the winner is blocked
- a hybrid recommendation only if the parts are clearly compatible
- the exact validation evidence used

## Decision Rules

- Prefer the solution with the best evidence, not the most ambitious design.
- Penalize claims that were not validated.
- Penalize broad changes when a narrower fix satisfies the brief.
- If two solutions are close, prefer the one with lower merge risk.
- If all solutions have evidence gaps, say so directly instead of forcing approval.
