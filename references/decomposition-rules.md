# Decomposition Rules

Use this file during intake when the request includes multiple subsystems, delivery stages, or several hard technical concerns.

## Core Rule

Do not replace the user's requested outcome with a safer but smaller problem.

Allowed:

- split the work into workstreams
- define milestones
- state blockers and assumptions
- suggest a phase order

Not allowed without explicit justification:

- changing "build a service" into "write a scaffold"
- changing "implement the system" into "produce architecture only"
- dropping major subsystems from the goal

If you must narrow scope, record it as:

- `preserved_goal`
- `interim_milestone`
- `reason_for_gap`
- `remaining_work`

## Intake Output Shape

For compound tasks, include a `workstream decomposition` section in `brief.md` with:

- workstream name
- purpose
- expected output
- suggested role
- dependencies

## Common Workstream Patterns

### AI Product Orchestration

Use this pattern for requests involving models, app integrations, and code generation:

- ingress and API surface
- model or prompt pipeline
- domain compiler or renderer
- safety and policy layer
- persistence or job orchestration
- evaluation and training loop

### Example: Telegram -> LLM -> FreeCAD

For a request like "user sends photo and dimensions in Telegram, model produces FreeCAD code":

Preserved goal:

- working service that accepts Telegram input and returns constrained FreeCAD-oriented output

Recommended workstreams:

- `telegram-ingress`
  - receive message, photo, and structured dimensions
- `vision-or-analysis`
  - convert photo into structured scene or geometry observations
- `cad-planning`
  - convert observations plus dimensions into a constrained plan
- `freecad-rendering`
  - translate the constrained plan into deterministic FreeCAD Python
- `safety-and-review`
  - validate supported shapes, confidence, and execution limits
- `evaluation-and-finetuning`
  - define whether fine-tuning is needed after baseline measurements

What not to do:

- do not collapse the whole request into "make a phase-1 scaffold" unless that is explicitly marked as an interim milestone
- do not remove Telegram or FreeCAD from the target system just because the repo is empty

## Review Consequences

If a solver only delivers architecture or a scaffold for a request that still targets a working MVP:

- score down `task_fit`
- score down `evidence` if the solver overclaims completion
- allow it only as a backup or milestone recommendation, not as the final winner by default
