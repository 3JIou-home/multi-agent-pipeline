# Agency Role Map

Use this file only when selecting roles for the current task. Prefer the smallest set that gives genuine solution diversity.

If a local checkout of `https://github.com/msitarzewski/agency-agents` is available, use it as the role catalog. Prefer `AGENCY_AGENTS_DIR=/path/to/agency-agents` when the repo is not in a standard location. If no checkout is available, keep the role names and adapt them to the local agent library.

## Level 1 Roles

Use these roles to build the intake brief and downstream prompt packets:

- `project-manager-senior` for turning a vague request into concrete work items
- `project-management-project-shepherd` for sequencing and handoffs
- `engineering-technical-writer` for prompt cleanup and exact requirements
- `skill-creator` when the task is to create or update a Codex skill
- `skill-installer` when the task is to list or install Codex skills

Use `specialized/agents-orchestrator.md` as conceptual inspiration only. This skill replaces it with a stricter three-level pipeline.

## Solver Role Matrix

### AI And Agentic Systems

- `engineering/engineering-ai-engineer.md`
- `engineering/engineering-backend-architect.md`
- `engineering/engineering-rapid-prototyper.md`

Use when the request is about LLMs, training or fine-tuning, inference pipelines, Telegram or chat integrations, code generation, agentic flows, or model-backed automation.

### Frontend

- `engineering/engineering-frontend-developer.md`
- `design/design-ui-designer.md`
- `design/design-ux-architect.md`

Use when the request is about UI, components, CSS, pages, accessibility, or browser behavior.

### Backend

- `engineering/engineering-backend-architect.md`
- `engineering/engineering-senior-developer.md`
- `engineering/engineering-devops-automator.md`

Use when the request is about APIs, services, databases, queues, or server-side logic.

### Fullstack

- `engineering/engineering-senior-developer.md`
- `engineering/engineering-frontend-developer.md`
- `engineering/engineering-backend-architect.md`

Use when the request spans UI plus server behavior or multiple layers of the same app.

### Infra

- `engineering/engineering-devops-automator.md`
- `support/support-infrastructure-maintainer.md`
- `engineering/engineering-security-engineer.md`

Use when the request touches CI/CD, containers, cloud resources, deployment, observability, or operational hardening.

### Security

- `engineering/engineering-security-engineer.md`
- `testing/testing-tool-evaluator.md`
- `support/support-legal-compliance-checker.md`

Use when the task is about vulnerability review, auth, secrets, policy, compliance, or secure-by-default changes.

### Documentation

- `engineering/engineering-technical-writer.md`
- `support/support-executive-summary-generator.md`
- `project-management/project-management-studio-operations.md`

Use when the main output is documentation, guides, runbooks, or condensed summaries.

### Research And Comparison

- `product/product-trend-researcher.md`
- `testing/testing-tool-evaluator.md`
- `support/support-analytics-reporter.md`

Use when the task is to compare tools, evaluate options, or produce a recommendation.

### Skills And Prompting

- `skill-creator`
- `engineering/engineering-technical-writer.md`
- `project-manager-senior`

Use when the task is to build skills, orchestrators, prompt libraries, or agent workflows.

## Reviewer Stack

Start from this default reviewer stack and add domain-specific reviewers only when needed:

- `testing/testing-reality-checker.md`
- `testing/testing-test-results-analyzer.md`
- `support/support-executive-summary-generator.md`

Add these when relevant:

- `testing/testing-api-tester.md` for API changes
- `testing/testing-accessibility-auditor.md` for UI changes
- `engineering/engineering-security-engineer.md` for security-sensitive diffs
- `testing/testing-performance-benchmarker.md` for latency or load concerns

## Diversity Rules

- Do not assign three solvers with near-identical roles.
- Keep at least one solver close to implementation and one focused on architecture or risk when using more than one solver.
- Use one reviewer voice only for the final verdict, even if several reviewer roles contribute evidence.
