#!/usr/bin/env python3
"""Scaffold a three-level multi-agent pipeline run directory."""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable


ROLE_MATRIX = {
    "ai": [
        "engineering/engineering-ai-engineer.md",
        "engineering/engineering-backend-architect.md",
        "engineering/engineering-rapid-prototyper.md",
    ],
    "frontend": [
        "engineering/engineering-frontend-developer.md",
        "design/design-ui-designer.md",
        "design/design-ux-architect.md",
    ],
    "backend": [
        "engineering/engineering-backend-architect.md",
        "engineering/engineering-senior-developer.md",
        "engineering/engineering-devops-automator.md",
    ],
    "fullstack": [
        "engineering/engineering-senior-developer.md",
        "engineering/engineering-frontend-developer.md",
        "engineering/engineering-backend-architect.md",
    ],
    "infra": [
        "engineering/engineering-devops-automator.md",
        "support/support-infrastructure-maintainer.md",
        "engineering/engineering-security-engineer.md",
    ],
    "security": [
        "engineering/engineering-security-engineer.md",
        "testing/testing-tool-evaluator.md",
        "support/support-legal-compliance-checker.md",
    ],
    "docs": [
        "engineering/engineering-technical-writer.md",
        "support/support-executive-summary-generator.md",
        "project-management/project-management-studio-operations.md",
    ],
    "research": [
        "product/product-trend-researcher.md",
        "testing/testing-tool-evaluator.md",
        "support/support-analytics-reporter.md",
    ],
    "skill": [
        "skill-creator",
        "engineering/engineering-technical-writer.md",
        "project-manager-senior",
    ],
}

REVIEWER_STACK = [
    "testing/testing-reality-checker.md",
    "testing/testing-test-results-analyzer.md",
    "support/support-executive-summary-generator.md",
]

ANGLE_SEQUENCE = [
    "implementation-first",
    "architecture-first",
    "risk-first",
]


@dataclass
class StackSignals:
    package_json: bool = False
    pyproject_toml: bool = False
    pytest_suite: bool = False
    go_mod: bool = False
    cargo_toml: bool = False
    makefile: bool = False
    terraform: bool = False


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--task", help="Raw user request text")
    parser.add_argument("--task-file", help="Path to a file containing the raw task")
    parser.add_argument("--workspace", default=".", help="Workspace path for repo inspection")
    parser.add_argument(
        "--output-dir",
        default="agent-runs",
        help="Directory where run folders will be created",
    )
    parser.add_argument("--title", help="Optional short title for the run directory")
    parser.add_argument(
        "--task-kind",
        choices=["auto", *ROLE_MATRIX.keys()],
        default="auto",
        help="Task kind override",
    )
    parser.add_argument(
        "--complexity",
        choices=["auto", "low", "medium", "high"],
        default="auto",
        help="Complexity override",
    )
    parser.add_argument(
        "--solver-count",
        type=int,
        choices=[1, 2, 3],
        help="Explicit solver count override",
    )
    return parser.parse_args()


def read_task(args: argparse.Namespace) -> str:
    if args.task:
        return args.task.strip()
    if args.task_file:
        return Path(args.task_file).read_text(encoding="utf-8").strip()
    raise SystemExit("Provide --task or --task-file.")


def slugify(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return cleaned[:48] or "task"


def infer_task_kind(task: str) -> str:
    text = task.lower()

    ai_words = [
        "ai",
        "ml",
        "llm",
        "llama",
        "lama",
        "fine-tune",
        "finetune",
        "train model",
        "rag",
        "embedding",
        "inference",
        "telegram",
        "freecad",
        "нейросет",
        "модель",
        "дообуч",
        "обуч",
        "телеграм",
    ]
    frontend_words = [
        "frontend",
        "ui",
        "ux",
        "css",
        "html",
        "react",
        "vue",
        "page",
        "component",
        "фронтенд",
        "интерфейс",
        "страница",
        "компонент",
        "верстк",
    ]
    backend_words = [
        "backend",
        "api",
        "database",
        "service",
        "queue",
        "worker",
        "sql",
        "бэкенд",
        "бекенд",
        "сервис",
        "база данных",
        "очеред",
    ]
    infra_words = [
        "deploy",
        "docker",
        "kubernetes",
        "terraform",
        "ansible",
        "ci",
        "cd",
        "infra",
        "деплой",
        "инфра",
        "инфраструктур",
        "контейнер",
        "k8s",
    ]
    security_words = [
        "security",
        "vulnerability",
        "auth",
        "secret",
        "token",
        "compliance",
        "audit",
        "безопас",
        "уязвим",
        "аудит",
        "секрет",
        "токен",
        "авторизац",
    ]
    docs_words = [
        "docs",
        "documentation",
        "readme",
        "guide",
        "summary",
        "spec",
        "документац",
        "гайд",
        "резюме",
        "спек",
        "описан",
        "readme",
    ]
    research_words = [
        "compare",
        "evaluate",
        "research",
        "recommend",
        "choose",
        "options",
        "сравн",
        "оцен",
        "исслед",
        "рекомен",
        "выбор",
        "вариант",
    ]
    skill_words = [
        "skill",
        "prompt",
        "codex",
        "скил",
        "промт",
        "кодекс",
    ]

    ai_hits = sum(word in text for word in ai_words)
    frontend_hits = sum(word in text for word in frontend_words)
    backend_hits = sum(word in text for word in backend_words)

    if sum(word in text for word in skill_words) >= 2:
        return "skill"
    if ai_hits >= 2:
        return "ai"
    if sum(word in text for word in security_words) >= 2:
        return "security"
    if sum(word in text for word in infra_words) >= 2:
        return "infra"
    if sum(word in text for word in docs_words) >= 2:
        return "docs"
    if sum(word in text for word in research_words) >= 2:
        return "research"
    if frontend_hits and backend_hits:
        return "fullstack"
    if frontend_hits:
        return "frontend"
    if backend_hits:
        return "backend"
    return "fullstack"


def infer_complexity(task: str) -> str:
    text = task.lower()
    score = 0
    score += min(len(text.split()) // 30, 3)
    score += sum(
        keyword in text
        for keyword in [
            "architecture",
            "pipeline",
            "workflow",
            "refactor",
            "migrate",
            "production",
            "several",
            "multiple",
            "compare",
            "orchestr",
            "архитект",
            "конвейер",
            "пайплайн",
            "несколько",
            "сравн",
            "оркестр",
            "агент",
            "промт",
            "тест",
            "резюме",
            "вариант",
            "review",
            "цензор",
        ]
    )
    if any(word in text for word in ["agent", "агент"]) and any(
        word in text for word in ["multiple", "several", "несколько", "вариант"]
    ):
        score += 2
    if any(word in text for word in ["pipeline", "workflow", "конвейер", "пайплайн"]) and any(
        word in text for word in ["test", "review", "тест", "цензор", "резюме"]
    ):
        score += 2
    if score >= 6:
        return "high"
    if score >= 3:
        return "medium"
    return "low"


def solver_count_for(complexity: str) -> int:
    return {"low": 1, "medium": 2, "high": 3}[complexity]


def detect_stack(workspace: Path) -> StackSignals:
    signals = StackSignals()
    if not workspace.exists():
        return signals
    max_depth = len(workspace.parts) + 3
    for path in workspace.rglob("*"):
        if any(part in {".git", "node_modules", ".venv", "venv", "__pycache__"} for part in path.parts):
            continue
        if len(path.parts) > max_depth:
            continue
        name = path.name
        if name == "package.json":
            signals.package_json = True
        elif name == "pyproject.toml":
            signals.pyproject_toml = True
        elif name == "pytest.ini" or name == "conftest.py":
            signals.pytest_suite = True
        elif name == "go.mod":
            signals.go_mod = True
        elif name == "Cargo.toml":
            signals.cargo_toml = True
        elif name == "Makefile":
            signals.makefile = True
        elif path.suffix == ".tf":
            signals.terraform = True
        elif name == "tests" and path.is_dir():
            signals.pytest_suite = True
    return signals


def extract_package_scripts(workspace: Path) -> list[str]:
    scripts: list[str] = []
    if not workspace.exists():
        return scripts
    for package_json in workspace.rglob("package.json"):
        if any(part in {".git", "node_modules"} for part in package_json.parts):
            continue
        try:
            data = json.loads(package_json.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        pkg_scripts = data.get("scripts", {})
        for name in ("test", "lint", "build"):
            if name in pkg_scripts:
                command = "npm test" if name == "test" else f"npm run {name}"
                if command not in scripts:
                    scripts.append(command)
        if scripts:
            break
    return scripts


def makefile_has_target(workspace: Path, target: str) -> bool:
    if not workspace.exists():
        return False
    for makefile in workspace.rglob("Makefile"):
        if any(part in {".git", "node_modules"} for part in makefile.parts):
            continue
        try:
            content = makefile.read_text(encoding="utf-8")
        except OSError:
            continue
        if re.search(rf"^{re.escape(target)}\s*:", content, re.MULTILINE):
            return True
    return False


def build_validation_commands(workspace: Path, signals: StackSignals) -> list[str]:
    commands: list[str] = []
    if signals.package_json:
        commands.extend(extract_package_scripts(workspace))
    if signals.pyproject_toml or signals.pytest_suite:
        commands.append("pytest")
    if signals.go_mod:
        commands.append("go test ./...")
    if signals.cargo_toml:
        commands.append("cargo test")
    if signals.makefile and makefile_has_target(workspace, "test"):
        commands.append("make test")
    if signals.terraform:
        commands.append("terraform validate")

    deduped: list[str] = []
    for command in commands:
        if command not in deduped:
            deduped.append(command)
    return deduped


def choose_roles(task_kind: str, solver_count: int) -> list[dict[str, str]]:
    roles = ROLE_MATRIX[task_kind][:solver_count]
    selected = []
    for index, role in enumerate(roles):
        selected.append(
            {
                "solver_id": f"solver-{chr(ord('a') + index)}",
                "role": role,
                "angle": ANGLE_SEQUENCE[index],
            }
        )
    return selected


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content.rstrip() + "\n", encoding="utf-8")


def bullet_list(values: Iterable[str]) -> str:
    items = list(values)
    if not items:
        return "- none"
    return "\n".join(f"- {value}" for value in items)


def render_request(
    task: str,
    workspace: Path,
    workspace_exists: bool,
    task_kind: str,
    complexity: str,
    solver_count: int,
) -> str:
    workspace_note = "present" if workspace_exists else "missing"
    warnings = ""
    if not workspace_exists:
        warnings = "\n## Warning\n\n- Workspace path does not exist. Treat this run as greenfield planning until the path is corrected.\n"
    return f"""# Raw Request

{task}

## Environment

- Workspace: `{workspace}`
- Workspace status: `{workspace_note}`
- Task kind guess: `{task_kind}`
- Complexity guess: `{complexity}`
- Suggested solver count: `{solver_count}`
{warnings}"""


def render_intake_prompt(
    run_dir: Path,
    workspace_exists: bool,
    task_kind: str,
    complexity: str,
    solver_count: int,
    validation_commands: list[str],
) -> str:
    workspace_warning = ""
    if not workspace_exists:
        workspace_warning = "\n- workspace is missing; correct the path or explicitly treat this as a greenfield planning run\n"
    return f"""# Level 1: Intake And Prompt Builder

Read:

- `{run_dir / 'request.md'}`
- `{run_dir / 'plan.json'}`
- `references/agency-role-map.md`

Your job is to prepare the pipeline, not solve the task.

Produce or update these artifacts:

- `brief.md` in the run directory with objective, deliverable, scope, constraints, definition of done, and validation expectations
- `plan.json` if task kind, complexity, solver count, or roles need adjustment
- prompt packets in `prompts/` if the downstream prompts need refinement

Current defaults:

- workspace exists: `{str(workspace_exists).lower()}`
- task kind: `{task_kind}`
- complexity: `{complexity}`
- solver count: `{solver_count}`
- suggested validation:
{bullet_list(validation_commands)}

Rules:

- keep the brief precise and execution-ready
- add only the minimal extra skills the downstream stages need
- do not implement the solution in this stage
- if the task is about Codex skills, prefer `skill-creator` or `skill-installer` over ad hoc instructions
{workspace_warning}"""


def render_solver_prompt(
    run_dir: Path,
    solver_id: str,
    role: str,
    angle: str,
    validation_commands: list[str],
) -> str:
    result_file = run_dir / "solutions" / solver_id / "RESULT.md"
    return f"""# Level 2: {solver_id}

Assigned role: `{role}`
Solution angle: `{angle}`

Read:

- `{run_dir / 'request.md'}`
- `{run_dir / 'brief.md'}` if it exists
- `{run_dir / 'plan.json'}`

Do not read sibling solution files.

Deliver:

- write your solution summary to `{result_file}`
- include assumptions, approach, implementation or patch summary, validation performed, and unresolved risks

Validation hints:
{bullet_list(validation_commands)}

Rules:

- solve the task from your assigned angle
- keep the output self-contained
- if you changed code, say exactly what you validated
- if you could not validate, say exactly why
"""


def render_review_prompt(run_dir: Path, validation_commands: list[str], reviewers: list[str]) -> str:
    solution_files = sorted((run_dir / "solutions").glob("*/RESULT.md"))
    solution_lines = "\n".join(f"- `{path}`" for path in solution_files)
    return f"""# Level 3: Censor And Reviewer

Read:

- `{run_dir / 'request.md'}`
- `{run_dir / 'brief.md'}` if it exists
- `{run_dir / 'plan.json'}`
- `references/review-rubric.md`
- all solver outputs:
{solution_lines}

Reviewer stack:
{bullet_list(reviewers)}

Validation suggestions:
{bullet_list(validation_commands)}

Deliver:

- `review/report.md` with a short summary for each solver and a final recommendation
- `review/scorecard.json` with numeric scores per solver

Rules:

- compare every solution against the brief, not against style preferences
- run the cheapest relevant validation first when code or config changed
- treat missing evidence as a penalty
- recommend a hybrid only when the parts are clearly compatible
"""


def main() -> None:
    args = parse_args()
    task = read_task(args)
    workspace = Path(args.workspace).resolve()
    workspace_exists = workspace.exists()
    title = args.title or " ".join(task.split()[:8])
    task_kind = infer_task_kind(task) if args.task_kind == "auto" else args.task_kind
    complexity = infer_complexity(task) if args.complexity == "auto" else args.complexity
    solver_count = args.solver_count or solver_count_for(complexity)
    signals = detect_stack(workspace)
    validation_commands = build_validation_commands(workspace, signals)
    roles = choose_roles(task_kind, solver_count)

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    run_dir = Path(args.output_dir).resolve() / f"{timestamp}-{slugify(title)}"
    run_dir.mkdir(parents=True, exist_ok=False)
    (run_dir / "prompts").mkdir()
    (run_dir / "solutions").mkdir()
    (run_dir / "review").mkdir()

    plan = {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "workspace": str(workspace),
        "workspace_exists": workspace_exists,
        "task_kind": task_kind,
        "complexity": complexity,
        "solver_count": solver_count,
        "solver_roles": roles,
        "reviewer_stack": REVIEWER_STACK,
        "stack_signals": asdict(signals),
        "validation_commands": validation_commands,
        "references": {
            "role_map": "references/agency-role-map.md",
            "review_rubric": "references/review-rubric.md",
        },
    }

    write_text(
        run_dir / "request.md",
        render_request(task, workspace, workspace_exists, task_kind, complexity, solver_count),
    )
    write_text(run_dir / "brief.md", "# Brief\n\nPending intake stage.\n")
    write_text(run_dir / "plan.json", json.dumps(plan, indent=2))
    write_text(
        run_dir / "prompts" / "level1-intake.md",
        render_intake_prompt(run_dir, workspace_exists, task_kind, complexity, solver_count, validation_commands),
    )

    for role_data in roles:
        solver_id = role_data["solver_id"]
        write_text(
            run_dir / "prompts" / f"level2-{solver_id}.md",
            render_solver_prompt(
                run_dir,
                solver_id=solver_id,
                role=role_data["role"],
                angle=role_data["angle"],
                validation_commands=validation_commands,
            ),
        )
        write_text(
            run_dir / "solutions" / solver_id / "RESULT.md",
            "# Result\n\nFill this file with the solver output.\n",
        )

    write_text(
        run_dir / "prompts" / "level3-review.md",
        render_review_prompt(run_dir, validation_commands, REVIEWER_STACK),
    )
    write_text(run_dir / "review" / "report.md", "# Review Report\n\nPending review stage.\n")
    write_text(run_dir / "review" / "scorecard.json", "{}\n")

    print(run_dir)


if __name__ == "__main__":
    main()
