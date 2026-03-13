#!/usr/bin/env python3
"""Scaffold a five-level multi-agent pipeline run directory."""

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
    parser.add_argument(
        "--prompt-format",
        choices=["markdown", "compact"],
        default="markdown",
        help="Prompt packet format to generate",
    )
    parser.add_argument(
        "--summary-language",
        default="ru",
        help="Language for the user-facing review summary, default: ru",
    )
    parser.add_argument(
        "--intake-research",
        choices=["research-first", "local-first", "local-only"],
        default="research-first",
        help="How intake should gather context before finalizing the brief",
    )
    parser.add_argument(
        "--execution-network",
        choices=["fetch-if-needed", "local-only"],
        default="fetch-if-needed",
        help="Whether execution may download/install missing dependencies or artifacts",
    )
    parser.add_argument(
        "--cache-root",
        default="~/.cache/multi-agent-pipeline",
        help="Shared cache root for research notes, downloads, wheels, models, and verification artifacts",
    )
    parser.add_argument(
        "--cache-policy",
        choices=["reuse", "refresh", "off"],
        default="reuse",
        help="Whether stages should reuse shared cache, ignore it, or disable it",
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


def infer_execution_mode(task_kind: str, complexity: str, task: str) -> str:
    text = task.lower()
    compound_signals = [
        "telegram",
        "freecad",
        "api",
        "bot",
        "service",
        "worker",
        "pipeline",
        "workflow",
        "telegram",
        "телеграм",
        "сервис",
        "бот",
        "пайплайн",
        "конвейер",
    ]
    if task_kind in {"ai", "fullstack", "backend"} and complexity != "low":
        return "decomposed"
    if sum(signal in text for signal in compound_signals) >= 3:
        return "decomposed"
    return "alternatives"


def workstream_hints_for(task_kind: str, task: str) -> list[dict[str, str]]:
    text = task.lower()
    if task_kind == "ai" and any(word in text for word in ["telegram", "freecad", "телеграм", "freecad"]):
        return [
            {
                "name": "telegram-ingress",
                "goal": "accept photo, dimensions, and follow-up answers from Telegram",
                "suggested_role": "engineering/engineering-backend-architect.md",
            },
            {
                "name": "vision-or-analysis",
                "goal": "turn image input into grounded geometry observations",
                "suggested_role": "engineering/engineering-ai-engineer.md",
            },
            {
                "name": "cad-planning",
                "goal": "convert observations plus dimensions into a constrained CAD plan",
                "suggested_role": "engineering/engineering-ai-engineer.md",
            },
            {
                "name": "freecad-rendering",
                "goal": "translate the constrained plan into deterministic FreeCAD Python",
                "suggested_role": "engineering/engineering-rapid-prototyper.md",
            },
            {
                "name": "safety-and-evaluation",
                "goal": "validate supported shapes, unsafe plans, and whether fine-tuning is justified",
                "suggested_role": "testing/testing-reality-checker.md",
            },
        ]

    default_map = {
        "frontend": [
            {
                "name": "ui-implementation",
                "goal": "build the requested frontend surface",
                "suggested_role": "engineering/engineering-frontend-developer.md",
            },
            {
                "name": "ux-and-validation",
                "goal": "validate usability, accessibility, and interface constraints",
                "suggested_role": "design/design-ux-architect.md",
            },
        ],
        "backend": [
            {
                "name": "service-layer",
                "goal": "build the core service or API behavior",
                "suggested_role": "engineering/engineering-backend-architect.md",
            },
            {
                "name": "persistence-and-ops",
                "goal": "define storage, jobs, and operational boundaries",
                "suggested_role": "engineering/engineering-devops-automator.md",
            },
        ],
        "fullstack": [
            {
                "name": "entry-surface",
                "goal": "build the user-facing or API-facing entrypoint",
                "suggested_role": "engineering/engineering-frontend-developer.md",
            },
            {
                "name": "core-service",
                "goal": "build the core domain behavior and data flow",
                "suggested_role": "engineering/engineering-backend-architect.md",
            },
            {
                "name": "safety-and-review",
                "goal": "validate correctness, evidence, and operational risk",
                "suggested_role": "testing/testing-reality-checker.md",
            },
        ],
    }
    return default_map.get(task_kind, [])


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


def compact_list(values: Iterable[str]) -> list[str]:
    return list(values)


def compact_lines(packet: dict) -> str:
    return json.dumps(packet, ensure_ascii=False, indent=2) + "\n"


def build_cache_config(cache_root: Path, cache_policy: str) -> dict[str, object]:
    expanded_root = cache_root.expanduser().resolve()
    enabled = cache_policy != "off"
    paths = {
        "research": str(expanded_root / "research"),
        "downloads": str(expanded_root / "downloads"),
        "wheelhouse": str(expanded_root / "wheelhouse"),
        "models": str(expanded_root / "models"),
        "verification": str(expanded_root / "verification"),
    }
    return {
        "enabled": enabled,
        "root": str(expanded_root),
        "policy": cache_policy,
        "paths": paths,
    }


def ensure_cache_dirs(cache_config: dict[str, object]) -> None:
    if not cache_config.get("enabled"):
        return
    root = Path(str(cache_config["root"]))
    root.mkdir(parents=True, exist_ok=True)
    for path in cache_config.get("paths", {}).values():
        Path(str(path)).mkdir(parents=True, exist_ok=True)


def infer_goal_checks(
    task: str,
    task_kind: str,
    workstream_hints: list[dict[str, str]],
) -> list[dict[str, object]]:
    text = task.lower()
    checks: list[dict[str, object]] = []
    seen: set[str] = set()

    def add_check(check_id: str, requirement: str, critical: bool = True) -> None:
        if check_id in seen:
            return
        seen.add(check_id)
        checks.append(
            {
                "id": check_id,
                "requirement": requirement,
                "critical": critical,
            }
        )

    if "telegram" in text or "телеграм" in text:
        add_check(
            "telegram_ingress",
            "accept task input through Telegram or a clearly equivalent transport path",
        )
    if any(word in text for word in ["photo", "image", "photo", "фото", "изображ"]):
        add_check(
            "photo_used_as_input",
            "use the provided photo as a real analysis input, not only as a presence check",
        )
    if any(word in text for word in ["dimension", "dimensions", "size", "sizes", "размер", "габарит"]):
        add_check(
            "dimension_capture",
            "capture and apply the provided dimensions in the generated plan or model",
        )
    if any(
        word in text
        for word in [
            "llm",
            "llama",
            "lama",
            "model",
            "vision",
            "analysis",
            "нейросет",
            "модель",
            "дообуч",
            "обуч",
        ]
    ):
        add_check(
            "analysis_adapter",
            "implement or preserve an analysis path that turns the requested inputs into grounded observations or bounded classifications",
        )
    if "freecad" in text:
        add_check(
            "freecad_output",
            "produce deterministic FreeCAD output from the structured plan",
        )
    if task_kind in {"ai", "backend", "fullstack"} or any(
        word in text for word in ["service", "bot", "api", "сервис", "бот", "entrypoint"]
    ):
        add_check(
            "runnable_entrypoint",
            "provide a runnable local entrypoint or service path for the implemented slice",
        )

    if not checks:
        for hint in workstream_hints[:5]:
            add_check(
                slugify(hint["name"]).replace("-", "_"),
                hint["goal"],
            )

    add_check(
        "validation_and_docs",
        "document and validate the implemented path so a human can run it and assess residual gaps",
        critical=False,
    )
    return checks


def render_request(
    task: str,
    workspace: Path,
    workspace_exists: bool,
    task_kind: str,
    complexity: str,
    solver_count: int,
    execution_mode: str,
    workstream_hints: list[dict[str, str]],
    summary_language: str,
    goal_checks: list[dict[str, object]],
    intake_research_mode: str,
    execution_network_mode: str,
    cache_config: dict[str, object],
) -> str:
    workspace_note = "present" if workspace_exists else "missing"
    warnings = ""
    if not workspace_exists:
        warnings = "\n## Warning\n\n- Workspace path does not exist. Treat this run as greenfield planning until the path is corrected.\n"
    workstream_lines = "\n".join(
        f"- `{item['name']}`: {item['goal']} (role: `{item['suggested_role']}`)"
        for item in workstream_hints
    ) or "- none"
    goal_check_lines = "\n".join(
        f"- `{'critical' if item.get('critical', True) else 'supporting'}` `{item['id']}`: {item['requirement']}"
        for item in goal_checks
    ) or "- none"
    return f"""# Raw Request

{task}

## Environment

- Workspace: `{workspace}`
- Workspace status: `{workspace_note}`
- Task kind guess: `{task_kind}`
- Complexity guess: `{complexity}`
- Execution mode guess: `{execution_mode}`
- Suggested solver count: `{solver_count}`
- User summary language: `{summary_language}`
- Intake research mode: `{intake_research_mode}`
- Execution network mode: `{execution_network_mode}`
- Cache policy: `{cache_config['policy']}`
- Cache root: `{cache_config['root']}`

## Workstream Hints

{workstream_lines}

## Initial Goal Checks

{goal_check_lines}
{warnings}"""


def render_intake_prompt(
    run_dir: Path,
    workspace_exists: bool,
    task_kind: str,
    complexity: str,
    solver_count: int,
    execution_mode: str,
    workstream_hints: list[dict[str, str]],
    validation_commands: list[str],
    prompt_format: str,
    summary_language: str,
    goal_checks: list[dict[str, object]],
    intake_research_mode: str,
    execution_network_mode: str,
    cache_config: dict[str, object],
) -> str:
    if prompt_format == "compact":
        return compact_lines(
            {
                "stage": "intake",
                "mode": "prepare",
                "read": [
                    str(run_dir / "request.md"),
                    str(run_dir / "plan.json"),
                    "references/agency-role-map.md",
                    "references/decomposition-rules.md",
                ],
                "write": [
                    str(run_dir / "brief.md"),
                    str(run_dir / "plan.json"),
                    str(run_dir / "prompts"),
                ],
                "defaults": {
                    "workspace_exists": workspace_exists,
                    "task_kind": task_kind,
                    "complexity": complexity,
                    "execution_mode": execution_mode,
                    "solver_count": solver_count,
                    "summary_language": summary_language,
                    "intake_research_mode": intake_research_mode,
                    "execution_network_mode": execution_network_mode,
                    "cache": cache_config,
                    "validation_commands": compact_list(validation_commands),
                    "workstream_hints": workstream_hints,
                    "goal_checks": goal_checks,
                },
                "rules": [
                    "preserve the original requested outcome as the top-level goal",
                    "decompose compound tasks into workstreams instead of silently shrinking the deliverable",
                    "refine the goal_checks list so it captures the critical user-visible capabilities that must be implemented before the run can be called complete",
                    "follow intake_research_mode when deciding whether to browse the web before finalizing the brief",
                    "if cache.policy is reuse, consult and update the research cache before duplicating external research",
                    "if you introduce an MVP or phase-1 scaffold, keep it as an interim milestone rather than the new goal",
                    "do not implement the solution in this stage",
                ],
                "required_brief_sections": [
                    "original requested outcome",
                    "objective",
                    "deliverable",
                    "goal coverage matrix",
                    "workstream decomposition",
                    "scope",
                    "constraints",
                    "interim milestone if needed",
                    "definition of done",
                    "validation expectations",
                    "open questions answerable from local context",
                ],
            }
        )

    workspace_warning = ""
    if not workspace_exists:
        workspace_warning = "\n- workspace is missing; correct the path or explicitly treat this as a greenfield planning run\n"
    workstream_lines = "\n".join(
        f"- `{item['name']}`: {item['goal']} (suggested role: `{item['suggested_role']}`)"
        for item in workstream_hints
    ) or "- none"
    goal_check_items = [
        f"{'critical' if item.get('critical', True) else 'supporting'} {item['id']}: {item['requirement']}"
        for item in goal_checks
    ]
    return f"""# Level 1: Intake And Prompt Builder

Read:

- `{run_dir / 'request.md'}`
- `{run_dir / 'plan.json'}`
- `references/agency-role-map.md`

Your job is to prepare the pipeline, not solve the task.

Produce or update these artifacts:

- `brief.md` in the run directory with original requested outcome, objective, deliverable, workstream decomposition, scope, constraints, definition of done, and validation expectations
- `plan.json` if task kind, complexity, solver count, roles, execution mode, or workstreams need adjustment
- prompt packets in `prompts/` if the downstream prompts need refinement

Current defaults:

- workspace exists: `{str(workspace_exists).lower()}`
- task kind: `{task_kind}`
- complexity: `{complexity}`
- execution mode: `{execution_mode}`
- solver count: `{solver_count}`
- user summary language: `{summary_language}`
- intake research mode: `{intake_research_mode}`
- execution network mode: `{execution_network_mode}`
- cache policy: `{cache_config['policy']}`
- cache root: `{cache_config['root']}`
- suggested validation:
{bullet_list(validation_commands)}

Initial goal checks to refine in `plan.json`:
{bullet_list(goal_check_items)}

Suggested workstream hints:
{workstream_lines}

Rules:

- preserve the user's requested outcome as the top-level goal
- keep the brief precise and execution-ready
- decompose compound tasks into workstreams instead of silently shrinking the requested deliverable
- update `plan.json` goal checks when the current list misses a critical user-visible capability
- if intake research mode is `research-first`, browse the web for solution patterns, current tool options, and likely implementation constraints before finalizing stages
- if intake research mode is `local-first`, inspect the workspace first and browse only when local context is insufficient
- if intake research mode is `local-only`, do not browse unless the user explicitly asks
- if cache policy is `reuse`, consult and update the shared research cache before duplicating external research
- add only the minimal extra skills the downstream stages need
- do not implement the solution in this stage
- if the task is about Codex skills, prefer `skill-creator` or `skill-installer` over ad hoc instructions
- if you propose a phase-1 scaffold, record it as an interim milestone and keep the original requested system as the preserved goal
{workspace_warning}"""


def render_solver_prompt(
    run_dir: Path,
    solver_id: str,
    role: str,
    angle: str,
    validation_commands: list[str],
    prompt_format: str,
) -> str:
    result_file = run_dir / "solutions" / solver_id / "RESULT.md"
    if prompt_format == "compact":
        return compact_lines(
            {
                "stage": solver_id,
                "mode": "solve",
                "role": role,
                "angle": angle,
                "read": [
                    str(run_dir / "request.md"),
                    str(run_dir / "brief.md"),
                    str(run_dir / "plan.json"),
                ],
                "write": [str(result_file)],
                "rules": [
                    "do not read sibling solver outputs",
                    "preserve the full requested system as the top-level goal",
                    "if you narrow scope, record it as phase 1 while keeping the preserved goal explicit",
                    "state validation performed or the exact blocker",
                ],
                "deliverables": [
                    "assumptions",
                    "approach",
                    "implementation summary or exact file plan",
                    "goal check coverage",
                    "workstream coverage",
                    "validation performed",
                    "unresolved risks",
                ],
                "validation_hints": compact_list(validation_commands),
            }
        )

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
- include assumptions, approach, implementation or patch summary, goal check coverage, validation performed, and unresolved risks

Validation hints:
{bullet_list(validation_commands)}

Rules:

- solve the task from your assigned angle
- keep the output self-contained
- if you changed code, say exactly what you validated
- if you could not validate, say exactly why
"""


def render_review_prompt(
    run_dir: Path,
    validation_commands: list[str],
    reviewers: list[str],
    prompt_format: str,
    summary_language: str,
) -> str:
    solution_files = sorted((run_dir / "solutions").glob("*/RESULT.md"))
    solution_lines = "\n".join(f"- `{path}`" for path in solution_files)
    if prompt_format == "compact":
        return compact_lines(
            {
                "stage": "review",
                "mode": "compare",
                "read": [
                    str(run_dir / "request.md"),
                    str(run_dir / "brief.md"),
                    str(run_dir / "plan.json"),
                    "references/review-rubric.md",
                    *[str(path) for path in solution_files],
                ],
                "write": [
                    str(run_dir / "review" / "report.md"),
                    str(run_dir / "review" / "scorecard.json"),
                    str(run_dir / "review" / "user-summary.md"),
                ],
                "reviewer_stack": reviewers,
                "user_summary_language": summary_language,
                "validation_hints": compact_list(validation_commands),
                "rules": [
                    "compare every solution against the brief, not against style preference",
                    "compare every solution against the plan goal_checks and call out uncovered critical checks",
                    "penalize silent scope reduction",
                    "architecture-only or scaffold-only output is insufficient when the brief still targets a working MVP",
                    "treat missing evidence as a penalty",
                    "write a short user-facing summary in the requested language",
                    "recommend a hybrid only when the parts are clearly compatible",
                ],
                "required_output": {
                    "report_sections": [
                        "per-solver summary",
                        "winner",
                        "backup",
                        "hybrid if compatible",
                        "validation evidence used",
                    ],
                    "scorecard": "numeric per-solver scores using the review rubric",
                    "user_summary_sections": [
                        "winner and why",
                        "backup option",
                        "main risks",
                        "what the user may want to correct before execution",
                        "recommended next action",
                    ],
                },
            }
        )

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
- `review/user-summary.md` as a short user-facing summary in `{summary_language}`

Rules:

- compare every solution against the brief, not against style preferences
- compare every solution against the plan goal checks and penalize uncovered critical capabilities
- run the cheapest relevant validation first when code or config changed
- treat missing evidence as a penalty
- write the user-facing summary for a human decision-maker who may want to adjust the plan before execution
- recommend a hybrid only when the parts are clearly compatible
"""


def render_execution_prompt(
    run_dir: Path,
    validation_commands: list[str],
    prompt_format: str,
    execution_network_mode: str,
    cache_config: dict[str, object],
) -> str:
    solution_files = sorted((run_dir / "solutions").glob("*/RESULT.md"))
    solution_lines = "\n".join(f"- `{path}`" for path in solution_files) or "- none"
    execution_report = run_dir / "execution" / "report.md"
    if prompt_format == "compact":
        return compact_lines(
            {
                "stage": "execution",
                "mode": "implement",
                "read": [
                    str(run_dir / "request.md"),
                    str(run_dir / "brief.md"),
                    str(run_dir / "plan.json"),
                    str(run_dir / "review" / "report.md"),
                    str(run_dir / "review" / "scorecard.json"),
                    str(run_dir / "review" / "user-summary.md"),
                    *[str(path) for path in solution_files],
                ],
                "write": [str(execution_report)],
                "rules": [
                    "implement the winner or explicitly justified hybrid in the primary workspace",
                    "treat workspace changes as the main deliverable and execution/report.md as the audit trail",
                    "follow the review recommendation unless local validation forces a narrower implementation",
                    "if execution_network_mode is fetch-if-needed, install or download missing dependencies, weights, adapters, repos, or tools when they are genuinely required to implement the chosen slice",
                    "if cache.policy is reuse, prefer cached downloads, wheels, models, and repos before fetching again",
                    "store newly fetched reusable artifacts in cache.paths.downloads, cache.paths.wheelhouse, or cache.paths.models when applicable",
                    "record exact install/download commands, sources, versions, and what was fetched",
                    "prefer primary or official sources for code, packages, models, and datasets",
                    "treat uncovered critical goal checks as blockers or explicitly deferred work, not as silent completion",
                    "run the cheapest relevant validation after edits and record exact commands and outcomes",
                    "if blocked, implement the highest-value slice and state the blocker precisely",
                ],
                "deliverables": [
                    "actual workspace changes",
                    "execution summary",
                    "changed files",
                    "validation performed",
                    "remaining blockers and next steps",
                ],
                "validation_hints": compact_list(validation_commands),
            }
        )

    return f"""# Level 4: Execution

Read:

- `{run_dir / 'request.md'}`
- `{run_dir / 'brief.md'}`
- `{run_dir / 'plan.json'}`
- `{run_dir / 'review' / 'report.md'}`
- `{run_dir / 'review' / 'scorecard.json'}`
- `{run_dir / 'review' / 'user-summary.md'}` if it exists
- relevant solver outputs:
{solution_lines}

Your job is to implement the recommended winner or compatible hybrid in the primary workspace.

Execution network mode:

- `{execution_network_mode}`

Cache:

- policy: `{cache_config['policy']}`
- root: `{cache_config['root']}`

Deliver:

- actual code or configuration changes in the workspace
- `execution/report.md` with implementation summary, changed files, validation performed, blockers, and next steps

Validation hints:
{bullet_list(validation_commands)}

Rules:

- treat workspace changes as the primary deliverable
- follow the review recommendation unless local validation forces an explicit deviation
- if execution network mode is `fetch-if-needed`, you may install or download genuinely required dependencies, model artifacts, or tools
- if cache policy is `reuse`, prefer cached wheels, models, repos, and downloads before fetching again
- store newly fetched reusable artifacts in the shared cache when practical
- record exact install/download commands, sources, versions, and fetched artifacts in `execution/report.md`
- prefer official or primary sources when fetching dependencies or model artifacts
- treat remaining uncovered critical goal checks as blockers or explicit partial-completion notes
- run the cheapest relevant validation after edits
- if full implementation is blocked, complete the highest-value slice and record the exact blocker
"""


def render_verification_prompt(
    run_dir: Path,
    validation_commands: list[str],
    prompt_format: str,
    summary_language: str,
) -> str:
    solution_files = sorted((run_dir / "solutions").glob("*/RESULT.md"))
    solution_lines = "\n".join(f"- `{path}`" for path in solution_files) or "- none"
    findings_file = run_dir / "verification" / "findings.md"
    user_summary_file = run_dir / "verification" / "user-summary.md"
    improvement_request_file = run_dir / "verification" / "improvement-request.md"
    goal_status_file = run_dir / "verification" / "goal-status.json"

    if prompt_format == "compact":
        return compact_lines(
            {
                "stage": "verification",
                "mode": "audit",
                "read": [
                    str(run_dir / "request.md"),
                    str(run_dir / "brief.md"),
                    str(run_dir / "plan.json"),
                    str(run_dir / "review" / "report.md"),
                    str(run_dir / "review" / "scorecard.json"),
                    str(run_dir / "review" / "user-summary.md"),
                    str(run_dir / "execution" / "report.md"),
                    "references/verification-rubric.md",
                    *[str(path) for path in solution_files],
                ],
                "write": [
                    str(findings_file),
                    str(user_summary_file),
                    str(improvement_request_file),
                    str(goal_status_file),
                ],
                "validation_hints": compact_list(validation_commands),
                "user_summary_language": summary_language,
                "rules": [
                    "review the actual workspace implementation, not only the plans",
                    "act in code-review mode: prioritize bugs, regressions, unsafe behavior, and missing validation",
                    "run the cheapest relevant checks first and record exact evidence or blockers",
                    "write findings ordered by severity with file references when possible",
                    "set goal_complete=false when any critical plan goal check remains missing, unverified, or replaced by a placeholder implementation",
                    "if there are no meaningful findings, say so explicitly",
                    "generate an improvement request that can seed a follow-up pipeline run against the existing codebase",
                ],
                "required_output": {
                    "findings_sections": [
                        "findings",
                        "open questions or assumptions",
                        "change summary or residual risks",
                    ],
                    "user_summary_sections": [
                        "overall result",
                        "top issues",
                        "whether a rerun is recommended",
                        "recommended next action",
                    ],
                    "goal_status": {
                        "goal_complete": "boolean",
                        "goal_verdict": "complete | partial | blocked",
                        "covered_checks": "list of covered goal_check ids",
                        "missing_checks": "list of missing or unverified critical goal_check ids",
                        "rerun_recommended": "boolean",
                        "recommended_next_action": "stop | rerun | manual-review",
                        "reason": "short explanation",
                    },
                    "improvement_request": "a concise task statement for the next run that preserves the original goal and targets the verified defects",
                },
            }
        )

    return f"""# Level 5: Verification And Improvement Seed

Read:

- `{run_dir / 'request.md'}`
- `{run_dir / 'brief.md'}`
- `{run_dir / 'plan.json'}`
- `{run_dir / 'review' / 'report.md'}`
- `{run_dir / 'review' / 'scorecard.json'}`
- `{run_dir / 'review' / 'user-summary.md'}`
- `{run_dir / 'execution' / 'report.md'}`
- `references/verification-rubric.md`
- solver outputs for context:
{solution_lines}

Your job is to review the actual implementation in the workspace after execution, produce findings, and seed the next improvement run if needed.

Deliver:

- `verification/findings.md` with ordered findings, file references, evidence, and residual risks
- `verification/user-summary.md` as a short user-facing summary in `{summary_language}`
- `verification/goal-status.json` with machine-readable goal completion status against `plan.json` goal checks
- `verification/improvement-request.md` with a concise follow-up task for the next pipeline run against the existing codebase

Validation hints:
{bullet_list(validation_commands)}

Rules:

- inspect the actual workspace, not just the proposed plans
- default to code-review mindset: list findings first, ordered by severity
- run the cheapest relevant validation first
- set `goal_complete=false` when any critical goal check remains missing, unverified, or only partially mocked
- if there are no meaningful findings, say that explicitly
- keep the improvement request actionable and tightly scoped to verified defects
"""


def main() -> None:
    args = parse_args()
    task = read_task(args)
    workspace = Path(args.workspace).resolve()
    workspace_exists = workspace.exists()
    cache_config = build_cache_config(Path(args.cache_root), args.cache_policy)
    ensure_cache_dirs(cache_config)
    title = args.title or " ".join(task.split()[:8])
    task_kind = infer_task_kind(task) if args.task_kind == "auto" else args.task_kind
    complexity = infer_complexity(task) if args.complexity == "auto" else args.complexity
    solver_count = args.solver_count or solver_count_for(complexity)
    execution_mode = infer_execution_mode(task_kind, complexity, task)
    workstream_hints = workstream_hints_for(task_kind, task)
    goal_checks = infer_goal_checks(task, task_kind, workstream_hints)
    signals = detect_stack(workspace)
    validation_commands = build_validation_commands(workspace, signals)
    roles = choose_roles(task_kind, solver_count)

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    run_dir = Path(args.output_dir).resolve() / f"{timestamp}-{slugify(title)}"
    run_dir.mkdir(parents=True, exist_ok=False)
    (run_dir / "prompts").mkdir()
    (run_dir / "solutions").mkdir()
    (run_dir / "review").mkdir()
    (run_dir / "execution").mkdir()
    (run_dir / "verification").mkdir()

    plan = {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "workspace": str(workspace),
        "workspace_exists": workspace_exists,
        "original_task": task,
        "task_kind": task_kind,
        "complexity": complexity,
        "execution_mode": execution_mode,
        "prompt_format": args.prompt_format,
        "summary_language": args.summary_language,
        "intake_research_mode": args.intake_research,
        "execution_network_mode": args.execution_network,
        "cache": cache_config,
        "solver_count": solver_count,
        "solver_roles": roles,
        "workstream_hints": workstream_hints,
        "goal_gate_enabled": True,
        "goal_checks": goal_checks,
        "reviewer_stack": REVIEWER_STACK,
        "stack_signals": asdict(signals),
        "validation_commands": validation_commands,
        "references": {
            "role_map": "references/agency-role-map.md",
            "decomposition_rules": "references/decomposition-rules.md",
            "review_rubric": "references/review-rubric.md",
            "verification_rubric": "references/verification-rubric.md",
        },
    }

    write_text(
        run_dir / "request.md",
        render_request(
            task,
            workspace,
            workspace_exists,
            task_kind,
            complexity,
            solver_count,
            execution_mode,
            workstream_hints,
            args.summary_language,
            goal_checks,
            args.intake_research,
            args.execution_network,
            cache_config,
        ),
    )
    write_text(run_dir / "brief.md", "# Brief\n\nPending intake stage.\n")
    write_text(run_dir / "plan.json", json.dumps(plan, indent=2))
    write_text(
        run_dir / "prompts" / "level1-intake.md",
        render_intake_prompt(
            run_dir,
            workspace_exists,
            task_kind,
            complexity,
            solver_count,
            execution_mode,
            workstream_hints,
            validation_commands,
            args.prompt_format,
            args.summary_language,
            goal_checks,
            args.intake_research,
            args.execution_network,
            cache_config,
        ),
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
                    prompt_format=args.prompt_format,
                ),
            )
        write_text(
            run_dir / "solutions" / solver_id / "RESULT.md",
            "# Result\n\nFill this file with the solver output.\n",
        )

    write_text(
        run_dir / "prompts" / "level3-review.md",
        render_review_prompt(
            run_dir,
            validation_commands,
            REVIEWER_STACK,
            args.prompt_format,
            args.summary_language,
        ),
    )
    write_text(run_dir / "review" / "report.md", "# Review Report\n\nPending review stage.\n")
    write_text(run_dir / "review" / "scorecard.json", "{}\n")
    write_text(run_dir / "review" / "user-summary.md", "# User Summary\n\nPending localized review summary.\n")
    write_text(
        run_dir / "prompts" / "level4-execution.md",
        render_execution_prompt(
            run_dir,
            validation_commands,
            args.prompt_format,
            args.execution_network,
            cache_config,
        ),
    )
    write_text(run_dir / "execution" / "report.md", "# Execution Report\n\nPending execution stage.\n")
    write_text(
        run_dir / "prompts" / "level5-verification.md",
        render_verification_prompt(
            run_dir,
            validation_commands,
            args.prompt_format,
            args.summary_language,
        ),
    )
    write_text(run_dir / "verification" / "findings.md", "# Findings\n\nPending verification stage.\n")
    write_text(
        run_dir / "verification" / "user-summary.md",
        "# Verification Summary\n\nPending localized verification summary.\n",
    )
    write_text(run_dir / "verification" / "goal-status.json", "{}\n")
    write_text(
        run_dir / "verification" / "improvement-request.md",
        "# Improvement Request\n\nPending verification stage.\n",
    )

    print(run_dir)


if __name__ == "__main__":
    main()
