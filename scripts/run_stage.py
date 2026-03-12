#!/usr/bin/env python3
"""Operate a multi-agent pipeline run directory."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

from init_run import render_review_prompt, render_solver_prompt


SKILL_DIR = Path(__file__).resolve().parent.parent
ROLE_MAP = SKILL_DIR / "references" / "agency-role-map.md"
REVIEW_RUBRIC = SKILL_DIR / "references" / "review-rubric.md"

BRIEF_PLACEHOLDER = "Pending intake stage."
RESULT_PLACEHOLDER = "Fill this file with the solver output."
REVIEW_PLACEHOLDER = "Pending review stage."
PLACEHOLDER_PREFIXES = (
    "pending ",
    "fill this file ",
    "todo",
    "tbd",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_dir", help="Path to a run directory created by init_run.py")

    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("status", help="Show stage completion status")
    subparsers.add_parser("next", help="Print the next stage id")

    show = subparsers.add_parser("show", help="Print the compiled prompt for a stage")
    show.add_argument("stage", help="Stage id such as intake, solver-a, or review")
    show.add_argument("--raw", action="store_true", help="Show the raw stage prompt file")

    copy = subparsers.add_parser("copy", help="Copy the compiled prompt for a stage to the clipboard")
    copy.add_argument("stage", help="Stage id such as intake, solver-a, or review")
    copy.add_argument("--raw", action="store_true", help="Copy the raw stage prompt file")

    start = subparsers.add_parser("start", help="Launch codex exec for a stage")
    start.add_argument("stage", help="Stage id such as intake, solver-a, or review")
    start.add_argument("--dry-run", action="store_true", help="Print the command and prompt, do not execute")
    start.add_argument("--model", help="Pass --model to codex exec")
    start.add_argument("--profile", help="Pass --profile to codex exec")
    start.add_argument("--oss", action="store_true", help="Pass --oss to codex exec")
    start.add_argument("--color", choices=["always", "never", "auto"], help="Pass --color to codex exec")
    start.add_argument("--force", action="store_true", help="Allow running a stage even if ordering checks fail")

    start_next = subparsers.add_parser("start-next", help="Launch codex exec for the next incomplete stage")
    start_next.add_argument("--dry-run", action="store_true", help="Print the command and prompt, do not execute")
    start_next.add_argument("--model", help="Pass --model to codex exec")
    start_next.add_argument("--profile", help="Pass --profile to codex exec")
    start_next.add_argument("--oss", action="store_true", help="Pass --oss to codex exec")
    start_next.add_argument("--color", choices=["always", "never", "auto"], help="Pass --color to codex exec")
    start_next.add_argument("--force", action="store_true", help="Allow running a stage even if ordering checks fail")

    return parser.parse_args()


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def discover_agency_agents_dir() -> Path | None:
    candidates: list[Path] = []

    env_path = os.environ.get("AGENCY_AGENTS_DIR")
    if env_path:
        candidates.append(Path(env_path).expanduser())

    cwd = Path.cwd()
    candidates.append(cwd / "agency-agents")
    candidates.append(Path.home() / "agency-agents")

    for base in (cwd, SKILL_DIR):
        for parent in (base, *base.parents):
            candidates.append(parent / "agency-agents")

    seen: set[Path] = set()
    for candidate in candidates:
        candidate = candidate.resolve()
        if candidate in seen:
            continue
        seen.add(candidate)
        if candidate.exists() and candidate.is_dir():
            return candidate
    return None


def load_plan(run_dir: Path) -> dict:
    return json.loads(read_text(run_dir / "plan.json"))


def solver_ids(run_dir: Path) -> list[str]:
    plan = load_plan(run_dir)
    solver_roles = plan.get("solver_roles", [])
    if solver_roles:
        return [item["solver_id"] for item in solver_roles]
    solutions_dir = run_dir / "solutions"
    if not solutions_dir.exists():
        return []
    return sorted(path.name for path in solutions_dir.iterdir() if path.is_dir())


def stage_prompt_path(run_dir: Path, stage: str) -> Path:
    if stage == "intake":
        return run_dir / "prompts" / "level1-intake.md"
    if stage == "review":
        return run_dir / "prompts" / "level3-review.md"
    if stage.startswith("solver-"):
        return run_dir / "prompts" / f"level2-{stage}.md"
    raise SystemExit(f"Unknown stage: {stage}")


def stage_output_paths(run_dir: Path, stage: str) -> list[Path]:
    if stage == "intake":
        return [run_dir / "brief.md"]
    if stage == "review":
        return [run_dir / "review" / "report.md", run_dir / "review" / "scorecard.json"]
    if stage.startswith("solver-"):
        return [run_dir / "solutions" / stage / "RESULT.md"]
    raise SystemExit(f"Unknown stage: {stage}")


def first_substantive_line(text: str) -> str:
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("#"):
            continue
        return line
    return ""


def output_looks_placeholder(stage: str, text: str) -> bool:
    normalized = first_substantive_line(text).lower()
    if not normalized:
        return True

    exact_markers = {
        BRIEF_PLACEHOLDER.lower(),
        RESULT_PLACEHOLDER.lower(),
        REVIEW_PLACEHOLDER.lower(),
        f"pending {stage.lower()} stage.",
        f"pending {stage.lower()} stage",
    }
    if normalized in exact_markers:
        return True

    return any(normalized.startswith(prefix) for prefix in PLACEHOLDER_PREFIXES)


def review_scorecard_complete(path: Path) -> bool:
    try:
        payload = json.loads(read_text(path))
    except json.JSONDecodeError:
        return False
    if not isinstance(payload, dict):
        return False
    return bool(payload)


def is_stage_complete(run_dir: Path, stage: str) -> bool:
    outputs = stage_output_paths(run_dir, stage)
    for output in outputs:
        if not output.exists():
            return False
    if stage == "review":
        report = outputs[0]
        if output_looks_placeholder(stage, read_text(report)):
            return False
        if not review_scorecard_complete(outputs[1]):
            return False
        return True
    return all(not output_looks_placeholder(stage, read_text(output)) for output in outputs)


def available_stages(run_dir: Path) -> list[str]:
    stages = ["intake"]
    stages.extend(solver_ids(run_dir))
    stages.append("review")
    return stages


def solver_role_map(run_dir: Path) -> dict[str, dict]:
    plan = load_plan(run_dir)
    return {item["solver_id"]: item for item in plan.get("solver_roles", [])}


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content.rstrip() + "\n", encoding="utf-8")


def sync_run_artifacts(run_dir: Path) -> None:
    plan = load_plan(run_dir)
    validation_commands = plan.get("validation_commands", [])
    prompt_format = plan.get("prompt_format", "markdown")

    for role_data in plan.get("solver_roles", []):
        solver_id = role_data["solver_id"]
        prompt_path = run_dir / "prompts" / f"level2-{solver_id}.md"
        result_path = run_dir / "solutions" / solver_id / "RESULT.md"

        if not prompt_path.exists():
            write_text(
                prompt_path,
                render_solver_prompt(
                    run_dir,
                    solver_id=solver_id,
                    role=role_data["role"],
                    angle=role_data["angle"],
                    validation_commands=validation_commands,
                    prompt_format=prompt_format,
                ),
            )
        if not result_path.exists():
            write_text(result_path, "# Result\n\nFill this file with the solver output.\n")

    review_prompt_path = run_dir / "prompts" / "level3-review.md"
    if not review_prompt_path.exists():
        write_text(
            review_prompt_path,
            render_review_prompt(
                run_dir,
                validation_commands,
                plan.get("reviewer_stack", []),
                prompt_format,
            ),
        )


def next_stage(run_dir: Path) -> str | None:
    for stage in available_stages(run_dir):
        if not is_stage_complete(run_dir, stage):
            return stage
    return None


def working_root(plan: dict, run_dir: Path) -> Path:
    workspace = Path(plan.get("workspace", ".")).expanduser()
    if plan.get("workspace_exists") and workspace.exists():
        return workspace
    return run_dir


def compile_prompt(run_dir: Path, stage: str) -> str:
    plan = load_plan(run_dir)
    workspace = Path(plan.get("workspace", ".")).expanduser()
    prompt_format = plan.get("prompt_format", "markdown")
    prompt_body = read_text(stage_prompt_path(run_dir, stage)).rstrip()
    outputs = "\n".join(f"- `{path}`" for path in stage_output_paths(run_dir, stage))
    agency_agents_dir = discover_agency_agents_dir()

    guidance = [
        f"- Run directory: `{run_dir}`",
        f"- Primary workspace: `{workspace}`",
        f"- Prompt format: `{prompt_format}`",
        f"- Role map reference: `{ROLE_MAP}`",
        f"- Review rubric reference: `{REVIEW_RUBRIC}`",
    ]
    if agency_agents_dir is not None:
        guidance.append(f"- Agency role catalog: `{agency_agents_dir}`")

    extra_rules = [
        "- Update the requested artifacts directly on disk.",
        "- Use the primary workspace for repo inspection when it exists.",
        "- If blocked, replace placeholders with a concrete blocker note instead of leaving them unchanged.",
    ]
    if stage.startswith("solver-"):
        extra_rules.append("- Do not read sibling solver outputs.")

    dynamic_context = []
    if stage.startswith("solver-"):
        role_data = solver_role_map(run_dir).get(stage)
        if role_data:
            dynamic_context.extend(
                [
                    f"- Solver role from current plan: `{role_data['role']}`",
                    f"- Solver angle from current plan: `{role_data['angle']}`",
                ]
            )
    if stage == "review":
        solver_outputs = [
            str(run_dir / "solutions" / solver_id / "RESULT.md")
            for solver_id in solver_ids(run_dir)
        ]
        if solver_outputs:
            dynamic_context.append("Current solver outputs from plan:")
            dynamic_context.extend(f"- `{path}`" for path in solver_outputs)

    return (
        f"You are executing stage `{stage}` of a multi-agent pipeline.\n\n"
        "Execution context:\n"
        f"{os.linesep.join(guidance)}\n\n"
        + (
            "Dynamic stage context:\n"
            f"{os.linesep.join(dynamic_context)}\n\n"
            if dynamic_context
            else ""
        )
        +
        "Required output files:\n"
        f"{outputs}\n\n"
        "Global rules:\n"
        f"{os.linesep.join(extra_rules)}\n\n"
        "Stage prompt:\n\n"
        f"{prompt_body}\n"
    )


def print_status(run_dir: Path) -> int:
    for stage in available_stages(run_dir):
        status = "done" if is_stage_complete(run_dir, stage) else "pending"
        print(f"{stage}: {status}")
    pending = next_stage(run_dir)
    print(f"next: {pending or 'none'}")
    return 0


def require_valid_order(run_dir: Path, stage: str, force: bool) -> None:
    if force:
        return
    if stage.startswith("solver-") and not is_stage_complete(run_dir, "intake"):
        raise SystemExit("Intake stage is still pending. Run intake first or pass --force.")
    if stage == "review":
        pending_solvers = [solver for solver in solver_ids(run_dir) if not is_stage_complete(run_dir, solver)]
        if pending_solvers:
            pending_text = ", ".join(pending_solvers)
            raise SystemExit(f"Solver stages still pending: {pending_text}. Run them first or pass --force.")


def copy_to_clipboard(text: str) -> None:
    subprocess.run(["pbcopy"], input=text, text=True, check=True)


def build_codex_command(run_dir: Path, stage: str, args: argparse.Namespace) -> tuple[list[str], str]:
    plan = load_plan(run_dir)
    root = working_root(plan, run_dir)
    prompt = compile_prompt(run_dir, stage)
    color = args.color or "never"

    command = [
        "codex",
        "exec",
        "--full-auto",
        "--skip-git-repo-check",
        "--color",
        color,
        "-C",
        str(root),
        "--add-dir",
        str(run_dir),
        "--add-dir",
        str(SKILL_DIR),
        "-",
    ]
    if args.model:
        command[2:2] = ["--model", args.model]
    if args.profile:
        command[2:2] = ["--profile", args.profile]
    if args.oss:
        command.insert(2, "--oss")
    return command, prompt


def start_stage(run_dir: Path, stage: str, args: argparse.Namespace) -> int:
    require_valid_order(run_dir, stage, args.force)
    command, prompt = build_codex_command(run_dir, stage, args)

    logs_dir = run_dir / "logs"
    logs_dir.mkdir(exist_ok=True)
    prompt_path = logs_dir / f"{stage}.prompt.md"
    last_message_path = logs_dir / f"{stage}.last.md"
    prompt_path.write_text(prompt, encoding="utf-8")

    command = command[:-1] + ["--output-last-message", str(last_message_path), "-"]

    if args.dry_run:
        print("Command:")
        print(" ".join(shell_quote(part) for part in command))
        print("\nPrompt:\n")
        print(prompt)
        return 0

    env = dict(os.environ)
    env.setdefault("NO_COLOR", "1")
    env.setdefault("TERM", "dumb")

    process = subprocess.run(command, input=prompt, text=True, env=env)
    return process.returncode


def shell_quote(value: str) -> str:
    if value == "":
        return "''"
    if all(ch.isalnum() or ch in "/._-:=+" for ch in value):
        return value
    return "'" + value.replace("'", "'\"'\"'") + "'"


def resolve_stage(run_dir: Path, stage: str) -> str:
    stages = available_stages(run_dir)
    if stage not in stages:
        valid = ", ".join(stages)
        raise SystemExit(f"Unknown stage '{stage}'. Valid stages: {valid}")
    return stage


def main() -> int:
    args = parse_args()
    run_dir = Path(args.run_dir).expanduser().resolve()
    if not run_dir.exists():
        raise SystemExit(f"Run directory does not exist: {run_dir}")
    if not (run_dir / "plan.json").exists():
        raise SystemExit(f"Missing plan.json in run directory: {run_dir}")

    sync_run_artifacts(run_dir)

    if args.command == "status":
        return print_status(run_dir)

    if args.command == "next":
        print(next_stage(run_dir) or "none")
        return 0

    if args.command in {"show", "copy", "start"}:
        stage = resolve_stage(run_dir, args.stage)

    if args.command == "show":
        text = read_text(stage_prompt_path(run_dir, stage)) if args.raw else compile_prompt(run_dir, stage)
        print(text)
        return 0

    if args.command == "copy":
        text = read_text(stage_prompt_path(run_dir, stage)) if args.raw else compile_prompt(run_dir, stage)
        copy_to_clipboard(text)
        print(f"Copied {stage} prompt to clipboard.")
        return 0

    if args.command == "start":
        return start_stage(run_dir, stage, args)

    if args.command == "start-next":
        stage = next_stage(run_dir)
        if stage is None:
            print("Pipeline is complete.")
            return 0
        return start_stage(run_dir, stage, args)

    raise SystemExit(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    sys.exit(main())
