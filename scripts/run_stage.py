#!/usr/bin/env python3
"""Operate a multi-agent pipeline run directory."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

from init_run import (
    render_execution_prompt,
    render_review_prompt,
    render_solver_prompt,
    render_verification_prompt,
)


SKILL_DIR = Path(__file__).resolve().parent.parent
ROLE_MAP = SKILL_DIR / "references" / "agency-role-map.md"
REVIEW_RUBRIC = SKILL_DIR / "references" / "review-rubric.md"
VERIFICATION_RUBRIC = SKILL_DIR / "references" / "verification-rubric.md"

BRIEF_PLACEHOLDER = "Pending intake stage."
RESULT_PLACEHOLDER = "Fill this file with the solver output."
REVIEW_PLACEHOLDER = "Pending review stage."
USER_SUMMARY_PLACEHOLDER = "Pending localized review summary."
EXECUTION_PLACEHOLDER = "Pending execution stage."
VERIFICATION_PLACEHOLDER = "Pending verification stage."
VERIFICATION_SUMMARY_PLACEHOLDER = "Pending localized verification summary."
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
    subparsers.add_parser("summary", help="Print the localized user-facing review summary")
    subparsers.add_parser("findings", help="Print verification findings")
    rerun = subparsers.add_parser(
        "rerun",
        help="Create a follow-up run from verification/improvement-request.md",
    )
    rerun.add_argument("--dry-run", action="store_true", help="Print the init_run.py command, do not execute")
    rerun.add_argument("--title", help="Optional title for the follow-up run")
    rerun.add_argument("--output-dir", help="Override output directory for the follow-up run")

    show = subparsers.add_parser("show", help="Print the compiled prompt for a stage")
    show.add_argument("stage", help="Stage id such as intake, solver-a, review, or execution")
    show.add_argument("--raw", action="store_true", help="Show the raw stage prompt file")

    copy = subparsers.add_parser("copy", help="Copy the compiled prompt for a stage to the clipboard")
    copy.add_argument("stage", help="Stage id such as intake, solver-a, review, or execution")
    copy.add_argument("--raw", action="store_true", help="Copy the raw stage prompt file")

    start = subparsers.add_parser("start", help="Launch codex exec for a stage")
    start.add_argument("stage", help="Stage id such as intake, solver-a, review, or execution")
    start.add_argument("--dry-run", action="store_true", help="Print the command and prompt, do not execute")
    start.add_argument("--model", help="Pass --model to codex exec")
    start.add_argument("--profile", help="Pass --profile to codex exec")
    start.add_argument("--oss", action="store_true", help="Pass --oss to codex exec")
    start.add_argument("--color", choices=["always", "never", "auto"], help="Pass --color to codex exec")
    start.add_argument("--force", action="store_true", help="Allow running a stage even if ordering checks fail")

    start_solvers = subparsers.add_parser(
        "start-solvers",
        help="Launch all pending solver stages in parallel",
    )
    start_solvers.add_argument("--dry-run", action="store_true", help="Print the commands and prompts, do not execute")
    start_solvers.add_argument("--model", help="Pass --model to codex exec")
    start_solvers.add_argument("--profile", help="Pass --profile to codex exec")
    start_solvers.add_argument("--oss", action="store_true", help="Pass --oss to codex exec")
    start_solvers.add_argument("--color", choices=["always", "never", "auto"], help="Pass --color to codex exec")
    start_solvers.add_argument("--force", action="store_true", help="Allow running solver stages even if ordering checks fail")

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


def cache_config(plan: dict) -> dict[str, object]:
    cache = plan.get("cache")
    if isinstance(cache, dict) and cache:
        return cache
    root = str((Path.home() / ".cache" / "multi-agent-pipeline").resolve())
    return {
        "enabled": False,
        "root": root,
        "policy": "off",
        "paths": {
            "research": str(Path(root) / "research"),
            "downloads": str(Path(root) / "downloads"),
            "wheelhouse": str(Path(root) / "wheelhouse"),
            "models": str(Path(root) / "models"),
            "verification": str(Path(root) / "verification"),
        },
    }


def ensure_cache_dirs_from_plan(plan: dict) -> None:
    cache = cache_config(plan)
    if not cache.get("enabled"):
        return
    root = Path(str(cache["root"]))
    root.mkdir(parents=True, exist_ok=True)
    for path in cache.get("paths", {}).values():
        Path(str(path)).mkdir(parents=True, exist_ok=True)


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
    if stage == "execution":
        return run_dir / "prompts" / "level4-execution.md"
    if stage == "verification":
        return run_dir / "prompts" / "level5-verification.md"
    if stage.startswith("solver-"):
        return run_dir / "prompts" / f"level2-{stage}.md"
    raise SystemExit(f"Unknown stage: {stage}")


def stage_output_paths(run_dir: Path, stage: str) -> list[Path]:
    if stage == "intake":
        return [run_dir / "brief.md"]
    if stage == "review":
        return [
            run_dir / "review" / "report.md",
            run_dir / "review" / "scorecard.json",
            run_dir / "review" / "user-summary.md",
        ]
    if stage == "execution":
        return [run_dir / "execution" / "report.md"]
    if stage == "verification":
        outputs = [
            run_dir / "verification" / "findings.md",
            run_dir / "verification" / "user-summary.md",
            run_dir / "verification" / "improvement-request.md",
        ]
        if goal_gate_enabled(run_dir):
            outputs.append(goal_status_path(run_dir))
        return outputs
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
        USER_SUMMARY_PLACEHOLDER.lower(),
        EXECUTION_PLACEHOLDER.lower(),
        VERIFICATION_PLACEHOLDER.lower(),
        VERIFICATION_SUMMARY_PLACEHOLDER.lower(),
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


def review_complete_without_summary(run_dir: Path) -> bool:
    report_path = run_dir / "review" / "report.md"
    scorecard_path = run_dir / "review" / "scorecard.json"
    if not report_path.exists() or not scorecard_path.exists():
        return False
    if output_looks_placeholder("review", read_text(report_path)):
        return False
    return review_scorecard_complete(scorecard_path)


def goal_status_path(run_dir: Path) -> Path:
    return run_dir / "verification" / "goal-status.json"


def goal_gate_enabled(run_dir: Path) -> bool:
    plan = load_plan(run_dir)
    return bool(plan.get("goal_gate_enabled"))


def goal_status_complete(path: Path) -> bool:
    try:
        payload = json.loads(read_text(path))
    except json.JSONDecodeError:
        return False
    if not isinstance(payload, dict) or not payload:
        return False
    if not isinstance(payload.get("goal_complete"), bool):
        return False
    if payload.get("goal_verdict") not in {"complete", "partial", "blocked"}:
        return False
    if not isinstance(payload.get("rerun_recommended"), bool):
        return False
    if not isinstance(payload.get("recommended_next_action"), str):
        return False
    return True


def load_goal_status(run_dir: Path) -> dict | None:
    path = goal_status_path(run_dir)
    if not path.exists():
        return None
    if not goal_status_complete(path):
        return None
    return json.loads(read_text(path))


def is_stage_complete(run_dir: Path, stage: str) -> bool:
    outputs = stage_output_paths(run_dir, stage)
    if stage == "review":
        report, scorecard, summary = outputs
        if not report.exists() or not scorecard.exists():
            return False
        if output_looks_placeholder(stage, read_text(report)):
            return False
        if not review_scorecard_complete(scorecard):
            return False
        if summary.exists() and output_looks_placeholder("review-summary", read_text(summary)):
            return False
        return True
    if stage == "verification":
        findings, summary, improvement_request, *_rest = outputs
        if not findings.exists() or not summary.exists() or not improvement_request.exists():
            return False
        if output_looks_placeholder(stage, read_text(findings)):
            return False
        if output_looks_placeholder("verification-summary", read_text(summary)):
            return False
        if output_looks_placeholder("improvement-request", read_text(improvement_request)):
            return False
        if goal_gate_enabled(run_dir):
            goal_status = goal_status_path(run_dir)
            if not goal_status.exists() or not goal_status_complete(goal_status):
                return False
        return True
    for output in outputs:
        if not output.exists():
            return False
    return all(not output_looks_placeholder(stage, read_text(output)) for output in outputs)


def available_stages(run_dir: Path) -> list[str]:
    stages = ["intake"]
    stages.extend(solver_ids(run_dir))
    stages.append("review")
    stages.append("execution")
    stages.append("verification")
    return stages


def solver_role_map(run_dir: Path) -> dict[str, dict]:
    plan = load_plan(run_dir)
    return {item["solver_id"]: item for item in plan.get("solver_roles", [])}


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content.rstrip() + "\n", encoding="utf-8")


def sync_run_artifacts(run_dir: Path) -> None:
    plan = load_plan(run_dir)
    ensure_cache_dirs_from_plan(plan)
    validation_commands = plan.get("validation_commands", [])
    prompt_format = plan.get("prompt_format", "markdown")
    summary_language = plan.get("summary_language", "ru")
    execution_network_mode = plan.get("execution_network_mode", "fetch-if-needed")
    cache = cache_config(plan)

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
                summary_language,
            ),
        )

    review_summary_path = run_dir / "review" / "user-summary.md"
    if not review_summary_path.exists() and not review_complete_without_summary(run_dir):
        write_text(review_summary_path, "# User Summary\n\nPending localized review summary.\n")

    execution_prompt_path = run_dir / "prompts" / "level4-execution.md"
    if not execution_prompt_path.exists():
        write_text(
            execution_prompt_path,
            render_execution_prompt(
                run_dir,
                validation_commands,
                prompt_format,
                execution_network_mode,
                cache,
            ),
        )

    execution_report_path = run_dir / "execution" / "report.md"
    if not execution_report_path.exists():
        write_text(execution_report_path, "# Execution Report\n\nPending execution stage.\n")

    verification_prompt_path = run_dir / "prompts" / "level5-verification.md"
    if not verification_prompt_path.exists():
        write_text(
            verification_prompt_path,
            render_verification_prompt(
                run_dir,
                validation_commands,
                prompt_format,
                summary_language,
            ),
        )

    verification_findings_path = run_dir / "verification" / "findings.md"
    if not verification_findings_path.exists():
        write_text(verification_findings_path, "# Findings\n\nPending verification stage.\n")

    verification_summary_path = run_dir / "verification" / "user-summary.md"
    if not verification_summary_path.exists():
        write_text(
            verification_summary_path,
            "# Verification Summary\n\nPending localized verification summary.\n",
        )

    verification_goal_status_path = goal_status_path(run_dir)
    if plan.get("goal_gate_enabled") and not verification_goal_status_path.exists():
        write_text(verification_goal_status_path, "{}\n")

    verification_improvement_path = run_dir / "verification" / "improvement-request.md"
    if not verification_improvement_path.exists():
        write_text(
            verification_improvement_path,
            "# Improvement Request\n\nPending verification stage.\n",
        )


def next_stage(run_dir: Path) -> str | None:
    for stage in available_stages(run_dir):
        if not is_stage_complete(run_dir, stage):
            return stage
    goal_status = load_goal_status(run_dir)
    if goal_status and not goal_status.get("goal_complete", False):
        return "rerun"
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
        f"- Intake research mode: `{plan.get('intake_research_mode', 'research-first')}`",
        f"- Execution network mode: `{plan.get('execution_network_mode', 'fetch-if-needed')}`",
        f"- Cache policy: `{cache_config(plan).get('policy', 'off')}`",
        f"- Cache root: `{cache_config(plan).get('root')}`",
        f"- Role map reference: `{ROLE_MAP}`",
        f"- Review rubric reference: `{REVIEW_RUBRIC}`",
        f"- Verification rubric reference: `{VERIFICATION_RUBRIC}`",
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
    goal_checks = plan.get("goal_checks", [])
    if goal_checks:
        dynamic_context.append("Goal checks from current plan:")
        for item in goal_checks:
            criticality = "critical" if item.get("critical", True) else "supporting"
            dynamic_context.append(
                f"- `{criticality}` `{item.get('id', 'unknown')}`: {item.get('requirement', '')}"
            )
    cache = cache_config(plan)
    if cache.get("enabled"):
        dynamic_context.append("Shared cache paths:")
        for name, path in cache.get("paths", {}).items():
            dynamic_context.append(f"- `{name}`: `{path}`")
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
    if stage == "execution":
        scorecard_path = run_dir / "review" / "scorecard.json"
        if scorecard_path.exists() and review_scorecard_complete(scorecard_path):
            scorecard = json.loads(read_text(scorecard_path))
            winner = scorecard.get("winner")
            backup = scorecard.get("backup")
            if winner:
                dynamic_context.append(f"- Review winner from scorecard: `{winner}`")
            if backup:
                dynamic_context.append(f"- Review backup from scorecard: `{backup}`")
        dynamic_context.extend(
            [
                f"- Review report: `{run_dir / 'review' / 'report.md'}`",
                f"- User-facing summary: `{run_dir / 'review' / 'user-summary.md'}`",
            ]
        )
    if stage == "verification":
        dynamic_context.extend(
            [
                f"- Execution report: `{run_dir / 'execution' / 'report.md'}`",
                f"- Verification findings output: `{run_dir / 'verification' / 'findings.md'}`",
                f"- Goal status output: `{goal_status_path(run_dir)}`",
                f"- Improvement request output: `{run_dir / 'verification' / 'improvement-request.md'}`",
            ]
        )

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
    goal_status = load_goal_status(run_dir)
    if goal_status is not None:
        goal_state = "complete" if goal_status.get("goal_complete") else goal_status.get("goal_verdict", "partial")
        print(f"goal: {goal_state}")
    elif goal_gate_enabled(run_dir):
        print("goal: pending-verification")
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
    if stage == "execution" and not is_stage_complete(run_dir, "review"):
        raise SystemExit("Review stage is still pending. Run review first or pass --force.")
    if stage == "verification" and not is_stage_complete(run_dir, "execution"):
        raise SystemExit("Execution stage is still pending. Run execution first or pass --force.")


def copy_to_clipboard(text: str) -> None:
    subprocess.run(["pbcopy"], input=text, text=True, check=True)


def build_codex_command(run_dir: Path, stage: str, args: argparse.Namespace) -> tuple[list[str], str]:
    plan = load_plan(run_dir)
    root = working_root(plan, run_dir)
    prompt = compile_prompt(run_dir, stage)
    color = args.color or "never"
    cache = cache_config(plan)

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
    if cache.get("enabled"):
        command[-1:-1] = ["--add-dir", str(cache["root"])]
    if args.model:
        command[2:2] = ["--model", args.model]
    if args.profile:
        command[2:2] = ["--profile", args.profile]
    if args.oss:
        command.insert(2, "--oss")
    return command, prompt


def prepare_stage_command(run_dir: Path, stage: str, args: argparse.Namespace) -> tuple[list[str], str, Path, Path]:
    command, prompt = build_codex_command(run_dir, stage, args)
    logs_dir = run_dir / "logs"
    logs_dir.mkdir(exist_ok=True)
    prompt_path = logs_dir / f"{stage}.prompt.md"
    last_message_path = logs_dir / f"{stage}.last.md"
    prompt_path.write_text(prompt, encoding="utf-8")
    command = command[:-1] + ["--output-last-message", str(last_message_path), "-"]
    return command, prompt, prompt_path, last_message_path


def print_status_after_action(run_dir: Path) -> None:
    print("\nStatus:\n")
    print_status(run_dir)


def start_stage(run_dir: Path, stage: str, args: argparse.Namespace) -> int:
    require_valid_order(run_dir, stage, args.force)
    command, prompt, _prompt_path, _last_message_path = prepare_stage_command(run_dir, stage, args)

    if args.dry_run:
        print("Command:")
        print(" ".join(shell_quote(part) for part in command))
        print("\nPrompt:\n")
        print(prompt)
        print_status_after_action(run_dir)
        return 0

    env = dict(os.environ)
    env.setdefault("NO_COLOR", "1")
    env.setdefault("TERM", "dumb")

    process = subprocess.run(command, input=prompt, text=True, env=env)
    sync_run_artifacts(run_dir)
    print_status_after_action(run_dir)
    return process.returncode


def pending_solver_stages(run_dir: Path) -> list[str]:
    return [stage for stage in solver_ids(run_dir) if not is_stage_complete(run_dir, stage)]


def start_solver_batch(run_dir: Path, stages: list[str], args: argparse.Namespace) -> int:
    if not stages:
        print("No pending solver stages.")
        print_status_after_action(run_dir)
        return 0
    for stage in stages:
        require_valid_order(run_dir, stage, args.force)

    prepared: list[tuple[str, list[str], str, Path]] = []
    for stage in stages:
        command, prompt, _prompt_path, _last_message_path = prepare_stage_command(run_dir, stage, args)
        log_path = run_dir / "logs" / f"{stage}.stdout.log"
        prepared.append((stage, command, prompt, log_path))

    if args.dry_run:
        for stage, command, prompt, _log_path in prepared:
            print(f"Stage: {stage}")
            print("Command:")
            print(" ".join(shell_quote(part) for part in command))
            print("\nPrompt:\n")
            print(prompt)
            print("\n---\n")
        print_status_after_action(run_dir)
        return 0

    env = dict(os.environ)
    env.setdefault("NO_COLOR", "1")
    env.setdefault("TERM", "dumb")

    running: dict[str, tuple[subprocess.Popen, Path, object]] = {}
    for stage, command, prompt, log_path in prepared:
        log_handle = log_path.open("w", encoding="utf-8")
        process = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            text=True,
            env=env,
        )
        assert process.stdin is not None
        process.stdin.write(prompt)
        process.stdin.close()
        running[stage] = (process, log_path, log_handle)
        print(f"Started {stage}. Log: {log_path}")

    exit_code = 0
    remaining = set(running)
    while remaining:
        for stage in list(remaining):
            process, log_path, log_handle = running[stage]
            result = process.poll()
            if result is None:
                continue
            log_handle.close()
            remaining.remove(stage)
            if result != 0:
                exit_code = result if exit_code == 0 else exit_code
            sync_run_artifacts(run_dir)
            print(f"\nCompleted {stage} with exit code {result}. Log: {log_path}")
            print_status_after_action(run_dir)
        if remaining:
            time.sleep(0.5)
    return exit_code


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


def print_user_summary(run_dir: Path) -> int:
    summary_path = run_dir / "review" / "user-summary.md"
    if summary_path.exists() and not output_looks_placeholder("review-summary", read_text(summary_path)):
        print(read_text(summary_path).rstrip())
        return 0

    report_path = run_dir / "review" / "report.md"
    if review_complete_without_summary(run_dir):
        print("Localized user summary is not available for this older run. Review report:\n")
        print(read_text(report_path).rstrip())
        return 0

    raise SystemExit("Localized user summary is not ready yet. Run the review stage first.")


def print_findings(run_dir: Path) -> int:
    findings_path = run_dir / "verification" / "findings.md"
    if findings_path.exists() and not output_looks_placeholder("verification", read_text(findings_path)):
        print(read_text(findings_path).rstrip())
        return 0
    raise SystemExit("Verification findings are not ready yet. Run the verification stage first.")


def create_follow_up_run(run_dir: Path, args: argparse.Namespace) -> int:
    if not is_stage_complete(run_dir, "verification"):
        raise SystemExit("Verification stage is still pending. Run verification first.")

    plan = load_plan(run_dir)
    improvement_request = run_dir / "verification" / "improvement-request.md"
    if not improvement_request.exists():
        raise SystemExit(f"Missing improvement request: {improvement_request}")

    output_dir = Path(args.output_dir).expanduser().resolve() if args.output_dir else run_dir.parent
    title = args.title or f"{run_dir.name}-improve"
    command = [
        sys.executable,
        str(SKILL_DIR / "scripts" / "init_run.py"),
        "--task-file",
        str(improvement_request),
        "--workspace",
        str(Path(plan.get("workspace", ".")).expanduser()),
        "--output-dir",
        str(output_dir),
        "--title",
        title,
        "--prompt-format",
        str(plan.get("prompt_format", "markdown")),
        "--summary-language",
        str(plan.get("summary_language", "ru")),
    ]

    if args.dry_run:
        print("Command:")
        print(" ".join(shell_quote(part) for part in command))
        return 0

    process = subprocess.run(command, text=True, capture_output=True)
    if process.returncode != 0:
        raise SystemExit(process.stderr.strip() or process.stdout.strip() or "Failed to create follow-up run.")
    new_run = process.stdout.strip()
    print(new_run)
    new_run_dir = Path(new_run).expanduser()
    if new_run_dir.exists():
        sync_run_artifacts(new_run_dir)
        print("\nNew run status:\n")
        print_status(new_run_dir)
    return 0


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

    if args.command == "summary":
        return print_user_summary(run_dir)

    if args.command == "findings":
        return print_findings(run_dir)

    if args.command == "rerun":
        return create_follow_up_run(run_dir, args)

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

    if args.command == "start-solvers":
        return start_solver_batch(run_dir, pending_solver_stages(run_dir), args)

    if args.command == "start-next":
        stage = next_stage(run_dir)
        if stage is None:
            print("Pipeline is complete.")
            return 0
        if stage == "rerun":
            rerun_args = argparse.Namespace(dry_run=args.dry_run, title=None, output_dir=None)
            return create_follow_up_run(run_dir, rerun_args)
        if stage.startswith("solver-"):
            return start_solver_batch(run_dir, pending_solver_stages(run_dir), args)
        return start_stage(run_dir, stage, args)

    raise SystemExit(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    sys.exit(main())
