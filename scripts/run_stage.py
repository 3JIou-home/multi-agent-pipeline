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

from cache_utils import ensure_cache_layout, format_size, load_cache_index, prune_cache, refresh_cache_index
from init_run import (
    HostFacts,
    render_intake_prompt,
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


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_dir", help="Path to a run directory created by init_run.py")

    subparsers = parser.add_subparsers(dest="command", required=True)

    status = subparsers.add_parser("status", help="Show stage completion status")
    status.add_argument("--json", action="store_true", help="Print machine-readable JSON")

    doctor = subparsers.add_parser("doctor", help="Check run consistency and recommend the next safe action")
    doctor.add_argument("--json", action="store_true", help="Print machine-readable JSON")

    subparsers.add_parser("next", help="Print the next stage id")
    subparsers.add_parser("summary", help="Print the localized user-facing review summary")
    subparsers.add_parser("findings", help="Print verification findings")
    subparsers.add_parser("augmented-task", help="Print the verification-generated follow-up task prompt")
    host_probe = subparsers.add_parser("host-probe", help="Capture and print local host/runtime facts for this run")
    host_probe.add_argument("--refresh", action="store_true", help="Re-run the host probe before printing")
    host_probe.add_argument("--history", action="store_true", help="List historical host probe snapshots after printing the latest one")
    recheck = subparsers.add_parser(
        "recheck",
        help="Reset a completed stage for a clean rerun without rewinding its upstream prerequisites",
    )
    recheck.add_argument("stage", choices=["verification"], help="Stage to rerun safely")
    recheck.add_argument("--dry-run", action="store_true", help="Show what would be reset without changing files")
    step_back = subparsers.add_parser(
        "step-back",
        help="Reset a stage and its dependent downstream stages back to pending",
    )
    step_back.add_argument("stage", help="Stage id such as intake, solver-a, review, execution, or verification")
    step_back.add_argument("--dry-run", action="store_true", help="Show what would be reset without changing files")
    cache_status = subparsers.add_parser("cache-status", help="Show shared cache status")
    cache_status.add_argument("--refresh", action="store_true", help="Rebuild cache index before printing")
    cache_status.add_argument(
        "--limit",
        type=int,
        default=5,
        help="Show up to this many largest cached files, default: 5",
    )
    cache_prune = subparsers.add_parser("cache-prune", help="Delete old cache files and rebuild cache index")
    cache_prune.add_argument(
        "--max-age-days",
        type=int,
        required=True,
        help="Remove files older than this many days",
    )
    cache_prune.add_argument(
        "--area",
        action="append",
        choices=["research", "downloads", "wheelhouse", "models", "verification"],
        help="Restrict pruning to one or more cache areas",
    )
    cache_prune.add_argument("--dry-run", action="store_true", help="Show what would be removed without deleting it")
    rerun = subparsers.add_parser(
        "rerun",
        help="Create a follow-up run from verification/improvement-request.md",
    )
    rerun.add_argument("--dry-run", action="store_true", help="Print the init_run.py command, do not execute")
    rerun.add_argument("--title", help="Optional title for the follow-up run")
    rerun.add_argument("--output-dir", help="Override output directory for the follow-up run")
    rerun.add_argument(
        "--prompt-source",
        choices=["auto", "improvement", "augmented"],
        default="auto",
        help="Choose whether rerun should use the narrow improvement request or the fuller augmented task",
    )

    show = subparsers.add_parser("show", help="Print the compiled prompt for a stage")
    show.add_argument("stage", help="Stage id such as intake, solver-a, review, or execution")
    show.add_argument("--raw", action="store_true", help="Show the raw stage prompt file")

    refresh_prompt = subparsers.add_parser("refresh-prompt", help="Regenerate the raw prompt file for a stage")
    refresh_prompt.add_argument("stage", help="Stage id such as intake, solver-a, review, execution, or verification")
    refresh_prompt.add_argument("--dry-run", action="store_true", help="Show the regenerated prompt without writing it")
    refresh_prompts = subparsers.add_parser(
        "refresh-prompts",
        help="Regenerate the raw prompt files for all currently available stages",
    )
    refresh_prompts.add_argument("--dry-run", action="store_true", help="Show the regenerated prompts without writing them")

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

    return parser.parse_args(argv)


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
    root = str((Path.home() / ".cache" / "multi-agent-pipeline").resolve())
    meta_root = str((Path(root) / ".meta").resolve())
    fallback = {
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
        "meta": {
            "root": meta_root,
            "index": str(Path(meta_root) / "index.json"),
            "locks": str(Path(meta_root) / "locks"),
        },
    }
    if not isinstance(cache, dict) or not cache:
        return fallback

    merged = dict(fallback)
    merged.update(cache)
    merged_paths = dict(fallback["paths"])
    merged_paths.update(cache.get("paths", {}) if isinstance(cache.get("paths"), dict) else {})
    merged["paths"] = merged_paths
    merged_meta = dict(fallback["meta"])
    merged_meta.update(cache.get("meta", {}) if isinstance(cache.get("meta"), dict) else {})
    merged["meta"] = merged_meta
    return merged


def ensure_cache_dirs_from_plan(plan: dict) -> None:
    ensure_cache_layout(cache_config(plan))


def host_probe_path(run_dir: Path) -> Path:
    return run_dir / "host" / "probe.json"


def host_probe_history_dir(run_dir: Path) -> Path:
    return run_dir / "host" / "probes"


def host_probe_history_paths(run_dir: Path) -> list[Path]:
    history_dir = host_probe_history_dir(run_dir)
    if not history_dir.exists():
        return []
    return sorted(
        [path for path in history_dir.iterdir() if path.is_file() and path.suffix == ".json"],
        key=lambda path: path.name,
    )


def _detect_local_host_probe() -> dict[str, object]:
    probe: dict[str, object] = {
        "source": "run_stage_local_python",
        "captured_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "python_executable": sys.executable,
        "python_version": sys.version.split()[0],
        "platform": sys.platform,
    }

    try:
        import platform as platform_module

        probe["machine"] = platform_module.machine()
    except Exception:
        probe["machine"] = "unknown"

    visible_env_keys: list[str] = []
    for prefix in ("LLM_", "TORCH", "PYTORCH", "CUDA", "HF_", "TRANSFORMERS_"):
        visible_env_keys.extend(sorted(key for key in os.environ if key.startswith(prefix)))
    probe["visible_env_keys"] = sorted(set(visible_env_keys))

    try:
        import torch  # type: ignore

        probe["torch_installed"] = True
        cuda_available = bool(torch.cuda.is_available())
        probe["cuda_available"] = cuda_available
        mps_backend = getattr(torch.backends, "mps", None)
        mps_built = bool(mps_backend.is_built()) if mps_backend and hasattr(mps_backend, "is_built") else None
        mps_available = (
            bool(mps_backend.is_available()) if mps_backend and hasattr(mps_backend, "is_available") else None
        )
        probe["mps_built"] = mps_built
        probe["mps_available"] = mps_available
        if cuda_available:
            probe["preferred_torch_device"] = "cuda"
        elif mps_available:
            probe["preferred_torch_device"] = "mps"
        else:
            probe["preferred_torch_device"] = "cpu"
    except Exception as exc:
        probe["torch_installed"] = False
        probe["cuda_available"] = None
        probe["mps_built"] = None
        probe["mps_available"] = None
        probe["preferred_torch_device"] = "cpu"
        probe["torch_error"] = f"{type(exc).__name__}: {exc}"

    return probe


def load_host_probe(run_dir: Path) -> dict[str, object] | None:
    path = host_probe_path(run_dir)
    if not path.exists():
        return None
    try:
        payload = json.loads(read_text(path))
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


def capture_host_probe(run_dir: Path) -> dict[str, object]:
    payload = _detect_local_host_probe()
    path = host_probe_path(run_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    history_dir = host_probe_history_dir(run_dir)
    history_dir.mkdir(parents=True, exist_ok=True)
    stamp = time.strftime("%Y%m%d-%H%M%S") + f"-{int(time.time_ns() % 1_000_000_000):09d}"
    history_path = history_dir / f"{stamp}.json"
    payload["artifact"] = str(path)
    payload["history_artifact"] = str(history_path)
    history_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return payload


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


def amendments_path(run_dir: Path) -> Path:
    return run_dir / "amendments.md"


def amendments_exist(run_dir: Path) -> bool:
    path = amendments_path(run_dir)
    if not path.exists():
        return False
    try:
        content = read_text(path).strip()
    except OSError:
        return False
    return bool(content and "Pending " not in content[:64])


def render_stage_prompt(run_dir: Path, stage: str) -> str:
    plan = load_plan(run_dir)
    validation_commands = plan.get("validation_commands", [])
    prompt_format = plan.get("prompt_format", "markdown")
    summary_language = plan.get("summary_language", "ru")
    stage_research_mode = str(plan.get("stage_research_mode", "local-first"))
    execution_network_mode = plan.get("execution_network_mode", "fetch-if-needed")
    cache = cache_config(plan)
    host_fact_payload = plan.get("host_facts", {})
    host_facts = HostFacts(**host_fact_payload) if isinstance(host_fact_payload, dict) and host_fact_payload else HostFacts(
        source="run_stage_fallback",
        captured_at="unknown",
        platform=sys.platform,
        machine="unknown",
        apple_silicon=False,
        torch_installed=False,
        cuda_available=None,
        mps_built=None,
        mps_available=None,
        preferred_torch_device="cpu",
    )

    if stage == "intake":
        return render_intake_prompt(
            run_dir,
            workspace_exists=bool(plan.get("workspace_exists")),
            task_kind=str(plan.get("task_kind", "research")),
            complexity=str(plan.get("complexity", "medium")),
            solver_count=int(plan.get("solver_count", 1)),
            execution_mode=str(plan.get("execution_mode", "single-pass")),
            workstream_hints=plan.get("workstream_hints", []),
            validation_commands=validation_commands,
            prompt_format=prompt_format,
            summary_language=summary_language,
            goal_checks=plan.get("goal_checks", []),
            intake_research_mode=str(plan.get("intake_research_mode", "research-first")),
            stage_research_mode=stage_research_mode,
            execution_network_mode=execution_network_mode,
            cache_config=cache,
            host_facts=host_facts,
        )
    if stage.startswith("solver-"):
        role_data = solver_role_map(run_dir).get(stage)
        if role_data is None:
            raise SystemExit(f"Missing solver role for stage: {stage}")
        return render_solver_prompt(
            run_dir,
            solver_id=stage,
            role=role_data["role"],
            angle=role_data["angle"],
            validation_commands=validation_commands,
            prompt_format=prompt_format,
            stage_research_mode=stage_research_mode,
        )
    if stage == "review":
        return render_review_prompt(
            run_dir,
            validation_commands,
            plan.get("reviewer_stack", []),
            prompt_format,
            summary_language,
            stage_research_mode,
        )
    if stage == "execution":
        return render_execution_prompt(
            run_dir,
            validation_commands,
            prompt_format,
            stage_research_mode,
            execution_network_mode,
            cache,
            host_facts,
        )
    if stage == "verification":
        return render_verification_prompt(
            run_dir,
            validation_commands,
            prompt_format,
            summary_language,
            stage_research_mode,
            host_facts,
        )
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
        if augmented_follow_up_enabled(run_dir):
            outputs.append(augmented_task_path(run_dir))
        if goal_gate_enabled(run_dir):
            outputs.append(goal_status_path(run_dir))
        return outputs
    if stage.startswith("solver-"):
        return [run_dir / "solutions" / stage / "RESULT.md"]
    raise SystemExit(f"Unknown stage: {stage}")


def stage_placeholder_content(run_dir: Path, stage: str) -> dict[Path, str]:
    if stage == "intake":
        return {run_dir / "brief.md": "# Brief\n\nPending intake stage.\n"}
    if stage == "review":
        return {
            run_dir / "review" / "report.md": "# Review Report\n\nPending review stage.\n",
            run_dir / "review" / "scorecard.json": "{}\n",
            run_dir / "review" / "user-summary.md": "# User Summary\n\nPending localized review summary.\n",
        }
    if stage == "execution":
        return {run_dir / "execution" / "report.md": "# Execution Report\n\nPending execution stage.\n"}
    if stage == "verification":
        placeholders = {
            run_dir / "verification" / "findings.md": "# Findings\n\nPending verification stage.\n",
            run_dir / "verification" / "user-summary.md": "# Verification Summary\n\nPending localized verification summary.\n",
            run_dir / "verification" / "improvement-request.md": "# Improvement Request\n\nPending verification stage.\n",
        }
        if augmented_follow_up_enabled(run_dir):
            placeholders[augmented_task_path(run_dir)] = "# Augmented Task\n\nPending verification stage.\n"
        if goal_gate_enabled(run_dir):
            placeholders[goal_status_path(run_dir)] = "{}\n"
        return placeholders
    if stage.startswith("solver-"):
        return {run_dir / "solutions" / stage / "RESULT.md": "# Result\n\nFill this file with the solver output.\n"}
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


def augmented_task_path(run_dir: Path) -> Path:
    return run_dir / "verification" / "augmented-task.md"


def augmented_follow_up_enabled(run_dir: Path) -> bool:
    plan = load_plan(run_dir)
    return bool(plan.get("augmented_follow_up_enabled"))


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
        findings = run_dir / "verification" / "findings.md"
        summary = run_dir / "verification" / "user-summary.md"
        improvement_request = run_dir / "verification" / "improvement-request.md"
        if not findings.exists() or not summary.exists() or not improvement_request.exists():
            return False
        if output_looks_placeholder(stage, read_text(findings)):
            return False
        if output_looks_placeholder("verification-summary", read_text(summary)):
            return False
        if output_looks_placeholder("improvement-request", read_text(improvement_request)):
            return False
        if augmented_follow_up_enabled(run_dir):
            augmented_task = augmented_task_path(run_dir)
            if not augmented_task.exists() or output_looks_placeholder("augmented-task", read_text(augmented_task)):
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
    (run_dir / "host").mkdir(exist_ok=True)
    validation_commands = plan.get("validation_commands", [])
    prompt_format = plan.get("prompt_format", "markdown")
    summary_language = plan.get("summary_language", "ru")
    stage_research_mode = str(plan.get("stage_research_mode", "local-first"))
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
                    stage_research_mode=stage_research_mode,
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
                stage_research_mode,
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
                stage_research_mode,
                execution_network_mode,
                cache,
                HostFacts(**plan.get("host_facts", {})) if isinstance(plan.get("host_facts"), dict) and plan.get("host_facts") else HostFacts(
                    source="run_stage_fallback",
                    captured_at="unknown",
                    platform=sys.platform,
                    machine="unknown",
                    apple_silicon=False,
                    torch_installed=False,
                    cuda_available=None,
                    mps_built=None,
                    mps_available=None,
                    preferred_torch_device="cpu",
                ),
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
                stage_research_mode,
                HostFacts(**plan.get("host_facts", {})) if isinstance(plan.get("host_facts"), dict) and plan.get("host_facts") else HostFacts(
                    source="run_stage_fallback",
                    captured_at="unknown",
                    platform=sys.platform,
                    machine="unknown",
                    apple_silicon=False,
                    torch_installed=False,
                    cuda_available=None,
                    mps_built=None,
                    mps_available=None,
                    preferred_torch_device="cpu",
                ),
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
    verification_augmented_path = augmented_task_path(run_dir)
    if plan.get("augmented_follow_up_enabled") and not verification_augmented_path.exists():
        write_text(
            verification_augmented_path,
            "# Augmented Task\n\nPending verification stage.\n",
        )


def next_stage(run_dir: Path) -> str | None:
    for stage in available_stages(run_dir):
        if not is_stage_complete(run_dir, stage):
            return stage
    goal_status = load_goal_status(run_dir)
    if goal_status and not goal_status.get("goal_complete", False):
        return "rerun"
    return None


def stage_reset_order(run_dir: Path, stage: str) -> list[str]:
    stages = available_stages(run_dir)
    if stage not in stages:
        valid = ", ".join(stages)
        raise SystemExit(f"Unknown stage '{stage}'. Valid stages: {valid}")

    if stage == "intake":
        return stages
    if stage.startswith("solver-"):
        downstream = [item for item in ("review", "execution", "verification") if item in stages]
        return [stage, *downstream]
    if stage == "review":
        return [item for item in ("review", "execution", "verification") if item in stages]
    if stage == "execution":
        return [item for item in ("execution", "verification") if item in stages]
    if stage == "verification":
        return ["verification"]
    raise SystemExit(f"Unsupported stage for step-back: {stage}")


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
        f"- Stage research mode: `{plan.get('stage_research_mode', 'local-first')}`",
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
    if amendments_exist(run_dir):
        extra_rules.append(
            "- Treat `amendments.md` as the latest authoritative user input when it adds constraints, corrections, or newly clarified expected behavior."
        )
    if stage.startswith("solver-"):
        extra_rules.append("- Do not read sibling solver outputs.")
    if stage in {"execution", "verification"}:
        extra_rules.append(
            "- Treat the latest host probe artifact from the launcher as authoritative local runtime evidence for device availability and visible environment keys."
        )

    dynamic_context = []
    goal_checks = plan.get("goal_checks", [])
    if goal_checks:
        dynamic_context.append("Goal checks from current plan:")
        for item in goal_checks:
            criticality = "critical" if item.get("critical", True) else "supporting"
            dynamic_context.append(
                f"- `{criticality}` `{item.get('id', 'unknown')}`: {item.get('requirement', '')}"
            )
    host_facts = plan.get("host_facts")
    if isinstance(host_facts, dict) and host_facts:
        dynamic_context.append("Host facts from current plan:")
        for key in (
            "source",
            "captured_at",
            "platform",
            "machine",
            "apple_silicon",
            "torch_installed",
            "cuda_available",
            "mps_built",
            "mps_available",
            "preferred_torch_device",
        ):
            if key in host_facts:
                dynamic_context.append(f"- `{key}`: `{host_facts[key]}`")
    host_probe = load_host_probe(run_dir)
    if isinstance(host_probe, dict) and host_probe:
        dynamic_context.append("Latest host probe from launcher:")
        dynamic_context.append(f"- `artifact`: `{host_probe_path(run_dir)}`")
        for key in (
            "source",
            "captured_at",
            "python_executable",
            "python_version",
            "platform",
            "machine",
            "torch_installed",
            "cuda_available",
            "mps_built",
            "mps_available",
            "preferred_torch_device",
        ):
            if key in host_probe:
                dynamic_context.append(f"- `{key}`: `{host_probe[key]}`")
        visible_env_keys = host_probe.get("visible_env_keys")
        if isinstance(visible_env_keys, list) and visible_env_keys:
            dynamic_context.append("- `visible_env_keys`: " + ", ".join(f"`{key}`" for key in visible_env_keys))
        if isinstance(host_facts, dict):
            planned_device = host_facts.get("preferred_torch_device")
            probed_device = host_probe.get("preferred_torch_device")
            if planned_device and probed_device and planned_device != probed_device:
                dynamic_context.append(
                    f"- `host_drift`: plan expected `{planned_device}` but latest launcher probe sees `{probed_device}`"
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
                f"- Augmented task output: `{augmented_task_path(run_dir)}`",
            ]
        )
    if amendments_exist(run_dir):
        dynamic_context.extend(
            [
                "Latest user amendments:",
                f"- `artifact`: `{amendments_path(run_dir)}`",
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


def stage_status_map(run_dir: Path) -> dict[str, str]:
    return {
        stage: ("done" if is_stage_complete(run_dir, stage) else "pending")
        for stage in available_stages(run_dir)
    }


def goal_state(run_dir: Path) -> str:
    goal_status = load_goal_status(run_dir)
    if goal_status is not None:
        return "complete" if goal_status.get("goal_complete") else goal_status.get("goal_verdict", "partial")
    if goal_gate_enabled(run_dir):
        return "pending-verification"
    return "n/a"


def host_probe_state(run_dir: Path) -> tuple[str, str | None]:
    host_probe = load_host_probe(run_dir)
    if host_probe is not None:
        preferred = str(host_probe.get("preferred_torch_device", "unknown"))
        history_count = len(host_probe_history_paths(run_dir))
        host_facts = load_plan(run_dir).get("host_facts", {})
        if isinstance(host_facts, dict):
            planned = host_facts.get("preferred_torch_device")
            if planned and planned != preferred:
                return f"captured ({preferred}, {history_count} history)", f"plan={planned} probe={preferred}"
        return f"captured ({preferred}, {history_count} history)", None
    return "missing", None


def newest_mtime(paths: list[Path]) -> float | None:
    mtimes = [path.stat().st_mtime for path in paths if path.exists()]
    return max(mtimes) if mtimes else None


def oldest_mtime(paths: list[Path]) -> float | None:
    mtimes = [path.stat().st_mtime for path in paths if path.exists()]
    return min(mtimes) if mtimes else None


def review_is_stale(run_dir: Path) -> bool:
    if not is_stage_complete(run_dir, "review"):
        return False
    upstream = [run_dir / "brief.md", *(run_dir / "solutions" / solver / "RESULT.md" for solver in solver_ids(run_dir))]
    downstream = [
        run_dir / "review" / "report.md",
        run_dir / "review" / "scorecard.json",
        run_dir / "review" / "user-summary.md",
    ]
    newest_upstream = newest_mtime(upstream)
    oldest_downstream = oldest_mtime(downstream)
    return newest_upstream is not None and oldest_downstream is not None and newest_upstream > oldest_downstream


def execution_is_stale(run_dir: Path) -> bool:
    if not is_stage_complete(run_dir, "execution"):
        return False
    upstream = [
        run_dir / "review" / "report.md",
        run_dir / "review" / "scorecard.json",
        run_dir / "review" / "user-summary.md",
    ]
    downstream = [run_dir / "execution" / "report.md"]
    newest_upstream = newest_mtime(upstream)
    oldest_downstream = oldest_mtime(downstream)
    return newest_upstream is not None and oldest_downstream is not None and newest_upstream > oldest_downstream


def verification_is_stale(run_dir: Path) -> bool:
    if not is_stage_complete(run_dir, "verification"):
        return False
    upstream = [run_dir / "execution" / "report.md", host_probe_path(run_dir)]
    downstream = [
        run_dir / "verification" / "findings.md",
        run_dir / "verification" / "user-summary.md",
        run_dir / "verification" / "improvement-request.md",
        goal_status_path(run_dir),
        augmented_task_path(run_dir),
    ]
    newest_upstream = newest_mtime(upstream)
    oldest_downstream = oldest_mtime(downstream)
    return newest_upstream is not None and oldest_downstream is not None and newest_upstream > oldest_downstream


def amendments_require_reintake(run_dir: Path) -> bool:
    if not amendments_exist(run_dir):
        return False
    amendment_file = amendments_path(run_dir)
    brief_file = run_dir / "brief.md"
    if not brief_file.exists():
        return True
    return amendment_file.stat().st_mtime > brief_file.stat().st_mtime


def safe_next_action(run_dir: Path) -> str:
    statuses = stage_status_map(run_dir)
    if amendments_require_reintake(run_dir):
        return "step-back intake"
    if statuses.get("verification") == "done" and statuses.get("execution") != "done":
        return "step-back verification"
    if statuses.get("execution") == "done" and statuses.get("review") != "done":
        return "step-back execution"
    if statuses.get("review") == "done" and any(statuses.get(solver) != "done" for solver in solver_ids(run_dir)):
        return "step-back review"
    if verification_is_stale(run_dir):
        return "recheck verification"
    if execution_is_stale(run_dir):
        return "step-back execution"
    if review_is_stale(run_dir):
        return "step-back review"
    pending = next_stage(run_dir)
    if pending is None:
        return "none"
    if pending == "rerun":
        return "rerun"
    if pending.startswith("solver-"):
        return "start-solvers"
    return f"start {pending}"


def doctor_report(run_dir: Path) -> dict[str, object]:
    statuses = stage_status_map(run_dir)
    issues: list[dict[str, str]] = []
    warnings: list[dict[str, str]] = []
    stale: list[str] = []

    if amendments_require_reintake(run_dir):
        stale.append("intake")
        warnings.append(
            {
                "severity": "warn",
                "message": "New amendments are newer than the current brief and stage outputs.",
                "fix": "Run `step-back intake` and continue from intake so the new user correction affects the brief and downstream stages.",
            }
        )

    if statuses.get("verification") == "done" and statuses.get("execution") != "done":
        issues.append(
            {
                "severity": "error",
                "message": "Verification is marked done while execution is pending.",
                "fix": "Run `step-back verification` or complete execution before trusting verification findings.",
            }
        )
    if statuses.get("execution") == "done" and statuses.get("review") != "done":
        issues.append(
            {
                "severity": "error",
                "message": "Execution is marked done while review is pending.",
                "fix": "Run `step-back execution` or complete review before trusting execution evidence.",
            }
        )
    if statuses.get("review") == "done":
        stale_solvers = [solver for solver in solver_ids(run_dir) if statuses.get(solver) != "done"]
        if stale_solvers:
            issues.append(
                {
                    "severity": "error",
                    "message": "Review is marked done while some solver stages are pending.",
                    "fix": f"Run `step-back review` or complete the missing solver stages: {', '.join(stale_solvers)}.",
                }
            )

    if review_is_stale(run_dir):
        stale.append("review")
        warnings.append(
            {
                "severity": "warn",
                "message": "Review artifacts are older than the latest solver outputs or brief.",
                "fix": "Run `step-back review` and repeat review so the verdict matches the current solver state.",
            }
        )
    if execution_is_stale(run_dir):
        stale.append("execution")
        warnings.append(
            {
                "severity": "warn",
                "message": "Execution artifacts are older than the latest review artifacts.",
                "fix": "Run `step-back execution` and repeat execution before trusting verification or rerun guidance.",
            }
        )
    if verification_is_stale(run_dir):
        stale.append("verification")
        warnings.append(
            {
                "severity": "warn",
                "message": "Verification artifacts are older than the latest execution evidence or host probe.",
                "fix": "Run `recheck verification` and repeat verification to refresh findings and goal status.",
            }
        )

    host_probe_label, host_drift = host_probe_state(run_dir)
    if host_probe_label == "missing" and (statuses.get("execution") == "done" or statuses.get("verification") == "done"):
        warnings.append(
            {
                "severity": "warn",
                "message": "No launcher-side host probe artifact is present for a run that already reached execution or verification.",
                "fix": "Run `host-probe --refresh` before repeating device-sensitive stages.",
            }
        )
    if host_drift:
        warnings.append(
            {
                "severity": "warn",
                "message": f"Host drift detected: {host_drift}.",
                "fix": "Treat launcher probe as authoritative and rerun device-sensitive stages from the same host environment.",
            }
        )

    goal = goal_state(run_dir)
    if goal in {"partial", "blocked"} and next_stage(run_dir) != "rerun":
        warnings.append(
            {
                "severity": "warn",
                "message": f"Goal state is `{goal}` but next stage is not `rerun`.",
                "fix": "Re-run verification or inspect goal-status.json for stale artifacts.",
            }
        )

    if augmented_follow_up_enabled(run_dir) and statuses.get("verification") == "done":
        augmented_task = augmented_task_path(run_dir)
        if not augmented_task.exists() or output_looks_placeholder("augmented-task", read_text(augmented_task)):
            warnings.append(
                {
                    "severity": "warn",
                    "message": "Verification completed without a substantive augmented follow-up task.",
                    "fix": "Recheck verification so follow-up reruns preserve the full verified context.",
                }
            )

    health = "healthy"
    if issues:
        health = "broken"
    elif warnings:
        health = "warning"

    return {
        "run_dir": str(run_dir),
        "health": health,
        "stages": statuses,
        "stale": stale,
        "host_probe": host_probe_label,
        "host_drift": host_drift,
        "goal": goal,
        "next": next_stage(run_dir) or "none",
        "safe_next_action": safe_next_action(run_dir),
        "issues": issues,
        "warnings": warnings,
    }


def status_report(run_dir: Path) -> dict[str, object]:
    statuses = stage_status_map(run_dir)
    host_probe_label, host_drift = host_probe_state(run_dir)
    return {
        "run_dir": str(run_dir),
        "stages": statuses,
        "host_probe": host_probe_label,
        "host_drift": host_drift,
        "goal": goal_state(run_dir),
        "next": next_stage(run_dir) or "none",
    }


def print_status(run_dir: Path, as_json: bool = False) -> int:
    report = status_report(run_dir)
    if as_json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0

    statuses = report["stages"]
    for stage in available_stages(run_dir):
        print(f"{stage}: {statuses[stage]}")
    print(f"host-probe: {report['host_probe']}")
    if report["host_drift"]:
        print(f"host-drift: {report['host_drift']}")
    print(f"goal: {report['goal']}")
    print(f"next: {report['next']}")
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
    if stage in {"execution", "verification"}:
        capture_host_probe(run_dir)
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
    refresh_cache_index(cache_config(load_plan(run_dir)))
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
            refresh_cache_index(cache_config(load_plan(run_dir)))
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


def print_augmented_task(run_dir: Path) -> int:
    augmented_path = augmented_task_path(run_dir)
    if augmented_path.exists() and not output_looks_placeholder("augmented-task", read_text(augmented_path)):
        print(read_text(augmented_path).rstrip())
        return 0

    improvement_path = run_dir / "verification" / "improvement-request.md"
    if improvement_path.exists() and not output_looks_placeholder("improvement-request", read_text(improvement_path)):
        print("Augmented task is not available for this run. Improvement request:\n")
        print(read_text(improvement_path).rstrip())
        return 0
    raise SystemExit("Augmented follow-up task is not ready yet. Run the verification stage first.")


def print_doctor(run_dir: Path, as_json: bool = False) -> int:
    report = doctor_report(run_dir)
    if as_json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0

    print(f"health: {report['health']}")
    print(f"goal: {report['goal']}")
    print(f"next: {report['next']}")
    print(f"safe-next-action: {report['safe_next_action']}")
    print(f"host-probe: {report['host_probe']}")
    stale = report.get("stale", [])
    if stale:
        print(f"stale: {', '.join(stale)}")
    if report.get("host_drift"):
        print(f"host-drift: {report['host_drift']}")

    issues = report.get("issues", [])
    warnings = report.get("warnings", [])
    if issues:
        print("\nissues:")
        for item in issues:
            print(f"- {item['message']}")
            print(f"  fix: {item['fix']}")
    if warnings:
        print("\nwarnings:")
        for item in warnings:
            print(f"- {item['message']}")
            print(f"  fix: {item['fix']}")
    if not issues and not warnings:
        print("\nNo consistency issues detected.")
    return 0


def print_cache_status(run_dir: Path, refresh: bool, limit: int) -> int:
    plan = load_plan(run_dir)
    cache = cache_config(plan)
    index = load_cache_index(cache, refresh=refresh)

    print(f"cache enabled: {'yes' if cache.get('enabled') else 'no'}")
    print(f"cache policy: {cache.get('policy', 'off')}")
    print(f"cache root: {cache.get('root')}")
    print(f"cache index: {cache.get('meta', {}).get('index', 'n/a')}")
    print(f"generated at: {index.get('generated_at', 'n/a')}")
    print(f"total files: {index.get('total_files', 0)}")
    print(f"total size: {format_size(int(index.get('total_size_bytes', 0)))}")

    areas = index.get("areas", {})
    if isinstance(areas, dict) and areas:
        print("\nareas:")
        for area in ("research", "downloads", "wheelhouse", "models", "verification"):
            payload = areas.get(area)
            if not isinstance(payload, dict):
                continue
            print(
                f"- {area}: {payload.get('file_count', 0)} files, "
                f"{format_size(int(payload.get('size_bytes', 0)))}"
            )

    entries = index.get("entries", [])
    if isinstance(entries, list) and entries and limit > 0:
        print("\nlargest files:")
        largest = sorted(
            (entry for entry in entries if isinstance(entry, dict)),
            key=lambda entry: int(entry.get("size_bytes", 0)),
            reverse=True,
        )[:limit]
        for entry in largest:
            print(
                f"- {entry.get('area', 'unknown')}: "
                f"{entry.get('relative_path', entry.get('path', 'unknown'))} "
                f"({format_size(int(entry.get('size_bytes', 0)))}, {entry.get('modified_at', 'unknown')})"
            )
    return 0


def run_cache_prune(run_dir: Path, args: argparse.Namespace) -> int:
    plan = load_plan(run_dir)
    cache = cache_config(plan)
    result = prune_cache(
        cache,
        max_age_days=args.max_age_days,
        area_filters=set(args.area) if args.area else None,
        dry_run=args.dry_run,
    )

    mode = "Would remove" if args.dry_run else "Removed"
    print(f"{mode} {result['removed_files']} files, {format_size(int(result['removed_bytes']))}.")
    areas = result.get("areas", {})
    if isinstance(areas, dict) and areas:
        print("areas:")
        for area in ("research", "downloads", "wheelhouse", "models", "verification"):
            payload = areas.get(area)
            if not isinstance(payload, dict):
                continue
            print(
                f"- {area}: {payload.get('removed_files', 0)} files, "
                f"{format_size(int(payload.get('removed_bytes', 0)))}"
            )

    print("\ncache status:\n")
    return print_cache_status(run_dir, refresh=False, limit=5)


def run_host_probe(run_dir: Path, refresh: bool, history: bool) -> int:
    payload = capture_host_probe(run_dir) if refresh or load_host_probe(run_dir) is None else load_host_probe(run_dir)
    assert payload is not None
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    plan = load_plan(run_dir)
    host_facts = plan.get("host_facts", {})
    if isinstance(host_facts, dict):
        planned = host_facts.get("preferred_torch_device")
        actual = payload.get("preferred_torch_device")
        if planned and actual and planned != actual:
            print(f"\nhost drift: plan preferred_torch_device={planned}, launcher probe={actual}")
    if history:
        paths = host_probe_history_paths(run_dir)
        print(f"\nhistory ({len(paths)}):")
        for path in paths[-10:]:
            print(f"- {path}")
    return 0


def step_back_stage(run_dir: Path, stage: str, dry_run: bool) -> int:
    reset_stages = stage_reset_order(run_dir, stage)
    reset_files: list[Path] = []
    for item in reset_stages:
        reset_files.extend(stage_placeholder_content(run_dir, item).keys())

    print("Will reset stages:")
    for item in reset_stages:
        print(f"- {item}")
    print("\nWill reset files:")
    for path in reset_files:
        print(f"- {path}")

    if dry_run:
        print("\nDry run. No files changed.\n")
        print_status_after_action(run_dir)
        return 0

    for item in reset_stages:
        for path, content in stage_placeholder_content(run_dir, item).items():
            write_text(path, content)

    if stage in {"execution", "verification"}:
        probe_path = host_probe_path(run_dir)
        if probe_path.exists():
            probe_path.unlink()

    sync_run_artifacts(run_dir)
    refresh_cache_index(cache_config(load_plan(run_dir)))
    print("\nReset complete.\n")
    print_status_after_action(run_dir)
    return 0


def recheck_stage(run_dir: Path, stage: str, dry_run: bool) -> int:
    if stage != "verification":
        raise SystemExit(f"Unsupported recheck stage: {stage}")
    if not is_stage_complete(run_dir, "execution"):
        raise SystemExit("Execution stage must be complete before rechecking verification.")

    reset_files = list(stage_placeholder_content(run_dir, stage).keys())
    print("Will reset stage for clean rerun:")
    print(f"- {stage}")
    print("\nWill reset files:")
    for path in reset_files:
        print(f"- {path}")

    if dry_run:
        print("\nDry run. No files changed.\n")
        print_status_after_action(run_dir)
        return 0

    for path, content in stage_placeholder_content(run_dir, stage).items():
        write_text(path, content)

    sync_run_artifacts(run_dir)
    refresh_cache_index(cache_config(load_plan(run_dir)))
    print("\nRecheck reset complete.\n")
    print_status_after_action(run_dir)
    return 0


def refresh_stage_prompt(run_dir: Path, stage: str, dry_run: bool) -> int:
    prompt_text = render_stage_prompt(run_dir, stage)
    prompt_path = stage_prompt_path(run_dir, stage)

    if dry_run:
        print(prompt_text.rstrip())
        return 0

    write_text(prompt_path, prompt_text)
    print(f"Refreshed prompt: {prompt_path}")
    return 0


def refresh_all_stage_prompts(run_dir: Path, dry_run: bool) -> int:
    for stage in available_stages(run_dir):
        refresh_stage_prompt(run_dir, stage, dry_run=dry_run)
        if dry_run and stage != available_stages(run_dir)[-1]:
            print("\n---\n")
    return 0


def follow_up_prompt_path(run_dir: Path, prompt_source: str) -> Path:
    augmented_path = augmented_task_path(run_dir)
    improvement_path = run_dir / "verification" / "improvement-request.md"

    if prompt_source == "augmented":
        return augmented_path
    if prompt_source == "improvement":
        return improvement_path
    if augmented_path.exists() and not output_looks_placeholder("augmented-task", read_text(augmented_path)):
        return augmented_path
    return improvement_path


def create_follow_up_run(run_dir: Path, args: argparse.Namespace) -> int:
    if not is_stage_complete(run_dir, "verification"):
        raise SystemExit("Verification stage is still pending. Run verification first.")

    plan = load_plan(run_dir)
    prompt_file = follow_up_prompt_path(run_dir, args.prompt_source)
    if not prompt_file.exists():
        raise SystemExit(f"Missing follow-up prompt file: {prompt_file}")
    prompt_label = "augmented-task" if prompt_file == augmented_task_path(run_dir) else "improvement-request"
    if output_looks_placeholder(prompt_label, read_text(prompt_file)):
        raise SystemExit(f"Follow-up prompt is still a placeholder: {prompt_file}")

    output_dir = Path(args.output_dir).expanduser().resolve() if args.output_dir else run_dir.parent
    title = args.title or f"{run_dir.name}-improve"
    command = [
        sys.executable,
        str(SKILL_DIR / "scripts" / "init_run.py"),
        "--task-file",
        str(prompt_file),
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
        "--intake-research",
        str(plan.get("intake_research_mode", "research-first")),
        "--stage-research",
        str(plan.get("stage_research_mode", "local-first")),
        "--execution-network",
        str(plan.get("execution_network_mode", "fetch-if-needed")),
        "--cache-root",
        str(cache_config(plan).get("root")),
        "--cache-policy",
        str(cache_config(plan).get("policy", "off")),
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
        refresh_cache_index(cache_config(load_plan(new_run_dir)))
        print("\nNew run status:\n")
        print_status(new_run_dir)
    return 0


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    run_dir = Path(args.run_dir).expanduser().resolve()
    if not run_dir.exists():
        raise SystemExit(f"Run directory does not exist: {run_dir}")
    if not (run_dir / "plan.json").exists():
        raise SystemExit(f"Missing plan.json in run directory: {run_dir}")

    sync_run_artifacts(run_dir)

    if args.command == "status":
        return print_status(run_dir, as_json=args.json)

    if args.command == "doctor":
        return print_doctor(run_dir, as_json=args.json)

    if args.command == "next":
        print(next_stage(run_dir) or "none")
        return 0

    if args.command == "summary":
        return print_user_summary(run_dir)

    if args.command == "findings":
        return print_findings(run_dir)

    if args.command == "augmented-task":
        return print_augmented_task(run_dir)

    if args.command == "host-probe":
        return run_host_probe(run_dir, refresh=args.refresh, history=args.history)

    if args.command == "recheck":
        return recheck_stage(run_dir, args.stage, dry_run=args.dry_run)

    if args.command == "step-back":
        stage = resolve_stage(run_dir, args.stage)
        return step_back_stage(run_dir, stage, dry_run=args.dry_run)

    if args.command == "refresh-prompt":
        stage = resolve_stage(run_dir, args.stage)
        return refresh_stage_prompt(run_dir, stage, dry_run=args.dry_run)

    if args.command == "refresh-prompts":
        return refresh_all_stage_prompts(run_dir, dry_run=args.dry_run)

    if args.command == "cache-status":
        return print_cache_status(run_dir, refresh=args.refresh, limit=args.limit)

    if args.command == "cache-prune":
        return run_cache_prune(run_dir, args)

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
