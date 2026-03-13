#!/usr/bin/env python3
"""Human-friendly CLI and TUI for multi-agent-pipeline."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import textwrap
import time
from pathlib import Path

import run_stage


SCRIPT_DIR = Path(__file__).resolve().parent
RUN_STAGE_SCRIPT = SCRIPT_DIR / "run_stage.py"
INIT_RUN_SCRIPT = SCRIPT_DIR / "init_run.py"
DEFAULT_RUN_ROOT = Path.home() / "agent-runs"
DELEGATED_COMMANDS = {
    "status",
    "doctor",
    "next",
    "summary",
    "findings",
    "augmented-task",
    "host-probe",
    "recheck",
    "step-back",
    "cache-status",
    "cache-prune",
    "rerun",
    "show",
    "refresh-prompt",
    "refresh-prompts",
    "copy",
    "start",
    "start-solvers",
    "start-next",
}
AUTOPILOT_ORDER = {
    "intake": 1,
    "solvers": 2,
    "review": 3,
    "execution": 4,
    "verification": 5,
    "rerun": 6,
    "none": 7,
}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", help="CLI action such as runs, tui, status, start-next, or doctor")
    parser.add_argument("run_dir", nargs="?", help="Run directory for delegated commands")
    parser.add_argument("extra", nargs=argparse.REMAINDER, help="Remaining arguments for the delegated command")
    parser.add_argument("--root", default=str(DEFAULT_RUN_ROOT), help="Run root used by `runs` and `tui`")
    parser.add_argument("--limit", type=int, default=30, help="How many runs `runs` and `tui` should load")
    return parser.parse_args(argv)


def discover_runs(root: Path) -> list[Path]:
    if not root.exists():
        return []
    runs = [path for path in root.iterdir() if path.is_dir() and (path / "plan.json").exists()]
    return sorted(runs, key=lambda path: path.name, reverse=True)


def shorten(value: str, width: int) -> str:
    if width <= 0:
        return ""
    if len(value) <= width:
        return value
    if width <= 3:
        return value[:width]
    return value[: width - 3] + "..."


def preview_text(run_dir: Path, max_chars: int = 3000) -> tuple[str, str]:
    candidates = [
        ("Summary", run_dir / "review" / "user-summary.md"),
        ("Findings", run_dir / "verification" / "findings.md"),
        ("Augmented", run_dir / "verification" / "augmented-task.md"),
        ("Execution", run_dir / "execution" / "report.md"),
        ("Brief", run_dir / "brief.md"),
    ]
    for label, path in candidates:
        if not path.exists():
            continue
        try:
            content = path.read_text(encoding="utf-8").strip()
        except OSError:
            continue
        if not content or "Pending " in content[:64] or "Fill this file" in content[:64]:
            continue
        return label, content[:max_chars]
    return "Preview", "No substantive artifact is available yet."


def latest_log_excerpt(run_dir: Path, line_limit: int = 12) -> tuple[str, list[str]]:
    logs_dir = run_dir / "logs"
    if not logs_dir.exists():
        return "Logs", ["No log files yet."]
    logs = [path for path in logs_dir.iterdir() if path.is_file()]
    if not logs:
        return "Logs", ["No log files yet."]
    latest = max(logs, key=lambda path: path.stat().st_mtime)
    try:
        lines = latest.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return latest.name, ["Could not read log file."]
    excerpt = lines[-line_limit:] if lines else ["<empty log>"]
    return latest.name, excerpt


def render_doctor_summary(run_dir: Path) -> list[str]:
    report = run_stage.doctor_report(run_dir)
    lines = [
        f"Run: {run_dir.name}",
        f"Health: {report['health']}",
        f"Goal: {report['goal']}",
        f"Next: {report['next']}",
        f"Safe action: {report['safe_next_action']}",
        f"Host probe: {report['host_probe']}",
    ]
    stale = report.get("stale", [])
    if stale:
        lines.append(f"Stale: {', '.join(stale)}")
    if report.get("host_drift"):
        lines.append(f"Host drift: {report['host_drift']}")
    issues = report.get("issues", [])
    warnings = report.get("warnings", [])
    if issues:
        lines.append("")
        lines.append("Issues:")
        for item in issues:
            lines.append(f"- {item['message']}")
    if warnings:
        lines.append("")
        lines.append("Warnings:")
        for item in warnings:
            lines.append(f"- {item['message']}")
    return lines


def print_runs(root: Path, limit: int) -> int:
    runs = discover_runs(root)[:limit]
    if not runs:
        print(f"No runs found under {root}")
        return 0
    for run_dir in runs:
        report = run_stage.doctor_report(run_dir)
        print(
            f"{run_dir} | health={report['health']} goal={report['goal']} "
            f"next={report['next']} safe={report['safe_next_action']}"
        )
    return 0


def append_amendment(run_dir: Path, note: str) -> Path:
    path = run_stage.amendments_path(run_dir)
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    existing = ""
    if path.exists():
        try:
            existing = path.read_text(encoding="utf-8").rstrip()
        except OSError:
            existing = ""
    entry = f"## {timestamp}\n\n{note.strip()}\n"
    content = existing + ("\n\n" if existing else "# Amendments\n\n") + entry
    path.write_text(content, encoding="utf-8")
    return path


def delete_run(run_dir: Path) -> None:
    shutil.rmtree(run_dir)


def choose_prune_candidates(root: Path, *, keep: int, older_than_days: int | None) -> list[Path]:
    runs = discover_runs(root)
    protected = set(runs[: max(0, keep)])
    candidates: list[Path] = []
    now = time.time()
    threshold = None if older_than_days is None else now - older_than_days * 86400
    for run_dir in runs:
        if run_dir in protected:
            continue
        if threshold is not None and run_dir.stat().st_mtime > threshold:
            continue
        candidates.append(run_dir)
    return candidates


def timestamp_slug(label: str) -> str:
    return time.strftime("%Y%m%d-%H%M%S") + "-" + label


def read_task_text(task: str | None, task_file: str | None) -> str:
    if task:
        return task.strip()
    if task_file:
        return Path(task_file).expanduser().read_text(encoding="utf-8").strip()
    raise SystemExit("Provide --task or --task-file.")


def extract_json_object(text: str) -> dict[str, object]:
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise SystemExit("Interview agent did not return a JSON object.")
    try:
        payload = json.loads(text[start : end + 1])
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Interview agent returned invalid JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise SystemExit("Interview agent returned JSON, but it was not an object.")
    return payload


def run_codex_last_message(
    *,
    prompt: str,
    workdir: Path,
    artifact_dir: Path,
    label: str,
) -> str:
    artifact_dir.mkdir(parents=True, exist_ok=True)
    prompt_path = artifact_dir / f"{label}.prompt.md"
    last_message_path = artifact_dir / f"{label}.last.md"
    prompt_path.write_text(prompt, encoding="utf-8")
    command = [
        "codex",
        "exec",
        "--full-auto",
        "--skip-git-repo-check",
        "--color",
        "never",
        "-C",
        str(workdir),
        "--add-dir",
        str(SCRIPT_DIR.parent),
        "--output-last-message",
        str(last_message_path),
        "-",
    ]
    env = dict(os.environ)
    env.setdefault("NO_COLOR", "1")
    env.setdefault("TERM", "dumb")
    result = subprocess.run(command, input=prompt, text=True, env=env, check=False)
    if result.returncode != 0:
        raise SystemExit(f"codex exec failed during `{label}` with exit code {result.returncode}.")
    if not last_message_path.exists():
        raise SystemExit(f"codex exec did not write the expected last-message artifact for `{label}`.")
    return last_message_path.read_text(encoding="utf-8")


def next_stage_bucket(run_dir: Path) -> str:
    next_item = run_stage.next_stage(run_dir)
    if next_item is None:
        return "none"
    if next_item.startswith("solver-"):
        return "solvers"
    return next_item


def should_stop_before_execution(run_dir: Path, *, auto_approve: bool) -> bool:
    statuses = run_stage.stage_status_map(run_dir)
    return statuses.get("review") == "done" and statuses.get("execution") != "done" and not auto_approve


def prompt_execution_confirmation(run_dir: Path) -> bool:
    summary_path = run_dir / "review" / "user-summary.md"
    if summary_path.exists():
        try:
            summary = summary_path.read_text(encoding="utf-8").strip()
        except OSError:
            summary = ""
        if summary and "Pending " not in summary[:64]:
            print("\nReview summary:\n")
            print(summary)
            print()
    answer = input("Review is complete. Start execution now? [y/N]: ").strip().lower()
    return answer in {"y", "yes"}


def maybe_copy_interview_artifacts(session_dir: Path, run_dir: Path) -> None:
    interview_dir = run_dir / "interview"
    interview_dir.mkdir(parents=True, exist_ok=True)
    for path in session_dir.iterdir():
        if path.is_file():
            shutil.copy2(path, interview_dir / path.name)


def automate_run(run_dir: Path, *, until: str, auto_approve: bool) -> int:
    target_rank = AUTOPILOT_ORDER[until]
    while True:
        bucket = next_stage_bucket(run_dir)
        if bucket == "none":
            print("\nPipeline is complete for this run.")
            return 0
        if bucket == "rerun":
            print("\nVerification recommends a follow-up rerun.")
            return 0
        if AUTOPILOT_ORDER[bucket] > target_rank:
            return 0
        if should_stop_before_execution(run_dir, auto_approve=auto_approve):
            if not prompt_execution_confirmation(run_dir):
                print("\nPaused before execution.")
                return 0
        if bucket == "solvers":
            rc = external_delegate("start-solvers", run_dir, [])
        else:
            rc = external_delegate("start", run_dir, [bucket])
        if rc != 0:
            print(f"\nStopped after `{bucket}` with exit code {rc}.")
            return rc
        next_bucket = next_stage_bucket(run_dir)
        if next_bucket == bucket:
            print(f"\nNo stage progress detected after `{bucket}`. Check the latest status and logs.")
            return 1
        updated = run_stage.doctor_report(run_dir)
        if updated["safe_next_action"] == "rerun":
            return 0


def delegate(command: str, run_dir: str, extra: list[str]) -> int:
    return run_stage.main([run_dir, command, *extra])


def external_delegate(command: str, run_dir: Path, extra: list[str]) -> int:
    return subprocess.run(
        [sys.executable, str(RUN_STAGE_SCRIPT), str(run_dir), command, *extra],
        check=False,
    ).returncode


def parse_local_options(
    argv: list[str],
    *,
    default_root: str,
    default_limit: int,
    name: str,
) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog=f"map.py {name}")
    parser.add_argument("root", nargs="?", default=default_root)
    parser.add_argument("--root", dest="root_override")
    parser.add_argument("--limit", type=int, default=default_limit)
    local = parser.parse_args(argv)
    if local.root_override:
        local.root = local.root_override
    return local


def parse_interview_options(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="map.py interview")
    parser.add_argument("--task")
    parser.add_argument("--task-file")
    parser.add_argument("--workspace", default=".")
    parser.add_argument("--output-dir", default=str(DEFAULT_RUN_ROOT))
    parser.add_argument("--title")
    parser.add_argument("--language", default="ru")
    parser.add_argument("--max-questions", type=int, default=6)
    return parser.parse_args(argv)


def parse_run_options(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="map.py run")
    parser.add_argument("--task")
    parser.add_argument("--task-file")
    parser.add_argument("--workspace", required=True)
    parser.add_argument("--output-dir", default=str(DEFAULT_RUN_ROOT))
    parser.add_argument("--title")
    parser.add_argument("--prompt-format", choices=["markdown", "compact"], default="markdown")
    parser.add_argument("--summary-language", default="ru")
    parser.add_argument("--intake-research", choices=["research-first", "local-first", "local-only"], default="research-first")
    parser.add_argument("--stage-research", choices=["research-first", "local-first", "local-only"], default="local-first")
    parser.add_argument("--execution-network", choices=["fetch-if-needed", "local-only"], default="fetch-if-needed")
    parser.add_argument("--cache-root", default="~/.cache/multi-agent-pipeline")
    parser.add_argument("--cache-policy", choices=["reuse", "refresh", "off"], default="reuse")
    parser.add_argument("--until", choices=["intake", "solvers", "review", "execution", "verification"], default="review")
    parser.add_argument("--auto-approve", action="store_true", help="Continue into execution without an interactive confirmation at the review boundary")
    parser.add_argument("--skip-interview", action="store_true", help="Use the provided task as-is instead of running the stage0 interview flow")
    parser.add_argument("--max-questions", type=int, default=6)
    return parser.parse_args(argv)


def parse_resume_options(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="map.py resume")
    parser.add_argument("run_dir")
    parser.add_argument("--until", choices=["intake", "solvers", "review", "execution", "verification"], default="verification")
    parser.add_argument("--auto-approve", action="store_true", help="Continue into execution without an interactive confirmation at the review boundary")
    return parser.parse_args(argv)


def parse_amend_options(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="map.py amend")
    parser.add_argument("run_dir")
    parser.add_argument("--note")
    parser.add_argument("--note-file")
    parser.add_argument(
        "--rewind",
        choices=["intake", "review", "execution", "verification", "none"],
        default="intake",
        help="Stage to rewind after appending the amendment. Default: intake",
    )
    parser.add_argument("--auto-refresh-prompts", action="store_true", help="Refresh all prompts after the amendment is recorded")
    return parser.parse_args(argv)


def parse_rm_options(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="map.py rm")
    parser.add_argument("run_dir")
    parser.add_argument("--yes", action="store_true", help="Delete without interactive confirmation")
    return parser.parse_args(argv)


def parse_prune_runs_options(argv: list[str], default_root: str, default_limit: int) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="map.py prune-runs")
    parser.add_argument("root", nargs="?", default=default_root)
    parser.add_argument("--keep", type=int, default=default_limit, help="Keep this many newest runs regardless of age")
    parser.add_argument("--older-than-days", type=int, help="Only prune runs older than this many days")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be deleted without deleting it")
    parser.add_argument("--yes", action="store_true", help="Delete without interactive confirmation")
    return parser.parse_args(argv)


def build_interview_questions_prompt(*, raw_task: str, workspace: Path, language: str, max_questions: int) -> str:
    return f"""You are the stage0 interview agent for multi-agent-pipeline.

Your job is to read the raw request, inspect the workspace when useful, and ask the domain-specific clarification questions that are actually needed before building an execution-ready task prompt.

Raw task:
{raw_task}

Workspace:
- path: `{workspace}`
- exists: `{workspace.exists()}`

Return JSON only. Use this schema:
{{
  "goal_summary": "short summary of the original goal",
  "questions": [
    {{
      "id": "stable_snake_case_id",
      "question": "user-facing question in {language}",
      "why": "short reason in {language}",
      "required": true
    }}
  ]
}}

Rules:
- preserve the original goal exactly; do not shrink it
- ask all important domain questions that materially affect decomposition, implementation, or goal verification
- ask no more than {max_questions} questions
- avoid questions already answered by the raw task
- prefer concrete engineering questions over generic project-management questions
- if the task is already clear enough, return an empty "questions" list
"""


def build_interview_finalize_prompt(*, raw_task: str, qa_pairs: list[dict[str, str]], language: str) -> str:
    qa_json = json.dumps(qa_pairs, ensure_ascii=False, indent=2)
    return f"""You are the stage0 prompt builder for multi-agent-pipeline.

Your job is to turn the raw request plus clarification answers into the final task prompt that will be passed into init_run.py.

Raw task:
{raw_task}

Clarifications:
{qa_json}

Write the final task in {language}. Return markdown only, with no code fences.

Rules:
- preserve the original goal exactly; do not downgrade it to scaffold-only or architecture-only
- incorporate the answered constraints and preferences directly
- carry forward unresolved uncertainties as explicit blockers or open assumptions
- make the task execution-ready for downstream agents
- include what counts as done
- include do-not-regress constraints when the answers imply them
"""


def run_interview_session(
    *,
    raw_task: str,
    workspace: Path,
    output_root: Path,
    title: str | None,
    language: str,
    max_questions: int,
) -> tuple[Path, Path]:
    session_dir = output_root / "_interviews" / timestamp_slug((title or "interview").lower().replace(" ", "-"))
    session_dir.mkdir(parents=True, exist_ok=False)
    (session_dir / "logs").mkdir()
    (session_dir / "raw-task.md").write_text(raw_task.rstrip() + "\n", encoding="utf-8")

    questions_prompt = build_interview_questions_prompt(
        raw_task=raw_task,
        workspace=workspace,
        language=language,
        max_questions=max_questions,
    )
    raw_questions = run_codex_last_message(
        prompt=questions_prompt,
        workdir=workspace if workspace.exists() else Path.cwd(),
        artifact_dir=session_dir / "logs",
        label="interview-questions",
    )
    question_payload = extract_json_object(raw_questions)
    (session_dir / "questions.json").write_text(
        json.dumps(question_payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    qa_pairs: list[dict[str, str]] = []
    questions = question_payload.get("questions", [])
    if isinstance(questions, list) and questions:
        print("\nStage0 interview\n")
        summary = question_payload.get("goal_summary")
        if isinstance(summary, str) and summary.strip():
            print(f"Goal summary: {summary.strip()}\n")
        for index, item in enumerate(questions, start=1):
            if not isinstance(item, dict):
                continue
            question = str(item.get("question", "")).strip()
            if not question:
                continue
            why = str(item.get("why", "")).strip()
            required = bool(item.get("required", True))
            print(f"{index}. {question}")
            if why:
                print(f"   why: {why}")
            answer = input("> ").strip()
            if required and not answer:
                while not answer:
                    answer = input("> ").strip()
            qa_pairs.append(
                {
                    "id": str(item.get("id", f"q{index}")),
                    "question": question,
                    "answer": answer,
                }
            )
            print()
    (session_dir / "answers.json").write_text(json.dumps(qa_pairs, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    final_prompt = build_interview_finalize_prompt(raw_task=raw_task, qa_pairs=qa_pairs, language=language)
    final_task_text = run_codex_last_message(
        prompt=final_prompt,
        workdir=workspace if workspace.exists() else Path.cwd(),
        artifact_dir=session_dir / "logs",
        label="interview-finalize",
    )
    final_task_path = session_dir / "final-task.md"
    final_task_path.write_text(final_task_text.rstrip() + "\n", encoding="utf-8")
    return session_dir, final_task_path


def create_run(
    *,
    task_file: Path,
    workspace: Path,
    output_dir: Path,
    title: str | None,
    prompt_format: str,
    summary_language: str,
    intake_research: str,
    stage_research: str,
    execution_network: str,
    cache_root: str,
    cache_policy: str,
) -> Path:
    command = [
        sys.executable,
        str(INIT_RUN_SCRIPT),
        "--task-file",
        str(task_file),
        "--workspace",
        str(workspace),
        "--output-dir",
        str(output_dir),
        "--prompt-format",
        prompt_format,
        "--summary-language",
        summary_language,
        "--intake-research",
        intake_research,
        "--stage-research",
        stage_research,
        "--execution-network",
        execution_network,
        "--cache-root",
        cache_root,
        "--cache-policy",
        cache_policy,
    ]
    if title:
        command.extend(["--title", title])
    result = subprocess.run(command, text=True, capture_output=True, check=False)
    if result.returncode != 0:
        raise SystemExit(result.stderr.strip() or result.stdout.strip() or "init_run.py failed")
    run_dir = Path(result.stdout.strip()).expanduser().resolve()
    if not run_dir.exists():
        raise SystemExit(f"init_run.py reported a run directory that does not exist: {run_dir}")
    return run_dir


def wrap_lines(lines: list[str], width: int) -> list[str]:
    wrapped: list[str] = []
    for line in lines:
        if not line:
            wrapped.append("")
            continue
        wrapped.extend(textwrap.wrap(line, width=width, replace_whitespace=False, drop_whitespace=False) or [""])
    return wrapped


def run_tui(root: Path, limit: int) -> int:
    try:
        import curses
    except ImportError as exc:
        raise SystemExit(f"curses is not available in this Python runtime: {exc}") from exc

    def _dashboard(stdscr: "curses._CursesWindow") -> int:
        curses.curs_set(0)
        stdscr.keypad(True)
        index = 0
        notice = "Ready"

        def current_runs() -> list[Path]:
            return discover_runs(root)[:limit]

        runs = current_runs()

        def run_action(command: str, run_dir: Path, *extra: str) -> int:
            nonlocal notice, runs, index
            curses.def_prog_mode()
            curses.endwin()
            try:
                rc = external_delegate(command, run_dir, list(extra))
                print(f"\nCommand exited with code {rc}. Press Enter to return to map.")
                try:
                    input()
                except EOFError:
                    pass
            finally:
                curses.reset_prog_mode()
                curses.curs_set(0)
                stdscr.clear()
                stdscr.refresh()
            runs = current_runs()
            if runs:
                index = max(0, min(index, len(runs) - 1))
            notice = f"Last command: {command} ({rc})"
            return rc

        while True:
            stdscr.erase()
            height, width = stdscr.getmaxyx()
            left_width = min(48, max(30, width // 3))
            right_x = left_width + 1
            right_width = max(20, width - right_x - 1)
            log_height = max(6, height // 4)
            top_height = max(8, height - log_height - 2)

            header = "MAP TUI  q quit  j/k move  g refresh  n next  s solvers  e execution  v verification  r rerun  h host-probe  p prompts"
            stdscr.addnstr(0, 0, shorten(header, width - 1), width - 1, curses.A_BOLD)
            stdscr.addnstr(1, 0, shorten(f"root: {root} | {notice}", width - 1), width - 1)

            if not runs:
                stdscr.addnstr(3, 0, shorten("No runs found. Press g to refresh or q to quit.", width - 1), width - 1)
                key = stdscr.getch()
                if key in (ord("q"), 27):
                    return 0
                if key == ord("g"):
                    runs = current_runs()
                continue

            selected = runs[index]
            report = run_stage.doctor_report(selected)

            stdscr.addnstr(3, 0, shorten("Runs", left_width - 1), left_width - 1, curses.A_UNDERLINE)
            for row, run_dir in enumerate(runs[: max(1, top_height - 5)], start=4):
                snapshot = run_stage.doctor_report(run_dir)
                line = f"{run_dir.name} [{snapshot['goal']}] -> {snapshot['next']}"
                attr = curses.A_REVERSE if run_dir == selected else curses.A_NORMAL
                stdscr.addnstr(row, 0, shorten(line, left_width - 1), left_width - 1, attr)

            summary_lines = render_doctor_summary(selected)
            preview_label, preview = preview_text(selected)
            detail_lines = summary_lines + ["", f"{preview_label}:", *preview.splitlines()]
            wrapped_detail = wrap_lines(detail_lines, right_width - 1)
            stdscr.addnstr(3, right_x, shorten("Details", right_width - 1), right_width - 1, curses.A_UNDERLINE)
            for offset, line in enumerate(wrapped_detail[: max(1, top_height - 3)], start=4):
                stdscr.addnstr(offset, right_x, shorten(line, right_width - 1), right_width - 1)

            log_title, log_lines = latest_log_excerpt(selected)
            log_y = height - log_height
            stdscr.addnstr(log_y, 0, shorten(f"Log tail: {log_title}", width - 1), width - 1, curses.A_UNDERLINE)
            wrapped_log = wrap_lines(log_lines, width - 1)
            for offset, line in enumerate(wrapped_log[: log_height - 1], start=log_y + 1):
                if offset >= height:
                    break
                stdscr.addnstr(offset, 0, shorten(line, width - 1), width - 1)

            stdscr.refresh()
            key = stdscr.getch()
            if key in (ord("q"), 27):
                return 0
            if key in (curses.KEY_DOWN, ord("j")):
                index = min(len(runs) - 1, index + 1)
                continue
            if key in (curses.KEY_UP, ord("k")):
                index = max(0, index - 1)
                continue
            if key == ord("g"):
                runs = current_runs()
                index = min(index, max(0, len(runs) - 1)) if runs else 0
                notice = "Refreshed run list"
                continue
            if key == ord("n"):
                run_action("start-next", selected)
                continue
            if key == ord("s"):
                run_action("start-solvers", selected)
                continue
            if key == ord("e"):
                run_action("start", selected, "execution")
                continue
            if key == ord("v"):
                run_action("start", selected, "verification")
                continue
            if key == ord("r"):
                run_action("rerun", selected)
                continue
            if key == ord("h"):
                run_action("host-probe", selected, "--refresh")
                continue
            if key == ord("p"):
                run_action("refresh-prompts", selected)
                continue

    return curses.wrapper(_dashboard)


def run_interview_command(args: argparse.Namespace) -> int:
    raw_task = read_task_text(args.task, args.task_file)
    workspace = Path(args.workspace).expanduser().resolve()
    output_root = Path(args.output_dir).expanduser().resolve()
    session_dir, final_task_path = run_interview_session(
        raw_task=raw_task,
        workspace=workspace,
        output_root=output_root,
        title=args.title,
        language=args.language,
        max_questions=args.max_questions,
    )
    print(f"interview session: {session_dir}")
    print(f"final task: {final_task_path}\n")
    print(final_task_path.read_text(encoding="utf-8").rstrip())
    return 0


def run_create_and_automate(args: argparse.Namespace) -> int:
    raw_task = read_task_text(args.task, args.task_file)
    workspace = Path(args.workspace).expanduser().resolve()
    output_dir = Path(args.output_dir).expanduser().resolve()

    if args.skip_interview:
        task_dir = output_dir / "_interviews" / timestamp_slug((args.title or "direct-task").lower().replace(" ", "-"))
        task_dir.mkdir(parents=True, exist_ok=False)
        final_task_path = task_dir / "final-task.md"
        final_task_path.write_text(raw_task.rstrip() + "\n", encoding="utf-8")
        session_dir = task_dir
    else:
        session_dir, final_task_path = run_interview_session(
            raw_task=raw_task,
            workspace=workspace,
            output_root=output_dir,
            title=args.title,
            language=args.summary_language,
            max_questions=args.max_questions,
        )

    run_dir = create_run(
        task_file=final_task_path,
        workspace=workspace,
        output_dir=output_dir,
        title=args.title,
        prompt_format=args.prompt_format,
        summary_language=args.summary_language,
        intake_research=args.intake_research,
        stage_research=args.stage_research,
        execution_network=args.execution_network,
        cache_root=args.cache_root,
        cache_policy=args.cache_policy,
    )
    maybe_copy_interview_artifacts(session_dir, run_dir)
    print(run_dir)
    return automate_run(run_dir, until=args.until, auto_approve=args.auto_approve)


def run_resume_command(args: argparse.Namespace) -> int:
    run_dir = Path(args.run_dir).expanduser().resolve()
    if not run_dir.exists():
        raise SystemExit(f"Run directory does not exist: {run_dir}")
    return automate_run(run_dir, until=args.until, auto_approve=args.auto_approve)


def read_amendment_text(note: str | None, note_file: str | None) -> str:
    if note:
        return note.strip()
    if note_file:
        return Path(note_file).expanduser().read_text(encoding="utf-8").strip()
    raise SystemExit("Provide --note or --note-file.")


def run_amend_command(args: argparse.Namespace) -> int:
    run_dir = Path(args.run_dir).expanduser().resolve()
    if not run_dir.exists():
        raise SystemExit(f"Run directory does not exist: {run_dir}")
    note = read_amendment_text(args.note, args.note_file)
    amendment_file = append_amendment(run_dir, note)
    print(f"Recorded amendment in {amendment_file}")

    if args.rewind != "none":
        rc = delegate("step-back", str(run_dir), [args.rewind])
        if rc != 0:
            return rc
    if args.auto_refresh_prompts:
        rc = delegate("refresh-prompts", str(run_dir), [])
        if rc != 0:
            return rc
    return 0


def run_rm_command(args: argparse.Namespace) -> int:
    run_dir = Path(args.run_dir).expanduser().resolve()
    if not run_dir.exists():
        raise SystemExit(f"Run directory does not exist: {run_dir}")
    if not args.yes:
        answer = input(f"Delete run {run_dir}? [y/N]: ").strip().lower()
        if answer not in {"y", "yes"}:
            print("Cancelled.")
            return 0
    delete_run(run_dir)
    print(f"Deleted {run_dir}")
    return 0


def run_prune_runs_command(args: argparse.Namespace) -> int:
    root = Path(args.root).expanduser().resolve()
    candidates = choose_prune_candidates(root, keep=args.keep, older_than_days=args.older_than_days)
    if not candidates:
        print("No runs matched the prune criteria.")
        return 0
    print("Runs to delete:")
    for run_dir in candidates:
        print(f"- {run_dir}")
    if args.dry_run:
        return 0
    if not args.yes:
        answer = input("Delete these runs? [y/N]: ").strip().lower()
        if answer not in {"y", "yes"}:
            print("Cancelled.")
            return 0
    for run_dir in candidates:
        delete_run(run_dir)
    print(f"Deleted {len(candidates)} runs.")
    return 0


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    command = args.command

    if command == "interview":
        local = parse_interview_options(([args.run_dir] if args.run_dir else []) + args.extra)
        return run_interview_command(local)

    if command == "run":
        local = parse_run_options(([args.run_dir] if args.run_dir else []) + args.extra)
        return run_create_and_automate(local)

    if command == "resume":
        local = parse_resume_options(([args.run_dir] if args.run_dir else []) + args.extra)
        return run_resume_command(local)

    if command == "amend":
        local = parse_amend_options(([args.run_dir] if args.run_dir else []) + args.extra)
        return run_amend_command(local)

    if command == "rm":
        local = parse_rm_options(([args.run_dir] if args.run_dir else []) + args.extra)
        return run_rm_command(local)

    if command == "prune-runs":
        local = parse_prune_runs_options(
            ([args.run_dir] if args.run_dir else []) + args.extra,
            default_root=args.root,
            default_limit=args.limit,
        )
        return run_prune_runs_command(local)

    if command == "runs":
        local = parse_local_options(
            ([args.run_dir] if args.run_dir else []) + args.extra,
            default_root=args.root,
            default_limit=args.limit,
            name="runs",
        )
        return print_runs(Path(local.root).expanduser().resolve(), local.limit)

    if command == "tui":
        local = parse_local_options(
            ([args.run_dir] if args.run_dir else []) + args.extra,
            default_root=args.root,
            default_limit=args.limit,
            name="tui",
        )
        return run_tui(Path(local.root).expanduser().resolve(), local.limit)

    if command not in DELEGATED_COMMANDS:
        raise SystemExit(f"Unknown command: {command}")
    if not args.run_dir:
        raise SystemExit(f"Command `{command}` requires <run_dir>.")
    return delegate(command, args.run_dir, args.extra)


if __name__ == "__main__":
    raise SystemExit(main())
