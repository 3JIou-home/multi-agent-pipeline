#!/usr/bin/env python3
"""Black-box agpipe TUI smoke harness driven by pexpect.

This script is intentionally external to the Rust test suite. It is useful for:

- recording deterministic TUI demos with VHS
- debugging terminal behavior from the outside
- reproducing wizard regressions without recompiling test helpers
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import shutil
import stat
import sys
import tempfile
import textwrap
import time

import pexpect
import pyte


TASK_TEXT = (
    "Сделай CLI на Python для обработки текстовых файлов, "
    "но сначала уточни у меня сценарий использования, аргументы и ожидаемый вывод."
)
ANSWER_TEXT = (
    "Нужны простой CLI, понятные аргументы, README с примером запуска "
    "и предсказуемый текстовый вывод."
)


def parse_args() -> argparse.Namespace:
    repo_root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--agpipe",
        default=str(repo_root / "target" / "release" / "agpipe"),
        help="Path to the agpipe binary.",
    )
    parser.add_argument(
        "--mode",
        choices=("create-only", "run-all"),
        default="run-all",
        help="Stop after creating the run or continue through the full mock pipeline.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=20.0,
        help="Default expect timeout in seconds.",
    )
    parser.add_argument(
        "--run-root",
        default="",
        help="Existing run root to reuse. Defaults to a temporary directory.",
    )
    parser.add_argument(
        "--workspace",
        default="",
        help="Existing workspace path to reuse. Defaults to a temporary directory.",
    )
    parser.add_argument(
        "--title",
        default="pexpect-vhs-demo",
        help="Pipeline title to enter in the wizard.",
    )
    parser.add_argument(
        "--keep-artifacts",
        action="store_true",
        help="Keep temporary roots instead of deleting them on exit.",
    )
    return parser.parse_args()


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def build_mock_codex(root: Path) -> Path:
    script_path = root / "mock-codex.zsh"
    script = textwrap.dedent(
        """\
#!/bin/zsh
set -euo pipefail

last_message=""
while (( $# > 0 )); do
  case "$1" in
    --output-last-message)
      last_message="$2"
      shift 2
      ;;
    *)
      shift
      ;;
  esac
done

prompt="$(cat)"
label="${${last_message:t}%.last.md}"
root="${last_message:h:h}"
mkdir -p "$root"

case "$label" in
  intake)
    cat > "$root/brief.md" <<'EOF'
# Brief

Mock intake completed.
EOF
    cat > "$last_message" <<'EOF'
Mock intake complete.
EOF
    ;;
  solver-*)
    mkdir -p "$root/solutions/$label"
    cat > "$root/solutions/$label/RESULT.md" <<EOF
# Result

Mock solution from $label.
EOF
    cat > "$last_message" <<EOF
Mock $label complete.
EOF
    ;;
  review)
    mkdir -p "$root/review"
    cat > "$root/review/report.md" <<'EOF'
# Review Report

Mock review complete.
EOF
    cat > "$root/review/scorecard.json" <<'JSON'
{
  "winner": "solver-a",
  "backup": "solver-b",
  "risks": []
}
JSON
    cat > "$root/review/user-summary.md" <<'EOF'
# User Summary

Mock review summary.
EOF
    cat > "$last_message" <<'EOF'
Mock review complete.
EOF
    ;;
  execution)
    mkdir -p "$root/execution"
    cat > "$root/execution/report.md" <<'EOF'
# Execution Report

Mock execution complete.
EOF
    cat > "$last_message" <<'EOF'
Mock execution complete.
EOF
    ;;
  verification)
    mkdir -p "$root/verification"
    cat > "$root/verification/findings.md" <<'EOF'
# Findings

Mock verification complete.
EOF
    cat > "$root/verification/user-summary.md" <<'EOF'
# Verification Summary

Mock verification summary.
EOF
    cat > "$root/verification/improvement-request.md" <<'EOF'
# Improvement Request

No follow-up changes required.
EOF
    cat > "$root/verification/augmented-task.md" <<'EOF'
# Augmented Task

No follow-up run required.
EOF
    cat > "$root/verification/goal-status.json" <<'JSON'
{
  "goal_complete": true,
  "goal_verdict": "complete",
  "rerun_recommended": false,
  "recommended_next_action": "none"
}
JSON
    cat > "$last_message" <<'EOF'
Mock verification complete.
EOF
    ;;
  *)
    cat > "$last_message" <<EOF
Mock $label complete.
EOF
    ;;
esac

print "mock $label complete"
"""
    )
    write_text(script_path, script)
    script_path.chmod(script_path.stat().st_mode | stat.S_IXUSR)
    return script_path


def drain_screen(
    child: pexpect.spawn, stream: pyte.Stream, deadline: float, *, quiet: bool = False
) -> None:
    while time.monotonic() < deadline:
        remaining = max(0.0, deadline - time.monotonic())
        try:
            chunk = child.read_nonblocking(size=65535, timeout=min(0.1, remaining))
        except pexpect.TIMEOUT:
            return
        except pexpect.EOF:
            return
        if not chunk:
            return
        stream.feed(chunk)
        if not quiet:
            sys.stdout.write(chunk)
            sys.stdout.flush()


def screen_text(screen: pyte.Screen) -> str:
    return "\n".join(screen.display)


def wait_screen_contains(
    child: pexpect.spawn,
    stream: pyte.Stream,
    screen: pyte.Screen,
    needle: str,
    timeout: float,
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if needle in screen_text(screen):
            return
        drain_screen(child, stream, deadline)
    current = screen_text(screen)
    raise RuntimeError(f"Timed out waiting for `{needle}`.\nCurrent screen:\n{current}")


def wait_screen_any(
    child: pexpect.spawn,
    stream: pyte.Stream,
    screen: pyte.Screen,
    needles: list[str],
    timeout: float,
) -> str:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        current = screen_text(screen)
        for needle in needles:
            if needle in current:
                return needle
        drain_screen(child, stream, deadline)
    current = screen_text(screen)
    joined = ", ".join(needles)
    raise RuntimeError(f"Timed out waiting for one of [{joined}].\nCurrent screen:\n{current}")


def find_created_run(run_root: Path) -> Path:
    runs = [
        path
        for path in sorted(run_root.iterdir())
        if path.is_dir() and path.name not in {".agpipe-ui", "_interviews"}
    ]
    if not runs:
        raise RuntimeError(f"No run directory was created under {run_root}")
    return runs[-1]


def run() -> int:
    args = parse_args()
    repo_root = Path(__file__).resolve().parents[1]
    agpipe = Path(args.agpipe).expanduser().resolve()
    if not agpipe.exists():
        raise SystemExit(
            f"agpipe binary not found at {agpipe}. Build it first with `cargo build --release`."
        )

    temp_paths: list[Path] = []
    if args.run_root:
        run_root = Path(args.run_root).expanduser().resolve()
        run_root.mkdir(parents=True, exist_ok=True)
    else:
        run_root = Path(tempfile.mkdtemp(prefix="agpipe-tui-pexpect-runs-"))
        temp_paths.append(run_root)

    if args.workspace:
        workspace = Path(args.workspace).expanduser().resolve()
        workspace.mkdir(parents=True, exist_ok=True)
    else:
        workspace = Path(tempfile.mkdtemp(prefix="agpipe-tui-pexpect-workspace-"))
        temp_paths.append(workspace)

    mock_root = Path(tempfile.mkdtemp(prefix="agpipe-tui-pexpect-mock-"))
    temp_paths.append(mock_root)
    mock_codex = build_mock_codex(mock_root)

    env = os.environ.copy()
    env.update(
        {
            "AGPIPE_CODEX_BIN": str(mock_codex),
            "AGPIPE_STAGE0_BACKEND": "local",
            "AGPIPE_CODEX_EPHEMERAL": "1",
            "TERM": env.get("TERM", "xterm-256color"),
            "COLORTERM": env.get("COLORTERM", "truecolor"),
        }
    )

    child = pexpect.spawn(
        str(agpipe),
        ["ui", "--root", str(run_root)],
        cwd=str(repo_root),
        env=env,
        encoding="utf-8",
        dimensions=(40, 120),
        timeout=args.timeout,
    )
    screen = pyte.Screen(120, 40)
    stream = pyte.Stream(screen)

    created_run = None
    try:
        drain_screen(child, stream, time.monotonic() + args.timeout)
        wait_screen_contains(child, stream, screen, "No runs found.", args.timeout)
        child.send("c")
        wait_screen_contains(child, stream, screen, "New Pipeline", args.timeout)
        child.send(TASK_TEXT)
        child.send("\t")
        child.send(str(workspace))
        child.send("\t")
        child.send(args.title)
        child.send("\t")
        child.send("\r")

        wait_screen_contains(child, stream, screen, "Interview", args.timeout)
        wait_screen_contains(child, stream, screen, "Answer", args.timeout)
        child.send(ANSWER_TEXT)
        child.send("\r")

        wait_screen_contains(child, stream, screen, "Final Task Prompt", args.timeout)
        if args.mode == "run-all":
            wait_screen_contains(
                child, stream, screen, "Create + Run All", args.timeout
            )
            child.send("\t")
        child.send("\r")

        if args.mode == "run-all":
            wait_screen_any(
                child, stream, screen, ["goal=complete", "next=none"], args.timeout * 3
            )
        else:
            wait_screen_contains(
                child, stream, screen, "Pipeline is ready.", args.timeout
            )

        created_run = find_created_run(run_root)
        request_path = created_run / "request.md"
        if not request_path.exists():
            raise RuntimeError(f"Expected request artifact is missing: {request_path}")

        interview_dir = created_run / "interview"
        expected_interview = [
            interview_dir / "raw-task.md",
            interview_dir / "questions.json",
            interview_dir / "answers-ui.json",
            interview_dir / "final-task.md",
        ]
        missing = [path for path in expected_interview if not path.exists()]
        if missing:
            names = ", ".join(str(path) for path in missing)
            raise RuntimeError(f"Missing interview artifacts: {names}")

        if args.mode == "run-all":
            verification = created_run / "verification" / "goal-status.json"
            if not verification.exists():
                raise RuntimeError(
                    f"Expected verification artifact is missing: {verification}"
                )

        child.send("q")
        child.expect(pexpect.EOF, timeout=args.timeout)
        print(
            f"\nTUI smoke passed: mode={args.mode} run_root={run_root} "
            f"workspace={workspace} run_dir={created_run}"
        )
        return 0
    finally:
        if child.isalive():
            child.close(force=True)
        if not args.keep_artifacts:
            for path in reversed(temp_paths):
                shutil.rmtree(path, ignore_errors=True)


if __name__ == "__main__":
    try:
        raise SystemExit(run())
    except KeyboardInterrupt:
        raise SystemExit(130)
