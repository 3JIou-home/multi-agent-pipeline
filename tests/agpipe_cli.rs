use serde::Deserialize;
use serde_json::{json, Value};
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

#[derive(Debug, Deserialize)]
struct InterviewQuestion {
    id: String,
    question: String,
}

#[derive(Debug, Deserialize)]
struct InterviewQuestionsPayload {
    session_dir: String,
    questions: Vec<InterviewQuestion>,
}

#[derive(Debug, Deserialize)]
struct InterviewFinalizePayload {
    session_dir: String,
    final_task_path: String,
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn agpipe_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_agpipe"))
}

fn temp_dir(name: &str) -> PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let path = std::env::temp_dir().join(format!("agpipe-cli-test-{name}-{unique}"));
    fs::create_dir_all(&path).expect("create temp dir");
    path
}

fn write_text(path: &Path, text: &str) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("create parent");
    }
    fs::write(path, text).expect("write text");
}

fn run_agpipe<I, S>(args: I, codex_bin: &Path) -> Output
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    run_agpipe_with_env(args, codex_bin, &[])
}

fn run_agpipe_with_env<I, S>(args: I, codex_bin: &Path, extra_env: &[(&str, &str)]) -> Output
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let args_vec: Vec<String> = args
        .into_iter()
        .map(|value| value.as_ref().to_string())
        .collect();
    let mut command = Command::new(agpipe_bin());
    command
        .current_dir(repo_root())
        .env("AGPIPE_CODEX_BIN", codex_bin)
        .args(&args_vec);
    for (key, value) in extra_env {
        command.env(key, value);
    }
    let output = command.output().expect("run agpipe");
    if !output.status.success() {
        panic!(
            "agpipe {:?} failed with status {:?}\nstdout:\n{}\nstderr:\n{}",
            args_vec,
            output.status.code(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    output
}

fn stdout_text(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).to_string()
}

fn line_count(path: &Path) -> usize {
    if !path.exists() {
        return 0;
    }
    fs::read_to_string(path)
        .expect("read line-count file")
        .lines()
        .count()
}

fn token_sum(path: &Path) -> usize {
    if !path.exists() {
        return 0;
    }
    fs::read_to_string(path)
        .expect("read token file")
        .lines()
        .filter_map(|line| line.trim().parse::<usize>().ok())
        .sum()
}

#[test]
fn agpipe_cli_help_is_ui_first_and_points_to_internal_commands() {
    let disabled_codex = PathBuf::from("/usr/bin/false");

    let output = run_agpipe(["--help"], &disabled_codex);
    let text = stdout_text(&output);

    assert!(text.contains("UI-first multi-agent pipeline runtime"));
    assert!(text.contains("agpipe ui --root ~/agent-runs"));
    assert!(text.contains("agpipe internal --help"));
    assert!(!text.contains("agpipe interview-questions"));
    assert!(!text.contains("agpipe interview-finalize"));
    assert!(!text.contains("agpipe create-run"));
    assert!(!text.contains("agpipe runtime-check"));
}

#[test]
fn agpipe_cli_stage0_local_fallback_preserves_contract_and_create_run_handoff() {
    let workspace = temp_dir("cli-stage0-local-workspace");
    let output_root = temp_dir("cli-stage0-local-output");
    let cache_root = temp_dir("cli-stage0-local-cache");
    let disabled_codex = PathBuf::from("/usr/bin/false");
    let task = "Полностью проверить pipeline, сохранить реальные stage0 artifacts и не сужать цель до scaffold-only proof.";

    let questions_output = run_agpipe_with_env(
        [
            "interview-questions",
            "--task",
            task,
            "--workspace",
            workspace.to_str().unwrap(),
            "--output-dir",
            output_root.to_str().unwrap(),
            "--language",
            "ru",
            "--max-questions",
            "2",
        ],
        &disabled_codex,
        &[("AGPIPE_STAGE0_BACKEND", "local")],
    );
    let questions_json: Value =
        serde_json::from_str(stdout_text(&questions_output).trim()).expect("parse questions");
    let session_dir = questions_json
        .get("session_dir")
        .and_then(|value| value.as_str())
        .expect("session dir");
    let question_items = questions_json
        .get("questions")
        .and_then(|value| value.as_array())
        .cloned()
        .unwrap_or_default();
    let answers: Vec<Value> = question_items
        .iter()
        .map(|item| {
            json!({
                "id": item.get("id").and_then(|value| value.as_str()).unwrap_or("q"),
                "question": item.get("question").and_then(|value| value.as_str()).unwrap_or(""),
                "answer": "Сохранить target-workspace live proof path."
            })
        })
        .collect();
    let answers_path = PathBuf::from(session_dir).join("answers-ui.json");
    write_text(
        &answers_path,
        &serde_json::to_string_pretty(&answers).expect("encode answers"),
    );

    let finalize_output = run_agpipe_with_env(
        [
            "interview-finalize",
            "--task",
            task,
            "--workspace",
            workspace.to_str().unwrap(),
            "--session-dir",
            session_dir,
            "--answers-file",
            answers_path.to_str().unwrap(),
            "--language",
            "ru",
        ],
        &disabled_codex,
        &[("AGPIPE_STAGE0_BACKEND", "local")],
    );
    let finalized: InterviewFinalizePayload =
        serde_json::from_str(stdout_text(&finalize_output).trim()).expect("parse finalize");
    let final_task = fs::read_to_string(&finalized.final_task_path).expect("read final task");
    assert!(final_task.contains("не сужать цель"));

    let create_output = run_agpipe_with_env(
        [
            "create-run",
            "--task-file",
            &finalized.final_task_path,
            "--workspace",
            workspace.to_str().unwrap(),
            "--output-dir",
            output_root.to_str().unwrap(),
            "--prompt-format",
            "compact",
            "--summary-language",
            "ru",
            "--intake-research",
            "research-first",
            "--stage-research",
            "local-first",
            "--execution-network",
            "fetch-if-needed",
            "--cache-root",
            cache_root.to_str().unwrap(),
            "--cache-policy",
            "reuse",
            "--interview-session",
            &finalized.session_dir,
        ],
        &disabled_codex,
        &[("AGPIPE_STAGE0_BACKEND", "local")],
    );
    let run_dir = PathBuf::from(stdout_text(&create_output).trim());
    assert!(run_dir.exists());
    assert!(run_dir
        .join("interview")
        .join("logs")
        .join("interview-questions.fallback.json")
        .exists());
    assert!(run_dir
        .join("interview")
        .join("logs")
        .join("interview-finalize.fallback.json")
        .exists());
    assert!(run_dir.join("interview").join("final-task.md").exists());
    assert!(fs::read_to_string(
        run_dir
            .join("interview")
            .join("logs")
            .join("interview-questions.fallback.json")
    )
    .expect("read fallback metadata")
    .contains("\"effective_backend\": \"local\""));

    let _ = fs::remove_dir_all(run_dir);
    let _ = fs::remove_dir_all(output_root);
    let _ = fs::remove_dir_all(workspace);
    let _ = fs::remove_dir_all(cache_root);
}

#[test]
fn agpipe_cli_hello_world_python_completes_without_codex_and_creates_runnable_script() {
    let workspace = temp_dir("hello-world-workspace");
    let output_root = temp_dir("hello-world-output");
    let cache_root = temp_dir("hello-world-cache");
    let disabled_codex = PathBuf::from("/usr/bin/false");

    let created = run_agpipe(
        [
            "internal",
            "create-run",
            "--task",
            "hello world на python",
            "--workspace",
            &workspace.display().to_string(),
            "--output-dir",
            &output_root.display().to_string(),
            "--prompt-format",
            "compact",
            "--summary-language",
            "ru",
            "--intake-research",
            "research-first",
            "--stage-research",
            "local-first",
            "--execution-network",
            "fetch-if-needed",
            "--cache-root",
            &cache_root.display().to_string(),
            "--cache-policy",
            "reuse",
        ],
        &disabled_codex,
    );
    let run_dir = PathBuf::from(stdout_text(&created).trim());
    assert!(run_dir.exists());

    let started = Instant::now();
    let resumed = run_agpipe(
        [
            "internal",
            "resume",
            &run_dir.display().to_string(),
            "--until",
            "verification",
            "--auto-approve",
        ],
        &disabled_codex,
    );
    let elapsed = started.elapsed();
    let resume_stdout = stdout_text(&resumed);

    assert!(
        elapsed.as_secs() < 10,
        "hello-world local fast path took too long: {:?}",
        elapsed
    );
    assert!(
        resume_stdout.contains("Completed verification with exit code 0")
            || resume_stdout.contains("Pipeline is complete"),
        "unexpected resume output:\n{}",
        resume_stdout
    );

    let main_py = workspace.join("main.py");
    assert!(main_py.exists(), "expected {}", main_py.display());
    assert!(fs::read_to_string(&main_py)
        .expect("read main.py")
        .contains("print(\"Hello, world!\")"));

    let python_output = Command::new("python3")
        .arg("main.py")
        .current_dir(&workspace)
        .output()
        .expect("run python hello world");
    assert!(python_output.status.success());
    assert_eq!(
        String::from_utf8_lossy(&python_output.stdout).trim(),
        "Hello, world!"
    );

    let verification_goal =
        fs::read_to_string(run_dir.join("verification").join("goal-status.json"))
            .expect("read goal status");
    assert!(verification_goal.contains("\"goal_complete\": true"));
}

#[test]
fn agpipe_cli_service_check_runs_a_real_local_service_with_multiple_inputs() {
    let workspace = temp_dir("service-check-workspace");
    let output_root = temp_dir("service-check-output");
    let disabled_codex = PathBuf::from("/usr/bin/false");

    let created = run_agpipe(
        [
            "internal",
            "create-run",
            "--task",
            "Поднять локальный HTTP сервис и прогнать несколько сценариев с разными входными данными.",
            "--workspace",
            &workspace.display().to_string(),
            "--output-dir",
            &output_root.display().to_string(),
            "--prompt-format",
            "compact",
        ],
        &disabled_codex,
    );
    let run_dir = PathBuf::from(stdout_text(&created).trim());
    assert!(run_dir.exists(), "expected {}", run_dir.display());

    write_text(
        &workspace.join("server.py"),
        r#"import json
import os
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse

PORT = int(os.environ.get("PORT", "18081"))


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/health":
            body = b"ok"
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if parsed.path == "/echo":
            value = parse_qs(parsed.query).get("value", [""])[0]
            body = json.dumps({"value": value}).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        self.send_error(404)

    def log_message(self, fmt, *args):
        return


HTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
"#,
    );

    let port = "18081";
    let health_command = "python3 -c \"import os, urllib.request; port=os.environ['PORT']; print(urllib.request.urlopen('http://127.0.0.1:%s/health' % port).read().decode())\"";
    let echo_alpha = "python3 -c \"import json, os, urllib.request; port=os.environ['PORT']; data=json.load(urllib.request.urlopen('http://127.0.0.1:%s/echo?value=alpha' % port)); print(data['value'])\"";
    let echo_beta = "python3 -c \"import json, os, urllib.request; port=os.environ['PORT']; data=json.load(urllib.request.urlopen('http://127.0.0.1:%s/echo?value=beta' % port)); print(data['value'])\"";
    let service_spec = json!({
        "version": 1,
        "mode": "process",
        "workdir": ".",
        "env": {
            "PORT": port,
        },
        "start_command": "python3 server.py",
        "ready_command": health_command,
        "ready_timeout_secs": 10,
        "ready_interval_ms": 200,
        "scenarios": [
            {
                "id": "health",
                "command": health_command,
                "expect_exit_code": 0,
                "expect_stdout_contains": ["ok"],
            },
            {
                "id": "echo-alpha",
                "command": echo_alpha,
                "expect_exit_code": 0,
                "expect_stdout_contains": ["alpha"],
            },
            {
                "id": "echo-beta",
                "command": echo_beta,
                "expect_exit_code": 0,
                "expect_stdout_contains": ["beta"],
            }
        ]
    });
    write_text(
        &workspace.join(".agpipe").join("runtime-check.json"),
        &format!(
            "{}\n",
            serde_json::to_string_pretty(&service_spec).expect("serialize service spec")
        ),
    );

    let check = run_agpipe(
        [
            "internal",
            "runtime-check",
            &run_dir.display().to_string(),
            "--phase",
            "verification",
        ],
        &disabled_codex,
    );
    let check_stdout = stdout_text(&check);
    assert!(
        check_stdout.contains("Runtime check passed"),
        "unexpected runtime-check output:\n{}",
        check_stdout
    );

    let summary_path = run_dir
        .join("runtime")
        .join("runtime-check")
        .join("verification")
        .join("summary.json");
    let summary: Value = serde_json::from_str(
        &fs::read_to_string(&summary_path).expect("read runtime-check summary"),
    )
    .expect("parse runtime-check summary");
    assert_eq!(summary["status"], "passed");
    assert_eq!(summary["ready_status"], "passed");
    assert_eq!(
        summary["scenarios"]
            .as_array()
            .expect("scenarios array")
            .len(),
        3
    );

    let summary_md = fs::read_to_string(
        run_dir
            .join("runtime")
            .join("runtime-check")
            .join("verification")
            .join("summary.md"),
    )
    .expect("read runtime-check summary md");
    assert!(summary_md.contains("echo-alpha"));
    assert!(summary_md.contains("echo-beta"));
}

#[test]
fn agpipe_cli_runtime_check_can_drive_a_tui_over_pty() {
    let workspace = temp_dir("runtime-check-tui-workspace");
    let output_root = temp_dir("runtime-check-tui-output");
    let disabled_codex = PathBuf::from("/usr/bin/false");

    let created = run_agpipe(
        [
            "internal",
            "create-run",
            "--task",
            "Собрать терминальный интерфейс и прогнать его end-to-end через PTY.",
            "--workspace",
            &workspace.display().to_string(),
            "--output-dir",
            &output_root.display().to_string(),
            "--prompt-format",
            "compact",
        ],
        &disabled_codex,
    );
    let run_dir = PathBuf::from(stdout_text(&created).trim());
    assert!(run_dir.exists(), "expected {}", run_dir.display());

    write_text(
        &workspace.join("menu.py"),
        r#"import sys
import termios
import tty

fd = sys.stdin.fileno()
old = termios.tcgetattr(fd)
try:
    tty.setraw(fd)
    sys.stdout.write("Main Menu\r\nPress 1 to select, q to quit\r\n")
    sys.stdout.flush()
    while True:
        ch = sys.stdin.read(1)
        if ch == "1":
            sys.stdout.write("\r\nSelected: 1\r\n")
            sys.stdout.flush()
        elif ch.lower() == "q":
            sys.stdout.write("\r\nBye\r\n")
            sys.stdout.flush()
            break
finally:
    termios.tcsetattr(fd, termios.TCSADRAIN, old)
"#,
    );

    let runtime_spec = json!({
        "version": 1,
        "mode": "workflow",
        "workdir": ".",
        "scenarios": [
            {
                "id": "tui-menu",
                "kind": "pty",
                "command": "python3 menu.py",
                "rows": 30,
                "cols": 100,
                "expect_exit_code": 0,
                "steps": [
                    {
                        "kind": "pty_wait_contains",
                        "pattern": "Main Menu",
                        "timeout_secs": 3
                    },
                    {
                        "kind": "pty_send_text",
                        "text": "1"
                    },
                    {
                        "kind": "pty_wait_contains",
                        "pattern": "Selected: 1",
                        "timeout_secs": 3
                    },
                    {
                        "kind": "pty_send_text",
                        "text": "q"
                    },
                    {
                        "kind": "pty_wait_contains",
                        "pattern": "Bye",
                        "timeout_secs": 3
                    }
                ]
            }
        ]
    });
    write_text(
        &workspace.join(".agpipe").join("runtime-check.json"),
        &format!(
            "{}\n",
            serde_json::to_string_pretty(&runtime_spec).expect("serialize runtime spec")
        ),
    );

    let check = run_agpipe(
        [
            "internal",
            "runtime-check",
            &run_dir.display().to_string(),
            "--phase",
            "verification",
        ],
        &disabled_codex,
    );
    let check_stdout = stdout_text(&check);
    assert!(
        check_stdout.contains("Runtime check passed"),
        "unexpected runtime-check output:\n{}",
        check_stdout
    );

    let summary_path = run_dir
        .join("runtime")
        .join("runtime-check")
        .join("verification")
        .join("summary.json");
    let summary: Value =
        serde_json::from_str(&fs::read_to_string(&summary_path).expect("read runtime summary"))
            .expect("parse runtime summary");
    assert_eq!(summary["status"], "passed");
    assert_eq!(summary["scenarios"][0]["status"], "passed");

    let screen_path = run_dir
        .join("runtime")
        .join("runtime-check")
        .join("verification")
        .join("scenarios")
        .join("tui-menu")
        .join("screen.txt");
    let screen = fs::read_to_string(&screen_path).expect("read pty screen");
    assert!(screen.contains("Selected: 1"));
    assert!(screen.contains("Bye"));
}

fn mock_codex_script(name: &str) -> (PathBuf, PathBuf, PathBuf, PathBuf) {
    let root = temp_dir(&format!("mock-codex-{name}"));
    let bin_path = root.join("mock-codex.zsh");
    let invocations_path = root.join("invocations.log");
    let tokens_path = root.join("tokens.log");
    let script = r#"#!/bin/zsh
set -euo pipefail

script_dir="${0:A:h}"
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
if [[ -z "$last_message" ]]; then
  echo "missing --output-last-message" >&2
  exit 2
fi

print -r -- "${last_message:t}" >> "$script_dir/invocations.log"
print -r -- "111" >> "$script_dir/tokens.log"

label="${${last_message:t}%.last.md}"
root="${last_message:h:h}"
mkdir -p "$root"

case "$label" in
  interview-questions)
    cat > "$last_message" <<'JSON'
{
  "goal_summary": "Mock service migration",
  "questions": [
    {
      "id": "target_scope",
      "question": "Нужно ли прогнать полный pipeline до verification?",
      "why": "Чтобы тест зафиксировал end-to-end path и cache reuse.",
      "required": true
    }
  ]
}
JSON
    print "mock interview-questions"
    ;;
  interview-finalize)
    cat > "$last_message" <<'MD'
# Final Task

Собрать тестовый Rust pipeline end-to-end.

- Пройти intake, solvers, review, execution и verification.
- Сохранить артефакты stage0.
- Не ломать cache reuse.
- Считать успехом полный run до `goal_complete=true`.
MD
    print "mock interview-finalize"
    ;;
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

Mock execution created the test service successfully.
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
Unhandled label: $label
EOF
    ;;
esac

print "mock $label complete"
"#;
    write_text(&bin_path, script);
    let mut permissions = fs::metadata(&bin_path)
        .expect("stat mock codex")
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&bin_path, permissions).expect("chmod mock codex");
    (root, bin_path, invocations_path, tokens_path)
}

#[test]
fn agpipe_cli_stage0_create_run_and_resume_complete_the_mock_pipeline() {
    let workspace = temp_dir("cli-workspace");
    let output_root = temp_dir("cli-output");
    let cache_root = temp_dir("cli-cache");
    let (mock_root, codex_bin, invocations_path, tokens_path) = mock_codex_script("full");

    let questions_output = run_agpipe(
        [
            "interview-questions",
            "--task",
            "Нужно собрать тестовый pipeline и проверить cache reuse.",
            "--workspace",
            workspace.to_str().unwrap(),
            "--output-dir",
            output_root.to_str().unwrap(),
            "--language",
            "ru",
            "--max-questions",
            "4",
        ],
        &codex_bin,
    );
    let questions: InterviewQuestionsPayload =
        serde_json::from_str(stdout_text(&questions_output).trim()).expect("parse questions");
    assert_eq!(questions.questions.len(), 1);

    let answers_path = PathBuf::from(&questions.session_dir).join("answers-ui.json");
    write_text(
        &answers_path,
        &serde_json::to_string_pretty(&json!([
            {
                "id": questions.questions[0].id,
                "question": questions.questions[0].question,
                "answer": "Да, до verification и с включённым cache."
            }
        ]))
        .expect("encode answers"),
    );

    let finalize_output = run_agpipe(
        [
            "interview-finalize",
            "--task",
            "Нужно собрать тестовый pipeline и проверить cache reuse.",
            "--workspace",
            workspace.to_str().unwrap(),
            "--session-dir",
            &questions.session_dir,
            "--answers-file",
            answers_path.to_str().unwrap(),
            "--language",
            "ru",
        ],
        &codex_bin,
    );
    let finalized: InterviewFinalizePayload =
        serde_json::from_str(stdout_text(&finalize_output).trim()).expect("parse finalize");
    assert!(PathBuf::from(&finalized.final_task_path).exists());

    let create_output = run_agpipe(
        [
            "create-run",
            "--task-file",
            &finalized.final_task_path,
            "--workspace",
            workspace.to_str().unwrap(),
            "--output-dir",
            output_root.to_str().unwrap(),
            "--prompt-format",
            "compact",
            "--summary-language",
            "ru",
            "--intake-research",
            "research-first",
            "--stage-research",
            "local-first",
            "--execution-network",
            "fetch-if-needed",
            "--cache-root",
            cache_root.to_str().unwrap(),
            "--cache-policy",
            "reuse",
            "--interview-session",
            &finalized.session_dir,
        ],
        &codex_bin,
    );
    let run_dir = PathBuf::from(stdout_text(&create_output).trim());
    assert!(run_dir.exists());
    assert!(run_dir.join("interview").join("final-task.md").exists());
    assert!(run_dir
        .join("interview")
        .join("logs")
        .join("interview-questions.prompt.md")
        .exists());
    assert!(run_dir
        .join("interview")
        .join("logs")
        .join("interview-finalize.last.md")
        .exists());

    let resume_output = run_agpipe(
        [
            "resume",
            run_dir.to_str().unwrap(),
            "--until",
            "verification",
            "--auto-approve",
        ],
        &codex_bin,
    );
    let resume_stdout = stdout_text(&resume_output);
    assert!(resume_stdout.contains("Completed verification with exit code 0"));

    let status_output = run_agpipe(["status", run_dir.to_str().unwrap()], &codex_bin);
    let status_text = stdout_text(&status_output);
    assert!(status_text.contains("goal: complete"));
    assert!(status_text.contains("next: none"));
    assert!(
        fs::read_to_string(run_dir.join("verification").join("findings.md"))
            .expect("read findings")
            .contains("Mock verification complete")
    );

    let plan: Value =
        serde_json::from_str(&fs::read_to_string(run_dir.join("plan.json")).expect("read plan"))
            .expect("parse plan");
    let has_execution_stage = plan
        .get("pipeline")
        .and_then(|value| value.get("stages"))
        .and_then(|value| value.as_array())
        .map(|stages| {
            stages.iter().any(|stage| {
                stage
                    .get("kind")
                    .and_then(|value| value.as_str())
                    .map(|kind| kind == "execution")
                    .unwrap_or(false)
            })
        })
        .unwrap_or(false);
    if has_execution_stage {
        assert!(
            fs::read_to_string(run_dir.join("execution").join("report.md"))
                .expect("read execution report")
                .contains("test service successfully")
        );
    }
    let expected_calls = 2usize + plan["pipeline"]["stages"].as_array().map(|v| v.len()).unwrap_or(0);
    assert_eq!(line_count(&invocations_path), expected_calls);
    assert_eq!(token_sum(&tokens_path), expected_calls * 111);

    let _ = fs::remove_dir_all(run_dir);
    let _ = fs::remove_dir_all(output_root);
    let _ = fs::remove_dir_all(workspace);
    let _ = fs::remove_dir_all(cache_root);
    let _ = fs::remove_dir_all(mock_root);
}

#[test]
fn agpipe_cli_cache_reuse_prevents_a_second_verification_backend_call() {
    let workspace = temp_dir("cli-cache-workspace");
    let output_root = temp_dir("cli-cache-output");
    let cache_root = temp_dir("cli-cache-root");
    let task_file = temp_dir("cli-cache-task").join("task.md");
    let (mock_root, codex_bin, invocations_path, tokens_path) = mock_codex_script("reuse");
    write_text(
        &task_file,
        "# Task\n\nПроверить cache reuse на verification stage.\n",
    );

    let create_output = run_agpipe(
        [
            "create-run",
            "--task-file",
            task_file.to_str().unwrap(),
            "--workspace",
            workspace.to_str().unwrap(),
            "--output-dir",
            output_root.to_str().unwrap(),
            "--prompt-format",
            "compact",
            "--summary-language",
            "ru",
            "--intake-research",
            "research-first",
            "--stage-research",
            "local-first",
            "--execution-network",
            "fetch-if-needed",
            "--cache-root",
            cache_root.to_str().unwrap(),
            "--cache-policy",
            "reuse",
        ],
        &codex_bin,
    );
    let run_dir = PathBuf::from(stdout_text(&create_output).trim());

    let first_resume = run_agpipe(
        [
            "resume",
            run_dir.to_str().unwrap(),
            "--until",
            "verification",
            "--auto-approve",
        ],
        &codex_bin,
    );
    assert!(stdout_text(&first_resume).contains("Completed verification with exit code 0"));
    let first_calls = line_count(&invocations_path);
    let first_tokens = token_sum(&tokens_path);
    assert!(first_calls > 0);

    let _ = run_agpipe(
        ["step-back", run_dir.to_str().unwrap(), "verification"],
        &codex_bin,
    );
    let second_verification = run_agpipe(
        ["start", run_dir.to_str().unwrap(), "verification"],
        &codex_bin,
    );
    let second_stdout = stdout_text(&second_verification);
    assert!(second_stdout.contains("Reused cached verification result."));
    assert_eq!(line_count(&invocations_path), first_calls);
    assert_eq!(token_sum(&tokens_path), first_tokens);

    let cache_status = run_agpipe(
        ["cache-status", run_dir.to_str().unwrap(), "--refresh"],
        &codex_bin,
    );
    assert!(stdout_text(&cache_status).contains("stage-results"));

    let _ = fs::remove_dir_all(run_dir);
    let _ = fs::remove_dir_all(output_root);
    let _ = fs::remove_dir_all(workspace);
    let _ = fs::remove_dir_all(cache_root);
    let _ = fs::remove_dir_all(mock_root);
}
