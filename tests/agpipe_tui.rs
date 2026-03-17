use portable_pty::{native_pty_system, CommandBuilder, PtySize};
use std::fs;
use std::io::{Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::mpsc::{self, Receiver};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn agpipe_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_agpipe"))
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn temp_dir(name: &str) -> PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let path = std::env::temp_dir().join(format!("agpipe-tui-test-{name}-{unique}"));
    fs::create_dir_all(&path).expect("create temp dir");
    path
}

fn write_text(path: &Path, text: &str) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("create parent");
    }
    fs::write(path, text).expect("write text");
}

fn mock_codex_script(name: &str) -> (PathBuf, PathBuf) {
    let root = temp_dir(&format!("mock-codex-{name}"));
    let bin_path = root.join("mock-codex.zsh");
    let script = r#"#!/bin/zsh
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
  interview-questions)
    cat > "$last_message" <<'JSON'
{
  "goal_summary": "Mock TUI pipeline",
  "questions": [
    {
      "id": "full_path",
      "question": "Пройти весь pipeline до verification?",
      "why": "Нужно проверить все стадии прямо через TUI.",
      "required": true
    }
  ]
}
JSON
    ;;
  interview-finalize)
    cat > "$root/final-task.md" <<'EOF'
# Final Task

Пройти intake, solver, review, execution и verification в TUI.
EOF
    cat > "$last_message" <<JSON
{
  "final_task_path": "$root/final-task.md"
}
JSON
    ;;
  intake)
    sleep 2
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
"#;
    write_text(&bin_path, script);
    let mut permissions = fs::metadata(&bin_path)
        .expect("stat mock codex")
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&bin_path, permissions).expect("chmod mock codex");
    (root, bin_path)
}

fn run_agpipe<I, S>(args: I, codex_bin: &Path) -> Output
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let args_vec: Vec<String> = args
        .into_iter()
        .map(|value| value.as_ref().to_string())
        .collect();
    let output = Command::new(agpipe_bin())
        .current_dir(repo_root())
        .env("AGPIPE_CODEX_BIN", codex_bin)
        .args(&args_vec)
        .output()
        .expect("run agpipe");
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

struct PtyApp {
    child: Box<dyn portable_pty::Child + Send + Sync>,
    writer: Box<dyn Write + Send>,
    rx: Receiver<Vec<u8>>,
    parser: vt100::Parser,
    _reader_handle: thread::JoinHandle<()>,
}

impl PtyApp {
    fn start(root: &Path, codex_bin: &Path) -> Self {
        let pty_system = native_pty_system();
        let pair = pty_system
            .openpty(PtySize {
                rows: 40,
                cols: 120,
                pixel_width: 0,
                pixel_height: 0,
            })
            .expect("open pty");
        let mut cmd = CommandBuilder::new(agpipe_bin());
        cmd.arg("ui");
        cmd.arg("--root");
        cmd.arg(root);
        cmd.cwd(repo_root());
        cmd.env("AGPIPE_CODEX_BIN", codex_bin);
        let child = pair.slave.spawn_command(cmd).expect("spawn agpipe tui");
        drop(pair.slave);
        let reader = pair.master.try_clone_reader().expect("clone reader");
        let writer = pair.master.take_writer().expect("take writer");
        let (rx, handle) = spawn_reader(reader);
        Self {
            child,
            writer,
            rx,
            parser: vt100::Parser::new(40, 120, 0),
            _reader_handle: handle,
        }
    }

    fn send_key(&mut self, key: &[u8]) {
        self.writer.write_all(key).expect("send key");
        self.writer.flush().expect("flush key");
    }

    fn send_text(&mut self, text: &str) {
        for ch in text.bytes() {
            self.send_key(&[ch]);
            thread::sleep(Duration::from_millis(20));
        }
    }

    fn drain(&mut self) {
        while let Ok(bytes) = self.rx.try_recv() {
            self.parser.process(&bytes);
        }
    }

    fn screen(&mut self) -> String {
        self.drain();
        self.parser.screen().contents()
    }

    fn wait_contains(&mut self, needle: &str, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        loop {
            let screen = self.screen();
            if screen.contains(needle) {
                return;
            }
            if Instant::now() >= deadline {
                panic!("Timed out waiting for `{needle}`.\nCurrent screen:\n{screen}");
            }
            match self.rx.recv_timeout(Duration::from_millis(100)) {
                Ok(bytes) => self.parser.process(&bytes),
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    panic!("PTY disconnected while waiting for `{needle}`");
                }
            }
        }
    }

    fn wait_until<F>(&mut self, timeout: Duration, predicate: F)
    where
        F: Fn(&str) -> bool,
    {
        let deadline = Instant::now() + timeout;
        loop {
            let screen = self.screen();
            if predicate(&screen) {
                return;
            }
            if Instant::now() >= deadline {
                panic!("Timed out waiting for custom condition.\nCurrent screen:\n{screen}");
            }
            match self.rx.recv_timeout(Duration::from_millis(100)) {
                Ok(bytes) => self.parser.process(&bytes),
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    panic!("PTY disconnected while waiting for custom condition");
                }
            }
        }
    }

    fn stop(mut self) {
        let _ = self.writer.write_all(b"q");
        let _ = self.writer.flush();
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            match self.child.try_wait() {
                Ok(Some(_)) => return,
                Ok(None) if Instant::now() < deadline => thread::sleep(Duration::from_millis(50)),
                _ => {
                    let _ = self.child.kill();
                    let _ = self.child.wait();
                    return;
                }
            }
        }
    }
}

fn spawn_reader(mut reader: Box<dyn Read + Send>) -> (Receiver<Vec<u8>>, thread::JoinHandle<()>) {
    let (tx, rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        let mut buf = [0u8; 4096];
        loop {
            match reader.read(&mut buf) {
                Ok(0) => break,
                Ok(size) => {
                    if tx.send(buf[..size].to_vec()).is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    });
    (rx, handle)
}

#[test]
fn agpipe_tui_running_mode_allows_artifact_navigation_and_next_stage_progress() {
    let workspace = temp_dir("tui-workspace");
    let output_root = temp_dir("tui-output");
    let cache_root = temp_dir("tui-cache");
    let task_file = temp_dir("tui-task").join("task.md");
    let (mock_root, codex_bin) = mock_codex_script("tui");
    write_text(&task_file, "# Task\n\nSmoke test the TUI.\n");

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
    let run_name = run_dir
        .file_name()
        .and_then(|value| value.to_str())
        .expect("run name")
        .to_string();

    let mut app = PtyApp::start(&output_root, &codex_bin);
    app.wait_contains("verify=pending", Duration::from_secs(3));
    app.wait_contains(&run_name, Duration::from_secs(3));
    app.wait_contains("next=intake", Duration::from_secs(3));

    app.send_key(b"n");
    app.send_key(b"1");
    app.wait_contains("Summary", Duration::from_secs(2));
    app.wait_until(Duration::from_secs(2), |screen| {
        screen.contains("Current pipeline state:") || screen.contains("Smoke test the TUI.")
    });
    app.send_key(&[27]);
    app.wait_contains("next=solver-a", Duration::from_secs(12));

    let screen = app.screen();
    assert!(screen.contains(&run_name));
    assert!(screen.contains("next=solver-a"));
    assert!(
        screen.contains("Running: solver-a")
            || screen.contains("running=solver-a")
            || screen.contains("Stage `solver-a` is currently running.")
            || screen.contains("Running: intake")
            || screen.contains("running=intake")
            || screen.contains("solver-a: pending")
            || screen.contains("Mock intake completed.")
            || screen.contains("Mock intake complete.")
            || screen.contains("Completed intake with exit code 0")
    );
    assert!(
        screen.contains("Mock intake completed.")
            || screen.contains("Mock intake complete.")
            || screen.contains("intake: done")
            || screen.contains("Completed intake with exit code 0")
    );

    app.stop();

    let _ = fs::remove_dir_all(run_dir);
    let _ = fs::remove_dir_all(output_root);
    let _ = fs::remove_dir_all(workspace);
    let _ = fs::remove_dir_all(cache_root);
    let _ = fs::remove_dir_all(mock_root);
}

#[test]
fn agpipe_tui_wizard_can_create_and_complete_a_pipeline() {
    let output_root = temp_dir("wizard-output");
    let workspace = PathBuf::from("/tmp/agpipe-tui-wizard-workspace");
    let _ = fs::remove_dir_all(&workspace);
    fs::create_dir_all(&workspace).expect("create wizard workspace");
    let (mock_root, codex_bin) = mock_codex_script("wizard");

    let mut app = PtyApp::start(&output_root, &codex_bin);
    app.wait_contains("No runs found.", Duration::from_secs(3));
    app.send_key(b"c");
    app.wait_contains("New Pipeline", Duration::from_secs(2));
    app.send_text("TUI wizard full pipeline");
    thread::sleep(Duration::from_millis(250));
    app.send_key(b"\t");
    thread::sleep(Duration::from_millis(100));
    app.send_text(workspace.to_str().unwrap());
    thread::sleep(Duration::from_millis(400));
    app.send_key(b"\t");
    thread::sleep(Duration::from_millis(100));
    app.send_text("wizard-run");
    thread::sleep(Duration::from_millis(250));
    app.send_key(b"\t");
    thread::sleep(Duration::from_millis(100));
    app.send_key(b"\r");

    app.wait_contains("Interview", Duration::from_secs(5));
    app.wait_contains("Answer", Duration::from_secs(5));
    app.send_text("Да, до verification.");
    app.send_key(b"\r");

    app.wait_contains("Final Task Prompt", Duration::from_secs(5));
    app.wait_contains("Create + Run All", Duration::from_secs(5));
    app.send_key(b"\t");
    app.send_key(b"\r");

    app.wait_until(Duration::from_secs(20), |screen| {
        screen.contains("goal=complete") || screen.contains("next=none")
    });
    app.send_key(b"1");
    app.wait_until(Duration::from_secs(5), |screen| {
        screen.contains("Mock verification summary.") || screen.contains("# Verification Summary")
    });

    app.stop();

    let _ = fs::remove_dir_all(output_root);
    let _ = fs::remove_dir_all(workspace);
    let _ = fs::remove_dir_all(mock_root);
}
