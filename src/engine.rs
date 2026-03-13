use serde::de::DeserializeOwned;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub struct Context {
    pub repo_root: PathBuf,
    pub python: String,
}

impl Context {
    pub fn discover() -> Result<Self, String> {
        let repo_root = if let Ok(path) = env::var("AGPIPE_REPO_ROOT") {
            PathBuf::from(path)
        } else if let Ok(path) = env::var("MAP_REPO_ROOT") {
            PathBuf::from(path)
        } else {
            discover_repo_root()?
        };
        Ok(Self {
            repo_root,
            python: env::var("AGPIPE_PYTHON")
                .or_else(|_| env::var("MAP_PYTHON"))
                .unwrap_or_else(|_| "python3".to_string()),
        })
    }

    pub fn run_stage_script(&self) -> PathBuf {
        self.repo_root.join("scripts").join("run_stage.py")
    }

    pub fn map_py_script(&self) -> PathBuf {
        self.repo_root.join("scripts").join("map.py")
    }
}

fn discover_repo_root() -> Result<PathBuf, String> {
    let exe = env::current_exe().map_err(|err| format!("Could not resolve current executable: {err}"))?;
    for ancestor in exe.ancestors() {
        if ancestor.join("scripts").join("run_stage.py").exists() {
            return Ok(ancestor.to_path_buf());
        }
    }
    let cwd = env::current_dir().map_err(|err| format!("Could not read current directory: {err}"))?;
    for ancestor in cwd.ancestors() {
        if ancestor.join("scripts").join("run_stage.py").exists() {
            return Ok(ancestor.to_path_buf());
        }
    }
    Err("Could not locate multi-agent-pipeline repo root. Set AGPIPE_REPO_ROOT.".to_string())
}

#[derive(Debug, Clone, Default)]
pub struct CommandResult {
    pub code: i32,
    pub stdout: String,
    pub stderr: String,
}

impl CommandResult {
    pub fn ok(message: impl Into<String>) -> Self {
        Self {
            code: 0,
            stdout: message.into(),
            stderr: String::new(),
        }
    }

    pub fn combined_output(&self) -> String {
        match (self.stdout.trim(), self.stderr.trim()) {
            ("", "") => String::new(),
            (stdout, "") => stdout.to_string(),
            ("", stderr) => stderr.to_string(),
            (stdout, stderr) => format!("{stdout}\n\n{stderr}"),
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize, Default)]
pub struct DoctorIssue {
    pub severity: String,
    pub message: String,
    pub fix: String,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize, Default)]
pub struct DoctorPayload {
    pub run_dir: String,
    pub health: String,
    pub stages: BTreeMap<String, String>,
    pub stale: Vec<String>,
    pub host_probe: String,
    pub host_drift: Option<String>,
    pub goal: String,
    pub next: String,
    pub safe_next_action: String,
    pub issues: Vec<DoctorIssue>,
    pub warnings: Vec<DoctorIssue>,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize, Default)]
pub struct StatusPayload {
    pub run_dir: String,
    pub stages: BTreeMap<String, String>,
    pub host_probe: String,
    pub host_drift: Option<String>,
    pub goal: String,
    pub next: String,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct RunSnapshot {
    pub run_dir: PathBuf,
    pub doctor: DoctorPayload,
    pub status: StatusPayload,
    pub preview_label: String,
    pub preview: String,
    pub log_title: String,
    pub log_lines: Vec<String>,
}

pub fn default_run_root() -> PathBuf {
    if let Ok(home) = env::var("HOME") {
        return PathBuf::from(home).join("agent-runs");
    }
    PathBuf::from("agent-runs")
}

pub fn discover_run_dirs(root: &Path) -> Result<Vec<PathBuf>, String> {
    if !root.exists() {
        return Ok(Vec::new());
    }
    let mut runs: Vec<PathBuf> = fs::read_dir(root)
        .map_err(|err| format!("Could not read {}: {err}", root.display()))?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.is_dir() && path.join("plan.json").exists())
        .collect();
    runs.sort();
    runs.reverse();
    Ok(runs)
}

pub fn load_run_snapshots(ctx: &Context, root: &Path, limit: usize) -> Result<Vec<RunSnapshot>, String> {
    let runs = discover_run_dirs(root)?;
    let mut snapshots = Vec::new();
    for run_dir in runs.into_iter().take(limit) {
        match load_run_snapshot(ctx, &run_dir) {
            Ok(snapshot) => snapshots.push(snapshot),
            Err(err) => {
                let doctor = DoctorPayload {
                    run_dir: run_dir.display().to_string(),
                    health: "broken".to_string(),
                    goal: "n/a".to_string(),
                    next: "none".to_string(),
                    safe_next_action: "none".to_string(),
                    warnings: vec![DoctorIssue {
                        severity: "warn".to_string(),
                        message: err.clone(),
                        fix: "Inspect the run directory and Python engine.".to_string(),
                    }],
                    ..DoctorPayload::default()
                };
                let status = StatusPayload {
                    run_dir: run_dir.display().to_string(),
                    host_probe: "unknown".to_string(),
                    goal: "n/a".to_string(),
                    next: "none".to_string(),
                    ..StatusPayload::default()
                };
                let (preview_label, preview) = preview_text(&run_dir, 2400);
                let (log_title, log_lines) = latest_log_excerpt(&run_dir, 12);
                snapshots.push(RunSnapshot {
                    run_dir,
                    doctor,
                    status,
                    preview_label,
                    preview,
                    log_title,
                    log_lines,
                });
            }
        }
    }
    Ok(snapshots)
}

pub fn load_run_snapshot(ctx: &Context, run_dir: &Path) -> Result<RunSnapshot, String> {
    let doctor = doctor_report(ctx, run_dir)?;
    let status = status_report(ctx, run_dir)?;
    let (preview_label, preview) = preview_text(run_dir, 2400);
    let (log_title, log_lines) = latest_log_excerpt(run_dir, 12);
    Ok(RunSnapshot {
        run_dir: run_dir.to_path_buf(),
        doctor,
        status,
        preview_label,
        preview,
        log_title,
        log_lines,
    })
}

pub fn doctor_report(ctx: &Context, run_dir: &Path) -> Result<DoctorPayload, String> {
    run_stage_json(ctx, run_dir, "doctor", &["--json"])
}

pub fn status_report(ctx: &Context, run_dir: &Path) -> Result<StatusPayload, String> {
    run_stage_json(ctx, run_dir, "status", &["--json"])
}

pub fn next_stage(ctx: &Context, run_dir: &Path) -> Result<String, String> {
    let result = run_stage_capture(ctx, run_dir, "next", &[])?;
    if result.code != 0 {
        return Err(nonempty_output(&result).unwrap_or_else(|| "next failed".to_string()));
    }
    Ok(result.stdout.trim().to_string())
}

pub fn run_stage_capture(
    ctx: &Context,
    run_dir: &Path,
    subcommand: &str,
    extra: &[&str],
) -> Result<CommandResult, String> {
    let mut command = Command::new(&ctx.python);
    command.arg(ctx.run_stage_script()).arg(run_dir).arg(subcommand);
    for item in extra {
        command.arg(item);
    }
    let output = command.output().map_err(|err| format!("Failed to run command: {err}"))?;
    Ok(CommandResult {
        code: output.status.code().unwrap_or(1),
        stdout: String::from_utf8_lossy(&output.stdout).to_string(),
        stderr: String::from_utf8_lossy(&output.stderr).to_string(),
    })
}

pub fn run_stage_stream(
    ctx: &Context,
    run_dir: &Path,
    subcommand: &str,
    extra: &[&str],
) -> Result<i32, String> {
    let mut command = Command::new(&ctx.python);
    command.arg(ctx.run_stage_script()).arg(run_dir).arg(subcommand);
    for item in extra {
        command.arg(item);
    }
    let status = command.status().map_err(|err| format!("Failed to run command: {err}"))?;
    Ok(status.code().unwrap_or(1))
}

pub fn map_py_stream(ctx: &Context, subcommand: &str, extra: &[String]) -> Result<i32, String> {
    let mut command = Command::new(&ctx.python);
    command.arg(ctx.map_py_script()).arg(subcommand);
    for item in extra {
        command.arg(item);
    }
    let status = command.status().map_err(|err| format!("Failed to run command: {err}"))?;
    Ok(status.code().unwrap_or(1))
}

pub fn automate_run(
    ctx: &Context,
    run_dir: &Path,
    until: &str,
    auto_approve: bool,
) -> Result<CommandResult, String> {
    let mut combined = String::new();
    loop {
        let next = next_stage(ctx, run_dir)?;
        let bucket = next_stage_bucket(&next);
        if bucket == "none" {
            combined.push_str("Pipeline is complete for this run.\n");
            break;
        }
        if bucket == "rerun" {
            combined.push_str("Verification recommends a follow-up rerun.\n");
            break;
        }
        if stage_rank(&bucket) > stage_rank(until) {
            break;
        }
        if bucket == "execution" && !auto_approve {
            combined.push_str("Paused before execution.\n");
            break;
        }
        let result = if bucket == "solvers" {
            run_stage_capture(ctx, run_dir, "start-solvers", &[])?
        } else {
            run_stage_capture(ctx, run_dir, "start", &[bucket.as_str()])?
        };
        if !result.stdout.trim().is_empty() {
            combined.push_str(result.stdout.trim_end());
            combined.push('\n');
        }
        if !result.stderr.trim().is_empty() {
            combined.push_str(result.stderr.trim_end());
            combined.push('\n');
        }
        if result.code != 0 {
            return Ok(CommandResult {
                code: result.code,
                stdout: combined,
                stderr: String::new(),
            });
        }
        let next_after = next_stage_bucket(&next_stage(ctx, run_dir)?);
        if next_after == bucket {
            return Err(format!("No stage progress detected after `{bucket}`. Check status and logs."));
        }
    }
    Ok(CommandResult {
        code: 0,
        stdout: combined,
        stderr: String::new(),
    })
}

pub fn execute_safe_next_action(ctx: &Context, run_dir: &Path) -> Result<CommandResult, String> {
    let report = doctor_report(ctx, run_dir)?;
    execute_named_action(ctx, run_dir, &report.safe_next_action)
}

pub fn execute_named_action(ctx: &Context, run_dir: &Path, action: &str) -> Result<CommandResult, String> {
    let trimmed = action.trim();
    if trimmed.is_empty() || trimmed == "none" {
        return Ok(CommandResult::ok("No action to run."));
    }
    if trimmed == "start-solvers" {
        return run_stage_capture(ctx, run_dir, "start-solvers", &[]);
    }
    if trimmed == "rerun" {
        return run_stage_capture(ctx, run_dir, "rerun", &[]);
    }
    if let Some(stage) = trimmed.strip_prefix("start ") {
        return run_stage_capture(ctx, run_dir, "start", &[stage.trim()]);
    }
    if let Some(stage) = trimmed.strip_prefix("step-back ") {
        return run_stage_capture(ctx, run_dir, "step-back", &[stage.trim()]);
    }
    if let Some(stage) = trimmed.strip_prefix("recheck ") {
        return run_stage_capture(ctx, run_dir, "recheck", &[stage.trim()]);
    }
    Err(format!("Unsupported safe-next-action: {trimmed}"))
}

pub fn amend_run(
    ctx: &Context,
    run_dir: &Path,
    note: &str,
    rewind: &str,
    auto_refresh_prompts: bool,
) -> Result<CommandResult, String> {
    append_amendment(run_dir, note)?;
    let amendment_path = run_dir.join("amendments.md");
    let mut details = format!("Recorded amendment in {}\n", amendment_path.display());
    if rewind != "none" {
        let result = run_stage_capture(ctx, run_dir, "step-back", &[rewind])?;
        details.push_str(result.stdout.trim_end());
        details.push('\n');
        if !result.stderr.trim().is_empty() {
            details.push_str(result.stderr.trim_end());
            details.push('\n');
        }
        if result.code != 0 {
            return Ok(CommandResult {
                code: result.code,
                stdout: details,
                stderr: String::new(),
            });
        }
    }
    if auto_refresh_prompts {
        let result = run_stage_capture(ctx, run_dir, "refresh-prompts", &[])?;
        details.push_str(result.stdout.trim_end());
        details.push('\n');
        if !result.stderr.trim().is_empty() {
            details.push_str(result.stderr.trim_end());
            details.push('\n');
        }
        if result.code != 0 {
            return Ok(CommandResult {
                code: result.code,
                stdout: details,
                stderr: String::new(),
            });
        }
    }
    let concise = format!(
        "Amendment saved in {}.\nRun rewound to `{}`.\nNext: press `r` to resume the pipeline or `s` to run the safe next action.",
        amendment_path.display(),
        rewind
    );
    Ok(CommandResult {
        code: 0,
        stdout: concise,
        stderr: details,
    })
}

pub fn delete_run(run_dir: &Path) -> Result<(), String> {
    fs::remove_dir_all(run_dir).map_err(|err| format!("Could not delete {}: {err}", run_dir.display()))
}

pub fn choose_prune_candidates(root: &Path, keep: usize, older_than_days: Option<u64>) -> Result<Vec<PathBuf>, String> {
    let runs = discover_run_dirs(root)?;
    let protected: Vec<PathBuf> = runs.iter().take(keep).cloned().collect();
    let threshold = older_than_days.map(|days| {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        now.saturating_sub(days * 86_400)
    });
    let mut candidates = Vec::new();
    for run_dir in runs {
        if protected.iter().any(|item| item == &run_dir) {
            continue;
        }
        if let Some(limit) = threshold {
            let modified = run_dir
                .metadata()
                .and_then(|meta| meta.modified())
                .ok()
                .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
                .map(|duration| duration.as_secs())
                .unwrap_or(0);
            if modified > limit {
                continue;
            }
        }
        candidates.push(run_dir);
    }
    Ok(candidates)
}

pub fn append_amendment(run_dir: &Path, note: &str) -> Result<PathBuf, String> {
    let path = run_dir.join("amendments.md");
    let timestamp = current_timestamp();
    let mut content = if path.exists() {
        fs::read_to_string(&path).map_err(|err| format!("Could not read {}: {err}", path.display()))?
    } else {
        "# Amendments\n\n".to_string()
    };
    if !content.ends_with('\n') {
        content.push('\n');
    }
    content.push_str(&format!("## {timestamp}\n\n{}\n", note.trim()));
    fs::write(&path, content).map_err(|err| format!("Could not write {}: {err}", path.display()))?;
    Ok(path)
}

pub fn preview_text(run_dir: &Path, max_chars: usize) -> (String, String) {
    let candidates = [
        ("Summary", run_dir.join("review").join("user-summary.md")),
        ("Findings", run_dir.join("verification").join("findings.md")),
        ("Augmented", run_dir.join("verification").join("augmented-task.md")),
        ("Execution", run_dir.join("execution").join("report.md")),
        ("Brief", run_dir.join("brief.md")),
    ];
    for (label, path) in candidates {
        if !path.exists() {
            continue;
        }
        if let Ok(content) = fs::read_to_string(&path) {
            let trimmed = content.trim();
            if trimmed.is_empty() || trimmed.starts_with("Pending ") || trimmed.starts_with("Fill this file") {
                continue;
            }
            return (label.to_string(), trimmed.chars().take(max_chars).collect());
        }
    }
    ("Preview".to_string(), "No substantive artifact is available yet.".to_string())
}

pub fn latest_log_excerpt(run_dir: &Path, line_limit: usize) -> (String, Vec<String>) {
    let logs_dir = run_dir.join("logs");
    if !logs_dir.exists() {
        return ("Logs".to_string(), vec!["No log files yet.".to_string()]);
    }
    let mut logs: Vec<PathBuf> = match fs::read_dir(&logs_dir) {
        Ok(entries) => entries.filter_map(|entry| entry.ok().map(|item| item.path())).collect(),
        Err(_) => Vec::new(),
    };
    if logs.is_empty() {
        return ("Logs".to_string(), vec!["No log files yet.".to_string()]);
    }
    logs.sort_by_key(|path| path.metadata().and_then(|meta| meta.modified()).ok());
    let latest = logs.pop().unwrap_or_else(|| logs_dir.join("unknown.log"));
    let title = latest
        .file_name()
        .map(|value| value.to_string_lossy().to_string())
        .unwrap_or_else(|| "Logs".to_string());
    match fs::read_to_string(&latest) {
        Ok(content) => {
            let lines: Vec<String> = content.lines().map(|line| line.to_string()).collect();
            if lines.is_empty() {
                (title, vec!["<empty log>".to_string()])
            } else {
                (title, lines.into_iter().rev().take(line_limit).collect::<Vec<_>>().into_iter().rev().collect())
            }
        }
        Err(_) => (title, vec!["Could not read log file.".to_string()]),
    }
}

fn run_stage_json<T: DeserializeOwned>(
    ctx: &Context,
    run_dir: &Path,
    subcommand: &str,
    extra: &[&str],
) -> Result<T, String> {
    let result = run_stage_capture(ctx, run_dir, subcommand, extra)?;
    if result.code != 0 {
        return Err(nonempty_output(&result).unwrap_or_else(|| format!("{subcommand} failed")));
    }
    serde_json::from_str(result.stdout.trim())
        .map_err(|err| format!("Could not parse `{subcommand}` JSON for {}: {err}", run_dir.display()))
}

fn current_timestamp() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    format!("{now}")
}

fn next_stage_bucket(value: &str) -> String {
    if value.starts_with("solver-") {
        "solvers".to_string()
    } else {
        value.to_string()
    }
}

fn stage_rank(stage: &str) -> usize {
    match stage {
        "intake" => 1,
        "solvers" => 2,
        "review" => 3,
        "execution" => 4,
        "verification" => 5,
        "rerun" => 6,
        "none" => 7,
        _ => 99,
    }
}

fn nonempty_output(result: &CommandResult) -> Option<String> {
    if !result.stderr.trim().is_empty() {
        Some(result.stderr.trim().to_string())
    } else if !result.stdout.trim().is_empty() {
        Some(result.stdout.trim().to_string())
    } else {
        None
    }
}
