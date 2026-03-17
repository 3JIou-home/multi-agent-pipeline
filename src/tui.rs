use crate::engine::{
    amend_run, automate_run, choose_prune_candidates, contextual_log_excerpt, delete_run,
    execute_safe_next_action, load_run_snapshots, run_stage_capture, task_flow_capture,
    with_engine_observer, Context, EngineObserver, RunSnapshot, RunTokenSummary,
};
use crate::runtime::{self, RuntimeJobState};
use crossterm::event::{
    self, DisableBracketedPaste, DisableMouseCapture, EnableBracketedPaste, EnableMouseCapture,
    Event, KeyCode, KeyEventKind, KeyModifiers, MouseEvent, MouseEventKind,
};
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, List, ListItem, ListState, Paragraph, Wrap};
use ratatui::Terminal;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use std::env;
use std::io::{self, Stdout};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver, TryRecvError};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime};

const DEFAULT_KEEP: usize = 20;
const DEFAULT_PRUNE_DAYS: u64 = 14;

enum Mode {
    Normal,
    AmendInput {
        buffer: String,
    },
    NewRunInput {
        draft: NewRunDraft,
        task_scroll: u16,
    },
    InterviewInput {
        draft: NewRunDraft,
        session_dir: PathBuf,
        goal_summary: String,
        questions: Vec<InterviewQuestion>,
        answers: Vec<String>,
        index: usize,
        buffer: String,
        answer_scroll: u16,
    },
    PromptReview {
        draft: NewRunDraft,
        session_dir: PathBuf,
        final_task_path: PathBuf,
        scroll: u16,
        selected: PromptReviewAction,
    },
    ConfirmDelete {
        selected: ConfirmChoice,
    },
    ConfirmPrune {
        selected: ConfirmChoice,
    },
    ArtifactView {
        kind: ArtifactKind,
        scroll: u16,
    },
}

#[derive(Clone, Copy)]
enum ConfirmChoice {
    Cancel,
    Confirm,
}

impl ConfirmChoice {
    fn toggle(self) -> Self {
        match self {
            Self::Cancel => Self::Confirm,
            Self::Confirm => Self::Cancel,
        }
    }
}

#[derive(Clone, Copy)]
enum PromptReviewAction {
    CreateOnly,
    CreateAndStart,
    Cancel,
}

impl PromptReviewAction {
    fn next(self) -> Self {
        match self {
            Self::CreateOnly => Self::CreateAndStart,
            Self::CreateAndStart => Self::Cancel,
            Self::Cancel => Self::CreateOnly,
        }
    }

    fn previous(self) -> Self {
        match self {
            Self::CreateOnly => Self::Cancel,
            Self::CreateAndStart => Self::CreateOnly,
            Self::Cancel => Self::CreateAndStart,
        }
    }
}

#[derive(Clone, Copy)]
enum ArtifactKind {
    Summary,
    Findings,
    Augmented,
    Execution,
    Brief,
}

impl ArtifactKind {
    fn label(self) -> &'static str {
        match self {
            Self::Summary => "Summary",
            Self::Findings => "Findings",
            Self::Augmented => "Augmented",
            Self::Execution => "Execution",
            Self::Brief => "Brief",
        }
    }

    fn path(self, run: &RunSnapshot) -> PathBuf {
        match self {
            Self::Summary => preferred_summary_path(run),
            Self::Findings => run.run_dir.join("verification").join("findings.md"),
            Self::Augmented => run.run_dir.join("verification").join("augmented-task.md"),
            Self::Execution => run.run_dir.join("execution").join("report.md"),
            Self::Brief => run.run_dir.join("brief.md"),
        }
    }

    fn next(self) -> Self {
        match self {
            Self::Summary => Self::Findings,
            Self::Findings => Self::Augmented,
            Self::Augmented => Self::Execution,
            Self::Execution => Self::Brief,
            Self::Brief => Self::Summary,
        }
    }

    fn previous(self) -> Self {
        match self {
            Self::Summary => Self::Brief,
            Self::Findings => Self::Summary,
            Self::Augmented => Self::Findings,
            Self::Execution => Self::Augmented,
            Self::Brief => Self::Execution,
        }
    }
}

fn preferred_summary_path(run: &RunSnapshot) -> PathBuf {
    let verification = run.run_dir.join("verification").join("user-summary.md");
    if let Ok(content) = std::fs::read_to_string(&verification) {
        if !looks_pending_artifact(&content) {
            let trimmed = content.trim();
            if !trimmed.is_empty() {
                return verification;
            }
        }
    }
    let review = run.run_dir.join("review").join("user-summary.md");
    if let Ok(content) = std::fs::read_to_string(&review) {
        if !looks_pending_artifact(&content) {
            let trimmed = content.trim();
            if !trimmed.is_empty() {
                return review;
            }
        }
    }
    run.run_dir.join("request.md")
}

fn summary_title(run: &RunSnapshot) -> &'static str {
    let path = preferred_summary_path(run);
    match path
        .parent()
        .and_then(|value| value.file_name())
        .and_then(|value| value.to_str())
    {
        Some("verification") => "Verification Summary",
        Some("review") => "Review Summary",
        _ => "Summary",
    }
}

#[derive(Clone)]
struct NewRunDraft {
    task: String,
    workspace: String,
    title: String,
    task_cursor: usize,
    workspace_cursor: usize,
    title_cursor: usize,
    field: NewRunField,
}

#[derive(Clone)]
enum JobKind {
    RunAction,
    InterviewQuestions {
        draft: NewRunDraft,
    },
    InterviewFinalize {
        draft: NewRunDraft,
        session_dir: PathBuf,
        answers: Vec<serde_json::Value>,
    },
}

#[derive(Clone, Copy)]
enum NewRunField {
    Task,
    Workspace,
    Title,
    Start,
    Cancel,
}

impl NewRunField {
    fn next(self) -> Self {
        match self {
            Self::Task => Self::Workspace,
            Self::Workspace => Self::Title,
            Self::Title => Self::Start,
            Self::Start => Self::Cancel,
            Self::Cancel => Self::Task,
        }
    }

    fn previous(self) -> Self {
        match self {
            Self::Task => Self::Cancel,
            Self::Workspace => Self::Task,
            Self::Title => Self::Workspace,
            Self::Start => Self::Title,
            Self::Cancel => Self::Start,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct InterviewQuestion {
    id: String,
    question: String,
    #[serde(default)]
    why: String,
    #[serde(default = "default_true")]
    required: bool,
}

#[derive(Debug, Deserialize)]
struct InterviewQuestionsPayload {
    session_dir: String,
    #[serde(default)]
    goal_summary: String,
    #[serde(default)]
    questions: Vec<InterviewQuestion>,
}

#[derive(Debug, Deserialize)]
struct InterviewFinalizePayload {
    final_task_path: String,
}

fn default_true() -> bool {
    true
}

fn parse_embedded_json<T: DeserializeOwned>(text: &str) -> Result<T, String> {
    let bytes = text.as_bytes();
    let mut spans = Vec::new();
    let mut start = None;
    let mut depth = 0usize;
    let mut in_string = false;
    let mut escape = false;

    for (index, byte) in bytes.iter().enumerate() {
        if in_string {
            if escape {
                escape = false;
                continue;
            }
            match byte {
                b'\\' => escape = true,
                b'"' => in_string = false,
                _ => {}
            }
            continue;
        }

        match byte {
            b'"' => in_string = true,
            b'{' => {
                if depth == 0 {
                    start = Some(index);
                }
                depth += 1;
            }
            b'}' => {
                if depth == 0 {
                    continue;
                }
                depth -= 1;
                if depth == 0 {
                    if let Some(start_index) = start.take() {
                        spans.push((start_index, index + 1));
                    }
                }
            }
            _ => {}
        }
    }

    if spans.is_empty() {
        return Err("No JSON object found in process output.".to_string());
    }

    let mut last_error = None;
    for (start, end) in spans.into_iter().rev() {
        match serde_json::from_str(&text[start..end]) {
            Ok(payload) => return Ok(payload),
            Err(err) => last_error = Some(err),
        }
    }

    Err(format!(
        "Could not parse embedded JSON: {}",
        last_error
            .map(|err| err.to_string())
            .unwrap_or_else(|| "unknown error".to_string())
    ))
}

fn parse_job_json<T: DeserializeOwned>(stdout: &str, raw_output: &str) -> Result<T, String> {
    let stdout = stdout.trim();
    if !stdout.is_empty() {
        if let Ok(payload) = parse_embedded_json(stdout) {
            return Ok(payload);
        }
    }

    let raw_output = raw_output.trim();
    if !raw_output.is_empty() {
        if let Ok(payload) = parse_embedded_json(raw_output) {
            return Ok(payload);
        }
    }

    let mut errors = Vec::new();
    if !stdout.is_empty() {
        if let Err(err) = parse_embedded_json::<T>(stdout) {
            errors.push(format!("stdout: {err}"));
        }
    }
    if !raw_output.is_empty() && raw_output != stdout {
        if let Err(err) = parse_embedded_json::<T>(raw_output) {
            errors.push(format!("process log: {err}"));
        }
    }
    if errors.is_empty() {
        Err("No JSON payload found in job output.".to_string())
    } else {
        Err(errors.join(" | "))
    }
}

fn format_token_count(tokens: u64) -> String {
    if tokens >= 1_000_000 {
        format!("{:.1}M", tokens as f64 / 1_000_000.0)
    } else if tokens >= 1_000 {
        format!("{:.1}k", tokens as f64 / 1_000.0)
    } else {
        tokens.to_string()
    }
}

fn run_token_summary_line(summary: &RunTokenSummary) -> String {
    let budget = if summary.budget_total_tokens > 0 {
        format_token_count(summary.budget_total_tokens)
    } else {
        "unset".to_string()
    };
    let used = format_token_count(summary.used_total_tokens);
    let saved = format_token_count(summary.estimated_saved_tokens);
    let remaining = summary
        .remaining_tokens
        .map(format_token_count)
        .unwrap_or_else(|| "unknown".to_string());
    let source = if summary.source.is_empty() {
        "local".to_string()
    } else {
        summary.source.clone()
    };
    let warning = match summary.remaining_tokens {
        Some(remaining)
            if summary.warning_threshold_tokens > 0
                && remaining <= summary.warning_threshold_tokens =>
        {
            " | warning=low"
        }
        _ => "",
    };
    format!(
        "used={used} | saved={saved} | remaining={remaining} | budget={budget} ({source}){warning}"
    )
}

fn status_bar_tokens(summary: &RunTokenSummary) -> String {
    if summary.budget_total_tokens == 0 {
        return String::new();
    }
    let remaining = summary
        .remaining_tokens
        .map(format_token_count)
        .unwrap_or_else(|| "unknown".to_string());
    format!(" | tokens={remaining} left")
}

pub struct App {
    root: PathBuf,
    limit: usize,
    runs: Vec<RunSnapshot>,
    selected: usize,
    wizard_selected: bool,
    preview_scroll: u16,
    log_scroll: u16,
    mouse_capture_enabled: bool,
    mode: Mode,
    notice: String,
    last_output: String,
    last_refresh: Instant,
    job: Option<RunningJob>,
    wizard_job: Option<RunningJob>,
}

struct RunningJob {
    kind: JobKind,
    label: String,
    run_dir: PathBuf,
    log_hint: Option<String>,
    command_hint: String,
    started_at: Instant,
    started_wallclock: SystemTime,
    pid: i32,
    pgid: i32,
    process_log: PathBuf,
    stream_rx: Option<Receiver<String>>,
    completion_rx: Option<Receiver<JobResult>>,
    stream_lines: Vec<String>,
    attached: bool,
    last_heartbeat: Instant,
}

fn is_rerun_job(job: &RunningJob) -> bool {
    job.label == "rerun" || job.command_hint == "rerun"
}

fn rerun_created_run_dir(stdout: &str) -> Option<PathBuf> {
    for line in stdout.lines() {
        let candidate = line.trim();
        if candidate.is_empty() {
            continue;
        }
        let path = PathBuf::from(candidate);
        if path.is_absolute() && path.join("plan.json").exists() {
            return Some(path);
        }
    }
    None
}

struct JobResult {
    code: i32,
    stdout: String,
    stderr: String,
}

struct FinishedJob {
    kind: JobKind,
    label: String,
    run_dir: PathBuf,
    completed_log_hint: Option<String>,
    result: JobResult,
    detached_finish: bool,
}

struct UiEngineObserver {
    run_dir: PathBuf,
    stream_tx: mpsc::Sender<String>,
}

#[derive(Debug, Deserialize, Default)]
struct TuiPlanSummary {
    #[serde(default)]
    pipeline: TuiPipelineSummary,
}

#[derive(Debug, Deserialize, Default)]
struct TuiPipelineSummary {
    #[serde(default)]
    stages: Vec<TuiPipelineStageSummary>,
}

#[derive(Debug, Deserialize, Default)]
struct TuiPipelineStageSummary {
    #[serde(default)]
    id: String,
    #[serde(default)]
    kind: String,
}

impl EngineObserver for UiEngineObserver {
    fn process_started(&self, pid: i32, pgid: i32) {
        let _ = runtime::update_job_process(&self.run_dir, pid, pgid, Some("running"));
    }

    fn line(&self, line: &str) {
        let _ = runtime::append_process_line(&self.run_dir, line);
        let _ = self.stream_tx.send(line.to_string());
    }

    fn stage_changed(&self, stage: &str) {
        let _ = runtime::update_job_stage(&self.run_dir, Some(stage), None);
    }

    fn interrupt_run_dir(&self) -> Option<PathBuf> {
        Some(self.run_dir.clone())
    }
}

impl App {
    fn new(ctx: &Context, root: PathBuf, limit: usize) -> Result<Self, String> {
        let runs = load_run_snapshots(ctx, &root, limit)?;
        let mut app = Self {
            root,
            limit,
            runs,
            selected: 0,
            wizard_selected: false,
            preview_scroll: 0,
            log_scroll: 0,
            mouse_capture_enabled: true,
            mode: Mode::Normal,
            notice: "Ready".to_string(),
            last_output: String::new(),
            last_refresh: Instant::now(),
            job: None,
            wizard_job: None,
        };
        sync_runtime_job(&mut app);
        sync_wizard_job(&mut app);
        Ok(app)
    }

    fn refresh(&mut self, ctx: &Context) -> Result<(), String> {
        let selected_path = self.runs.get(self.selected).map(|run| run.run_dir.clone());
        self.runs = load_run_snapshots(ctx, &self.root, self.limit)?;
        if let Some(path) = selected_path {
            if let Some(index) = self.runs.iter().position(|run| run.run_dir == path) {
                self.selected = index;
            } else if !self.runs.is_empty() {
                self.selected = self.selected.min(self.runs.len().saturating_sub(1));
            } else {
                self.selected = 0;
            }
        } else {
            self.selected = 0;
        }
        self.last_refresh = Instant::now();
        sync_runtime_job(self);
        sync_wizard_job(self);
        if self.wizard_job().is_none() {
            self.wizard_selected = false;
        }
        Ok(())
    }

    fn wizard_job(&self) -> Option<&RunningJob> {
        self.wizard_job.as_ref()
    }

    fn selected_run(&self) -> Option<&RunSnapshot> {
        if self.wizard_selected && self.wizard_job().is_some() {
            return None;
        }
        self.runs.get(self.selected)
    }

    fn move_down(&mut self) {
        if self.wizard_selected && self.wizard_job().is_some() {
            self.wizard_selected = false;
            self.preview_scroll = 0;
            self.log_scroll = 0;
            return;
        }
        if !self.runs.is_empty() {
            let next = (self.selected + 1).min(self.runs.len() - 1);
            if next != self.selected {
                self.selected = next;
                self.preview_scroll = 0;
                self.log_scroll = 0;
            }
        }
    }

    fn move_up(&mut self) {
        if self.wizard_job().is_some() {
            if self.wizard_selected {
                return;
            }
            if self.selected == 0 {
                self.wizard_selected = true;
                self.preview_scroll = 0;
                self.log_scroll = 0;
                return;
            }
        }
        let next = self.selected.saturating_sub(1);
        if next != self.selected {
            self.selected = next;
            self.preview_scroll = 0;
            self.log_scroll = 0;
        }
    }

    fn select_run_by_path(&mut self, run_dir: &Path) {
        if let Some(index) = self.runs.iter().position(|run| run.run_dir == run_dir) {
            if self.selected != index {
                self.selected = index;
                self.preview_scroll = 0;
                self.log_scroll = 0;
            }
            self.wizard_selected = false;
        }
    }
}

impl RunningJob {
    #[allow(clippy::too_many_arguments)]
    fn owned(
        kind: JobKind,
        label: String,
        run_dir: PathBuf,
        log_hint: Option<String>,
        command_hint: String,
        pid: i32,
        pgid: i32,
        process_log: PathBuf,
        stream_rx: Receiver<String>,
        completion_rx: Receiver<JobResult>,
    ) -> Self {
        Self {
            kind,
            label,
            run_dir,
            log_hint,
            command_hint,
            started_at: Instant::now(),
            started_wallclock: SystemTime::now(),
            pid,
            pgid,
            process_log,
            stream_rx: Some(stream_rx),
            completion_rx: Some(completion_rx),
            stream_lines: Vec::new(),
            attached: false,
            last_heartbeat: Instant::now(),
        }
    }

    fn attached(run_dir: PathBuf, state: RuntimeJobState) -> Self {
        let elapsed = runtime::elapsed_from_unix(state.started_at_unix);
        let started_at = Instant::now()
            .checked_sub(elapsed)
            .unwrap_or_else(Instant::now);
        let process_log = runtime::runtime_dir(&run_dir).join(state.process_log.clone());
        Self {
            kind: JobKind::RunAction,
            label: state.label.clone(),
            run_dir,
            log_hint: state.stage.clone(),
            command_hint: state.command_hint.clone(),
            started_at,
            started_wallclock: runtime::system_time_from_unix(state.started_at_unix),
            pid: state.pid,
            pgid: state.pgid,
            process_log,
            stream_rx: None,
            completion_rx: None,
            stream_lines: Vec::new(),
            attached: true,
            last_heartbeat: Instant::now(),
        }
    }

    fn refresh_from_state(&mut self, state: RuntimeJobState) {
        self.label = state.label;
        self.log_hint = state.stage;
        self.command_hint = state.command_hint;
        self.pid = state.pid;
        self.pgid = state.pgid;
        self.started_wallclock = runtime::system_time_from_unix(state.started_at_unix);
        self.started_at = Instant::now()
            .checked_sub(runtime::elapsed_from_unix(state.started_at_unix))
            .unwrap_or_else(Instant::now);
        self.process_log = runtime::runtime_dir(&self.run_dir).join(state.process_log);
    }
}

fn reconcile_job_stage_with_run(job: &mut RunningJob, run: Option<&RunSnapshot>) {
    if is_rerun_job(job) {
        return;
    }
    let Some(run) = run else {
        return;
    };
    let next = run.status.next.trim();
    if next.is_empty() || next == "none" || next == "rerun" {
        return;
    }
    let current = job.log_hint.as_deref().unwrap_or("").trim();
    if current.is_empty() {
        job.log_hint = Some(next.to_string());
        let _ = runtime::update_job_stage(&job.run_dir, Some(next), None);
        return;
    }
    if current == next {
        return;
    }
    let current_state = run.status.stages.get(current).map(String::as_str);
    if current_state.is_none() || matches!(current_state, Some("done")) {
        job.log_hint = Some(next.to_string());
        let _ = runtime::update_job_stage(&job.run_dir, Some(next), None);
    }
}

fn run_has_active_job(run_dir: &Path) -> bool {
    runtime::active_job_state(run_dir).is_some()
}

fn detach_tracked_job(app: &mut App) {
    if let Some(job) = app.job.take() {
        app.notice = format!(
            "Detached from {}. It continues in the background.",
            running_job_label(&job)
        );
        app.last_output = format!(
            "Detached from `{}` in {}. The process is still running; reselect that run to monitor it again.",
            job_stage_label(&job),
            job.run_dir
                .file_name()
                .and_then(|value| value.to_str())
                .unwrap_or("run")
        );
    }
}

fn ensure_job_slot_for_run(app: &mut App, run_dir: &Path) -> bool {
    if let Some(job) = app.job.as_ref() {
        if job.run_dir == run_dir {
            app.notice =
                "This run already has an active job. Attached to the existing process.".to_string();
            return false;
        }
        detach_tracked_job(app);
    }
    if let Some(state) = runtime::active_job_state(run_dir) {
        let mut attached = RunningJob::attached(run_dir.to_path_buf(), state);
        let run = app.runs.iter().find(|run| run.run_dir == run_dir);
        reconcile_job_stage_with_run(&mut attached, run);
        app.notice = format!(
            "Attached to running {} for {}",
            job_stage_label(&attached),
            run_dir
                .file_name()
                .and_then(|value| value.to_str())
                .unwrap_or("run")
        );
        app.job = Some(attached);
        return false;
    }
    true
}

fn ensure_wizard_job_slot(app: &mut App, run_dir: &Path) -> bool {
    if let Some(job) = app.wizard_job.as_ref() {
        if job.run_dir == run_dir {
            app.notice = "Stage0 creation is already running. Attached to the existing wizard job."
                .to_string();
            return false;
        }
    }
    if let Some(state) = runtime::active_job_state(run_dir) {
        app.wizard_job = Some(RunningJob::attached(run_dir.to_path_buf(), state));
        app.notice = "Attached to existing stage0 creation job.".to_string();
        return false;
    }
    true
}

fn attach_selected_run_job_if_any(app: &mut App) {
    let Some(selected) = app.selected_run().map(|run| run.run_dir.clone()) else {
        return;
    };
    let Some(state) = runtime::active_job_state(&selected) else {
        return;
    };
    let should_switch = match app.job.as_ref() {
        Some(job) => job.run_dir != selected,
        None => true,
    };
    if !should_switch {
        return;
    }
    let mut attached = RunningJob::attached(selected.clone(), state);
    let run = app.runs.iter().find(|run| run.run_dir == selected);
    reconcile_job_stage_with_run(&mut attached, run);
    app.notice = format!(
        "Attached to running {} for {}",
        job_stage_label(&attached),
        selected
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("run")
    );
    app.job = Some(attached);
}

fn delete_run_if_safe(app: &mut App, run_dir: &Path) -> Result<bool, String> {
    if run_has_active_job(run_dir) {
        app.notice = format!(
            "Cannot delete {} while it has an active job.",
            run_dir
                .file_name()
                .and_then(|value| value.to_str())
                .unwrap_or("run")
        );
        app.last_output =
            "Interrupt or wait for the active run to finish before deleting it.".to_string();
        return Ok(false);
    }
    delete_run(run_dir)?;
    if app
        .job
        .as_ref()
        .map(|job| job.run_dir == run_dir)
        .unwrap_or(false)
    {
        app.job = None;
    }
    Ok(true)
}

fn prune_candidates_without_active_jobs(candidates: Vec<PathBuf>) -> (Vec<PathBuf>, usize) {
    let mut runnable = Vec::new();
    let mut skipped = 0usize;
    for run_dir in candidates {
        if run_has_active_job(&run_dir) {
            skipped += 1;
        } else {
            runnable.push(run_dir);
        }
    }
    (runnable, skipped)
}

fn sync_runtime_job(app: &mut App) {
    attach_selected_run_job_if_any(app);
    if let Some(job) = app.job.as_mut() {
        if let Some(state) = runtime::active_job_state(&job.run_dir) {
            job.refresh_from_state(state);
            let run = app.runs.iter().find(|run| run.run_dir == job.run_dir);
            reconcile_job_stage_with_run(job, run);
        } else if job.attached {
            app.notice = format!(
                "Observed job for {} is no longer running.",
                job.run_dir
                    .file_name()
                    .and_then(|value| value.to_str())
                    .unwrap_or("run")
            );
            app.job = None;
        }
        return;
    }

    let mut candidates: Vec<PathBuf> = Vec::new();
    if let Some(run) = app.selected_run() {
        candidates.push(run.run_dir.clone());
    }
    for run in &app.runs {
        if !candidates.iter().any(|item| item == &run.run_dir) {
            candidates.push(run.run_dir.clone());
        }
    }
    for run_dir in candidates {
        if let Some(state) = runtime::active_job_state(&run_dir) {
            let mut attached = RunningJob::attached(run_dir.clone(), state);
            let run = app.runs.iter().find(|run| run.run_dir == run_dir);
            reconcile_job_stage_with_run(&mut attached, run);
            app.notice = format!(
                "Attached to running {} for {}",
                job_stage_label(&attached),
                run_dir
                    .file_name()
                    .and_then(|value| value.to_str())
                    .unwrap_or("run")
            );
            app.job = Some(attached);
            break;
        }
    }
}

fn sync_wizard_job(app: &mut App) {
    let run_dir = ui_job_dir(&app.root);
    if let Some(job) = app.wizard_job.as_mut() {
        if let Some(state) = runtime::active_job_state(&job.run_dir) {
            job.refresh_from_state(state);
        } else if job.attached {
            app.notice = "Observed stage0 job is no longer running.".to_string();
            app.wizard_job = None;
        }
        return;
    }
    if let Some(state) = runtime::active_job_state(&run_dir) {
        app.wizard_job = Some(RunningJob::attached(run_dir, state));
    }
}

fn default_new_run_draft() -> NewRunDraft {
    NewRunDraft {
        task: String::new(),
        workspace: String::new(),
        title: String::new(),
        task_cursor: 0,
        workspace_cursor: 0,
        title_cursor: 0,
        field: NewRunField::Task,
    }
}

fn char_count(value: &str) -> usize {
    value.chars().count()
}

fn char_to_byte_index(value: &str, cursor: usize) -> usize {
    value
        .char_indices()
        .nth(cursor)
        .map(|(index, _)| index)
        .unwrap_or_else(|| value.len())
}

fn insert_text_at_cursor(value: &mut String, cursor: &mut usize, text: &str) {
    let byte_index = char_to_byte_index(value, *cursor);
    value.insert_str(byte_index, text);
    *cursor = cursor.saturating_add(text.chars().count());
}

fn backspace_at_cursor(value: &mut String, cursor: &mut usize) {
    if *cursor == 0 {
        return;
    }
    let start = char_to_byte_index(value, cursor.saturating_sub(1));
    let end = char_to_byte_index(value, *cursor);
    value.replace_range(start..end, "");
    *cursor = cursor.saturating_sub(1);
}

fn ui_job_dir(root: &Path) -> PathBuf {
    root.join(".agpipe-ui")
}

fn create_run_from_draft(
    ctx: &Context,
    app: &mut App,
    draft: &NewRunDraft,
    task_file: Option<&Path>,
    session_dir: &Path,
) -> Result<PathBuf, String> {
    let cache_root = env::var("XDG_CACHE_HOME")
        .map(PathBuf::from)
        .or_else(|_| env::var("HOME").map(|home| PathBuf::from(home).join(".cache")))
        .unwrap_or_else(|_| PathBuf::from(".cache"))
        .join("multi-agent-pipeline");
    let mut args = vec![
        "--workspace".to_string(),
        draft.workspace.clone(),
        "--output-dir".to_string(),
        app.root.display().to_string(),
        "--prompt-format".to_string(),
        "compact".to_string(),
        "--summary-language".to_string(),
        "ru".to_string(),
        "--intake-research".to_string(),
        "research-first".to_string(),
        "--stage-research".to_string(),
        "local-first".to_string(),
        "--execution-network".to_string(),
        "fetch-if-needed".to_string(),
        "--cache-root".to_string(),
        cache_root.display().to_string(),
        "--cache-policy".to_string(),
        "reuse".to_string(),
    ];
    if let Some(task_file) = task_file {
        args.push("--task-file".to_string());
        args.push(task_file.display().to_string());
    } else {
        args.push("--task".to_string());
        args.push(draft.task.clone());
    }
    if !draft.title.trim().is_empty() {
        args.push("--title".to_string());
        args.push(draft.title.clone());
    }
    if session_dir.exists() {
        args.push("--interview-session".to_string());
        args.push(session_dir.display().to_string());
    }
    let result = crate::engine::task_flow_capture(ctx, "create-run", &args)?;
    if result.code != 0 {
        app.last_output = result.combined_output();
        return Err("Run creation failed.".to_string());
    }
    Ok(PathBuf::from(result.stdout.trim()))
}

fn spawn_resume_for_run(ctx: &Context, app: &mut App, run_dir: PathBuf) -> Result<(), String> {
    if !ensure_job_slot_for_run(app, &run_dir) {
        return Ok(());
    }
    let index = app.runs.iter().position(|run| run.run_dir == run_dir);
    if let Some(index) = index {
        app.selected = index;
    }
    spawn_action(
        ctx,
        app,
        "resume",
        vec![
            "resume".to_string(),
            "--until".to_string(),
            "verification".to_string(),
            "--auto-approve".to_string(),
        ],
    )
}

fn spawn_interview_questions_job(
    ctx: &Context,
    app: &mut App,
    draft: NewRunDraft,
) -> Result<(), String> {
    let run_dir = ui_job_dir(&app.root);
    if !ensure_wizard_job_slot(app, &run_dir) {
        return Ok(());
    }
    let label = "interview-questions".to_string();
    let command_hint = "stage0 interview questions".to_string();
    let output_root = app.root.clone();
    let draft_for_job = draft.clone();
    spawn_engine_job(
        ctx,
        app,
        JobKind::InterviewQuestions { draft },
        label,
        run_dir,
        None,
        command_hint,
        move |ctx| {
            let args = vec![
                "--task".to_string(),
                draft_for_job.task.clone(),
                "--workspace".to_string(),
                draft_for_job.workspace.clone(),
                "--output-dir".to_string(),
                output_root.display().to_string(),
                "--language".to_string(),
                "ru".to_string(),
                "--max-questions".to_string(),
                "6".to_string(),
            ];
            task_flow_capture(ctx, "interview-questions", &args)
        },
    )?;
    app.wizard_selected = true;
    app.preview_scroll = 0;
    app.log_scroll = 0;
    Ok(())
}

fn spawn_interview_finalize_job(
    ctx: &Context,
    app: &mut App,
    draft: NewRunDraft,
    session_dir: PathBuf,
    answers: Vec<serde_json::Value>,
) -> Result<(), String> {
    let answers_path = session_dir.join("answers-ui.json");
    std::fs::write(
        &answers_path,
        serde_json::to_vec_pretty(&answers)
            .map_err(|err| format!("Could not serialize interview answers: {err}"))?,
    )
    .map_err(|err| format!("Could not write {}: {err}", answers_path.display()))?;
    let run_dir = ui_job_dir(&app.root);
    if !ensure_wizard_job_slot(app, &run_dir) {
        return Ok(());
    }
    let label = "interview-finalize".to_string();
    let command_hint = "stage0 interview finalize".to_string();
    let draft_for_job = draft.clone();
    let session_for_job = session_dir.clone();
    spawn_engine_job(
        ctx,
        app,
        JobKind::InterviewFinalize {
            draft,
            session_dir,
            answers,
        },
        label,
        run_dir,
        None,
        command_hint,
        move |ctx| {
            let args = vec![
                "--task".to_string(),
                draft_for_job.task.clone(),
                "--workspace".to_string(),
                draft_for_job.workspace.clone(),
                "--session-dir".to_string(),
                session_for_job.display().to_string(),
                "--answers-file".to_string(),
                answers_path.display().to_string(),
                "--language".to_string(),
                "ru".to_string(),
            ];
            task_flow_capture(ctx, "interview-finalize", &args)
        },
    )?;
    app.wizard_selected = true;
    app.preview_scroll = 0;
    app.log_scroll = 0;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn spawn_engine_job<F>(
    ctx: &Context,
    app: &mut App,
    kind: JobKind,
    label: String,
    run_dir: PathBuf,
    log_hint: Option<String>,
    command_hint: String,
    job_fn: F,
) -> Result<(), String>
where
    F: FnOnce(&Context) -> Result<crate::engine::CommandResult, String> + Send + 'static,
{
    app.notice = format!("Running {label}");
    app.last_output.clear();
    let (stream_tx, stream_rx) = mpsc::channel();
    let (completion_tx, completion_rx) = mpsc::channel();
    runtime::start_pending_job(&run_dir, &label, log_hint.as_deref(), &command_hint)?;
    let _ = runtime::append_process_line(&run_dir, &format!("Starting {command_hint}"));
    let _ = stream_tx.send(format!("Starting {command_hint}"));
    let observer = Arc::new(UiEngineObserver {
        run_dir: run_dir.clone(),
        stream_tx: stream_tx.clone(),
    });
    let ctx = ctx.clone();
    let run_dir_for_thread = run_dir.clone();
    thread::spawn(move || {
        let result = with_engine_observer(observer, || job_fn(&ctx));
        let job_result = match result {
            Ok(result) => JobResult {
                code: result.code,
                stdout: result.stdout,
                stderr: result.stderr,
            },
            Err(err) => JobResult {
                code: if err.contains("Interrupted from agpipe.") {
                    130
                } else {
                    1
                },
                stdout: String::new(),
                stderr: err,
            },
        };
        if !job_result.stdout.trim().is_empty() {
            let _ = runtime::append_process_line(&run_dir_for_thread, job_result.stdout.trim_end());
        }
        if !job_result.stderr.trim().is_empty() {
            let _ = runtime::append_process_line(&run_dir_for_thread, job_result.stderr.trim_end());
        }
        let status = if job_result.code == 0 {
            "completed"
        } else if job_result.code == 130 {
            "interrupted"
        } else {
            "failed"
        };
        let message = if job_result.stderr.trim().is_empty() {
            None
        } else {
            Some(job_result.stderr.as_str())
        };
        let _ = runtime::finish_job(&run_dir_for_thread, status, Some(job_result.code), message);
        let _ = completion_tx.send(job_result);
    });
    let tracked = RunningJob::owned(
        kind,
        label,
        run_dir.clone(),
        log_hint,
        command_hint,
        0,
        0,
        runtime::process_log_path(&run_dir),
        stream_rx,
        completion_rx,
    );
    if matches!(tracked.kind, JobKind::RunAction) {
        app.job = Some(tracked);
    } else {
        app.wizard_job = Some(tracked);
    }
    Ok(())
}

fn interview_answers(mode: &Mode) -> Vec<serde_json::Value> {
    let mut pairs = Vec::new();
    if let Mode::InterviewInput {
        questions, answers, ..
    } = mode
    {
        for (index, question) in questions.iter().enumerate() {
            pairs.push(serde_json::json!({
                "id": question.id,
                "question": question.question,
                "answer": answers.get(index).cloned().unwrap_or_default(),
            }));
        }
    }
    pairs
}

pub fn launch(ctx: &Context, root: &Path, limit: usize) -> Result<(), String> {
    let mut app = App::new(ctx, root.to_path_buf(), limit)?;
    let mut terminal = init_terminal()?;
    let result = run_app(ctx, &mut terminal, &mut app);
    restore_terminal(&mut terminal)?;
    result
}

fn init_terminal() -> Result<Terminal<CrosstermBackend<Stdout>>, String> {
    enable_raw_mode().map_err(|err| format!("Could not enable raw mode: {err}"))?;
    let mut stdout = io::stdout();
    execute!(
        stdout,
        EnterAlternateScreen,
        EnableMouseCapture,
        EnableBracketedPaste
    )
    .map_err(|err| format!("Could not enter alternate screen: {err}"))?;
    let backend = CrosstermBackend::new(stdout);
    Terminal::new(backend).map_err(|err| format!("Could not initialize terminal: {err}"))
}

fn restore_terminal(terminal: &mut Terminal<CrosstermBackend<Stdout>>) -> Result<(), String> {
    disable_raw_mode().map_err(|err| format!("Could not disable raw mode: {err}"))?;
    execute!(
        terminal.backend_mut(),
        DisableBracketedPaste,
        DisableMouseCapture,
        LeaveAlternateScreen
    )
    .map_err(|err| format!("Could not leave alternate screen: {err}"))?;
    terminal
        .show_cursor()
        .map_err(|err| format!("Could not restore cursor: {err}"))
}

fn run_app(
    ctx: &Context,
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    app: &mut App,
) -> Result<(), String> {
    loop {
        terminal
            .draw(|frame| draw(frame, app))
            .map_err(|err| format!("TUI draw failed: {err}"))?;

        if event::poll(Duration::from_millis(200))
            .map_err(|err| format!("Event poll failed: {err}"))?
        {
            let event = event::read().map_err(|err| format!("Event read failed: {err}"))?;
            match event {
                Event::Key(key) => {
                    if key.kind == KeyEventKind::Press && handle_key(ctx, app, key)? {
                        return Ok(());
                    }
                }
                Event::Paste(text) => handle_paste(app, &text),
                Event::Mouse(mouse) => {
                    let size = terminal
                        .size()
                        .map_err(|err| format!("Could not inspect terminal size: {err}"))?;
                    handle_mouse(app, mouse, size);
                }
                _ => {}
            }
        }

        poll_job(ctx, app)?;

        let refresh_interval = if app.job.is_some() || app.wizard_job.is_some() {
            Duration::from_secs(1)
        } else {
            Duration::from_secs(3)
        };
        if app.last_refresh.elapsed() > refresh_interval {
            let _ = app.refresh(ctx);
        }
    }
}

fn handle_key(
    ctx: &Context,
    app: &mut App,
    key: crossterm::event::KeyEvent,
) -> Result<bool, String> {
    match app.mode {
        Mode::Normal => handle_normal_key(ctx, app, key),
        Mode::ConfirmDelete { .. } => handle_delete_confirm(ctx, app, key),
        Mode::ConfirmPrune { .. } => handle_prune_confirm(ctx, app, key),
        Mode::ArtifactView { .. } => handle_artifact_view_key(app, key),
        Mode::AmendInput { .. } => handle_amend_key(ctx, app, key),
        Mode::NewRunInput { .. } => handle_new_run_key(ctx, app, key),
        Mode::InterviewInput { .. } => handle_interview_key(ctx, app, key),
        Mode::PromptReview { .. } => handle_prompt_review_key(ctx, app, key),
    }
}

fn set_mouse_capture(enabled: bool) -> Result<(), String> {
    let mut stdout = io::stdout();
    if enabled {
        execute!(stdout, EnableMouseCapture)
            .map_err(|err| format!("Could not enable mouse capture: {err}"))
    } else {
        execute!(stdout, DisableMouseCapture)
            .map_err(|err| format!("Could not disable mouse capture: {err}"))
    }
}

fn toggle_mouse_capture(app: &mut App) -> Result<(), String> {
    let enabled = !app.mouse_capture_enabled;
    set_mouse_capture(enabled)?;
    app.mouse_capture_enabled = enabled;
    app.notice = if enabled {
        "Mouse scroll enabled. Terminal text selection is disabled until you press m again."
            .to_string()
    } else {
        "Mouse capture disabled. Terminal text selection works again.".to_string()
    };
    Ok(())
}

#[derive(Clone, Copy)]
struct NormalModeRects {
    runs: Rect,
    preview: Rect,
    logs: Rect,
}

fn root_layout(size: Rect) -> Vec<Rect> {
    Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(2),
            Constraint::Length(1),
            Constraint::Min(10),
            Constraint::Length(15),
            Constraint::Length(1),
        ])
        .split(size)
        .to_vec()
}

fn normal_mode_rects(size: Rect) -> NormalModeRects {
    let layout = root_layout(size);
    let body = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(32), Constraint::Percentage(68)])
        .split(layout[2]);
    let details = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(58), Constraint::Percentage(42)])
        .split(body[1]);
    NormalModeRects {
        runs: body[0],
        preview: details[0],
        logs: layout[3],
    }
}

fn point_in_rect(column: u16, row: u16, rect: Rect) -> bool {
    rect.width > 0
        && rect.height > 0
        && column >= rect.x
        && column < rect.x.saturating_add(rect.width)
        && row >= rect.y
        && row < rect.y.saturating_add(rect.height)
}

fn scroll_up(value: &mut u16, amount: u16) {
    *value = value.saturating_sub(amount);
}

fn scroll_down(value: &mut u16, amount: u16) {
    *value = value.saturating_add(amount);
}

fn wrapped_line_count(text: &str, content_width: u16) -> usize {
    let width = content_width.max(1) as usize;
    if text.is_empty() {
        return 1;
    }
    text.split('\n')
        .map(|line| {
            let char_count = line.chars().count().max(1);
            char_count.div_ceil(width)
        })
        .sum()
}

fn clamp_scroll_for_text(text: &str, area: Rect, requested: u16) -> u16 {
    let visible_lines = area.height.saturating_sub(2) as usize;
    if visible_lines == 0 {
        return 0;
    }
    let content_width = area.width.saturating_sub(2);
    let total_lines = wrapped_line_count(text, content_width);
    let max_scroll = total_lines.saturating_sub(visible_lines);
    requested.min(max_scroll as u16)
}

fn handle_mouse(app: &mut App, mouse: MouseEvent, size: Rect) {
    if !app.mouse_capture_enabled {
        return;
    }
    match &mut app.mode {
        Mode::Normal => handle_normal_mouse(app, mouse, size),
        Mode::ArtifactView { scroll, .. } => {
            if point_in_rect(mouse.column, mouse.row, centered_rect(88, 86, size)) {
                match mouse.kind {
                    MouseEventKind::ScrollDown => scroll_down(scroll, 3),
                    MouseEventKind::ScrollUp => scroll_up(scroll, 3),
                    _ => {}
                }
            }
        }
        Mode::PromptReview { scroll, .. } => {
            if point_in_rect(mouse.column, mouse.row, centered_rect(88, 86, size)) {
                match mouse.kind {
                    MouseEventKind::ScrollDown => scroll_down(scroll, 3),
                    MouseEventKind::ScrollUp => scroll_up(scroll, 3),
                    _ => {}
                }
            }
        }
        Mode::NewRunInput { task_scroll, .. } => {
            let popup = new_run_popup_rect(size);
            let layout = new_run_popup_layout(popup);
            if point_in_rect(mouse.column, mouse.row, layout[1]) {
                match mouse.kind {
                    MouseEventKind::ScrollDown => scroll_down(task_scroll, 3),
                    MouseEventKind::ScrollUp => scroll_up(task_scroll, 3),
                    _ => {}
                }
            }
        }
        Mode::InterviewInput { answer_scroll, .. } => {
            let popup = interview_popup_rect(size);
            let layout = interview_popup_layout(popup);
            if point_in_rect(mouse.column, mouse.row, layout[3]) {
                match mouse.kind {
                    MouseEventKind::ScrollDown => scroll_down(answer_scroll, 3),
                    MouseEventKind::ScrollUp => scroll_up(answer_scroll, 3),
                    _ => {}
                }
            }
        }
        _ => {}
    }
}

fn handle_normal_mouse(app: &mut App, mouse: MouseEvent, size: Rect) {
    let rects = normal_mode_rects(size);
    match mouse.kind {
        MouseEventKind::ScrollDown => {
            if point_in_rect(mouse.column, mouse.row, rects.runs) {
                app.move_down();
                attach_selected_run_job_if_any(app);
            } else if point_in_rect(mouse.column, mouse.row, rects.preview) {
                scroll_down(&mut app.preview_scroll, 3);
            } else if point_in_rect(mouse.column, mouse.row, rects.logs) {
                scroll_down(&mut app.log_scroll, 3);
            }
        }
        MouseEventKind::ScrollUp => {
            if point_in_rect(mouse.column, mouse.row, rects.runs) {
                app.move_up();
                attach_selected_run_job_if_any(app);
            } else if point_in_rect(mouse.column, mouse.row, rects.preview) {
                scroll_up(&mut app.preview_scroll, 3);
            } else if point_in_rect(mouse.column, mouse.row, rects.logs) {
                scroll_up(&mut app.log_scroll, 3);
            }
        }
        _ => {}
    }
}

fn keyboard_layout_alias(primary: char) -> Option<char> {
    match primary {
        'q' => Some('й'),
        'w' => Some('ц'),
        'e' => Some('у'),
        'r' => Some('к'),
        't' => Some('е'),
        'y' => Some('н'),
        'u' => Some('г'),
        'i' => Some('ш'),
        'o' => Some('щ'),
        'p' => Some('з'),
        '[' => Some('х'),
        ']' => Some('ъ'),
        'a' => Some('ф'),
        's' => Some('ы'),
        'd' => Some('в'),
        'f' => Some('а'),
        'g' => Some('п'),
        'h' => Some('р'),
        'j' => Some('о'),
        'k' => Some('л'),
        'l' => Some('д'),
        'z' => Some('я'),
        'x' => Some('ч'),
        'c' => Some('с'),
        'v' => Some('м'),
        'b' => Some('и'),
        'n' => Some('т'),
        'm' => Some('ь'),
        _ => None,
    }
}

fn key_is_char(key: crossterm::event::KeyEvent, primary: char) -> bool {
    match key.code {
        KeyCode::Char(ch) => {
            let lower = ch.to_lowercase().next().unwrap_or(ch);
            lower == primary || keyboard_layout_alias(primary) == Some(lower)
        }
        _ => false,
    }
}

fn handle_normal_key(
    ctx: &Context,
    app: &mut App,
    key: crossterm::event::KeyEvent,
) -> Result<bool, String> {
    match key.code {
        _ if key.modifiers.contains(KeyModifiers::CONTROL) && key_is_char(key, 'c') => {
            return Ok(true)
        }
        _ if key_is_char(key, 'q') => return Ok(true),
        _ if key_is_char(key, 'm') => toggle_mouse_capture(app)?,
        _ if key_is_char(key, 'i') => interrupt_job(app)?,
        KeyCode::Esc => app.notice = "Ready".to_string(),
        KeyCode::Down => {
            app.move_down();
            attach_selected_run_job_if_any(app);
        }
        _ if key_is_char(key, 'j') => {
            app.move_down();
            attach_selected_run_job_if_any(app);
        }
        KeyCode::Up => {
            app.move_up();
            attach_selected_run_job_if_any(app);
        }
        _ if key_is_char(key, 'k') => {
            app.move_up();
            attach_selected_run_job_if_any(app);
        }
        _ if key_is_char(key, 'g') => {
            app.refresh(ctx)?;
            app.notice = "Refreshed run list".to_string();
        }
        KeyCode::Enter => {
            if let Some(run) = app.selected_run() {
                app.mode = Mode::ArtifactView {
                    kind: preferred_artifact_kind(run),
                    scroll: 0,
                };
            }
        }
        _ if key_is_char(key, 'o') => {
            if let Some(run) = app.selected_run() {
                app.mode = Mode::ArtifactView {
                    kind: preferred_artifact_kind(run),
                    scroll: 0,
                };
            }
        }
        KeyCode::Char('1') => {
            app.mode = Mode::ArtifactView {
                kind: ArtifactKind::Summary,
                scroll: 0,
            };
        }
        KeyCode::Char('2') => {
            app.mode = Mode::ArtifactView {
                kind: ArtifactKind::Findings,
                scroll: 0,
            };
        }
        KeyCode::Char('3') => {
            app.mode = Mode::ArtifactView {
                kind: ArtifactKind::Augmented,
                scroll: 0,
            };
        }
        KeyCode::Char('4') => {
            app.mode = Mode::ArtifactView {
                kind: ArtifactKind::Execution,
                scroll: 0,
            };
        }
        KeyCode::Char('5') => {
            app.mode = Mode::ArtifactView {
                kind: ArtifactKind::Brief,
                scroll: 0,
            };
        }
        _ if key_is_char(key, 'c') => {
            app.mode = Mode::NewRunInput {
                draft: default_new_run_draft(),
                task_scroll: 0,
            };
        }
        _ if key_is_char(key, 's') => {
            spawn_action(ctx, app, "safe-next", vec!["safe-next".to_string()])?
        }
        _ if key_is_char(key, 'n') => {
            spawn_action(ctx, app, "start-next", vec!["start-next".to_string()])?
        }
        _ if key_is_char(key, 'r') || key_is_char(key, 'w') => spawn_action(
            ctx,
            app,
            "resume",
            vec![
                "resume".to_string(),
                "--until".to_string(),
                "verification".to_string(),
                "--auto-approve".to_string(),
            ],
        )?,
        _ if key_is_char(key, 'a') => {
            app.mode = Mode::AmendInput {
                buffer: String::new(),
            }
        }
        _ if key_is_char(key, 'y') => spawn_action(ctx, app, "rerun", vec!["rerun".to_string()])?,
        _ if key_is_char(key, 'h') => spawn_action(
            ctx,
            app,
            "host-probe",
            vec!["host-probe".to_string(), "--refresh".to_string()],
        )?,
        _ if key_is_char(key, 'x') => {
            if app.selected_run().is_some() {
                app.mode = Mode::ConfirmDelete {
                    selected: ConfirmChoice::Confirm,
                };
            }
        }
        _ if key_is_char(key, 'p') => {
            app.mode = Mode::ConfirmPrune {
                selected: ConfirmChoice::Cancel,
            };
        }
        _ if key_is_char(key, 'u') => spawn_action(
            ctx,
            app,
            "refresh-prompts",
            vec!["refresh-prompts".to_string()],
        )?,
        _ if key_is_char(key, 'b') => spawn_action(
            ctx,
            app,
            "step-back review",
            vec!["step-back".to_string(), "review".to_string()],
        )?,
        _ if key_is_char(key, 'v') => spawn_action(
            ctx,
            app,
            "recheck verification",
            vec!["recheck".to_string(), "verification".to_string()],
        )?,
        _ => {}
    }
    Ok(false)
}

fn normalize_single_line_paste(text: &str) -> String {
    sanitize_terminal_input(text, false)
}

fn sanitize_multiline_paste(text: &str) -> String {
    sanitize_terminal_input(text, true)
}

fn sanitize_terminal_input(text: &str, preserve_newlines: bool) -> String {
    let mut out = String::new();
    let mut chars = text.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '\u{1b}' => skip_terminal_escape(&mut chars),
            '\r' => {
                if matches!(chars.peek(), Some('\n')) {
                    continue;
                }
                if preserve_newlines {
                    out.push('\n');
                } else {
                    out.push(' ');
                }
            }
            '\n' => {
                if preserve_newlines {
                    out.push('\n');
                } else {
                    out.push(' ');
                }
            }
            '\t' => out.push_str("    "),
            ch if ch.is_control() => {}
            _ => out.push(ch),
        }
    }
    out
}

fn skip_terminal_escape<I>(chars: &mut std::iter::Peekable<I>)
where
    I: Iterator<Item = char>,
{
    match chars.next() {
        Some('[') => {
            for ch in chars.by_ref() {
                if ('@'..='~').contains(&ch) {
                    break;
                }
            }
        }
        Some(']') | Some('P') | Some('X') | Some('^') | Some('_') => {
            while let Some(ch) = chars.next() {
                if ch == '\u{7}' {
                    break;
                }
                if ch == '\u{1b}' && matches!(chars.peek(), Some('\\')) {
                    let _ = chars.next();
                    break;
                }
            }
        }
        Some(_) | None => {}
    }
}

fn handle_paste(app: &mut App, text: &str) {
    match &mut app.mode {
        Mode::AmendInput { buffer } => buffer.push_str(&sanitize_multiline_paste(text)),
        Mode::NewRunInput { draft, .. } => match draft.field {
            NewRunField::Task => {
                let text = sanitize_multiline_paste(text);
                insert_text_at_cursor(&mut draft.task, &mut draft.task_cursor, &text);
            }
            NewRunField::Workspace => {
                let text = normalize_single_line_paste(text);
                insert_text_at_cursor(&mut draft.workspace, &mut draft.workspace_cursor, &text);
            }
            NewRunField::Title => {
                let text = normalize_single_line_paste(text);
                insert_text_at_cursor(&mut draft.title, &mut draft.title_cursor, &text);
            }
            NewRunField::Start | NewRunField::Cancel => {}
        },
        Mode::InterviewInput { buffer, .. } => buffer.push_str(&sanitize_multiline_paste(text)),
        Mode::PromptReview { .. }
        | Mode::Normal
        | Mode::ConfirmDelete { .. }
        | Mode::ConfirmPrune { .. }
        | Mode::ArtifactView { .. } => {}
    }
}

fn handle_amend_key(
    ctx: &Context,
    app: &mut App,
    key: crossterm::event::KeyEvent,
) -> Result<bool, String> {
    let mut submit_note: Option<String> = None;
    let mut cancel = false;
    if let Mode::AmendInput { buffer } = &mut app.mode {
        match key.code {
            KeyCode::Esc => cancel = true,
            KeyCode::Enter => submit_note = Some(buffer.trim().to_string()),
            KeyCode::Backspace => {
                buffer.pop();
            }
            KeyCode::Char(ch) => {
                buffer.push(ch);
            }
            _ => {}
        }
    }
    if cancel {
        app.mode = Mode::Normal;
        return Ok(false);
    }
    if let Some(note) = submit_note {
        app.mode = Mode::Normal;
        if note.is_empty() {
            app.notice = "Amendment was empty".to_string();
            return Ok(false);
        }
        spawn_action(
            ctx,
            app,
            "amend",
            vec![
                "amend".to_string(),
                "--note".to_string(),
                note,
                "--rewind".to_string(),
                "intake".to_string(),
                "--auto-refresh-prompts".to_string(),
            ],
        )?;
        app.notice =
            "Amendment saved. Press n for the next stage or r/w to run the whole pipeline."
                .to_string();
    }
    Ok(false)
}

fn handle_new_run_key(
    ctx: &Context,
    app: &mut App,
    key: crossterm::event::KeyEvent,
) -> Result<bool, String> {
    let mut submit = false;
    let mut cancel = false;
    if let Mode::NewRunInput { draft, task_scroll } = &mut app.mode {
        match key.code {
            KeyCode::Esc => cancel = true,
            KeyCode::Enter => match draft.field {
                NewRunField::Task => {
                    insert_text_at_cursor(&mut draft.task, &mut draft.task_cursor, "\n")
                }
                NewRunField::Workspace => draft.field = NewRunField::Title,
                NewRunField::Title => draft.field = NewRunField::Start,
                NewRunField::Start => submit = true,
                NewRunField::Cancel => cancel = true,
            },
            KeyCode::BackTab => draft.field = draft.field.previous(),
            KeyCode::Tab | KeyCode::Down => draft.field = draft.field.next(),
            KeyCode::Up => draft.field = draft.field.previous(),
            KeyCode::PageDown => {
                if matches!(draft.field, NewRunField::Task) {
                    *task_scroll = task_scroll.saturating_add(6);
                }
            }
            KeyCode::PageUp => {
                if matches!(draft.field, NewRunField::Task) {
                    *task_scroll = task_scroll.saturating_sub(6);
                }
            }
            KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                match draft.field {
                    NewRunField::Task => {
                        draft.task.clear();
                        draft.task_cursor = 0;
                    }
                    NewRunField::Workspace => {
                        draft.workspace.clear();
                        draft.workspace_cursor = 0;
                    }
                    NewRunField::Title => {
                        draft.title.clear();
                        draft.title_cursor = 0;
                    }
                    NewRunField::Start | NewRunField::Cancel => {}
                }
            }
            KeyCode::Backspace => match draft.field {
                NewRunField::Task => backspace_at_cursor(&mut draft.task, &mut draft.task_cursor),
                NewRunField::Workspace => {
                    backspace_at_cursor(&mut draft.workspace, &mut draft.workspace_cursor)
                }
                NewRunField::Title => {
                    backspace_at_cursor(&mut draft.title, &mut draft.title_cursor)
                }
                NewRunField::Start | NewRunField::Cancel => {}
            },
            KeyCode::Left => match draft.field {
                NewRunField::Task => draft.task_cursor = draft.task_cursor.saturating_sub(1),
                NewRunField::Workspace => {
                    draft.workspace_cursor = draft.workspace_cursor.saturating_sub(1)
                }
                NewRunField::Title => draft.title_cursor = draft.title_cursor.saturating_sub(1),
                NewRunField::Cancel => draft.field = NewRunField::Start,
                NewRunField::Start => draft.field = NewRunField::Cancel,
            },
            KeyCode::Right => match draft.field {
                NewRunField::Task => {
                    draft.task_cursor = draft
                        .task_cursor
                        .saturating_add(1)
                        .min(char_count(&draft.task))
                }
                NewRunField::Workspace => {
                    draft.workspace_cursor = draft
                        .workspace_cursor
                        .saturating_add(1)
                        .min(char_count(&draft.workspace))
                }
                NewRunField::Title => {
                    draft.title_cursor = draft
                        .title_cursor
                        .saturating_add(1)
                        .min(char_count(&draft.title))
                }
                NewRunField::Start => draft.field = NewRunField::Cancel,
                NewRunField::Cancel => draft.field = NewRunField::Start,
            },
            KeyCode::Home => match draft.field {
                NewRunField::Task => draft.task_cursor = 0,
                NewRunField::Workspace => draft.workspace_cursor = 0,
                NewRunField::Title => draft.title_cursor = 0,
                NewRunField::Start | NewRunField::Cancel => {}
            },
            KeyCode::End => match draft.field {
                NewRunField::Task => draft.task_cursor = char_count(&draft.task),
                NewRunField::Workspace => draft.workspace_cursor = char_count(&draft.workspace),
                NewRunField::Title => draft.title_cursor = char_count(&draft.title),
                NewRunField::Start | NewRunField::Cancel => {}
            },
            KeyCode::Char(ch) => match draft.field {
                NewRunField::Task => {
                    insert_text_at_cursor(&mut draft.task, &mut draft.task_cursor, &ch.to_string())
                }
                NewRunField::Workspace => insert_text_at_cursor(
                    &mut draft.workspace,
                    &mut draft.workspace_cursor,
                    &ch.to_string(),
                ),
                NewRunField::Title => insert_text_at_cursor(
                    &mut draft.title,
                    &mut draft.title_cursor,
                    &ch.to_string(),
                ),
                NewRunField::Start | NewRunField::Cancel => {}
            },
            _ => {}
        }
    }
    if cancel {
        app.mode = Mode::Normal;
        return Ok(false);
    }
    if submit {
        let draft = match &app.mode {
            Mode::NewRunInput { draft, .. } => draft.clone(),
            _ => return Ok(false),
        };
        if draft.task.trim().is_empty() {
            app.notice = "New pipeline requires a task.".to_string();
            return Ok(false);
        }
        if draft.workspace.trim().is_empty() {
            app.notice = "New pipeline requires a workspace path.".to_string();
            return Ok(false);
        }
        app.mode = Mode::Normal;
        app.notice = "Generating stage0 interview questions...".to_string();
        spawn_interview_questions_job(ctx, app, draft)?;
    }
    Ok(false)
}

fn handle_interview_key(
    ctx: &Context,
    app: &mut App,
    key: crossterm::event::KeyEvent,
) -> Result<bool, String> {
    let mut cancel = false;
    let mut advance = false;
    if let Mode::InterviewInput {
        buffer,
        answer_scroll,
        ..
    } = &mut app.mode
    {
        match key.code {
            KeyCode::Esc => cancel = true,
            KeyCode::Enter | KeyCode::Down => advance = true,
            KeyCode::Up | KeyCode::BackTab => {
                if let Mode::InterviewInput {
                    answers,
                    index,
                    buffer,
                    answer_scroll,
                    ..
                } = &mut app.mode
                {
                    answers[*index].clone_from(buffer);
                    if *index > 0 {
                        *index -= 1;
                        buffer.clone_from(&answers[*index]);
                        *answer_scroll = 0;
                    }
                }
                return Ok(false);
            }
            KeyCode::PageDown => *answer_scroll = answer_scroll.saturating_add(6),
            KeyCode::PageUp => *answer_scroll = answer_scroll.saturating_sub(6),
            KeyCode::Backspace => {
                buffer.pop();
            }
            KeyCode::Char(ch) => {
                buffer.push(ch);
            }
            _ => {}
        }
    }
    if cancel {
        app.mode = Mode::Normal;
        app.wizard_selected = false;
        app.notice = "New pipeline interview cancelled.".to_string();
        return Ok(false);
    }
    if !advance {
        return Ok(false);
    }

    let mut should_finalize = false;
    let mut payload: Option<(NewRunDraft, PathBuf)> = None;

    if let Mode::InterviewInput {
        draft,
        session_dir,
        questions,
        answers,
        index,
        buffer,
        answer_scroll,
        ..
    } = &mut app.mode
    {
        let answer = buffer.trim().to_string();
        let question = &questions[*index];
        if question.required && answer.is_empty() {
            app.notice = "This question requires an answer.".to_string();
            return Ok(false);
        }
        answers[*index] = answer;
        if *index + 1 >= questions.len() {
            should_finalize = true;
            payload = Some((draft.clone(), session_dir.clone()));
        } else {
            *index += 1;
            buffer.clone_from(&answers[*index]);
            *answer_scroll = 0;
        }
    }
    if !should_finalize {
        return Ok(false);
    }

    let (draft, session_dir) =
        payload.ok_or_else(|| "Interview finalize payload was not prepared.".to_string())?;
    let qa_pairs = interview_answers(&app.mode);
    app.mode = Mode::Normal;
    app.notice = "Finalizing interview prompt...".to_string();
    spawn_interview_finalize_job(ctx, app, draft, session_dir, qa_pairs)?;
    Ok(false)
}

fn handle_prompt_review_key(
    ctx: &Context,
    app: &mut App,
    key: crossterm::event::KeyEvent,
) -> Result<bool, String> {
    let mut submit = None;
    if let Mode::PromptReview {
        selected, scroll, ..
    } = &mut app.mode
    {
        match key.code {
            _ if key_is_char(key, 'q') => return Ok(true),
            _ if key_is_char(key, 'm') => {
                toggle_mouse_capture(app)?;
                return Ok(false);
            }
            KeyCode::Esc => {
                app.mode = Mode::Normal;
                app.wizard_selected = false;
                app.notice = "Prompt review closed. No run was created.".to_string();
                return Ok(false);
            }
            KeyCode::Down => *scroll = scroll.saturating_add(1),
            _ if key_is_char(key, 'j') => *scroll = scroll.saturating_add(1),
            KeyCode::Up => *scroll = scroll.saturating_sub(1),
            _ if key_is_char(key, 'k') => *scroll = scroll.saturating_sub(1),
            KeyCode::PageDown => *scroll = scroll.saturating_add(12),
            KeyCode::PageUp => *scroll = scroll.saturating_sub(12),
            KeyCode::Left | KeyCode::BackTab => *selected = selected.previous(),
            KeyCode::Right | KeyCode::Tab => *selected = selected.next(),
            KeyCode::Enter => submit = Some(*selected),
            _ => {}
        }
    }
    if let Some(action) = submit {
        match action {
            PromptReviewAction::Cancel => {
                app.mode = Mode::Normal;
                app.notice = "Prompt review cancelled. No run was created.".to_string();
            }
            PromptReviewAction::CreateOnly | PromptReviewAction::CreateAndStart => {
                let (draft, session_dir, final_task_path) = match &app.mode {
                    Mode::PromptReview {
                        draft,
                        session_dir,
                        final_task_path,
                        ..
                    } => (draft.clone(), session_dir.clone(), final_task_path.clone()),
                    _ => return Ok(false),
                };
                let created = create_run_from_draft(
                    ctx,
                    app,
                    &draft,
                    Some(final_task_path.as_path()),
                    &session_dir,
                )?;
                app.mode = Mode::Normal;
                app.refresh(ctx)?;
                app.select_run_by_path(&created);
                if matches!(action, PromptReviewAction::CreateAndStart) {
                    app.notice = "Run created. Starting the pipeline...".to_string();
                    spawn_resume_for_run(ctx, app, created)?;
                } else {
                    app.notice =
                        "Run created. Press n for the next stage or r/w to run the whole stack."
                            .to_string();
                    app.last_output = format!(
                        "Created {}\n\nPipeline is ready. Start it from the TUI when you want.",
                        created.display()
                    );
                }
            }
        }
    }
    Ok(false)
}

fn handle_artifact_view_key(
    app: &mut App,
    key: crossterm::event::KeyEvent,
) -> Result<bool, String> {
    let mut next_mode = None;
    if let Mode::ArtifactView { kind, scroll } = app.mode {
        match key.code {
            _ if key_is_char(key, 'q') => return Ok(true),
            _ if key_is_char(key, 'm') => {
                toggle_mouse_capture(app)?;
                return Ok(false);
            }
            KeyCode::Esc => app.mode = Mode::Normal,
            KeyCode::Down => {
                next_mode = Some(Mode::ArtifactView {
                    kind,
                    scroll: scroll.saturating_add(1),
                });
            }
            _ if key_is_char(key, 'j') => {
                next_mode = Some(Mode::ArtifactView {
                    kind,
                    scroll: scroll.saturating_add(1),
                });
            }
            KeyCode::Up => {
                next_mode = Some(Mode::ArtifactView {
                    kind,
                    scroll: scroll.saturating_sub(1),
                });
            }
            _ if key_is_char(key, 'k') => {
                next_mode = Some(Mode::ArtifactView {
                    kind,
                    scroll: scroll.saturating_sub(1),
                });
            }
            KeyCode::PageDown => {
                next_mode = Some(Mode::ArtifactView {
                    kind,
                    scroll: scroll.saturating_add(12),
                });
            }
            KeyCode::PageUp => {
                next_mode = Some(Mode::ArtifactView {
                    kind,
                    scroll: scroll.saturating_sub(12),
                });
            }
            KeyCode::Left => {
                next_mode = Some(Mode::ArtifactView {
                    kind: kind.previous(),
                    scroll: 0,
                });
            }
            _ if key_is_char(key, '[') => {
                next_mode = Some(Mode::ArtifactView {
                    kind: kind.previous(),
                    scroll: 0,
                });
            }
            KeyCode::Right | KeyCode::Tab => {
                next_mode = Some(Mode::ArtifactView {
                    kind: kind.next(),
                    scroll: 0,
                });
            }
            _ if key_is_char(key, ']') => {
                next_mode = Some(Mode::ArtifactView {
                    kind: kind.next(),
                    scroll: 0,
                });
            }
            KeyCode::Char('1') => {
                next_mode = Some(Mode::ArtifactView {
                    kind: ArtifactKind::Summary,
                    scroll: 0,
                });
            }
            KeyCode::Char('2') => {
                next_mode = Some(Mode::ArtifactView {
                    kind: ArtifactKind::Findings,
                    scroll: 0,
                });
            }
            KeyCode::Char('3') => {
                next_mode = Some(Mode::ArtifactView {
                    kind: ArtifactKind::Augmented,
                    scroll: 0,
                });
            }
            KeyCode::Char('4') => {
                next_mode = Some(Mode::ArtifactView {
                    kind: ArtifactKind::Execution,
                    scroll: 0,
                });
            }
            KeyCode::Char('5') => {
                next_mode = Some(Mode::ArtifactView {
                    kind: ArtifactKind::Brief,
                    scroll: 0,
                });
            }
            _ => {}
        }
    }
    if let Some(mode) = next_mode {
        app.mode = mode;
    }
    Ok(false)
}

fn handle_delete_confirm(
    ctx: &Context,
    app: &mut App,
    key: crossterm::event::KeyEvent,
) -> Result<bool, String> {
    let mut selected = match app.mode {
        Mode::ConfirmDelete { selected } => selected,
        _ => ConfirmChoice::Cancel,
    };
    match key.code {
        _ if key_is_char(key, 'q') => return Ok(true),
        KeyCode::Esc => app.mode = Mode::Normal,
        _ if key_is_char(key, 'c') => app.mode = Mode::Normal,
        _ if key_is_char(key, 'd') || key_is_char(key, 'x') => {
            if let Some(run) = app.selected_run() {
                let run_dir = run.run_dir.clone();
                if delete_run_if_safe(app, &run_dir)? {
                    app.notice = format!("Deleted {}", run_dir.display());
                    app.last_output.clone_from(&app.notice);
                    app.mode = Mode::Normal;
                    app.refresh(ctx)?;
                } else {
                    app.mode = Mode::Normal;
                }
            } else {
                app.mode = Mode::Normal;
            }
        }
        KeyCode::Left | KeyCode::Right | KeyCode::Tab => {
            selected = selected.toggle();
            app.mode = Mode::ConfirmDelete { selected };
        }
        KeyCode::Enter => match selected {
            ConfirmChoice::Cancel => app.mode = Mode::Normal,
            ConfirmChoice::Confirm => {
                if let Some(run) = app.selected_run() {
                    let run_dir = run.run_dir.clone();
                    if delete_run_if_safe(app, &run_dir)? {
                        app.notice = format!("Deleted {}", run_dir.display());
                        app.last_output.clone_from(&app.notice);
                        app.mode = Mode::Normal;
                        app.refresh(ctx)?;
                    } else {
                        app.mode = Mode::Normal;
                    }
                } else {
                    app.mode = Mode::Normal;
                }
            }
        },
        _ => app.mode = Mode::ConfirmDelete { selected },
    }
    Ok(false)
}

fn handle_prune_confirm(
    ctx: &Context,
    app: &mut App,
    key: crossterm::event::KeyEvent,
) -> Result<bool, String> {
    let mut selected = match app.mode {
        Mode::ConfirmPrune { selected } => selected,
        _ => ConfirmChoice::Cancel,
    };
    match key.code {
        _ if key_is_char(key, 'q') => return Ok(true),
        KeyCode::Esc => app.mode = Mode::Normal,
        _ if key_is_char(key, 'c') => app.mode = Mode::Normal,
        _ if key_is_char(key, 'p') => {
            let candidates =
                choose_prune_candidates(&app.root, DEFAULT_KEEP, Some(DEFAULT_PRUNE_DAYS))?;
            let (candidates, skipped) = prune_candidates_without_active_jobs(candidates);
            let count = candidates.len();
            for run_dir in candidates {
                delete_run(&run_dir)?;
            }
            app.notice = if skipped == 0 {
                format!("Pruned {count} run(s)")
            } else {
                format!("Pruned {count} run(s); skipped {skipped} active run(s)")
            };
            app.last_output = format!(
                "Deleted runs older than {DEFAULT_PRUNE_DAYS} days while keeping the newest {DEFAULT_KEEP}."
            );
            app.mode = Mode::Normal;
            app.refresh(ctx)?;
        }
        KeyCode::Left | KeyCode::Right | KeyCode::Tab => {
            selected = selected.toggle();
            app.mode = Mode::ConfirmPrune { selected };
        }
        KeyCode::Enter => match selected {
            ConfirmChoice::Cancel => app.mode = Mode::Normal,
            ConfirmChoice::Confirm => {
                let candidates =
                    choose_prune_candidates(&app.root, DEFAULT_KEEP, Some(DEFAULT_PRUNE_DAYS))?;
                let (candidates, skipped) = prune_candidates_without_active_jobs(candidates);
                let count = candidates.len();
                for run_dir in candidates {
                    delete_run(&run_dir)?;
                }
                app.notice = if skipped == 0 {
                    format!("Pruned {count} run(s)")
                } else {
                    format!("Pruned {count} run(s); skipped {skipped} active run(s)")
                };
                app.last_output = format!(
                    "Deleted runs older than {DEFAULT_PRUNE_DAYS} days while keeping the newest {DEFAULT_KEEP}."
                );
                app.mode = Mode::Normal;
                app.refresh(ctx)?;
            }
        },
        _ => app.mode = Mode::ConfirmPrune { selected },
    }
    Ok(false)
}

fn spawn_action(
    ctx: &Context,
    app: &mut App,
    label: &str,
    args: Vec<String>,
) -> Result<(), String> {
    let run_dir = match app.selected_run() {
        Some(run) => run.run_dir.clone(),
        None => {
            app.notice = "No selected run".to_string();
            return Ok(());
        }
    };
    if !ensure_job_slot_for_run(app, &run_dir) {
        return Ok(());
    }
    let label_text = label.to_string();
    let command_hint = match label {
        "start-next" => "start-next".to_string(),
        "safe-next" => "safe-next-action".to_string(),
        "resume" => "resume until verification".to_string(),
        other => other.to_string(),
    };
    let log_hint = app
        .selected_run()
        .and_then(|run| infer_log_hint(label, run));
    let action_label = log_hint.clone().unwrap_or_else(|| label.to_string());
    app.notice = format!(
        "Running {action_label} for {}",
        run_dir
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("run")
    );
    app.last_output.clear();
    let args_for_log = args.join(" ");
    let run_dir_for_job = run_dir.clone();
    let label_for_job = label_text.clone();
    let command_hint_for_job = command_hint.clone();
    let ctx_for_job = ctx.clone();
    spawn_engine_job(
        ctx,
        app,
        JobKind::RunAction,
        label_text,
        run_dir,
        log_hint,
        command_hint,
        move |_| {
            let _ = runtime::append_process_line(
                &run_dir_for_job,
                &format!("Args: {}", args_for_log.trim()),
            );
            match label_for_job.as_str() {
                "start-next" => {
                    run_stage_capture(&ctx_for_job, &run_dir_for_job, "start-next", &[])
                }
                "rerun" => run_stage_capture(&ctx_for_job, &run_dir_for_job, "rerun", &[]),
                "host-probe" => {
                    run_stage_capture(&ctx_for_job, &run_dir_for_job, "host-probe", &["--refresh"])
                }
                "refresh-prompts" => {
                    run_stage_capture(&ctx_for_job, &run_dir_for_job, "refresh-prompts", &[])
                }
                "step-back review" => {
                    run_stage_capture(&ctx_for_job, &run_dir_for_job, "step-back", &["review"])
                }
                "recheck verification" => {
                    run_stage_capture(&ctx_for_job, &run_dir_for_job, "recheck", &["verification"])
                }
                "safe-next" => execute_safe_next_action(&ctx_for_job, &run_dir_for_job),
                "resume" => automate_run(&ctx_for_job, &run_dir_for_job, "verification", true),
                "amend" => {
                    let mut note = String::new();
                    let mut rewind = "none".to_string();
                    let mut auto_refresh = false;
                    let mut index = 1usize;
                    while index < args.len() {
                        match args[index].as_str() {
                            "--note" => {
                                index += 1;
                                note = args.get(index).cloned().unwrap_or_default();
                            }
                            "--rewind" => {
                                index += 1;
                                rewind = args
                                    .get(index)
                                    .cloned()
                                    .unwrap_or_else(|| "none".to_string());
                            }
                            "--auto-refresh-prompts" => auto_refresh = true,
                            _ => {}
                        }
                        index += 1;
                    }
                    amend_run(&ctx_for_job, &run_dir_for_job, &note, &rewind, auto_refresh)
                }
                other => Err(format!(
                    "Unsupported in-process action: {other} ({command_hint_for_job})"
                )),
            }
        },
    )
}

fn poll_tracked_job(job: &mut RunningJob, runs: &[RunSnapshot]) -> Option<FinishedJob> {
    let mut finished = None;
    {
        if let Some(rx) = job.stream_rx.as_ref() {
            while rx.try_recv().is_ok() {}
        }
        let fresh_lines = runtime::tail_process_log(&job.run_dir, 40);
        if !fresh_lines.is_empty() {
            job.stream_lines = fresh_lines;
        }

        let mut completion = None;
        let mut completion_channel_closed = false;
        if let Some(rx) = job.completion_rx.as_ref() {
            match rx.try_recv() {
                Ok(result) => completion = Some(result),
                Err(TryRecvError::Disconnected) => completion_channel_closed = true,
                Err(TryRecvError::Empty) => {}
            }
        }
        if completion.is_none() {
            if let Some(state) = runtime::load_job_state(&job.run_dir) {
                if state.is_active() {
                    job.refresh_from_state(state.clone());
                    let run = runs.iter().find(|run| run.run_dir == job.run_dir);
                    reconcile_job_stage_with_run(job, run);
                }
                if state.exit_code.is_some() || !state.is_active() {
                    let code = state.exit_code.unwrap_or(match state.status.as_str() {
                        "completed" => 0,
                        "interrupted" => 130,
                        _ => 1,
                    });
                    completion = Some(JobResult {
                        code,
                        stdout: String::new(),
                        stderr: state.message.unwrap_or_default(),
                    });
                }
            } else if (job.completion_rx.is_none() || completion_channel_closed)
                && runtime::active_job_state(&job.run_dir).is_none()
            {
                finished = Some((
                    job.kind.clone(),
                    job.label.clone(),
                    job.run_dir.clone(),
                    job.log_hint.clone(),
                    JobResult {
                        code: 1,
                        stdout: String::new(),
                        stderr: String::new(),
                    },
                    true,
                ));
            }
        }
        if completion.is_none() && job.last_heartbeat.elapsed() >= Duration::from_secs(1) {
            let state = runtime::load_job_state(&job.run_dir);
            if state
                .as_ref()
                .map(|item| item.is_active() && (item.pid > 0 || item.pgid > 0))
                .unwrap_or(false)
            {
                let status = if let Some(run) = runs.iter().find(|run| run.run_dir == job.run_dir) {
                    if job_stalled(run, job) {
                        "stalled"
                    } else {
                        "running"
                    }
                } else {
                    "running"
                };
                let _ = runtime::touch_job(&job.run_dir, status);
            }
            job.last_heartbeat = Instant::now();
        }
        if let Some(result) = completion {
            finished = Some((
                job.kind.clone(),
                job.label.clone(),
                job.run_dir.clone(),
                job.log_hint.clone(),
                result,
                false,
            ));
        }
    }
    finished.map(
        |(kind, label, run_dir, completed_log_hint, result, detached_finish)| FinishedJob {
            kind,
            label,
            run_dir,
            completed_log_hint,
            result,
            detached_finish,
        },
    )
}

fn handle_finished_job(ctx: &Context, app: &mut App, finished: FinishedJob) -> Result<(), String> {
    let FinishedJob {
        kind,
        label,
        run_dir,
        completed_log_hint,
        result,
        detached_finish,
    } = finished;
    {
        let output_lines = runtime::tail_process_log(&run_dir, 80);
        let raw_output = output_lines.join("\n");
        let exit_code = result.code;
        let combined_output = match (result.stdout.trim(), result.stderr.trim()) {
            ("", "") => raw_output.clone(),
            ("", stderr) => stderr.to_string(),
            (stdout, "") => stdout.to_string(),
            (stdout, stderr) => format!("{stdout}\n\n{stderr}"),
        };
        app.last_output = if combined_output.trim().is_empty() {
            if detached_finish {
                format!(
                    "Tracked job `{label}` is no longer alive. Inspect run artifacts and logs under {}.",
                    run_dir.display()
                )
            } else {
                format!("{label} finished.")
            }
        } else {
            combined_output
        };
        app.notice = match exit_code {
            0 => format!("{label} completed"),
            130 => format!("{label} interrupted"),
            code => format!("{label} failed with exit code {code}"),
        };
        match kind {
            JobKind::RunAction => {
                app.refresh(ctx)?;
                if exit_code == 0 && label == "rerun" {
                    if let Some(created) = rerun_created_run_dir(&result.stdout) {
                        app.select_run_by_path(&created);
                        app.notice = format!(
                            "Follow-up run created: {}",
                            created
                                .file_name()
                                .and_then(|value| value.to_str())
                                .unwrap_or("run")
                        );
                    }
                }
                if let Some(updated) = app.runs.iter_mut().find(|item| item.run_dir == run_dir) {
                    let (log_title, log_lines) = contextual_log_excerpt(
                        &run_dir,
                        completed_log_hint.as_deref(),
                        Some(&updated.status.next),
                        12,
                    );
                    updated.log_title = log_title;
                    updated.log_lines = log_lines;
                }
            }
            JobKind::InterviewQuestions { draft } => {
                if exit_code == 0 {
                    match parse_job_json::<InterviewQuestionsPayload>(&result.stdout, &raw_output) {
                        Ok(payload) => {
                            if payload.questions.is_empty() {
                                let created =
                                    create_run_from_draft(ctx, app, &draft, None, Path::new(""))?;
                                app.refresh(ctx)?;
                                app.select_run_by_path(&created);
                                app.notice =
                                    "Run created. Press n for the next stage or r/w to run the whole stack."
                                        .to_string();
                                app.last_output = format!(
                                    "Created {}\n\nThe pipeline has not been started yet.",
                                    created.display()
                                );
                            } else {
                                let questions = payload.questions;
                                app.mode = Mode::InterviewInput {
                                    draft,
                                    session_dir: PathBuf::from(payload.session_dir),
                                    goal_summary: payload.goal_summary,
                                    answers: vec![String::new(); questions.len()],
                                    questions,
                                    index: 0,
                                    buffer: String::new(),
                                    answer_scroll: 0,
                                };
                                app.notice = "Stage0 interview questions are ready.".to_string();
                            }
                        }
                        Err(err) => {
                            app.mode = Mode::NewRunInput {
                                draft,
                                task_scroll: 0,
                            };
                            app.notice = format!("Could not parse interview questions JSON: {err}");
                        }
                    }
                } else {
                    app.mode = Mode::NewRunInput {
                        draft,
                        task_scroll: 0,
                    };
                }
            }
            JobKind::InterviewFinalize {
                draft,
                session_dir,
                answers,
            } => {
                if exit_code == 0 {
                    match parse_job_json::<InterviewFinalizePayload>(&result.stdout, &raw_output) {
                        Ok(payload) => {
                            app.mode = Mode::PromptReview {
                                draft,
                                session_dir,
                                final_task_path: PathBuf::from(payload.final_task_path),
                                scroll: 0,
                                selected: PromptReviewAction::CreateOnly,
                            };
                            app.notice =
                                "Review the final task prompt, then create the run explicitly."
                                    .to_string();
                        }
                        Err(err) => {
                            app.mode = Mode::InterviewInput {
                                draft,
                                session_dir,
                                goal_summary: format!(
                                    "Could not finalize the interview prompt: {err}"
                                ),
                                questions: Vec::new(),
                                answers: answers
                                    .iter()
                                    .map(|value| {
                                        value
                                            .get("answer")
                                            .and_then(|item| item.as_str())
                                            .unwrap_or_default()
                                            .to_string()
                                    })
                                    .collect(),
                                index: 0,
                                buffer: String::new(),
                                answer_scroll: 0,
                            };
                            app.notice = format!("Could not parse interview finalize JSON: {err}");
                        }
                    }
                } else {
                    app.mode = Mode::NewRunInput {
                        draft,
                        task_scroll: 0,
                    };
                }
            }
        }
    }
    Ok(())
}

fn poll_job(ctx: &Context, app: &mut App) -> Result<(), String> {
    let run_finished = app
        .job
        .as_mut()
        .and_then(|job| poll_tracked_job(job, &app.runs));
    if run_finished.is_some() {
        app.job = None;
    }
    if let Some(finished) = run_finished {
        handle_finished_job(ctx, app, finished)?;
    }

    let wizard_finished = app
        .wizard_job
        .as_mut()
        .and_then(|job| poll_tracked_job(job, &app.runs));
    if wizard_finished.is_some() {
        app.wizard_job = None;
    }
    if let Some(finished) = wizard_finished {
        handle_finished_job(ctx, app, finished)?;
    }
    Ok(())
}

fn interrupt_job(app: &mut App) -> Result<(), String> {
    let job = if app.wizard_selected {
        app.wizard_job.as_mut().or(app.job.as_mut())
    } else {
        app.job.as_mut().or(app.wizard_job.as_mut())
    };
    let Some(job) = job else {
        app.notice = "No running action to interrupt.".to_string();
        return Ok(());
    };
    runtime::request_interrupt(&job.run_dir)?;
    if job.pgid <= 0 {
        app.notice = format!("Interrupt requested for {}", job.label);
        app.last_output =
            "Cancellation is queued. The job has not published a live child process yet."
                .to_string();
        return Ok(());
    }
    if let Err(err) = runtime::interrupt_process_group(job.pgid) {
        app.notice = format!("Could not interrupt {}", job.label);
        app.last_output = err;
        return Ok(());
    }
    app.notice = format!("Interrupt requested for {}", job.label);
    app.last_output = format!(
        "Interrupt requested for `{}` in {}.",
        job.command_hint,
        job.run_dir
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("run")
    );
    Ok(())
}

fn draw(frame: &mut ratatui::Frame<'_>, app: &App) {
    let size = frame.size();
    let layout = root_layout(size);

    draw_header(frame, layout[0], app);
    draw_status_bar(frame, layout[1], app);
    draw_body(frame, layout[2], app);
    draw_logs(frame, layout[3], app);
    draw_footer(frame, layout[4], app);

    match &app.mode {
        Mode::AmendInput { buffer } => draw_amend_popup(frame, size, buffer),
        Mode::NewRunInput { draft, task_scroll } => {
            draw_new_run_popup(frame, size, draft, *task_scroll);
            if let Some((x, y)) = new_run_cursor_position(size, draft, *task_scroll) {
                frame.set_cursor(x, y);
            }
        }
        Mode::InterviewInput {
            goal_summary,
            questions,
            answers,
            index,
            buffer,
            answer_scroll,
            ..
        } => {
            draw_interview_popup(
                frame,
                size,
                goal_summary,
                questions,
                answers,
                *index,
                buffer,
                *answer_scroll,
            );
            if let Some((x, y)) = interview_cursor_position(size, buffer, *answer_scroll) {
                frame.set_cursor(x, y);
            }
        }
        Mode::PromptReview {
            final_task_path,
            scroll,
            selected,
            ..
        } => draw_prompt_review_popup(frame, size, final_task_path, *scroll, *selected),
        Mode::ConfirmDelete { selected } => draw_confirm_popup(
            frame,
            size,
            "Delete Run",
            "Delete the selected run?",
            *selected,
            "Delete",
        ),
        Mode::ConfirmPrune { selected } => draw_confirm_popup(
            frame,
            size,
            "Prune Runs",
            "Delete runs older than 14 days while keeping the newest 20?",
            *selected,
            "Prune",
        ),
        Mode::ArtifactView { kind, scroll } => {
            draw_artifact_popup(frame, size, app, *kind, *scroll)
        }
        Mode::Normal => {}
    }
}

fn draw_header(frame: &mut ratatui::Frame<'_>, area: Rect, app: &App) {
    let running_job = if app.wizard_selected {
        app.wizard_job.as_ref().or(app.job.as_ref())
    } else {
        app.job.as_ref().or(app.wizard_job.as_ref())
    };
    let running = running_job
        .map(|job| {
            format!(
                " | running: {}",
                running_job_display_label(app.selected_run(), job)
            )
        })
        .unwrap_or_default();
    let lines = vec![
        Line::from(vec![Span::styled(
            "agpipe",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )]),
        Line::from(format!("notice: {}{}", app.notice, running)),
    ];
    let paragraph = Paragraph::new(lines).block(Block::default().borders(Borders::BOTTOM));
    frame.render_widget(paragraph, area);
}

fn footer_shortcuts(app: &App) -> &'static str {
    match app.mode {
        Mode::ArtifactView { .. } => {
            "q quit  Esc close  j/k scroll  PgUp/PgDn scroll  [ ] switch  m mouse-select/scroll"
        }
        Mode::ConfirmDelete { .. } => "Esc cancel  Enter apply  Left/Right switch",
        Mode::ConfirmPrune { .. } => "Esc cancel  Enter apply  Left/Right switch",
        Mode::AmendInput { .. } => "Esc cancel  Enter save+rewind",
        Mode::NewRunInput { .. } => "Tab switch fields  Enter edit/apply  PgUp/PgDn scroll  Esc cancel",
        Mode::InterviewInput { .. } => "Enter next  Up previous  PgUp/PgDn scroll  Esc cancel",
        Mode::PromptReview { .. } => {
            "Enter apply  Tab switch action  j/k scroll  m mouse-select/scroll  Esc cancel"
        }
        Mode::Normal => {
            "q quit  Esc clear  j/k move  m mouse-select/scroll  Enter open  c create  n next  r/w run-all  a amend  i interrupt"
        }
    }
}

fn draw_footer(frame: &mut ratatui::Frame<'_>, area: Rect, app: &App) {
    let paragraph = Paragraph::new(footer_shortcuts(app))
        .style(Style::default().bg(Color::DarkGray).fg(Color::White));
    frame.render_widget(paragraph, area);
}

fn draw_status_bar(frame: &mut ratatui::Frame<'_>, area: Rect, app: &App) {
    let text = if app.wizard_selected {
        if let Some(job) = app.wizard_job() {
            format!(
                "run=creating-pipeline | stage0={} | elapsed={}s | status=running",
                job.command_hint,
                job.started_at.elapsed().as_secs()
            )
        } else {
            "No run selected".to_string()
        }
    } else if let Some(run) = app.selected_run() {
        let run_name = run
            .run_dir
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("run");
        let running = app
            .job
            .as_ref()
            .map(|job| {
                if job.run_dir == run.run_dir {
                    if job_stalled(run, job) {
                        format!(
                            " | running={} | stalled",
                            running_job_display_label(Some(run), job)
                        )
                    } else {
                        format!(" | running={}", running_job_display_label(Some(run), job))
                    }
                } else {
                    format!(" | background={}", running_job_label(job))
                }
            })
            .unwrap_or_default();
        let wizard = app
            .wizard_job()
            .map(|job| format!(" | wizard={}", running_job_label(job)))
            .unwrap_or_default();
        format!(
            "run={run_name} | health={} | verification={} | verdict={} | next={}{} | host={}{}{}{}",
            run.doctor.health,
            ui_verification_state(run),
            ui_goal_state(run),
            run.doctor.next,
            run.doctor
                .last_attempt
                .as_ref()
                .map(|attempt| format!(" | last={}({})", attempt.stage, attempt.status))
                .unwrap_or_default(),
            run.doctor.host_probe,
            status_bar_tokens(&run.token_summary),
            running,
            wizard
        )
    } else {
        "No run selected".to_string()
    };
    let paragraph =
        Paragraph::new(text).style(Style::default().bg(Color::DarkGray).fg(Color::White));
    frame.render_widget(paragraph, area);
}

fn draw_body(frame: &mut ratatui::Frame<'_>, _area: Rect, app: &App) {
    let size = frame.size();
    let layout = root_layout(size);
    let body = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(32), Constraint::Percentage(68)])
        .split(layout[2]);
    let rects = normal_mode_rects(size);
    draw_runs(frame, rects.runs, app);
    draw_details(frame, body[1], app);
}

fn draw_runs(frame: &mut ratatui::Frame<'_>, area: Rect, app: &App) {
    let mut items: Vec<ListItem<'_>> = Vec::new();
    if let Some(job) = app.wizard_job() {
        let line1 = "Creating Pipeline".to_string();
        let line2 = format!("stage0 | step={}", job.command_hint);
        let line3 = if job_stalled_without_run(job) {
            format!(
                "status=running | elapsed={}s | stalled",
                job.started_at.elapsed().as_secs()
            )
        } else {
            format!(
                "status=running | elapsed={}s",
                job.started_at.elapsed().as_secs()
            )
        };
        items.push(ListItem::new(vec![
            Line::from(line1),
            Line::from(line2),
            Line::from(line3),
        ]));
    }
    if app.runs.is_empty() && items.is_empty() {
        items.push(ListItem::new("No runs found."));
    } else {
        items.extend(app.runs.iter().map(|run| {
            let run_name = run
                .run_dir
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("run");
            let running = app
                .job
                .as_ref()
                .filter(|job| job.run_dir == run.run_dir)
                .map(|job| {
                    if job_stalled(run, job) {
                        format!(
                            " | running={} | stalled",
                            running_job_display_label(Some(run), job)
                        )
                    } else {
                        format!(" | running={}", running_job_display_label(Some(run), job))
                    }
                })
                .unwrap_or_default();
            let line1 = run_name.to_string();
            let (line2, base_line3) = run_list_detail_lines(run);
            let line3 = format!("{base_line3}{running}");
            ListItem::new(vec![
                Line::from(line1),
                Line::from(line2),
                Line::from(line3),
            ])
        }));
    }
    let list = List::new(items)
        .block(
            Block::default()
                .title("Runs (health | next | verify | verdict)")
                .borders(Borders::ALL),
        )
        .highlight_style(
            Style::default()
                .bg(Color::Blue)
                .fg(Color::Black)
                .add_modifier(Modifier::BOLD),
        )
        .highlight_symbol(">> ");
    let mut state = ListState::default();
    if !app.runs.is_empty() || app.wizard_job().is_some() {
        let selection = if app.wizard_selected && app.wizard_job().is_some() {
            0
        } else if app.wizard_job().is_some() {
            app.selected.saturating_add(1)
        } else {
            app.selected
        };
        state.select(Some(selection));
    }
    frame.render_stateful_widget(list, area, &mut state);
}

fn draw_details(frame: &mut ratatui::Frame<'_>, area: Rect, app: &App) {
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(58), Constraint::Percentage(42)])
        .split(area);
    if app.selected_run().is_none() {
        if let Some(job) = app.wizard_job() {
            let preview_text = format!(
                "Creating a new pipeline.\n\nCurrent step: {}\nElapsed: {}s\n\nThe run list will update only after stage0 finishes building the final task prompt and create-run succeeds.\n\nUse the lower pane for process output. Press `i` to interrupt if this stays stuck.",
                job.command_hint,
                job.started_at.elapsed().as_secs(),
            );
            let detail_text = format!(
                "Wizard state: stage0\nProcess: {}\nJob dir: {}\n\nCurrent behavior:\n- no new run exists yet\n- interview artifacts are being written under _interviews/\n- the new run appears only after interview-finalize completes",
                job.command_hint,
                job.run_dir.display(),
            );
            let preview_scroll =
                clamp_scroll_for_text(&preview_text, vertical[0], app.preview_scroll);
            let preview = Paragraph::new(preview_text)
                .block(
                    Block::default()
                        .title("Creating Pipeline")
                        .borders(Borders::ALL),
                )
                .scroll((preview_scroll, 0))
                .wrap(Wrap { trim: false });
            let detail = Paragraph::new(detail_text)
                .block(Block::default().title("Activity").borders(Borders::ALL))
                .wrap(Wrap { trim: false });
            frame.render_widget(preview, vertical[0]);
            frame.render_widget(detail, vertical[1]);
            return;
        }
    }
    let lower = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(56), Constraint::Percentage(44)])
        .split(vertical[1]);
    let (detail_text, action_text, preview_title, preview_text) =
        if let Some(run) = app.selected_run() {
            let mut lines = vec![
                format!("Run: {}", run.run_dir.display()),
                format!("Health: {}", run.doctor.health),
                format!("Verification: {}", ui_verification_state(run)),
                format!("Verdict: {}", ui_goal_state(run)),
                format!("Next: {}", run.doctor.next),
                format!("Safe action: {}", run.doctor.safe_next_action),
                format!("Host probe: {}", run.doctor.host_probe),
                format!("Tokens: {}", run_token_summary_line(&run.token_summary)),
            ];
            if let Some(attempt) = &run.doctor.last_attempt {
                lines.push(format!(
                    "Last attempt: {} via {} -> {}",
                    attempt.stage, attempt.label, attempt.status
                ));
                if !attempt.message.trim().is_empty() {
                    lines.push(format!("Last attempt detail: {}", attempt.message));
                }
            }
            if let Some(drift) = &run.doctor.host_drift {
                lines.push(format!("Host drift: {drift}"));
            }
            if !run.doctor.stale.is_empty() {
                lines.push(format!("Stale: {}", run.doctor.stale.join(", ")));
            }
            if !run.doctor.issues.is_empty() {
                lines.push(String::new());
                lines.push("Issues:".to_string());
                for issue in &run.doctor.issues {
                    lines.push(format!("- {}", issue.message));
                }
            }
            if !run.doctor.warnings.is_empty() {
                lines.push(String::new());
                lines.push("Warnings:".to_string());
                for warning in &run.doctor.warnings {
                    lines.push(format!("- {}", warning.message));
                }
            }
            if let Some(job) = &app.job {
                if job.run_dir != run.run_dir {
                    lines.push(String::new());
                    lines.push(format!(
                        "Background job: {} for {}",
                        running_job_label(job),
                        job.run_dir
                            .file_name()
                            .and_then(|value| value.to_str())
                            .unwrap_or("wizard")
                    ));
                }
            }
            if let Some(job) = app.wizard_job() {
                lines.push(String::new());
                lines.push(format!(
                    "Background job: {} for creating-pipeline",
                    running_job_label(job)
                ));
            }
            let (preview_title, preview_text) = live_preview(app, run);
            let preview_title = if preview_title == "Summary" {
                summary_title(run).to_string()
            } else {
                preview_title
            };
            let action_text = action_panel_text(app, Some(run));
            (lines.join("\n"), action_text, preview_title, preview_text)
        } else {
            (
                "No run selected.".to_string(),
                action_panel_text(app, None),
                "Preview".to_string(),
                "No substantive artifact is available yet.".to_string(),
            )
        };
    let preview_scroll = clamp_scroll_for_text(&preview_text, vertical[0], app.preview_scroll);
    let preview = Paragraph::new(preview_text)
        .block(Block::default().title(preview_title).borders(Borders::ALL))
        .scroll((preview_scroll, 0))
        .wrap(Wrap { trim: false });
    let detail = Paragraph::new(detail_text)
        .block(Block::default().title("Run Facts").borders(Borders::ALL))
        .wrap(Wrap { trim: false });
    let actions = Paragraph::new(action_text)
        .block(Block::default().title("Actions").borders(Borders::ALL))
        .wrap(Wrap { trim: false });
    frame.render_widget(preview, vertical[0]);
    frame.render_widget(detail, lower[0]);
    frame.render_widget(actions, lower[1]);
}

fn draw_logs(frame: &mut ratatui::Frame<'_>, area: Rect, app: &App) {
    let (title, content) = if let Some(job) = app.wizard_job().filter(|_| app.wizard_selected) {
        let persisted = runtime::tail_process_log(&job.run_dir, 20);
        let mut lines = vec![
            format!("Stage0: {}", job.command_hint),
            format!("Job dir: {}", job.run_dir.display()),
            String::new(),
        ];
        if !job.stream_lines.is_empty() {
            lines.push("Process output:".to_string());
            lines.extend(job.stream_lines.iter().cloned());
            lines.push(String::new());
        }
        if persisted.is_empty() {
            lines.push("Waiting for fresh stage0 output.".to_string());
        } else {
            lines.extend(persisted);
        }
        (
            format!("Running: {}", running_job_label(job)),
            lines.join("\n"),
        )
    } else if let Some(job) = &app.job {
        if let Some(run) = app.selected_run() {
            if job.run_dir == run.run_dir {
                let (log_title, log_lines) = live_log_excerpt(run, job);
                let run_name = run
                    .run_dir
                    .file_name()
                    .and_then(|s| s.to_str())
                    .unwrap_or("run");
                let mut lines = vec![
                    format!(
                        "Stage: {}{}",
                        job_display_label(Some(run), job),
                        if job.attached { " (attached)" } else { "" }
                    ),
                    format!("Run: {run_name}"),
                    format!("Log source: {log_title}"),
                    String::new(),
                ];
                if let Some(launcher) = job_launcher_label(job) {
                    lines.insert(1, format!("Launcher: {launcher}"));
                }
                if !job.stream_lines.is_empty() {
                    lines.push("Process output:".to_string());
                    lines.extend(job.stream_lines.iter().cloned());
                    lines.push(String::new());
                }
                if job_stalled(run, job) {
                    lines.push(
                        "No fresh output for a while. This stage may be stalled.".to_string(),
                    );
                    lines.push("Press i to interrupt the current action.".to_string());
                    lines.push(String::new());
                }
                lines.extend(log_lines);
                (
                    format!("Running: {}", running_job_display_label(Some(run), job)),
                    lines.join("\n"),
                )
            } else {
                let run_name = run
                    .run_dir
                    .file_name()
                    .and_then(|s| s.to_str())
                    .unwrap_or("run");
                let bg_name = job
                    .run_dir
                    .file_name()
                    .and_then(|s| s.to_str())
                    .unwrap_or("wizard");
                let (log_title, log_lines) = (run.log_title.clone(), run.log_lines.clone());
                let mut lines = vec![
                    format!("Background job: {} for {}", running_job_label(job), bg_name),
                    format!("Selected run: {run_name}"),
                    format!("Log source: {log_title}"),
                    String::new(),
                ];
                if job_stalled_without_run(job)
                    || app
                        .runs
                        .iter()
                        .find(|candidate| candidate.run_dir == job.run_dir)
                        .map(|job_run| job_stalled(job_run, job))
                        .unwrap_or(false)
                {
                    lines.push(
                        "Background job may be stalled. Press i to interrupt it.".to_string(),
                    );
                    lines.push(String::new());
                }
                lines.extend(log_lines);
                (format!("Log tail: {log_title}"), lines.join("\n"))
            }
        } else {
            let persisted = runtime::tail_process_log(&job.run_dir, 20);
            if persisted.is_empty() {
                (
                    "Running".to_string(),
                    format!("{} is running in the background.", running_job_label(job)),
                )
            } else {
                (
                    format!("Running: {}", running_job_label(job)),
                    persisted.join("\n"),
                )
            }
        }
    } else if !app.last_output.trim().is_empty() {
        ("Last action".to_string(), app.last_output.clone())
    } else if let Some(run) = app.selected_run() {
        let (log_title, log_lines) = if let Some(job) = &app.job {
            if job.run_dir == run.run_dir {
                contextual_log_excerpt(
                    &run.run_dir,
                    job.log_hint.as_deref(),
                    Some(&run.status.next),
                    12,
                )
            } else {
                (run.log_title.clone(), run.log_lines.clone())
            }
        } else {
            (run.log_title.clone(), run.log_lines.clone())
        };
        (format!("Log tail: {}", log_title), log_lines.join("\n"))
    } else {
        ("Logs".to_string(), "No log files yet.".to_string())
    };
    let scroll = clamp_scroll_for_text(&content, area, app.log_scroll);
    let logs = Paragraph::new(content)
        .block(Block::default().title(title).borders(Borders::ALL))
        .scroll((scroll, 0))
        .wrap(Wrap { trim: false });
    frame.render_widget(logs, area);
}

fn job_stalled_without_run(job: &RunningJob) -> bool {
    if job.started_at.elapsed() < Duration::from_secs(20) {
        return false;
    }
    let Ok(meta) = std::fs::metadata(&job.process_log) else {
        return job.started_at.elapsed() >= Duration::from_secs(30);
    };
    let Ok(modified) = meta.modified() else {
        return job.started_at.elapsed() >= Duration::from_secs(30);
    };
    modified < SystemTime::now() - Duration::from_secs(15)
}

fn pipeline_kind_is_solver(kind: &str) -> bool {
    matches!(
        kind.trim().to_ascii_lowercase().as_str(),
        "solver" | "research" | "analysis" | "researcher"
    )
}

fn solver_batch_stage_ids(run: &RunSnapshot) -> Vec<String> {
    let plan_path = run.run_dir.join("plan.json");
    let Ok(text) = std::fs::read_to_string(plan_path) else {
        return Vec::new();
    };
    let Ok(plan) = serde_json::from_str::<TuiPlanSummary>(&text) else {
        return Vec::new();
    };
    plan.pipeline
        .stages
        .into_iter()
        .filter(|stage| pipeline_kind_is_solver(&stage.kind))
        .map(|stage| stage.id)
        .filter(|id| {
            run.doctor
                .stages
                .get(id)
                .or_else(|| run.status.stages.get(id))
                .map(|state| state != "done")
                .unwrap_or(true)
        })
        .collect()
}

fn parallel_solver_batch_ids(run: &RunSnapshot, job: &RunningJob) -> Vec<String> {
    if is_rerun_job(job) {
        return Vec::new();
    }
    if !(job.command_hint == "start-solvers" || job.command_hint.starts_with("resume")) {
        return Vec::new();
    }
    let ids = solver_batch_stage_ids(run);
    if ids.len() > 1 {
        ids
    } else {
        Vec::new()
    }
}

fn job_stage_label(job: &RunningJob) -> String {
    if is_rerun_job(job) {
        "follow-up run".to_string()
    } else if let Some(stage) = &job.log_hint {
        stage.clone()
    } else {
        job.label.clone()
    }
}

fn job_launcher_label(job: &RunningJob) -> Option<String> {
    if is_rerun_job(job) {
        return Some(job.command_hint.clone());
    }
    let stage = job_stage_label(job);
    if job.command_hint == stage {
        None
    } else {
        Some(job.command_hint.clone())
    }
}

fn running_job_label(job: &RunningJob) -> String {
    let elapsed = job.started_at.elapsed().as_secs();
    format!("{} ({elapsed}s)", job_stage_label(job))
}

fn job_display_label(run: Option<&RunSnapshot>, job: &RunningJob) -> String {
    if let Some(run) = run {
        if !parallel_solver_batch_ids(run, job).is_empty() {
            return "solvers (parallel)".to_string();
        }
    }
    job_stage_label(job)
}

fn running_job_display_label(run: Option<&RunSnapshot>, job: &RunningJob) -> String {
    let elapsed = job.started_at.elapsed().as_secs();
    format!("{} ({elapsed}s)", job_display_label(run, job))
}

fn live_preview(app: &App, run: &RunSnapshot) -> (String, String) {
    if let Some(job) = &app.job {
        if job.run_dir == run.run_dir {
            if is_rerun_job(job) {
                return (
                    if job_stalled(run, job) {
                        "Live status (stalled)".to_string()
                    } else {
                        "Live status".to_string()
                    },
                    format!(
                        "Creating a follow-up run from this verified source run.\n\nLauncher command: {}\nElapsed: {}s\n\nSource run state:\n- Verification: {}\n- Verdict: {}\n- Safe action: {}\n\nThe source run itself is not re-executing pipeline stages. A new follow-up run directory will appear separately and will become selected after creation completes.\n\n{}",
                        job.command_hint,
                        job.started_at.elapsed().as_secs(),
                        ui_verification_state(run),
                        ui_goal_state(run),
                        run.doctor.safe_next_action,
                        if job_stalled(run, job) {
                            "No fresh output detected for the follow-up creation job. Press `i` to interrupt if it stays stuck."
                        } else {
                            "Waiting for follow-up run creation output."
                        },
                    ),
                );
            }
            let stage = job_display_label(Some(run), job);
            let batch_ids = parallel_solver_batch_ids(run, job);
            let launcher = job_launcher_label(job)
                .map(|value| format!("\nLauncher command: {value}"))
                .unwrap_or_default();
            let live_log_stage = job.log_hint.as_deref().unwrap_or(stage.as_str());
            let live_path = run
                .run_dir
                .join("logs")
                .join(format!("{live_log_stage}.last.md"));
            if is_fresh_for_job(&live_path, job) {
                if let Ok(content) = std::fs::read_to_string(&live_path) {
                    let trimmed = content.trim();
                    if !trimmed.is_empty() {
                        return (format!("Live {stage}"), trimmed.to_string());
                    }
                }
            }
            return (
                if job_stalled(run, job) {
                    "Live status (stalled)".to_string()
                } else {
                    "Live status".to_string()
                },
                format!(
                    "Stage `{stage}` is currently running.{launcher}\nElapsed: {}s\n{}\nCurrent next field: {}\nVerification stage: {}\nVerification verdict: {}\nSafe action: {}\n\n{}\n\nUse the lower log pane for live agent output.",
                    job.started_at.elapsed().as_secs(),
                    if batch_ids.is_empty() {
                        String::new()
                    } else {
                        format!("Parallel batch stages: {}\n", batch_ids.join(", "))
                    },
                    run.doctor.next,
                    ui_verification_state(run),
                    ui_goal_state(run),
                    run.doctor.safe_next_action,
                    if job_stalled(run, job) {
                        "No fresh output detected for this stage. Press `i` to interrupt if it stays stuck."
                    } else {
                        "Waiting for fresh live output from the current stage."
                    },
                ),
            );
        }
    }
    (run.preview_label.clone(), run.preview.clone())
}

fn live_log_excerpt(run: &RunSnapshot, job: &RunningJob) -> (String, Vec<String>) {
    let stage = job_display_label(Some(run), job);
    let batch_ids = parallel_solver_batch_ids(run, job);
    let live_log_stage = job.log_hint.as_deref().unwrap_or(stage.as_str());
    if !stage_has_fresh_live_log(run, job, live_log_stage) {
        let mut lines = vec![
            format!("No fresh live output yet for stage `{stage}`."),
            "No fresh stage log was written after this attempt started.".to_string(),
        ];
        if let Some(launcher) = job_launcher_label(job) {
            lines.push(format!("Launcher command: {launcher}"));
        }
        if !batch_ids.is_empty() {
            lines.push(format!("Parallel batch stages: {}", batch_ids.join(", ")));
        }
        lines.push(format!("Elapsed: {}s", job.started_at.elapsed().as_secs()));
        return (format!("Waiting for {stage}"), lines);
    }
    let (title, mut lines) = contextual_log_excerpt(
        &run.run_dir,
        job.log_hint.as_deref(),
        Some(&run.status.next),
        12,
    );
    if !batch_ids.is_empty() {
        lines.insert(
            0,
            format!("Parallel batch stages: {}", batch_ids.join(", ")),
        );
        lines.insert(1, String::new());
    }
    (title, lines)
}

fn is_fresh_for_job(path: &Path, job: &RunningJob) -> bool {
    let Ok(meta) = std::fs::metadata(path) else {
        return false;
    };
    let Ok(modified) = meta.modified() else {
        return false;
    };
    modified >= job.started_wallclock
}

fn stage_has_fresh_live_log(run: &RunSnapshot, job: &RunningJob, stage: &str) -> bool {
    let logs_dir = run.run_dir.join("logs");
    let trimmed = stage.trim();
    if trimmed.is_empty() || trimmed == "none" || trimmed == "rerun" {
        return is_fresh_for_job(&runtime::process_log_path(&run.run_dir), job);
    }
    if [
        format!("{trimmed}.last.md"),
        format!("{trimmed}.stdout.log"),
        format!("{trimmed}.stderr.log"),
    ]
    .into_iter()
    .map(|candidate| logs_dir.join(candidate))
    .any(|path| is_fresh_for_job(&path, job))
    {
        return true;
    }
    is_fresh_for_job(&runtime::process_log_path(&run.run_dir), job)
}

fn job_stalled(run: &RunSnapshot, job: &RunningJob) -> bool {
    if job.started_at.elapsed() < Duration::from_secs(20) {
        return false;
    }
    let stage = match job.log_hint.as_deref() {
        Some(value) => value,
        None => return false,
    };
    !stage_has_fresh_output(run, job, stage)
}

fn stage_has_fresh_output(run: &RunSnapshot, job: &RunningJob, stage: &str) -> bool {
    let mut candidates = vec![
        run.run_dir.join("logs").join(format!("{stage}.last.md")),
        run.run_dir.join("logs").join(format!("{stage}.stdout.log")),
        run.run_dir.join("logs").join(format!("{stage}.stderr.log")),
        runtime::process_log_path(&run.run_dir),
    ];
    match stage {
        "review" => {
            candidates.push(run.run_dir.join("review").join("report.md"));
            candidates.push(run.run_dir.join("review").join("user-summary.md"));
            candidates.push(run.run_dir.join("review").join("scorecard.json"));
        }
        "execution" => {
            candidates.push(run.run_dir.join("execution").join("report.md"));
        }
        "verification" => {
            candidates.push(run.run_dir.join("verification").join("findings.md"));
            candidates.push(run.run_dir.join("verification").join("user-summary.md"));
            candidates.push(run.run_dir.join("verification").join("goal-status.json"));
        }
        "intake" => {
            candidates.push(run.run_dir.join("brief.md"));
            candidates.push(run.run_dir.join("plan.json"));
        }
        solver if solver.starts_with("solver-") => {
            candidates.push(run.run_dir.join("solutions").join(solver).join("RESULT.md"));
        }
        _ => {}
    }
    candidates
        .into_iter()
        .any(|path| is_fresh_for_job(&path, job))
}

fn infer_log_hint(label: &str, run: &RunSnapshot) -> Option<String> {
    if label.contains("verification") {
        return Some("verification".to_string());
    }
    if label.contains("execution") {
        return Some("execution".to_string());
    }
    if label.contains("review") {
        return Some("review".to_string());
    }
    if label == "resume" || label == "start-next" || label == "safe-next" {
        let next = run.status.next.trim();
        if !next.is_empty() && next != "none" && next != "rerun" {
            return Some(next.to_string());
        }
    }
    if label == "amend" {
        return Some("intake".to_string());
    }
    None
}

fn draw_amend_popup(frame: &mut ratatui::Frame<'_>, area: Rect, buffer: &str) {
    let popup = centered_rect(70, 28, area);
    frame.render_widget(Clear, popup);
    let text = format!(
        "Add amendment for the selected run.\n\nPress Enter to save and rewind intake.\nThen use n to run the next stage or r to resume the whole run.\nPress Esc to cancel.\n\n{}",
        buffer
    );
    let paragraph = Paragraph::new(text)
        .block(Block::default().title("Amend Run").borders(Borders::ALL))
        .wrap(Wrap { trim: false });
    frame.render_widget(paragraph, popup);
}

fn draw_new_run_popup(
    frame: &mut ratatui::Frame<'_>,
    area: Rect,
    draft: &NewRunDraft,
    requested_task_scroll: u16,
) {
    let popup = new_run_popup_rect(area);
    frame.render_widget(Clear, popup);
    let layout = new_run_popup_layout(popup);
    let task_scroll = text_scroll_for_cursor(
        layout[1],
        &draft.task,
        draft.task_cursor,
        requested_task_scroll,
    );
    let field_name = match draft.field {
        NewRunField::Task => "Task",
        NewRunField::Workspace => "Workspace",
        NewRunField::Title => "Title",
        NewRunField::Start => "Start",
        NewRunField::Cancel => "Cancel",
    };
    let start_style = if matches!(draft.field, NewRunField::Start) {
        Style::default()
            .fg(Color::Black)
            .bg(Color::Green)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(Color::White).bg(Color::DarkGray)
    };
    let cancel_style = if matches!(draft.field, NewRunField::Cancel) {
        Style::default()
            .fg(Color::Black)
            .bg(Color::Yellow)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(Color::White).bg(Color::DarkGray)
    };
    let buttons = Line::from(vec![
        Span::raw("  "),
        Span::styled(" Start Interview ", start_style),
        Span::raw("   "),
        Span::styled(" Cancel ", cancel_style),
    ]);
    let header = Paragraph::new(format!(
        "Create a new pipeline.\nFill the task, set the workspace, then start stage0.\nActive field: {field_name}"
    ))
    .block(Block::default().title("New Pipeline").borders(Borders::ALL))
    .wrap(Wrap { trim: false });
    frame.render_widget(header, layout[0]);
    frame.render_widget(
        field_block(
            "Task",
            if draft.task.is_empty() {
                "<type the raw task here>"
            } else {
                draft.task.as_str()
            },
            matches!(draft.field, NewRunField::Task),
            task_scroll,
        ),
        layout[1],
    );
    frame.render_widget(
        field_block(
            "Workspace",
            if draft.workspace.is_empty() {
                "<required: absolute path to the target workspace>"
            } else {
                draft.workspace.as_str()
            },
            matches!(draft.field, NewRunField::Workspace),
            0,
        ),
        layout[2],
    );
    frame.render_widget(
        field_block(
            "Title",
            if draft.title.is_empty() {
                "<optional>"
            } else {
                draft.title.as_str()
            },
            matches!(draft.field, NewRunField::Title),
            0,
        ),
        layout[3],
    );
    frame.render_widget(
        Paragraph::new("Tab switch  Enter edit/apply  PgUp/PgDn scroll  Esc cancel")
            .block(Block::default().borders(Borders::ALL)),
        layout[4],
    );
    frame.render_widget(
        Paragraph::new(vec![Line::from(""), buttons])
            .block(Block::default().borders(Borders::ALL))
            .wrap(Wrap { trim: false }),
        layout[5],
    );
}

#[allow(clippy::too_many_arguments)]
fn draw_interview_popup(
    frame: &mut ratatui::Frame<'_>,
    area: Rect,
    goal_summary: &str,
    questions: &[InterviewQuestion],
    answers: &[String],
    index: usize,
    buffer: &str,
    requested_answer_scroll: u16,
) {
    let popup = interview_popup_rect(area);
    frame.render_widget(Clear, popup);
    let layout = interview_popup_layout(popup);
    let answer_scroll = text_scroll_for_cursor(
        layout[3],
        buffer,
        char_count(buffer),
        requested_answer_scroll,
    );
    let question = questions.get(index);
    let progress = format!("Question {}/{}", index + 1, questions.len());
    let answered = if answers.is_empty() {
        "No answers yet.".to_string()
    } else {
        answers
            .iter()
            .enumerate()
            .filter(|(_, value)| !value.trim().is_empty())
            .map(|(i, value)| format!("{}. {}", i + 1, value))
            .collect::<Vec<_>>()
            .join("\n")
    };
    frame.render_widget(
        Paragraph::new(format!(
            "{progress}\nUse Up for the previous answer. Enter saves and continues."
        ))
        .block(Block::default().title("Interview").borders(Borders::ALL))
        .wrap(Wrap { trim: false }),
        layout[0],
    );
    frame.render_widget(
        field_block(
            "Goal summary",
            if goal_summary.trim().is_empty() {
                "<not provided>"
            } else {
                goal_summary
            },
            false,
            0,
        ),
        layout[1],
    );
    frame.render_widget(
        field_block(
            "Question / Why",
            &format!(
                "{}\n\nWhy: {}",
                question
                    .map(|item| item.question.as_str())
                    .unwrap_or("<done>"),
                question
                    .map(|item| if item.why.trim().is_empty() {
                        "<no explanation>"
                    } else {
                        item.why.as_str()
                    })
                    .unwrap_or("<done>")
            ),
            false,
            0,
        ),
        layout[2],
    );
    frame.render_widget(
        field_block(
            "Answer",
            if buffer.is_empty() {
                "<type here>"
            } else {
                buffer
            },
            true,
            answer_scroll,
        ),
        layout[3],
    );
    frame.render_widget(
        field_block(
            "Answered so far",
            if answered.trim().is_empty() {
                "No answers yet."
            } else {
                answered.as_str()
            },
            false,
            0,
        ),
        layout[4],
    );
}

fn new_run_popup_rect(area: Rect) -> Rect {
    centered_rect_with_min(area, 90, 80, 84, 19)
}

fn new_run_popup_layout(popup: Rect) -> Vec<Rect> {
    Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(4),
            Constraint::Min(4),
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Length(2),
            Constraint::Length(3),
        ])
        .split(popup)
        .to_vec()
}

fn interview_popup_rect(area: Rect) -> Rect {
    centered_rect_with_min(area, 90, 85, 90, 20)
}

fn interview_popup_layout(popup: Rect) -> Vec<Rect> {
    Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(4),
            Constraint::Length(4),
            Constraint::Length(5),
            Constraint::Min(4),
            Constraint::Length(3),
        ])
        .split(popup)
        .to_vec()
}

fn block_inner_area(area: Rect) -> Rect {
    Rect {
        x: area.x.saturating_add(1),
        y: area.y.saturating_add(1),
        width: area.width.saturating_sub(2),
        height: area.height.saturating_sub(2),
    }
}

fn text_cursor_row_col(value: &str, width: usize, cursor: usize) -> (usize, usize) {
    if width == 0 {
        return (0, 0);
    }
    let mut row = 0usize;
    let mut col = 0usize;
    for ch in value.chars().take(cursor) {
        if ch == '\n' {
            row = row.saturating_add(1);
            col = 0;
            continue;
        }
        if col >= width {
            row = row.saturating_add(col / width);
            col %= width;
        }
        if col + 1 > width {
            row = row.saturating_add(1);
            col = 0;
        }
        col += 1;
    }
    if col >= width {
        row = row.saturating_add(col / width);
        col %= width;
    }
    (row, col)
}

fn text_scroll_for_cursor(area: Rect, value: &str, cursor: usize, requested: u16) -> u16 {
    let inner = block_inner_area(area);
    if inner.width == 0 || inner.height == 0 {
        return 0;
    }
    let width = inner.width as usize;
    let visible_rows = inner.height as usize;
    let (row, _) = text_cursor_row_col(value, width, cursor);
    let total_lines = wrapped_line_count(value, inner.width);
    let max_scroll = total_lines.saturating_sub(visible_rows);
    let min_scroll = row.saturating_sub(visible_rows.saturating_sub(1));
    requested.max(min_scroll as u16).min(max_scroll as u16)
}

fn text_cursor_position(area: Rect, value: &str, cursor: usize, scroll: u16) -> Option<(u16, u16)> {
    let inner = block_inner_area(area);
    if inner.width == 0 || inner.height == 0 {
        return None;
    }
    let width = inner.width as usize;
    let max_row = inner.height.saturating_sub(1) as usize;
    let (row, col) = text_cursor_row_col(value, width, cursor);
    let row = row.saturating_sub(scroll as usize).min(max_row);
    let col = col.min(inner.width.saturating_sub(1) as usize);
    Some((
        inner.x.saturating_add(col as u16),
        inner.y.saturating_add(row as u16),
    ))
}

fn new_run_cursor_position(
    area: Rect,
    draft: &NewRunDraft,
    requested_task_scroll: u16,
) -> Option<(u16, u16)> {
    let popup = new_run_popup_rect(area);
    let layout = new_run_popup_layout(popup);
    match draft.field {
        NewRunField::Task => {
            let scroll = text_scroll_for_cursor(
                layout[1],
                &draft.task,
                draft.task_cursor,
                requested_task_scroll,
            );
            text_cursor_position(layout[1], &draft.task, draft.task_cursor, scroll)
        }
        NewRunField::Workspace => {
            text_cursor_position(layout[2], &draft.workspace, draft.workspace_cursor, 0)
        }
        NewRunField::Title => text_cursor_position(layout[3], &draft.title, draft.title_cursor, 0),
        NewRunField::Start | NewRunField::Cancel => None,
    }
}

fn interview_cursor_position(
    area: Rect,
    buffer: &str,
    requested_answer_scroll: u16,
) -> Option<(u16, u16)> {
    let popup = interview_popup_rect(area);
    let layout = interview_popup_layout(popup);
    let cursor = char_count(buffer);
    let scroll = text_scroll_for_cursor(layout[3], buffer, cursor, requested_answer_scroll);
    text_cursor_position(layout[3], buffer, cursor, scroll)
}

fn draw_prompt_review_popup(
    frame: &mut ratatui::Frame<'_>,
    area: Rect,
    final_task_path: &Path,
    scroll: u16,
    selected: PromptReviewAction,
) {
    let popup = centered_rect(88, 86, area);
    frame.render_widget(Clear, popup);
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(5),
            Constraint::Min(12),
            Constraint::Length(3),
        ])
        .split(popup);
    let create_style = if matches!(selected, PromptReviewAction::CreateOnly) {
        Style::default()
            .fg(Color::Black)
            .bg(Color::Green)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(Color::White).bg(Color::DarkGray)
    };
    let start_style = if matches!(selected, PromptReviewAction::CreateAndStart) {
        Style::default()
            .fg(Color::Black)
            .bg(Color::Blue)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(Color::White).bg(Color::DarkGray)
    };
    let cancel_style = if matches!(selected, PromptReviewAction::Cancel) {
        Style::default()
            .fg(Color::Black)
            .bg(Color::Yellow)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(Color::White).bg(Color::DarkGray)
    };
    let prompt_text = std::fs::read_to_string(final_task_path)
        .unwrap_or_else(|_| format!("Could not read {}", final_task_path.display()));
    frame.render_widget(
        Paragraph::new(
            "Review the final task prompt before creating the run.\n\nj/k scroll  Tab switch action  Enter apply  Esc cancel"
                .to_string(),
        )
        .block(Block::default().title("Final Task Prompt").borders(Borders::ALL))
        .wrap(Wrap { trim: false }),
        layout[0],
    );
    frame.render_widget(
        Paragraph::new(prompt_text)
            .block(Block::default().borders(Borders::ALL))
            .scroll((scroll, 0))
            .wrap(Wrap { trim: false }),
        layout[1],
    );
    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::raw("  "),
            Span::styled(" Create Run ", create_style),
            Span::raw("   "),
            Span::styled(" Create + Run All ", start_style),
            Span::raw("   "),
            Span::styled(" Cancel ", cancel_style),
        ]))
        .block(Block::default().borders(Borders::ALL)),
        layout[2],
    );
}

fn field_block(title: &str, value: &str, active: bool, scroll: u16) -> Paragraph<'static> {
    let block = if active {
        Block::default()
            .title(title.to_string())
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Cyan))
    } else {
        Block::default()
            .title(title.to_string())
            .borders(Borders::ALL)
    };
    Paragraph::new(value.to_string())
        .block(block)
        .scroll((scroll, 0))
        .wrap(Wrap { trim: false })
}

fn action_panel_text(app: &App, run: Option<&RunSnapshot>) -> String {
    if let Some(job) = &app.job {
        if let Some(run) = run {
            if job.run_dir == run.run_dir {
                return format!(
                    "Primary\n- i interrupt current stage\n- j/k switch runs\n- Enter open preferred artifact\n- 1 Summary  2 Findings  3 Augmented  4 Execution  5 Brief\n\nBackground-capable\n- c create another pipeline\n- x delete only inactive runs\n- p prune skips active runs\n\nCurrent\n- running: {}\n- next: {}\n- safe action: {}\n",
                    running_job_display_label(Some(run), job),
                    run.doctor.next,
                    run.doctor.safe_next_action
                );
            }
        }
        return format!(
            "Background job\n- {}\n- i interrupt tracked job\n- j/k move between runs\n- c create another pipeline\n- n next stage on the selected inactive run\n- r/w run the whole stack on the selected inactive run\n- y rerun\n- h/u refresh helpers\n- x delete only inactive runs\n- p prune skips active runs\n- Enter open artifacts for the selected run\n",
            running_job_label(job)
        );
    }
    if let Some(job) = app.wizard_job() {
        return format!(
            "Wizard\n- i interrupt stage0 creation\n- j/k move between runs\n- c create another pipeline after this one finishes\n\nCurrent\n- running: {}\n",
            running_job_label(job)
        );
    }
    if let Some(run) = run {
        format!(
            "Primary\n- n next stage\n- r/w run whole stack\n- a amend and rewind\n- Enter open preferred artifact\n\nSecondary\n- y rerun\n- h refresh host probe\n- u refresh prompts\n- x delete run\n- p prune runs\n\nCurrent\n- next: {}\n- safe action: {}\n- solver fan-out uses parallel start-solvers when applicable\n",
            run.doctor.next,
            run.doctor.safe_next_action
        )
    } else {
        "Primary\n- c create pipeline\n- g refresh run list\n- j/k move\n- q quit\n".to_string()
    }
}

fn draw_confirm_popup(
    frame: &mut ratatui::Frame<'_>,
    area: Rect,
    title: &str,
    message: &str,
    selected: ConfirmChoice,
    confirm_label: &str,
) {
    let popup = centered_rect(60, 20, area);
    frame.render_widget(Clear, popup);
    let lines = vec![
        Line::from(message),
        Line::from(""),
        button_line(selected, confirm_label),
        Line::from(""),
        Line::from("Use Left/Right or Tab to choose, Enter to apply, Esc or c to cancel."),
    ];
    let paragraph = Paragraph::new(lines)
        .block(Block::default().title(title).borders(Borders::ALL))
        .wrap(Wrap { trim: false });
    frame.render_widget(paragraph, popup);
}

fn button_line(selected: ConfirmChoice, confirm_label: &str) -> Line<'static> {
    let cancel_style = if matches!(selected, ConfirmChoice::Cancel) {
        Style::default()
            .fg(Color::Black)
            .bg(Color::Yellow)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(Color::White).bg(Color::DarkGray)
    };
    let confirm_style = if matches!(selected, ConfirmChoice::Confirm) {
        Style::default()
            .fg(Color::Black)
            .bg(Color::Green)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(Color::White).bg(Color::DarkGray)
    };
    Line::from(vec![
        Span::raw("  "),
        Span::styled(" Cancel ", cancel_style),
        Span::raw("   "),
        Span::styled(format!(" {confirm_label} "), confirm_style),
    ])
}

fn draw_artifact_popup(
    frame: &mut ratatui::Frame<'_>,
    area: Rect,
    app: &App,
    kind: ArtifactKind,
    scroll: u16,
) {
    let popup = centered_rect(88, 86, area);
    frame.render_widget(Clear, popup);
    let content = if let Some(run) = app.selected_run() {
        artifact_content(run, kind)
    } else {
        "No run selected.".to_string()
    };
    let label = if matches!(kind, ArtifactKind::Summary) {
        app.selected_run()
            .map(summary_title)
            .unwrap_or_else(|| kind.label())
    } else {
        kind.label()
    };
    let title = format!(
        "{}  scroll j/k PgUp/PgDn  switch [ ] or 1..5  Esc close",
        label
    );
    let paragraph = Paragraph::new(content)
        .block(Block::default().title(title).borders(Borders::ALL))
        .scroll((scroll, 0))
        .wrap(Wrap { trim: false });
    frame.render_widget(paragraph, popup);
}

fn preferred_artifact_kind(run: &RunSnapshot) -> ArtifactKind {
    match run.preview_label.as_str() {
        "Summary" => ArtifactKind::Summary,
        "Request" => ArtifactKind::Summary,
        "Findings" => ArtifactKind::Findings,
        "Augmented" => ArtifactKind::Augmented,
        "Execution" => ArtifactKind::Execution,
        "Brief" => ArtifactKind::Brief,
        _ => ArtifactKind::Summary,
    }
}

fn artifact_content(run: &RunSnapshot, kind: ArtifactKind) -> String {
    let path = kind.path(run);
    match std::fs::read_to_string(&path) {
        Ok(content) if !content.trim().is_empty() => {
            if looks_pending_artifact(&content) {
                let pipeline_state = summarize_pipeline_state(run);
                let goal_state = summarize_goal_state(run);
                format!(
                    "{} is not ready yet.\n\n{}\n\nCurrent pipeline state:\n- health: {}\n- pipeline: {}\n- goal status: {}\n- next action: {}\n- host probe: {}\n\nTo continue from the UI:\n1. Press Esc to close this view.\n2. Press n to run the next stage.\n3. Press r to resume the whole pipeline.\n\nUse Details/Logs for realtime progress.",
                    kind.label(),
                    artifact_ready_hint(kind),
                    run.doctor.health,
                    pipeline_state,
                    goal_state,
                    run.doctor.safe_next_action,
                    run.doctor.host_probe,
                )
            } else {
                content
            }
        }
        Ok(_) => format!("{} is empty.", path.display()),
        Err(_) => format!("{} is not available for this run.", path.display()),
    }
}

fn looks_pending_artifact(content: &str) -> bool {
    let trimmed = content.trim();
    if trimmed.is_empty() {
        return true;
    }
    let lower = trimmed.to_lowercase();
    lower.contains("pending localized review summary")
        || lower.contains("pending localized verification summary")
        || lower.contains("pending review stage")
        || lower.contains("pending verification stage")
        || lower.contains("pending execution stage")
        || lower.contains("pending intake stage")
        || lower.contains("fill this file with the solver output")
}

fn artifact_ready_hint(kind: ArtifactKind) -> &'static str {
    match kind {
        ArtifactKind::Summary => {
            "This view prefers verification summary, falls back to review summary, and only then to the seeded request or follow-up task."
        }
        ArtifactKind::Findings => {
            "This artifact is generated after the verification stage completes."
        }
        ArtifactKind::Augmented => {
            "This artifact is generated after the verification stage completes."
        }
        ArtifactKind::Execution => {
            "This artifact is generated after the execution stage completes."
        }
        ArtifactKind::Brief => "This artifact is generated after the intake stage completes.",
    }
}

fn summarize_pipeline_state(run: &RunSnapshot) -> String {
    let next = run.doctor.next.trim();
    match next {
        "none" => "pipeline complete".to_string(),
        "rerun" => "verification recommends a follow-up rerun".to_string(),
        "" => "waiting for the next stage".to_string(),
        stage => format!("waiting for `{stage}`"),
    }
}

fn summarize_goal_state(run: &RunSnapshot) -> String {
    let verification_done = run
        .doctor
        .stages
        .get("verification")
        .map(|value| value == "done")
        .unwrap_or(false);
    let late_stage = matches!(run.doctor.next.as_str(), "verification" | "rerun" | "none");
    if verification_done
        || late_stage
        || matches!(run.doctor.goal.as_str(), "complete" | "partial" | "blocked")
    {
        run.doctor.goal.clone()
    } else {
        "not evaluated yet".to_string()
    }
}

fn ui_goal_state(run: &RunSnapshot) -> String {
    let next = run.doctor.next.trim();
    if run.doctor.goal == "pending-verification"
        && !matches!(next, "verification" | "none" | "rerun")
    {
        "not-evaluated".to_string()
    } else {
        run.doctor.goal.clone()
    }
}

fn ui_verification_state(run: &RunSnapshot) -> String {
    run.doctor
        .stages
        .get("verification")
        .or_else(|| run.status.stages.get("verification"))
        .cloned()
        .unwrap_or_else(|| "missing".to_string())
}

fn run_list_detail_lines(run: &RunSnapshot) -> (String, String) {
    (
        format!("health={} | next={}", run.doctor.health, run.doctor.next),
        format!(
            "verify={} | verdict={}",
            ui_verification_state(run),
            ui_goal_state(run)
        ),
    )
}

fn centered_rect(percent_x: u16, percent_y: u16, area: Rect) -> Rect {
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(area);
    let horizontal = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(vertical[1]);
    horizontal[1]
}

fn centered_rect_with_min(
    area: Rect,
    percent_x: u16,
    percent_y: u16,
    min_width: u16,
    min_height: u16,
) -> Rect {
    let max_width = area.width.saturating_sub(2).max(1);
    let max_height = area.height.saturating_sub(2).max(1);
    let width = (((area.width as u32) * (percent_x as u32)) / 100)
        .max(min_width as u32)
        .min(max_width as u32) as u16;
    let height = (((area.height as u32) * (percent_y as u32)) / 100)
        .max(min_height as u32)
        .min(max_height as u32) as u16;
    Rect {
        x: area.x + area.width.saturating_sub(width) / 2,
        y: area.y + area.height.saturating_sub(height) / 2,
        width,
        height,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        block_inner_area, default_new_run_draft, delete_run_if_safe, ensure_job_slot_for_run,
        handle_finished_job, handle_mouse, handle_new_run_key, handle_normal_key, handle_paste,
        interview_cursor_position, interview_popup_layout, interview_popup_rect, job_display_label,
        key_is_char, live_log_excerpt, live_preview, new_run_cursor_position, new_run_popup_layout,
        new_run_popup_rect, normal_mode_rects, parse_embedded_json, poll_job,
        preferred_summary_path, rerun_created_run_dir, run_list_detail_lines, running_job_label,
        summary_title, text_scroll_for_cursor, ui_goal_state, ui_verification_state, App,
        ArtifactKind, FinishedJob, InterviewQuestionsPayload, JobKind, JobResult, Mode,
        NewRunDraft, NewRunField, RunningJob,
    };
    use crate::engine::{Context, DoctorPayload, RunSnapshot, RunTokenSummary, StatusPayload};
    use crate::runtime;
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers, MouseEvent, MouseEventKind};
    use ratatui::layout::Rect;
    use std::collections::BTreeMap;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::mpsc;
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    fn temp_dir(name: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("agpipe-tui-test-{name}-{unique}"));
        fs::create_dir_all(&path).expect("create temp dir");
        path
    }

    fn sample_run(goal: &str, next: &str, verification: &str) -> RunSnapshot {
        RunSnapshot {
            run_dir: PathBuf::from("/tmp/agpipe-test-run"),
            doctor: DoctorPayload {
                run_dir: "/tmp/agpipe-test-run".to_string(),
                health: "healthy".to_string(),
                stages: BTreeMap::from([
                    ("review".to_string(), "pending".to_string()),
                    ("verification".to_string(), verification.to_string()),
                ]),
                host_probe: "captured (mps)".to_string(),
                goal: goal.to_string(),
                next: next.to_string(),
                safe_next_action: format!("start {next}"),
                ..DoctorPayload::default()
            },
            status: StatusPayload {
                run_dir: "/tmp/agpipe-test-run".to_string(),
                stages: BTreeMap::from([
                    ("review".to_string(), "pending".to_string()),
                    ("verification".to_string(), verification.to_string()),
                ]),
                host_probe: "captured (mps)".to_string(),
                goal: goal.to_string(),
                next: next.to_string(),
                ..StatusPayload::default()
            },
            token_summary: RunTokenSummary {
                budget_total_tokens: 150_000,
                remaining_tokens: Some(149_000),
                source: "test".to_string(),
                ..RunTokenSummary::default()
            },
            preview_label: "Preview".to_string(),
            preview: String::new(),
            log_title: "Logs".to_string(),
            log_lines: Vec::new(),
        }
    }

    fn test_context() -> Context {
        Context {
            repo_root: PathBuf::from("/tmp/agpipe-test-repo"),
            codex_bin: "codex".to_string(),
            stage0_backend: "codex".to_string(),
            stage_backend: "codex".to_string(),
            openai_api_base: "https://api.openai.com/v1".to_string(),
            openai_api_key: None,
            openai_model: "gpt-5".to_string(),
            openai_prompt_cache_key_prefix: "agpipe-stage0-v1".to_string(),
            openai_prompt_cache_retention: None,
            openai_store: false,
            openai_background: false,
        }
    }

    #[test]
    fn ui_goal_state_hides_pending_verification_before_late_stages() {
        let run = sample_run("pending-verification", "review", "pending");
        assert_eq!(ui_goal_state(&run), "not-evaluated");
    }

    #[test]
    fn ui_goal_state_keeps_pending_verification_for_verification_stage() {
        let run = sample_run("pending-verification", "verification", "pending");
        assert_eq!(ui_goal_state(&run), "pending-verification");
    }

    #[test]
    fn ui_goal_state_keeps_final_goal_values() {
        let run = sample_run("partial", "rerun", "done");
        assert_eq!(ui_goal_state(&run), "partial");
    }

    #[test]
    fn ui_verification_state_reports_stage_status() {
        let run = sample_run("partial", "rerun", "done");
        assert_eq!(ui_verification_state(&run), "done");
    }

    #[test]
    fn ui_verification_state_falls_back_to_status_snapshot() {
        let mut run = sample_run("partial", "rerun", "done");
        run.doctor.stages.remove("verification");

        assert_eq!(ui_verification_state(&run), "done");
    }

    #[test]
    fn run_list_detail_lines_keep_verification_and_verdict_visible() {
        let run = sample_run("partial", "rerun", "done");

        let (line2, line3) = run_list_detail_lines(&run);

        assert_eq!(line2, "health=healthy | next=rerun");
        assert_eq!(line3, "verify=done | verdict=partial");
    }

    #[test]
    fn key_aliases_work_in_cyrillic_layout_for_global_shortcuts() {
        let quit_key = KeyEvent::new(KeyCode::Char('й'), KeyModifiers::NONE);
        let next_key = KeyEvent::new(KeyCode::Char('т'), KeyModifiers::NONE);
        let resume_key = KeyEvent::new(KeyCode::Char('к'), KeyModifiers::NONE);
        let whole_stack_key = KeyEvent::new(KeyCode::Char('ц'), KeyModifiers::NONE);

        assert!(key_is_char(quit_key, 'q'));
        assert!(key_is_char(next_key, 'n'));
        assert!(key_is_char(resume_key, 'r'));
        assert!(key_is_char(whole_stack_key, 'w'));
    }

    #[test]
    fn parse_embedded_json_ignores_leading_log_lines() {
        let payload = parse_embedded_json::<InterviewQuestionsPayload>(
            "Starting stage0 interview questions\nArgs: interview-questions --task foo\n{\n  \"session_dir\": \"/tmp/session\",\n  \"goal_summary\": \"goal\",\n  \"questions\": []\n}\n",
        )
        .expect("embedded json should parse");

        assert_eq!(payload.session_dir, "/tmp/session");
        assert_eq!(payload.goal_summary, "goal");
        assert!(payload.questions.is_empty());
    }

    #[test]
    fn parse_embedded_json_prefers_last_complete_object_in_noisy_log() {
        let payload = parse_embedded_json::<InterviewQuestionsPayload>(
            "noise\n{\n  \"goal_summary\": \"first\",\n  \"questions\": []\n}\n{\ntokens used\n  \"goal_summary\": \"broken\"\n}\n{\n  \"goal_summary\": \"final\",\n  \"questions\": [\n    {\n      \"id\": \"scope\",\n      \"question\": \"Need scope?\",\n      \"why\": \"Needed for execution.\",\n      \"required\": true\n    }\n  ],\n  \"session_dir\": \"/tmp/final\"\n}\n",
        )
        .expect("embedded json should parse last valid object");

        assert_eq!(payload.session_dir, "/tmp/final");
        assert_eq!(payload.goal_summary, "final");
        assert_eq!(payload.questions.len(), 1);
        assert_eq!(payload.questions[0].id, "scope");
    }

    #[test]
    fn handle_finished_interview_questions_uses_stdout_when_process_log_has_no_json() {
        let ctx = test_context();
        let session_dir = temp_dir("interview-stdout-session");
        let run_dir = temp_dir("interview-stdout-run");
        fs::create_dir_all(run_dir.join("runtime")).expect("create runtime dir");
        fs::write(
            run_dir.join("runtime").join("process.log"),
            "Starting stage0 interview questions\n",
        )
        .expect("write process log");
        let draft = NewRunDraft {
            task: "Fix the pipeline".to_string(),
            workspace: "/tmp/workspace".to_string(),
            title: String::new(),
            task_cursor: 0,
            workspace_cursor: 0,
            title_cursor: 0,
            field: NewRunField::Task,
        };
        let mut app = App {
            root: PathBuf::from("/tmp"),
            limit: 20,
            runs: Vec::new(),
            selected: 0,
            wizard_selected: false,
            preview_scroll: 0,
            log_scroll: 0,
            mouse_capture_enabled: true,
            mode: Mode::Normal,
            notice: String::new(),
            last_output: String::new(),
            last_refresh: Instant::now(),
            job: None,
            wizard_job: None,
        };

        handle_finished_job(
            &ctx,
            &mut app,
            FinishedJob {
                kind: JobKind::InterviewQuestions {
                    draft: draft.clone(),
                },
                label: "interview-questions".to_string(),
                run_dir,
                completed_log_hint: None,
                result: JobResult {
                    code: 0,
                    stdout: format!(
                        "{{\n  \"session_dir\": \"{}\",\n  \"goal_summary\": \"Need one answer\",\n  \"questions\": [{{\n    \"id\": \"scope\",\n    \"question\": \"Run verification too?\",\n    \"why\": \"Needed for the downstream plan.\",\n    \"required\": true\n  }}]\n}}\n",
                        session_dir.display()
                    ),
                    stderr: String::new(),
                },
                detached_finish: false,
            },
        )
        .expect("handle finished interview questions");

        match &app.mode {
            Mode::InterviewInput {
                goal_summary,
                questions,
                ..
            } => {
                assert_eq!(goal_summary, "Need one answer");
                assert_eq!(questions.len(), 1);
                assert_eq!(questions[0].id, "scope");
            }
            _ => panic!("expected interview input mode"),
        }

        let _ = fs::remove_dir_all(session_dir);
    }

    #[test]
    fn new_run_cursor_starts_at_task_field_origin_when_empty() {
        let draft = NewRunDraft {
            task: String::new(),
            workspace: "/tmp/workspace".to_string(),
            title: String::new(),
            task_cursor: 0,
            workspace_cursor: "/tmp/workspace".chars().count(),
            title_cursor: 0,
            field: NewRunField::Task,
        };
        let area = Rect::new(0, 0, 120, 40);
        let popup = new_run_popup_rect(area);
        let layout = new_run_popup_layout(popup);
        let inner = block_inner_area(layout[1]);

        let cursor = new_run_cursor_position(area, &draft, 0);

        assert_eq!(cursor, Some((inner.x, inner.y)));
    }

    #[test]
    fn text_scroll_for_cursor_allows_manual_scroll_beyond_cursor_row() {
        let area = Rect::new(0, 0, 40, 8);
        let value = (0..20)
            .map(|index| format!("line {index}"))
            .collect::<Vec<_>>()
            .join("\n");

        let scroll = text_scroll_for_cursor(area, &value, 0, 50);

        assert!(
            scroll > 0,
            "expected scroll to remain possible for long text"
        );
    }

    #[test]
    fn interview_cursor_moves_with_multiline_answer() {
        let area = Rect::new(0, 0, 120, 40);
        let base = interview_cursor_position(area, "first line", 0);
        let multiline = interview_cursor_position(area, "first line\nsecond", 0);

        assert!(multiline.unwrap().1 > base.unwrap().1);
        assert!(multiline.unwrap().0 <= base.unwrap().0);
    }

    #[test]
    fn popup_layouts_leave_space_for_fields_on_small_terminals() {
        let small = Rect::new(0, 0, 80, 24);
        let new_run = new_run_popup_layout(new_run_popup_rect(small));
        let interview = interview_popup_layout(interview_popup_rect(small));

        assert!(new_run[1].height >= 4);
        assert!(new_run[2].height >= 3);
        assert!(new_run[3].height >= 3);
        assert!(interview[3].height >= 4);
        assert!(interview[4].height >= 3);
    }

    #[test]
    fn selected_run_is_hidden_while_stage0_wizard_is_selected() {
        let app = App {
            root: PathBuf::from("/tmp"),
            limit: 20,
            runs: vec![sample_run("pending-verification", "intake", "pending")],
            selected: 0,
            wizard_selected: true,
            preview_scroll: 0,
            log_scroll: 0,
            mouse_capture_enabled: true,
            mode: Mode::Normal,
            notice: String::new(),
            last_output: String::new(),
            last_refresh: Instant::now(),
            job: None,
            wizard_job: Some(RunningJob {
                kind: JobKind::InterviewQuestions {
                    draft: default_new_run_draft(),
                },
                label: "interview-questions".to_string(),
                run_dir: PathBuf::from("/tmp/.agpipe-ui"),
                log_hint: None,
                command_hint: "stage0 interview questions".to_string(),
                started_at: Instant::now() - Duration::from_secs(2),
                started_wallclock: SystemTime::now() - Duration::from_secs(2),
                pid: 1,
                pgid: 1,
                process_log: PathBuf::from("/tmp/.agpipe-ui/process.log"),
                stream_rx: None,
                completion_rx: None,
                stream_lines: Vec::new(),
                attached: false,
                last_heartbeat: Instant::now(),
            }),
        };

        assert!(app.selected_run().is_none());
        assert!(app.wizard_job().is_some());
    }

    #[test]
    fn move_down_from_wizard_selection_returns_to_first_run() {
        let mut app = App {
            root: PathBuf::from("/tmp"),
            limit: 20,
            runs: vec![sample_run("pending-verification", "intake", "pending")],
            selected: 0,
            wizard_selected: true,
            preview_scroll: 0,
            log_scroll: 0,
            mouse_capture_enabled: true,
            mode: Mode::Normal,
            notice: String::new(),
            last_output: String::new(),
            last_refresh: Instant::now(),
            job: None,
            wizard_job: Some(RunningJob {
                kind: JobKind::InterviewQuestions {
                    draft: default_new_run_draft(),
                },
                label: "interview-questions".to_string(),
                run_dir: PathBuf::from("/tmp/.agpipe-ui"),
                log_hint: None,
                command_hint: "stage0 interview questions".to_string(),
                started_at: Instant::now() - Duration::from_secs(2),
                started_wallclock: SystemTime::now() - Duration::from_secs(2),
                pid: 1,
                pgid: 1,
                process_log: PathBuf::from("/tmp/.agpipe-ui/process.log"),
                stream_rx: None,
                completion_rx: None,
                stream_lines: Vec::new(),
                attached: false,
                last_heartbeat: Instant::now(),
            }),
        };

        app.move_down();

        assert!(!app.wizard_selected);
        assert!(app.selected_run().is_some());
    }

    #[test]
    fn spawn_stage0_job_keeps_existing_run_job_attached() {
        let ctx = test_context();
        let run_dir = temp_dir("keep-run-job");
        let (_stream_tx, stream_rx) = mpsc::channel();
        let (_completion_tx, completion_rx) = mpsc::channel();
        let run_job = RunningJob {
            kind: JobKind::RunAction,
            label: "execution".to_string(),
            run_dir: run_dir.clone(),
            log_hint: Some("execution".to_string()),
            command_hint: "resume until verification".to_string(),
            started_at: Instant::now() - Duration::from_secs(9),
            started_wallclock: SystemTime::now() - Duration::from_secs(9),
            pid: 1,
            pgid: 1,
            process_log: run_dir.join("runtime").join("process.log"),
            stream_rx: Some(stream_rx),
            completion_rx: Some(completion_rx),
            stream_lines: Vec::new(),
            attached: true,
            last_heartbeat: Instant::now(),
        };
        let mut app = App {
            root: temp_dir("keep-run-job-root"),
            limit: 20,
            runs: vec![sample_run(
                "pending-verification",
                "verification",
                "pending",
            )],
            selected: 0,
            wizard_selected: false,
            preview_scroll: 0,
            log_scroll: 0,
            mouse_capture_enabled: true,
            mode: Mode::Normal,
            notice: String::new(),
            last_output: String::new(),
            last_refresh: Instant::now(),
            job: Some(run_job),
            wizard_job: None,
        };
        let wizard_run_dir = super::ui_job_dir(&app.root);

        super::spawn_engine_job(
            &ctx,
            &mut app,
            JobKind::InterviewQuestions {
                draft: default_new_run_draft(),
            },
            "interview-questions".to_string(),
            wizard_run_dir,
            None,
            "stage0 interview questions".to_string(),
            |_ctx| {
                Ok(crate::engine::CommandResult {
                    code: 0,
                    stdout: String::new(),
                    stderr: String::new(),
                })
            },
        )
        .expect("spawn stage0 job");

        assert_eq!(
            app.job.as_ref().map(|job| job.run_dir.clone()),
            Some(run_dir.clone())
        );
        assert!(app.wizard_job().is_some());

        let _ = fs::remove_dir_all(run_dir);
        let _ = fs::remove_dir_all(&app.root);
    }

    #[test]
    fn live_log_excerpt_uses_fresh_stdout_when_last_md_is_missing() {
        let run_dir = temp_dir("live-log-stdout");
        fs::create_dir_all(run_dir.join("logs")).expect("create logs");
        fs::write(
            run_dir.join("logs").join("execution.stdout.log"),
            "line one\nline two\n",
        )
        .expect("write stdout log");
        let mut run = sample_run("pending-verification", "execution", "pending");
        run.run_dir.clone_from(&run_dir);
        let (_stream_tx, stream_rx) = mpsc::channel();
        let (_completion_tx, completion_rx) = mpsc::channel();
        let job = RunningJob {
            kind: JobKind::RunAction,
            label: "start-next".to_string(),
            run_dir: run_dir.clone(),
            log_hint: Some("execution".to_string()),
            command_hint: "start-next".to_string(),
            started_at: Instant::now() - Duration::from_secs(5),
            started_wallclock: SystemTime::now() - Duration::from_secs(5),
            pid: 1,
            pgid: 1,
            process_log: run_dir.join(".runtime-job.log"),
            stream_rx: Some(stream_rx),
            completion_rx: Some(completion_rx),
            stream_lines: Vec::new(),
            attached: true,
            last_heartbeat: Instant::now(),
        };

        let (title, lines) = live_log_excerpt(&run, &job);

        assert_eq!(title, "execution.stdout.log");
        assert!(lines.iter().any(|line| line.contains("line one")));

        let _ = fs::remove_dir_all(run_dir);
    }

    #[test]
    fn running_job_label_prefers_stage_name_over_launcher_command() {
        let run = sample_run("pending-verification", "intake", "pending");
        let (_stream_tx, stream_rx) = mpsc::channel();
        let (_completion_tx, completion_rx) = mpsc::channel();
        let job = RunningJob {
            kind: JobKind::RunAction,
            label: "start-next".to_string(),
            run_dir: run.run_dir.clone(),
            log_hint: Some("intake".to_string()),
            command_hint: "start-next".to_string(),
            started_at: Instant::now() - Duration::from_secs(5),
            started_wallclock: SystemTime::now() - Duration::from_secs(5),
            pid: 1,
            pgid: 1,
            process_log: run.run_dir.join(".runtime-job.log"),
            stream_rx: Some(stream_rx),
            completion_rx: Some(completion_rx),
            stream_lines: Vec::new(),
            attached: true,
            last_heartbeat: Instant::now(),
        };

        let label = running_job_label(&job);

        assert!(label.starts_with("intake ("));
    }

    #[test]
    fn job_display_label_uses_parallel_solver_batch_label() {
        let run_dir = temp_dir("parallel-solver-label");
        fs::write(
            run_dir.join("plan.json"),
            r#"{
  "pipeline": {
    "stages": [
      {"id": "intake", "kind": "intake"},
      {"id": "solver-a", "kind": "solver"},
      {"id": "solver-b", "kind": "research"},
      {"id": "review", "kind": "review"}
    ]
  }
}"#,
        )
        .expect("write plan");
        let mut run = sample_run("pending-verification", "solver-a", "pending");
        run.run_dir.clone_from(&run_dir);
        run.doctor
            .stages
            .insert("solver-a".to_string(), "pending".to_string());
        run.doctor
            .stages
            .insert("solver-b".to_string(), "pending".to_string());
        run.status
            .stages
            .insert("solver-a".to_string(), "pending".to_string());
        run.status
            .stages
            .insert("solver-b".to_string(), "pending".to_string());
        let (_stream_tx, stream_rx) = mpsc::channel();
        let (_completion_tx, completion_rx) = mpsc::channel();
        let job = RunningJob {
            kind: JobKind::RunAction,
            label: "resume".to_string(),
            run_dir: run_dir.clone(),
            log_hint: Some("solver-a".to_string()),
            command_hint: "resume until verification".to_string(),
            started_at: Instant::now() - Duration::from_secs(5),
            started_wallclock: SystemTime::now() - Duration::from_secs(5),
            pid: 1,
            pgid: 1,
            process_log: run_dir.join(".runtime-job.log"),
            stream_rx: Some(stream_rx),
            completion_rx: Some(completion_rx),
            stream_lines: Vec::new(),
            attached: true,
            last_heartbeat: Instant::now(),
        };

        assert_eq!(job_display_label(Some(&run), &job), "solvers (parallel)");

        let _ = fs::remove_dir_all(run_dir);
    }

    #[test]
    fn live_log_excerpt_uses_stage_name_and_separate_launcher_line() {
        let run = sample_run("pending-verification", "intake", "pending");
        let (_stream_tx, stream_rx) = mpsc::channel();
        let (_completion_tx, completion_rx) = mpsc::channel();
        let job = RunningJob {
            kind: JobKind::RunAction,
            label: "start-next".to_string(),
            run_dir: run.run_dir.clone(),
            log_hint: Some("intake".to_string()),
            command_hint: "start-next".to_string(),
            started_at: Instant::now() - Duration::from_secs(5),
            started_wallclock: SystemTime::now() - Duration::from_secs(5),
            pid: 1,
            pgid: 1,
            process_log: run.run_dir.join(".runtime-job.log"),
            stream_rx: Some(stream_rx),
            completion_rx: Some(completion_rx),
            stream_lines: Vec::new(),
            attached: true,
            last_heartbeat: Instant::now(),
        };

        let (title, lines) = live_log_excerpt(&run, &job);

        assert_eq!(title, "Waiting for intake");
        assert!(lines
            .iter()
            .any(|line| line == "Launcher command: start-next"));
        assert!(lines
            .iter()
            .any(|line| line == "No fresh live output yet for stage `intake`."));
    }

    #[test]
    fn preferred_summary_path_uses_verification_summary_when_available() {
        let run_dir = temp_dir("preferred-summary");
        fs::create_dir_all(run_dir.join("review")).expect("create review dir");
        fs::create_dir_all(run_dir.join("verification")).expect("create verification dir");
        fs::write(
            run_dir.join("review").join("user-summary.md"),
            "# User Summary\n\nReview summary.\n",
        )
        .expect("write review summary");
        fs::write(
            run_dir.join("verification").join("user-summary.md"),
            "# Verification Summary\n\nVerification summary.\n",
        )
        .expect("write verification summary");
        let mut run = sample_run("partial", "rerun", "done");
        run.run_dir.clone_from(&run_dir);

        let path = preferred_summary_path(&run);

        assert!(path.ends_with("verification/user-summary.md"));

        let _ = fs::remove_dir_all(run_dir);
    }

    #[test]
    fn preferred_summary_path_falls_back_to_request_when_summaries_are_missing() {
        let run_dir = temp_dir("preferred-request");
        fs::write(
            run_dir.join("request.md"),
            "# Follow-up Task\n\nFinish the preserved-path smoke.\n",
        )
        .expect("write request");
        let mut run = sample_run("pending-verification", "intake", "pending");
        run.run_dir.clone_from(&run_dir);

        let path = preferred_summary_path(&run);

        assert!(path.ends_with("request.md"));

        let _ = fs::remove_dir_all(run_dir);
    }

    #[test]
    fn summary_title_uses_review_when_verification_summary_is_still_placeholder() {
        let run_dir = temp_dir("summary-title-review");
        fs::create_dir_all(run_dir.join("review")).expect("create review dir");
        fs::create_dir_all(run_dir.join("verification")).expect("create verification dir");
        fs::write(
            run_dir.join("review").join("user-summary.md"),
            "# User Summary\n\nReview summary.\n",
        )
        .expect("write review summary");
        fs::write(
            run_dir.join("verification").join("user-summary.md"),
            "# Verification Summary\n\nPending localized verification summary.\n",
        )
        .expect("write verification placeholder");
        let mut run = sample_run("pending-verification", "verification", "pending");
        run.run_dir.clone_from(&run_dir);

        assert_eq!(summary_title(&run), "Review Summary");

        let _ = fs::remove_dir_all(run_dir);
    }

    #[test]
    fn summary_title_uses_verification_when_verification_summary_is_ready() {
        let run_dir = temp_dir("summary-title-verification");
        fs::create_dir_all(run_dir.join("review")).expect("create review dir");
        fs::create_dir_all(run_dir.join("verification")).expect("create verification dir");
        fs::write(
            run_dir.join("review").join("user-summary.md"),
            "# User Summary\n\nReview summary.\n",
        )
        .expect("write review summary");
        fs::write(
            run_dir.join("verification").join("user-summary.md"),
            "# Verification Summary\n\nVerification summary.\n",
        )
        .expect("write verification summary");
        let mut run = sample_run("partial", "rerun", "done");
        run.run_dir.clone_from(&run_dir);

        assert_eq!(summary_title(&run), "Verification Summary");

        let _ = fs::remove_dir_all(run_dir);
    }

    #[test]
    fn rerun_created_run_dir_parses_first_existing_run_path() {
        let created = temp_dir("rerun-created");
        fs::write(created.join("plan.json"), "{}\n").expect("write plan");

        let parsed = rerun_created_run_dir(&format!(
            "{}\n\nNew run status:\n\nnext: intake\n",
            created.display()
        ));

        assert_eq!(parsed, Some(created.clone()));

        let _ = fs::remove_dir_all(created);
    }

    #[test]
    fn live_preview_for_rerun_explains_source_run_is_not_reexecuting() {
        let run = sample_run("partial", "rerun", "done");
        let (_stream_tx, stream_rx) = mpsc::channel();
        let (_completion_tx, completion_rx) = mpsc::channel();
        let job = RunningJob {
            kind: JobKind::RunAction,
            label: "rerun".to_string(),
            run_dir: run.run_dir.clone(),
            log_hint: None,
            command_hint: "rerun".to_string(),
            started_at: Instant::now() - Duration::from_secs(5),
            started_wallclock: SystemTime::now() - Duration::from_secs(5),
            pid: 1,
            pgid: 1,
            process_log: run.run_dir.join(".runtime-job.log"),
            stream_rx: Some(stream_rx),
            completion_rx: Some(completion_rx),
            stream_lines: Vec::new(),
            attached: true,
            last_heartbeat: Instant::now(),
        };
        let app = App {
            root: PathBuf::from("/tmp"),
            limit: 20,
            runs: vec![run],
            selected: 0,
            wizard_selected: false,
            preview_scroll: 0,
            log_scroll: 0,
            mouse_capture_enabled: true,
            mode: Mode::Normal,
            notice: String::new(),
            last_output: String::new(),
            last_refresh: Instant::now(),
            job: Some(job),
            wizard_job: None,
        };

        let (title, text) = live_preview(&app, &app.runs[0]);

        assert_eq!(title, "Live status");
        assert!(text.contains("Creating a follow-up run"));
        assert!(text.contains("The source run itself is not re-executing pipeline stages."));
    }

    #[test]
    fn mouse_wheel_over_logs_scrolls_logs_without_changing_selected_run() {
        let mut app = App {
            root: PathBuf::from("/tmp"),
            limit: 20,
            runs: vec![
                sample_run("partial", "rerun", "done"),
                sample_run("pending-verification", "verification", "pending"),
            ],
            selected: 1,
            wizard_selected: false,
            preview_scroll: 0,
            log_scroll: 0,
            mouse_capture_enabled: true,
            mode: Mode::Normal,
            notice: String::new(),
            last_output: String::new(),
            last_refresh: Instant::now(),
            job: None,
            wizard_job: None,
        };
        let rects = normal_mode_rects(Rect::new(0, 0, 120, 40));

        handle_mouse(
            &mut app,
            MouseEvent {
                kind: MouseEventKind::ScrollDown,
                column: rects.logs.x.saturating_add(2),
                row: rects.logs.y.saturating_add(2),
                modifiers: KeyModifiers::NONE,
            },
            Rect::new(0, 0, 120, 40),
        );

        assert_eq!(app.selected, 1);
        assert_eq!(app.log_scroll, 3);
    }

    #[test]
    fn ensure_job_slot_for_run_detaches_background_job_for_other_run() {
        let run_a_dir = temp_dir("detach-bg-a");
        let run_b_dir = temp_dir("detach-bg-b");
        let mut run_a = sample_run("partial", "rerun", "done");
        run_a.run_dir.clone_from(&run_a_dir);
        let mut run_b = sample_run("pending-verification", "intake", "pending");
        run_b.run_dir.clone_from(&run_b_dir);
        let (_stream_tx, stream_rx) = mpsc::channel();
        let (_completion_tx, completion_rx) = mpsc::channel();
        let job = RunningJob {
            kind: JobKind::RunAction,
            label: "verification".to_string(),
            run_dir: run_a_dir.clone(),
            log_hint: Some("verification".to_string()),
            command_hint: "start-next".to_string(),
            started_at: Instant::now() - Duration::from_secs(5),
            started_wallclock: SystemTime::now() - Duration::from_secs(5),
            pid: 1,
            pgid: 1,
            process_log: run_a_dir.join(".runtime-job.log"),
            stream_rx: Some(stream_rx),
            completion_rx: Some(completion_rx),
            stream_lines: Vec::new(),
            attached: true,
            last_heartbeat: Instant::now(),
        };
        let mut app = App {
            root: PathBuf::from("/tmp"),
            limit: 20,
            runs: vec![run_a, run_b],
            selected: 1,
            wizard_selected: false,
            preview_scroll: 0,
            log_scroll: 0,
            mouse_capture_enabled: true,
            mode: Mode::Normal,
            notice: String::new(),
            last_output: String::new(),
            last_refresh: Instant::now(),
            job: Some(job),
            wizard_job: None,
        };

        let ready = ensure_job_slot_for_run(&mut app, &run_b_dir);

        assert!(ready);
        assert!(app.job.is_none());
        assert!(app.notice.contains("Detached"));

        let _ = fs::remove_dir_all(run_a_dir);
        let _ = fs::remove_dir_all(run_b_dir);
    }

    #[test]
    fn handle_normal_key_allows_create_while_background_job_is_running() {
        let ctx = test_context();
        let run_a_dir = temp_dir("create-bg-a");
        let run_b_dir = temp_dir("create-bg-b");
        let mut run_a = sample_run("partial", "rerun", "done");
        run_a.run_dir.clone_from(&run_a_dir);
        let mut run_b = sample_run("pending-verification", "review", "pending");
        run_b.run_dir.clone_from(&run_b_dir);
        let (_stream_tx, stream_rx) = mpsc::channel();
        let (_completion_tx, completion_rx) = mpsc::channel();
        let job = RunningJob {
            kind: JobKind::RunAction,
            label: "solver-a".to_string(),
            run_dir: run_a_dir.clone(),
            log_hint: Some("solver-a".to_string()),
            command_hint: "start-next".to_string(),
            started_at: Instant::now() - Duration::from_secs(5),
            started_wallclock: SystemTime::now() - Duration::from_secs(5),
            pid: 1,
            pgid: 1,
            process_log: run_a_dir.join(".runtime-job.log"),
            stream_rx: Some(stream_rx),
            completion_rx: Some(completion_rx),
            stream_lines: Vec::new(),
            attached: true,
            last_heartbeat: Instant::now(),
        };
        let mut app = App {
            root: PathBuf::from("/tmp"),
            limit: 20,
            runs: vec![run_a, run_b],
            selected: 1,
            wizard_selected: false,
            preview_scroll: 0,
            log_scroll: 0,
            mouse_capture_enabled: true,
            mode: Mode::Normal,
            notice: String::new(),
            last_output: String::new(),
            last_refresh: Instant::now(),
            job: Some(job),
            wizard_job: None,
        };

        let _ = handle_normal_key(
            &ctx,
            &mut app,
            KeyEvent::new(KeyCode::Char('c'), KeyModifiers::NONE),
        );

        assert!(matches!(app.mode, Mode::NewRunInput { .. }));

        let _ = fs::remove_dir_all(run_a_dir);
        let _ = fs::remove_dir_all(run_b_dir);
    }

    #[test]
    fn handle_normal_key_switches_to_selected_runs_active_job() {
        let ctx = test_context();
        let run_a_dir = temp_dir("switch-bg-a");
        let run_b_dir = temp_dir("switch-bg-b");
        fs::create_dir_all(run_b_dir.join("runtime")).expect("create runtime dir");
        runtime::start_job(&run_b_dir, "review", Some("review"), "start-next", 1, 1)
            .expect("start runtime job");

        let mut run_a = sample_run("partial", "rerun", "done");
        run_a.run_dir.clone_from(&run_a_dir);
        let mut run_b = sample_run("pending-verification", "review", "pending");
        run_b.run_dir.clone_from(&run_b_dir);
        let (_stream_tx, stream_rx) = mpsc::channel();
        let (_completion_tx, completion_rx) = mpsc::channel();
        let background = RunningJob {
            kind: JobKind::RunAction,
            label: "solver-a".to_string(),
            run_dir: run_a_dir.clone(),
            log_hint: Some("solver-a".to_string()),
            command_hint: "start-next".to_string(),
            started_at: Instant::now() - Duration::from_secs(5),
            started_wallclock: SystemTime::now() - Duration::from_secs(5),
            pid: 1,
            pgid: 1,
            process_log: run_a_dir.join(".runtime-job.log"),
            stream_rx: Some(stream_rx),
            completion_rx: Some(completion_rx),
            stream_lines: Vec::new(),
            attached: true,
            last_heartbeat: Instant::now(),
        };
        let mut app = App {
            root: PathBuf::from("/tmp"),
            limit: 20,
            runs: vec![run_a, run_b],
            selected: 0,
            wizard_selected: false,
            preview_scroll: 0,
            log_scroll: 0,
            mouse_capture_enabled: true,
            mode: Mode::Normal,
            notice: String::new(),
            last_output: String::new(),
            last_refresh: Instant::now(),
            job: Some(background),
            wizard_job: None,
        };

        let _ = handle_normal_key(
            &ctx,
            &mut app,
            KeyEvent::new(KeyCode::Char('j'), KeyModifiers::NONE),
        );

        assert_eq!(app.selected, 1);
        assert_eq!(
            app.job.as_ref().map(|job| job.run_dir.clone()),
            Some(run_b_dir.clone())
        );

        let _ = runtime::finish_job(&run_b_dir, "completed", Some(0), None);
        let _ = fs::remove_dir_all(run_a_dir);
        let _ = fs::remove_dir_all(run_b_dir);
    }

    #[test]
    fn delete_run_if_safe_refuses_active_run() {
        let run_dir = temp_dir("delete-active");
        runtime::start_job(&run_dir, "resume", Some("intake"), "resume", 1, 1)
            .expect("start runtime job");
        let mut app = App {
            root: PathBuf::from("/tmp"),
            limit: 20,
            runs: Vec::new(),
            selected: 0,
            wizard_selected: false,
            preview_scroll: 0,
            log_scroll: 0,
            mouse_capture_enabled: true,
            mode: Mode::Normal,
            notice: String::new(),
            last_output: String::new(),
            last_refresh: Instant::now(),
            job: None,
            wizard_job: None,
        };

        let deleted = delete_run_if_safe(&mut app, &run_dir).expect("safe delete check");

        assert!(!deleted);
        assert!(run_dir.exists());
        assert!(app.notice.contains("Cannot delete"));

        let _ = runtime::finish_job(&run_dir, "completed", Some(0), None);
        let _ = fs::remove_dir_all(run_dir);
    }

    #[test]
    fn poll_job_clears_owned_job_when_runtime_state_is_already_completed() {
        let ctx = test_context();
        let run_dir = temp_dir("owned-job-runtime-completed");
        runtime::start_pending_job(&run_dir, "start-next", Some("intake"), "start-next")
            .expect("start pending runtime job");
        runtime::finish_job(&run_dir, "completed", Some(0), None).expect("finish runtime job");

        let mut run = sample_run("pending-verification", "solver-a", "pending");
        run.run_dir.clone_from(&run_dir);
        let (_stream_tx, stream_rx) = mpsc::channel();
        let (_completion_tx, completion_rx) = mpsc::channel::<super::JobResult>();
        let mut app = App {
            root: PathBuf::from("/tmp"),
            limit: 20,
            runs: vec![run],
            selected: 0,
            wizard_selected: false,
            preview_scroll: 0,
            log_scroll: 0,
            mouse_capture_enabled: true,
            mode: Mode::Normal,
            notice: String::new(),
            last_output: String::new(),
            last_refresh: Instant::now(),
            job: Some(RunningJob {
                kind: JobKind::RunAction,
                label: "start-next".to_string(),
                run_dir: run_dir.clone(),
                log_hint: Some("intake".to_string()),
                command_hint: "start-next".to_string(),
                started_at: Instant::now() - Duration::from_secs(5),
                started_wallclock: SystemTime::now() - Duration::from_secs(5),
                pid: 0,
                pgid: 0,
                process_log: run_dir.join("runtime").join("process.log"),
                stream_rx: Some(stream_rx),
                completion_rx: Some(completion_rx),
                stream_lines: Vec::new(),
                attached: false,
                last_heartbeat: Instant::now(),
            }),
            wizard_job: None,
        };

        poll_job(&ctx, &mut app).expect("poll job");

        assert!(app.job.is_none());
        assert!(app.notice.contains("completed"));

        let _ = fs::remove_dir_all(run_dir);
    }

    #[test]
    fn poll_job_reconciles_stale_resume_stage_with_current_next_stage() {
        let ctx = test_context();
        let run_dir = temp_dir("resume-stage-reconcile");
        runtime::start_job(
            &run_dir,
            "resume",
            Some("intake"),
            "resume until verification",
            std::process::id() as i32,
            std::process::id() as i32,
        )
        .expect("start runtime job");

        let mut run = sample_run("pending-verification", "solver-a", "pending");
        run.run_dir.clone_from(&run_dir);
        run.status
            .stages
            .insert("intake".to_string(), "done".to_string());
        run.status
            .stages
            .insert("solver-a".to_string(), "pending".to_string());
        run.doctor
            .stages
            .insert("intake".to_string(), "done".to_string());
        run.doctor
            .stages
            .insert("solver-a".to_string(), "pending".to_string());

        let (_stream_tx, stream_rx) = mpsc::channel();
        let (_completion_tx, completion_rx) = mpsc::channel::<super::JobResult>();
        let mut app = App {
            root: PathBuf::from("/tmp"),
            limit: 20,
            runs: vec![run],
            selected: 0,
            wizard_selected: false,
            preview_scroll: 0,
            log_scroll: 0,
            mouse_capture_enabled: true,
            mode: Mode::Normal,
            notice: String::new(),
            last_output: String::new(),
            last_refresh: Instant::now(),
            job: Some(RunningJob {
                kind: JobKind::RunAction,
                label: "resume".to_string(),
                run_dir: run_dir.clone(),
                log_hint: Some("intake".to_string()),
                command_hint: "resume until verification".to_string(),
                started_at: Instant::now() - Duration::from_secs(5),
                started_wallclock: SystemTime::now() - Duration::from_secs(5),
                pid: std::process::id() as i32,
                pgid: std::process::id() as i32,
                process_log: run_dir.join("runtime").join("process.log"),
                stream_rx: Some(stream_rx),
                completion_rx: Some(completion_rx),
                stream_lines: Vec::new(),
                attached: false,
                last_heartbeat: Instant::now(),
            }),
            wizard_job: None,
        };

        poll_job(&ctx, &mut app).expect("poll job");

        assert_eq!(
            app.job.as_ref().and_then(|job| job.log_hint.as_deref()),
            Some("solver-a")
        );
        assert_eq!(
            runtime::load_job_state(&run_dir)
                .and_then(|state| state.stage)
                .as_deref(),
            Some("solver-a")
        );

        let _ = runtime::finish_job(&run_dir, "completed", Some(0), None);
        let _ = fs::remove_dir_all(run_dir);
    }

    #[test]
    fn poll_job_keeps_owned_job_when_runtime_state_is_temporarily_missing() {
        let ctx = test_context();
        let run_dir = temp_dir("owned-job-missing-runtime-state");
        let mut run = sample_run("pending-verification", "solver-a", "pending");
        run.run_dir.clone_from(&run_dir);

        let (_stream_tx, stream_rx) = mpsc::channel();
        let (_completion_tx, completion_rx) = mpsc::channel::<super::JobResult>();
        let mut app = App {
            root: PathBuf::from("/tmp"),
            limit: 20,
            runs: vec![run],
            selected: 0,
            wizard_selected: false,
            preview_scroll: 0,
            log_scroll: 0,
            mouse_capture_enabled: true,
            mode: Mode::Normal,
            notice: String::new(),
            last_output: String::new(),
            last_refresh: Instant::now(),
            job: Some(RunningJob {
                kind: JobKind::RunAction,
                label: "resume".to_string(),
                run_dir: run_dir.clone(),
                log_hint: Some("solver-a".to_string()),
                command_hint: "resume until verification".to_string(),
                started_at: Instant::now() - Duration::from_secs(2),
                started_wallclock: SystemTime::now() - Duration::from_secs(2),
                pid: 0,
                pgid: 0,
                process_log: run_dir.join("runtime").join("process.log"),
                stream_rx: Some(stream_rx),
                completion_rx: Some(completion_rx),
                stream_lines: Vec::new(),
                attached: false,
                last_heartbeat: Instant::now(),
            }),
            wizard_job: None,
        };

        poll_job(&ctx, &mut app).expect("poll job");

        assert!(
            app.job.is_some(),
            "owned job should not be cleared without completion"
        );

        let _ = fs::remove_dir_all(run_dir);
    }

    #[test]
    fn handle_paste_keeps_multiline_task_text_in_new_run_input() {
        let mut app = App {
            root: PathBuf::from("/tmp"),
            limit: 20,
            runs: vec![sample_run("pending-verification", "intake", "pending")],
            selected: 0,
            wizard_selected: false,
            preview_scroll: 0,
            log_scroll: 0,
            mouse_capture_enabled: true,
            mode: Mode::NewRunInput {
                draft: NewRunDraft {
                    task: String::new(),
                    workspace: "/tmp/workspace".to_string(),
                    title: String::new(),
                    task_cursor: 0,
                    workspace_cursor: "/tmp/workspace".chars().count(),
                    title_cursor: 0,
                    field: NewRunField::Task,
                },
                task_scroll: 0,
            },
            notice: String::new(),
            last_output: String::new(),
            last_refresh: Instant::now(),
            job: None,
            wizard_job: None,
        };

        handle_paste(
            &mut app,
            "# Fix Qwen Analysis IR Failure\n\nLLM_FREECAD_GOAL_MODE=qwen_only\n",
        );

        match &app.mode {
            Mode::NewRunInput { draft, .. } => {
                assert!(draft.task.contains("Fix Qwen Analysis IR Failure"));
                assert!(draft.task.contains("qwen_only"));
                assert!(draft.task.contains('\n'));
            }
            _ => panic!("expected new run input mode"),
        }
    }

    #[test]
    fn handle_paste_strips_terminal_escape_sequences_from_task_text() {
        let mut app = App {
            root: PathBuf::from("/tmp"),
            limit: 20,
            runs: vec![sample_run("pending-verification", "intake", "pending")],
            selected: 0,
            wizard_selected: false,
            preview_scroll: 0,
            log_scroll: 0,
            mouse_capture_enabled: true,
            mode: Mode::NewRunInput {
                draft: NewRunDraft {
                    task: String::new(),
                    workspace: "/tmp/workspace".to_string(),
                    title: String::new(),
                    task_cursor: 0,
                    workspace_cursor: "/tmp/workspace".chars().count(),
                    title_cursor: 0,
                    field: NewRunField::Task,
                },
                task_scroll: 0,
            },
            notice: String::new(),
            last_output: String::new(),
            last_refresh: Instant::now(),
            job: None,
            wizard_job: None,
        };

        handle_paste(
            &mut app,
            "- no e\u{1b}]11;rgb:0000/0000/0000\u{1b}\\xplicit `--template`\n",
        );

        match &app.mode {
            Mode::NewRunInput { draft, .. } => {
                assert_eq!(draft.task, "- no explicit `--template`\n");
                assert!(!draft.task.contains('\u{1b}'));
            }
            _ => panic!("expected new run input mode"),
        }
    }

    #[test]
    fn new_run_input_treats_q_as_text_not_cancel() {
        let ctx = test_context();
        let mut app = App {
            root: PathBuf::from("/tmp"),
            limit: 20,
            runs: vec![sample_run("pending-verification", "intake", "pending")],
            selected: 0,
            wizard_selected: false,
            preview_scroll: 0,
            log_scroll: 0,
            mouse_capture_enabled: true,
            mode: Mode::NewRunInput {
                draft: NewRunDraft {
                    task: String::new(),
                    workspace: "/tmp/workspace".to_string(),
                    title: String::new(),
                    task_cursor: 0,
                    workspace_cursor: "/tmp/workspace".chars().count(),
                    title_cursor: 0,
                    field: NewRunField::Task,
                },
                task_scroll: 0,
            },
            notice: String::new(),
            last_output: String::new(),
            last_refresh: Instant::now(),
            job: None,
            wizard_job: None,
        };

        handle_new_run_key(
            &ctx,
            &mut app,
            KeyEvent::new(KeyCode::Char('q'), KeyModifiers::NONE),
        )
        .expect("handle q");

        match &app.mode {
            Mode::NewRunInput { draft, .. } => assert_eq!(draft.task, "q"),
            _ => panic!("expected new run input mode"),
        }
    }

    #[test]
    fn new_run_input_moves_cursor_left_and_inserts_in_place() {
        let ctx = test_context();
        let mut app = App {
            root: PathBuf::from("/tmp"),
            limit: 20,
            runs: vec![sample_run("pending-verification", "intake", "pending")],
            selected: 0,
            wizard_selected: false,
            preview_scroll: 0,
            log_scroll: 0,
            mouse_capture_enabled: true,
            mode: Mode::NewRunInput {
                draft: NewRunDraft {
                    task: "abcd".to_string(),
                    workspace: "/tmp/workspace".to_string(),
                    title: String::new(),
                    task_cursor: 4,
                    workspace_cursor: "/tmp/workspace".chars().count(),
                    title_cursor: 0,
                    field: NewRunField::Task,
                },
                task_scroll: 0,
            },
            notice: String::new(),
            last_output: String::new(),
            last_refresh: Instant::now(),
            job: None,
            wizard_job: None,
        };

        handle_new_run_key(
            &ctx,
            &mut app,
            KeyEvent::new(KeyCode::Left, KeyModifiers::NONE),
        )
        .expect("move cursor left");
        handle_new_run_key(
            &ctx,
            &mut app,
            KeyEvent::new(KeyCode::Char('X'), KeyModifiers::NONE),
        )
        .expect("insert at cursor");

        match &app.mode {
            Mode::NewRunInput { draft, .. } => {
                assert_eq!(draft.task, "abcXd");
                assert_eq!(draft.task_cursor, 4);
            }
            _ => panic!("expected new run input mode"),
        }
    }

    #[test]
    fn mouse_wheel_over_preview_scrolls_request_without_changing_selected_run() {
        let mut app = App {
            root: PathBuf::from("/tmp"),
            limit: 20,
            runs: vec![
                sample_run("pending-verification", "intake", "pending"),
                sample_run("partial", "rerun", "done"),
            ],
            selected: 0,
            wizard_selected: false,
            preview_scroll: 0,
            log_scroll: 0,
            mouse_capture_enabled: true,
            mode: Mode::Normal,
            notice: String::new(),
            last_output: String::new(),
            last_refresh: Instant::now(),
            job: None,
            wizard_job: None,
        };
        let rects = normal_mode_rects(Rect::new(0, 0, 120, 40));

        handle_mouse(
            &mut app,
            MouseEvent {
                kind: MouseEventKind::ScrollDown,
                column: rects.preview.x.saturating_add(2),
                row: rects.preview.y.saturating_add(2),
                modifiers: KeyModifiers::NONE,
            },
            Rect::new(0, 0, 120, 40),
        );

        assert_eq!(app.selected, 0);
        assert_eq!(app.preview_scroll, 3);
        assert_eq!(app.log_scroll, 0);
    }

    #[test]
    fn mouse_wheel_scrolls_artifact_view_request_popup() {
        let run = sample_run("pending-verification", "intake", "pending");
        let mut app = App {
            root: PathBuf::from("/tmp"),
            limit: 20,
            runs: vec![run],
            selected: 0,
            wizard_selected: false,
            preview_scroll: 0,
            log_scroll: 0,
            mouse_capture_enabled: true,
            mode: Mode::ArtifactView {
                kind: ArtifactKind::Summary,
                scroll: 0,
            },
            notice: String::new(),
            last_output: String::new(),
            last_refresh: Instant::now(),
            job: None,
            wizard_job: None,
        };

        handle_mouse(
            &mut app,
            MouseEvent {
                kind: MouseEventKind::ScrollDown,
                column: 40,
                row: 12,
                modifiers: KeyModifiers::NONE,
            },
            Rect::new(0, 0, 120, 40),
        );

        match app.mode {
            Mode::ArtifactView { scroll, .. } => assert_eq!(scroll, 3),
            _ => panic!("artifact view should remain active"),
        }
    }
}
