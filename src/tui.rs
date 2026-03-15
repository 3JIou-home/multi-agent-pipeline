use crate::engine::{
    amend_run, automate_run, choose_prune_candidates, contextual_log_excerpt, delete_run,
    execute_safe_next_action, load_run_snapshots, run_stage_capture, task_flow_capture,
    with_engine_observer, Context, EngineObserver, RunSnapshot, RunTokenSummary,
};
use crate::runtime::{self, RuntimeJobState};
use crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers};
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
use std::sync::mpsc::{self, Receiver};
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
    },
    InterviewInput {
        draft: NewRunDraft,
        session_dir: PathBuf,
        goal_summary: String,
        questions: Vec<InterviewQuestion>,
        answers: Vec<String>,
        index: usize,
        buffer: String,
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

    fn path(self, run_dir: &Path) -> PathBuf {
        match self {
            Self::Summary => run_dir.join("review").join("user-summary.md"),
            Self::Findings => run_dir.join("verification").join("findings.md"),
            Self::Augmented => run_dir.join("verification").join("augmented-task.md"),
            Self::Execution => run_dir.join("execution").join("report.md"),
            Self::Brief => run_dir.join("brief.md"),
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

#[derive(Clone)]
struct NewRunDraft {
    task: String,
    workspace: String,
    title: String,
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
    let start = text
        .find('{')
        .ok_or_else(|| "No JSON object start found in process output.".to_string())?;
    let end = text
        .rfind('}')
        .ok_or_else(|| "No JSON object end found in process output.".to_string())?;
    if end <= start {
        return Err("Malformed JSON object boundaries in process output.".to_string());
    }
    serde_json::from_str(&text[start..=end])
        .map_err(|err| format!("Could not parse embedded JSON: {err}"))
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
    mode: Mode,
    notice: String,
    last_output: String,
    last_refresh: Instant,
    job: Option<RunningJob>,
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

struct JobResult {
    code: i32,
    stdout: String,
    stderr: String,
}

struct UiEngineObserver {
    run_dir: PathBuf,
    stream_tx: mpsc::Sender<String>,
}

impl EngineObserver for UiEngineObserver {
    fn process_started(&self, pid: i32, pgid: i32) {
        let _ = runtime::update_job_process(&self.run_dir, pid, pgid, Some("running"));
    }

    fn line(&self, line: &str) {
        let _ = runtime::append_process_line(&self.run_dir, line);
        let _ = self.stream_tx.send(line.to_string());
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
            mode: Mode::Normal,
            notice: "Ready".to_string(),
            last_output: String::new(),
            last_refresh: Instant::now(),
            job: None,
        };
        sync_runtime_job(&mut app);
        Ok(app)
    }

    fn refresh(&mut self, ctx: &Context) -> Result<(), String> {
        let selected_path = self.selected_run().map(|run| run.run_dir.clone());
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
        Ok(())
    }

    fn selected_run(&self) -> Option<&RunSnapshot> {
        self.runs.get(self.selected)
    }

    fn move_down(&mut self) {
        if !self.runs.is_empty() {
            self.selected = (self.selected + 1).min(self.runs.len() - 1);
        }
    }

    fn move_up(&mut self) {
        self.selected = self.selected.saturating_sub(1);
    }

    fn select_run_by_path(&mut self, run_dir: &Path) {
        if let Some(index) = self.runs.iter().position(|run| run.run_dir == run_dir) {
            self.selected = index;
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

fn sync_runtime_job(app: &mut App) {
    if let Some(job) = app.job.as_mut() {
        if job.attached {
            if let Some(state) = runtime::active_job_state(&job.run_dir) {
                job.refresh_from_state(state);
            } else {
                app.notice = format!(
                    "Observed job for {} is no longer running.",
                    job.run_dir
                        .file_name()
                        .and_then(|value| value.to_str())
                        .unwrap_or("run")
                );
                app.job = None;
            }
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
            let attached = RunningJob::attached(run_dir.clone(), state);
            app.notice = format!(
                "Attached to running {} for {}",
                attached.command_hint,
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

fn default_new_run_draft() -> NewRunDraft {
    NewRunDraft {
        task: String::new(),
        workspace: String::new(),
        title: String::new(),
        field: NewRunField::Task,
    }
}

fn active_new_run_buffer(draft: &mut NewRunDraft) -> &mut String {
    match draft.field {
        NewRunField::Task => &mut draft.task,
        NewRunField::Workspace => &mut draft.workspace,
        NewRunField::Title => &mut draft.title,
        NewRunField::Start | NewRunField::Cancel => &mut draft.title,
    }
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
    if app.job.is_some() {
        app.notice = "A job is already running.".to_string();
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
    if app.job.is_some() {
        app.notice = "Another action is already running.".to_string();
        return Ok(());
    }
    let run_dir = ui_job_dir(&app.root);
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
    )
}

fn spawn_interview_finalize_job(
    ctx: &Context,
    app: &mut App,
    draft: NewRunDraft,
    session_dir: PathBuf,
    answers: Vec<serde_json::Value>,
) -> Result<(), String> {
    if app.job.is_some() {
        app.notice = "Another action is already running.".to_string();
        return Ok(());
    }
    let answers_path = session_dir.join("answers-ui.json");
    std::fs::write(
        &answers_path,
        serde_json::to_vec_pretty(&answers)
            .map_err(|err| format!("Could not serialize interview answers: {err}"))?,
    )
    .map_err(|err| format!("Could not write {}: {err}", answers_path.display()))?;
    let run_dir = ui_job_dir(&app.root);
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
    )
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
    app.job = Some(RunningJob::owned(
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
    ));
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
    execute!(stdout, EnterAlternateScreen)
        .map_err(|err| format!("Could not enter alternate screen: {err}"))?;
    let backend = CrosstermBackend::new(stdout);
    Terminal::new(backend).map_err(|err| format!("Could not initialize terminal: {err}"))
}

fn restore_terminal(terminal: &mut Terminal<CrosstermBackend<Stdout>>) -> Result<(), String> {
    disable_raw_mode().map_err(|err| format!("Could not disable raw mode: {err}"))?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)
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
            if let Event::Key(key) = event {
                if key.kind == KeyEventKind::Press && handle_key(ctx, app, key)? {
                    return Ok(());
                }
            }
        }

        poll_job(ctx, app)?;

        let refresh_interval = if app.job.is_some() {
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
    if app.job.is_some() {
        match key.code {
            _ if key_is_char(key, 'q') => return Ok(true),
            KeyCode::Esc => {
                app.notice =
                    "A job is still running. Use i to interrupt or keep monitoring it.".to_string();
                return Ok(false);
            }
            _ if key_is_char(key, 'i') => {
                interrupt_job(app)?;
                return Ok(false);
            }
            KeyCode::Down if !matches!(key.code, KeyCode::Char(_)) => {
                app.move_down();
                return Ok(false);
            }
            _ if key_is_char(key, 'j') => {
                app.move_down();
                return Ok(false);
            }
            KeyCode::Up if !matches!(key.code, KeyCode::Char(_)) => {
                app.move_up();
                return Ok(false);
            }
            _ if key_is_char(key, 'k') => {
                app.move_up();
                return Ok(false);
            }
            _ if key_is_char(key, 'g') => {
                app.refresh(ctx)?;
                app.notice = "Refreshed run list".to_string();
                return Ok(false);
            }
            KeyCode::Enter => {
                if let Some(run) = app.selected_run() {
                    app.mode = Mode::ArtifactView {
                        kind: preferred_artifact_kind(run),
                        scroll: 0,
                    };
                }
                return Ok(false);
            }
            _ if key_is_char(key, 'o') => {
                if let Some(run) = app.selected_run() {
                    app.mode = Mode::ArtifactView {
                        kind: preferred_artifact_kind(run),
                        scroll: 0,
                    };
                }
                return Ok(false);
            }
            KeyCode::Char('1') => {
                app.mode = Mode::ArtifactView {
                    kind: ArtifactKind::Summary,
                    scroll: 0,
                };
                return Ok(false);
            }
            KeyCode::Char('2') => {
                app.mode = Mode::ArtifactView {
                    kind: ArtifactKind::Findings,
                    scroll: 0,
                };
                return Ok(false);
            }
            KeyCode::Char('3') => {
                app.mode = Mode::ArtifactView {
                    kind: ArtifactKind::Augmented,
                    scroll: 0,
                };
                return Ok(false);
            }
            KeyCode::Char('4') => {
                app.mode = Mode::ArtifactView {
                    kind: ArtifactKind::Execution,
                    scroll: 0,
                };
                return Ok(false);
            }
            KeyCode::Char('5') => {
                app.mode = Mode::ArtifactView {
                    kind: ArtifactKind::Brief,
                    scroll: 0,
                };
                return Ok(false);
            }
            _ => {
                app.notice =
                    "An action is still running. Use i to interrupt it, or open artifacts in read-only mode."
                        .to_string();
                return Ok(false);
            }
        }
    }

    match key.code {
        _ if key.modifiers.contains(KeyModifiers::CONTROL) && key_is_char(key, 'c') => {
            return Ok(true)
        }
        _ if key_is_char(key, 'q') => return Ok(true),
        KeyCode::Esc => app.notice = "Ready".to_string(),
        KeyCode::Down => app.move_down(),
        _ if key_is_char(key, 'j') => app.move_down(),
        KeyCode::Up => app.move_up(),
        _ if key_is_char(key, 'k') => app.move_up(),
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
            };
        }
        _ if key_is_char(key, 's') => {
            spawn_action(ctx, app, "safe-next", vec!["safe-next".to_string()])?
        }
        _ if key_is_char(key, 'n') => {
            spawn_action(ctx, app, "start-next", vec!["start-next".to_string()])?
        }
        _ if key_is_char(key, 'r') => spawn_action(
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
            "Amendment saved. Press n for the next stage or r to resume the pipeline.".to_string();
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
    if let Mode::NewRunInput { draft } = &mut app.mode {
        match key.code {
            KeyCode::Char('q') if !key.modifiers.contains(KeyModifiers::CONTROL) => cancel = true,
            KeyCode::Esc => cancel = true,
            KeyCode::Enter => match draft.field {
                NewRunField::Task => draft.task.push('\n'),
                NewRunField::Workspace => draft.field = NewRunField::Title,
                NewRunField::Title => draft.field = NewRunField::Start,
                NewRunField::Start => submit = true,
                NewRunField::Cancel => cancel = true,
            },
            KeyCode::BackTab => draft.field = draft.field.previous(),
            KeyCode::Tab | KeyCode::Down => draft.field = draft.field.next(),
            KeyCode::Up => draft.field = draft.field.previous(),
            KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                match draft.field {
                    NewRunField::Task => draft.task.clear(),
                    NewRunField::Workspace => draft.workspace.clear(),
                    NewRunField::Title => draft.title.clear(),
                    NewRunField::Start | NewRunField::Cancel => {}
                }
            }
            KeyCode::Backspace => match draft.field {
                NewRunField::Task | NewRunField::Workspace | NewRunField::Title => {
                    active_new_run_buffer(draft).pop();
                }
                NewRunField::Start | NewRunField::Cancel => {}
            },
            KeyCode::Char(ch) => match draft.field {
                NewRunField::Task | NewRunField::Workspace | NewRunField::Title => {
                    active_new_run_buffer(draft).push(ch);
                }
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
            Mode::NewRunInput { draft } => draft.clone(),
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
    if let Mode::InterviewInput { buffer, .. } = &mut app.mode {
        match key.code {
            KeyCode::Char('q') if !key.modifiers.contains(KeyModifiers::CONTROL) => cancel = true,
            KeyCode::Esc => cancel = true,
            KeyCode::Enter | KeyCode::Down => advance = true,
            KeyCode::Up | KeyCode::BackTab => {
                if let Mode::InterviewInput {
                    answers,
                    index,
                    buffer,
                    ..
                } = &mut app.mode
                {
                    answers[*index].clone_from(buffer);
                    if *index > 0 {
                        *index -= 1;
                        buffer.clone_from(&answers[*index]);
                    }
                }
                return Ok(false);
            }
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
            KeyCode::Esc => {
                app.mode = Mode::Normal;
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
                        "Run created. Press n for the next stage or r to resume.".to_string();
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
                delete_run(&run_dir)?;
                app.notice = format!("Deleted {}", run_dir.display());
                app.last_output.clone_from(&app.notice);
                app.mode = Mode::Normal;
                app.refresh(ctx)?;
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
                    delete_run(&run_dir)?;
                    app.notice = format!("Deleted {}", run_dir.display());
                    app.last_output.clone_from(&app.notice);
                    app.mode = Mode::Normal;
                    app.refresh(ctx)?;
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
            let count = candidates.len();
            for run_dir in candidates {
                delete_run(&run_dir)?;
            }
            app.notice = format!("Pruned {count} run(s)");
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
                let count = candidates.len();
                for run_dir in candidates {
                    delete_run(&run_dir)?;
                }
                app.notice = format!("Pruned {count} run(s)");
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
    if app.job.is_some() {
        app.notice = "Another action is already running.".to_string();
        return Ok(());
    }
    if let Some(state) = runtime::active_job_state(&run_dir) {
        app.job = Some(RunningJob::attached(run_dir.clone(), state));
        app.notice =
            "This run already has an active job. Attached to the existing process.".to_string();
        return Ok(());
    }
    let label_text = label.to_string();
    let command_hint = match label {
        "start-next" => "start-next".to_string(),
        "safe-next" => "safe-next-action".to_string(),
        "resume" => "resume until verification".to_string(),
        other => other.to_string(),
    };
    app.notice = format!(
        "Running {label} for {}",
        run_dir
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("run")
    );
    app.last_output.clear();
    let log_hint = app
        .selected_run()
        .and_then(|run| infer_log_hint(label, run));
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

fn poll_job(ctx: &Context, app: &mut App) -> Result<(), String> {
    let mut finished = None;
    if let Some(job) = app.job.as_mut() {
        if let Some(rx) = job.stream_rx.as_ref() {
            while rx.try_recv().is_ok() {}
        }
        let fresh_lines = runtime::tail_process_log(&job.run_dir, 40);
        if !fresh_lines.is_empty() {
            job.stream_lines = fresh_lines;
        }
        if job.last_heartbeat.elapsed() >= Duration::from_secs(1) {
            let status = if let Some(run) = app.runs.iter().find(|run| run.run_dir == job.run_dir) {
                if job_stalled(run, job) {
                    "stalled"
                } else {
                    "running"
                }
            } else {
                "running"
            };
            let _ = runtime::touch_job(&job.run_dir, status);
            job.last_heartbeat = Instant::now();
        }

        let mut completion = None;
        if let Some(rx) = job.completion_rx.as_ref() {
            if let Ok(result) = rx.try_recv() {
                completion = Some(result);
            }
        } else if runtime::active_job_state(&job.run_dir).is_none() {
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

    if let Some((kind, label, run_dir, completed_log_hint, result, detached_finish)) = finished {
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
        app.job = None;
        match kind {
            JobKind::RunAction => {
                app.refresh(ctx)?;
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
                    match parse_embedded_json::<InterviewQuestionsPayload>(&raw_output) {
                        Ok(payload) => {
                            if payload.questions.is_empty() {
                                let created =
                                    create_run_from_draft(ctx, app, &draft, None, Path::new(""))?;
                                app.refresh(ctx)?;
                                app.select_run_by_path(&created);
                                app.notice =
                                    "Run created. Press n for the next stage or r to resume."
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
                                };
                                app.notice = "Stage0 interview questions are ready.".to_string();
                            }
                        }
                        Err(err) => {
                            app.mode = Mode::NewRunInput { draft };
                            app.notice = format!("Could not parse interview questions JSON: {err}");
                        }
                    }
                } else {
                    app.mode = Mode::NewRunInput { draft };
                }
            }
            JobKind::InterviewFinalize {
                draft,
                session_dir,
                answers,
            } => {
                if exit_code == 0 {
                    match parse_embedded_json::<InterviewFinalizePayload>(&raw_output) {
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
                            };
                            app.notice = format!("Could not parse interview finalize JSON: {err}");
                        }
                    }
                } else {
                    app.mode = Mode::NewRunInput { draft };
                }
            }
        }
    }
    Ok(())
}

fn interrupt_job(app: &mut App) -> Result<(), String> {
    let Some(job) = app.job.as_mut() else {
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
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(2),
            Constraint::Length(1),
            Constraint::Min(10),
            Constraint::Length(15),
            Constraint::Length(1),
        ])
        .split(size);

    draw_header(frame, layout[0], app);
    draw_status_bar(frame, layout[1], app);
    draw_body(frame, layout[2], app);
    draw_logs(frame, layout[3], app);
    draw_footer(frame, layout[4], app);

    match &app.mode {
        Mode::AmendInput { buffer } => draw_amend_popup(frame, size, buffer),
        Mode::NewRunInput { draft } => draw_new_run_popup(frame, size, draft),
        Mode::InterviewInput {
            goal_summary,
            questions,
            answers,
            index,
            buffer,
            ..
        } => draw_interview_popup(
            frame,
            size,
            goal_summary,
            questions,
            answers,
            *index,
            buffer,
        ),
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
    let running = app
        .job
        .as_ref()
        .map(|job| format!(" | running: {}", running_job_label(job)))
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
        Mode::ArtifactView { .. } => "q quit  Esc close  j/k scroll  PgUp/PgDn scroll  [ ] switch",
        Mode::ConfirmDelete { .. } => "Esc cancel  Enter apply  Left/Right switch",
        Mode::ConfirmPrune { .. } => "Esc cancel  Enter apply  Left/Right switch",
        Mode::AmendInput { .. } => "Esc cancel  Enter save+rewind",
        Mode::NewRunInput { .. } => "Tab switch fields  Enter edit/apply  Esc cancel",
        Mode::InterviewInput { .. } => "Enter next  Up previous  Esc cancel",
        Mode::PromptReview { .. } => "Enter apply  Tab switch action  j/k scroll  Esc cancel",
        Mode::Normal if app.job.is_some() => "q quit  Esc clear  i interrupt  j/k move  Enter open",
        Mode::Normal => {
            "q quit  Esc clear  j/k move  Enter open  c create  n next  r resume  a amend"
        }
    }
}

fn draw_footer(frame: &mut ratatui::Frame<'_>, area: Rect, app: &App) {
    let paragraph = Paragraph::new(footer_shortcuts(app))
        .style(Style::default().bg(Color::DarkGray).fg(Color::White));
    frame.render_widget(paragraph, area);
}

fn draw_status_bar(frame: &mut ratatui::Frame<'_>, area: Rect, app: &App) {
    let text = if let Some(run) = app.selected_run() {
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
                        format!(" | running={} | stalled", running_job_label(job))
                    } else {
                        format!(" | running={}", running_job_label(job))
                    }
                } else {
                    format!(" | background={}", running_job_label(job))
                }
            })
            .unwrap_or_default();
        format!(
            "run={run_name} | health={} | goal={} | next={} | host={}{}{}",
            run.doctor.health,
            ui_goal_state(run),
            run.doctor.next,
            run.doctor.host_probe,
            status_bar_tokens(&run.token_summary),
            running
        )
    } else {
        "No run selected".to_string()
    };
    let paragraph =
        Paragraph::new(text).style(Style::default().bg(Color::DarkGray).fg(Color::White));
    frame.render_widget(paragraph, area);
}

fn draw_body(frame: &mut ratatui::Frame<'_>, area: Rect, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(32), Constraint::Percentage(68)])
        .split(area);
    draw_runs(frame, chunks[0], app);
    draw_details(frame, chunks[1], app);
}

fn draw_runs(frame: &mut ratatui::Frame<'_>, area: Rect, app: &App) {
    let items: Vec<ListItem<'_>> = if app.runs.is_empty() {
        vec![ListItem::new("No runs found.")]
    } else {
        app.runs
            .iter()
            .map(|run| {
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
                            format!(" | running={} | stalled", running_job_label(job))
                        } else {
                            format!(" | running={}", running_job_label(job))
                        }
                    })
                    .unwrap_or_default();
                let line1 = run_name.to_string();
                let line2 = format!(
                    "health={} | next={} | goal={}{}",
                    run.doctor.health,
                    run.doctor.next,
                    ui_goal_state(run),
                    running
                );
                ListItem::new(vec![Line::from(line1), Line::from(line2)])
            })
            .collect()
    };
    let list = List::new(items)
        .block(
            Block::default()
                .title("Runs (health | next | goal)")
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
    if !app.runs.is_empty() {
        state.select(Some(app.selected));
    }
    frame.render_stateful_widget(list, area, &mut state);
}

fn draw_details(frame: &mut ratatui::Frame<'_>, area: Rect, app: &App) {
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(58), Constraint::Percentage(42)])
        .split(area);
    if app.selected_run().is_none() {
        if let Some(job) = &app.job {
            if matches!(job.kind, JobKind::RunAction) {
                // Fall through to the generic details path below for normal run actions.
            } else {
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
                let preview = Paragraph::new(preview_text)
                    .block(
                        Block::default()
                            .title("Creating Pipeline")
                            .borders(Borders::ALL),
                    )
                    .wrap(Wrap { trim: false });
                let detail = Paragraph::new(detail_text)
                    .block(Block::default().title("Activity").borders(Borders::ALL))
                    .wrap(Wrap { trim: false });
                frame.render_widget(preview, vertical[0]);
                frame.render_widget(detail, vertical[1]);
                return;
            }
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
                format!("Goal: {}", ui_goal_state(run)),
                format!("Next: {}", run.doctor.next),
                format!("Safe action: {}", run.doctor.safe_next_action),
                format!("Host probe: {}", run.doctor.host_probe),
                format!("Tokens: {}", run_token_summary_line(&run.token_summary)),
            ];
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
            let (preview_title, preview_text) = live_preview(app, run);
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
    let preview = Paragraph::new(preview_text)
        .block(Block::default().title(preview_title).borders(Borders::ALL))
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
    let (title, content) = if let Some(job) = &app.job {
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
                        "Process: {}{}",
                        job.command_hint,
                        if job.attached { " (attached)" } else { "" }
                    ),
                    format!("Run: {run_name}"),
                    format!("Log source: {log_title}"),
                    String::new(),
                ];
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
                    format!("Running: {}", running_job_label(job)),
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
    let logs = Paragraph::new(content)
        .block(Block::default().title(title).borders(Borders::ALL))
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

fn running_job_label(job: &RunningJob) -> String {
    let elapsed = job.started_at.elapsed().as_secs();
    if let Some(stage) = &job.log_hint {
        format!("{stage} ({elapsed}s)")
    } else {
        format!("{} ({elapsed}s)", job.label)
    }
}

fn live_preview(app: &App, run: &RunSnapshot) -> (String, String) {
    if let Some(job) = &app.job {
        if job.run_dir == run.run_dir {
            let stage = job.log_hint.as_deref().unwrap_or("current-stage");
            let live_path = run.run_dir.join("logs").join(format!("{stage}.last.md"));
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
                    "Stage `{stage}` is currently running.\n\nLauncher command: {}\nElapsed: {}s\n\nCurrent next field: {}\nGoal status: {}\nSafe action: {}\n\n{}\n\nUse the lower log pane for live agent output.",
                    job.command_hint,
                    job.started_at.elapsed().as_secs(),
                    run.doctor.next,
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
    let stage = job.log_hint.as_deref().unwrap_or("current-stage");
    let last_path = run.run_dir.join("logs").join(format!("{stage}.last.md"));
    if !is_fresh_for_job(&last_path, job) {
        return (
            format!("Waiting for {stage}"),
            vec![
                format!("No fresh live output yet for stage `{stage}`."),
                "The previous .last.md belongs to an older attempt and is being ignored."
                    .to_string(),
                format!("Launcher command: {}", job.command_hint),
                format!("Elapsed: {}s", job.started_at.elapsed().as_secs()),
            ],
        );
    }
    contextual_log_excerpt(
        &run.run_dir,
        job.log_hint.as_deref(),
        Some(&run.status.next),
        12,
    )
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

fn draw_new_run_popup(frame: &mut ratatui::Frame<'_>, area: Rect, draft: &NewRunDraft) {
    let popup = centered_rect(78, 44, area);
    frame.render_widget(Clear, popup);
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(8),
            Constraint::Length(12),
            Constraint::Length(5),
            Constraint::Length(5),
            Constraint::Length(3),
            Constraint::Length(3),
        ])
        .split(popup);
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
        "Create a new pipeline.\n\nFill the task, set an explicit workspace, then start stage0 interview.\n\nActive field: {field_name}"
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
        ),
        layout[3],
    );
    frame.render_widget(
        Paragraph::new("Tab switches fields. Enter edits/applies. Esc cancels.")
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

fn draw_interview_popup(
    frame: &mut ratatui::Frame<'_>,
    area: Rect,
    goal_summary: &str,
    questions: &[InterviewQuestion],
    answers: &[String],
    index: usize,
    buffer: &str,
) {
    let popup = centered_rect(82, 56, area);
    frame.render_widget(Clear, popup);
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(5),
            Constraint::Length(7),
            Constraint::Length(10),
            Constraint::Min(10),
            Constraint::Length(8),
        ])
        .split(popup);
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
            "{progress}\nUse Up to revisit the previous answer.\nEnter saves and continues. Esc cancels."
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
        ),
        layout[4],
    );
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
            Span::styled(" Create + Start ", start_style),
            Span::raw("   "),
            Span::styled(" Cancel ", cancel_style),
        ]))
        .block(Block::default().borders(Borders::ALL)),
        layout[2],
    );
}

fn field_block(title: &str, value: &str, active: bool) -> Paragraph<'static> {
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
        .wrap(Wrap { trim: false })
}

fn action_panel_text(app: &App, run: Option<&RunSnapshot>) -> String {
    if let Some(job) = &app.job {
        if let Some(run) = run {
            if job.run_dir == run.run_dir {
                return format!(
                    "Primary\n- i interrupt current stage\n- Enter open preferred artifact\n- 1 Summary  2 Findings  3 Augmented  4 Execution  5 Brief\n\nCurrent\n- running: {}\n- next: {}\n- safe action: {}\n",
                    running_job_label(job),
                    run.doctor.next,
                    run.doctor.safe_next_action
                );
            }
        }
        return format!(
            "Background job\n- {}\n- Press i to interrupt\n- j/k move between runs\n- Enter open artifacts for the selected run\n",
            running_job_label(job)
        );
    }
    if let Some(run) = run {
        format!(
            "Primary\n- n next stage\n- r resume pipeline\n- a amend and rewind\n- Enter open preferred artifact\n\nSecondary\n- y rerun\n- h refresh host probe\n- u refresh prompts\n- x delete run\n- p prune runs\n\nCurrent\n- next: {}\n- safe action: {}\n",
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
    let title = format!(
        "{}  scroll j/k PgUp/PgDn  switch [ ] or 1..5  Esc close",
        kind.label()
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
        "Findings" => ArtifactKind::Findings,
        "Augmented" => ArtifactKind::Augmented,
        "Execution" => ArtifactKind::Execution,
        "Brief" => ArtifactKind::Brief,
        _ => ArtifactKind::Summary,
    }
}

fn artifact_content(run: &RunSnapshot, kind: ArtifactKind) -> String {
    let path = kind.path(&run.run_dir);
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
        ArtifactKind::Summary => "This artifact is generated after the review stage completes.",
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

#[cfg(test)]
mod tests {
    use super::{key_is_char, parse_embedded_json, ui_goal_state, InterviewQuestionsPayload};
    use crate::engine::{DoctorPayload, RunSnapshot, RunTokenSummary, StatusPayload};
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
    use std::collections::BTreeMap;
    use std::path::PathBuf;

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
    fn key_aliases_work_in_cyrillic_layout_for_global_shortcuts() {
        let quit_key = KeyEvent::new(KeyCode::Char('й'), KeyModifiers::NONE);
        let next_key = KeyEvent::new(KeyCode::Char('т'), KeyModifiers::NONE);
        let resume_key = KeyEvent::new(KeyCode::Char('к'), KeyModifiers::NONE);

        assert!(key_is_char(quit_key, 'q'));
        assert!(key_is_char(next_key, 'n'));
        assert!(key_is_char(resume_key, 'r'));
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
}
