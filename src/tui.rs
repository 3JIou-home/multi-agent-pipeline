use crate::engine::{
    amend_run, automate_run, choose_prune_candidates, delete_run, execute_safe_next_action,
    latest_log_excerpt, load_run_snapshots, run_stage_capture, Context, RunSnapshot,
};
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
use std::io::{self, Stdout};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver};
use std::thread;
use std::time::{Duration, Instant};

const DEFAULT_KEEP: usize = 20;
const DEFAULT_PRUNE_DAYS: u64 = 14;

enum Mode {
    Normal,
    AmendInput { buffer: String },
    ConfirmDelete { selected: ConfirmChoice },
    ConfirmPrune { selected: ConfirmChoice },
    ArtifactView { kind: ArtifactKind, scroll: u16 },
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
    label: String,
    run_dir: PathBuf,
    receiver: Receiver<Result<crate::engine::CommandResult, String>>,
}

impl App {
    fn new(ctx: &Context, root: PathBuf, limit: usize) -> Result<Self, String> {
        let runs = load_run_snapshots(ctx, &root, limit)?;
        Ok(Self {
            root,
            limit,
            runs,
            selected: 0,
            mode: Mode::Normal,
            notice: "Ready".to_string(),
            last_output: String::new(),
            last_refresh: Instant::now(),
            job: None,
        })
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
    execute!(stdout, EnterAlternateScreen).map_err(|err| format!("Could not enter alternate screen: {err}"))?;
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

        if event::poll(Duration::from_millis(200)).map_err(|err| format!("Event poll failed: {err}"))? {
            let event = event::read().map_err(|err| format!("Event read failed: {err}"))?;
            if let Event::Key(key) = event {
                if key.kind == KeyEventKind::Press {
                    if handle_key(ctx, app, key)? {
                        return Ok(());
                    }
                }
            }
        }

        poll_job(ctx, app)?;

        if app.last_refresh.elapsed() > Duration::from_secs(3) {
            let _ = app.refresh(ctx);
        }
    }
}

fn handle_key(ctx: &Context, app: &mut App, key: crossterm::event::KeyEvent) -> Result<bool, String> {
    match app.mode {
        Mode::Normal => handle_normal_key(ctx, app, key),
        Mode::ConfirmDelete { .. } => handle_delete_confirm(ctx, app, key),
        Mode::ConfirmPrune { .. } => handle_prune_confirm(ctx, app, key),
        Mode::ArtifactView { .. } => handle_artifact_view_key(app, key),
        Mode::AmendInput { .. } => handle_amend_key(ctx, app, key),
    }
}

fn handle_normal_key(ctx: &Context, app: &mut App, key: crossterm::event::KeyEvent) -> Result<bool, String> {
    if app.job.is_some() {
        match key.code {
            KeyCode::Char('q') | KeyCode::Esc => return Ok(true),
            KeyCode::Down | KeyCode::Char('j') => {
                app.move_down();
                return Ok(false);
            }
            KeyCode::Up | KeyCode::Char('k') => {
                app.move_up();
                return Ok(false);
            }
            KeyCode::Char('g') => {
                app.refresh(ctx)?;
                app.notice = "Refreshed run list".to_string();
                return Ok(false);
            }
            _ => {
                app.notice = "An action is still running. Wait for it to finish or quit with q.".to_string();
                return Ok(false);
            }
        }
    }

    match key.code {
        KeyCode::Char('q') | KeyCode::Esc => return Ok(true),
        KeyCode::Down | KeyCode::Char('j') => app.move_down(),
        KeyCode::Up | KeyCode::Char('k') => app.move_up(),
        KeyCode::Char('g') => {
            app.refresh(ctx)?;
            app.notice = "Refreshed run list".to_string();
        }
        KeyCode::Enter | KeyCode::Char('o') => {
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
        KeyCode::Char('s') => run_action(ctx, app, "safe-next", |ctx, run_dir| execute_safe_next_action(&ctx, &run_dir))?,
        KeyCode::Char('n') => {
            run_action(ctx, app, "start-next", |ctx, run_dir| run_stage_capture(&ctx, &run_dir, "start-next", &[]))?
        }
        KeyCode::Char('r') => run_action(ctx, app, "resume", |ctx, run_dir| automate_run(&ctx, &run_dir, "verification", true))?,
        KeyCode::Char('a') => app.mode = Mode::AmendInput { buffer: String::new() },
        KeyCode::Char('y') => run_action(ctx, app, "rerun", |ctx, run_dir| run_stage_capture(&ctx, &run_dir, "rerun", &[]))?,
        KeyCode::Char('h') => {
            run_action(ctx, app, "host-probe", |ctx, run_dir| run_stage_capture(&ctx, &run_dir, "host-probe", &["--refresh"]))?
        }
        KeyCode::Char('x') => {
            if app.selected_run().is_some() {
                app.mode = Mode::ConfirmDelete {
                    selected: ConfirmChoice::Cancel,
                };
            }
        }
        KeyCode::Char('p') => {
            app.mode = Mode::ConfirmPrune {
                selected: ConfirmChoice::Cancel,
            };
        }
        KeyCode::Char('u') => {
            run_action(ctx, app, "refresh-prompts", |ctx, run_dir| run_stage_capture(&ctx, &run_dir, "refresh-prompts", &[]))?
        }
        KeyCode::Char('b') => {
            run_action(ctx, app, "step-back review", |ctx, run_dir| run_stage_capture(&ctx, &run_dir, "step-back", &["review"]))?
        }
        KeyCode::Char('v') => {
            run_action(ctx, app, "recheck verification", |ctx, run_dir| run_stage_capture(&ctx, &run_dir, "recheck", &["verification"]))?
        }
        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => return Ok(true),
        _ => {}
    }
    Ok(false)
}

fn handle_amend_key(ctx: &Context, app: &mut App, key: crossterm::event::KeyEvent) -> Result<bool, String> {
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
        run_action(ctx, app, "amend", move |ctx, run_dir| amend_run(&ctx, &run_dir, &note, "intake", true))?;
        app.notice = "Amendment saved. Press r to resume or s to run the safe next action.".to_string();
    }
    Ok(false)
}

fn handle_artifact_view_key(app: &mut App, key: crossterm::event::KeyEvent) -> Result<bool, String> {
    let mut next_mode = None;
    if let Mode::ArtifactView { kind, scroll } = app.mode {
        match key.code {
            KeyCode::Char('q') => return Ok(true),
            KeyCode::Esc => app.mode = Mode::Normal,
            KeyCode::Down | KeyCode::Char('j') => {
                next_mode = Some(Mode::ArtifactView {
                    kind,
                    scroll: scroll.saturating_add(1),
                });
            }
            KeyCode::Up | KeyCode::Char('k') => {
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
            KeyCode::Left | KeyCode::Char('[') => {
                next_mode = Some(Mode::ArtifactView {
                    kind: kind.previous(),
                    scroll: 0,
                });
            }
            KeyCode::Right | KeyCode::Char(']') | KeyCode::Tab => {
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
        KeyCode::Char('q') => return Ok(true),
        KeyCode::Esc => app.mode = Mode::Normal,
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
                    app.last_output = app.notice.clone();
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
        KeyCode::Char('q') => return Ok(true),
        KeyCode::Esc => app.mode = Mode::Normal,
        KeyCode::Left | KeyCode::Right | KeyCode::Tab => {
            selected = selected.toggle();
            app.mode = Mode::ConfirmPrune { selected };
        }
        KeyCode::Enter => match selected {
            ConfirmChoice::Cancel => app.mode = Mode::Normal,
            ConfirmChoice::Confirm => {
                let candidates = choose_prune_candidates(&app.root, DEFAULT_KEEP, Some(DEFAULT_PRUNE_DAYS))?;
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

fn run_action<F>(ctx: &Context, app: &mut App, label: &str, action: F) -> Result<(), String>
where
    F: FnOnce(Context, PathBuf) -> Result<crate::engine::CommandResult, String> + Send + 'static,
{
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
    let label_text = label.to_string();
    app.notice = format!(
        "Running {label} for {}",
        run_dir.file_name().and_then(|s| s.to_str()).unwrap_or("run")
    );
    app.last_output = format!(
        "{} is running in the background.\nUse Details and Log tail for realtime progress.",
        label_text
    );
    let (tx, rx) = mpsc::channel();
    let ctx_clone = ctx.clone();
    let run_dir_clone = run_dir.clone();
    thread::spawn(move || {
        let result = action(ctx_clone, run_dir_clone);
        let _ = tx.send(result);
    });
    app.job = Some(RunningJob {
        label: label_text,
        run_dir,
        receiver: rx,
    });
    Ok(())
}

fn poll_job(ctx: &Context, app: &mut App) -> Result<(), String> {
    let mut finished = None;
    if let Some(job) = &app.job {
        match job.receiver.try_recv() {
            Ok(result) => finished = Some((job.label.clone(), job.run_dir.clone(), result)),
            Err(mpsc::TryRecvError::Empty) => {}
            Err(mpsc::TryRecvError::Disconnected) => {
                finished = Some((
                    job.label.clone(),
                    job.run_dir.clone(),
                    Err("Background action channel disconnected.".to_string()),
                ));
            }
        }
    }

    if let Some((label, run_dir, result)) = finished {
        app.job = None;
        match result {
            Ok(result) => {
                if result.code == 0 {
                    app.last_output = if !result.stdout.trim().is_empty() {
                        result.stdout.trim_end().to_string()
                    } else {
                        result.combined_output()
                    };
                    app.notice = format!("{label} completed");
                } else {
                    app.last_output = result.combined_output();
                    app.notice = format!("{label} failed with exit code {}", result.code);
                }
            }
            Err(err) => {
                app.last_output = err.clone();
                app.notice = format!("{label} failed");
            }
        }
        app.refresh(ctx)?;
        if let Some(updated) = app.runs.iter_mut().find(|item| item.run_dir == run_dir) {
            let (_log_title, log_lines) = latest_log_excerpt(&run_dir, 12);
            updated.log_lines = log_lines;
        }
    }
    Ok(())
}

fn draw(frame: &mut ratatui::Frame<'_>, app: &App) {
    let size = frame.size();
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(2),
            Constraint::Min(10),
            Constraint::Length(10),
        ])
        .split(size);

    draw_header(frame, layout[0], app);
    draw_body(frame, layout[1], app);
    draw_logs(frame, layout[2], app);

    match &app.mode {
        Mode::AmendInput { buffer } => draw_amend_popup(frame, size, buffer),
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
        Mode::ArtifactView { kind, scroll } => draw_artifact_popup(frame, size, app, *kind, *scroll),
        Mode::Normal => {}
    }
}

fn draw_header(frame: &mut ratatui::Frame<'_>, area: Rect, app: &App) {
    let running = app
        .job
        .as_ref()
        .map(|job| format!(" | running: {}", job.label))
        .unwrap_or_default();
    let lines = vec![
        Line::from(vec![
            Span::styled("agpipe", Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)),
            Span::raw("  q quit  j/k move  g refresh  o open  1..5 artifacts  s safe  r resume  a amend  y rerun  h probe  x delete  p prune"),
        ]),
        Line::from(format!("root: {} | {}{}", app.root.display(), app.notice, running)),
    ];
    let paragraph = Paragraph::new(lines).block(Block::default().borders(Borders::BOTTOM));
    frame.render_widget(paragraph, area);
}

fn draw_body(frame: &mut ratatui::Frame<'_>, area: Rect, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(34), Constraint::Percentage(66)])
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
                let line = format!(
                    "{} [{}] -> {}",
                    run.run_dir.file_name().and_then(|s| s.to_str()).unwrap_or("run"),
                    run.doctor.goal,
                    run.doctor.next
                );
                ListItem::new(line)
            })
            .collect()
    };
    let list = List::new(items)
        .block(Block::default().title("Runs").borders(Borders::ALL))
        .highlight_style(Style::default().bg(Color::Blue).fg(Color::Black).add_modifier(Modifier::BOLD))
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
        .constraints([Constraint::Percentage(68), Constraint::Percentage(32)])
        .split(area);
    let (detail_text, preview_title, preview_text) = if let Some(run) = app.selected_run() {
        let mut lines = vec![
            format!("Run: {}", run.run_dir.display()),
            format!("Health: {}", run.doctor.health),
            format!("Goal: {}", run.doctor.goal),
            format!("Next: {}", run.doctor.next),
            format!("Safe action: {}", run.doctor.safe_next_action),
            format!("Host probe: {}", run.doctor.host_probe),
            "Fix flow: a = amend+rewind, then r = resume or s = safe-next".to_string(),
            "Artifacts: o/Enter = open current, 1 Summary, 2 Findings, 3 Augmented, 4 Execution, 5 Brief".to_string(),
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
        (lines.join("\n"), run.preview_label.clone(), run.preview.clone())
    } else {
        (
            "No run selected.".to_string(),
            "Preview".to_string(),
            "No substantive artifact is available yet.".to_string(),
        )
    };
    let preview = Paragraph::new(preview_text)
        .block(Block::default().title(preview_title).borders(Borders::ALL))
        .wrap(Wrap { trim: false });
    let detail = Paragraph::new(detail_text)
        .block(Block::default().title("Details").borders(Borders::ALL))
        .wrap(Wrap { trim: false });
    frame.render_widget(preview, vertical[0]);
    frame.render_widget(detail, vertical[1]);
}

fn draw_logs(frame: &mut ratatui::Frame<'_>, area: Rect, app: &App) {
    let (title, content) = if !app.last_output.trim().is_empty() {
        ("Last action".to_string(), app.last_output.clone())
    } else if let Some(run) = app.selected_run() {
        (format!("Log tail: {}", run.log_title), run.log_lines.join("\n"))
    } else {
        ("Logs".to_string(), "No log files yet.".to_string())
    };
    let logs = Paragraph::new(content)
        .block(Block::default().title(title).borders(Borders::ALL))
        .wrap(Wrap { trim: false });
    frame.render_widget(logs, area);
}

fn draw_amend_popup(frame: &mut ratatui::Frame<'_>, area: Rect, buffer: &str) {
    let popup = centered_rect(70, 28, area);
    frame.render_widget(Clear, popup);
    let text = format!(
        "Add amendment for the selected run.\n\nPress Enter to save and rewind intake.\nThen use r to resume the whole run or s to execute only the safe next step.\nPress Esc to cancel.\n\n{}",
        buffer
    );
    let paragraph = Paragraph::new(text)
        .block(Block::default().title("Amend Run").borders(Borders::ALL))
        .wrap(Wrap { trim: false });
    frame.render_widget(paragraph, popup);
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
        Line::from("Use Left/Right or Tab to choose, Enter to confirm, Esc to cancel."),
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
                format!(
                    "{} is not ready yet.\n\nCurrent run state:\n- health: {}\n- goal: {}\n- next: {}\n- safe action: {}\n- host probe: {}\n\nUse Details/Logs for realtime progress, or press `r` to resume and `s` for one safe step.",
                    kind.label(),
                    run.doctor.health,
                    run.doctor.goal,
                    run.doctor.next,
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
