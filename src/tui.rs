use crate::engine::{
    amend_run, automate_run, choose_prune_candidates, contextual_log_excerpt, delete_run,
    execute_safe_next_action, load_run_snapshots, run_has_plan_artifact, run_stage_capture,
    task_flow_capture, with_engine_observer, Context, EngineObserver, RunSnapshot,
    RunTokenSummary,
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
use std::fs;
use std::io::{self, Stdout};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver, TryRecvError};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime};

const DEFAULT_KEEP: usize = 20;
const DEFAULT_PRUNE_DAYS: u64 = 14;
const MAX_LAST_OUTPUT_CHARS: usize = 12_000;
const AUTO_TAIL_SCROLL: u16 = u16::MAX;
const ACTIVE_STAGE_RECENT_OUTPUT_SECS: u64 = 90;
const UI_BG: Color = Color::Rgb(9, 11, 15);
const UI_PANEL_BG: Color = Color::Rgb(20, 24, 30);
const UI_PANEL_BG_ALT: Color = Color::Rgb(28, 33, 41);
const UI_TEXT: Color = Color::Rgb(232, 236, 242);
const UI_MUTED: Color = Color::Rgb(154, 163, 177);
const UI_BORDER: Color = Color::Rgb(84, 96, 112);
const UI_ACCENT: Color = Color::Rgb(128, 182, 255);
const UI_INFO: Color = Color::Rgb(106, 200, 255);
const UI_SUCCESS: Color = Color::Rgb(120, 198, 143);
const UI_WARN: Color = Color::Rgb(242, 199, 102);
const UI_DANGER: Color = Color::Rgb(231, 114, 114);
const UI_SELECTION_BG: Color = Color::Rgb(50, 70, 102);

fn themed_block(title: impl Into<String>) -> Block<'static> {
    Block::default()
        .title(title.into())
        .borders(Borders::ALL)
        .border_style(Style::default().fg(UI_BORDER))
        .style(Style::default().fg(UI_TEXT).bg(UI_PANEL_BG))
}

fn themed_panel() -> Block<'static> {
    Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(UI_BORDER))
        .style(Style::default().fg(UI_TEXT).bg(UI_PANEL_BG))
}

fn highlighted_running_block(title: impl Into<String>, stalled: bool) -> Block<'static> {
    let accent = if stalled { UI_WARN } else { UI_INFO };
    Block::default()
        .title(Line::from(vec![
            Span::styled(
                if stalled { " STALLED " } else { " LIVE " },
                Style::default()
                    .fg(UI_BG)
                    .bg(accent)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw(" "),
            Span::styled(
                title.into(),
                Style::default().fg(UI_TEXT).add_modifier(Modifier::BOLD),
            ),
        ]))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(accent))
        .style(Style::default().fg(UI_TEXT).bg(UI_PANEL_BG))
}

fn selected_button_style(color: Color) -> Style {
    Style::default()
        .fg(UI_BG)
        .bg(color)
        .add_modifier(Modifier::BOLD)
}

fn idle_button_style() -> Style {
    Style::default().fg(UI_TEXT).bg(UI_PANEL_BG_ALT)
}

enum Mode {
    Normal,
    AmendInput {
        buffer: String,
    },
    RerunInput {
        buffer: String,
        scroll: u16,
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
    Doctor,
}

impl ArtifactKind {
    fn label(self) -> &'static str {
        match self {
            Self::Summary => "Summary",
            Self::Findings => "Findings",
            Self::Augmented => "Augmented",
            Self::Execution => "Execution",
            Self::Brief => "Brief",
            Self::Doctor => "Doctor",
        }
    }

    fn path(self, run: &RunSnapshot) -> PathBuf {
        match self {
            Self::Summary => preferred_summary_path(run),
            Self::Findings => run.run_dir.join("verification").join("findings.md"),
            Self::Augmented => run.run_dir.join("verification").join("augmented-task.md"),
            Self::Execution => run.run_dir.join("execution").join("report.md"),
            Self::Brief => run.run_dir.join("brief.md"),
            Self::Doctor => run.run_dir.join("runtime").join("doctor.txt"),
        }
    }

    fn next(self) -> Self {
        match self {
            Self::Summary => Self::Findings,
            Self::Findings => Self::Augmented,
            Self::Augmented => Self::Execution,
            Self::Execution => Self::Brief,
            Self::Brief => Self::Doctor,
            Self::Doctor => Self::Summary,
        }
    }

    fn previous(self) -> Self {
        match self {
            Self::Summary => Self::Doctor,
            Self::Findings => Self::Summary,
            Self::Augmented => Self::Findings,
            Self::Execution => Self::Augmented,
            Self::Brief => Self::Execution,
            Self::Doctor => Self::Brief,
        }
    }
}

fn preferred_summary_path(run: &RunSnapshot) -> PathBuf {
    match run.preview_label.as_str() {
        "Verification Summary" => {
            return run.run_dir.join("verification").join("user-summary.md");
        }
        "Review Summary" => {
            return run.run_dir.join("review").join("user-summary.md");
        }
        _ => {}
    }
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
    match run.preview_label.as_str() {
        "Verification Summary" => "Verification Summary",
        "Review Summary" => "Review Summary",
        _ => match preferred_summary_path(run)
            .parent()
            .and_then(|value| value.file_name())
            .and_then(|value| value.to_str())
        {
            Some("verification") => "Verification Summary",
            Some("review") => "Review Summary",
            _ => "Summary",
        },
    }
}

#[derive(Clone, Copy)]
struct TextLayoutState {
    scroll: u16,
    cursor: Option<(u16, u16)>,
}

fn text_layout_state(area: Rect, value: &str, cursor: usize, requested_scroll: u16) -> TextLayoutState {
    let inner = block_inner_area(area);
    if inner.width == 0 || inner.height == 0 {
        return TextLayoutState {
            scroll: 0,
            cursor: None,
        };
    }
    let width = inner.width as usize;
    let visible_rows = inner.height as usize;
    let (row, col) = text_cursor_row_col(value, width, cursor);
    let total_lines = wrapped_line_count(value, inner.width);
    let max_scroll = total_lines.saturating_sub(visible_rows);
    let min_scroll = row.saturating_sub(visible_rows.saturating_sub(1));
    let scroll = requested_scroll
        .max(min_scroll as u16)
        .min(max_scroll as u16);
    let max_row = inner.height.saturating_sub(1) as usize;
    let clamped_row = row.saturating_sub(scroll as usize).min(max_row);
    let clamped_col = col.min(inner.width.saturating_sub(1) as usize);
    TextLayoutState {
        scroll,
        cursor: Some((
            inner.x.saturating_add(clamped_col as u16),
            inner.y.saturating_add(clamped_row as u16),
        )),
    }
}

fn new_run_task_layout_state(area: Rect, draft: &NewRunDraft, requested_task_scroll: u16) -> TextLayoutState {
    let popup = new_run_popup_rect(area);
    let layout = new_run_popup_layout(popup);
    text_layout_state(layout[1], &draft.task, draft.task_cursor, requested_task_scroll)
}

fn interview_answer_layout_state(
    area: Rect,
    buffer: &str,
    requested_answer_scroll: u16,
) -> TextLayoutState {
    let popup = interview_popup_rect(area);
    let layout = interview_popup_layout(popup);
    text_layout_state(layout[3], buffer, char_count(buffer), requested_answer_scroll)
}

fn rerun_input_layout_state(area: Rect, buffer: &str, requested_scroll: u16) -> TextLayoutState {
    let popup = centered_rect_with_min(area, 78, 40, 64, 11);
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(5),
            Constraint::Min(3),
            Constraint::Length(3),
        ])
        .split(popup);
    text_layout_state(layout[1], buffer, char_count(buffer), requested_scroll)
}

fn prompt_review_text(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|_| format!("Could not read {}", path.display()))
}

fn solver_batch_stage_ids(run: &RunSnapshot) -> Vec<String> {
    run.solver_stage_ids
        .iter()
        .filter(|id| {
            run.doctor
                .stages
                .get(id.as_str())
                .or_else(|| run.status.stages.get(id.as_str()))
                .map(|state| state != "done")
                .unwrap_or(true)
        })
        .cloned()
        .collect()
}

fn running_preview_title(preview_label: &str, run: &RunSnapshot) -> String {
    if preview_label == "Summary" {
        summary_title(run).to_string()
    } else {
        preview_label.to_string()
    }
}

fn draw_rerun_popup(
    frame: &mut ratatui::Frame<'_>,
    area: Rect,
    buffer: &str,
    requested_scroll: u16,
) -> Option<(u16, u16)> {
    let popup = centered_rect_with_min(area, 78, 40, 64, 11);
    frame.render_widget(Clear, popup);
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(5),
            Constraint::Min(3),
            Constraint::Length(3),
        ])
        .split(popup);
    let state = rerun_input_layout_state(area, buffer, requested_scroll);
    frame.render_widget(
        Paragraph::new(
            "Optionally add fresh user guidance for the follow-up run.\n\nThis note will be appended to the verification-seeded rerun prompt.\nPress Enter to create the rerun or Esc to cancel.",
        )
        .style(Style::default().fg(UI_TEXT).bg(UI_PANEL_BG))
        .block(themed_block("Rerun Note"))
        .wrap(Wrap { trim: false }),
        layout[0],
    );
    frame.render_widget(
        Paragraph::new(if buffer.is_empty() {
            "Type here, or leave blank to create the rerun with verification context only.".to_string()
        } else {
            buffer.to_string()
        })
        .style(Style::default().fg(UI_TEXT).bg(UI_PANEL_BG))
        .block(themed_block("Additional User Guidance"))
        .scroll((state.scroll, 0))
        .wrap(Wrap { trim: false }),
        layout[1],
    );
    frame.render_widget(
        Paragraph::new("Enter create rerun  Esc cancel  PgUp/PgDn scroll  Ctrl+U clear")
            .style(Style::default().fg(UI_TEXT).bg(UI_PANEL_BG))
            .block(themed_panel()),
        layout[2],
    );
    state.cursor
}

fn draw_new_run_popup(
    frame: &mut ratatui::Frame<'_>,
    area: Rect,
    draft: &NewRunDraft,
    requested_task_scroll: u16,
) -> Option<(u16, u16)> {
    let popup = new_run_popup_rect(area);
    frame.render_widget(Clear, popup);
    let layout = new_run_popup_layout(popup);
    let task_state = new_run_task_layout_state(area, draft, requested_task_scroll);
    let field_name = match draft.field {
        NewRunField::Task => "Task",
        NewRunField::Workspace => "Workspace",
        NewRunField::Title => "Title",
        NewRunField::Start => "Start",
        NewRunField::Cancel => "Cancel",
    };
    let start_style = if matches!(draft.field, NewRunField::Start) {
        selected_button_style(UI_SUCCESS)
    } else {
        idle_button_style()
    };
    let cancel_style = if matches!(draft.field, NewRunField::Cancel) {
        selected_button_style(UI_WARN)
    } else {
        idle_button_style()
    };
    let buttons = Line::from(vec![
        Span::raw("  "),
        Span::styled(" Start Stage0 ", start_style),
        Span::raw("   "),
        Span::styled(" Cancel ", cancel_style),
    ]);
    let header = Paragraph::new(format!(
        "Create a new pipeline.\n1. Describe the task.\n2. Set the target workspace.\n3. Optionally set a title, then launch stage0.\nActive field: {field_name}"
    ))
    .style(Style::default().fg(UI_TEXT).bg(UI_PANEL_BG))
    .block(themed_block("New Pipeline"))
    .wrap(Wrap { trim: false });
    frame.render_widget(header, layout[0]);
    frame.render_widget(
        field_block(
            "Task (Required)",
            if draft.task.is_empty() {
                "<describe the job in plain language; multiline is allowed>"
            } else {
                draft.task.as_str()
            },
            matches!(draft.field, NewRunField::Task),
            task_state.scroll,
        ),
        layout[1],
    );
    frame.render_widget(
        field_block(
            "Workspace (Required)",
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
            "Title (Optional)",
            if draft.title.is_empty() {
                "<leave blank to auto-generate from the task>"
            } else {
                draft.title.as_str()
            },
            matches!(draft.field, NewRunField::Title),
            0,
        ),
        layout[3],
    );
    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled("Tab", Style::default().fg(UI_ACCENT).add_modifier(Modifier::BOLD)),
            Span::raw(" next field   "),
            Span::styled("Ctrl+Enter", Style::default().fg(UI_SUCCESS).add_modifier(Modifier::BOLD)),
            Span::raw(" start stage0   "),
            Span::styled("Esc", Style::default().fg(UI_WARN).add_modifier(Modifier::BOLD)),
            Span::raw(" cancel"),
        ]))
        .style(Style::default().fg(UI_MUTED).bg(UI_PANEL_BG))
        .block(themed_block("Quick Actions")),
        layout[4],
    );
    frame.render_widget(
        Paragraph::new(buttons)
            .style(Style::default().fg(UI_TEXT).bg(UI_PANEL_BG))
            .block(themed_block("Actions"))
            .wrap(Wrap { trim: false }),
        layout[5],
    );
    match draft.field {
        NewRunField::Task => task_state.cursor,
        NewRunField::Workspace => {
            text_layout_state(layout[2], &draft.workspace, draft.workspace_cursor, 0).cursor
        }
        NewRunField::Title => text_layout_state(layout[3], &draft.title, draft.title_cursor, 0).cursor,
        NewRunField::Start | NewRunField::Cancel => None,
    }
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
) -> Option<(u16, u16)> {
    let popup = interview_popup_rect(area);
    frame.render_widget(Clear, popup);
    let layout = interview_popup_layout(popup);
    let answer_state = interview_answer_layout_state(area, buffer, requested_answer_scroll);
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
        .style(Style::default().fg(UI_TEXT).bg(UI_PANEL_BG))
        .block(themed_block("Interview"))
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
            answer_state.scroll,
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
    answer_state.cursor
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
    #[serde(default)]
    id: String,
    question: String,
    #[serde(alias = "reason", default)]
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
struct InterviewQuestionsPayloadLoose {
    #[serde(default)]
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

fn normalize_interview_questions(questions: Vec<InterviewQuestion>) -> Vec<InterviewQuestion> {
    questions
        .into_iter()
        .enumerate()
        .map(|(index, mut question)| {
            if question.id.trim().is_empty() {
                question.id = format!("q{}", index + 1);
            }
            question
        })
        .collect()
}

fn latest_interview_session_dir(root: &Path) -> Option<PathBuf> {
    let mut candidates: Vec<PathBuf> = fs::read_dir(root.join("_interviews"))
        .ok()?
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| path.is_dir())
        .collect();
    candidates.sort();
    candidates.pop()
}

fn recover_interview_questions_payload(
    root: &Path,
    stdout: &str,
    raw_output: &str,
) -> Result<InterviewQuestionsPayload, String> {
    if let Ok(mut payload) = parse_job_json::<InterviewQuestionsPayloadLoose>(stdout, raw_output) {
        if payload.session_dir.trim().is_empty() {
            if let Some(session_dir) = latest_interview_session_dir(root) {
                payload.session_dir = session_dir.display().to_string();
            }
        }
        if !payload.session_dir.trim().is_empty() {
            return Ok(InterviewQuestionsPayload {
                session_dir: payload.session_dir,
                goal_summary: payload.goal_summary,
                questions: normalize_interview_questions(payload.questions),
            });
        }
    }

    let session_dir = latest_interview_session_dir(root)
        .ok_or_else(|| "No interview session directory was created.".to_string())?;
    let questions_path = session_dir.join("questions.json");
    let text = fs::read_to_string(&questions_path)
        .map_err(|err| format!("Could not read {}: {err}", questions_path.display()))?;
    let payload = serde_json::from_str::<InterviewQuestionsPayloadLoose>(&text)
        .map_err(|err| format!("Could not parse {}: {err}", questions_path.display()))?;
    Ok(InterviewQuestionsPayload {
        session_dir: session_dir.display().to_string(),
        goal_summary: payload.goal_summary,
        questions: normalize_interview_questions(payload.questions),
    })
}

fn interview_final_task_path(session_dir: &Path) -> PathBuf {
    session_dir.join("final-task.md")
}

fn prompt_review_mode(draft: NewRunDraft, session_dir: PathBuf, final_task_path: PathBuf) -> Mode {
    Mode::PromptReview {
        draft,
        session_dir,
        final_task_path,
        scroll: 0,
        selected: PromptReviewAction::CreateOnly,
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
        if path.is_absolute() && run_has_plan_artifact(&path) {
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
    noise_filter: Mutex<ProcessLogNoiseFilter>,
}

#[derive(Default)]
struct ProcessLogNoiseFilter {
    suppressing_analytics_html: bool,
}

impl ProcessLogNoiseFilter {
    fn filter_line(&mut self, line: &str) -> Option<String> {
        let trimmed = line.trim();
        if trimmed.contains("codex_core::analytics_client: events failed with status 403 Forbidden")
        {
            self.suppressing_analytics_html =
                trimmed.contains("<html") && !trimmed.contains("</html>");
            return Some("[suppressed codex analytics 403 HTML noise]".to_string());
        }
        if self.suppressing_analytics_html {
            if trimmed.contains("</html>") {
                self.suppressing_analytics_html = false;
            }
            return None;
        }
        Some(line.to_string())
    }
}

impl EngineObserver for UiEngineObserver {
    fn process_started(&self, pid: i32, pgid: i32) {
        let _ = runtime::touch_job(&self.run_dir, "running");
        let _ = runtime::append_process_line(
            &self.run_dir,
            &format!("Attached child process: pid={pid} pgid={pgid}"),
        );
    }

    fn line(&self, line: &str) {
        let filtered = self
            .noise_filter
            .lock()
            .ok()
            .and_then(|mut filter| filter.filter_line(line))
            .unwrap_or_else(|| line.to_string());
        if filtered.is_empty() {
            return;
        }
        let _ = runtime::append_process_line(&self.run_dir, &filtered);
        let _ = self.stream_tx.send(filtered);
    }

    fn stage_changed(&self, stage: &str) {
        let _ = runtime::update_job_stage(&self.run_dir, Some(stage), None);
        let _ = runtime::append_process_line(&self.run_dir, &format!("Stage changed: {stage}"));
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
            log_scroll: AUTO_TAIL_SCROLL,
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
        clear_stale_terminal_notice(self);
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
            self.log_scroll = AUTO_TAIL_SCROLL;
            return;
        }
        if !self.runs.is_empty() {
            let next = (self.selected + 1).min(self.runs.len() - 1);
            if next != self.selected {
                self.selected = next;
                self.preview_scroll = 0;
                self.log_scroll = AUTO_TAIL_SCROLL;
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
                self.log_scroll = AUTO_TAIL_SCROLL;
                return;
            }
        }
        let next = self.selected.saturating_sub(1);
        if next != self.selected {
            self.selected = next;
            self.preview_scroll = 0;
            self.log_scroll = AUTO_TAIL_SCROLL;
        }
    }

    fn select_run_by_path(&mut self, run_dir: &Path) {
        if let Some(index) = self.runs.iter().position(|run| run.run_dir == run_dir) {
            if self.selected != index {
                self.selected = index;
                self.preview_scroll = 0;
                self.log_scroll = AUTO_TAIL_SCROLL;
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
    if job.label != "resume" {
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
        let _ = runtime::append_process_line(&job.run_dir, &format!("Stage changed: {next}"));
        return;
    }
    if current == next {
        return;
    }
    let current_state = run.status.stages.get(current).map(String::as_str);
    if current_state.is_none() || matches!(current_state, Some("done")) {
        job.log_hint = Some(next.to_string());
        let _ = runtime::update_job_stage(&job.run_dir, Some(next), None);
        let _ = runtime::append_process_line(&job.run_dir, &format!("Stage changed: {next}"));
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
    clear_stale_terminal_notice(app);
}

fn stale_terminal_notice(text: &str) -> bool {
    text.contains("Tracked process is no longer alive")
        || text.contains("is no longer alive. Inspect run artifacts and logs")
}

fn clear_stale_terminal_notice(app: &mut App) {
    if !stale_terminal_notice(&app.last_output) {
        return;
    }
    if app.job.is_some() || app.wizard_job.is_some() {
        app.last_output.clear();
        return;
    }
    if let Some(run) = app.selected_run() {
        if !run.doctor.health.trim().eq_ignore_ascii_case("broken") {
            app.last_output.clear();
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
    app.log_scroll = AUTO_TAIL_SCROLL;
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
    app.log_scroll = AUTO_TAIL_SCROLL;
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
    // Track the owning TUI process immediately so runtime status stays alive for
    // the full in-process job, even when parallel child `codex` workers start
    // and finish independently.
    let owner_pid = std::process::id() as i32;
    runtime::start_job(
        &run_dir,
        &label,
        log_hint.as_deref(),
        &command_hint,
        owner_pid,
        owner_pid,
    )?;
    let _ = runtime::append_process_line(&run_dir, &format!("Starting {command_hint}"));
    let _ = stream_tx.send(format!("Starting {command_hint}"));
    let observer = Arc::new(UiEngineObserver {
        run_dir: run_dir.clone(),
        stream_tx: stream_tx.clone(),
        noise_filter: Mutex::new(ProcessLogNoiseFilter::default()),
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

        let poll_interval = if text_entry_mode_active(app) {
            Duration::from_millis(100)
        } else {
            Duration::from_millis(250)
        };
        if event::poll(poll_interval)
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
        if !text_entry_mode_active(app) && app.last_refresh.elapsed() > refresh_interval {
            let _ = app.refresh(ctx);
        }
    }
}

fn replace_recent_stream_lines(current: &mut Vec<String>, incoming: Vec<String>, limit: usize) {
    if incoming.is_empty() {
        return;
    }
    current.extend(incoming);
    if current.len() > limit {
        let drop_count = current.len().saturating_sub(limit);
        current.drain(0..drop_count);
    }
}

fn compact_last_output(text: &str) -> String {
    if text.chars().count() <= MAX_LAST_OUTPUT_CHARS {
        return text.to_string();
    }
    let head_chars = MAX_LAST_OUTPUT_CHARS * 2 / 3;
    let tail_chars = MAX_LAST_OUTPUT_CHARS / 4;
    let head: String = text.chars().take(head_chars).collect();
    let tail: String = text
        .chars()
        .rev()
        .take(tail_chars)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    format!(
        "{head}\n\n[output truncated for TUI responsiveness]\n\n{tail}"
    )
}

fn text_entry_mode_active(app: &App) -> bool {
    matches!(
        app.mode,
        Mode::AmendInput { .. }
            | Mode::RerunInput { .. }
            | Mode::NewRunInput { .. }
            | Mode::InterviewInput { .. }
            | Mode::PromptReview { .. }
    )
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
        Mode::ArtifactView { .. } => handle_artifact_view_key(ctx, app, key),
        Mode::AmendInput { .. } => handle_amend_key(ctx, app, key),
        Mode::RerunInput { .. } => handle_rerun_input_key(ctx, app, key),
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

#[derive(Clone, Copy)]
struct BodyRects {
    runs: Rect,
    details: Rect,
}

#[derive(Clone, Copy)]
struct DetailRects {
    preview: Rect,
    facts: Rect,
    actions: Rect,
}

fn root_layout(size: Rect) -> Vec<Rect> {
    let log_height = if size.height < 32 {
        10
    } else if size.height < 40 {
        12
    } else {
        15
    };
    Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(2),
            Constraint::Length(1),
            Constraint::Min(10),
            Constraint::Length(log_height),
            Constraint::Length(1),
        ])
        .split(size)
        .to_vec()
}

fn body_rects(area: Rect) -> BodyRects {
    if area.width < 150 {
        let runs_height = ((area.height as u32) * 32 / 100).clamp(8, 11) as u16;
        let split = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(runs_height), Constraint::Min(8)])
            .split(area);
        BodyRects {
            runs: split[0],
            details: split[1],
        }
    } else {
        let split = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(32), Constraint::Percentage(68)])
            .split(area);
        BodyRects {
            runs: split[0],
            details: split[1],
        }
    }
}

fn detail_rects(area: Rect) -> DetailRects {
    if area.width < 118 {
        let preview_share: u16 = if area.height < 18 { 44 } else { 50 };
        let lower_share: u16 = 100u16.saturating_sub(preview_share);
        let bottom_share: u16 = (lower_share / 2).max(20);
        let middle_share = lower_share.saturating_sub(bottom_share);
        let split = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Percentage(preview_share),
                Constraint::Percentage(middle_share),
                Constraint::Percentage(bottom_share),
            ])
            .split(area);
        DetailRects {
            preview: split[0],
            facts: split[1],
            actions: split[2],
        }
    } else {
        let vertical = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Percentage(58), Constraint::Percentage(42)])
            .split(area);
        let lower = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(56), Constraint::Percentage(44)])
            .split(vertical[1]);
        DetailRects {
            preview: vertical[0],
            facts: lower[0],
            actions: lower[1],
        }
    }
}

fn normal_mode_rects(size: Rect) -> NormalModeRects {
    let layout = root_layout(size);
    let body = body_rects(layout[2]);
    let details = detail_rects(body.details);
    NormalModeRects {
        runs: body.runs,
        preview: details.preview,
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
    if requested == 0 {
        return 0;
    }
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
                let max_scroll = log_panel_max_scroll(app, rects.logs);
                if app.log_scroll != AUTO_TAIL_SCROLL {
                    scroll_down(&mut app.log_scroll, 3);
                    if max_scroll > 0 && app.log_scroll >= max_scroll {
                        app.log_scroll = AUTO_TAIL_SCROLL;
                    }
                }
            }
        }
        MouseEventKind::ScrollUp => {
            if point_in_rect(mouse.column, mouse.row, rects.runs) {
                app.move_up();
                attach_selected_run_job_if_any(app);
            } else if point_in_rect(mouse.column, mouse.row, rects.preview) {
                scroll_up(&mut app.preview_scroll, 3);
            } else if point_in_rect(mouse.column, mouse.row, rects.logs) {
                if app.log_scroll == AUTO_TAIL_SCROLL {
                    let max_scroll = log_panel_max_scroll(app, rects.logs);
                    if max_scroll > 0 {
                        app.log_scroll = max_scroll.saturating_sub(3);
                    }
                } else {
                    scroll_up(&mut app.log_scroll, 3);
                }
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
        KeyCode::Char('?') => {
            app.notice = "Shortcut help opened in the lower pane.".to_string();
            app.last_output = shortcut_help_text(app);
        }
        _ if key_is_char(key, 'f') => spawn_action(
            ctx,
            app,
            "doctor fix",
            vec!["doctor".to_string(), "--fix".to_string()],
        )?,
        _ if key_is_char(key, 'd') => {
            if app.selected_run().is_some() {
                app.refresh(ctx)?;
                app.mode = Mode::ArtifactView {
                    kind: ArtifactKind::Doctor,
                    scroll: 0,
                };
                app.notice = "Opened doctor report for the selected run.".to_string();
            } else {
                app.notice = "No selected run for doctor.".to_string();
            }
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
        KeyCode::Char('6') => {
            app.mode = Mode::ArtifactView {
                kind: ArtifactKind::Doctor,
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
        KeyCode::Char(' ') => spawn_action(ctx, app, "safe-next", vec!["safe-next".to_string()])?,
        _ if key_is_char(key, 'n') => {
            spawn_action(ctx, app, "start-next", vec!["start-next".to_string()])?
        }
        KeyCode::Char('.') => spawn_action(ctx, app, "start-next", vec!["start-next".to_string()])?,
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
        _ if key_is_char(key, 'y') => {
            if app.selected_run().is_some() {
                app.mode = Mode::RerunInput {
                    buffer: String::new(),
                    scroll: 0,
                };
                app.notice = "Add optional guidance for the rerun, then press Enter.".to_string();
            } else {
                app.notice = "No selected run".to_string();
            }
        }
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
        Mode::RerunInput { buffer, .. } => buffer.push_str(&sanitize_multiline_paste(text)),
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

fn handle_rerun_input_key(
    ctx: &Context,
    app: &mut App,
    key: crossterm::event::KeyEvent,
) -> Result<bool, String> {
    let mut submit_note: Option<String> = None;
    let mut cancel = false;
    if let Mode::RerunInput { buffer, scroll } = &mut app.mode {
        match key.code {
            KeyCode::Esc => cancel = true,
            KeyCode::Enter => submit_note = Some(buffer.trim().to_string()),
            KeyCode::Backspace => {
                buffer.pop();
            }
            KeyCode::PageDown => *scroll = scroll.saturating_add(6),
            KeyCode::PageUp => *scroll = scroll.saturating_sub(6),
            KeyCode::Down => *scroll = scroll.saturating_add(1),
            KeyCode::Up => *scroll = scroll.saturating_sub(1),
            KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                buffer.clear();
                *scroll = 0;
            }
            KeyCode::Char(ch) => {
                buffer.push(ch);
            }
            _ => {}
        }
    }
    if cancel {
        app.mode = Mode::Normal;
        app.notice = "Rerun creation cancelled.".to_string();
        return Ok(false);
    }
    if let Some(note) = submit_note {
        app.mode = Mode::Normal;
        let mut args = vec!["rerun".to_string()];
        if !note.is_empty() {
            args.push("--note".to_string());
            args.push(note);
        }
        spawn_action(ctx, app, "rerun", args)?;
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
            KeyCode::Enter if key.modifiers.contains(KeyModifiers::CONTROL) => submit = true,
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
    ctx: &Context,
    app: &mut App,
    key: crossterm::event::KeyEvent,
) -> Result<bool, String> {
    let mut next_mode = None;
    if let Mode::ArtifactView { kind, scroll } = app.mode {
        match key.code {
            _ if key_is_char(key, 'q') => return Ok(true),
            _ if key_is_char(key, 'f') => {
                app.mode = Mode::Normal;
                spawn_action(
                    ctx,
                    app,
                    "doctor fix",
                    vec!["doctor".to_string(), "--fix".to_string()],
                )?;
                return Ok(false);
            }
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
            KeyCode::Char('6') => {
                next_mode = Some(Mode::ArtifactView {
                    kind: ArtifactKind::Doctor,
                    scroll: 0,
                });
            }
            _ if key_is_char(key, 'd') => {
                next_mode = Some(Mode::ArtifactView {
                    kind: ArtifactKind::Doctor,
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
        "doctor fix" => "doctor --fix".to_string(),
        "resume" => "resume until verification".to_string(),
        other => other.to_string(),
    };
    let log_hint = resolve_action_log_hint(ctx, label, &run_dir, app.selected_run());
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
                "doctor fix" => {
                    run_stage_capture(&ctx_for_job, &run_dir_for_job, "doctor", &["--fix"])
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

fn resolve_action_log_hint(
    ctx: &Context,
    label: &str,
    run_dir: &Path,
    selected_run: Option<&RunSnapshot>,
) -> Option<String> {
    match label {
        "start-next" | "resume" => live_next_stage_log_hint(ctx, run_dir)
            .or_else(|| selected_run.and_then(|run| infer_log_hint(label, run))),
        "safe-next" => live_safe_next_log_hint(ctx, run_dir)
            .or_else(|| selected_run.and_then(|run| infer_log_hint(label, run))),
        _ => selected_run.and_then(|run| infer_log_hint(label, run)),
    }
}

fn live_next_stage_log_hint(ctx: &Context, run_dir: &Path) -> Option<String> {
    let next = crate::engine::next_stage(ctx, run_dir).ok()?;
    let trimmed = next.trim();
    if trimmed.is_empty() || trimmed == "none" || trimmed == "rerun" {
        return None;
    }
    Some(trimmed.to_string())
}

fn live_safe_next_log_hint(ctx: &Context, run_dir: &Path) -> Option<String> {
    let report = crate::engine::doctor_report(ctx, run_dir).ok()?;
    let action = report.safe_next_action.trim();
    if action == "start-solvers" {
        return live_next_stage_log_hint(ctx, run_dir);
    }
    if let Some(stage) = action.strip_prefix("start ") {
        return Some(stage.trim().to_string());
    }
    if let Some(stage) = action.strip_prefix("recheck ") {
        return Some(stage.trim().to_string());
    }
    if let Some(stage) = action.strip_prefix("step-back ") {
        return Some(stage.trim().to_string());
    }
    None
}

fn poll_tracked_job(job: &mut RunningJob, runs: &[RunSnapshot]) -> Option<FinishedJob> {
    let mut finished = None;
    {
        let mut saw_stream_update = false;
        if let Some(rx) = job.stream_rx.as_ref() {
            let mut incoming = Vec::new();
            loop {
                match rx.try_recv() {
                    Ok(line) => incoming.push(line),
                    Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => break,
                }
            }
            if !incoming.is_empty() {
                replace_recent_stream_lines(&mut job.stream_lines, incoming, 40);
                saw_stream_update = true;
            }
        }
        if !saw_stream_update && job.stream_rx.is_none() {
            let fresh_lines = runtime::tail_process_log(&job.run_dir, 40);
            if !fresh_lines.is_empty() {
                job.stream_lines = fresh_lines;
            }
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
                .map(|item| item.is_active())
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
            compact_last_output(&combined_output)
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
                                let session_dir = PathBuf::from(payload.session_dir);
                                fs::create_dir_all(&session_dir).map_err(|err| {
                                    format!(
                                        "Could not create interview session {}: {err}",
                                        session_dir.display()
                                    )
                                })?;
                                let final_task_path = interview_final_task_path(&session_dir);
                                fs::write(&final_task_path, format!("{}\n", draft.task.trim()))
                                    .map_err(|err| {
                                        format!(
                                            "Could not write final task prompt {}: {err}",
                                            final_task_path.display()
                                        )
                                    })?;
                                app.mode = prompt_review_mode(draft, session_dir, final_task_path);
                                app.notice = "No clarification questions were needed. Review the final task prompt, then create the run explicitly.".to_string();
                                app.last_output =
                                    "Stage0 produced a downstream-ready task without additional questions."
                                        .to_string();
                            } else {
                                let questions = normalize_interview_questions(payload.questions);
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
                            match recover_interview_questions_payload(
                                &app.root,
                                &result.stdout,
                                &raw_output,
                            ) {
                                Ok(payload) => {
                                    if payload.questions.is_empty() {
                                        let session_dir = PathBuf::from(payload.session_dir);
                                        fs::create_dir_all(&session_dir).map_err(|create_err| {
                                            format!(
                                                "Could not create interview session {}: {create_err}",
                                                session_dir.display()
                                            )
                                        })?;
                                        let final_task_path =
                                            interview_final_task_path(&session_dir);
                                        fs::write(
                                            &final_task_path,
                                            format!("{}\n", draft.task.trim()),
                                        )
                                        .map_err(
                                            |write_err| {
                                                format!(
                                                "Could not write final task prompt {}: {write_err}",
                                                final_task_path.display()
                                            )
                                            },
                                        )?;
                                        app.mode =
                                            prompt_review_mode(draft, session_dir, final_task_path);
                                    } else {
                                        let questions =
                                            normalize_interview_questions(payload.questions);
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
                                    }
                                    app.notice = format!(
                                        "Recovered stage0 interview payload from session artifacts after noisy output: {err}"
                                    );
                                }
                                Err(recovery_err) => {
                                    app.mode = Mode::NewRunInput {
                                        draft,
                                        task_scroll: 0,
                                    };
                                    app.notice = format!(
                                        "Could not parse interview questions JSON: {err} | recovery failed: {recovery_err}"
                                    );
                                }
                            }
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
                let recovered_final_task = interview_final_task_path(&session_dir);
                if exit_code == 0 {
                    match parse_job_json::<InterviewFinalizePayload>(&result.stdout, &raw_output) {
                        Ok(payload) => {
                            app.mode = prompt_review_mode(
                                draft,
                                session_dir,
                                PathBuf::from(payload.final_task_path),
                            );
                            app.notice =
                                "Review the final task prompt, then create the run explicitly."
                                    .to_string();
                        }
                        Err(err) => {
                            if recovered_final_task.exists() {
                                app.mode = prompt_review_mode(
                                    draft,
                                    session_dir,
                                    recovered_final_task.clone(),
                                );
                                app.notice = format!(
                                    "Recovered the final task prompt from {} after noisy stage0 output.",
                                    recovered_final_task.display()
                                );
                            } else {
                                app.mode = Mode::Normal;
                                app.wizard_selected = false;
                                app.notice =
                                    format!("Could not parse interview finalize JSON: {err}");
                                app.last_output = format!(
                                    "{}\n\nNo recoverable final task prompt was found under {}.",
                                    raw_output.trim(),
                                    session_dir.display()
                                )
                                .trim()
                                .to_string();
                            }
                        }
                    }
                } else if recovered_final_task.exists() {
                    app.mode = prompt_review_mode(draft, session_dir, recovered_final_task.clone());
                    app.notice = format!(
                        "Stage0 finalize reported an error, but recovered the final task prompt from {}.",
                        recovered_final_task.display()
                    );
                    if !raw_output.trim().is_empty() {
                        app.last_output = raw_output;
                    }
                } else {
                    app.mode = Mode::Normal;
                    app.wizard_selected = false;
                    app.notice = "Interview finalize failed. No final task prompt was recovered."
                        .to_string();
                    if !raw_output.trim().is_empty() {
                        app.last_output = raw_output;
                    } else if !answers.is_empty() {
                        app.last_output = format!(
                            "Captured {} answer(s), but stage0 finalize did not produce a usable final task prompt.",
                            answers.len()
                        );
                    }
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
        Mode::RerunInput { buffer, scroll } => {
            if let Some((x, y)) = draw_rerun_popup(frame, size, buffer, *scroll) {
                frame.set_cursor(x, y);
            }
        }
        Mode::NewRunInput { draft, task_scroll } => {
            if let Some((x, y)) = draw_new_run_popup(frame, size, draft, *task_scroll) {
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
            if let Some((x, y)) = draw_interview_popup(
                frame,
                size,
                goal_summary,
                questions,
                answers,
                *index,
                buffer,
                *answer_scroll,
            ) {
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
            Style::default().fg(UI_ACCENT).add_modifier(Modifier::BOLD),
        )]),
        Line::from(vec![
            Span::styled("notice: ", Style::default().fg(UI_MUTED)),
            Span::styled(
                format!("{}{}", app.notice, running),
                Style::default().fg(UI_TEXT),
            ),
        ]),
    ];
    let paragraph = Paragraph::new(lines)
        .style(Style::default().bg(UI_BG))
        .block(
            Block::default()
                .borders(Borders::BOTTOM)
                .border_style(Style::default().fg(UI_BORDER))
                .style(Style::default().bg(UI_BG)),
        );
    frame.render_widget(paragraph, area);
}

fn health_color(health: &str) -> Color {
    match health.trim().to_ascii_lowercase().as_str() {
        "healthy" | "ok" => UI_SUCCESS,
        "warning" | "stale" | "degraded" => UI_WARN,
        "broken" | "failed" | "error" | "blocked" => UI_DANGER,
        _ => UI_TEXT,
    }
}

fn verification_color(state: &str) -> Color {
    match state.trim().to_ascii_lowercase().as_str() {
        "done" | "complete" | "passed" => UI_SUCCESS,
        "running" | "working" | "active" => UI_INFO,
        "pending" | "queued" => UI_WARN,
        "missing" | "failed" | "error" | "blocked" => UI_DANGER,
        _ => UI_TEXT,
    }
}

fn goal_color(goal: &str) -> Color {
    match goal.trim().to_ascii_lowercase().as_str() {
        "complete" => UI_SUCCESS,
        "partial" | "pending-verification" => UI_WARN,
        "blocked" | "failed" | "error" => UI_DANGER,
        "not-evaluated" | "not evaluated yet" => UI_MUTED,
        _ => UI_TEXT,
    }
}

fn next_stage_color(next: &str) -> Color {
    match next.trim().to_ascii_lowercase().as_str() {
        "none" => UI_SUCCESS,
        "rerun" => UI_WARN,
        "missing" | "failed" | "error" | "blocked" => UI_DANGER,
        _ => UI_ACCENT,
    }
}

fn running_state_color(stalled: bool) -> Color {
    if stalled {
        UI_WARN
    } else {
        UI_INFO
    }
}

fn status_pair(label: &str, value: impl Into<String>, color: Color) -> Vec<Span<'static>> {
    vec![
        Span::raw(format!("{label}=")),
        Span::styled(value.into(), Style::default().fg(color)),
    ]
}

fn footer_shortcuts_for_width(app: &App, width: u16) -> &'static str {
    match app.mode {
        Mode::ArtifactView { .. } => {
            if width < 116 {
                "q quit  Esc close  j/k scroll  [ ] or 1..6/d  f fix  ? help  m mouse"
            } else {
                "q quit  Esc close  j/k scroll  PgUp/PgDn scroll  [ ] switch  1..6/d doctor  f fix  ? help  m mouse-select/scroll"
            }
        }
        Mode::ConfirmDelete { .. } => "Esc cancel  Enter apply  Left/Right switch",
        Mode::ConfirmPrune { .. } => "Esc cancel  Enter apply  Left/Right switch",
        Mode::AmendInput { .. } => "Esc cancel  Enter save+rewind",
        Mode::RerunInput { .. } => "Esc cancel  Enter create rerun  PgUp/PgDn scroll  Ctrl+U clear",
        Mode::NewRunInput { .. } => {
            "Tab switch fields  Enter edit/apply  PgUp/PgDn scroll  Esc cancel"
        }
        Mode::InterviewInput { .. } => "Enter next  Up previous  PgUp/PgDn scroll  Esc cancel",
        Mode::PromptReview { .. } => {
            "Enter apply  Tab switch action  j/k scroll  m mouse-select/scroll  Esc cancel"
        }
        Mode::Normal => {
            if width < 116 {
                "q quit  j/k move  Enter open  Space/s safe  ./n next  d doctor  x delete  ? help  c create"
            } else if width < 156 {
                "q quit  j/k move  1..6 view  Enter/o open  Space/s safe  ./n next  d doctor  f fix  x delete  p prune  ? help"
            } else {
                "q quit  Esc clear  j/k move  1..6 view  Enter/o open  Space/s safe  ./n next  d doctor  f fix  x delete-inactive  p prune  ? help  c create  r/w all  a amend  y rerun+note  g/h/u refresh  i stop"
            }
        }
    }
}

#[cfg(test)]
fn footer_shortcuts(app: &App) -> &'static str {
    footer_shortcuts_for_width(app, u16::MAX)
}

fn shortcut_help_text(app: &App) -> String {
    let current = app
        .selected_run()
        .map(|run| {
            format!(
                "Current run\n- outcome: {}\n- next: {}\n- safe action: {}\n- health: {}\n",
                ui_outcome_state(run),
                run.doctor.next,
                run.doctor.safe_next_action,
                run.doctor.health
            )
        })
        .unwrap_or_else(|| "Current run\n- none selected\n".to_string());
    format!(
        "Shortcut help\n\nNavigation\n- q quit\n- j/k move between runs\n- Enter or o open preferred artifact\n- 1..6 open Summary / Findings / Augmented / Execution / Brief / Doctor\n\nRun control\n- Space or s safe-next from doctor guidance\n- . or n start the next stage directly\n- r or w resume the whole stack until verification\n- i interrupt the current tracked job\n- d open doctor report\n- f apply doctor auto-fix\n\nWorkflow\n- c create a new pipeline\n- a amend and rewind\n- y open rerun input and optionally add fresh user guidance before creating the rerun\n- b step back to review\n- v recheck verification\n\nRefresh and maintenance\n- g reload run snapshots\n- h refresh host probe\n- u refresh prompts\n- x delete the selected inactive run\n- p prune old inactive runs\n- ? show this help in the lower pane\n\nOutcome meanings\n- follow-up-needed = this run finished verification and is done, but verification recommends a new rerun\n- in-progress = the pipeline is still moving toward verification\n- verifying = verification is the current late-stage gate\n\n{}",
        current
    )
}

fn draw_footer(frame: &mut ratatui::Frame<'_>, area: Rect, app: &App) {
    let paragraph = Paragraph::new(footer_shortcuts_for_width(app, area.width))
        .style(Style::default().bg(UI_PANEL_BG_ALT).fg(UI_TEXT));
    frame.render_widget(paragraph, area);
}

fn draw_status_bar(frame: &mut ratatui::Frame<'_>, area: Rect, app: &App) {
    let line = if app.wizard_selected {
        if let Some(job) = app.wizard_job() {
            let stalled = job_stalled_without_run(job);
            let mut spans = vec![
                Span::raw("run="),
                Span::styled(
                    "creating-pipeline",
                    Style::default().add_modifier(Modifier::BOLD),
                ),
                Span::raw(" | "),
            ];
            spans.extend(status_pair("stage0", job.command_hint.clone(), UI_INFO));
            spans.push(Span::raw(" | "));
            spans.extend(status_pair(
                "elapsed",
                format!("{}s", job.started_at.elapsed().as_secs()),
                UI_TEXT,
            ));
            spans.push(Span::raw(" | "));
            spans.extend(status_pair(
                "status",
                if stalled { "stalled" } else { "running" },
                running_state_color(stalled),
            ));
            Line::from(spans)
        } else {
            Line::from("No run selected")
        }
    } else if let Some(run) = app.selected_run() {
        let run_name = run
            .run_dir
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("run");
        let verify = ui_verification_state(run);
        let goal = ui_goal_state(run);
        let outcome = ui_outcome_state(run);
        let narrow = area.width < 110;
        let compact = area.width < 150;
        let mut spans = vec![
            Span::raw("run="),
            Span::styled(
                run_name.to_string(),
                Style::default().add_modifier(Modifier::BOLD),
            ),
            Span::raw(" | "),
        ];
        spans.extend(status_pair(
            "outcome",
            outcome.clone(),
            outcome_color(&outcome),
        ));
        spans.push(Span::raw(" | "));
        spans.extend(status_pair(
            "health",
            run.doctor.health.clone(),
            health_color(&run.doctor.health),
        ));
        spans.push(Span::raw(" | "));
        spans.extend(status_pair(
            "verification",
            verify.clone(),
            verification_color(&verify),
        ));
        spans.push(Span::raw(" | "));
        spans.extend(status_pair("verdict", goal.clone(), goal_color(&goal)));
        spans.push(Span::raw(" | "));
        spans.extend(status_pair(
            "next",
            run.doctor.next.clone(),
            next_stage_color(&run.doctor.next),
        ));
        if !compact {
            if let Some(attempt) = &run.doctor.last_attempt {
                spans.push(Span::raw(" | "));
                spans.extend(status_pair(
                    "last",
                    format!("{}({})", attempt.stage, attempt.status),
                    UI_MUTED,
                ));
            }
        }
        if !narrow {
            spans.push(Span::raw(" | "));
            spans.extend(status_pair("host", run.doctor.host_probe.clone(), UI_TEXT));
            if !compact {
                let token_text = status_bar_tokens(&run.token_summary);
                if !token_text.is_empty() {
                    spans.push(Span::raw(token_text));
                }
            }
        }
        if let Some(job) = &app.job {
            spans.push(Span::raw(" | "));
            if job.run_dir == run.run_dir {
                let stalled = job_stalled(run, job);
                spans.extend(status_pair(
                    "running",
                    running_job_display_label(Some(run), job),
                    running_state_color(stalled),
                ));
                if stalled {
                    spans.push(Span::raw(" | "));
                    spans.extend(status_pair("status", "stalled", UI_WARN));
                }
            } else {
                spans.extend(status_pair("background", running_job_label(job), UI_INFO));
            }
        }
        if let Some(job) = app.wizard_job() {
            spans.push(Span::raw(" | "));
            spans.extend(status_pair(
                "wizard",
                running_job_label(job),
                running_state_color(job_stalled_without_run(job)),
            ));
        }
        Line::from(spans)
    } else {
        Line::from("No run selected")
    };
    let paragraph = Paragraph::new(line).style(Style::default().bg(UI_PANEL_BG_ALT).fg(UI_TEXT));
    frame.render_widget(paragraph, area);
}

fn draw_body(frame: &mut ratatui::Frame<'_>, _area: Rect, app: &App) {
    let size = frame.size();
    let layout = root_layout(size);
    let body = body_rects(layout[2]);
    let rects = normal_mode_rects(size);
    draw_runs(frame, rects.runs, app);
    draw_details(frame, body.details, app);
}

fn draw_runs(frame: &mut ratatui::Frame<'_>, area: Rect, app: &App) {
    let mut items: Vec<ListItem<'_>> = Vec::new();
    if let Some(job) = app.wizard_job() {
        let stalled = job_stalled_without_run(job);
        let line1 = "Creating Pipeline".to_string();
        let line2 = Line::from(vec![
            Span::raw("stage0 | step="),
            Span::styled(job.command_hint.clone(), Style::default().fg(UI_INFO)),
        ]);
        let mut line3_spans = vec![
            Span::raw("status="),
            Span::styled(
                if stalled { "stalled" } else { "running" },
                Style::default().fg(running_state_color(stalled)),
            ),
            Span::raw(" | elapsed="),
            Span::styled(
                format!("{}s", job.started_at.elapsed().as_secs()),
                Style::default().fg(UI_TEXT),
            ),
        ];
        if stalled {
            line3_spans.push(Span::raw(" | "));
            line3_spans.push(Span::styled(
                "interrupt suggested",
                Style::default().fg(UI_WARN),
            ));
        }
        items.push(ListItem::new(vec![
            Line::from(line1),
            line2,
            Line::from(line3_spans),
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
            let verify = ui_verification_state(run);
            let outcome = ui_outcome_state(run);
            let mut line2_spans = vec![
                Span::raw("outcome="),
                Span::styled(
                    outcome.clone(),
                    Style::default().fg(outcome_color(&outcome)),
                ),
                Span::raw(" | health="),
                Span::styled(
                    run.doctor.health.clone(),
                    Style::default().fg(health_color(&run.doctor.health)),
                ),
            ];
            let mut line3_spans = vec![
                Span::raw("verify="),
                Span::styled(
                    verify.clone(),
                    Style::default().fg(verification_color(&verify)),
                ),
                Span::raw(" | next="),
                Span::styled(
                    run.doctor.next.clone(),
                    Style::default().fg(next_stage_color(&run.doctor.next)),
                ),
            ];
            if !running.is_empty() {
                let stalled = app
                    .job
                    .as_ref()
                    .filter(|job| job.run_dir == run.run_dir)
                    .map(|job| job_stalled(run, job))
                    .unwrap_or(false);
                line3_spans.push(Span::raw(" | running="));
                line3_spans.push(Span::styled(
                    running
                        .trim_start_matches(" | running=")
                        .trim_end_matches(" | stalled")
                        .to_string(),
                    Style::default().fg(running_state_color(stalled)),
                ));
                if stalled {
                    line3_spans.push(Span::raw(" | "));
                    line3_spans.push(Span::styled("stalled", Style::default().fg(UI_WARN)));
                }
            }
            ListItem::new(vec![
                Line::from(line1),
                Line::from(std::mem::take(&mut line2_spans)),
                Line::from(line3_spans),
            ])
        }));
    }
    let list = List::new(items)
        .style(Style::default().fg(UI_TEXT).bg(UI_PANEL_BG))
        .block(themed_block("Runs"))
        .highlight_style(
            Style::default()
                .bg(UI_SELECTION_BG)
                .fg(UI_TEXT)
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
    let details = detail_rects(area);
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
                clamp_scroll_for_text(&preview_text, details.preview, app.preview_scroll);
            let preview = Paragraph::new(preview_text)
                .style(Style::default().fg(UI_TEXT).bg(UI_PANEL_BG))
                .block(themed_block("Creating Pipeline"))
                .scroll((preview_scroll, 0))
                .wrap(Wrap { trim: false });
            let detail = Paragraph::new(detail_text)
                .style(Style::default().fg(UI_TEXT).bg(UI_PANEL_BG))
                .block(themed_block("Activity"))
                .wrap(Wrap { trim: false });
            frame.render_widget(preview, details.preview);
            frame.render_widget(detail, details.facts);
            frame.render_widget(
                Paragraph::new(
                    "Primary\n- i interrupt stage0 creation\n- c queue another pipeline later",
                )
                .style(Style::default().fg(UI_TEXT).bg(UI_PANEL_BG))
                .block(themed_block("Actions"))
                .wrap(Wrap { trim: false }),
                details.actions,
            );
            return;
        }
    }
    let (detail_text, action_text, preview_title, preview_text) =
        if let Some(run) = app.selected_run() {
            let mut lines = vec![
                format!("Run: {}", run.run_dir.display()),
                format!("Outcome: {}", ui_outcome_state(run)),
                format!("Health: {}", run.doctor.health),
                format!("Verification: {}", ui_verification_state(run)),
                format!("Verdict: {}", ui_goal_state(run)),
                format!("Next: {}", run.doctor.next),
                format!("Safe action: {}", run.doctor.safe_next_action),
                format!("Host probe: {}", run.doctor.host_probe),
                format!("Tokens: {}", run_token_summary_line(&run.token_summary)),
            ];
            if ui_outcome_state(run) == "follow-up-needed" {
                lines.push(
                    "Lifecycle: verification completed; this run is done, but a follow-up rerun is recommended."
                        .to_string(),
                );
            }
            if run.doctor.health == "broken" {
                lines.push("Doctor: press `d` or `6` to inspect issues and fixes.".to_string());
            }
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
            let preview_title = running_preview_title(&preview_title, run);
            let action_text = action_panel_text_for_width(app, Some(run), details.actions.width);
            (lines.join("\n"), action_text, preview_title, preview_text)
        } else {
            (
                "No run selected.".to_string(),
                action_panel_text_for_width(app, None, details.actions.width),
                "Preview".to_string(),
                "No substantive artifact is available yet.".to_string(),
            )
        };
    let preview_scroll = if app.preview_scroll == 0 {
        0
    } else {
        clamp_scroll_for_text(&preview_text, details.preview, app.preview_scroll)
    };
    let preview = Paragraph::new(preview_text)
        .style(Style::default().fg(UI_TEXT).bg(UI_PANEL_BG))
        .block(themed_block(preview_title))
        .scroll((preview_scroll, 0))
        .wrap(Wrap { trim: false });
    let detail = Paragraph::new(detail_text)
        .style(Style::default().fg(UI_TEXT).bg(UI_PANEL_BG))
        .block(themed_block("Run Facts"))
        .wrap(Wrap { trim: false });
    let actions = Paragraph::new(action_text)
        .style(Style::default().fg(UI_TEXT).bg(UI_PANEL_BG))
        .block(themed_block("Actions"))
        .wrap(Wrap { trim: false });
    frame.render_widget(preview, details.preview);
    frame.render_widget(detail, details.facts);
    frame.render_widget(actions, details.actions);
}

fn draw_logs(frame: &mut ratatui::Frame<'_>, area: Rect, app: &App) {
    let (title, lines, highlight_running, stalled) = log_panel_state(app);
    let content = lines
        .iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    let scroll = effective_log_scroll(&content, area, app.log_scroll);
    let logs = Paragraph::new(lines)
        .style(Style::default().fg(UI_TEXT).bg(UI_PANEL_BG))
        .block(if highlight_running {
            highlighted_running_block(title, stalled)
        } else {
            themed_block(title)
        })
        .scroll((scroll, 0))
        .wrap(Wrap { trim: false });
    frame.render_widget(logs, area);
}

fn log_panel_state(app: &App) -> (String, Vec<Line<'static>>, bool, bool) {
    if let Some(job) = app.wizard_job().filter(|_| app.wizard_selected) {
        let persisted = runtime::tail_process_log(&job.run_dir, 20);
        let stalled = job_stalled_without_run(job);
        let mut lines = vec![
            Line::from(vec![
                Span::styled(
                    "Stage0",
                    Style::default().fg(UI_INFO).add_modifier(Modifier::BOLD),
                ),
                Span::raw(": "),
                Span::styled(job.command_hint.clone(), Style::default().fg(UI_TEXT)),
            ]),
            Line::from(vec![
                Span::styled("Job dir", Style::default().fg(UI_MUTED)),
                Span::raw(": "),
                Span::raw(job.run_dir.display().to_string()),
            ]),
            Line::default(),
        ];
        if !job.stream_lines.is_empty() {
            lines.push(Line::from(vec![Span::styled(
                "Process output:",
                Style::default().fg(UI_ACCENT).add_modifier(Modifier::BOLD),
            )]));
            lines.extend(job.stream_lines.iter().cloned().map(Line::from));
            lines.push(Line::default());
        }
        if persisted.is_empty() {
            lines.push(Line::from("Waiting for fresh stage0 output."));
        } else {
            lines.extend(persisted.into_iter().map(Line::from));
        }
        return (
            format!("Running: {}", running_job_label(job)),
            lines,
            true,
            stalled,
        );
    }
    if let Some(job) = &app.job {
        if let Some(run) = app.selected_run() {
            if job.run_dir == run.run_dir {
                let (log_title, log_lines) = live_log_excerpt(run, job);
                let run_name = run
                    .run_dir
                    .file_name()
                    .and_then(|s| s.to_str())
                    .unwrap_or("run");
                let stalled = job_stalled(run, job);
                let mut lines = vec![
                    Line::from(vec![
                        Span::styled(
                            "Stage",
                            Style::default()
                                .fg(if stalled { UI_WARN } else { UI_INFO })
                                .add_modifier(Modifier::BOLD),
                        ),
                        Span::raw(": "),
                        Span::styled(
                            format!(
                                "{}{}",
                                job_display_label(Some(run), job),
                                if job.attached { " (attached)" } else { "" }
                            ),
                            Style::default()
                                .fg(if stalled { UI_WARN } else { UI_TEXT })
                                .add_modifier(Modifier::BOLD),
                        ),
                    ]),
                    Line::from(vec![
                        Span::styled("Run", Style::default().fg(UI_MUTED)),
                        Span::raw(": "),
                        Span::raw(run_name.to_string()),
                    ]),
                    Line::from(vec![
                        Span::styled("Log source", Style::default().fg(UI_MUTED)),
                        Span::raw(": "),
                        Span::raw(log_title.clone()),
                    ]),
                    Line::default(),
                ];
                if let Some(launcher) = job_launcher_label(job) {
                    lines.insert(
                        1,
                        Line::from(vec![
                            Span::styled("Launcher", Style::default().fg(UI_ACCENT)),
                            Span::raw(": "),
                            Span::styled(launcher, Style::default().fg(UI_TEXT)),
                        ]),
                    );
                }
                if !job.stream_lines.is_empty() {
                    lines.push(Line::from(vec![Span::styled(
                        "Process output:",
                        Style::default().fg(UI_ACCENT).add_modifier(Modifier::BOLD),
                    )]));
                    lines.extend(job.stream_lines.iter().cloned().map(Line::from));
                    lines.push(Line::default());
                }
                if stalled {
                    lines.push(Line::from(vec![Span::styled(
                        "No fresh output for a while. This stage may be stalled.",
                        Style::default().fg(UI_WARN).add_modifier(Modifier::BOLD),
                    )]));
                    lines.push(Line::from(vec![
                        Span::styled("Press ", Style::default().fg(UI_MUTED)),
                        Span::styled(
                            "i",
                            Style::default().fg(UI_WARN).add_modifier(Modifier::BOLD),
                        ),
                        Span::styled(
                            " to interrupt the current action.",
                            Style::default().fg(UI_MUTED),
                        ),
                    ]));
                    lines.push(Line::default());
                }
                lines.extend(log_lines.into_iter().map(Line::from));
                return (
                    format!("Running: {}", running_job_display_label(Some(run), job)),
                    lines,
                    true,
                    stalled,
                );
            }
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
                Line::from(vec![
                    Span::styled("Background job", Style::default().fg(UI_INFO)),
                    Span::raw(": "),
                    Span::raw(format!("{} for {}", running_job_label(job), bg_name)),
                ]),
                Line::from(vec![
                    Span::styled("Selected run", Style::default().fg(UI_MUTED)),
                    Span::raw(": "),
                    Span::raw(run_name.to_string()),
                ]),
                Line::from(vec![
                    Span::styled("Log source", Style::default().fg(UI_MUTED)),
                    Span::raw(": "),
                    Span::raw(log_title.clone()),
                ]),
                Line::default(),
            ];
            if job_stalled_without_run(job)
                || app
                    .runs
                    .iter()
                    .find(|candidate| candidate.run_dir == job.run_dir)
                    .map(|job_run| job_stalled(job_run, job))
                    .unwrap_or(false)
            {
                lines.push(Line::from(vec![Span::styled(
                    "Background job may be stalled. Press i to interrupt it.",
                    Style::default().fg(UI_WARN).add_modifier(Modifier::BOLD),
                )]));
                lines.push(Line::default());
            }
            lines.extend(log_lines.into_iter().map(Line::from));
            return (format!("Log tail: {log_title}"), lines, false, false);
        }
        let persisted = runtime::tail_process_log(&job.run_dir, 20);
        if persisted.is_empty() {
            return (
                "Running".to_string(),
                vec![Line::from(format!(
                    "{} is running in the background.",
                    running_job_label(job)
                ))],
                false,
                false,
            );
        }
        return (
            format!("Running: {}", running_job_label(job)),
            persisted.into_iter().map(Line::from).collect(),
            false,
            false,
        );
    }
    if !app.last_output.trim().is_empty() {
        return (
            "Last action".to_string(),
            app.last_output
                .lines()
                .map(|line| Line::from(line.to_string()))
                .collect(),
            false,
            false,
        );
    }
    if let Some(run) = app.selected_run() {
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
        return (
            format!("Log tail: {}", log_title),
            log_lines.into_iter().map(Line::from).collect(),
            false,
            false,
        );
    }
    (
        "Logs".to_string(),
        vec![Line::from("No log files yet.")],
        false,
        false,
    )
}

fn effective_log_scroll(text: &str, area: Rect, requested: u16) -> u16 {
    if requested == AUTO_TAIL_SCROLL {
        return clamp_scroll_for_text(text, area, u16::MAX);
    }
    clamp_scroll_for_text(text, area, requested)
}

fn log_panel_max_scroll(app: &App, area: Rect) -> u16 {
    let (_, lines, _, _) = log_panel_state(app);
    let content = lines
        .iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    clamp_scroll_for_text(&content, area, u16::MAX)
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

fn is_recent_output(path: &Path, max_age: Duration) -> bool {
    let Ok(meta) = std::fs::metadata(path) else {
        return false;
    };
    let Ok(modified) = meta.modified() else {
        return false;
    };
    modified >= SystemTime::now() - max_age
}

fn stage_has_fresh_live_log(run: &RunSnapshot, _job: &RunningJob, stage: &str) -> bool {
    let logs_dir = run.run_dir.join("logs");
    let trimmed = stage.trim();
    if trimmed.is_empty() || trimmed == "none" || trimmed == "rerun" {
        return is_recent_output(
            &runtime::process_log_path(&run.run_dir),
            Duration::from_secs(ACTIVE_STAGE_RECENT_OUTPUT_SECS),
        );
    }
    if [
        format!("{trimmed}.last.md"),
        format!("{trimmed}.stdout.log"),
        format!("{trimmed}.stderr.log"),
    ]
    .into_iter()
    .map(|candidate| logs_dir.join(candidate))
    .any(|path| is_recent_output(&path, Duration::from_secs(ACTIVE_STAGE_RECENT_OUTPUT_SECS)))
    {
        return true;
    }
    false
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

fn stage_has_fresh_output(run: &RunSnapshot, _job: &RunningJob, stage: &str) -> bool {
    let mut candidates = vec![
        run.run_dir.join("logs").join(format!("{stage}.last.md")),
        run.run_dir.join("logs").join(format!("{stage}.stdout.log")),
        run.run_dir.join("logs").join(format!("{stage}.stderr.log")),
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
        "" | "none" | "rerun" => {
            candidates.push(runtime::process_log_path(&run.run_dir));
        }
        _ => {}
    }
    candidates
        .into_iter()
        .any(|path| is_recent_output(&path, Duration::from_secs(ACTIVE_STAGE_RECENT_OUTPUT_SECS)))
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
        .style(Style::default().fg(UI_TEXT).bg(UI_PANEL_BG))
        .block(themed_block("Amend Run"))
        .wrap(Wrap { trim: false });
    frame.render_widget(paragraph, popup);
}

fn new_run_popup_rect(area: Rect) -> Rect {
    centered_rect_with_min(area, 92, 84, 72, 18)
}

fn new_run_popup_layout(popup: Rect) -> Vec<Rect> {
    let compact = popup.height <= 20 || popup.width <= 82;
    Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(if compact { 4 } else { 5 }),
            Constraint::Min(if compact { 4 } else { 6 }),
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Length(2),
            Constraint::Length(3),
        ])
        .split(popup)
        .to_vec()
}

fn interview_popup_rect(area: Rect) -> Rect {
    centered_rect_with_min(area, 90, 85, 72, 17)
}

fn interview_popup_layout(popup: Rect) -> Vec<Rect> {
    let compact = popup.height <= 18 || popup.width <= 82;
    Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(if compact { 3 } else { 4 }),
            Constraint::Length(if compact { 3 } else { 4 }),
            Constraint::Length(if compact { 4 } else { 5 }),
            Constraint::Min(3),
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

#[cfg(test)]
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

#[cfg(test)]
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

#[cfg(test)]
fn new_run_cursor_position(
    area: Rect,
    draft: &NewRunDraft,
    requested_task_scroll: u16,
) -> Option<(u16, u16)> {
    match draft.field {
        NewRunField::Task => new_run_task_layout_state(area, draft, requested_task_scroll).cursor,
        NewRunField::Workspace => {
            let popup = new_run_popup_rect(area);
            let layout = new_run_popup_layout(popup);
            text_cursor_position(layout[2], &draft.workspace, draft.workspace_cursor, 0)
        }
        NewRunField::Title => {
            let popup = new_run_popup_rect(area);
            let layout = new_run_popup_layout(popup);
            text_cursor_position(layout[3], &draft.title, draft.title_cursor, 0)
        }
        NewRunField::Start | NewRunField::Cancel => None,
    }
}

#[cfg(test)]
fn interview_cursor_position(
    area: Rect,
    buffer: &str,
    requested_answer_scroll: u16,
) -> Option<(u16, u16)> {
    interview_answer_layout_state(area, buffer, requested_answer_scroll).cursor
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
        selected_button_style(UI_SUCCESS)
    } else {
        idle_button_style()
    };
    let start_style = if matches!(selected, PromptReviewAction::CreateAndStart) {
        selected_button_style(UI_ACCENT)
    } else {
        idle_button_style()
    };
    let cancel_style = if matches!(selected, PromptReviewAction::Cancel) {
        selected_button_style(UI_WARN)
    } else {
        idle_button_style()
    };
    let prompt_text = prompt_review_text(final_task_path);
    frame.render_widget(
        Paragraph::new(
            "Review the final task prompt before creating the run.\n\nj/k scroll  Tab switch action  Enter apply  Esc cancel"
                .to_string(),
        )
        .style(Style::default().fg(UI_TEXT).bg(UI_PANEL_BG))
        .block(themed_block("Final Task Prompt"))
        .wrap(Wrap { trim: false }),
        layout[0],
    );
    frame.render_widget(
        Paragraph::new(prompt_text)
            .style(Style::default().fg(UI_TEXT).bg(UI_PANEL_BG))
            .block(themed_panel())
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
        .style(Style::default().fg(UI_TEXT).bg(UI_PANEL_BG))
        .block(themed_panel()),
        layout[2],
    );
}

fn field_block(title: &str, value: &str, active: bool, scroll: u16) -> Paragraph<'static> {
    let block = if active {
        themed_block(title.to_string()).border_style(Style::default().fg(UI_ACCENT))
    } else {
        themed_block(title.to_string())
    };
    Paragraph::new(value.to_string())
        .style(Style::default().fg(UI_TEXT).bg(UI_PANEL_BG))
        .block(block)
        .scroll((scroll, 0))
        .wrap(Wrap { trim: false })
}

fn action_panel_text_for_width(app: &App, run: Option<&RunSnapshot>, width: u16) -> String {
    let compact = width < 52;
    if let Some(job) = &app.job {
        if let Some(run) = run {
            if job.run_dir == run.run_dir {
                let open_hint = if run.doctor.health == "broken" {
                    "Enter open doctor report"
                } else {
                    "Enter open preferred artifact"
                };
                if compact {
                    return format!(
                        "Primary\n- i interrupt\n- d doctor\n- f auto-fix\n- {open_hint}\n- 1..6 artifacts\n\nCurrent\n- running: {}\n- outcome: {}\n- next: {}\n- safe: {}\n",
                        running_job_display_label(Some(run), job),
                        ui_outcome_state(run),
                        run.doctor.next,
                        run.doctor.safe_next_action
                    );
                }
                return format!(
                    "Primary\n- i interrupt current stage\n- d doctor report\n- f doctor auto-fix\n- j/k switch runs\n- {open_hint}\n- 1 Summary  2 Findings  3 Augmented  4 Execution  5 Brief  6 Doctor\n\nBackground-capable\n- c create another pipeline\n- x delete only inactive runs\n- p prune skips active runs\n\nCurrent\n- running: {}\n- outcome: {}\n- next: {}\n- safe action: {}\n",
                    running_job_display_label(Some(run), job),
                    ui_outcome_state(run),
                    run.doctor.next,
                    run.doctor.safe_next_action
                );
            }
        }
        if compact {
            return format!(
                "Background\n- {}\n- i interrupt\n- d doctor\n- f auto-fix\n- n next  s safe\n- 1..6 artifacts\n",
                running_job_label(job)
            );
        }
        return format!(
            "Background job\n- {}\n- i interrupt tracked job\n- d open doctor for the selected run\n- f apply doctor auto-fix to the selected run\n- j/k move between runs\n- c create another pipeline\n- n next stage on the selected inactive run\n- s safe-next on the selected inactive run\n- r/w run the whole stack on the selected inactive run\n- y rerun\n- h/u refresh helpers\n- b step back to review\n- v recheck verification\n- 1..6 open artifacts for the selected run\n- x delete only inactive runs\n- p prune skips active runs\n",
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
        let open_hint = if run.doctor.health == "broken" {
            "Enter open doctor report"
        } else {
            "Enter open preferred artifact"
        };
        if compact {
            return format!(
                "Primary\n- d doctor\n- f auto-fix\n- n next\n- s safe-next\n- r/w run all\n- {open_hint}\n- 1..6 artifacts\n\nSecondary\n- a amend  y rerun\n- x delete  p prune\n- h host  u prompts\n- b review  v verify\n\nCurrent\n- outcome: {}\n- next: {}\n- safe: {}\n",
                ui_outcome_state(run),
                run.doctor.next,
                run.doctor.safe_next_action
            );
        }
        format!(
            "Primary\n- d doctor report\n- f doctor auto-fix\n- n next stage\n- s safe-next from doctor\n- r/w run whole stack\n- {open_hint}\n- 1 Summary  2 Findings  3 Augmented  4 Execution  5 Brief  6 Doctor\n\nSecondary\n- a amend and rewind\n- y rerun\n- h refresh host probe\n- u refresh prompts\n- b step back to review\n- v recheck verification\n- x delete run\n- p prune runs\n\nCurrent\n- outcome: {}\n- next: {}\n- safe action: {}\n- solver fan-out uses parallel start-solvers when applicable\n",
            ui_outcome_state(run),
            run.doctor.next,
            run.doctor.safe_next_action
        )
    } else {
        "Primary\n- c create pipeline\n- g refresh run list\n- j/k move\n- q quit\n".to_string()
    }
}

#[cfg(test)]
fn action_panel_text(app: &App, run: Option<&RunSnapshot>) -> String {
    action_panel_text_for_width(app, run, u16::MAX)
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
        .style(Style::default().fg(UI_TEXT).bg(UI_PANEL_BG))
        .block(themed_block(title.to_string()))
        .wrap(Wrap { trim: false });
    frame.render_widget(paragraph, popup);
}

fn button_line(selected: ConfirmChoice, confirm_label: &str) -> Line<'static> {
    let cancel_style = if matches!(selected, ConfirmChoice::Cancel) {
        selected_button_style(UI_WARN)
    } else {
        idle_button_style()
    };
    let confirm_style = if matches!(selected, ConfirmChoice::Confirm) {
        selected_button_style(UI_SUCCESS)
    } else {
        idle_button_style()
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
        "{}  scroll j/k PgUp/PgDn  switch [ ] or 1..6/d  Esc close",
        label
    );
    let paragraph = Paragraph::new(content)
        .style(Style::default().fg(UI_TEXT).bg(UI_PANEL_BG))
        .block(themed_block(title))
        .scroll((scroll, 0))
        .wrap(Wrap { trim: false });
    frame.render_widget(paragraph, popup);
}

fn preferred_artifact_kind(run: &RunSnapshot) -> ArtifactKind {
    if run.doctor.health.trim().eq_ignore_ascii_case("broken") {
        return ArtifactKind::Doctor;
    }
    match run.preview_label.as_str() {
        "Summary" => ArtifactKind::Summary,
        "Request" => ArtifactKind::Summary,
        "Findings" => ArtifactKind::Findings,
        "Augmented" => ArtifactKind::Augmented,
        "Execution" => ArtifactKind::Execution,
        "Brief" => ArtifactKind::Brief,
        "Doctor" => ArtifactKind::Doctor,
        _ => ArtifactKind::Summary,
    }
}

fn artifact_content(run: &RunSnapshot, kind: ArtifactKind) -> String {
    if matches!(kind, ArtifactKind::Doctor) {
        return doctor_artifact_content(run);
    }
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

fn doctor_artifact_content(run: &RunSnapshot) -> String {
    let mut lines = vec![
        format!("Run: {}", run.run_dir.display()),
        format!("Health: {}", run.doctor.health),
        format!("Goal: {}", run.doctor.goal),
        format!("Next: {}", run.doctor.next),
        format!("Safe action: {}", run.doctor.safe_next_action),
        format!("Host probe: {}", run.doctor.host_probe),
    ];
    if let Some(drift) = &run.doctor.host_drift {
        lines.push(format!("Host drift: {drift}"));
    }
    if !run.doctor.stale.is_empty() {
        lines.push(format!("Stale: {}", run.doctor.stale.join(", ")));
    }
    if let Some(attempt) = &run.doctor.last_attempt {
        lines.push(String::new());
        lines.push(format!(
            "Last attempt: {} via {} -> {}",
            attempt.stage, attempt.label, attempt.status
        ));
        if let Some(code) = attempt.exit_code {
            lines.push(format!("Exit code: {code}"));
        }
        if !attempt.message.trim().is_empty() {
            lines.push(format!("Detail: {}", attempt.message));
        }
    }
    if !run.doctor.issues.is_empty() {
        lines.push(String::new());
        lines.push("Issues:".to_string());
        for issue in &run.doctor.issues {
            lines.push(format!("- {}", issue.message));
            lines.push(format!("  Fix: {}", issue.fix));
        }
    }
    if !run.doctor.warnings.is_empty() {
        lines.push(String::new());
        lines.push("Warnings:".to_string());
        for warning in &run.doctor.warnings {
            lines.push(format!("- {}", warning.message));
            lines.push(format!("  Fix: {}", warning.fix));
        }
    }
    if run.doctor.issues.is_empty() && run.doctor.warnings.is_empty() {
        lines.push(String::new());
        lines.push("No consistency issues detected.".to_string());
    }
    lines.push(String::new());
    lines.push("TUI actions:".to_string());
    lines.push("- d or 6 reopen this doctor report".to_string());
    if !run.doctor.fix_actions.is_empty() {
        lines.push("- f apply doctor auto-fix actions".to_string());
        lines.push(format!(
            "- planned fix: {}",
            run.doctor.fix_actions.join(" -> ")
        ));
    }
    lines.push("- s run safe-next from current doctor guidance".to_string());
    lines.push("- n start only the next stage".to_string());
    lines.push("- r or w resume the whole pipeline".to_string());
    lines.join("\n")
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
        ArtifactKind::Doctor => {
            "This view is generated from the latest doctor snapshot for the selected run."
        }
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

fn ui_outcome_state(run: &RunSnapshot) -> String {
    let goal = ui_goal_state(run);
    let verification = ui_verification_state(run);
    let next = run.doctor.next.trim();
    match (goal.as_str(), verification.as_str(), next) {
        ("partial", "done", "rerun") => "follow-up-needed".to_string(),
        ("complete", "done", "none") => "complete".to_string(),
        ("pending-verification", "pending", "verification") => "verifying".to_string(),
        ("not-evaluated", _, _) => "in-progress".to_string(),
        _ => goal,
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

fn outcome_color(outcome: &str) -> Color {
    match outcome.trim().to_ascii_lowercase().as_str() {
        "complete" => UI_SUCCESS,
        "follow-up-needed" | "verifying" | "in-progress" => UI_WARN,
        "blocked" | "failed" | "error" => UI_DANGER,
        _ => UI_TEXT,
    }
}

#[cfg(test)]
fn run_list_detail_lines(run: &RunSnapshot) -> (String, String) {
    (
        format!(
            "outcome={} | health={}",
            ui_outcome_state(run),
            run.doctor.health
        ),
        format!(
            "verify={} | next={}",
            ui_verification_state(run),
            run.doctor.next
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
        block_inner_area, clamp_scroll_for_text, default_new_run_draft, delete_run_if_safe,
        effective_log_scroll, ensure_job_slot_for_run, footer_shortcuts, goal_color,
        handle_finished_job, handle_mouse, handle_new_run_key, handle_normal_key, handle_paste,
        health_color, interview_cursor_position, interview_popup_layout, interview_popup_rect,
        job_display_label, key_is_char, live_log_excerpt, live_preview, new_run_cursor_position,
        new_run_popup_layout, new_run_popup_rect, normal_mode_rects, parse_embedded_json,
        poll_job, poll_tracked_job, preferred_summary_path, rerun_created_run_dir,
        run_list_detail_lines, running_job_label, shortcut_help_text, stale_terminal_notice,
        summary_title, text_scroll_for_cursor, ui_goal_state, ui_outcome_state,
        ui_verification_state, verification_color, App, ArtifactKind, FinishedJob,
        InterviewQuestionsPayload, JobKind, JobResult, Mode, NewRunDraft, NewRunField,
        ProcessLogNoiseFilter, RunningJob, AUTO_TAIL_SCROLL, UI_DANGER, UI_SUCCESS, UI_WARN,
    };
    use crate::engine::{Context, DoctorPayload, RunSnapshot, RunTokenSummary, StatusPayload};
    use crate::runtime;
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers, MouseEvent, MouseEventKind};
    use ratatui::layout::Rect;
    use serde_json::json;
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
            solver_stage_ids: Vec::new(),
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
    fn ui_outcome_state_marks_partial_rerun_as_follow_up_needed() {
        let run = sample_run("partial", "rerun", "done");
        assert_eq!(ui_outcome_state(&run), "follow-up-needed");
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

        assert_eq!(line2, "outcome=follow-up-needed | health=healthy");
        assert_eq!(line3, "verify=done | next=rerun");
    }

    #[test]
    fn footer_shortcuts_in_normal_mode_lists_safe_next_and_artifact_views() {
        let app = App {
            root: PathBuf::from("/tmp"),
            limit: 20,
            runs: vec![sample_run("partial", "rerun", "done")],
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

        let footer = footer_shortcuts(&app);
        assert!(footer.contains("1..6 view"));
        assert!(footer.contains("Space/s safe"));
        assert!(footer.contains("./n next"));
        assert!(footer.contains("? help"));
        assert!(footer.contains("d doctor"));
        assert!(footer.contains("f fix"));
        assert!(footer.contains("x delete-inactive"));
        assert!(footer.contains("p prune"));
    }

    #[test]
    fn footer_shortcuts_compact_normal_mode_mentions_doctor_and_next() {
        let app = App {
            root: PathBuf::from("/tmp"),
            limit: 20,
            runs: vec![sample_run("partial", "rerun", "done")],
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

        let footer = super::footer_shortcuts_for_width(&app, 100);
        assert!(footer.contains("d doctor"));
        assert!(footer.contains("Space/s safe"));
        assert!(footer.contains("./n next"));
        assert!(footer.contains("? help"));
        assert!(footer.contains("x delete"));
        assert!(!footer.contains("g/h/u refresh"));
    }

    #[test]
    fn footer_shortcuts_for_rerun_input_mentions_create_rerun() {
        let app = App {
            root: PathBuf::from("/tmp"),
            limit: 20,
            runs: vec![sample_run("partial", "rerun", "done")],
            selected: 0,
            wizard_selected: false,
            preview_scroll: 0,
            log_scroll: 0,
            mouse_capture_enabled: true,
            mode: Mode::RerunInput {
                buffer: String::new(),
                scroll: 0,
            },
            notice: String::new(),
            last_output: String::new(),
            last_refresh: Instant::now(),
            job: None,
            wizard_job: None,
        };

        let footer = super::footer_shortcuts_for_width(&app, 120);
        assert!(footer.contains("Enter create rerun"));
        assert!(footer.contains("Ctrl+U clear"));
    }

    #[test]
    fn shortcut_help_text_lists_fast_aliases_and_doctor_tools() {
        let app = App {
            root: PathBuf::from("/tmp"),
            limit: 20,
            runs: vec![sample_run("partial", "verification", "pending")],
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

        let help = shortcut_help_text(&app);
        assert!(help.contains("Space or s safe-next"));
        assert!(help.contains(". or n start the next stage directly"));
        assert!(help.contains("d open doctor report"));
        assert!(help.contains("y open rerun input"));
        assert!(help.contains("? show this help"));
    }

    #[test]
    fn handle_normal_key_y_opens_rerun_input() {
        let ctx = test_context();
        let mut app = App {
            root: PathBuf::from("/tmp"),
            limit: 20,
            runs: vec![sample_run("partial", "rerun", "done")],
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

        let _ = handle_normal_key(
            &ctx,
            &mut app,
            KeyEvent::new(KeyCode::Char('y'), KeyModifiers::NONE),
        );

        assert!(matches!(app.mode, Mode::RerunInput { .. }));
        assert!(app.notice.contains("optional guidance"));
    }

    #[test]
    fn stale_terminal_notice_matches_dead_process_message() {
        assert!(stale_terminal_notice(
            "Tracked process is no longer alive. Inspect run artifacts and logs."
        ));
        assert!(!stale_terminal_notice("review completed"));
    }

    #[test]
    fn action_panel_for_idle_run_mentions_safe_next_and_recheck_tools() {
        let run = sample_run("pending-verification", "verification", "pending");
        let app = App {
            root: PathBuf::from("/tmp"),
            limit: 20,
            runs: vec![run.clone()],
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

        let text = super::action_panel_text(&app, Some(&run));
        assert!(text.contains("safe-next"));
        assert!(text.contains("1 Summary"));
        assert!(text.contains("step back to review"));
        assert!(text.contains("recheck verification"));
        assert!(text.contains("doctor report"));
        assert!(text.contains("doctor auto-fix"));
        assert!(text.contains("x delete run"));
        assert!(text.contains("p prune runs"));
    }

    #[test]
    fn action_panel_compact_mode_mentions_doctor_and_artifact_range() {
        let run = sample_run("pending-verification", "verification", "pending");
        let app = App {
            root: PathBuf::from("/tmp"),
            limit: 20,
            runs: vec![run.clone()],
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

        let text = super::action_panel_text_for_width(&app, Some(&run), 40);
        assert!(text.contains("d doctor"));
        assert!(text.contains("f auto-fix"));
        assert!(text.contains("1..6 artifacts"));
        assert!(text.contains("x delete"));
        assert!(text.contains("p prune"));
    }

    #[test]
    fn preferred_artifact_kind_uses_doctor_for_broken_runs() {
        let mut run = sample_run("pending-verification", "verification", "pending");
        run.doctor.health = "broken".to_string();

        assert!(matches!(
            super::preferred_artifact_kind(&run),
            ArtifactKind::Doctor
        ));
    }

    #[test]
    fn semantic_colors_distinguish_healthy_complete_and_blocked_states() {
        assert_eq!(health_color("healthy"), UI_SUCCESS);
        assert_eq!(verification_color("pending"), UI_WARN);
        assert_eq!(goal_color("complete"), UI_SUCCESS);
        assert_eq!(goal_color("blocked"), UI_DANGER);
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
    fn handle_finished_interview_questions_with_empty_payload_enters_prompt_review() {
        let ctx = test_context();
        let session_dir = temp_dir("interview-empty-questions-session");
        let run_dir = temp_dir("interview-empty-questions-run");
        fs::create_dir_all(run_dir.join("runtime")).expect("create runtime dir");
        fs::write(
            run_dir.join("runtime").join("process.log"),
            "Starting stage0 interview questions\n",
        )
        .expect("write process log");
        let draft = NewRunDraft {
            task: "Review the code and keep it review-only.".to_string(),
            workspace: "/tmp/workspace".to_string(),
            title: "review-fast-path".to_string(),
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
                        "{{\n  \"session_dir\": \"{}\",\n  \"goal_summary\": \"Review only\",\n  \"questions\": []\n}}\n",
                        session_dir.display()
                    ),
                    stderr: String::new(),
                },
                detached_finish: false,
            },
        )
        .expect("handle finished empty interview questions");

        match &app.mode {
            Mode::PromptReview {
                final_task_path, ..
            } => {
                assert_eq!(final_task_path, &session_dir.join("final-task.md"));
                let final_task = fs::read_to_string(final_task_path).expect("read final task");
                assert!(final_task.contains("Review the code and keep it review-only."));
            }
            _ => panic!("expected prompt review mode"),
        }
        assert!(app
            .notice
            .contains("No clarification questions were needed"));

        let _ = fs::remove_dir_all(session_dir);
    }

    #[test]
    fn handle_finished_interview_questions_recovers_from_session_artifacts_when_stdout_omits_session_dir(
    ) {
        let ctx = test_context();
        let root = temp_dir("interview-recover-root");
        let session_dir = root.join("_interviews").join("20260330-171500-interview");
        fs::create_dir_all(&session_dir).expect("create session dir");
        fs::write(
            session_dir.join("questions.json"),
            serde_json::to_vec_pretty(&json!({
                "goal_summary": "Recovered from disk",
                "questions": [
                    {
                        "question": "Which repo should be the baseline?",
                        "reason": "Need a baseline.",
                        "required": true
                    }
                ]
            }))
            .expect("serialize questions"),
        )
        .expect("write questions.json");
        let run_dir = temp_dir("interview-recover-run");
        fs::create_dir_all(run_dir.join("runtime")).expect("create runtime dir");
        fs::write(
            run_dir.join("runtime").join("process.log"),
            "Starting stage0 interview questions\n{\"goal_summary\":\"Recovered from disk\",\"questions\":[{\"question\":\"Which repo should be the baseline?\",\"reason\":\"Need a baseline.\",\"required\":true}]}\n",
        )
        .expect("write process log");
        let draft = NewRunDraft {
            task: "Analyze CI agent technologies.".to_string(),
            workspace: "/tmp/workspace".to_string(),
            title: "recover-interview".to_string(),
            task_cursor: 0,
            workspace_cursor: 0,
            title_cursor: 0,
            field: NewRunField::Task,
        };
        let mut app = App {
            root: root.clone(),
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
                    stdout: "{\n  \"goal_summary\": \"Recovered from disk\",\n  \"questions\": [\n    {\n      \"question\": \"Which repo should be the baseline?\",\n      \"reason\": \"Need a baseline.\",\n      \"required\": true\n    }\n  ]\n}\n".to_string(),
                    stderr: String::new(),
                },
                detached_finish: false,
            },
        )
        .expect("handle finished recovered interview questions");

        match &app.mode {
            Mode::InterviewInput {
                session_dir,
                goal_summary,
                questions,
                ..
            } => {
                assert_eq!(
                    session_dir,
                    &root.join("_interviews").join("20260330-171500-interview")
                );
                assert_eq!(goal_summary, "Recovered from disk");
                assert_eq!(questions.len(), 1);
                assert_eq!(questions[0].id, "q1");
                assert_eq!(questions[0].why, "Need a baseline.");
            }
            _ => panic!("expected interview input mode"),
        }
        assert!(app.notice.contains("Recovered stage0 interview payload"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn handle_finished_interview_finalize_recovers_prompt_review_from_session_file() {
        let ctx = test_context();
        let session_dir = temp_dir("interview-finalize-recover-session");
        let final_task_path = session_dir.join("final-task.md");
        fs::write(&final_task_path, "# Final Task\n\nRecovered prompt.\n")
            .expect("write final task");
        let run_dir = temp_dir("interview-finalize-recover-run");
        fs::create_dir_all(run_dir.join("runtime")).expect("create runtime dir");
        fs::write(
            run_dir.join("runtime").join("process.log"),
            "Starting stage0 interview finalize\nnoise without json\n",
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
                kind: JobKind::InterviewFinalize {
                    draft: draft.clone(),
                    session_dir: session_dir.clone(),
                    answers: vec![json!({
                        "id": "scope",
                        "question": "Run verification too?",
                        "answer": "Yes"
                    })],
                },
                label: "interview-finalize".to_string(),
                run_dir,
                completed_log_hint: None,
                result: JobResult {
                    code: 0,
                    stdout: "not json".to_string(),
                    stderr: String::new(),
                },
                detached_finish: false,
            },
        )
        .expect("handle finished interview finalize");

        match &app.mode {
            Mode::PromptReview {
                final_task_path, ..
            } => assert_eq!(final_task_path, &session_dir.join("final-task.md")),
            _ => panic!("expected prompt review mode"),
        }
        assert!(app.notice.contains("Recovered the final task prompt"));

        let _ = fs::remove_dir_all(session_dir);
    }

    #[test]
    fn handle_finished_interview_finalize_failure_does_not_return_to_new_pipeline_popup() {
        let ctx = test_context();
        let session_dir = temp_dir("interview-finalize-failure-session");
        let run_dir = temp_dir("interview-finalize-failure-run");
        fs::create_dir_all(run_dir.join("runtime")).expect("create runtime dir");
        fs::write(
            run_dir.join("runtime").join("process.log"),
            "Starting stage0 interview finalize\nfailed\n",
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
            wizard_selected: true,
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
                kind: JobKind::InterviewFinalize {
                    draft,
                    session_dir: session_dir.clone(),
                    answers: Vec::new(),
                },
                label: "interview-finalize".to_string(),
                run_dir,
                completed_log_hint: None,
                result: JobResult {
                    code: 1,
                    stdout: String::new(),
                    stderr: "boom".to_string(),
                },
                detached_finish: false,
            },
        )
        .expect("handle finished interview finalize failure");

        assert!(matches!(app.mode, Mode::Normal));
        assert!(!app.wizard_selected);
        assert!(app.notice.contains("No final task prompt was recovered"));

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
    fn normal_mode_rects_stack_main_panels_on_narrow_terminals() {
        let rects = normal_mode_rects(Rect::new(0, 0, 118, 34));

        assert!(rects.preview.y > rects.runs.y);
        assert_eq!(rects.runs.width, 118);
        assert_eq!(rects.preview.width, 118);
        assert!(rects.logs.y > rects.preview.y);
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
        let mut run = sample_run("pending-verification", "solver-a", "pending");
        run.run_dir.clone_from(&run_dir);
        run.solver_stage_ids = vec!["solver-a".to_string(), "solver-b".to_string()];
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
    fn live_log_excerpt_waits_for_current_stage_even_when_process_log_is_fresh() {
        let run_dir = temp_dir("live-log-wait-current-stage");
        fs::create_dir_all(run_dir.join("logs")).expect("create logs");
        fs::create_dir_all(run_dir.join("runtime")).expect("create runtime");
        fs::write(
            run_dir.join("logs").join("intake.stdout.log"),
            "old intake output\n",
        )
        .expect("write intake log");
        fs::write(
            run_dir.join("runtime").join("process.log"),
            "Stage changed: solver-a\nstill waiting\n",
        )
        .expect("write process log");
        let mut run = sample_run("pending-verification", "solver-a", "pending");
        run.run_dir.clone_from(&run_dir);
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
            process_log: run_dir.join("runtime").join("process.log"),
            stream_rx: Some(stream_rx),
            completion_rx: Some(completion_rx),
            stream_lines: Vec::new(),
            attached: true,
            last_heartbeat: Instant::now(),
        };

        let (title, lines) = live_log_excerpt(&run, &job);

        assert_eq!(title, "Waiting for solver-a");
        assert!(lines
            .iter()
            .any(|line| line == "No fresh live output yet for stage `solver-a`."));

        let _ = fs::remove_dir_all(run_dir);
    }

    #[test]
    fn poll_tracked_job_prefers_stream_updates_over_retailing_process_log() {
        let run_dir = temp_dir("poll-stream-preferred");
        fs::create_dir_all(run_dir.join("runtime")).expect("create runtime");
        fs::write(
            run_dir.join("runtime").join("process.log"),
            "persisted old line\n",
        )
        .expect("write process log");
        let (stream_tx, stream_rx) = mpsc::channel();
        let (_completion_tx, completion_rx) = mpsc::channel();
        let mut job = RunningJob {
            kind: JobKind::RunAction,
            label: "resume".to_string(),
            run_dir: run_dir.clone(),
            log_hint: Some("solver-a".to_string()),
            command_hint: "resume until verification".to_string(),
            started_at: Instant::now() - Duration::from_secs(5),
            started_wallclock: SystemTime::now() - Duration::from_secs(5),
            pid: 1,
            pgid: 1,
            process_log: run_dir.join("runtime").join("process.log"),
            stream_rx: Some(stream_rx),
            completion_rx: Some(completion_rx),
            stream_lines: Vec::new(),
            attached: false,
            last_heartbeat: Instant::now(),
        };
        stream_tx
            .send("fresh stream line".to_string())
            .expect("send stream line");

        let finished = poll_tracked_job(&mut job, &[]);

        assert!(finished.is_none());
        assert_eq!(job.stream_lines, vec!["fresh stream line"]);

        let _ = fs::remove_dir_all(run_dir);
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
    fn rerun_created_run_dir_accepts_snapshot_only_run_path() {
        let created = temp_dir("rerun-created-snapshot");
        fs::create_dir_all(created.join("runtime")).expect("create runtime");
        fs::write(created.join("runtime").join("plan.snapshot.json"), "{}\n")
            .expect("write snapshot");

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
    fn effective_log_scroll_defaults_log_panel_to_tail() {
        let area = Rect::new(0, 0, 80, 8);
        let content = (0..40)
            .map(|index| format!("line {index}"))
            .collect::<Vec<_>>()
            .join("\n");
        let scroll = effective_log_scroll(&content, area, AUTO_TAIL_SCROLL);
        let max_scroll = clamp_scroll_for_text(&content, area, u16::MAX);
        assert_eq!(scroll, max_scroll);
        assert!(scroll > 0);
    }

    #[test]
    fn mouse_wheel_over_logs_from_follow_tail_moves_above_bottom() {
        let mut app = App {
            root: PathBuf::from("/tmp"),
            limit: 20,
            runs: Vec::new(),
            selected: 0,
            wizard_selected: false,
            preview_scroll: 0,
            log_scroll: AUTO_TAIL_SCROLL,
            mouse_capture_enabled: true,
            mode: Mode::Normal,
            notice: String::new(),
            last_output: (0..60)
                .map(|index| format!("line {index}"))
                .collect::<Vec<_>>()
                .join("\n"),
            last_refresh: Instant::now(),
            job: None,
            wizard_job: None,
        };
        let rects = normal_mode_rects(Rect::new(0, 0, 120, 40));

        handle_mouse(
            &mut app,
            MouseEvent {
                kind: MouseEventKind::ScrollUp,
                column: rects.logs.x.saturating_add(2),
                row: rects.logs.y.saturating_add(2),
                modifiers: KeyModifiers::NONE,
            },
            Rect::new(0, 0, 120, 40),
        );

        assert_ne!(app.log_scroll, AUTO_TAIL_SCROLL);
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
    fn spawn_engine_job_tracks_tui_owner_pid_immediately() {
        let ctx = test_context();
        let run_dir = temp_dir("spawn-engine-job-owner-pid");
        let mut run = sample_run("pending-verification", "solver-a", "pending");
        run.run_dir.clone_from(&run_dir);
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
            job: None,
            wizard_job: None,
        };

        super::spawn_engine_job(
            &ctx,
            &mut app,
            JobKind::RunAction,
            "resume".to_string(),
            run_dir.clone(),
            Some("solver-a".to_string()),
            "resume until verification".to_string(),
            move |_| {
                std::thread::sleep(Duration::from_millis(200));
                Ok(crate::engine::CommandResult::ok("ok"))
            },
        )
        .expect("spawn engine job");

        let state = runtime::load_job_state(&run_dir).expect("load runtime state");
        assert_eq!(state.pid, std::process::id() as i32);
        assert_eq!(state.status, "running");

        std::thread::sleep(Duration::from_millis(300));
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
    fn poll_job_keeps_start_next_stage_after_completion_of_single_step() {
        let ctx = test_context();
        let run_dir = temp_dir("start-next-stage-sticky");
        runtime::start_job(
            &run_dir,
            "start-next",
            Some("intake"),
            "start-next",
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
                label: "start-next".to_string(),
                run_dir: run_dir.clone(),
                log_hint: Some("intake".to_string()),
                command_hint: "start-next".to_string(),
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
            Some("intake")
        );
        assert_eq!(
            runtime::load_job_state(&run_dir)
                .and_then(|state| state.stage)
                .as_deref(),
            Some("intake")
        );

        let _ = runtime::finish_job(&run_dir, "completed", Some(0), None);
        let _ = fs::remove_dir_all(run_dir);
    }

    #[test]
    fn resolve_action_log_hint_prefers_live_next_stage_over_stale_snapshot_for_start_next() {
        let mut ctx = test_context();
        ctx.repo_root = PathBuf::from("/tmp/multi-agent-pipeline");
        let workspace = temp_dir("live-next-stage-workspace");
        let output_root = temp_dir("live-next-stage-output");
        let result = crate::engine::task_flow_capture(
            &ctx,
            "create-run",
            &[
                "--task".to_string(),
                "Проведи обзор кода без изменения файлов.".to_string(),
                "--workspace".to_string(),
                workspace.display().to_string(),
                "--output-dir".to_string(),
                output_root.display().to_string(),
                "--prompt-format".to_string(),
                "compact".to_string(),
                "--summary-language".to_string(),
                "ru".to_string(),
            ],
        )
        .expect("create run");
        assert_eq!(result.code, 0);
        let run_dir = PathBuf::from(result.stdout.trim());

        let mut stale_run = sample_run("pending-verification", "solver-a", "pending");
        stale_run.run_dir.clone_from(&run_dir);
        stale_run.status.next = "solver-a".to_string();
        stale_run.doctor.next = "solver-a".to_string();

        let hint = super::resolve_action_log_hint(&ctx, "start-next", &run_dir, Some(&stale_run));

        assert_eq!(hint.as_deref(), Some("intake"));

        let _ = fs::remove_dir_all(output_root);
        let _ = fs::remove_dir_all(workspace);
    }

    #[test]
    fn process_log_noise_filter_suppresses_codex_analytics_html() {
        let mut filter = ProcessLogNoiseFilter::default();

        assert_eq!(
            filter.filter_line(
                "2026-03-30T09:51:39.984278Z  WARN codex_core::analytics_client: events failed with status 403 Forbidden: <html>"
            ),
            Some("[suppressed codex analytics 403 HTML noise]".to_string())
        );
        assert_eq!(filter.filter_line("  <head>"), None);
        assert_eq!(filter.filter_line("  </html>"), None);
        assert_eq!(
            filter.filter_line("Completed execution with exit code 0."),
            Some("Completed execution with exit code 0.".to_string())
        );
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
    fn poll_job_heartbeats_owned_job_even_without_published_child_pid() {
        let ctx = test_context();
        let run_dir = temp_dir("owned-job-heartbeat-no-pid");
        runtime::start_pending_job(&run_dir, "safe-next", Some("solver-a"), "safe-next-action")
            .expect("start pending runtime job");
        let before = runtime::load_job_state(&run_dir)
            .expect("load initial state")
            .updated_at_unix;
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
                label: "safe-next".to_string(),
                run_dir: run_dir.clone(),
                log_hint: Some("solver-a".to_string()),
                command_hint: "safe-next-action".to_string(),
                started_at: Instant::now() - Duration::from_secs(10),
                started_wallclock: SystemTime::now() - Duration::from_secs(10),
                pid: 0,
                pgid: 0,
                process_log: run_dir.join("runtime").join("process.log"),
                stream_rx: Some(stream_rx),
                completion_rx: Some(completion_rx),
                stream_lines: Vec::new(),
                attached: false,
                last_heartbeat: Instant::now() - Duration::from_secs(2),
            }),
            wizard_job: None,
        };

        poll_job(&ctx, &mut app).expect("poll job");

        let updated = runtime::load_job_state(&run_dir).expect("load updated state");
        assert!(
            updated.updated_at_unix >= before,
            "owned job heartbeat should refresh runtime state"
        );
        assert!(
            matches!(updated.status.as_str(), "running" | "stalled"),
            "owned job heartbeat should keep runtime state active, got {}",
            updated.status
        );
        assert!(app.job.is_some(), "owned job should remain tracked");

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
