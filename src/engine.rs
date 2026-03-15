use curl::easy::{Easy, List};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{json, Value};
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::ffi::CStr;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const ROLE_MAP_REF: &str = "references/agency-role-map.md";
const REVIEW_RUBRIC_REF: &str = "references/review-rubric.md";
const VERIFICATION_RUBRIC_REF: &str = "references/verification-rubric.md";
const DECOMPOSITION_RULES_REF: &str = "references/decomposition-rules.md";
const EMBEDDED_ROLE_MAP: &str = include_str!("../references/agency-role-map.md");
const EMBEDDED_REVIEW_RUBRIC: &str = include_str!("../references/review-rubric.md");
const EMBEDDED_VERIFICATION_RUBRIC: &str = include_str!("../references/verification-rubric.md");
const EMBEDDED_DECOMPOSITION_RULES: &str = include_str!("../references/decomposition-rules.md");
const CACHE_AREAS: [&str; 6] = [
    "research",
    "downloads",
    "wheelhouse",
    "models",
    "verification",
    "stage-results",
];
const STAGE_RESULTS_AREA: &str = "stage-results";
const REVIEWER_STACK: [&str; 3] = [
    "testing/testing-reality-checker.md",
    "testing/testing-test-results-analyzer.md",
    "support/support-executive-summary-generator.md",
];
const ANGLE_SEQUENCE: [&str; 3] = ["implementation-first", "architecture-first", "risk-first"];
const PLACEHOLDER_PREFIXES: [&str; 3] = ["pending ", "fill this file", "fill this"];
const RESPONSES_DOC_MAX_CHARS_PER_DOC: usize = 20_000;
const RESPONSES_DOC_TOTAL_CHARS: usize = 80_000;
const RESPONSES_POLL_MAX_ATTEMPTS: u32 = 300;
const RESPONSES_HTTP_MAX_ATTEMPTS: u32 = 4;
const RESPONSES_VERIFICATION_WORKSPACE_MAX_FILES: usize = 12;
const RESPONSES_VERIFICATION_WORKSPACE_MAX_FILE_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone)]
pub struct Context {
    pub repo_root: PathBuf,
    pub codex_bin: String,
    pub stage0_backend: String,
    pub stage_backend: String,
    pub openai_api_base: String,
    pub openai_api_key: Option<String>,
    pub openai_model: String,
    pub openai_prompt_cache_key_prefix: String,
    pub openai_prompt_cache_retention: Option<String>,
    pub openai_store: bool,
    pub openai_background: bool,
}

pub trait EngineObserver: Send + Sync {
    fn process_started(&self, _pid: i32, _pgid: i32) {}
    fn line(&self, _line: &str) {}
}

thread_local! {
    static ENGINE_OBSERVER: RefCell<Option<Arc<dyn EngineObserver>>> = RefCell::new(None);
}

pub fn with_engine_observer<T, F>(observer: Arc<dyn EngineObserver>, f: F) -> T
where
    F: FnOnce() -> T,
{
    ENGINE_OBSERVER.with(|slot| {
        let previous = slot.replace(Some(observer));
        let result = f();
        let _ = slot.replace(previous);
        result
    })
}

fn engine_observer_present() -> bool {
    ENGINE_OBSERVER.with(|slot| slot.borrow().is_some())
}

fn notify_process_started(pid: i32, pgid: i32) {
    ENGINE_OBSERVER.with(|slot| {
        if let Some(observer) = slot.borrow().as_ref() {
            observer.process_started(pid, pgid);
        }
    });
}

fn notify_output_line(line: &str) {
    ENGINE_OBSERVER.with(|slot| {
        if let Some(observer) = slot.borrow().as_ref() {
            observer.line(line);
        }
    });
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
            codex_bin: env::var("AGPIPE_CODEX_BIN").unwrap_or_else(|_| "codex".to_string()),
            stage0_backend: env::var("AGPIPE_STAGE0_BACKEND")
                .or_else(|_| env::var("AGPIPE_LLM_BACKEND"))
                .unwrap_or_else(|_| "codex".to_string()),
            stage_backend: env::var("AGPIPE_STAGE_BACKEND")
                .or_else(|_| env::var("AGPIPE_AGENT_BACKEND"))
                .unwrap_or_else(|_| "codex".to_string()),
            openai_api_base: env::var("AGPIPE_OPENAI_API_BASE")
                .unwrap_or_else(|_| "https://api.openai.com/v1".to_string()),
            openai_api_key: env::var("OPENAI_API_KEY")
                .ok()
                .or_else(|| env::var("AGPIPE_OPENAI_API_KEY").ok()),
            openai_model: env::var("AGPIPE_OPENAI_MODEL")
                .or_else(|_| env::var("OPENAI_MODEL"))
                .unwrap_or_else(|_| "gpt-5".to_string()),
            openai_prompt_cache_key_prefix: env::var("AGPIPE_OPENAI_PROMPT_CACHE_KEY_PREFIX")
                .unwrap_or_else(|_| "agpipe-stage0-v1".to_string()),
            openai_prompt_cache_retention: env::var("AGPIPE_OPENAI_PROMPT_CACHE_RETENTION").ok(),
            openai_store: env_flag("AGPIPE_OPENAI_STORE").unwrap_or(false),
            openai_background: env_flag("AGPIPE_OPENAI_BACKGROUND").unwrap_or(true),
        })
    }
}

fn env_flag(name: &str) -> Option<bool> {
    env::var(name).ok().map(|value| {
        let normalized = value.trim().to_ascii_lowercase();
        !matches!(normalized.as_str(), "0" | "false" | "no" | "off")
    })
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
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DoctorIssue {
    #[serde(default)]
    pub severity: String,
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub fix: String,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DoctorPayload {
    #[serde(default)]
    pub run_dir: String,
    #[serde(default)]
    pub health: String,
    #[serde(default)]
    pub stages: BTreeMap<String, String>,
    #[serde(default)]
    pub stale: Vec<String>,
    #[serde(default)]
    pub host_probe: String,
    #[serde(default)]
    pub host_drift: Option<String>,
    #[serde(default)]
    pub goal: String,
    #[serde(default)]
    pub next: String,
    #[serde(default)]
    pub safe_next_action: String,
    #[serde(default)]
    pub issues: Vec<DoctorIssue>,
    #[serde(default)]
    pub warnings: Vec<DoctorIssue>,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StatusPayload {
    #[serde(default)]
    pub run_dir: String,
    #[serde(default)]
    pub stages: BTreeMap<String, String>,
    #[serde(default)]
    pub host_probe: String,
    #[serde(default)]
    pub host_drift: Option<String>,
    #[serde(default)]
    pub goal: String,
    #[serde(default)]
    pub next: String,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct RunSnapshot {
    pub run_dir: PathBuf,
    pub doctor: DoctorPayload,
    pub status: StatusPayload,
    pub token_summary: RunTokenSummary,
    pub preview_label: String,
    pub preview: String,
    pub log_title: String,
    pub log_lines: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct RunTokenSummary {
    pub budget_total_tokens: u64,
    pub warning_threshold_tokens: u64,
    pub used_total_tokens: u64,
    pub prompt_tokens: u64,
    pub cached_prompt_tokens: u64,
    pub completion_tokens: u64,
    pub estimated_saved_tokens: u64,
    pub remaining_tokens: Option<u64>,
    pub source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct WorkstreamHint {
    #[serde(default)]
    name: String,
    #[serde(default)]
    goal: String,
    #[serde(default)]
    suggested_role: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum WorkstreamHintWire {
    Structured(WorkstreamHint),
    LegacyString(String),
}

impl WorkstreamHint {
    fn from_wire(value: WorkstreamHintWire) -> Self {
        match value {
            WorkstreamHintWire::Structured(hint) => hint,
            WorkstreamHintWire::LegacyString(value) => Self::from_legacy_string(value),
        }
    }

    fn from_legacy_string(value: String) -> Self {
        let name = value.trim().to_string();
        Self {
            goal: name.clone(),
            name,
            suggested_role: String::new(),
        }
    }
}

fn deserialize_workstream_hints<'de, D>(deserializer: D) -> Result<Vec<WorkstreamHint>, D::Error>
where
    D: Deserializer<'de>,
{
    let hints = Option::<Vec<WorkstreamHintWire>>::deserialize(deserializer)?.unwrap_or_default();
    Ok(hints.into_iter().map(WorkstreamHint::from_wire).collect())
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct GoalCheck {
    #[serde(default)]
    id: String,
    #[serde(default)]
    requirement: String,
    #[serde(default = "default_true")]
    critical: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct SolverRole {
    #[serde(default)]
    solver_id: String,
    #[serde(default)]
    role: String,
    #[serde(default)]
    angle: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct StackSignals {
    #[serde(default)]
    package_json: bool,
    #[serde(default)]
    pyproject_toml: bool,
    #[serde(default)]
    pytest_suite: bool,
    #[serde(default)]
    go_mod: bool,
    #[serde(default)]
    cargo_toml: bool,
    #[serde(default)]
    makefile: bool,
    #[serde(default)]
    terraform: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct CacheMeta {
    #[serde(default)]
    root: String,
    #[serde(default)]
    index: String,
    #[serde(default)]
    locks: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct CacheLockOwner {
    #[serde(default)]
    pid: i32,
    #[serde(default)]
    created_at_unix: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct CacheConfig {
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    root: String,
    #[serde(default)]
    policy: String,
    #[serde(default)]
    paths: BTreeMap<String, String>,
    #[serde(default)]
    meta: CacheMeta,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct TokenBudget {
    #[serde(default)]
    total_tokens: u64,
    #[serde(default)]
    warning_threshold_tokens: u64,
    #[serde(default)]
    source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct TokenUsage {
    #[serde(default)]
    source: String,
    #[serde(default)]
    prompt_tokens: u64,
    #[serde(default)]
    cached_prompt_tokens: u64,
    #[serde(default)]
    completion_tokens: u64,
    #[serde(default)]
    total_tokens: u64,
    #[serde(default)]
    estimated_saved_tokens: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct PromptCacheHashes {
    #[serde(default)]
    combined: String,
    #[serde(default)]
    stable_prefix: String,
    #[serde(default)]
    dynamic_suffix: String,
}

#[derive(Debug, Clone, Default)]
struct HttpJsonResponse {
    status: u32,
    body: String,
    headers: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct RunBackendConfig {
    #[serde(default)]
    stage0_backend: String,
    #[serde(default)]
    stage_backend: String,
    #[serde(default)]
    openai_model: String,
    #[serde(default)]
    openai_background: bool,
    #[serde(default)]
    openai_store: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct StageCacheManifest {
    #[serde(default)]
    version: u8,
    #[serde(default)]
    key: String,
    #[serde(default)]
    stage: String,
    #[serde(default)]
    created_at: String,
    #[serde(default)]
    command: Vec<String>,
    #[serde(default)]
    prompt_hash: String,
    #[serde(default)]
    stable_prefix_hash: String,
    #[serde(default)]
    dynamic_suffix_hash: String,
    #[serde(default)]
    workspace_hash: String,
    #[serde(default)]
    token_usage: TokenUsage,
    #[serde(default)]
    inputs: Vec<String>,
    #[serde(default)]
    outputs: Vec<String>,
    #[serde(default)]
    logs: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct RunTokenLedgerEntry {
    #[serde(default)]
    stage: String,
    #[serde(default)]
    mode: String,
    #[serde(default)]
    recorded_at: String,
    #[serde(default)]
    cache_key: String,
    #[serde(default)]
    prompt_hashes: PromptCacheHashes,
    #[serde(default)]
    workspace_hash: String,
    #[serde(default)]
    usage: TokenUsage,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct RunTokenLedger {
    #[serde(default)]
    version: u8,
    #[serde(default)]
    updated_at: String,
    #[serde(default)]
    budget: TokenBudget,
    #[serde(default)]
    entries: Vec<RunTokenLedgerEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct HostFacts {
    #[serde(default)]
    source: String,
    #[serde(default)]
    captured_at: String,
    #[serde(default)]
    platform: String,
    #[serde(default)]
    machine: String,
    #[serde(default)]
    apple_silicon: bool,
    #[serde(default)]
    torch_installed: bool,
    #[serde(default)]
    cuda_available: Option<bool>,
    #[serde(default)]
    mps_built: Option<bool>,
    #[serde(default)]
    mps_available: Option<bool>,
    #[serde(default)]
    preferred_torch_device: String,
    #[serde(default)]
    visible_env_keys: Vec<String>,
    #[serde(default)]
    artifact: Option<String>,
    #[serde(default)]
    history_artifact: Option<String>,
    #[serde(default)]
    torch_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct Plan {
    #[serde(default)]
    created_at: String,
    #[serde(default)]
    workspace: String,
    #[serde(default)]
    workspace_exists: bool,
    #[serde(default)]
    original_task: String,
    #[serde(default)]
    task_kind: String,
    #[serde(default)]
    complexity: String,
    #[serde(default)]
    execution_mode: String,
    #[serde(default)]
    prompt_format: String,
    #[serde(default)]
    summary_language: String,
    #[serde(default)]
    intake_research_mode: String,
    #[serde(default)]
    stage_research_mode: String,
    #[serde(default)]
    execution_network_mode: String,
    #[serde(default)]
    cache: CacheConfig,
    #[serde(default)]
    token_budget: TokenBudget,
    #[serde(default)]
    host_facts: HostFacts,
    #[serde(default)]
    solver_count: usize,
    #[serde(default)]
    solver_roles: Vec<SolverRole>,
    #[serde(default, deserialize_with = "deserialize_workstream_hints")]
    workstream_hints: Vec<WorkstreamHint>,
    #[serde(default)]
    goal_gate_enabled: bool,
    #[serde(default)]
    augmented_follow_up_enabled: bool,
    #[serde(default)]
    goal_checks: Vec<GoalCheck>,
    #[serde(default)]
    reviewer_stack: Vec<String>,
    #[serde(default)]
    stack_signals: StackSignals,
    #[serde(default)]
    validation_commands: Vec<String>,
    #[serde(default)]
    references: BTreeMap<String, String>,
    #[serde(default)]
    pipeline: PipelineConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct PipelineConfig {
    #[serde(default)]
    source: String,
    #[serde(default)]
    stages: Vec<PipelineStageSpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct PipelineStageSpec {
    #[serde(default)]
    id: String,
    #[serde(default)]
    kind: String,
    #[serde(default)]
    role: String,
    #[serde(default)]
    role_source: String,
    #[serde(default)]
    angle: String,
    #[serde(default)]
    depends_on: Vec<String>,
    #[serde(default)]
    description: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PipelineStageKind {
    Intake,
    Solver,
    Review,
    Execution,
    Verification,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct InterviewQuestion {
    #[serde(default)]
    id: String,
    #[serde(default)]
    question: String,
    #[serde(default)]
    why: String,
    #[serde(default = "default_true")]
    required: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct InterviewQuestionsPayload {
    #[serde(default)]
    session_dir: String,
    #[serde(default)]
    raw_task_path: String,
    #[serde(default)]
    goal_summary: String,
    #[serde(default)]
    questions: Vec<InterviewQuestion>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct InterviewFinalizePayload {
    #[serde(default)]
    session_dir: String,
    #[serde(default)]
    final_task_path: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StageBackendKind {
    Codex,
    Responses,
    LocalTemplate(LocalTemplateKind),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LocalTemplateKind {
    HelloWorldPython,
}

#[derive(Debug, Clone)]
struct ResponseTextFormat {
    name: String,
    schema: Value,
}

#[derive(Debug, Deserialize, Default)]
struct ResponsesIntakePayload {
    #[serde(default)]
    brief_md: String,
    #[serde(default)]
    plan_json: Value,
}

#[derive(Debug, Deserialize, Default)]
struct ResponsesSolverPayload {
    #[serde(default)]
    result_md: String,
}

#[derive(Debug, Deserialize, Default)]
struct ResponsesReviewPayload {
    #[serde(default)]
    report_md: String,
    #[serde(default)]
    scorecard_json: Value,
    #[serde(default)]
    user_summary_md: String,
}

#[derive(Debug, Deserialize, Default)]
struct ResponsesVerificationPayload {
    #[serde(default)]
    findings_md: String,
    #[serde(default)]
    user_summary_md: String,
    #[serde(default)]
    improvement_request_md: String,
    #[serde(default)]
    augmented_task_md: String,
    #[serde(default)]
    goal_status_json: Value,
}

#[derive(Debug, Clone, Default)]
struct StartArgs {
    stage: Option<String>,
    force: bool,
    dry_run: bool,
    color: Option<String>,
    model: Option<String>,
    profile: Option<String>,
    oss: bool,
}

#[derive(Debug, Clone, Default)]
struct ShowArgs {
    stage: String,
    raw: bool,
}

#[derive(Debug, Clone, Default)]
struct StageOnlyArgs {
    stage: String,
    dry_run: bool,
}

#[derive(Debug, Clone, Default)]
struct CacheStatusArgs {
    refresh: bool,
    limit: usize,
}

#[derive(Debug, Clone, Default)]
struct CachePruneArgs {
    max_age_days: Option<i64>,
    area: Vec<String>,
    dry_run: bool,
}

#[derive(Debug, Clone, Default)]
struct HostProbeArgs {
    refresh: bool,
    history: bool,
}

#[derive(Debug, Clone, Default)]
struct RerunArgs {
    dry_run: bool,
    title: Option<String>,
    output_dir: Option<PathBuf>,
    prompt_source: Option<String>,
}

#[derive(Debug, Clone, Default)]
struct InterviewArgs {
    task: Option<String>,
    task_file: Option<PathBuf>,
    workspace: PathBuf,
    output_dir: PathBuf,
    title: Option<String>,
    language: String,
    max_questions: usize,
}

#[derive(Debug, Clone, Default)]
struct InterviewFinalizeArgs {
    task: Option<String>,
    task_file: Option<PathBuf>,
    workspace: PathBuf,
    session_dir: PathBuf,
    answers_file: PathBuf,
    language: String,
}

#[derive(Debug, Clone, Default)]
struct CreateRunArgs {
    task: Option<String>,
    task_file: Option<PathBuf>,
    workspace: PathBuf,
    output_dir: PathBuf,
    title: Option<String>,
    prompt_format: String,
    summary_language: String,
    intake_research: String,
    stage_research: String,
    execution_network: String,
    cache_root: String,
    cache_policy: String,
    interview_session: Option<PathBuf>,
    pipeline_file: Option<PathBuf>,
}

#[derive(Debug, Clone, Default)]
struct RunArgs {
    task: Option<String>,
    task_file: Option<PathBuf>,
    workspace: PathBuf,
    output_dir: PathBuf,
    title: Option<String>,
    prompt_format: String,
    summary_language: String,
    intake_research: String,
    stage_research: String,
    execution_network: String,
    cache_root: String,
    cache_policy: String,
    until: String,
    auto_approve: bool,
    skip_interview: bool,
    max_questions: usize,
    pipeline_file: Option<PathBuf>,
}

fn default_true() -> bool {
    true
}

fn discover_repo_root() -> Result<PathBuf, String> {
    let exe =
        env::current_exe().map_err(|err| format!("Could not resolve current executable: {err}"))?;
    for ancestor in exe.ancestors() {
        if looks_like_repo_root(ancestor) {
            return Ok(ancestor.to_path_buf());
        }
    }
    let cwd =
        env::current_dir().map_err(|err| format!("Could not read current directory: {err}"))?;
    for ancestor in cwd.ancestors() {
        if looks_like_repo_root(ancestor) {
            return Ok(ancestor.to_path_buf());
        }
    }
    materialize_embedded_repo_root()
}

fn looks_like_repo_root(path: &Path) -> bool {
    path.join("Cargo.toml").exists()
        && path.join("src").join("main.rs").exists()
        && path.join("references").join("review-rubric.md").exists()
}

fn embedded_repo_root() -> PathBuf {
    if let Ok(path) = env::var("AGPIPE_ASSET_ROOT") {
        return PathBuf::from(path);
    }
    if let Ok(path) = env::var("XDG_CACHE_HOME") {
        return PathBuf::from(path).join("agpipe").join("embedded-repo");
    }
    if let Ok(home) = env::var("HOME") {
        return PathBuf::from(home)
            .join(".cache")
            .join("agpipe")
            .join("embedded-repo");
    }
    env::temp_dir().join("agpipe-embedded-repo")
}

fn materialize_embedded_repo_root() -> Result<PathBuf, String> {
    let root = embedded_repo_root();
    fs::create_dir_all(root.join("references"))
        .map_err(|err| format!("Could not create embedded references dir: {err}"))?;
    fs::create_dir_all(root.join("src"))
        .map_err(|err| format!("Could not create embedded src dir: {err}"))?;
    write_text(
        &root.join("Cargo.toml"),
        "[package]\nname = \"agpipe-embedded-assets\"\nversion = \"0.0.0\"\nedition = \"2021\"",
    )?;
    write_text(&root.join("src").join("main.rs"), "fn main() {}")?;
    write_text(&root.join(ROLE_MAP_REF), EMBEDDED_ROLE_MAP)?;
    write_text(&root.join(REVIEW_RUBRIC_REF), EMBEDDED_REVIEW_RUBRIC)?;
    write_text(
        &root.join(VERIFICATION_RUBRIC_REF),
        EMBEDDED_VERIFICATION_RUBRIC,
    )?;
    write_text(
        &root.join(DECOMPOSITION_RULES_REF),
        EMBEDDED_DECOMPOSITION_RULES,
    )?;
    Ok(root)
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

fn read_text(path: &Path) -> Result<String, String> {
    fs::read_to_string(path).map_err(|err| format!("Could not read {}: {err}", path.display()))
}

fn write_text(path: &Path, content: &str) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|err| format!("Could not create {}: {err}", parent.display()))?;
    }
    fs::write(path, format!("{}\n", content.trim_end()))
        .map_err(|err| format!("Could not write {}: {err}", path.display()))
}

fn write_json<T: Serialize>(path: &Path, payload: &T) -> Result<(), String> {
    let content = serde_json::to_string_pretty(payload)
        .map_err(|err| format!("Could not serialize {}: {err}", path.display()))?;
    write_text(path, &content)
}

fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T, String> {
    let text = read_text(path)?;
    serde_json::from_str(&text).map_err(|err| format!("Could not parse {}: {err}", path.display()))
}

fn platform_name() -> String {
    match env::consts::OS {
        "macos" => "darwin".to_string(),
        other => other.to_string(),
    }
}

fn machine_name() -> String {
    unsafe {
        let mut uts: libc::utsname = std::mem::zeroed();
        if libc::uname(&mut uts) == 0 {
            CStr::from_ptr(uts.machine.as_ptr())
                .to_string_lossy()
                .into_owned()
        } else {
            env::consts::ARCH.to_string()
        }
    }
}

fn format_time(fmt: &[u8]) -> String {
    unsafe {
        let mut raw: libc::time_t = 0;
        libc::time(&mut raw);
        let mut tm: libc::tm = std::mem::zeroed();
        if libc::localtime_r(&raw, &mut tm).is_null() {
            return SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
                .to_string();
        }
        let mut buffer = [0i8; 64];
        let len = libc::strftime(
            buffer.as_mut_ptr(),
            buffer.len(),
            fmt.as_ptr() as *const i8,
            &tm,
        );
        let bytes = std::slice::from_raw_parts(buffer.as_ptr() as *const u8, len);
        String::from_utf8_lossy(bytes).into_owned()
    }
}

fn iso_timestamp() -> String {
    format_time(b"%Y-%m-%dT%H:%M:%S\0")
}

fn current_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn run_timestamp() -> String {
    format_time(b"%Y%m%d-%H%M%S\0")
}

fn timestamp_slug(label: &str) -> String {
    format!("{}-{}", run_timestamp(), slugify(label))
}

fn slugify(value: &str) -> String {
    let mut out = String::new();
    let mut last_dash = false;
    for ch in value.to_lowercase().chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch);
            last_dash = false;
        } else if !last_dash {
            out.push('-');
            last_dash = true;
        }
    }
    let trimmed = out.trim_matches('-').to_string();
    let shortened: String = trimmed.chars().take(48).collect();
    if shortened.is_empty() {
        "task".to_string()
    } else {
        shortened
    }
}

fn file_mtime(path: &Path) -> SystemTime {
    path.metadata()
        .and_then(|meta| meta.modified())
        .unwrap_or(UNIX_EPOCH)
}

fn read_utf8_text_if_reasonable(path: &Path, max_bytes: usize) -> Option<String> {
    let bytes = fs::read(path).ok()?;
    if bytes.is_empty() || bytes.len() > max_bytes || bytes.contains(&0) {
        return None;
    }
    String::from_utf8(bytes).ok()
}

fn backend_config_path(run_dir: &Path) -> PathBuf {
    run_dir.join("runtime").join("backend-config.json")
}

fn context_backend_config(ctx: &Context) -> RunBackendConfig {
    RunBackendConfig {
        stage0_backend: ctx.stage0_backend.clone(),
        stage_backend: ctx.stage_backend.clone(),
        openai_model: ctx.openai_model.clone(),
        openai_background: ctx.openai_background,
        openai_store: ctx.openai_store,
    }
}

fn persist_run_backend_config(run_dir: &Path, ctx: &Context) -> Result<(), String> {
    write_json(&backend_config_path(run_dir), &context_backend_config(ctx))
}

fn load_run_backend_config(run_dir: &Path) -> Option<RunBackendConfig> {
    let path = backend_config_path(run_dir);
    if !path.exists() {
        return None;
    }
    read_json(&path).ok()
}

fn compact_lines<T: Serialize>(payload: &T) -> String {
    serde_json::to_string_pretty(payload).unwrap_or_else(|_| "{}".to_string()) + "\n"
}

fn bullet_list<I>(values: I) -> String
where
    I: IntoIterator,
    I::Item: AsRef<str>,
{
    let items: Vec<String> = values
        .into_iter()
        .map(|item| item.as_ref().to_string())
        .collect();
    if items.is_empty() {
        "- none".to_string()
    } else {
        items
            .into_iter()
            .map(|item| format!("- {item}"))
            .collect::<Vec<_>>()
            .join("\n")
    }
}

fn walk_tree(
    root: &Path,
    max_extra_depth: usize,
    skip_names: &[&str],
) -> Result<Vec<PathBuf>, String> {
    if !root.exists() {
        return Ok(Vec::new());
    }
    let base_depth = root.components().count();
    let skip: BTreeSet<&str> = skip_names.iter().copied().collect();
    let mut stack = vec![root.to_path_buf()];
    let mut out = Vec::new();
    while let Some(path) = stack.pop() {
        let depth = path.components().count().saturating_sub(base_depth);
        if depth > max_extra_depth {
            continue;
        }
        let entries = match fs::read_dir(&path) {
            Ok(entries) => entries,
            Err(_) => continue,
        };
        for entry in entries.flatten() {
            let child = entry.path();
            let name = child
                .file_name()
                .and_then(|value| value.to_str())
                .unwrap_or_default();
            if child.is_dir() {
                if skip.contains(name) {
                    continue;
                }
                stack.push(child);
            } else {
                out.push(child);
            }
        }
    }
    Ok(out)
}

fn role_matrix(task_kind: &str) -> Vec<&'static str> {
    match task_kind {
        "ai" => vec![
            "engineering/engineering-ai-engineer.md",
            "engineering/engineering-backend-architect.md",
            "engineering/engineering-rapid-prototyper.md",
        ],
        "frontend" => vec![
            "engineering/engineering-frontend-developer.md",
            "design/design-ui-designer.md",
            "design/design-ux-architect.md",
        ],
        "backend" => vec![
            "engineering/engineering-backend-architect.md",
            "engineering/engineering-senior-developer.md",
            "engineering/engineering-devops-automator.md",
        ],
        "fullstack" => vec![
            "engineering/engineering-senior-developer.md",
            "engineering/engineering-frontend-developer.md",
            "engineering/engineering-backend-architect.md",
        ],
        "infra" => vec![
            "engineering/engineering-devops-automator.md",
            "support/support-infrastructure-maintainer.md",
            "engineering/engineering-security-engineer.md",
        ],
        "security" => vec![
            "engineering/engineering-security-engineer.md",
            "testing/testing-tool-evaluator.md",
            "support/support-legal-compliance-checker.md",
        ],
        "docs" => vec![
            "engineering/engineering-technical-writer.md",
            "support/support-executive-summary-generator.md",
            "project-management/project-management-studio-operations.md",
        ],
        "research" => vec![
            "product/product-trend-researcher.md",
            "testing/testing-tool-evaluator.md",
            "support/support-analytics-reporter.md",
        ],
        "skill" => vec![
            "skill-creator",
            "engineering/engineering-technical-writer.md",
            "project-manager-senior",
        ],
        _ => vec![
            "engineering/engineering-senior-developer.md",
            "engineering/engineering-backend-architect.md",
            "engineering/engineering-devops-automator.md",
        ],
    }
}

fn infer_task_kind(task: &str) -> String {
    let text = task.to_lowercase();
    let count = |words: &[&str]| words.iter().filter(|word| text.contains(**word)).count();
    let ai_hits = count(&[
        "ai",
        "ml",
        "llm",
        "llama",
        "lama",
        "fine-tune",
        "finetune",
        "train model",
        "rag",
        "embedding",
        "inference",
        "telegram",
        "freecad",
        "нейросет",
        "модель",
        "дообуч",
        "обуч",
        "телеграм",
    ]);
    let frontend_hits = count(&[
        "frontend",
        "ui",
        "ux",
        "css",
        "html",
        "react",
        "vue",
        "page",
        "component",
        "фронтенд",
        "интерфейс",
        "страница",
        "компонент",
        "верстк",
    ]);
    let backend_hits = count(&[
        "backend",
        "api",
        "database",
        "service",
        "queue",
        "worker",
        "python",
        "питон",
        "script",
        "скрипт",
        "cli",
        "command line",
        "console",
        "fastapi",
        "flask",
        "django",
        "sql",
        "бэкенд",
        "бекенд",
        "сервис",
        "база данных",
        "очеред",
    ]);

    if count(&["skill", "prompt", "codex", "скил", "промт", "кодекс"]) >= 2 {
        return "skill".to_string();
    }
    if ai_hits >= 2 {
        return "ai".to_string();
    }
    if count(&[
        "security",
        "vulnerability",
        "auth",
        "secret",
        "token",
        "compliance",
        "audit",
        "безопас",
        "уязвим",
        "аудит",
        "секрет",
        "токен",
        "авторизац",
    ]) >= 2
    {
        return "security".to_string();
    }
    if count(&[
        "deploy",
        "docker",
        "kubernetes",
        "terraform",
        "ansible",
        "ci",
        "cd",
        "infra",
        "деплой",
        "инфра",
        "инфраструктур",
        "контейнер",
        "k8s",
    ]) >= 2
    {
        return "infra".to_string();
    }
    if count(&[
        "docs",
        "documentation",
        "readme",
        "guide",
        "summary",
        "spec",
        "документац",
        "гайд",
        "резюме",
        "спек",
        "описан",
    ]) >= 2
    {
        return "docs".to_string();
    }
    if count(&[
        "compare",
        "evaluate",
        "research",
        "recommend",
        "choose",
        "options",
        "сравн",
        "оцен",
        "исслед",
        "рекомен",
        "выбор",
        "вариант",
    ]) >= 2
    {
        return "research".to_string();
    }
    if frontend_hits > 0 && backend_hits > 0 {
        return "fullstack".to_string();
    }
    if frontend_hits > 0 {
        return "frontend".to_string();
    }
    if backend_hits > 0 {
        return "backend".to_string();
    }
    "fullstack".to_string()
}

fn infer_complexity(task: &str) -> String {
    let text = task.to_lowercase();
    let mut score = std::cmp::min(text.split_whitespace().count() / 30, 3);
    for keyword in [
        "architecture",
        "pipeline",
        "workflow",
        "refactor",
        "migrate",
        "production",
        "several",
        "multiple",
        "compare",
        "orchestr",
        "архитект",
        "конвейер",
        "пайплайн",
        "несколько",
        "сравн",
        "оркестр",
        "агент",
        "промт",
        "тест",
        "резюме",
        "вариант",
        "review",
        "цензор",
    ] {
        if text.contains(keyword) {
            score += 1;
        }
    }
    if (text.contains("agent") || text.contains("агент"))
        && (text.contains("multiple")
            || text.contains("several")
            || text.contains("несколько")
            || text.contains("вариант"))
    {
        score += 2;
    }
    if (text.contains("pipeline")
        || text.contains("workflow")
        || text.contains("конвейер")
        || text.contains("пайплайн"))
        && (text.contains("test")
            || text.contains("review")
            || text.contains("тест")
            || text.contains("цензор"))
    {
        score += 2;
    }
    if score >= 6 {
        "high".to_string()
    } else if score >= 3 {
        "medium".to_string()
    } else {
        "low".to_string()
    }
}

fn solver_count_for(complexity: &str) -> usize {
    match complexity {
        "low" => 1,
        "medium" => 2,
        _ => 3,
    }
}

fn infer_token_budget(complexity: &str) -> TokenBudget {
    let env_budget = env::var("AGPIPE_TOKEN_BUDGET")
        .ok()
        .or_else(|| env::var("OPENAI_TOKEN_BUDGET").ok())
        .and_then(|value| value.trim().parse::<u64>().ok());
    let total_tokens = env_budget.unwrap_or(match complexity {
        "low" => 50_000,
        "medium" => 150_000,
        _ => 300_000,
    });
    let warning_threshold_tokens = std::cmp::max(total_tokens / 10, 1_000);
    let source = if env_budget.is_some() {
        "env".to_string()
    } else {
        "planned-default".to_string()
    };
    TokenBudget {
        total_tokens,
        warning_threshold_tokens,
        source,
    }
}

fn infer_execution_mode(task_kind: &str, complexity: &str, task: &str) -> String {
    let text = task.to_lowercase();
    let compound = [
        "telegram",
        "freecad",
        "api",
        "bot",
        "service",
        "worker",
        "pipeline",
        "workflow",
        "телеграм",
        "сервис",
        "бот",
        "пайплайн",
        "конвейер",
    ];
    if matches!(task_kind, "ai" | "fullstack" | "backend") && complexity != "low" {
        return "decomposed".to_string();
    }
    if compound
        .iter()
        .filter(|signal| text.contains(**signal))
        .count()
        >= 3
    {
        return "decomposed".to_string();
    }
    "alternatives".to_string()
}

fn workstream_hints_for(task_kind: &str, task: &str) -> Vec<WorkstreamHint> {
    let text = task.to_lowercase();
    if task_kind == "ai"
        && (text.contains("telegram") || text.contains("телеграм"))
        && text.contains("freecad")
    {
        return vec![
            WorkstreamHint {
                name: "telegram-ingress".to_string(),
                goal: "accept photo, dimensions, and follow-up answers from Telegram".to_string(),
                suggested_role: "engineering/engineering-backend-architect.md".to_string(),
            },
            WorkstreamHint {
                name: "vision-or-analysis".to_string(),
                goal: "turn image input into grounded geometry observations".to_string(),
                suggested_role: "engineering/engineering-ai-engineer.md".to_string(),
            },
            WorkstreamHint {
                name: "cad-planning".to_string(),
                goal: "convert observations plus dimensions into a constrained CAD plan"
                    .to_string(),
                suggested_role: "engineering/engineering-ai-engineer.md".to_string(),
            },
            WorkstreamHint {
                name: "freecad-rendering".to_string(),
                goal: "translate the constrained plan into deterministic FreeCAD automation"
                    .to_string(),
                suggested_role: "engineering/engineering-rapid-prototyper.md".to_string(),
            },
            WorkstreamHint {
                name: "safety-and-evaluation".to_string(),
                goal:
                    "validate supported shapes, unsafe plans, and whether fine-tuning is justified"
                        .to_string(),
                suggested_role: "testing/testing-reality-checker.md".to_string(),
            },
        ];
    }
    match task_kind {
        "frontend" => vec![
            WorkstreamHint {
                name: "ui-implementation".to_string(),
                goal: "build the requested frontend surface".to_string(),
                suggested_role: "engineering/engineering-frontend-developer.md".to_string(),
            },
            WorkstreamHint {
                name: "ux-and-validation".to_string(),
                goal: "validate usability, accessibility, and interface constraints".to_string(),
                suggested_role: "design/design-ux-architect.md".to_string(),
            },
        ],
        "backend" => vec![
            WorkstreamHint {
                name: "service-layer".to_string(),
                goal: "build the core service or API behavior".to_string(),
                suggested_role: "engineering/engineering-backend-architect.md".to_string(),
            },
            WorkstreamHint {
                name: "persistence-and-ops".to_string(),
                goal: "define storage, jobs, and operational boundaries".to_string(),
                suggested_role: "engineering/engineering-devops-automator.md".to_string(),
            },
        ],
        "fullstack" => vec![
            WorkstreamHint {
                name: "entry-surface".to_string(),
                goal: "build the user-facing or API-facing entrypoint".to_string(),
                suggested_role: "engineering/engineering-frontend-developer.md".to_string(),
            },
            WorkstreamHint {
                name: "core-service".to_string(),
                goal: "build the core domain behavior and data flow".to_string(),
                suggested_role: "engineering/engineering-backend-architect.md".to_string(),
            },
            WorkstreamHint {
                name: "safety-and-review".to_string(),
                goal: "validate correctness, evidence, and operational risk".to_string(),
                suggested_role: "testing/testing-reality-checker.md".to_string(),
            },
        ],
        _ => Vec::new(),
    }
}

fn detect_stack(workspace: &Path) -> StackSignals {
    let mut signals = StackSignals::default();
    if !workspace.exists() {
        return signals;
    }
    let files = walk_tree(
        workspace,
        3,
        &[".git", "node_modules", ".venv", "venv", "__pycache__"],
    )
    .unwrap_or_default();
    for path in files {
        let name = path
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or_default();
        match name {
            "package.json" => signals.package_json = true,
            "pyproject.toml" => signals.pyproject_toml = true,
            "pytest.ini" | "conftest.py" => signals.pytest_suite = true,
            "go.mod" => signals.go_mod = true,
            "Cargo.toml" => signals.cargo_toml = true,
            "Makefile" => signals.makefile = true,
            _ => {
                if path.extension().and_then(|ext| ext.to_str()) == Some("tf") {
                    signals.terraform = true;
                }
                if name == "tests" {
                    signals.pytest_suite = true;
                }
            }
        }
    }
    signals
}

fn extract_package_scripts(workspace: &Path) -> Vec<String> {
    let mut scripts = Vec::new();
    if !workspace.exists() {
        return scripts;
    }
    let files = walk_tree(workspace, 3, &[".git", "node_modules"]).unwrap_or_default();
    for package_json in files
        .into_iter()
        .filter(|path| path.file_name().and_then(|value| value.to_str()) == Some("package.json"))
    {
        let Ok(text) = read_text(&package_json) else {
            continue;
        };
        let Ok(payload) = serde_json::from_str::<Value>(&text) else {
            continue;
        };
        let Some(obj) = payload.get("scripts").and_then(|value| value.as_object()) else {
            continue;
        };
        for name in ["test", "lint", "build"] {
            if obj.contains_key(name) {
                let cmd = if name == "test" {
                    "npm test".to_string()
                } else {
                    format!("npm run {name}")
                };
                if !scripts.contains(&cmd) {
                    scripts.push(cmd);
                }
            }
        }
        if !scripts.is_empty() {
            break;
        }
    }
    scripts
}

fn makefile_has_target(workspace: &Path, target: &str) -> bool {
    if !workspace.exists() {
        return false;
    }
    let files = walk_tree(workspace, 3, &[".git", "node_modules"]).unwrap_or_default();
    for makefile in files
        .into_iter()
        .filter(|path| path.file_name().and_then(|value| value.to_str()) == Some("Makefile"))
    {
        let Ok(text) = read_text(&makefile) else {
            continue;
        };
        if text
            .lines()
            .any(|line| line.trim_start().starts_with(&format!("{target}:")))
        {
            return true;
        }
    }
    false
}

fn build_validation_commands(workspace: &Path, signals: &StackSignals) -> Vec<String> {
    let mut commands = Vec::new();
    if signals.package_json {
        commands.extend(extract_package_scripts(workspace));
    }
    if signals.pyproject_toml || signals.pytest_suite {
        commands.push("pytest".to_string());
    }
    if signals.go_mod {
        commands.push("go test ./...".to_string());
    }
    if signals.cargo_toml {
        commands.push("cargo test".to_string());
    }
    if signals.makefile && makefile_has_target(workspace, "test") {
        commands.push("make test".to_string());
    }
    if signals.terraform {
        commands.push("terraform validate".to_string());
    }
    let mut deduped = Vec::new();
    for command in commands {
        if !deduped.contains(&command) {
            deduped.push(command);
        }
    }
    deduped
}

fn common_python_roots() -> Vec<PathBuf> {
    let mut roots = Vec::new();
    if let Ok(venv) = env::var("VIRTUAL_ENV") {
        roots.push(PathBuf::from(venv));
    }
    if let Ok(conda) = env::var("CONDA_PREFIX") {
        roots.push(PathBuf::from(conda));
    }
    if let Ok(home) = env::var("HOME") {
        let pyenv_root = PathBuf::from(home).join(".pyenv").join("versions");
        if let Ok(entries) = fs::read_dir(pyenv_root) {
            for entry in entries.flatten() {
                roots.push(entry.path());
            }
        }
    }
    roots
}

fn torch_installation_detected() -> bool {
    for root in common_python_roots() {
        let Ok(entries) = walk_tree(&root, 5, &["bin", "include", ".git"]) else {
            continue;
        };
        if entries.iter().any(|path| {
            let path_text = path.to_string_lossy();
            path_text.contains("site-packages/torch") || path_text.contains("dist-packages/torch")
        }) {
            return true;
        }
    }
    false
}

fn parse_env_bool(name: &str) -> Option<bool> {
    env::var(name)
        .ok()
        .and_then(|value| match value.to_lowercase().as_str() {
            "1" | "true" | "yes" | "y" => Some(true),
            "0" | "false" | "no" | "n" => Some(false),
            _ => None,
        })
}

fn detect_host_facts(source: &str) -> HostFacts {
    let platform = platform_name();
    let machine = machine_name();
    let apple_silicon = platform == "darwin" && matches!(machine.as_str(), "arm64" | "aarch64");
    let mut facts = HostFacts {
        source: source.to_string(),
        captured_at: iso_timestamp(),
        platform,
        machine,
        apple_silicon,
        visible_env_keys: visible_env_keys(),
        ..HostFacts::default()
    };
    if facts.preferred_torch_device.is_empty() {
        facts.preferred_torch_device = "cpu".to_string();
    }

    if !facts.torch_installed {
        facts.torch_installed = torch_installation_detected();
    }
    if facts.cuda_available.is_none() {
        facts.cuda_available = Some(
            env::var("CUDA_VISIBLE_DEVICES")
                .map(|value| !value.is_empty())
                .unwrap_or(false),
        );
    }
    if facts.mps_built.is_none() {
        facts.mps_built = Some(apple_silicon);
    }
    if facts.mps_available.is_none() {
        facts.mps_available = Some(apple_silicon);
    }
    if facts.torch_installed && facts.preferred_torch_device == "cpu" {
        if facts.cuda_available == Some(true) {
            facts.preferred_torch_device = "cuda".to_string();
        } else if facts.mps_available == Some(true) {
            facts.preferred_torch_device = "mps".to_string();
        }
    }

    if let Some(value) = parse_env_bool("AGPIPE_TORCH_INSTALLED") {
        facts.torch_installed = value;
    }
    if let Some(value) = parse_env_bool("AGPIPE_CUDA_AVAILABLE") {
        facts.cuda_available = Some(value);
    }
    if let Some(value) = parse_env_bool("AGPIPE_MPS_BUILT") {
        facts.mps_built = Some(value);
    }
    if let Some(value) = parse_env_bool("AGPIPE_MPS_AVAILABLE") {
        facts.mps_available = Some(value);
    }
    if let Ok(value) = env::var("AGPIPE_PREFERRED_TORCH_DEVICE") {
        facts.preferred_torch_device = value;
    }
    facts
}

fn choose_roles(task_kind: &str, solver_count: usize) -> Vec<SolverRole> {
    role_matrix(task_kind)
        .into_iter()
        .take(solver_count)
        .enumerate()
        .map(|(index, role)| SolverRole {
            solver_id: format!("solver-{}", (b'a' + index as u8) as char),
            role: role.to_string(),
            angle: ANGLE_SEQUENCE
                .get(index)
                .copied()
                .unwrap_or("implementation-first")
                .to_string(),
        })
        .collect()
}

fn build_cache_config(cache_root: &Path, cache_policy: &str) -> CacheConfig {
    let root = cache_root.expanduser().resolve();
    let root = root.unwrap_or_else(|_| cache_root.expanduser().to_path_buf());
    let meta_root = root.join(".meta");
    let mut paths = BTreeMap::new();
    for area in CACHE_AREAS {
        paths.insert(area.to_string(), root.join(area).display().to_string());
    }
    CacheConfig {
        enabled: cache_policy != "off",
        root: root.display().to_string(),
        policy: cache_policy.to_string(),
        paths,
        meta: CacheMeta {
            root: meta_root.display().to_string(),
            index: meta_root.join("index.json").display().to_string(),
            locks: meta_root.join("locks").display().to_string(),
        },
    }
}

trait ExpandPath {
    fn expanduser(&self) -> PathBuf;
    fn resolve(&self) -> Result<PathBuf, String>;
}

impl ExpandPath for Path {
    fn expanduser(&self) -> PathBuf {
        let text = self.to_string_lossy();
        if let Some(rest) = text.strip_prefix("~/") {
            if let Ok(home) = env::var("HOME") {
                return PathBuf::from(home).join(rest);
            }
        }
        self.to_path_buf()
    }

    fn resolve(&self) -> Result<PathBuf, String> {
        let expanded = self.expanduser();
        if expanded.is_absolute() {
            Ok(expanded)
        } else {
            env::current_dir()
                .map(|cwd| cwd.join(expanded))
                .map_err(|err| format!("Could not resolve path {}: {err}", self.display()))
        }
    }
}

fn fallback_cache_config() -> CacheConfig {
    build_cache_config(&PathBuf::from("~/.cache/multi-agent-pipeline"), "off")
}

fn merge_cache_config(cache: &CacheConfig) -> CacheConfig {
    let mut merged = fallback_cache_config();
    if !cache.root.is_empty() {
        merged.root.clone_from(&cache.root);
    }
    if !cache.policy.is_empty() {
        merged.policy.clone_from(&cache.policy);
    }
    merged.enabled = cache.enabled;
    for (key, value) in &cache.paths {
        merged.paths.insert(key.clone(), value.clone());
    }
    if !cache.meta.root.is_empty() {
        merged.meta.root.clone_from(&cache.meta.root);
    }
    if !cache.meta.index.is_empty() {
        merged.meta.index.clone_from(&cache.meta.index);
    }
    if !cache.meta.locks.is_empty() {
        merged.meta.locks.clone_from(&cache.meta.locks);
    }
    merged
}

fn ensure_cache_layout(cache: &CacheConfig) -> Result<(), String> {
    if !cache.enabled {
        return Ok(());
    }
    fs::create_dir_all(&cache.root)
        .map_err(|err| format!("Could not create {}: {err}", cache.root))?;
    for path in cache.paths.values() {
        fs::create_dir_all(path).map_err(|err| format!("Could not create {path}: {err}"))?;
    }
    fs::create_dir_all(&cache.meta.root)
        .map_err(|err| format!("Could not create {}: {err}", cache.meta.root))?;
    fs::create_dir_all(&cache.meta.locks)
        .map_err(|err| format!("Could not create {}: {err}", cache.meta.locks))?;
    Ok(())
}

fn cache_lock_owner_path(lock_dir: &Path) -> PathBuf {
    lock_dir.join("owner.json")
}

fn write_cache_lock_owner(lock_dir: &Path) -> Result<(), String> {
    write_json(
        &cache_lock_owner_path(lock_dir),
        &CacheLockOwner {
            pid: std::process::id() as i32,
            created_at_unix: current_unix_secs(),
        },
    )
}

fn lock_dir_age(lock_dir: &Path) -> Option<Duration> {
    let modified = fs::metadata(lock_dir).ok()?.modified().ok()?;
    SystemTime::now().duration_since(modified).ok()
}

fn stale_cache_lock(lock_dir: &Path) -> bool {
    if let Ok(owner) = read_json::<CacheLockOwner>(&cache_lock_owner_path(lock_dir)) {
        return owner.pid <= 0 || !crate::runtime::pid_alive(owner.pid);
    }
    lock_dir_age(lock_dir)
        .map(|age| age >= Duration::from_secs(2))
        .unwrap_or(false)
}

fn remove_stale_cache_lock(lock_dir: &Path) -> Result<bool, String> {
    if !lock_dir.exists() || !stale_cache_lock(lock_dir) {
        return Ok(false);
    }
    fs::remove_dir_all(lock_dir).map_err(|err| {
        format!(
            "Could not remove stale cache lock {}: {err}",
            lock_dir.display()
        )
    })?;
    Ok(true)
}

fn cache_lock<F, T>(cache: &CacheConfig, name: &str, mut f: F) -> Result<T, String>
where
    F: FnMut() -> Result<T, String>,
{
    if !cache.enabled {
        return f();
    }
    ensure_cache_layout(cache)?;
    let lock_dir = PathBuf::from(&cache.meta.locks).join(format!("{name}.lock"));
    let deadline = SystemTime::now() + Duration::from_secs(30);
    loop {
        match fs::create_dir(&lock_dir) {
            Ok(_) => {
                if let Err(err) = write_cache_lock_owner(&lock_dir) {
                    let _ = fs::remove_dir_all(&lock_dir);
                    return Err(err);
                }
                break;
            }
            Err(_) => {
                if remove_stale_cache_lock(&lock_dir)? {
                    continue;
                }
                if SystemTime::now() >= deadline {
                    return Err(format!(
                        "Timed out waiting for cache lock: {}",
                        lock_dir.display()
                    ));
                }
                thread::sleep(Duration::from_millis(100));
            }
        }
    }
    let result = f();
    let _ = fs::remove_dir_all(&lock_dir);
    result
}

fn scan_cache_area(area: &str, path: &Path) -> Vec<Value> {
    if !path.exists() {
        return Vec::new();
    }
    let mut entries = Vec::new();
    let files = walk_tree(path, usize::MAX / 2, &[]).unwrap_or_default();
    for file_path in files.into_iter().filter(|candidate| candidate.is_file()) {
        let Ok(stat) = file_path.metadata() else {
            continue;
        };
        let modified_at = stat
            .modified()
            .ok()
            .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
            .map(|duration| duration.as_secs())
            .unwrap_or(0);
        entries.push(json!({
            "area": area,
            "path": file_path.display().to_string(),
            "relative_path": file_path.strip_prefix(path).ok().map(|item| item.display().to_string()).unwrap_or_else(|| file_path.display().to_string()),
            "size_bytes": stat.len(),
            "modified_at": modified_at,
        }));
    }
    entries
}

fn build_cache_index(cache: &CacheConfig) -> Result<Value, String> {
    ensure_cache_layout(cache)?;
    let mut entries = Vec::new();
    let mut areas = serde_json::Map::new();
    let mut total_size = 0u64;
    let mut total_files = 0u64;
    let root = PathBuf::from(&cache.root);
    for area in CACHE_AREAS {
        let area_path = cache
            .paths
            .get(area)
            .map(PathBuf::from)
            .unwrap_or_else(|| root.join(area));
        let area_entries = scan_cache_area(area, &area_path);
        let area_size: u64 = area_entries
            .iter()
            .filter_map(|item| item.get("size_bytes").and_then(|value| value.as_u64()))
            .sum();
        total_size += area_size;
        total_files += area_entries.len() as u64;
        entries.extend(area_entries.clone());
        areas.insert(
            area.to_string(),
            json!({
                "path": area_path.display().to_string(),
                "file_count": area_entries.len(),
                "size_bytes": area_size,
            }),
        );
    }
    Ok(json!({
        "version": 1,
        "generated_at": iso_timestamp(),
        "root": cache.root,
        "policy": cache.policy,
        "total_files": total_files,
        "total_size_bytes": total_size,
        "areas": areas,
        "entries": entries,
    }))
}

fn atomic_write_json(path: &Path, payload: &Value) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|err| format!("Could not create {}: {err}", parent.display()))?;
    }
    let tmp = path.with_extension("tmp");
    let text = serde_json::to_string_pretty(payload)
        .map_err(|err| format!("Could not serialize {}: {err}", path.display()))?;
    fs::write(&tmp, format!("{text}\n"))
        .map_err(|err| format!("Could not write {}: {err}", tmp.display()))?;
    fs::rename(&tmp, path).map_err(|err| {
        format!(
            "Could not move {} to {}: {err}",
            tmp.display(),
            path.display()
        )
    })?;
    Ok(())
}

fn refresh_cache_index(cache: &CacheConfig) -> Result<Value, String> {
    if !cache.enabled {
        return Ok(json!({
            "version": 1,
            "generated_at": iso_timestamp(),
            "root": cache.root,
            "policy": "off",
            "total_files": 0,
            "total_size_bytes": 0,
            "areas": {},
            "entries": [],
        }));
    }
    cache_lock(cache, "index", || {
        let index = build_cache_index(cache)?;
        atomic_write_json(&PathBuf::from(&cache.meta.index), &index)?;
        Ok(index)
    })
}

fn load_cache_index(cache: &CacheConfig, refresh: bool) -> Result<Value, String> {
    if !cache.enabled {
        return refresh_cache_index(cache);
    }
    ensure_cache_layout(cache)?;
    let path = PathBuf::from(&cache.meta.index);
    if refresh || !path.exists() {
        return refresh_cache_index(cache);
    }
    let text = read_text(&path)?;
    match serde_json::from_str::<Value>(&text) {
        Ok(payload) if payload.is_object() => Ok(payload),
        _ => refresh_cache_index(cache),
    }
}

#[derive(Debug, Clone, Default)]
struct StableDigest {
    a: u64,
    b: u64,
}

impl StableDigest {
    fn new() -> Self {
        Self {
            a: 0xcbf29ce484222325,
            b: 0x84222325cbf29ce4,
        }
    }

    fn update(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.a ^= *byte as u64;
            self.a = self.a.wrapping_mul(0x100000001b3);
            self.b ^= (*byte as u64).wrapping_add(0x9e3779b97f4a7c15);
            self.b = self.b.wrapping_mul(0x100000001b3 ^ 0x9e3779b97f4a7c15);
        }
    }

    fn update_text(&mut self, value: &str) {
        self.update(value.as_bytes());
        self.update(b"\0");
    }

    fn hex(&self) -> String {
        format!("{:016x}{:016x}", self.a, self.b)
    }
}

fn cache_area_path(cache: &CacheConfig, area: &str) -> PathBuf {
    cache
        .paths
        .get(area)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(&cache.root).join(area))
}

fn stage_cache_root(cache: &CacheConfig) -> PathBuf {
    cache_area_path(cache, STAGE_RESULTS_AREA)
}

fn stage_cache_entry_dir(cache: &CacheConfig, key: &str) -> PathBuf {
    stage_cache_root(cache).join(key)
}

fn stage_cache_manifest_path(cache: &CacheConfig, key: &str) -> PathBuf {
    stage_cache_entry_dir(cache, key).join("manifest.json")
}

fn stage_cache_payload_dir(cache: &CacheConfig, key: &str) -> PathBuf {
    stage_cache_entry_dir(cache, key).join("files")
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct WorkspaceIndexEntry {
    #[serde(default)]
    path: String,
    #[serde(default)]
    digest: String,
    #[serde(default)]
    size_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct WorkspaceIndexManifest {
    #[serde(default)]
    version: u8,
    #[serde(default)]
    root: String,
    #[serde(default)]
    generated_at: String,
    #[serde(default)]
    digest: String,
    #[serde(default)]
    file_count: usize,
    #[serde(default)]
    total_size_bytes: u64,
    #[serde(default)]
    entries: Vec<WorkspaceIndexEntry>,
}

fn workspace_index_path(cache: &CacheConfig, workspace_root: &Path) -> PathBuf {
    let mut digest = StableDigest::new();
    digest.update_text(&workspace_root.display().to_string());
    PathBuf::from(&cache.meta.root)
        .join("workspace-index")
        .join(format!("{}.json", digest.hex()))
}

fn path_is_host_probe(path: &Path) -> bool {
    path.file_name().and_then(|value| value.to_str()) == Some("probe.json")
        && path
            .parent()
            .and_then(|value| value.file_name())
            .and_then(|value| value.to_str())
            == Some("host")
}

fn normalize_host_probe_for_cache(mut facts: HostFacts) -> HostFacts {
    facts.captured_at.clear();
    facts.artifact = None;
    facts.history_artifact = None;
    facts
}

fn read_file_digest(path: &Path) -> Result<String, String> {
    if path_is_host_probe(path) {
        let facts: HostFacts = read_json(path)?;
        let normalized = normalize_host_probe_for_cache(facts);
        let text = serde_json::to_string(&normalized)
            .map_err(|err| format!("Could not normalize {}: {err}", path.display()))?;
        let mut digest = StableDigest::new();
        digest.update_text(&text);
        return Ok(digest.hex());
    }
    let mut file =
        File::open(path).map_err(|err| format!("Could not open {}: {err}", path.display()))?;
    let mut digest = StableDigest::new();
    let mut buffer = [0u8; 8192];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|err| format!("Could not read {}: {err}", path.display()))?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(digest.hex())
}

fn normalize_prompt_for_cache(prompt: &str) -> String {
    prompt
        .lines()
        .filter(|line| {
            let trimmed = line.trim();
            !trimmed.starts_with("- `captured_at`:")
                && !trimmed.starts_with("- Host facts captured at:")
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn split_prompt_for_cache(prompt: &str) -> PromptCacheHashes {
    let normalized = normalize_prompt_for_cache(prompt);
    let mut stable_prefix = normalized.clone();
    let mut dynamic_suffix = String::new();
    for marker in ["\nDynamic stage context:\n", "\nDynamic context:\n"] {
        if let Some(index) = normalized.find(marker) {
            stable_prefix = normalized[..index].trim_end().to_string();
            dynamic_suffix = normalized[index..].trim_start().to_string();
            break;
        }
    }
    if dynamic_suffix.is_empty() {
        if let Some(index) = normalized.find("\nStage prompt:\n") {
            stable_prefix = normalized[..index].trim_end().to_string();
            dynamic_suffix = normalized[index..].trim_start().to_string();
        }
    }
    let mut combined_digest = StableDigest::new();
    combined_digest.update_text(&normalized);
    let mut stable_digest = StableDigest::new();
    stable_digest.update_text(&stable_prefix);
    let mut dynamic_digest = StableDigest::new();
    dynamic_digest.update_text(&dynamic_suffix);
    PromptCacheHashes {
        combined: combined_digest.hex(),
        stable_prefix: stable_digest.hex(),
        dynamic_suffix: dynamic_digest.hex(),
    }
}

fn build_workspace_index(path: &Path) -> Result<WorkspaceIndexManifest, String> {
    if !path.exists() || !path.is_dir() {
        return Ok(WorkspaceIndexManifest {
            version: 1,
            root: path.display().to_string(),
            generated_at: iso_timestamp(),
            digest: "missing-workspace".to_string(),
            file_count: 0,
            total_size_bytes: 0,
            entries: Vec::new(),
        });
    }
    let mut digest = StableDigest::new();
    let mut entries = Vec::new();
    let mut total_size_bytes = 0u64;
    let mut files = walk_tree(
        path,
        usize::MAX / 2,
        &[
            ".git",
            "node_modules",
            ".venv",
            "venv",
            "__pycache__",
            "target",
            ".cache",
            "dist",
            "build",
        ],
    )
    .unwrap_or_default();
    files.sort();
    for file in files.into_iter().filter(|item| item.is_file()) {
        let relative = file
            .strip_prefix(path)
            .ok()
            .map(|item| item.display().to_string())
            .unwrap_or_else(|| file.display().to_string());
        let file_digest = read_file_digest(&file)?;
        let size_bytes = file.metadata().map(|meta| meta.len()).unwrap_or(0);
        total_size_bytes += size_bytes;
        digest.update_text(&relative);
        digest.update_text(&file_digest);
        entries.push(WorkspaceIndexEntry {
            path: relative,
            digest: file_digest,
            size_bytes,
        });
    }
    Ok(WorkspaceIndexManifest {
        version: 1,
        root: path.display().to_string(),
        generated_at: iso_timestamp(),
        digest: digest.hex(),
        file_count: entries.len(),
        total_size_bytes,
        entries,
    })
}

fn workspace_snapshot_hash(cache: &CacheConfig, path: &Path) -> Result<String, String> {
    let index = build_workspace_index(path)?;
    if cache.enabled {
        let path = workspace_index_path(cache, path);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|err| format!("Could not create {}: {err}", parent.display()))?;
        }
        write_json(&path, &index)?;
    }
    Ok(index.digest)
}

fn estimate_token_count(text: &str) -> u64 {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return 0;
    }
    let bytes = trimmed.len() as u64;
    std::cmp::max(1, bytes.div_ceil(4))
}

fn response_payload_path(last_message_path: &Path) -> Option<PathBuf> {
    let file_name = last_message_path.file_name()?.to_str()?;
    let response_name = file_name.replace(".last.md", ".response.json");
    Some(last_message_path.parent()?.join(response_name))
}

fn build_stage_token_usage(prompt: &str, last_message_path: &Path) -> TokenUsage {
    if let Some(response_path) = response_payload_path(last_message_path) {
        if response_path.exists() {
            if let Ok(payload) = read_json::<Value>(&response_path) {
                return extract_response_usage(&payload);
            }
        }
    }
    let prompt_tokens = estimate_token_count(&normalize_prompt_for_cache(prompt));
    let completion_text = read_text(last_message_path).unwrap_or_default();
    let completion_tokens = estimate_token_count(&completion_text);
    TokenUsage {
        source: "estimated-local".to_string(),
        prompt_tokens,
        cached_prompt_tokens: 0,
        completion_tokens,
        total_tokens: prompt_tokens + completion_tokens,
        estimated_saved_tokens: 0,
    }
}

fn token_usage_from_cache_hit(manifest: &StageCacheManifest) -> TokenUsage {
    let saved_total = if manifest.token_usage.total_tokens > 0 {
        manifest.token_usage.total_tokens
    } else {
        manifest
            .token_usage
            .prompt_tokens
            .saturating_add(manifest.token_usage.completion_tokens)
    };
    TokenUsage {
        source: format!("cache-hit:{}", manifest.token_usage.source),
        prompt_tokens: 0,
        cached_prompt_tokens: manifest
            .token_usage
            .prompt_tokens
            .max(manifest.token_usage.cached_prompt_tokens),
        completion_tokens: 0,
        total_tokens: 0,
        estimated_saved_tokens: saved_total,
    }
}

fn stage_input_paths(
    ctx: &Context,
    plan: &Plan,
    run_dir: &Path,
    stage: &str,
) -> Result<Vec<PathBuf>, String> {
    let mut paths = vec![run_dir.join("request.md"), run_dir.join("plan.json")];
    if amendments_exist(run_dir) {
        paths.push(amendments_path(run_dir));
    }
    paths.push(ctx.repo_root.join(ROLE_MAP_REF));
    let stage_kind = pipeline_stage_kind_for(plan, run_dir, stage)?;
    match stage_kind {
        PipelineStageKind::Intake => {
            paths.push(ctx.repo_root.join(DECOMPOSITION_RULES_REF));
        }
        PipelineStageKind::Solver => {
            paths.push(run_dir.join("brief.md"));
        }
        PipelineStageKind::Review => {
            paths.push(run_dir.join("brief.md"));
            paths.push(ctx.repo_root.join(REVIEW_RUBRIC_REF));
            for solver in solver_ids(plan, run_dir) {
                paths.push(run_dir.join("solutions").join(solver).join("RESULT.md"));
            }
        }
        PipelineStageKind::Execution => {
            paths.push(run_dir.join("brief.md"));
            if first_stage_id_for_kind(plan, run_dir, PipelineStageKind::Review)?.is_some() {
                paths.push(run_dir.join("review").join("report.md"));
                paths.push(run_dir.join("review").join("scorecard.json"));
                paths.push(run_dir.join("review").join("user-summary.md"));
            }
            for solver in solver_ids(plan, run_dir) {
                paths.push(run_dir.join("solutions").join(solver).join("RESULT.md"));
            }
            let probe = host_probe_path(run_dir);
            if probe.exists() {
                paths.push(probe);
            }
        }
        PipelineStageKind::Verification => {
            paths.push(run_dir.join("brief.md"));
            if first_stage_id_for_kind(plan, run_dir, PipelineStageKind::Review)?.is_some() {
                paths.push(run_dir.join("review").join("report.md"));
                paths.push(run_dir.join("review").join("scorecard.json"));
                paths.push(run_dir.join("review").join("user-summary.md"));
            }
            if first_stage_id_for_kind(plan, run_dir, PipelineStageKind::Execution)?.is_some() {
                paths.push(run_dir.join("execution").join("report.md"));
            }
            paths.push(ctx.repo_root.join(VERIFICATION_RUBRIC_REF));
            let probe = host_probe_path(run_dir);
            if probe.exists() {
                paths.push(probe);
            }
        }
    }
    paths.sort();
    paths.dedup();
    Ok(paths)
}

#[allow(clippy::too_many_arguments)]
fn stage_cache_key(
    ctx: &Context,
    cache: &CacheConfig,
    plan: &Plan,
    run_dir: &Path,
    stage: &str,
    backend: StageBackendKind,
    command: &[String],
    prompt: &str,
) -> Result<(String, Vec<String>, PromptCacheHashes, String), String> {
    let mut digest = StableDigest::new();
    digest.update_text("stage-cache-v2");
    digest.update_text(stage);
    digest.update_text(match backend {
        StageBackendKind::Codex => "backend:codex",
        StageBackendKind::Responses => "backend:responses",
        StageBackendKind::LocalTemplate(LocalTemplateKind::HelloWorldPython) => {
            "backend:local-template:hello-world-python"
        }
    });
    for part in command {
        digest.update_text(part);
    }
    let prompt_hashes = split_prompt_for_cache(prompt);
    digest.update_text(&prompt_hashes.combined);
    digest.update_text(&prompt_hashes.stable_prefix);
    digest.update_text(&prompt_hashes.dynamic_suffix);
    let inputs = stage_input_paths(ctx, plan, run_dir, stage)?;
    let mut input_descriptions = Vec::new();
    for path in inputs {
        let label = if path.exists() {
            let hash = read_file_digest(&path)?;
            format!("{}:{hash}", path.display())
        } else {
            format!("{}:missing", path.display())
        };
        digest.update_text(&label);
        input_descriptions.push(label);
    }
    let workspace_hash = if backend_reads_workspace(plan, run_dir, backend, stage)? {
        workspace_snapshot_hash(cache, &working_root(plan, run_dir))?
    } else {
        "workspace-not-read-by-backend".to_string()
    };
    digest.update_text(&workspace_hash);
    Ok((
        digest.hex(),
        input_descriptions,
        prompt_hashes,
        workspace_hash,
    ))
}

fn stage_cache_rel_paths(run_dir: &Path, paths: &[PathBuf]) -> Vec<String> {
    paths
        .iter()
        .filter_map(|path| {
            path.strip_prefix(run_dir)
                .ok()
                .map(|value| value.display().to_string())
        })
        .collect()
}

fn restore_stage_cache(
    cache: &CacheConfig,
    key: &str,
    run_dir: &Path,
    stage: &str,
) -> Result<Option<StageCacheManifest>, String> {
    let manifest_path = stage_cache_manifest_path(cache, key);
    if !manifest_path.exists() {
        return Ok(None);
    }
    let manifest: StageCacheManifest = read_json(&manifest_path)?;
    if manifest.stage != stage {
        return Ok(None);
    }
    let payload_root = stage_cache_payload_dir(cache, key);
    if !payload_root.exists() {
        return Ok(None);
    }
    for relative in manifest
        .outputs
        .iter()
        .chain(manifest.logs.iter())
        .cloned()
        .collect::<Vec<_>>()
    {
        let source = payload_root.join(&relative);
        if !source.exists() {
            return Ok(None);
        }
        let target = run_dir.join(&relative);
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent)
                .map_err(|err| format!("Could not create {}: {err}", parent.display()))?;
        }
        fs::copy(&source, &target).map_err(|err| {
            format!(
                "Could not restore cached artifact {} to {}: {err}",
                source.display(),
                target.display()
            )
        })?;
    }
    Ok(Some(manifest))
}

#[allow(clippy::too_many_arguments)]
fn store_stage_cache(
    cache: &CacheConfig,
    key: &str,
    run_dir: &Path,
    stage: &str,
    command: &[String],
    prompt_hashes: &PromptCacheHashes,
    workspace_hash: &str,
    token_usage: &TokenUsage,
    inputs: &[String],
    outputs: &[PathBuf],
    logs: &[PathBuf],
) -> Result<(), String> {
    if !cache.enabled || cache.policy == "off" {
        return Ok(());
    }
    let output_rel = stage_cache_rel_paths(run_dir, outputs);
    let log_rel = stage_cache_rel_paths(run_dir, logs);
    let entry_dir = stage_cache_entry_dir(cache, key);
    let payload_dir = stage_cache_payload_dir(cache, key);
    cache_lock(cache, &format!("stage-result-{key}"), || {
        fs::create_dir_all(&payload_dir)
            .map_err(|err| format!("Could not create {}: {err}", payload_dir.display()))?;
        for path in outputs.iter().chain(logs.iter()) {
            if !path.exists() {
                continue;
            }
            let relative = path.strip_prefix(run_dir).map_err(|_| {
                format!(
                    "Cached stage artifact {} is outside run dir {}",
                    path.display(),
                    run_dir.display()
                )
            })?;
            let target = payload_dir.join(relative);
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent)
                    .map_err(|err| format!("Could not create {}: {err}", parent.display()))?;
            }
            fs::copy(path, &target).map_err(|err| {
                format!(
                    "Could not copy {} to {}: {err}",
                    path.display(),
                    target.display()
                )
            })?;
        }
        let manifest = StageCacheManifest {
            version: 2,
            key: key.to_string(),
            stage: stage.to_string(),
            created_at: iso_timestamp(),
            command: command.to_vec(),
            prompt_hash: prompt_hashes.combined.clone(),
            stable_prefix_hash: prompt_hashes.stable_prefix.clone(),
            dynamic_suffix_hash: prompt_hashes.dynamic_suffix.clone(),
            workspace_hash: workspace_hash.to_string(),
            token_usage: token_usage.clone(),
            inputs: inputs.to_vec(),
            outputs: output_rel.clone(),
            logs: log_rel.clone(),
        };
        write_json(&entry_dir.join("manifest.json"), &manifest)
    })
}

fn token_ledger_path(run_dir: &Path) -> PathBuf {
    run_dir.join("runtime").join("token-ledger.json")
}

fn load_token_ledger(run_dir: &Path, plan: &Plan) -> Result<RunTokenLedger, String> {
    let path = token_ledger_path(run_dir);
    if !path.exists() {
        return Ok(RunTokenLedger {
            version: 1,
            updated_at: iso_timestamp(),
            budget: plan.token_budget.clone(),
            entries: Vec::new(),
        });
    }
    let mut ledger: RunTokenLedger = read_json(&path)?;
    if ledger.budget.total_tokens == 0 {
        ledger.budget = plan.token_budget.clone();
    }
    if ledger.version == 0 {
        ledger.version = 1;
    }
    Ok(ledger)
}

fn save_token_ledger(run_dir: &Path, ledger: &RunTokenLedger) -> Result<(), String> {
    let path = token_ledger_path(run_dir);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|err| format!("Could not create {}: {err}", parent.display()))?;
    }
    write_json(&path, ledger)
}

#[allow(clippy::too_many_arguments)]
fn record_token_usage(
    run_dir: &Path,
    plan: &Plan,
    stage: &str,
    mode: &str,
    cache_key: &str,
    prompt_hashes: &PromptCacheHashes,
    workspace_hash: &str,
    usage: &TokenUsage,
) -> Result<(), String> {
    let mut ledger = load_token_ledger(run_dir, plan)?;
    ledger.updated_at = iso_timestamp();
    ledger.entries.push(RunTokenLedgerEntry {
        stage: stage.to_string(),
        mode: mode.to_string(),
        recorded_at: ledger.updated_at.clone(),
        cache_key: cache_key.to_string(),
        prompt_hashes: prompt_hashes.clone(),
        workspace_hash: workspace_hash.to_string(),
        usage: usage.clone(),
    });
    save_token_ledger(run_dir, &ledger)
}

fn summarize_token_ledger(run_dir: &Path, plan: &Plan) -> Result<RunTokenSummary, String> {
    let ledger = load_token_ledger(run_dir, plan)?;
    let mut summary = RunTokenSummary {
        budget_total_tokens: ledger.budget.total_tokens,
        warning_threshold_tokens: ledger.budget.warning_threshold_tokens,
        source: ledger.budget.source.clone(),
        ..RunTokenSummary::default()
    };
    for entry in ledger.entries {
        summary.prompt_tokens = summary
            .prompt_tokens
            .saturating_add(entry.usage.prompt_tokens);
        summary.cached_prompt_tokens = summary
            .cached_prompt_tokens
            .saturating_add(entry.usage.cached_prompt_tokens);
        summary.completion_tokens = summary
            .completion_tokens
            .saturating_add(entry.usage.completion_tokens);
        summary.used_total_tokens = summary
            .used_total_tokens
            .saturating_add(entry.usage.total_tokens);
        summary.estimated_saved_tokens = summary
            .estimated_saved_tokens
            .saturating_add(entry.usage.estimated_saved_tokens);
    }
    summary.remaining_tokens = if summary.budget_total_tokens > 0 {
        Some(
            summary
                .budget_total_tokens
                .saturating_sub(summary.used_total_tokens),
        )
    } else {
        None
    };
    Ok(summary)
}

fn format_size(size_bytes: u64) -> String {
    let units = ["B", "KB", "MB", "GB", "TB"];
    let mut size = size_bytes as f64;
    for unit in units {
        if size < 1024.0 || unit == "TB" {
            return if unit == "B" {
                format!("{} B", size as u64)
            } else {
                format!("{size:.1} {unit}")
            };
        }
        size /= 1024.0;
    }
    format!("{size_bytes} B")
}

fn prune_cache(
    cache: &CacheConfig,
    max_age_days: Option<i64>,
    area_filters: &[String],
    dry_run: bool,
) -> Result<Value, String> {
    if !cache.enabled {
        return Ok(json!({
            "removed_files": 0,
            "removed_bytes": 0,
            "areas": {},
            "dry_run": dry_run,
            "index": refresh_cache_index(cache)?,
        }));
    }
    let threshold = max_age_days.map(|days| {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64
            - days * 86_400
    });
    let filter_set: BTreeSet<String> = area_filters.iter().cloned().collect();
    cache_lock(cache, "prune", || {
        let mut removed_files = 0u64;
        let mut removed_bytes = 0u64;
        let mut per_area = serde_json::Map::new();
        let root = PathBuf::from(&cache.root);
        for area in CACHE_AREAS {
            if !filter_set.is_empty() && !filter_set.contains(area) {
                continue;
            }
            let area_path = cache
                .paths
                .get(area)
                .map(PathBuf::from)
                .unwrap_or_else(|| root.join(area));
            if !area_path.exists() {
                continue;
            }
            let files = walk_tree(&area_path, usize::MAX / 2, &[]).unwrap_or_default();
            for file_path in files.into_iter().filter(|path| path.is_file()) {
                let Ok(stat) = file_path.metadata() else {
                    continue;
                };
                let modified = stat
                    .modified()
                    .ok()
                    .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
                    .map(|value| value.as_secs() as i64)
                    .unwrap_or(0);
                if let Some(limit) = threshold {
                    if modified > limit {
                        continue;
                    }
                }
                removed_files += 1;
                removed_bytes += stat.len();
                let entry = per_area
                    .entry(area.to_string())
                    .or_insert_with(|| json!({"removed_files": 0u64, "removed_bytes": 0u64}));
                if let Some(obj) = entry.as_object_mut() {
                    obj.insert(
                        "removed_files".to_string(),
                        json!(
                            obj.get("removed_files")
                                .and_then(|value| value.as_u64())
                                .unwrap_or(0)
                                + 1
                        ),
                    );
                    obj.insert(
                        "removed_bytes".to_string(),
                        json!(
                            obj.get("removed_bytes")
                                .and_then(|value| value.as_u64())
                                .unwrap_or(0)
                                + stat.len()
                        ),
                    );
                }
                if !dry_run {
                    let _ = fs::remove_file(&file_path);
                }
            }
            if !dry_run {
                let mut dirs = Vec::new();
                collect_dirs(&area_path, &mut dirs);
                dirs.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
                for dir in dirs {
                    let _ = fs::remove_dir(&dir);
                }
            }
        }
        let index = refresh_cache_index(cache)?;
        Ok(json!({
            "removed_files": removed_files,
            "removed_bytes": removed_bytes,
            "areas": per_area,
            "dry_run": dry_run,
            "index": index,
        }))
    })
}

fn collect_dirs(root: &Path, out: &mut Vec<PathBuf>) {
    if !root.exists() {
        return;
    }
    let entries = match fs::read_dir(root) {
        Ok(entries) => entries,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_dirs(&path, out);
            out.push(path);
        }
    }
}

fn cache_config_from_plan(plan: &Plan) -> CacheConfig {
    merge_cache_config(&plan.cache)
}

fn plan_workspace(plan: &Plan) -> PathBuf {
    PathBuf::from(if plan.workspace.is_empty() {
        "."
    } else {
        &plan.workspace
    })
    .expanduser()
}

fn load_plan(run_dir: &Path) -> Result<Plan, String> {
    let path = run_dir.join("plan.json");
    let text = read_text(&path)?;
    match serde_json::from_str(&text) {
        Ok(plan) => Ok(plan),
        Err(_) => {
            let mut value: Value = serde_json::from_str(&text)
                .map_err(|err| format!("Could not parse {}: {err}", path.display()))?;
            normalize_legacy_plan_json(&mut value);
            serde_json::from_value(value)
                .map_err(|err| format!("Could not parse {}: {err}", path.display()))
        }
    }
}

fn save_plan(run_dir: &Path, plan: &Plan) -> Result<(), String> {
    write_json(&run_dir.join("plan.json"), plan)
}

fn normalize_legacy_plan_json(value: &mut Value) {
    let Some(obj) = value.as_object_mut() else {
        return;
    };
    let Some(hints) = obj.get_mut("workstream_hints") else {
        return;
    };
    let Some(items) = hints.as_array_mut() else {
        return;
    };
    for item in items.iter_mut() {
        if let Some(name) = item
            .as_str()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            *item = json!({
                "name": name,
                "goal": name,
                "suggested_role": "",
            });
        }
    }
}

fn pipeline_kind_from_str(value: &str) -> Option<PipelineStageKind> {
    match value.trim().to_ascii_lowercase().as_str() {
        "intake" | "brief" | "planning" => Some(PipelineStageKind::Intake),
        "solver" | "research" | "analysis" | "researcher" => Some(PipelineStageKind::Solver),
        "review" | "compare" | "synthesis" => Some(PipelineStageKind::Review),
        "execution" | "implement" | "implementation" | "apply" => {
            Some(PipelineStageKind::Execution)
        }
        "verification" | "verify" | "audit" | "check" => Some(PipelineStageKind::Verification),
        _ => None,
    }
}

fn pipeline_kind_label(kind: PipelineStageKind) -> &'static str {
    match kind {
        PipelineStageKind::Intake => "intake",
        PipelineStageKind::Solver => "solver",
        PipelineStageKind::Review => "review",
        PipelineStageKind::Execution => "execution",
        PipelineStageKind::Verification => "verification",
    }
}

fn strip_matching_quotes(value: &str) -> &str {
    let trimmed = value.trim();
    if trimmed.len() >= 2 {
        let bytes = trimmed.as_bytes();
        if (bytes[0] == b'"' && bytes[trimmed.len() - 1] == b'"')
            || (bytes[0] == b'\'' && bytes[trimmed.len() - 1] == b'\'')
        {
            return &trimmed[1..trimmed.len() - 1];
        }
    }
    trimmed
}

fn parse_inline_yaml_list(value: &str) -> Vec<String> {
    let trimmed = strip_matching_quotes(value);
    if !(trimmed.starts_with('[') && trimmed.ends_with(']')) {
        return Vec::new();
    }
    let inner = &trimmed[1..trimmed.len() - 1];
    inner
        .split(',')
        .map(strip_matching_quotes)
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(|item| item.to_string())
        .collect()
}

fn parse_pipeline_stage_field(
    stage: &mut PipelineStageSpec,
    raw_line: &str,
    line_number: usize,
) -> Result<(), String> {
    let Some((key, value)) = raw_line.split_once(':') else {
        return Err(format!(
            "Invalid pipeline YAML line {}: expected `key: value`.",
            line_number
        ));
    };
    let key = key.trim();
    let value = value.trim();
    match key {
        "id" => stage.id = strip_matching_quotes(value).to_string(),
        "kind" => stage.kind = strip_matching_quotes(value).to_string(),
        "role" => stage.role = strip_matching_quotes(value).to_string(),
        "angle" => stage.angle = strip_matching_quotes(value).to_string(),
        "description" => stage.description = strip_matching_quotes(value).to_string(),
        "depends_on" | "needs" => stage.depends_on = parse_inline_yaml_list(value),
        other => {
            return Err(format!(
                "Unsupported pipeline YAML field `{other}` on line {}.",
                line_number
            ))
        }
    }
    Ok(())
}

fn parse_pipeline_yaml_spec(source: &str) -> Result<PipelineConfig, String> {
    let mut in_pipeline = false;
    let mut in_stages = false;
    let mut stages = Vec::new();
    let mut current: Option<PipelineStageSpec> = None;

    for (line_number, raw_line) in source.lines().enumerate() {
        let line_number = line_number + 1;
        let trimmed = raw_line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let indent = raw_line.chars().take_while(|ch| *ch == ' ').count();

        if indent == 0 && trimmed == "pipeline:" {
            in_pipeline = true;
            in_stages = false;
            continue;
        }
        if indent == 0 && trimmed == "stages:" {
            in_pipeline = false;
            in_stages = true;
            continue;
        }
        if in_pipeline && indent == 2 && trimmed == "stages:" {
            in_stages = true;
            continue;
        }
        if !in_stages {
            continue;
        }

        if trimmed.starts_with("- ") {
            if let Some(stage) = current.take() {
                stages.push(stage);
            }
            let mut stage = PipelineStageSpec::default();
            let remainder = trimmed.trim_start_matches("- ").trim();
            if !remainder.is_empty() {
                parse_pipeline_stage_field(&mut stage, remainder, line_number)?;
            }
            current = Some(stage);
            continue;
        }

        let Some(stage) = current.as_mut() else {
            return Err(format!(
                "Invalid pipeline YAML line {}: stage field is not attached to a list item.",
                line_number
            ));
        };
        parse_pipeline_stage_field(stage, trimmed, line_number)?;
    }

    if let Some(stage) = current.take() {
        stages.push(stage);
    }

    Ok(PipelineConfig {
        source: "yaml".to_string(),
        stages,
    })
}

fn normalize_pipeline_stage_specs(
    config: &PipelineConfig,
    _plan: &Plan,
) -> Result<Vec<PipelineStageSpec>, String> {
    let mut stages = config.stages.clone();
    if stages.is_empty() {
        return Ok(Vec::new());
    }

    let mut normalized: Vec<PipelineStageSpec> = Vec::new();
    let mut seen = BTreeSet::new();
    let mut counts = BTreeMap::new();
    for (index, mut stage) in stages.drain(..).enumerate() {
        let kind = pipeline_kind_from_str(&stage.kind).ok_or_else(|| {
            format!(
                "Unsupported pipeline stage kind `{}` for stage `{}`.",
                stage.kind, stage.id
            )
        })?;
        if stage.id.trim().is_empty() {
            stage.id = format!("{}-{}", pipeline_kind_label(kind), index + 1);
        }
        stage.id = slugify(&stage.id);
        if stage.id.is_empty() {
            return Err("Pipeline stage id became empty after normalization.".to_string());
        }
        stage.kind = pipeline_kind_label(kind).to_string();
        if stage.role_source.trim().is_empty() {
            stage.role_source = if stage.role.trim().is_empty() || config.source == "default" {
                "auto".to_string()
            } else {
                "explicit".to_string()
            };
        }
        if kind == PipelineStageKind::Solver && stage.angle.trim().is_empty() {
            let solver_index = normalized
                .iter()
                .filter(|item| item.kind == "solver")
                .count();
            stage.angle = ANGLE_SEQUENCE[solver_index % ANGLE_SEQUENCE.len()].to_string();
        }
        if !seen.insert(stage.id.clone()) {
            return Err(format!("Duplicate pipeline stage id `{}`.", stage.id));
        }
        *counts.entry(stage.kind.clone()).or_insert(0usize) += 1;
        normalized.push(stage);
    }

    if counts.get("intake").copied().unwrap_or(0) == 0 {
        normalized.insert(
            0,
            PipelineStageSpec {
                id: "intake".to_string(),
                kind: "intake".to_string(),
                ..PipelineStageSpec::default()
            },
        );
    }
    for singleton in ["intake", "review", "execution", "verification"] {
        if normalized
            .iter()
            .filter(|stage| stage.kind == singleton)
            .count()
            > 1
        {
            return Err(format!(
                "Pipeline currently supports only one `{singleton}` stage."
            ));
        }
    }
    let stage_ids: BTreeSet<String> = normalized.iter().map(|stage| stage.id.clone()).collect();
    for stage in &normalized {
        for dep in &stage.depends_on {
            let dep_id = slugify(dep);
            if !stage_ids.contains(&dep_id) {
                return Err(format!(
                    "Pipeline stage `{}` depends on unknown stage `{}`.",
                    stage.id, dep
                ));
            }
        }
    }
    Ok(normalized)
}

fn default_solver_stage_ids(plan: &Plan, run_dir: Option<&Path>) -> Vec<String> {
    if !plan.solver_roles.is_empty() {
        return plan
            .solver_roles
            .iter()
            .map(|item| item.solver_id.clone())
            .collect();
    }
    if let Some(run_dir) = run_dir {
        let solutions_dir = run_dir.join("solutions");
        if solutions_dir.exists() {
            let mut ids: Vec<String> = fs::read_dir(solutions_dir)
                .ok()
                .into_iter()
                .flatten()
                .filter_map(|entry| entry.ok().map(|value| value.path()))
                .filter(|path| path.is_dir())
                .filter_map(|path| {
                    path.file_name()
                        .map(|name| name.to_string_lossy().to_string())
                })
                .collect();
            ids.sort();
            if !ids.is_empty() {
                return ids;
            }
        }
    }
    let count = std::cmp::max(1, plan.solver_count);
    (0..count)
        .map(|index| format!("solver-{}", (b'a' + index as u8) as char))
        .collect()
}

fn default_pipeline_stage_specs(plan: &Plan, run_dir: Option<&Path>) -> Vec<PipelineStageSpec> {
    let solver_ids = default_solver_stage_ids(plan, run_dir);
    let solver_role_map: BTreeMap<String, SolverRole> = plan
        .solver_roles
        .iter()
        .map(|item| (item.solver_id.clone(), item.clone()))
        .collect();
    let mut stages = vec![PipelineStageSpec {
        id: "intake".to_string(),
        kind: "intake".to_string(),
        ..PipelineStageSpec::default()
    }];
    for (index, solver_id) in solver_ids.into_iter().enumerate() {
        let role = solver_role_map.get(&solver_id).cloned().unwrap_or_else(|| {
            choose_roles(&plan.task_kind, std::cmp::max(1, plan.solver_count))
                .get(index)
                .cloned()
                .unwrap_or(SolverRole {
                    solver_id: solver_id.clone(),
                    role: "engineering/engineering-senior-developer.md".to_string(),
                    angle: ANGLE_SEQUENCE[index % ANGLE_SEQUENCE.len()].to_string(),
                })
        });
        stages.push(PipelineStageSpec {
            id: solver_id.clone(),
            kind: "solver".to_string(),
            role: role.role,
            role_source: "auto".to_string(),
            angle: role.angle,
            ..PipelineStageSpec::default()
        });
    }
    stages.push(PipelineStageSpec {
        id: "review".to_string(),
        kind: "review".to_string(),
        ..PipelineStageSpec::default()
    });
    stages.push(PipelineStageSpec {
        id: "execution".to_string(),
        kind: "execution".to_string(),
        ..PipelineStageSpec::default()
    });
    stages.push(PipelineStageSpec {
        id: "verification".to_string(),
        kind: "verification".to_string(),
        ..PipelineStageSpec::default()
    });
    stages
}

fn pipeline_stage_specs(
    plan: &Plan,
    run_dir: Option<&Path>,
) -> Result<Vec<PipelineStageSpec>, String> {
    let config = if plan.pipeline.stages.is_empty() {
        PipelineConfig {
            source: "default".to_string(),
            stages: default_pipeline_stage_specs(plan, run_dir),
        }
    } else {
        plan.pipeline.clone()
    };
    normalize_pipeline_stage_specs(&config, plan)
}

fn apply_pipeline_solver_defaults(plan: &mut Plan, run_dir: Option<&Path>) -> Result<(), String> {
    let mut stages = pipeline_stage_specs(plan, run_dir)?;
    let solver_total = stages
        .iter()
        .filter(|stage| pipeline_kind_from_str(&stage.kind) == Some(PipelineStageKind::Solver))
        .count();
    let default_roles = choose_roles(&plan.task_kind, std::cmp::max(1, solver_total));
    let mut solver_index = 0usize;
    let mut solver_roles = Vec::new();
    for stage in &mut stages {
        if pipeline_kind_from_str(&stage.kind) != Some(PipelineStageKind::Solver) {
            continue;
        }
        if stage.role_source.trim().eq_ignore_ascii_case("auto") || stage.role.trim().is_empty() {
            let fallback = default_roles.get(solver_index).cloned().unwrap_or(SolverRole {
                solver_id: stage.id.clone(),
                role: "engineering/engineering-senior-developer.md".to_string(),
                angle: ANGLE_SEQUENCE[solver_index % ANGLE_SEQUENCE.len()].to_string(),
            });
            stage.role = fallback.role;
        }
        if stage.angle.trim().is_empty() {
            stage.angle = ANGLE_SEQUENCE[solver_index % ANGLE_SEQUENCE.len()].to_string();
        }
        solver_roles.push(SolverRole {
            solver_id: stage.id.clone(),
            role: stage.role.clone(),
            angle: stage.angle.clone(),
        });
        solver_index += 1;
    }
    plan.pipeline.stages = stages;
    plan.solver_count = solver_roles.len();
    plan.solver_roles = solver_roles;
    Ok(())
}

fn pipeline_stage_spec(
    plan: &Plan,
    run_dir: &Path,
    stage: &str,
) -> Result<PipelineStageSpec, String> {
    pipeline_stage_specs(plan, Some(run_dir))?
        .into_iter()
        .find(|item| item.id == stage)
        .ok_or_else(|| format!("Unknown stage: {stage}"))
}

fn pipeline_stage_kind_for(
    plan: &Plan,
    run_dir: &Path,
    stage: &str,
) -> Result<PipelineStageKind, String> {
    let spec = pipeline_stage_spec(plan, run_dir, stage)?;
    pipeline_kind_from_str(&spec.kind).ok_or_else(|| format!("Unknown stage kind: {}", spec.kind))
}

fn stage_ids_for_kind(
    plan: &Plan,
    run_dir: &Path,
    kind: PipelineStageKind,
) -> Result<Vec<String>, String> {
    Ok(pipeline_stage_specs(plan, Some(run_dir))?
        .into_iter()
        .filter(|item| pipeline_kind_from_str(&item.kind) == Some(kind))
        .map(|item| item.id)
        .collect())
}

fn first_stage_id_for_kind(
    plan: &Plan,
    run_dir: &Path,
    kind: PipelineStageKind,
) -> Result<Option<String>, String> {
    Ok(stage_ids_for_kind(plan, run_dir, kind)?.into_iter().next())
}

fn pipeline_spec_candidates(workspace: &Path) -> Vec<PathBuf> {
    vec![
        workspace.join("agpipe.pipeline.yml"),
        workspace.join("agpipe.pipeline.yaml"),
        workspace.join(".agpipe").join("pipeline.yml"),
        workspace.join(".agpipe").join("pipeline.yaml"),
    ]
}

fn load_pipeline_config(
    workspace: &Path,
    explicit_path: Option<&Path>,
    plan: &Plan,
) -> Result<Option<PipelineConfig>, String> {
    let mut candidates = Vec::new();
    if let Some(path) = explicit_path {
        candidates.push(path.to_path_buf());
    } else {
        candidates.extend(pipeline_spec_candidates(workspace));
    }
    for path in candidates {
        if !path.exists() {
            continue;
        }
        let text = read_text(&path)?;
        let mut config = parse_pipeline_yaml_spec(&text)
            .map_err(|err| format!("Could not parse pipeline YAML {}: {err}", path.display()))?;
        config.source = path.display().to_string();
        config.stages = normalize_pipeline_stage_specs(&config, plan)?;
        return Ok(Some(config));
    }
    Ok(None)
}

fn prior_pipeline_stages(
    plan: &Plan,
    run_dir: &Path,
    stage: &str,
) -> Result<Vec<PipelineStageSpec>, String> {
    let stages = pipeline_stage_specs(plan, Some(run_dir))?;
    let Some(index) = stages.iter().position(|item| item.id == stage) else {
        return Err(format!("Unknown stage: {stage}"));
    };
    Ok(stages.into_iter().take(index).collect())
}

fn stage_resume_bucket_for_run(run_dir: &Path, stage: &str) -> Result<String, String> {
    if matches!(stage, "none" | "rerun") {
        return Ok(stage.to_string());
    }
    let plan = load_plan(run_dir)?;
    let kind = pipeline_stage_kind_for(&plan, run_dir, stage)?;
    Ok(match kind {
        PipelineStageKind::Solver => "solvers".to_string(),
        other => pipeline_kind_label(other).to_string(),
    })
}

fn stage_rank(value: &str) -> usize {
    match value {
        "intake" => 1,
        "solvers" | "solver" => 2,
        "review" => 3,
        "execution" => 4,
        "verification" => 5,
        "rerun" => 6,
        "none" => 7,
        _ => 99,
    }
}

fn until_rank_for_run(run_dir: &Path, until: &str) -> Result<usize, String> {
    let trimmed = until.trim();
    if trimmed.is_empty() {
        return Ok(stage_rank("verification"));
    }
    if stage_rank(trimmed) != 99 {
        return Ok(stage_rank(trimmed));
    }
    stage_resume_bucket_for_run(run_dir, trimmed).map(|bucket| stage_rank(&bucket))
}

fn host_probe_path(run_dir: &Path) -> PathBuf {
    run_dir.join("host").join("probe.json")
}

fn host_probe_history_dir(run_dir: &Path) -> PathBuf {
    run_dir.join("host").join("probes")
}

fn host_probe_history_paths(run_dir: &Path) -> Vec<PathBuf> {
    let history_dir = host_probe_history_dir(run_dir);
    if !history_dir.exists() {
        return Vec::new();
    }
    let mut paths: Vec<PathBuf> = fs::read_dir(history_dir)
        .ok()
        .into_iter()
        .flatten()
        .filter_map(|entry| entry.ok().map(|value| value.path()))
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("json"))
        .collect();
    paths.sort();
    paths
}

fn visible_env_keys() -> Vec<String> {
    let mut keys = Vec::new();
    for prefix in ["LLM_", "TORCH", "PYTORCH", "CUDA", "HF_", "TRANSFORMERS_"] {
        for key in env::vars()
            .map(|(name, _)| name)
            .filter(|name| name.starts_with(prefix))
        {
            keys.push(key);
        }
    }
    keys.sort();
    keys.dedup();
    keys
}

fn load_host_probe(run_dir: &Path) -> Option<HostFacts> {
    let path = host_probe_path(run_dir);
    if !path.exists() {
        return None;
    }
    read_json(&path).ok()
}

fn capture_host_probe(run_dir: &Path) -> Result<HostFacts, String> {
    let mut facts = detect_host_facts("run_stage_local_rust");
    let path = host_probe_path(run_dir);
    let history_dir = host_probe_history_dir(run_dir);
    fs::create_dir_all(&history_dir)
        .map_err(|err| format!("Could not create {}: {err}", history_dir.display()))?;
    let stamp = format!(
        "{}-{:09}",
        run_timestamp(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos()
    );
    let history_path = history_dir.join(format!("{stamp}.json"));
    facts.artifact = Some(path.display().to_string());
    facts.history_artifact = Some(history_path.display().to_string());
    write_json(&history_path, &facts)?;
    write_json(&path, &facts)?;
    Ok(facts)
}

fn normalize_host_string(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        "unknown".to_string()
    } else {
        trimmed.to_string()
    }
}

fn compare_host_string(diffs: &mut Vec<String>, label: &str, planned: &str, observed: &str) {
    let planned = normalize_host_string(planned);
    let observed = normalize_host_string(observed);
    if planned != observed {
        diffs.push(format!("{label}: plan={planned}, probe={observed}"));
    }
}

fn compare_host_bool(diffs: &mut Vec<String>, label: &str, planned: bool, observed: bool) {
    if planned != observed {
        diffs.push(format!("{label}: plan={planned}, probe={observed}"));
    }
}

fn compare_host_option_bool(
    diffs: &mut Vec<String>,
    label: &str,
    planned: Option<bool>,
    observed: Option<bool>,
) {
    let planned = option_bool(planned);
    let observed = option_bool(observed);
    if planned != observed {
        diffs.push(format!("{label}: plan={planned}, probe={observed}"));
    }
}

fn host_drift_details(planned: &HostFacts, observed: &HostFacts) -> Vec<String> {
    let mut diffs = Vec::new();
    compare_host_string(
        &mut diffs,
        "platform",
        &planned.platform,
        &observed.platform,
    );
    compare_host_string(&mut diffs, "machine", &planned.machine, &observed.machine);
    compare_host_bool(
        &mut diffs,
        "apple_silicon",
        planned.apple_silicon,
        observed.apple_silicon,
    );
    compare_host_bool(
        &mut diffs,
        "torch_installed",
        planned.torch_installed,
        observed.torch_installed,
    );
    compare_host_option_bool(
        &mut diffs,
        "cuda_available",
        planned.cuda_available,
        observed.cuda_available,
    );
    compare_host_option_bool(
        &mut diffs,
        "mps_built",
        planned.mps_built,
        observed.mps_built,
    );
    compare_host_option_bool(
        &mut diffs,
        "mps_available",
        planned.mps_available,
        observed.mps_available,
    );
    compare_host_string(
        &mut diffs,
        "preferred_torch_device",
        &planned.preferred_torch_device,
        &observed.preferred_torch_device,
    );
    diffs
}

fn host_drift_message(planned: &HostFacts, observed: &HostFacts) -> Option<String> {
    let diffs = host_drift_details(planned, observed);
    if diffs.is_empty() {
        None
    } else {
        Some(diffs.join("; "))
    }
}

fn solver_ids(plan: &Plan, run_dir: &Path) -> Vec<String> {
    if let Ok(ids) = stage_ids_for_kind(plan, run_dir, PipelineStageKind::Solver) {
        if !ids.is_empty() {
            return ids;
        }
    }
    default_solver_stage_ids(plan, Some(run_dir))
}

fn stage_prompt_path(run_dir: &Path, stage: &str) -> Result<PathBuf, String> {
    let plan = load_plan(run_dir)?;
    let kind = pipeline_stage_kind_for(&plan, run_dir, stage)?;
    let path = match (kind, stage) {
        (PipelineStageKind::Intake, "intake") => run_dir.join("prompts").join("level1-intake.md"),
        (PipelineStageKind::Review, "review") => run_dir.join("prompts").join("level3-review.md"),
        (PipelineStageKind::Execution, "execution") => {
            run_dir.join("prompts").join("level4-execution.md")
        }
        (PipelineStageKind::Verification, "verification") => {
            run_dir.join("prompts").join("level5-verification.md")
        }
        (PipelineStageKind::Intake, _) => {
            run_dir.join("prompts").join(format!("level1-{stage}.md"))
        }
        (PipelineStageKind::Solver, _) => {
            run_dir.join("prompts").join(format!("level2-{stage}.md"))
        }
        (PipelineStageKind::Review, _) => {
            run_dir.join("prompts").join(format!("level3-{stage}.md"))
        }
        (PipelineStageKind::Execution, _) => {
            run_dir.join("prompts").join(format!("level4-{stage}.md"))
        }
        (PipelineStageKind::Verification, _) => {
            run_dir.join("prompts").join(format!("level5-{stage}.md"))
        }
    };
    Ok(path)
}

fn amendments_path(run_dir: &Path) -> PathBuf {
    run_dir.join("amendments.md")
}

fn amendments_exist(run_dir: &Path) -> bool {
    let path = amendments_path(run_dir);
    if !path.exists() {
        return false;
    }
    read_text(&path)
        .map(|content| {
            let trimmed = content.trim();
            !trimmed.is_empty() && !trimmed.starts_with("Pending ")
        })
        .unwrap_or(false)
}

#[allow(clippy::too_many_arguments)]
fn render_request(
    task: &str,
    workspace: &Path,
    workspace_exists: bool,
    task_kind: &str,
    complexity: &str,
    solver_count: usize,
    execution_mode: &str,
    workstream_hints: &[WorkstreamHint],
    summary_language: &str,
    goal_checks: &[GoalCheck],
    intake_research_mode: &str,
    stage_research_mode: &str,
    execution_network_mode: &str,
    cache: &CacheConfig,
    host_facts: &HostFacts,
) -> String {
    let workstream_lines = if workstream_hints.is_empty() {
        "- none".to_string()
    } else {
        workstream_hints
            .iter()
            .map(|item| {
                format!(
                    "- `{}`: {} (role: `{}`)",
                    item.name, item.goal, item.suggested_role
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    };
    let goal_lines = if goal_checks.is_empty() {
        "- none".to_string()
    } else {
        goal_checks
            .iter()
            .map(|item| {
                format!(
                    "- `{}` `{}`: {}",
                    if item.critical {
                        "critical"
                    } else {
                        "supporting"
                    },
                    item.id,
                    item.requirement
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    };
    let warning = if workspace_exists {
        String::new()
    } else {
        "\n## Warning\n\n- Workspace path does not exist. Treat this run as greenfield planning until the path is corrected.\n".to_string()
    };
    format!(
        "# Raw Request\n\n{}\n\n## Environment\n\n- Workspace: `{}`\n- Workspace status: `{}`\n- Task kind guess: `{}`\n- Complexity guess: `{}`\n- Execution mode guess: `{}`\n- Suggested solver count: `{}`\n- User summary language: `{}`\n- Intake research mode: `{}`\n- Stage research mode: `{}`\n- Execution network mode: `{}`\n- Cache policy: `{}`\n- Cache root: `{}`\n- Host facts source: `{}`\n- Host facts captured at: `{}`\n- Host platform: `{}`\n- Host machine: `{}`\n- Apple Silicon: `{}`\n- Torch installed: `{}`\n- CUDA available: `{}`\n- MPS built: `{}`\n- MPS available: `{}`\n- Preferred torch device: `{}`\n\n## Workstream Hints\n\n{}\n\n## Initial Goal Checks\n\n{}{}\n",
        task.trim(),
        workspace.display(),
        if workspace_exists { "present" } else { "missing" },
        task_kind,
        complexity,
        execution_mode,
        solver_count,
        summary_language,
        intake_research_mode,
        stage_research_mode,
        execution_network_mode,
        cache.policy,
        cache.root,
        host_facts.source,
        host_facts.captured_at,
        host_facts.platform,
        host_facts.machine,
        host_facts.apple_silicon,
        host_facts.torch_installed,
        option_bool(host_facts.cuda_available),
        option_bool(host_facts.mps_built),
        option_bool(host_facts.mps_available),
        host_facts.preferred_torch_device,
        workstream_lines,
        goal_lines,
        warning,
    )
}

fn option_bool(value: Option<bool>) -> String {
    value
        .map(|item| item.to_string())
        .unwrap_or_else(|| "null".to_string())
}

fn render_intake_prompt(run_dir: &Path, plan: &Plan) -> String {
    if plan.prompt_format == "compact" {
        return compact_lines(&json!({
            "stage": "intake",
            "mode": "prepare",
            "read": [
                run_dir.join("request.md").display().to_string(),
                run_dir.join("plan.json").display().to_string(),
                DECOMPOSITION_RULES_REF,
                ROLE_MAP_REF
            ],
            "write": [
                run_dir.join("brief.md").display().to_string(),
                run_dir.join("plan.json").display().to_string(),
                run_dir.join("prompts").display().to_string()
            ],
            "defaults": {
                "workspace_exists": plan.workspace_exists,
                "task_kind": plan.task_kind,
                "complexity": plan.complexity,
                "execution_mode": plan.execution_mode,
                "solver_count": plan.solver_count,
                "summary_language": plan.summary_language,
                "intake_research_mode": plan.intake_research_mode,
                "stage_research_mode": plan.stage_research_mode,
                "execution_network_mode": plan.execution_network_mode,
                "cache": cache_config_from_plan(plan),
                "host_facts": plan.host_facts,
                "validation_commands": plan.validation_commands,
                "workstream_hints": plan.workstream_hints,
                "goal_checks": plan.goal_checks,
            },
            "rules": [
                "preserve the original requested outcome as the top-level goal",
                "decompose compound tasks into workstreams instead of silently shrinking the deliverable",
                "refine the goal_checks list so it captures critical user-visible capabilities",
                "follow intake_research_mode when deciding whether to browse before finalizing the brief",
                "treat host_facts from plan.json as authoritative local execution facts",
                "if cache.policy is reuse, consult and update the research cache before duplicating external research",
                "do not implement the solution in this stage"
            ],
            "required_brief_sections": [
                "original requested outcome",
                "objective",
                "deliverable",
                "goal coverage matrix",
                "workstream decomposition",
                "scope",
                "constraints",
                "interim milestone if needed",
                "definition of done",
                "validation expectations",
                "open questions answerable from local context"
            ]
        }));
    }
    format!(
        "# Level 1: Intake And Prompt Builder\n\nRead:\n\n- `{}`\n- `{}`\n- `{}`\n\nProduce or update `brief.md`, `plan.json`, and downstream prompts.\n\nCurrent defaults:\n\n- workspace exists: `{}`\n- task kind: `{}`\n- complexity: `{}`\n- execution mode: `{}`\n- solver count: `{}`\n- user summary language: `{}`\n- intake research mode: `{}`\n- stage research mode: `{}`\n- execution network mode: `{}`\n- cache policy: `{}`\n- cache root: `{}`\n- host facts source: `{}`\n- preferred torch device: `{}`\n- suggested validation:\n{}\n\nInitial goal checks to refine in `plan.json`:\n{}\n\nRules:\n\n- preserve the user's requested outcome as the top-level goal\n- keep the brief execution-ready\n- decompose compound tasks into workstreams instead of shrinking the deliverable\n- update goal checks when critical capabilities are missing\n- treat host_facts in `plan.json` as authoritative local execution facts\n- follow intake_research_mode when deciding whether to browse\n- do not implement the solution in this stage\n",
        run_dir.join("request.md").display(),
        run_dir.join("plan.json").display(),
        ROLE_MAP_REF,
        plan.workspace_exists,
        plan.task_kind,
        plan.complexity,
        plan.execution_mode,
        plan.solver_count,
        plan.summary_language,
        plan.intake_research_mode,
        plan.stage_research_mode,
        plan.execution_network_mode,
        cache_config_from_plan(plan).policy,
        cache_config_from_plan(plan).root,
        plan.host_facts.source,
        plan.host_facts.preferred_torch_device,
        bullet_list(plan.validation_commands.iter().map(|item| item.as_str())),
        bullet_list(plan.goal_checks.iter().map(|item| format!("{} {}: {}", if item.critical { "critical" } else { "supporting" }, item.id, item.requirement)).collect::<Vec<_>>().iter().map(|item| item.as_str()))
    )
}

fn render_solver_prompt(run_dir: &Path, plan: &Plan, solver: &SolverRole) -> String {
    let result_file = run_dir
        .join("solutions")
        .join(&solver.solver_id)
        .join("RESULT.md");
    if plan.prompt_format == "compact" {
        return compact_lines(&json!({
            "stage": solver.solver_id,
            "mode": "solve",
            "role": solver.role,
            "angle": solver.angle,
            "read": [
                run_dir.join("request.md").display().to_string(),
                run_dir.join("brief.md").display().to_string(),
                run_dir.join("plan.json").display().to_string(),
            ],
            "write": [result_file.display().to_string()],
            "stage_research_mode": plan.stage_research_mode,
            "rules": [
                "do not read sibling solver outputs",
                "preserve the full requested system as the top-level goal",
                "if you narrow scope, record it as phase 1 while keeping the preserved goal explicit",
                "follow stage_research_mode when deciding whether to use web research during problem solving",
                "state validation performed or the exact blocker"
            ],
            "deliverables": [
                "assumptions",
                "approach",
                "implementation summary or exact file plan",
                "goal check coverage",
                "workstream coverage",
                "validation performed",
                "unresolved risks"
            ],
            "validation_hints": plan.validation_commands
        }));
    }
    format!(
        "# Level 2: {}\n\nAssigned role: `{}`\nSolution angle: `{}`\n\nRead:\n\n- `{}`\n- `{}`\n- `{}`\n\nDo not read sibling solution files.\n\nDeliver:\n\n- write your solution summary to `{}`\n- include assumptions, approach, implementation summary, goal check coverage, validation performed, and unresolved risks\n\nValidation hints:\n{}\n\nStage research mode:\n\n- `{}`\n",
        solver.solver_id,
        solver.role,
        solver.angle,
        run_dir.join("request.md").display(),
        run_dir.join("brief.md").display(),
        run_dir.join("plan.json").display(),
        result_file.display(),
        bullet_list(plan.validation_commands.iter().map(|item| item.as_str())),
        plan.stage_research_mode
    )
}

fn render_review_prompt(run_dir: &Path, plan: &Plan) -> String {
    let solution_files: Vec<String> = solver_ids(plan, run_dir)
        .into_iter()
        .map(|solver| {
            run_dir
                .join("solutions")
                .join(solver)
                .join("RESULT.md")
                .display()
                .to_string()
        })
        .collect();
    if plan.prompt_format == "compact" {
        return compact_lines(&json!({
            "stage": "review",
            "mode": "compare",
            "read": [
                run_dir.join("request.md").display().to_string(),
                run_dir.join("brief.md").display().to_string(),
                run_dir.join("plan.json").display().to_string(),
                REVIEW_RUBRIC_REF,
                solution_files,
            ],
            "write": [
                run_dir.join("review").join("report.md").display().to_string(),
                run_dir.join("review").join("scorecard.json").display().to_string(),
                run_dir.join("review").join("user-summary.md").display().to_string(),
            ],
            "reviewer_stack": if plan.reviewer_stack.is_empty() { REVIEWER_STACK.iter().map(|item| item.to_string()).collect::<Vec<_>>() } else { plan.reviewer_stack.clone() },
            "user_summary_language": plan.summary_language,
            "stage_research_mode": plan.stage_research_mode,
            "validation_hints": plan.validation_commands,
            "rules": [
                "compare every solution against the brief, not style preference",
                "compare every solution against the plan goal_checks and call out uncovered critical checks",
                "follow stage_research_mode when deciding whether to use web research during review",
                "penalize silent scope reduction",
                "treat missing evidence as a penalty",
                "write a short user-facing summary in the requested language",
                "recommend a hybrid only when the parts are clearly compatible"
            ]
        }));
    }
    format!(
        "# Level 3: Censor And Reviewer\n\nRead:\n\n- `{}`\n- `{}`\n- `{}`\n- `{}`\n- solver outputs:\n{}\n\nReviewer stack:\n{}\n\nStage research mode:\n\n- `{}`\n\nValidation hints:\n{}\n\nDeliver:\n\n- `review/report.md`\n- `review/scorecard.json`\n- `review/user-summary.md`\n",
        run_dir.join("request.md").display(),
        run_dir.join("brief.md").display(),
        run_dir.join("plan.json").display(),
        REVIEW_RUBRIC_REF,
        bullet_list(solution_files.iter().map(|item| item.as_str())),
        bullet_list(plan.reviewer_stack.iter().map(|item| item.as_str())),
        plan.stage_research_mode,
        bullet_list(plan.validation_commands.iter().map(|item| item.as_str()))
    )
}

fn render_execution_prompt(run_dir: &Path, plan: &Plan) -> String {
    let solution_files: Vec<String> = solver_ids(plan, run_dir)
        .into_iter()
        .map(|solver| {
            run_dir
                .join("solutions")
                .join(solver)
                .join("RESULT.md")
                .display()
                .to_string()
        })
        .collect();
    let review_present = first_stage_id_for_kind(plan, run_dir, PipelineStageKind::Review)
        .ok()
        .flatten()
        .is_some();
    let mut read_paths = vec![
        run_dir.join("request.md").display().to_string(),
        run_dir.join("brief.md").display().to_string(),
        run_dir.join("plan.json").display().to_string(),
    ];
    if review_present {
        read_paths.push(
            run_dir
                .join("review")
                .join("report.md")
                .display()
                .to_string(),
        );
        read_paths.push(
            run_dir
                .join("review")
                .join("scorecard.json")
                .display()
                .to_string(),
        );
        read_paths.push(
            run_dir
                .join("review")
                .join("user-summary.md")
                .display()
                .to_string(),
        );
    }
    read_paths.push(format!(
        "solver outputs:\n{}",
        bullet_list(solution_files.iter().map(|item| item.as_str()))
    ));
    if plan.prompt_format == "compact" {
        let mut compact_reads = vec![
            run_dir.join("request.md").display().to_string(),
            run_dir.join("brief.md").display().to_string(),
            run_dir.join("plan.json").display().to_string(),
        ];
        if review_present {
            compact_reads.push(
                run_dir
                    .join("review")
                    .join("report.md")
                    .display()
                    .to_string(),
            );
            compact_reads.push(
                run_dir
                    .join("review")
                    .join("scorecard.json")
                    .display()
                    .to_string(),
            );
            compact_reads.push(
                run_dir
                    .join("review")
                    .join("user-summary.md")
                    .display()
                    .to_string(),
            );
        }
        compact_reads.push(solution_files.clone().join(", "));
        let mut rules = vec![
            "implement the selected plan in the primary workspace".to_string(),
            "treat workspace changes as the main deliverable and execution/report.md as the audit trail".to_string(),
            "follow stage_research_mode when deciding whether to use web research during implementation, debugging, and validation".to_string(),
            "if execution_network_mode is fetch-if-needed, install or download missing dependencies only when genuinely required".to_string(),
            "if cache.policy is reuse, prefer cached downloads, wheels, models, and repos before fetching again".to_string(),
            "record exact install/download commands, sources, versions, and what was fetched".to_string(),
            "respect host_facts.preferred_torch_device from plan.json when running torch-based work".to_string(),
            "treat uncovered critical goal checks as blockers or explicitly deferred work".to_string(),
            "run the cheapest relevant validation after edits and record exact commands and outcomes".to_string(),
            "if blocked, implement the highest-value slice and state the blocker precisely".to_string(),
        ];
        if review_present {
            rules.insert(
                2,
                "follow the review recommendation unless local validation forces a narrower implementation".to_string(),
            );
        } else {
            rules.insert(
                2,
                "there is no review stage in this pipeline, so synthesize the best implementation directly from the brief and solver outputs".to_string(),
            );
        }
        return compact_lines(&json!({
            "stage": "execution",
            "mode": "implement",
            "stage_research_mode": plan.stage_research_mode,
            "read": compact_reads,
            "write": [run_dir.join("execution").join("report.md").display().to_string()],
            "rules": rules,
            "deliverables": [
                "actual workspace changes",
                "execution summary",
                "changed files",
                "validation performed",
                "remaining blockers and next steps"
            ],
            "validation_hints": plan.validation_commands
        }));
    }
    let review_section = if review_present {
        format!(
            "- `{}`\n- `{}`\n- `{}`\n",
            run_dir.join("review").join("report.md").display(),
            run_dir.join("review").join("scorecard.json").display(),
            run_dir.join("review").join("user-summary.md").display()
        )
    } else {
        "- no review stage in this pipeline; synthesize directly from brief and solver outputs\n"
            .to_string()
    };
    let review_rule = if review_present {
        "follow the review recommendation unless validation forces a narrower implementation"
    } else {
        "there is no review stage in this pipeline, so synthesize the best implementation directly from the brief and solver outputs"
    };
    format!(
        "# Level 4: Execution\n\nRead:\n\n- `{}`\n- `{}`\n- `{}`\n{}- relevant solver outputs:\n{}\n\nExecution guidance:\n\n- {}\n\nExecution network mode:\n\n- `{}`\n\nStage research mode:\n\n- `{}`\n\nCache:\n\n- policy: `{}`\n- root: `{}`\n\nDeliver:\n\n- actual code or configuration changes in the workspace\n- `execution/report.md`\n\nValidation hints:\n{}\n",
        run_dir.join("request.md").display(),
        run_dir.join("brief.md").display(),
        run_dir.join("plan.json").display(),
        review_section,
        bullet_list(solution_files.iter().map(|item| item.as_str())),
        review_rule,
        plan.execution_network_mode,
        plan.stage_research_mode,
        cache_config_from_plan(plan).policy,
        cache_config_from_plan(plan).root,
        bullet_list(plan.validation_commands.iter().map(|item| item.as_str()))
    )
}

fn render_verification_prompt(run_dir: &Path, plan: &Plan) -> String {
    let review_present = first_stage_id_for_kind(plan, run_dir, PipelineStageKind::Review)
        .ok()
        .flatten()
        .is_some();
    let execution_present = first_stage_id_for_kind(plan, run_dir, PipelineStageKind::Execution)
        .ok()
        .flatten()
        .is_some();
    if plan.prompt_format == "compact" {
        let mut reads = vec![
            run_dir.join("request.md").display().to_string(),
            run_dir.join("brief.md").display().to_string(),
            run_dir.join("plan.json").display().to_string(),
        ];
        if review_present {
            reads.push(
                run_dir
                    .join("review")
                    .join("report.md")
                    .display()
                    .to_string(),
            );
            reads.push(
                run_dir
                    .join("review")
                    .join("scorecard.json")
                    .display()
                    .to_string(),
            );
            reads.push(
                run_dir
                    .join("review")
                    .join("user-summary.md")
                    .display()
                    .to_string(),
            );
        }
        if execution_present {
            reads.push(
                run_dir
                    .join("execution")
                    .join("report.md")
                    .display()
                    .to_string(),
            );
        }
        reads.push(VERIFICATION_RUBRIC_REF.to_string());
        return compact_lines(&json!({
            "stage": "verification",
            "mode": "audit",
            "read": reads,
            "write": [
                run_dir.join("verification").join("findings.md").display().to_string(),
                run_dir.join("verification").join("user-summary.md").display().to_string(),
                run_dir.join("verification").join("improvement-request.md").display().to_string(),
                run_dir.join("verification").join("augmented-task.md").display().to_string(),
                run_dir.join("verification").join("goal-status.json").display().to_string()
            ],
            "validation_hints": plan.validation_commands,
            "user_summary_language": plan.summary_language,
            "stage_research_mode": plan.stage_research_mode,
            "rules": [
                "review the actual workspace implementation, not only the plans",
                "act in code-review mode: prioritize bugs, regressions, unsafe behavior, and missing validation",
                "start from execution/report.md and the review verdict",
                "run the cheapest relevant checks first and record exact evidence or blockers",
                "write findings ordered by severity with file references when possible",
                "verify device choice against host_facts from plan.json when relevant",
                "set goal_complete=false when any critical plan goal check remains missing, unverified, or replaced by a placeholder implementation",
                "if there are no meaningful findings, say so explicitly"
            ]
        }));
    }
    let review_lines = if review_present {
        format!(
            "- `{}`\n- `{}`\n- `{}`\n",
            run_dir.join("review").join("report.md").display(),
            run_dir.join("review").join("scorecard.json").display(),
            run_dir.join("review").join("user-summary.md").display(),
        )
    } else {
        String::new()
    };
    let execution_line = if execution_present {
        format!(
            "- `{}`\n",
            run_dir.join("execution").join("report.md").display()
        )
    } else {
        String::new()
    };
    format!(
        "# Level 5: Verification And Improvement Seed\n\nRead:\n\n- `{}`\n- `{}`\n- `{}`\n{}{}- `{}`\n\nDeliver:\n\n- `verification/findings.md`\n- `verification/user-summary.md`\n- `verification/goal-status.json`\n- `verification/improvement-request.md`\n- `verification/augmented-task.md`\n\nStage research mode:\n\n- `{}`\n\nValidation hints:\n{}\n",
        run_dir.join("request.md").display(),
        run_dir.join("brief.md").display(),
        run_dir.join("plan.json").display(),
        review_lines,
        execution_line,
        VERIFICATION_RUBRIC_REF,
        plan.stage_research_mode,
        bullet_list(plan.validation_commands.iter().map(|item| item.as_str()))
    )
}

fn ensure_reviewer_stack(plan: &mut Plan) {
    if plan.reviewer_stack.is_empty() {
        plan.reviewer_stack = REVIEWER_STACK.iter().map(|item| item.to_string()).collect();
    }
}

fn infer_goal_checks(
    task: &str,
    task_kind: &str,
    workstream_hints: &[WorkstreamHint],
) -> Vec<GoalCheck> {
    let text = task.to_lowercase();
    let mut checks = Vec::new();
    let mut seen = BTreeSet::new();
    fn add_goal_check(
        seen: &mut BTreeSet<String>,
        checks: &mut Vec<GoalCheck>,
        id: &str,
        requirement: &str,
        critical: bool,
    ) {
        if seen.insert(id.to_string()) {
            checks.push(GoalCheck {
                id: id.to_string(),
                requirement: requirement.to_string(),
                critical,
            });
        }
    }
    if text.contains("telegram") || text.contains("телеграм") {
        add_goal_check(
            &mut seen,
            &mut checks,
            "telegram_ingress",
            "accept task input through Telegram or a clearly equivalent transport path",
            true,
        );
    }
    if ["photo", "image", "фото", "изображ"]
        .iter()
        .any(|word| text.contains(word))
    {
        add_goal_check(
            &mut seen,
            &mut checks,
            "photo_used_as_input",
            "use the provided photo as a real analysis input, not only as a presence check",
            true,
        );
    }
    if [
        "dimension",
        "dimensions",
        "size",
        "sizes",
        "размер",
        "габарит",
    ]
    .iter()
    .any(|word| text.contains(word))
    {
        add_goal_check(
            &mut seen,
            &mut checks,
            "dimension_capture",
            "capture and apply the provided dimensions in the generated plan or model",
            true,
        );
    }
    if [
        "llm",
        "llama",
        "lama",
        "model",
        "vision",
        "analysis",
        "нейросет",
        "модель",
        "дообуч",
        "обуч",
    ]
    .iter()
    .any(|word| text.contains(word))
    {
        add_goal_check(&mut seen, &mut checks, "analysis_adapter", "implement or preserve an analysis path that turns the requested inputs into grounded observations or bounded classifications", true);
    }
    if text.contains("freecad") {
        add_goal_check(
            &mut seen,
            &mut checks,
            "freecad_output",
            "produce deterministic FreeCAD output from the structured plan",
            true,
        );
    }
    if matches!(task_kind, "ai" | "backend" | "fullstack")
        || ["service", "bot", "api", "сервис", "бот", "entrypoint"]
            .iter()
            .any(|word| text.contains(word))
    {
        add_goal_check(
            &mut seen,
            &mut checks,
            "runnable_entrypoint",
            "provide a runnable local entrypoint or service path for the implemented slice",
            true,
        );
    }
    if checks.is_empty() {
        for hint in workstream_hints.iter().take(5) {
            add_goal_check(
                &mut seen,
                &mut checks,
                &slugify(&hint.name).replace('-', "_"),
                &hint.goal,
                true,
            );
        }
    }
    add_goal_check(
        &mut seen,
        &mut checks,
        "validation_and_docs",
        "document and validate the implemented path so a human can run it and assess residual gaps",
        false,
    );
    checks
}

#[allow(clippy::too_many_arguments)]
fn create_run(
    ctx: &Context,
    task_text: &str,
    workspace: &Path,
    output_dir: &Path,
    title: Option<&str>,
    prompt_format: &str,
    summary_language: &str,
    intake_research: &str,
    stage_research: &str,
    execution_network: &str,
    cache_root: &str,
    cache_policy: &str,
    pipeline_file: Option<&Path>,
) -> Result<PathBuf, String> {
    let workspace = workspace.expanduser().resolve()?;
    let workspace_exists = workspace.exists();
    let cache = build_cache_config(&PathBuf::from(cache_root), cache_policy);
    ensure_cache_layout(&cache)?;
    let _ = refresh_cache_index(&cache)?;
    let title = title.map(|value| value.to_string()).unwrap_or_else(|| {
        task_text
            .split_whitespace()
            .take(8)
            .collect::<Vec<_>>()
            .join(" ")
    });
    let task_kind = infer_task_kind(task_text);
    let complexity = infer_complexity(task_text);
    let solver_count = solver_count_for(&complexity);
    let execution_mode = infer_execution_mode(&task_kind, &complexity, task_text);
    let workstream_hints = workstream_hints_for(&task_kind, task_text);
    let goal_checks = infer_goal_checks(task_text, &task_kind, &workstream_hints);
    let token_budget = infer_token_budget(&complexity);
    let stack_signals = detect_stack(&workspace);
    let host_facts = detect_host_facts("init_run_local_rust");
    let validation_commands = build_validation_commands(&workspace, &stack_signals);
    let roles = choose_roles(&task_kind, solver_count);
    let run_dir =
        output_dir
            .expanduser()
            .resolve()?
            .join(format!("{}-{}", run_timestamp(), slugify(&title)));
    fs::create_dir_all(run_dir.join("prompts"))
        .map_err(|err| format!("Could not create run prompts dir: {err}"))?;
    fs::create_dir_all(run_dir.join("solutions"))
        .map_err(|err| format!("Could not create run solutions dir: {err}"))?;
    fs::create_dir_all(run_dir.join("review"))
        .map_err(|err| format!("Could not create run review dir: {err}"))?;
    fs::create_dir_all(run_dir.join("execution"))
        .map_err(|err| format!("Could not create run execution dir: {err}"))?;
    fs::create_dir_all(run_dir.join("verification"))
        .map_err(|err| format!("Could not create run verification dir: {err}"))?;

    let mut plan = Plan {
        created_at: iso_timestamp(),
        workspace: workspace.display().to_string(),
        workspace_exists,
        original_task: task_text.to_string(),
        task_kind,
        complexity,
        execution_mode,
        prompt_format: prompt_format.to_string(),
        summary_language: summary_language.to_string(),
        intake_research_mode: intake_research.to_string(),
        stage_research_mode: stage_research.to_string(),
        execution_network_mode: execution_network.to_string(),
        cache: cache.clone(),
        token_budget,
        host_facts,
        solver_count,
        solver_roles: roles,
        workstream_hints,
        goal_gate_enabled: true,
        augmented_follow_up_enabled: true,
        goal_checks,
        reviewer_stack: REVIEWER_STACK.iter().map(|item| item.to_string()).collect(),
        stack_signals,
        validation_commands,
        references: BTreeMap::from([
            ("role_map".to_string(), ROLE_MAP_REF.to_string()),
            (
                "decomposition_rules".to_string(),
                DECOMPOSITION_RULES_REF.to_string(),
            ),
            ("review_rubric".to_string(), REVIEW_RUBRIC_REF.to_string()),
            (
                "verification_rubric".to_string(),
                VERIFICATION_RUBRIC_REF.to_string(),
            ),
        ]),
        pipeline: PipelineConfig::default(),
    };
    plan.pipeline =
        load_pipeline_config(&workspace, pipeline_file, &plan)?.unwrap_or_else(|| PipelineConfig {
            source: "default".to_string(),
            stages: default_pipeline_stage_specs(&plan, None),
        });
    apply_pipeline_solver_defaults(&mut plan, None)?;
    ensure_reviewer_stack(&mut plan);
    write_text(
        &run_dir.join("request.md"),
        &render_request(
            task_text,
            &workspace,
            workspace_exists,
            &plan.task_kind,
            &plan.complexity,
            plan.solver_count,
            &plan.execution_mode,
            &plan.workstream_hints,
            &plan.summary_language,
            &plan.goal_checks,
            &plan.intake_research_mode,
            &plan.stage_research_mode,
            &plan.execution_network_mode,
            &cache,
            &plan.host_facts,
        ),
    )?;
    write_text(
        &run_dir.join("brief.md"),
        "# Brief\n\nPending intake stage.\n",
    )?;
    save_plan(&run_dir, &plan)?;
    save_token_ledger(
        &run_dir,
        &RunTokenLedger {
            version: 1,
            updated_at: iso_timestamp(),
            budget: plan.token_budget.clone(),
            entries: Vec::new(),
        },
    )?;
    persist_run_backend_config(&run_dir, ctx)?;
    sync_run_artifacts(ctx, &run_dir)?;
    Ok(run_dir)
}

fn render_stage_prompt(run_dir: &Path, stage: &str) -> Result<String, String> {
    let mut plan = load_plan(run_dir)?;
    ensure_reviewer_stack(&mut plan);
    let spec = pipeline_stage_spec(&plan, run_dir, stage)?;
    match pipeline_kind_from_str(&spec.kind) {
        Some(PipelineStageKind::Intake) => Ok(render_intake_prompt(run_dir, &plan)),
        Some(PipelineStageKind::Review) => Ok(render_review_prompt(run_dir, &plan)),
        Some(PipelineStageKind::Execution) => Ok(render_execution_prompt(run_dir, &plan)),
        Some(PipelineStageKind::Verification) => Ok(render_verification_prompt(run_dir, &plan)),
        Some(PipelineStageKind::Solver) => Ok(render_solver_prompt(
            run_dir,
            &plan,
            &SolverRole {
                solver_id: spec.id,
                role: spec.role,
                angle: spec.angle,
            },
        )),
        None => Err(format!("Unknown stage: {stage}")),
    }
}

fn stage_output_paths(plan: &Plan, run_dir: &Path, stage: &str) -> Result<Vec<PathBuf>, String> {
    let outputs = match pipeline_stage_kind_for(plan, run_dir, stage)? {
        PipelineStageKind::Intake => vec![run_dir.join("brief.md"), run_dir.join("plan.json")],
        PipelineStageKind::Review => vec![
            run_dir.join("review").join("report.md"),
            run_dir.join("review").join("scorecard.json"),
            run_dir.join("review").join("user-summary.md"),
        ],
        PipelineStageKind::Execution => vec![run_dir.join("execution").join("report.md")],
        PipelineStageKind::Verification => {
            let mut items = vec![
                run_dir.join("verification").join("findings.md"),
                run_dir.join("verification").join("user-summary.md"),
                run_dir.join("verification").join("improvement-request.md"),
            ];
            if plan.augmented_follow_up_enabled {
                items.push(run_dir.join("verification").join("augmented-task.md"));
            }
            if plan.goal_gate_enabled {
                items.push(run_dir.join("verification").join("goal-status.json"));
            }
            items
        }
        PipelineStageKind::Solver => {
            vec![run_dir.join("solutions").join(stage).join("RESULT.md")]
        }
    };
    Ok(outputs)
}

fn stage_placeholder_content(
    plan: &Plan,
    run_dir: &Path,
    stage: &str,
) -> Result<Vec<(PathBuf, String)>, String> {
    let pairs = match pipeline_stage_kind_for(plan, run_dir, stage)? {
        PipelineStageKind::Intake => vec![(
            run_dir.join("brief.md"),
            "# Brief\n\nPending intake stage.\n".to_string(),
        )],
        PipelineStageKind::Review => vec![
            (
                run_dir.join("review").join("report.md"),
                "# Review Report\n\nPending review stage.\n".to_string(),
            ),
            (
                run_dir.join("review").join("scorecard.json"),
                "{}\n".to_string(),
            ),
            (
                run_dir.join("review").join("user-summary.md"),
                "# User Summary\n\nPending localized review summary.\n".to_string(),
            ),
        ],
        PipelineStageKind::Execution => vec![(
            run_dir.join("execution").join("report.md"),
            "# Execution Report\n\nPending execution stage.\n".to_string(),
        )],
        PipelineStageKind::Verification => {
            let mut items = vec![
                (
                    run_dir.join("verification").join("findings.md"),
                    "# Findings\n\nPending verification stage.\n".to_string(),
                ),
                (
                    run_dir.join("verification").join("user-summary.md"),
                    "# Verification Summary\n\nPending localized verification summary.\n"
                        .to_string(),
                ),
                (
                    run_dir.join("verification").join("improvement-request.md"),
                    "# Improvement Request\n\nPending verification stage.\n".to_string(),
                ),
            ];
            if plan.augmented_follow_up_enabled {
                items.push((
                    run_dir.join("verification").join("augmented-task.md"),
                    "# Augmented Task\n\nPending verification stage.\n".to_string(),
                ));
            }
            if plan.goal_gate_enabled {
                items.push((
                    run_dir.join("verification").join("goal-status.json"),
                    "{}\n".to_string(),
                ));
            }
            items
        }
        PipelineStageKind::Solver => vec![(
            run_dir.join("solutions").join(stage).join("RESULT.md"),
            "# Result\n\nFill this file with the solver output.\n".to_string(),
        )],
    };
    Ok(pairs)
}

fn first_substantive_line(text: &str) -> String {
    for raw in text.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        return line.to_string();
    }
    String::new()
}

fn output_looks_placeholder(stage: &str, text: &str) -> bool {
    let normalized = first_substantive_line(text).to_lowercase();
    if normalized.is_empty() {
        return true;
    }
    let exact = [
        "pending intake stage.",
        "fill this file with the solver output.",
        "pending review stage.",
        "pending localized review summary.",
        "pending execution stage.",
        "pending verification stage.",
        "pending localized verification summary.",
        &format!("pending {stage} stage."),
        &format!("pending {stage} stage"),
    ];
    if exact.iter().any(|item| normalized == *item) {
        return true;
    }
    PLACEHOLDER_PREFIXES
        .iter()
        .any(|prefix| normalized.starts_with(prefix))
}

fn review_scorecard_complete(path: &Path) -> bool {
    read_json::<Value>(path)
        .map(|payload| {
            payload
                .as_object()
                .map(|item| !item.is_empty())
                .unwrap_or(false)
        })
        .unwrap_or(false)
}

fn review_complete_without_summary(run_dir: &Path) -> bool {
    let report_path = run_dir.join("review").join("report.md");
    let scorecard_path = run_dir.join("review").join("scorecard.json");
    if !report_path.exists() || !scorecard_path.exists() {
        return false;
    }
    let Ok(text) = read_text(&report_path) else {
        return false;
    };
    if output_looks_placeholder("review", &text) {
        return false;
    }
    review_scorecard_complete(&scorecard_path)
}

fn goal_status_path(run_dir: &Path) -> PathBuf {
    run_dir.join("verification").join("goal-status.json")
}

fn augmented_task_path(run_dir: &Path) -> PathBuf {
    run_dir.join("verification").join("augmented-task.md")
}

fn goal_status_complete(path: &Path) -> bool {
    read_json::<Value>(path)
        .ok()
        .and_then(|payload| payload.as_object().cloned())
        .map(|payload| {
            payload
                .get("goal_complete")
                .and_then(|value| value.as_bool())
                .is_some()
                && matches!(
                    payload.get("goal_verdict").and_then(|value| value.as_str()),
                    Some("complete" | "partial" | "blocked")
                )
                && payload
                    .get("rerun_recommended")
                    .and_then(|value| value.as_bool())
                    .is_some()
                && payload
                    .get("recommended_next_action")
                    .and_then(|value| value.as_str())
                    .is_some()
        })
        .unwrap_or(false)
}

fn load_goal_status(run_dir: &Path) -> Option<Value> {
    let path = goal_status_path(run_dir);
    if !path.exists() || !goal_status_complete(&path) {
        return None;
    }
    read_json(&path).ok()
}

fn is_stage_complete(run_dir: &Path, stage: &str) -> Result<bool, String> {
    let plan = load_plan(run_dir)?;
    let kind = pipeline_stage_kind_for(&plan, run_dir, stage)?;
    let outputs = stage_output_paths(&plan, run_dir, stage)?;
    if kind == PipelineStageKind::Review {
        let report = run_dir.join("review").join("report.md");
        let scorecard = run_dir.join("review").join("scorecard.json");
        let summary = run_dir.join("review").join("user-summary.md");
        if !report.exists() || !scorecard.exists() {
            return Ok(false);
        }
        if output_looks_placeholder(stage, &read_text(&report).unwrap_or_default()) {
            return Ok(false);
        }
        if !review_scorecard_complete(&scorecard) {
            return Ok(false);
        }
        if summary.exists()
            && output_looks_placeholder("review-summary", &read_text(&summary).unwrap_or_default())
        {
            return Ok(false);
        }
        return Ok(true);
    }
    if kind == PipelineStageKind::Verification {
        let findings = run_dir.join("verification").join("findings.md");
        let summary = run_dir.join("verification").join("user-summary.md");
        let improvement = run_dir.join("verification").join("improvement-request.md");
        if !findings.exists() || !summary.exists() || !improvement.exists() {
            return Ok(false);
        }
        if output_looks_placeholder("verification", &read_text(&findings).unwrap_or_default())
            || output_looks_placeholder(
                "verification-summary",
                &read_text(&summary).unwrap_or_default(),
            )
            || output_looks_placeholder(
                "improvement-request",
                &read_text(&improvement).unwrap_or_default(),
            )
        {
            return Ok(false);
        }
        if plan.augmented_follow_up_enabled {
            let augmented = augmented_task_path(run_dir);
            if !augmented.exists()
                || output_looks_placeholder(
                    "augmented-task",
                    &read_text(&augmented).unwrap_or_default(),
                )
            {
                return Ok(false);
            }
        }
        if plan.goal_gate_enabled {
            let goal = goal_status_path(run_dir);
            if !goal.exists() || !goal_status_complete(&goal) {
                return Ok(false);
            }
        }
        return Ok(true);
    }
    for output in outputs {
        if !output.exists() {
            return Ok(false);
        }
        let text = read_text(&output).unwrap_or_default();
        if output_looks_placeholder(stage, &text) {
            return Ok(false);
        }
    }
    Ok(true)
}

fn available_stages(run_dir: &Path) -> Result<Vec<String>, String> {
    let plan = load_plan(run_dir)?;
    Ok(pipeline_stage_specs(&plan, Some(run_dir))?
        .into_iter()
        .map(|stage| stage.id)
        .collect())
}

fn sync_run_artifacts(ctx: &Context, run_dir: &Path) -> Result<(), String> {
    let mut plan = load_plan(run_dir)?;
    apply_pipeline_solver_defaults(&mut plan, Some(run_dir))?;
    ensure_reviewer_stack(&mut plan);
    let cache = cache_config_from_plan(&plan);
    let pipeline = pipeline_stage_specs(&plan, Some(run_dir))?;
    ensure_cache_layout(&cache)?;
    fs::create_dir_all(run_dir.join("host"))
        .map_err(|err| format!("Could not create host dir: {err}"))?;

    for stage in pipeline {
        let prompt_path = stage_prompt_path(run_dir, &stage.id)?;
        let prompt = render_stage_prompt(run_dir, &stage.id)?;
        write_text(&prompt_path, &prompt)?;
        for (path, content) in stage_placeholder_content(&plan, run_dir, &stage.id)? {
            if path.exists() {
                if path.ends_with("review/user-summary.md")
                    && review_complete_without_summary(run_dir)
                {
                    continue;
                }
                continue;
            }
            write_text(&path, &content)?;
        }
    }
    if !backend_config_path(run_dir).exists() {
        persist_run_backend_config(run_dir, ctx)?;
    }
    save_plan(run_dir, &plan)?;
    let _ = refresh_cache_index(&cache)?;
    Ok(())
}

fn next_stage_for_run(run_dir: &Path) -> Result<Option<String>, String> {
    for stage in available_stages(run_dir)? {
        if !is_stage_complete(run_dir, &stage)? {
            return Ok(Some(stage));
        }
    }
    if let Some(goal_status) = load_goal_status(run_dir) {
        if !goal_status
            .get("goal_complete")
            .and_then(|value| value.as_bool())
            .unwrap_or(false)
        {
            return Ok(Some("rerun".to_string()));
        }
    }
    Ok(None)
}

fn newest_mtime(paths: &[PathBuf]) -> Option<SystemTime> {
    paths
        .iter()
        .filter_map(|path| path.metadata().ok()?.modified().ok())
        .max()
}

fn oldest_mtime(paths: &[PathBuf]) -> Option<SystemTime> {
    paths
        .iter()
        .filter_map(|path| path.metadata().ok()?.modified().ok())
        .min()
}

fn review_is_stale(run_dir: &Path) -> Result<bool, String> {
    let plan = load_plan(run_dir)?;
    let Some(review_stage) = first_stage_id_for_kind(&plan, run_dir, PipelineStageKind::Review)?
    else {
        return Ok(false);
    };
    if !is_stage_complete(run_dir, &review_stage)? {
        return Ok(false);
    }
    let mut upstream = vec![run_dir.join("brief.md")];
    upstream.extend(
        solver_ids(&plan, run_dir)
            .into_iter()
            .map(|solver| run_dir.join("solutions").join(solver).join("RESULT.md")),
    );
    let downstream = vec![
        run_dir.join("review").join("report.md"),
        run_dir.join("review").join("scorecard.json"),
        run_dir.join("review").join("user-summary.md"),
    ];
    Ok(match (newest_mtime(&upstream), oldest_mtime(&downstream)) {
        (Some(upstream_time), Some(downstream_time)) => upstream_time > downstream_time,
        _ => false,
    })
}

fn execution_is_stale(run_dir: &Path) -> Result<bool, String> {
    let plan = load_plan(run_dir)?;
    let Some(execution_stage) =
        first_stage_id_for_kind(&plan, run_dir, PipelineStageKind::Execution)?
    else {
        return Ok(false);
    };
    if !is_stage_complete(run_dir, &execution_stage)? {
        return Ok(false);
    }
    let upstream = vec![
        run_dir.join("review").join("report.md"),
        run_dir.join("review").join("scorecard.json"),
        run_dir.join("review").join("user-summary.md"),
    ];
    let downstream = vec![run_dir.join("execution").join("report.md")];
    Ok(match (newest_mtime(&upstream), oldest_mtime(&downstream)) {
        (Some(upstream_time), Some(downstream_time)) => upstream_time > downstream_time,
        _ => false,
    })
}

fn verification_is_stale(run_dir: &Path) -> Result<bool, String> {
    let plan = load_plan(run_dir)?;
    let Some(verification_stage) =
        first_stage_id_for_kind(&plan, run_dir, PipelineStageKind::Verification)?
    else {
        return Ok(false);
    };
    if !is_stage_complete(run_dir, &verification_stage)? {
        return Ok(false);
    }
    let upstream = vec![
        run_dir.join("execution").join("report.md"),
        host_probe_path(run_dir),
    ];
    let downstream = vec![
        run_dir.join("verification").join("findings.md"),
        run_dir.join("verification").join("user-summary.md"),
        run_dir.join("verification").join("improvement-request.md"),
        goal_status_path(run_dir),
        augmented_task_path(run_dir),
    ];
    Ok(match (newest_mtime(&upstream), oldest_mtime(&downstream)) {
        (Some(upstream_time), Some(downstream_time)) => upstream_time > downstream_time,
        _ => false,
    })
}

fn amendments_require_reintake(run_dir: &Path) -> bool {
    if !amendments_exist(run_dir) {
        return false;
    }
    let amendment = amendments_path(run_dir);
    let brief = run_dir.join("brief.md");
    if !brief.exists() {
        return true;
    }
    match (
        amendment
            .metadata()
            .ok()
            .and_then(|meta| meta.modified().ok()),
        brief.metadata().ok().and_then(|meta| meta.modified().ok()),
    ) {
        (Some(a), Some(b)) => a > b,
        _ => false,
    }
}

fn host_probe_state(run_dir: &Path) -> Result<(String, Option<String>), String> {
    if let Some(host_probe) = load_host_probe(run_dir) {
        let preferred = if host_probe.preferred_torch_device.is_empty() {
            "unknown".to_string()
        } else {
            host_probe.preferred_torch_device.clone()
        };
        let history_count = host_probe_history_paths(run_dir).len();
        let plan = load_plan(run_dir)?;
        return Ok((
            format!("captured ({preferred}, {history_count} history)"),
            host_drift_message(&plan.host_facts, &host_probe),
        ));
    }
    Ok(("missing".to_string(), None))
}

fn stage_status_map(run_dir: &Path) -> Result<BTreeMap<String, String>, String> {
    let mut map = BTreeMap::new();
    for stage in available_stages(run_dir)? {
        map.insert(
            stage.clone(),
            if is_stage_complete(run_dir, &stage)? {
                "done".to_string()
            } else {
                "pending".to_string()
            },
        );
    }
    Ok(map)
}

fn goal_state(run_dir: &Path) -> Result<String, String> {
    if let Some(goal_status) = load_goal_status(run_dir) {
        return Ok(
            if goal_status
                .get("goal_complete")
                .and_then(|value| value.as_bool())
                .unwrap_or(false)
            {
                "complete".to_string()
            } else {
                goal_status
                    .get("goal_verdict")
                    .and_then(|value| value.as_str())
                    .unwrap_or("partial")
                    .to_string()
            },
        );
    }
    let plan = load_plan(run_dir)?;
    Ok(if plan.goal_gate_enabled {
        "pending-verification".to_string()
    } else {
        "n/a".to_string()
    })
}

fn safe_next_action_for_run(run_dir: &Path) -> Result<String, String> {
    let statuses = stage_status_map(run_dir)?;
    let plan = load_plan(run_dir)?;
    let solver_ids = solver_ids(&plan, run_dir);
    let review_stage = first_stage_id_for_kind(&plan, run_dir, PipelineStageKind::Review)?;
    let execution_stage = first_stage_id_for_kind(&plan, run_dir, PipelineStageKind::Execution)?;
    let verification_stage =
        first_stage_id_for_kind(&plan, run_dir, PipelineStageKind::Verification)?;
    if amendments_require_reintake(run_dir) {
        return Ok("step-back intake".to_string());
    }
    if execution_stage.is_some()
        && verification_stage
            .as_ref()
            .and_then(|stage| statuses.get(stage))
            .map(String::as_str)
            == Some("done")
        && execution_stage
            .as_ref()
            .and_then(|stage| statuses.get(stage))
            .map(String::as_str)
            != Some("done")
    {
        return Ok(format!(
            "step-back {}",
            verification_stage.unwrap_or_else(|| "verification".to_string())
        ));
    }
    if review_stage.is_some()
        && execution_stage
            .as_ref()
            .and_then(|stage| statuses.get(stage))
            .map(String::as_str)
            == Some("done")
        && review_stage
            .as_ref()
            .and_then(|stage| statuses.get(stage))
            .map(String::as_str)
            != Some("done")
    {
        return Ok(format!(
            "step-back {}",
            execution_stage.unwrap_or_else(|| "execution".to_string())
        ));
    }
    if review_stage
        .as_ref()
        .and_then(|stage| statuses.get(stage))
        .map(String::as_str)
        == Some("done")
        && solver_ids
            .iter()
            .any(|solver| statuses.get(solver).map(String::as_str) != Some("done"))
    {
        return Ok(format!(
            "step-back {}",
            review_stage.unwrap_or_else(|| "review".to_string())
        ));
    }
    if verification_is_stale(run_dir)? {
        return Ok(format!(
            "recheck {}",
            verification_stage.unwrap_or_else(|| "verification".to_string())
        ));
    }
    if execution_is_stale(run_dir)? {
        return Ok(format!(
            "step-back {}",
            execution_stage.unwrap_or_else(|| "execution".to_string())
        ));
    }
    if review_is_stale(run_dir)? {
        return Ok(format!(
            "step-back {}",
            review_stage.unwrap_or_else(|| "review".to_string())
        ));
    }
    match next_stage_for_run(run_dir)? {
        None => Ok("none".to_string()),
        Some(stage) if stage == "rerun" => Ok("rerun".to_string()),
        Some(stage) if stage_resume_bucket_for_run(run_dir, &stage)? == "solvers" => {
            Ok("start-solvers".to_string())
        }
        Some(stage) => Ok(format!("start {stage}")),
    }
}

fn doctor_payload(ctx: &Context, run_dir: &Path) -> Result<DoctorPayload, String> {
    let statuses = stage_status_map(run_dir)?;
    let mut issues = Vec::new();
    let mut warnings = Vec::new();
    let mut stale = Vec::new();
    let plan = load_plan(run_dir)?;
    let solver_ids = solver_ids(&plan, run_dir);
    let review_stage = first_stage_id_for_kind(&plan, run_dir, PipelineStageKind::Review)?;
    let execution_stage = first_stage_id_for_kind(&plan, run_dir, PipelineStageKind::Execution)?;
    let verification_stage =
        first_stage_id_for_kind(&plan, run_dir, PipelineStageKind::Verification)?;
    let backend_config =
        load_run_backend_config(run_dir).unwrap_or_else(|| context_backend_config(ctx));

    if matches!(
        backend_config.stage0_backend.trim().to_lowercase().as_str(),
        "responses" | "openai" | "responses-api"
    ) || matches!(
        backend_config.stage_backend.trim().to_lowercase().as_str(),
        "responses" | "responses-readonly" | "mixed" | "openai" | "responses-api"
    ) {
        if backend_config.openai_background {
            warnings.push(DoctorIssue {
                severity: "warn".to_string(),
                message: "Responses background mode is enabled; this stores response state and is not ZDR-compatible.".to_string(),
                fix: "Set `AGPIPE_OPENAI_BACKGROUND=0` for stricter privacy, or keep it enabled only when asynchronous runs are worth the storage tradeoff.".to_string(),
            });
        } else if backend_config.openai_store {
            warnings.push(DoctorIssue {
                severity: "warn".to_string(),
                message: "Responses request storage is enabled even though background mode is off.".to_string(),
                fix: "Set `AGPIPE_OPENAI_STORE=0` unless you explicitly need stored Responses state for this run.".to_string(),
            });
        }
    }

    if amendments_require_reintake(run_dir) {
        stale.push(
            first_stage_id_for_kind(&plan, run_dir, PipelineStageKind::Intake)?
                .unwrap_or_else(|| "intake".to_string()),
        );
        warnings.push(DoctorIssue {
            severity: "warn".to_string(),
            message: "New amendments are newer than the current brief and stage outputs.".to_string(),
            fix: "Run `step-back intake` and continue from intake so the new user correction affects the brief and downstream stages.".to_string(),
        });
    }
    if execution_stage.is_some()
        && verification_stage
            .as_ref()
            .and_then(|stage| statuses.get(stage))
            .map(String::as_str)
            == Some("done")
        && execution_stage
            .as_ref()
            .and_then(|stage| statuses.get(stage))
            .map(String::as_str)
            != Some("done")
    {
        issues.push(DoctorIssue {
            severity: "error".to_string(),
            message: "Verification is marked done while execution is pending.".to_string(),
            fix: "Run `step-back verification` or complete execution before trusting verification findings.".to_string(),
        });
    }
    if review_stage.is_some()
        && execution_stage
            .as_ref()
            .and_then(|stage| statuses.get(stage))
            .map(String::as_str)
            == Some("done")
        && review_stage
            .as_ref()
            .and_then(|stage| statuses.get(stage))
            .map(String::as_str)
            != Some("done")
    {
        issues.push(DoctorIssue {
            severity: "error".to_string(),
            message: "Execution is marked done while review is pending.".to_string(),
            fix: "Run `step-back execution` or complete review before trusting execution evidence."
                .to_string(),
        });
    }
    if review_stage
        .as_ref()
        .and_then(|stage| statuses.get(stage))
        .map(String::as_str)
        == Some("done")
    {
        let stale_solvers: Vec<String> = solver_ids
            .iter()
            .filter(|solver| statuses.get(*solver).map(String::as_str) != Some("done"))
            .cloned()
            .collect();
        if !stale_solvers.is_empty() {
            issues.push(DoctorIssue {
                severity: "error".to_string(),
                message: "Review is marked done while some solver stages are pending.".to_string(),
                fix: format!(
                    "Run `step-back review` or complete the missing solver stages: {}.",
                    stale_solvers.join(", ")
                ),
            });
        }
    }
    if review_is_stale(run_dir)? {
        stale.push(review_stage.unwrap_or_else(|| "review".to_string()));
        warnings.push(DoctorIssue {
            severity: "warn".to_string(),
            message: "Review artifacts are older than the latest solver outputs or brief.".to_string(),
            fix: "Run `step-back review` and repeat review so the verdict matches the current solver state.".to_string(),
        });
    }
    if execution_is_stale(run_dir)? {
        stale.push(
            execution_stage
                .clone()
                .unwrap_or_else(|| "execution".to_string()),
        );
        warnings.push(DoctorIssue {
            severity: "warn".to_string(),
            message: "Execution artifacts are older than the latest review artifacts.".to_string(),
            fix: "Run `step-back execution` and repeat execution before trusting verification or rerun guidance.".to_string(),
        });
    }
    if verification_is_stale(run_dir)? {
        stale.push(
            verification_stage
                .clone()
                .unwrap_or_else(|| "verification".to_string()),
        );
        warnings.push(DoctorIssue {
            severity: "warn".to_string(),
            message: "Verification artifacts are older than the latest execution evidence or host probe.".to_string(),
            fix: "Run `recheck verification` and repeat verification to refresh findings and goal status.".to_string(),
        });
    }
    let (host_probe_label, host_drift) = host_probe_state(run_dir)?;
    if host_probe_label == "missing"
        && (execution_stage
            .as_ref()
            .and_then(|stage| statuses.get(stage))
            .map(String::as_str)
            == Some("done")
            || verification_stage
                .as_ref()
                .and_then(|stage| statuses.get(stage))
                .map(String::as_str)
                == Some("done"))
    {
        warnings.push(DoctorIssue {
            severity: "warn".to_string(),
            message: "No launcher-side host probe artifact is present for a run that already reached execution or verification.".to_string(),
            fix: "Run `host-probe --refresh` before repeating device-sensitive stages.".to_string(),
        });
    }
    if let Some(drift) = &host_drift {
        warnings.push(DoctorIssue {
            severity: "warn".to_string(),
            message: format!("Host drift detected: {drift}."),
            fix: "Treat launcher probe as authoritative and rerun device-sensitive stages from the same host environment.".to_string(),
        });
    }
    let goal = goal_state(run_dir)?;
    if matches!(goal.as_str(), "partial" | "blocked")
        && next_stage_for_run(run_dir)?.as_deref() != Some("rerun")
    {
        warnings.push(DoctorIssue {
            severity: "warn".to_string(),
            message: format!("Goal state is `{goal}` but next stage is not `rerun`."),
            fix: "Re-run verification or inspect goal-status.json for stale artifacts.".to_string(),
        });
    }
    if plan.augmented_follow_up_enabled
        && verification_stage
            .as_ref()
            .and_then(|stage| statuses.get(stage))
            .map(String::as_str)
            == Some("done")
    {
        let augmented = augmented_task_path(run_dir);
        if !augmented.exists()
            || output_looks_placeholder(
                "augmented-task",
                &read_text(&augmented).unwrap_or_default(),
            )
        {
            warnings.push(DoctorIssue {
                severity: "warn".to_string(),
                message: "Verification completed without a substantive augmented follow-up task."
                    .to_string(),
                fix: "Recheck verification so follow-up reruns preserve the full verified context."
                    .to_string(),
            });
        }
    }
    let health = if !issues.is_empty() {
        "broken"
    } else if !warnings.is_empty() {
        "warning"
    } else {
        "healthy"
    };
    Ok(DoctorPayload {
        run_dir: run_dir.display().to_string(),
        health: health.to_string(),
        stages: statuses,
        stale,
        host_probe: host_probe_label,
        host_drift,
        goal,
        next: next_stage_for_run(run_dir)?.unwrap_or_else(|| "none".to_string()),
        safe_next_action: safe_next_action_for_run(run_dir)?,
        issues,
        warnings,
    })
}

fn status_payload(run_dir: &Path) -> Result<StatusPayload, String> {
    let (host_probe, host_drift) = host_probe_state(run_dir)?;
    Ok(StatusPayload {
        run_dir: run_dir.display().to_string(),
        stages: stage_status_map(run_dir)?,
        host_probe,
        host_drift,
        goal: goal_state(run_dir)?,
        next: next_stage_for_run(run_dir)?.unwrap_or_else(|| "none".to_string()),
    })
}

pub fn load_run_snapshots(
    ctx: &Context,
    root: &Path,
    limit: usize,
) -> Result<Vec<RunSnapshot>, String> {
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
                        fix: "Inspect the run directory and Rust engine.".to_string(),
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
                let (log_title, log_lines) =
                    contextual_log_excerpt(&run_dir, None, Some(&status.next), 12);
                snapshots.push(RunSnapshot {
                    run_dir,
                    doctor,
                    status,
                    token_summary: RunTokenSummary::default(),
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
    let plan = load_plan(run_dir)?;
    let token_summary = summarize_token_ledger(run_dir, &plan)?;
    let (preview_label, preview) = preview_text(run_dir, 2400);
    let (log_title, log_lines) = contextual_log_excerpt(run_dir, None, Some(&status.next), 12);
    Ok(RunSnapshot {
        run_dir: run_dir.to_path_buf(),
        doctor,
        status,
        token_summary,
        preview_label,
        preview,
        log_title,
        log_lines,
    })
}

pub fn doctor_report(ctx: &Context, run_dir: &Path) -> Result<DoctorPayload, String> {
    sync_run_artifacts(ctx, run_dir)?;
    doctor_payload(ctx, run_dir)
}

pub fn status_report(_ctx: &Context, run_dir: &Path) -> Result<StatusPayload, String> {
    sync_run_artifacts(_ctx, run_dir)?;
    status_payload(run_dir)
}

pub fn next_stage(_ctx: &Context, run_dir: &Path) -> Result<String, String> {
    sync_run_artifacts(_ctx, run_dir)?;
    Ok(next_stage_for_run(run_dir)?.unwrap_or_else(|| "none".to_string()))
}

fn discover_agency_agents_dir(ctx: &Context) -> Option<PathBuf> {
    if let Ok(path) = env::var("AGENCY_AGENTS_DIR") {
        let candidate = PathBuf::from(path);
        if candidate.exists() {
            return Some(candidate);
        }
    }
    if let Ok(home) = env::var("HOME") {
        let home_catalog = PathBuf::from(home).join("agency-agents");
        if home_catalog.exists() {
            return Some(home_catalog);
        }
    }
    let parent = ctx.repo_root.parent()?;
    let sibling = parent.parent().unwrap_or(parent).join("agency-agents");
    if sibling.exists() {
        Some(sibling)
    } else {
        None
    }
}

fn compile_prompt(ctx: &Context, run_dir: &Path, stage: &str) -> Result<String, String> {
    let plan = load_plan(run_dir)?;
    let stage_kind = pipeline_stage_kind_for(&plan, run_dir, stage)?;
    let workspace = plan_workspace(&plan);
    let prompt_body = read_text(&stage_prompt_path(run_dir, stage)?)?;
    let outputs = stage_output_paths(&plan, run_dir, stage)?
        .into_iter()
        .map(|path| format!("- `{}`", path.display()))
        .collect::<Vec<_>>()
        .join("\n");

    let mut guidance = vec![
        format!("- Run directory: `{}`", run_dir.display()),
        format!("- Primary workspace: `{}`", workspace.display()),
        format!("- Prompt format: `{}`", plan.prompt_format),
        format!("- Intake research mode: `{}`", plan.intake_research_mode),
        format!("- Stage research mode: `{}`", plan.stage_research_mode),
        format!(
            "- Execution network mode: `{}`",
            plan.execution_network_mode
        ),
        format!("- Cache policy: `{}`", cache_config_from_plan(&plan).policy),
        format!("- Cache root: `{}`", cache_config_from_plan(&plan).root),
        format!(
            "- Role map reference: `{}`",
            ctx.repo_root.join(ROLE_MAP_REF).display()
        ),
        format!(
            "- Review rubric reference: `{}`",
            ctx.repo_root.join(REVIEW_RUBRIC_REF).display()
        ),
        format!(
            "- Verification rubric reference: `{}`",
            ctx.repo_root.join(VERIFICATION_RUBRIC_REF).display()
        ),
    ];
    if let Some(agency) = discover_agency_agents_dir(ctx) {
        guidance.push(format!("- Agency role catalog: `{}`", agency.display()));
    }

    let mut extra_rules = vec![
        "- Update the requested artifacts directly on disk.".to_string(),
        "- Use the primary workspace for repo inspection when it exists.".to_string(),
        "- If blocked, replace placeholders with a concrete blocker note instead of leaving them unchanged.".to_string(),
    ];
    if amendments_exist(run_dir) {
        extra_rules.push("- Treat `amendments.md` as the latest authoritative user input when it adds constraints, corrections, or newly clarified expected behavior.".to_string());
    }
    if stage_kind == PipelineStageKind::Solver {
        extra_rules.push("- Do not read sibling solver outputs.".to_string());
    }
    if matches!(
        stage_kind,
        PipelineStageKind::Execution | PipelineStageKind::Verification
    ) {
        extra_rules.push("- Treat the latest host probe artifact from the launcher as authoritative local runtime evidence for device availability and visible environment keys.".to_string());
    }

    let mut dynamic_context = Vec::new();
    if !plan.goal_checks.is_empty() {
        dynamic_context.push("Goal checks from current plan:".to_string());
        dynamic_context.extend(plan.goal_checks.iter().map(|item| {
            format!(
                "- `{}` `{}`: {}",
                if item.critical {
                    "critical"
                } else {
                    "supporting"
                },
                item.id,
                item.requirement
            )
        }));
    }
    if !plan.host_facts.source.is_empty() {
        dynamic_context.push("Host facts from current plan:".to_string());
        for (key, value) in [
            ("source", plan.host_facts.source.clone()),
            ("captured_at", plan.host_facts.captured_at.clone()),
            ("platform", plan.host_facts.platform.clone()),
            ("machine", plan.host_facts.machine.clone()),
            ("apple_silicon", plan.host_facts.apple_silicon.to_string()),
            (
                "torch_installed",
                plan.host_facts.torch_installed.to_string(),
            ),
            (
                "cuda_available",
                option_bool(plan.host_facts.cuda_available),
            ),
            ("mps_built", option_bool(plan.host_facts.mps_built)),
            ("mps_available", option_bool(plan.host_facts.mps_available)),
            (
                "preferred_torch_device",
                plan.host_facts.preferred_torch_device.clone(),
            ),
        ] {
            if !value.is_empty() {
                dynamic_context.push(format!("- `{key}`: `{value}`"));
            }
        }
    }
    if let Some(host_probe) = load_host_probe(run_dir) {
        dynamic_context.push("Latest host probe from launcher:".to_string());
        dynamic_context.push(format!(
            "- `artifact`: `{}`",
            host_probe_path(run_dir).display()
        ));
        for (key, value) in [
            ("source", host_probe.source.clone()),
            ("captured_at", host_probe.captured_at.clone()),
            ("platform", host_probe.platform.clone()),
            ("machine", host_probe.machine.clone()),
            ("apple_silicon", host_probe.apple_silicon.to_string()),
            ("torch_installed", host_probe.torch_installed.to_string()),
            ("cuda_available", option_bool(host_probe.cuda_available)),
            ("mps_built", option_bool(host_probe.mps_built)),
            ("mps_available", option_bool(host_probe.mps_available)),
            (
                "preferred_torch_device",
                host_probe.preferred_torch_device.clone(),
            ),
        ] {
            if !value.is_empty() {
                dynamic_context.push(format!("- `{key}`: `{value}`"));
            }
        }
        if !host_probe.visible_env_keys.is_empty() {
            dynamic_context.push(format!(
                "- `visible_env_keys`: {}",
                host_probe
                    .visible_env_keys
                    .iter()
                    .map(|key| format!("`{key}`"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        if let Some(drift) = host_drift_message(&plan.host_facts, &host_probe) {
            dynamic_context.push(format!("- `host_drift`: {drift}"));
        }
    }
    let cache = cache_config_from_plan(&plan);
    if cache.enabled {
        dynamic_context.push("Shared cache paths:".to_string());
        for (name, path) in cache.paths {
            dynamic_context.push(format!("- `{name}`: `{path}`"));
        }
    }
    if stage_kind == PipelineStageKind::Solver {
        if let Ok(spec) = pipeline_stage_spec(&plan, run_dir, stage) {
            dynamic_context.push(format!("- Solver role from current plan: `{}`", spec.role));
            dynamic_context.push(format!(
                "- Solver angle from current plan: `{}`",
                spec.angle
            ));
        }
    }
    if stage_kind == PipelineStageKind::Review {
        let outputs: Vec<String> = solver_ids(&plan, run_dir)
            .into_iter()
            .map(|solver| {
                run_dir
                    .join("solutions")
                    .join(solver)
                    .join("RESULT.md")
                    .display()
                    .to_string()
            })
            .collect();
        if !outputs.is_empty() {
            dynamic_context.push("Current solver outputs from plan:".to_string());
            dynamic_context.extend(outputs.into_iter().map(|path| format!("- `{path}`")));
        }
    }
    if stage_kind == PipelineStageKind::Execution {
        if first_stage_id_for_kind(&plan, run_dir, PipelineStageKind::Review)?.is_some() {
            let scorecard_path = run_dir.join("review").join("scorecard.json");
            if review_scorecard_complete(&scorecard_path) {
                if let Ok(scorecard) = read_json::<Value>(&scorecard_path) {
                    if let Some(winner) = scorecard.get("winner").and_then(|value| value.as_str()) {
                        dynamic_context.push(format!("- Review winner from scorecard: `{winner}`"));
                    }
                    if let Some(backup) = scorecard.get("backup").and_then(|value| value.as_str()) {
                        dynamic_context.push(format!("- Review backup from scorecard: `{backup}`"));
                    }
                }
            }
            dynamic_context.push(format!(
                "- Review report: `{}`",
                run_dir.join("review").join("report.md").display()
            ));
            dynamic_context.push(format!(
                "- User-facing summary: `{}`",
                run_dir.join("review").join("user-summary.md").display()
            ));
        } else {
            dynamic_context.push(
                "- No review stage is configured in this pipeline; execution will synthesize directly from the brief and solver outputs.".to_string(),
            );
        }
    }
    if stage_kind == PipelineStageKind::Verification {
        if first_stage_id_for_kind(&plan, run_dir, PipelineStageKind::Execution)?.is_some() {
            dynamic_context.push(format!(
                "- Execution report: `{}`",
                run_dir.join("execution").join("report.md").display()
            ));
        }
        dynamic_context.push(format!(
            "- Verification findings output: `{}`",
            run_dir.join("verification").join("findings.md").display()
        ));
        dynamic_context.push(format!(
            "- Goal status output: `{}`",
            goal_status_path(run_dir).display()
        ));
        dynamic_context.push(format!(
            "- Improvement request output: `{}`",
            run_dir
                .join("verification")
                .join("improvement-request.md")
                .display()
        ));
        dynamic_context.push(format!(
            "- Augmented task output: `{}`",
            augmented_task_path(run_dir).display()
        ));
    }
    if amendments_exist(run_dir) {
        dynamic_context.push("Latest user amendments:".to_string());
        dynamic_context.push(format!(
            "- `artifact`: `{}`",
            amendments_path(run_dir).display()
        ));
    }

    Ok(format!(
        "You are executing stage `{}` of a multi-agent pipeline.\n\nExecution context:\n{}\n\n{}Required output files:\n{}\n\nGlobal rules:\n{}\n\nStage prompt:\n\n{}\n",
        stage,
        guidance.join("\n"),
        if dynamic_context.is_empty() { String::new() } else { format!("Dynamic stage context:\n{}\n\n", dynamic_context.join("\n")) },
        outputs,
        extra_rules.join("\n"),
        prompt_body.trim_end(),
    ))
}

fn detect_local_template(plan: &Plan) -> Option<LocalTemplateKind> {
    let text = plan.original_task.to_lowercase();
    let is_hello_world = text.contains("hello world")
        || text.contains("hello-world")
        || text.contains("hello_world")
        || text.contains("hellow world")
        || text.contains("привет мир");
    let is_python = text.contains("python")
        || text.contains("питон")
        || text.contains("python3")
        || text.contains(".py");
    if is_hello_world && is_python {
        Some(LocalTemplateKind::HelloWorldPython)
    } else {
        None
    }
}

fn stage_backend_kind(
    ctx: &Context,
    run_dir: &Path,
    stage: &str,
) -> Result<StageBackendKind, String> {
    let plan = load_plan(run_dir)?;
    let kind = pipeline_stage_kind_for(&plan, run_dir, stage)?;
    if let Some(template) = detect_local_template(&plan) {
        return Ok(StageBackendKind::LocalTemplate(template));
    }
    let configured = ctx.stage_backend.trim().to_lowercase();
    if matches!(
        configured.as_str(),
        "responses" | "responses-readonly" | "mixed"
    ) && kind != PipelineStageKind::Execution
    {
        Ok(StageBackendKind::Responses)
    } else {
        Ok(StageBackendKind::Codex)
    }
}

fn backend_reads_workspace(
    plan: &Plan,
    run_dir: &Path,
    backend: StageBackendKind,
    stage: &str,
) -> Result<bool, String> {
    Ok(match backend {
        StageBackendKind::Responses => {
            pipeline_stage_kind_for(plan, run_dir, stage)? == PipelineStageKind::Verification
        }
        _ => true,
    })
}

fn read_reference_asset(ctx: &Context, relative: &str) -> Result<String, String> {
    let path = ctx.repo_root.join(relative);
    if path.exists() {
        return read_text(&path);
    }
    match relative {
        ROLE_MAP_REF => Ok(EMBEDDED_ROLE_MAP.to_string()),
        REVIEW_RUBRIC_REF => Ok(EMBEDDED_REVIEW_RUBRIC.to_string()),
        VERIFICATION_RUBRIC_REF => Ok(EMBEDDED_VERIFICATION_RUBRIC.to_string()),
        DECOMPOSITION_RULES_REF => Ok(EMBEDDED_DECOMPOSITION_RULES.to_string()),
        _ => Err(format!("Unknown embedded reference asset: {relative}")),
    }
}

fn stage_context_documents(
    ctx: &Context,
    run_dir: &Path,
    stage: &str,
) -> Result<Vec<(String, String)>, String> {
    let plan = load_plan(run_dir)?;
    let kind = pipeline_stage_kind_for(&plan, run_dir, stage)?;
    let mut docs = Vec::new();
    match kind {
        PipelineStageKind::Intake => {
            docs.push((
                "request.md".to_string(),
                read_text(&run_dir.join("request.md"))?,
            ));
            docs.push((
                "plan.json".to_string(),
                read_text(&run_dir.join("plan.json"))?,
            ));
            if amendments_exist(run_dir) {
                docs.push((
                    "amendments.md".to_string(),
                    read_text(&amendments_path(run_dir))?,
                ));
            }
            docs.push((
                "decomposition-rules.md".to_string(),
                read_reference_asset(ctx, DECOMPOSITION_RULES_REF)?,
            ));
            docs.push((
                "agency-role-map.md".to_string(),
                read_reference_asset(ctx, ROLE_MAP_REF)?,
            ));
        }
        PipelineStageKind::Solver => {
            docs.push((
                "request.md".to_string(),
                read_text(&run_dir.join("request.md"))?,
            ));
            docs.push((
                "brief.md".to_string(),
                read_text(&run_dir.join("brief.md"))?,
            ));
            docs.push((
                "plan.json".to_string(),
                read_text(&run_dir.join("plan.json"))?,
            ));
        }
        PipelineStageKind::Review => {
            docs.push((
                "request.md".to_string(),
                read_text(&run_dir.join("request.md"))?,
            ));
            docs.push((
                "brief.md".to_string(),
                read_text(&run_dir.join("brief.md"))?,
            ));
            docs.push((
                "plan.json".to_string(),
                read_text(&run_dir.join("plan.json"))?,
            ));
            docs.push((
                "review-rubric.md".to_string(),
                read_reference_asset(ctx, REVIEW_RUBRIC_REF)?,
            ));
            for solver in solver_ids(&plan, run_dir) {
                docs.push((
                    format!("solutions/{solver}/RESULT.md"),
                    read_text(&run_dir.join("solutions").join(&solver).join("RESULT.md"))?,
                ));
            }
        }
        PipelineStageKind::Execution => {
            docs.push((
                "request.md".to_string(),
                read_text(&run_dir.join("request.md"))?,
            ));
            docs.push((
                "brief.md".to_string(),
                read_text(&run_dir.join("brief.md"))?,
            ));
            docs.push((
                "plan.json".to_string(),
                read_text(&run_dir.join("plan.json"))?,
            ));
            if first_stage_id_for_kind(&plan, run_dir, PipelineStageKind::Review)?.is_some() {
                docs.push((
                    "review/report.md".to_string(),
                    read_text(&run_dir.join("review").join("report.md"))?,
                ));
                docs.push((
                    "review/scorecard.json".to_string(),
                    read_text(&run_dir.join("review").join("scorecard.json"))?,
                ));
                docs.push((
                    "review/user-summary.md".to_string(),
                    read_text(&run_dir.join("review").join("user-summary.md"))?,
                ));
            }
            for solver in solver_ids(&plan, run_dir) {
                docs.push((
                    format!("solutions/{solver}/RESULT.md"),
                    read_text(&run_dir.join("solutions").join(&solver).join("RESULT.md"))?,
                ));
            }
        }
        PipelineStageKind::Verification => {
            docs.push((
                "request.md".to_string(),
                read_text(&run_dir.join("request.md"))?,
            ));
            docs.push((
                "brief.md".to_string(),
                read_text(&run_dir.join("brief.md"))?,
            ));
            docs.push((
                "plan.json".to_string(),
                read_text(&run_dir.join("plan.json"))?,
            ));
            docs.extend(verification_workspace_documents(&working_root(
                &plan, run_dir,
            ))?);
            if first_stage_id_for_kind(&plan, run_dir, PipelineStageKind::Review)?.is_some() {
                docs.push((
                    "review/report.md".to_string(),
                    read_text(&run_dir.join("review").join("report.md"))?,
                ));
                docs.push((
                    "review/scorecard.json".to_string(),
                    read_text(&run_dir.join("review").join("scorecard.json"))?,
                ));
                docs.push((
                    "review/user-summary.md".to_string(),
                    read_text(&run_dir.join("review").join("user-summary.md"))?,
                ));
            }
            if first_stage_id_for_kind(&plan, run_dir, PipelineStageKind::Execution)?.is_some() {
                docs.push((
                    "execution/report.md".to_string(),
                    read_text(&run_dir.join("execution").join("report.md"))?,
                ));
            }
            let host_probe = host_probe_path(run_dir);
            if host_probe.exists() {
                docs.push(("host/probe.json".to_string(), read_text(&host_probe)?));
            }
            docs.push((
                "verification-rubric.md".to_string(),
                read_reference_asset(ctx, VERIFICATION_RUBRIC_REF)?,
            ));
        }
    }
    Ok(docs)
}

fn verification_workspace_documents(
    workspace_root: &Path,
) -> Result<Vec<(String, String)>, String> {
    let files = walk_tree(
        workspace_root,
        usize::MAX / 2,
        &[
            ".git",
            "node_modules",
            ".venv",
            "venv",
            "__pycache__",
            "target",
            ".cache",
            "dist",
            "build",
        ],
    )?;
    let mut candidates: Vec<PathBuf> = files.into_iter().filter(|path| path.is_file()).collect();
    candidates.sort_by(|left, right| {
        file_mtime(right)
            .cmp(&file_mtime(left))
            .then_with(|| left.cmp(right))
    });
    let selected: Vec<PathBuf> = candidates
        .iter()
        .filter_map(|path| {
            read_utf8_text_if_reasonable(path, RESPONSES_VERIFICATION_WORKSPACE_MAX_FILE_BYTES)
                .map(|_| path.clone())
        })
        .take(RESPONSES_VERIFICATION_WORKSPACE_MAX_FILES)
        .collect();
    let mut summary = format!(
        "workspace_root: {}\nselected_recent_text_files: {}\n",
        workspace_root.display(),
        selected.len()
    );
    if selected.is_empty() {
        summary.push_str("No recent UTF-8 workspace files were embedded.\n");
        return Ok(vec![("workspace/RECENT_FILES.md".to_string(), summary)]);
    }
    for path in &selected {
        let relative = path
            .strip_prefix(workspace_root)
            .ok()
            .map(|item| item.display().to_string())
            .unwrap_or_else(|| path.display().to_string());
        summary.push_str(&format!("- {relative}\n"));
    }
    let mut docs = vec![("workspace/RECENT_FILES.md".to_string(), summary)];
    for path in selected {
        let Some(text) =
            read_utf8_text_if_reasonable(&path, RESPONSES_VERIFICATION_WORKSPACE_MAX_FILE_BYTES)
        else {
            continue;
        };
        let relative = path
            .strip_prefix(workspace_root)
            .ok()
            .map(|item| item.display().to_string())
            .unwrap_or_else(|| path.display().to_string());
        docs.push((format!("workspace/{relative}"), text));
    }
    Ok(docs)
}

fn truncate_document_for_prompt(text: &str, max_chars: usize) -> String {
    let trimmed = text.trim();
    if trimmed.chars().count() <= max_chars {
        return trimmed.to_string();
    }
    let shortened: String = trimmed.chars().take(max_chars).collect();
    format!("{shortened}\n\n[truncated by agpipe]")
}

fn parse_structured_json_output<T: DeserializeOwned>(text: &str) -> Result<T, String> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err("Structured output was empty.".to_string());
    }
    serde_json::from_str(trimmed).map_err(|err| format!("Could not parse structured JSON: {err}"))
}

fn responses_text_format_for_label(label: &str) -> Option<ResponseTextFormat> {
    let schema = match label {
        "interview-questions" => json!({
            "type": "object",
            "additionalProperties": false,
            "required": ["goal_summary", "questions"],
            "properties": {
                "goal_summary": {"type": "string"},
                "questions": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "additionalProperties": false,
                        "required": ["id", "question", "why", "required"],
                        "properties": {
                            "id": {"type": "string"},
                            "question": {"type": "string"},
                            "why": {"type": "string"},
                            "required": {"type": "boolean"}
                        }
                    }
                }
            }
        }),
        "intake" => json!({
            "type": "object",
            "additionalProperties": false,
            "required": ["brief_md", "plan_json"],
            "properties": {
                "brief_md": {"type": "string"},
                "plan_json": {
                    "type": "object",
                    "additionalProperties": true
                }
            }
        }),
        value if value == "solver" || value.starts_with("solver-") => json!({
            "type": "object",
            "additionalProperties": false,
            "required": ["result_md"],
            "properties": {
                "result_md": {"type": "string"}
            }
        }),
        "review" => json!({
            "type": "object",
            "additionalProperties": false,
            "required": ["report_md", "scorecard_json", "user_summary_md"],
            "properties": {
                "report_md": {"type": "string"},
                "scorecard_json": {
                    "type": "object",
                    "additionalProperties": true
                },
                "user_summary_md": {"type": "string"}
            }
        }),
        "verification" => json!({
            "type": "object",
            "additionalProperties": false,
            "required": [
                "findings_md",
                "user_summary_md",
                "improvement_request_md",
                "augmented_task_md",
                "goal_status_json"
            ],
            "properties": {
                "findings_md": {"type": "string"},
                "user_summary_md": {"type": "string"},
                "improvement_request_md": {"type": "string"},
                "augmented_task_md": {"type": "string"},
                "goal_status_json": {
                    "type": "object",
                    "additionalProperties": true
                }
            }
        }),
        _ => return None,
    };
    Some(ResponseTextFormat {
        name: format!("agpipe_{}_v1", slugify(label)),
        schema,
    })
}

fn responses_text_format_for_stage(
    plan: &Plan,
    run_dir: &Path,
    stage: &str,
) -> Result<Option<ResponseTextFormat>, String> {
    Ok(match pipeline_stage_kind_for(plan, run_dir, stage)? {
        PipelineStageKind::Intake => responses_text_format_for_label("intake"),
        PipelineStageKind::Solver => responses_text_format_for_label("solver"),
        PipelineStageKind::Review => responses_text_format_for_label("review"),
        PipelineStageKind::Execution => None,
        PipelineStageKind::Verification => responses_text_format_for_label("verification"),
    })
}

fn render_responses_context_documents(docs: Vec<(String, String)>) -> String {
    let mut rendered = Vec::new();
    let mut remaining = RESPONSES_DOC_TOTAL_CHARS;
    let mut omitted = Vec::new();
    for (label, text) in docs {
        if remaining == 0 {
            omitted.push(label);
            continue;
        }
        let budget = RESPONSES_DOC_MAX_CHARS_PER_DOC.min(remaining);
        if budget == 0 {
            omitted.push(label);
            continue;
        }
        let truncated = truncate_document_for_prompt(&text, budget);
        remaining = remaining.saturating_sub(truncated.chars().count());
        rendered.push(format!("### {}\n```text\n{}\n```", label, truncated));
    }
    if !omitted.is_empty() {
        rendered.push(format!(
            "[{} additional context document(s) omitted by agpipe due to prompt budget: {}]",
            omitted.len(),
            omitted.join(", ")
        ));
    }
    rendered.join("\n\n")
}

fn build_responses_stage_prompt(
    ctx: &Context,
    run_dir: &Path,
    stage: &str,
) -> Result<String, String> {
    let plan = load_plan(run_dir)?;
    let stage_kind = pipeline_stage_kind_for(&plan, run_dir, stage)?;
    let base_prompt = compile_prompt(ctx, run_dir, stage)?;
    let docs = stage_context_documents(ctx, run_dir, stage)?;
    let rendered_docs = render_responses_context_documents(docs);
    let schema_guidance = match stage_kind {
        PipelineStageKind::Intake => "Return the complete intake payload through the configured structured output schema.\n\nRules:\n- keep `plan_json` compatible with the existing agpipe Plan schema\n- preserve cache config, host facts, and existing defaults unless the prompt gives a clear reason to change them\n- make sure `summary_language`, `stage_research_mode`, `execution_network_mode`, `goal_checks`, `solver_roles`, and `pipeline` remain coherent".to_string(),
        PipelineStageKind::Solver => "Return the complete solver payload through the configured structured output schema.\n\nRules:\n- include assumptions, approach, implementation plan, validation, and unresolved risks\n- keep the full requested goal explicit".to_string(),
        PipelineStageKind::Review => format!(
            "Return the complete review payload through the configured structured output schema.\n\nRules:\n- choose a winner or explicit hybrid in `scorecard_json`\n- include enough structure in `scorecard_json` for downstream execution\n- write `user_summary_md` in {}",
            plan.summary_language
        ),
        PipelineStageKind::Execution => {
            return Err("Responses backend is not implemented for execution stages.".to_string())
        }
        PipelineStageKind::Verification => format!(
            "Return the complete verification payload through the configured structured output schema.\n\nRules:\n- set `goal_status_json.goal_complete=false` when any critical goal check is missing or unverified\n- include `goal_verdict`, `rerun_recommended`, and `recommended_next_action` in `goal_status_json`\n- write `user_summary_md` in {}",
            plan.summary_language
        ),
    };
    Ok(format!(
        "{base_prompt}\n\nBackend mode: `responses-readonly`.\nYou do not have filesystem tools in this backend. All required local context is embedded below. Do not reference unseen files.\nUse the configured structured output schema for your final answer.\n\n{schema_guidance}\n\nEmbedded local context:\n\n{rendered_docs}\n"
    ))
}

fn write_responses_stage_outputs(
    plan: &Plan,
    run_dir: &Path,
    stage: &str,
    raw_output: &str,
) -> Result<(), String> {
    match pipeline_stage_kind_for(plan, run_dir, stage)? {
        PipelineStageKind::Intake => {
            let payload: ResponsesIntakePayload = parse_structured_json_output(raw_output)?;
            if payload.brief_md.trim().is_empty() {
                return Err("Responses intake output is missing `brief_md`.".to_string());
            }
            let plan: Plan = serde_json::from_value(payload.plan_json)
                .map_err(|err| format!("Could not parse intake `plan_json`: {err}"))?;
            write_text(&run_dir.join("brief.md"), payload.brief_md.trim_end())?;
            save_plan(run_dir, &plan)?;
        }
        PipelineStageKind::Solver => {
            let payload: ResponsesSolverPayload = parse_structured_json_output(raw_output)?;
            if payload.result_md.trim().is_empty() {
                return Err(format!(
                    "Responses output for `{stage}` is missing `result_md`."
                ));
            }
            write_text(
                &run_dir.join("solutions").join(stage).join("RESULT.md"),
                payload.result_md.trim_end(),
            )?;
        }
        PipelineStageKind::Review => {
            let payload: ResponsesReviewPayload = parse_structured_json_output(raw_output)?;
            if payload.report_md.trim().is_empty()
                || payload.user_summary_md.trim().is_empty()
                || !payload.scorecard_json.is_object()
            {
                return Err("Responses review output is missing required fields.".to_string());
            }
            write_text(
                &run_dir.join("review").join("report.md"),
                payload.report_md.trim_end(),
            )?;
            write_json(
                &run_dir.join("review").join("scorecard.json"),
                &payload.scorecard_json,
            )?;
            write_text(
                &run_dir.join("review").join("user-summary.md"),
                payload.user_summary_md.trim_end(),
            )?;
        }
        PipelineStageKind::Execution => {
            return Err("Responses backend does not persist execution stages.".to_string())
        }
        PipelineStageKind::Verification => {
            let payload: ResponsesVerificationPayload = parse_structured_json_output(raw_output)?;
            if payload.findings_md.trim().is_empty()
                || payload.user_summary_md.trim().is_empty()
                || payload.improvement_request_md.trim().is_empty()
            {
                return Err(
                    "Responses verification output is missing required markdown fields."
                        .to_string(),
                );
            }
            write_text(
                &run_dir.join("verification").join("findings.md"),
                payload.findings_md.trim_end(),
            )?;
            write_text(
                &run_dir.join("verification").join("user-summary.md"),
                payload.user_summary_md.trim_end(),
            )?;
            write_text(
                &run_dir.join("verification").join("improvement-request.md"),
                payload.improvement_request_md.trim_end(),
            )?;
            if !payload.augmented_task_md.trim().is_empty() {
                write_text(
                    &run_dir.join("verification").join("augmented-task.md"),
                    payload.augmented_task_md.trim_end(),
                )?;
            }
            if payload.goal_status_json.is_object() {
                write_json(
                    &run_dir.join("verification").join("goal-status.json"),
                    &payload.goal_status_json,
                )?;
            }
        }
    }
    Ok(())
}

fn working_root(plan: &Plan, run_dir: &Path) -> PathBuf {
    let workspace = plan_workspace(plan);
    if plan.workspace_exists && workspace.exists() {
        workspace
    } else {
        run_dir.to_path_buf()
    }
}

fn shell_quote(value: &str) -> String {
    if value.is_empty() {
        return "''".to_string();
    }
    if value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || "/._-:=+".contains(ch))
    {
        return value.to_string();
    }
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

fn build_codex_command(
    ctx: &Context,
    run_dir: &Path,
    stage: &str,
    args: &StartArgs,
) -> Result<(Vec<String>, String), String> {
    let plan = load_plan(run_dir)?;
    let root = working_root(&plan, run_dir);
    let prompt = compile_prompt(ctx, run_dir, stage)?;
    let color = args.color.clone().unwrap_or_else(|| "never".to_string());
    let cache = cache_config_from_plan(&plan);
    let mut command = vec![
        ctx.codex_bin.clone(),
        "exec".to_string(),
        "--full-auto".to_string(),
        "--skip-git-repo-check".to_string(),
        "--color".to_string(),
        color,
        "-C".to_string(),
        root.display().to_string(),
        "--add-dir".to_string(),
        run_dir.display().to_string(),
        "--add-dir".to_string(),
        ctx.repo_root.display().to_string(),
        "-".to_string(),
    ];
    if cache.enabled {
        command.splice(
            command.len() - 1..command.len() - 1,
            ["--add-dir".to_string(), cache.root.clone()],
        );
    }
    if let Some(model) = &args.model {
        command.splice(2..2, ["--model".to_string(), model.clone()]);
    }
    if let Some(profile) = &args.profile {
        command.splice(2..2, ["--profile".to_string(), profile.clone()]);
    }
    if args.oss {
        command.insert(2, "--oss".to_string());
    }
    Ok((command, prompt))
}

fn build_responses_command(
    ctx: &Context,
    run_dir: &Path,
    stage: &str,
) -> Result<(Vec<String>, String), String> {
    let plan = load_plan(run_dir)?;
    let prompt = build_responses_stage_prompt(ctx, run_dir, stage)?;
    let mut command = vec![
        "responses-api".to_string(),
        "stage".to_string(),
        stage.to_string(),
        "--model".to_string(),
        ctx.openai_model.clone(),
        "--workdir".to_string(),
        working_root(&plan, run_dir).display().to_string(),
    ];
    if ctx.openai_background {
        command.push("--background".to_string());
    }
    Ok((command, prompt))
}

fn build_local_template_command(
    template: LocalTemplateKind,
    ctx: &Context,
    run_dir: &Path,
    stage: &str,
) -> Result<(Vec<String>, String), String> {
    let template_name = match template {
        LocalTemplateKind::HelloWorldPython => "hello-world-python",
    };
    let prompt = compile_prompt(ctx, run_dir, stage)?;
    Ok((
        vec![
            "agpipe-local-template".to_string(),
            template_name.to_string(),
            stage.to_string(),
        ],
        prompt,
    ))
}

type PreparedStageCommand = (Vec<String>, String, PathBuf, PathBuf, PathBuf, PathBuf);

fn prepare_stage_command(
    ctx: &Context,
    run_dir: &Path,
    stage: &str,
    args: &StartArgs,
) -> Result<PreparedStageCommand, String> {
    let logs_dir = run_dir.join("logs");
    fs::create_dir_all(&logs_dir)
        .map_err(|err| format!("Could not create {}: {err}", logs_dir.display()))?;
    let prompt_path = logs_dir.join(format!("{stage}.prompt.md"));
    let last_message_path = logs_dir.join(format!("{stage}.last.md"));
    let stdout_path = logs_dir.join(format!("{stage}.stdout.log"));
    let stderr_path = logs_dir.join(format!("{stage}.stderr.log"));
    let backend = stage_backend_kind(ctx, run_dir, stage)?;
    let (mut command, prompt) = match backend {
        StageBackendKind::Codex => build_codex_command(ctx, run_dir, stage, args)?,
        StageBackendKind::Responses => build_responses_command(ctx, run_dir, stage)?,
        StageBackendKind::LocalTemplate(template) => {
            build_local_template_command(template, ctx, run_dir, stage)?
        }
    };
    write_text(&prompt_path, &prompt)?;
    if backend == StageBackendKind::Codex {
        command.pop();
        command.extend([
            "--output-last-message".to_string(),
            last_message_path.display().to_string(),
            "-".to_string(),
        ]);
    }
    Ok((
        command,
        prompt,
        prompt_path,
        last_message_path,
        stdout_path,
        stderr_path,
    ))
}

fn run_prompted_command_capture(
    command: &[String],
    prompt: &str,
    stdout_path: &Path,
    stderr_path: &Path,
    run_dir: Option<&Path>,
) -> Result<i32, String> {
    let mut cmd = Command::new(&command[0]);
    cmd.args(&command[1..]);
    cmd.stdin(Stdio::piped());
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());
    unsafe {
        cmd.pre_exec(|| {
            if libc::setpgid(0, 0) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut child = cmd
        .spawn()
        .map_err(|err| format!("Failed to run command: {err}"))?;
    let pid = child.id() as i32;
    let mut interrupted = false;
    notify_process_started(pid, pid);
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "Spawned command did not expose stdout.".to_string())?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| "Spawned command did not expose stderr.".to_string())?;
    let stdout_thread = stream_pipe_to_file(stdout, stdout_path.to_path_buf());
    let stderr_thread = stream_pipe_to_file(stderr, stderr_path.to_path_buf());
    if let Some(mut stdin) = child.stdin.take() {
        stdin
            .write_all(prompt.as_bytes())
            .map_err(|err| format!("Could not write command stdin: {err}"))?;
    }
    let status = loop {
        if let Some(run_dir) = run_dir {
            if crate::runtime::interrupt_requested(run_dir) {
                interrupted = true;
                let _ = unsafe { libc::kill(-pid, libc::SIGTERM) };
                let mut finished = None;
                for _ in 0..20 {
                    match child.try_wait() {
                        Ok(Some(status)) => {
                            finished = Some(status);
                            break;
                        }
                        Ok(None) => thread::sleep(Duration::from_millis(100)),
                        Err(err) => {
                            return Err(format!("Could not wait for interrupted command: {err}"))
                        }
                    }
                }
                if let Some(status) = finished {
                    break status;
                }
                let _ = unsafe { libc::kill(-pid, libc::SIGKILL) };
                let status = child
                    .wait()
                    .map_err(|err| format!("Could not wait for interrupted command: {err}"))?;
                break status;
            }
        }
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) => thread::sleep(Duration::from_millis(100)),
            Err(err) => return Err(format!("Could not wait for command: {err}")),
        }
    };
    stdout_thread.join().map_err(|_| {
        format!(
            "Could not join stdout capture thread for {}",
            stdout_path.display()
        )
    })??;
    stderr_thread.join().map_err(|_| {
        format!(
            "Could not join stderr capture thread for {}",
            stderr_path.display()
        )
    })??;
    if interrupted {
        Ok(130)
    } else {
        Ok(status.code().unwrap_or(1))
    }
}

#[allow(clippy::too_many_arguments)]
fn run_responses_stage_capture(
    ctx: &Context,
    run_dir: &Path,
    stage: &str,
    prompt: &str,
    prompt_path: &Path,
    last_message_path: &Path,
    stdout_path: &Path,
    stderr_path: &Path,
) -> Result<i32, String> {
    let plan = load_plan(run_dir)?;
    let logs_dir = prompt_path
        .parent()
        .ok_or_else(|| format!("Missing logs dir parent for {}", prompt_path.display()))?;
    write_text(prompt_path, prompt)?;
    let text_format = responses_text_format_for_stage(&plan, run_dir, stage)?;
    let raw_output = run_responses_last_message(
        ctx,
        prompt,
        &working_root(&plan, run_dir),
        logs_dir,
        stage,
        Some(run_dir),
        text_format.as_ref(),
    )?;
    if !last_message_path.exists() {
        write_text(last_message_path, &raw_output)?;
    }
    write_responses_stage_outputs(&plan, run_dir, stage, &raw_output)?;
    append_log_line(
        stdout_path,
        &format!("persisted responses-backed stage outputs for `{stage}`"),
    )?;
    let _ = stderr_path;
    Ok(0)
}

fn local_template_token_usage(template: LocalTemplateKind) -> TokenUsage {
    let source = match template {
        LocalTemplateKind::HelloWorldPython => "local-template:hello-world-python",
    };
    TokenUsage {
        source: source.to_string(),
        prompt_tokens: 0,
        cached_prompt_tokens: 0,
        completion_tokens: 0,
        total_tokens: 0,
        estimated_saved_tokens: 0,
    }
}

fn hello_world_workspace_root(plan: &Plan, run_dir: &Path) -> Result<PathBuf, String> {
    let configured = plan_workspace(plan);
    let target = if plan.workspace.trim().is_empty() {
        run_dir.to_path_buf()
    } else {
        configured
    };
    fs::create_dir_all(&target).map_err(|err| {
        format!(
            "Could not create hello-world workspace {}: {err}",
            target.display()
        )
    })?;
    Ok(target)
}

fn hello_world_script() -> &'static str {
    "def main() -> None:\n    print(\"Hello, world!\")\n\n\nif __name__ == \"__main__\":\n    main()\n"
}

fn run_python_hello_world_validation(root: &Path) -> Result<String, String> {
    let output = Command::new("python3")
        .arg("main.py")
        .current_dir(root)
        .output()
        .map_err(|err| format!("Could not run python3 main.py in {}: {err}", root.display()))?;
    if !output.status.success() {
        return Err(format!(
            "python3 main.py failed in {}: {}",
            root.display(),
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn write_local_template_stage_outputs(
    template: LocalTemplateKind,
    run_dir: &Path,
    stage: &str,
) -> Result<String, String> {
    let mut plan = load_plan(run_dir)?;
    let stage_kind = pipeline_stage_kind_for(&plan, run_dir, stage)?;
    match template {
        LocalTemplateKind::HelloWorldPython => match stage_kind {
            PipelineStageKind::Intake => {
                plan.task_kind = "backend".to_string();
                plan.complexity = "low".to_string();
                plan.execution_mode = "full".to_string();
                plan.solver_count = 1;
                plan.solver_roles = vec![SolverRole {
                    solver_id: "solver-a".to_string(),
                    role: "engineering/engineering-senior-developer.md".to_string(),
                    angle: "implementation-first".to_string(),
                }];
                plan.validation_commands = vec!["python3 main.py".to_string()];
                plan.workstream_hints = vec![WorkstreamHint {
                    name: "hello-world-entrypoint".to_string(),
                    goal: "create a runnable Python hello-world entrypoint".to_string(),
                    suggested_role: "engineering/engineering-senior-developer.md".to_string(),
                }];
                plan.goal_checks = vec![
                    GoalCheck {
                        id: "runnable_entrypoint".to_string(),
                        requirement:
                            "provide a runnable Python entrypoint that prints Hello, world!"
                                .to_string(),
                        critical: true,
                    },
                    GoalCheck {
                        id: "validation".to_string(),
                        requirement: "validate the script with python3 main.py".to_string(),
                        critical: false,
                    },
                ];
                save_plan(run_dir, &plan)?;
                write_text(
                    &run_dir.join("brief.md"),
                    "# Brief\n\n## Original requested outcome\nCreate a Python hello-world program.\n\n## Objective\nProduce a runnable `main.py` that prints `Hello, world!`.\n\n## Deliverable\nA minimal Python workspace with a single entrypoint and a short execution report.\n\n## Goal coverage matrix\n- `runnable_entrypoint`: `main.py` prints `Hello, world!`\n- `validation`: run `python3 main.py`\n\n## Workstream decomposition\n- create the entrypoint\n- validate the output\n- summarize the result\n\n## Scope\n- one Python file\n- no external dependencies\n\n## Constraints\n- keep the implementation minimal and deterministic\n\n## Definition of done\n- `python3 main.py` exits successfully\n- stdout is exactly `Hello, world!`\n\n## Validation expectations\n- run `python3 main.py`\n",
                )?;
                Ok("Local template intake completed for Python hello-world.".to_string())
            }
            PipelineStageKind::Solver => {
                write_text(
                    &run_dir.join("solutions").join(stage).join("RESULT.md"),
                    "# Result\n\n## Proposed implementation\nCreate `main.py` with a `main()` function that prints `Hello, world!` and a standard `if __name__ == \"__main__\"` guard.\n\n## Validation\nRun `python3 main.py`.\n\n## Risks\nNo meaningful risks for this trivial implementation.\n",
                )?;
                Ok("Local template solver output generated.".to_string())
            }
            PipelineStageKind::Review => {
                let selected = solver_ids(&plan, run_dir)
                    .into_iter()
                    .next()
                    .unwrap_or_else(|| "solver-a".to_string());
                write_text(
                    &run_dir.join("review").join("report.md"),
                    &format!(
                        "# Review Report\n\nSelected `{selected}` because it satisfies the full request with the minimal deterministic Python implementation.\n"
                    ),
                )?;
                write_json(
                    &run_dir.join("review").join("scorecard.json"),
                    &json!({
                        "winner": selected,
                        "selected": selected,
                        "why": "Minimal runnable Python hello-world implementation."
                    }),
                )?;
                write_text(
                    &run_dir.join("review").join("user-summary.md"),
                    "# User Summary\n\nВыбран простой вариант: один `main.py`, который выводит `Hello, world!`.\n",
                )?;
                Ok("Local template review selected the minimal solver output.".to_string())
            }
            PipelineStageKind::Execution => {
                let root = hello_world_workspace_root(&plan, run_dir)?;
                write_text(&root.join("main.py"), hello_world_script())?;
                write_text(
                    &root.join("README.md"),
                    "# Hello World\n\nRun:\n\n```bash\npython3 main.py\n```\n",
                )?;
                if !plan.workspace_exists {
                    plan.workspace_exists = true;
                    plan.workspace = root.display().to_string();
                    save_plan(run_dir, &plan)?;
                }
                let output = run_python_hello_world_validation(&root)?;
                write_text(
                    &run_dir.join("execution").join("report.md"),
                    &format!(
                        "# Execution Report\n\nCreated `main.py` in `{}`.\n\nValidation:\n- command: `python3 main.py`\n- stdout: `{}`\n",
                        root.display(),
                        output
                    ),
                )?;
                Ok(format!(
                    "Local template execution created {} and validated `python3 main.py`.",
                    root.join("main.py").display()
                ))
            }
            PipelineStageKind::Verification => {
                let root = hello_world_workspace_root(&plan, run_dir)?;
                let output = run_python_hello_world_validation(&root)?;
                let success = output == "Hello, world!";
                write_text(
                    &run_dir.join("verification").join("findings.md"),
                    if success {
                        "# Findings\n\nNo critical findings. `python3 main.py` prints `Hello, world!`.\n"
                    } else {
                        "# Findings\n\nCritical finding: `python3 main.py` did not produce the expected `Hello, world!` output.\n"
                    },
                )?;
                write_text(
                    &run_dir.join("verification").join("user-summary.md"),
                    if success {
                        "# Verification Summary\n\nПроверка прошла успешно: программа запускается и печатает `Hello, world!`.\n"
                    } else {
                        "# Verification Summary\n\nПроверка не прошла: вывод программы не совпал с ожидаемым `Hello, world!`.\n"
                    },
                )?;
                write_text(
                    &run_dir.join("verification").join("improvement-request.md"),
                    if success {
                        "# Improvement Request\n\nNo follow-up is required.\n"
                    } else {
                        "# Improvement Request\n\nFix the entrypoint so `python3 main.py` prints `Hello, world!`.\n"
                    },
                )?;
                write_text(
                    &run_dir.join("verification").join("augmented-task.md"),
                    if success {
                        "# Augmented Task\n\nKeep the verified Python hello-world implementation unchanged.\n"
                    } else {
                        "# Augmented Task\n\nRepair the Python hello-world implementation and re-run verification.\n"
                    },
                )?;
                write_json(
                    &run_dir.join("verification").join("goal-status.json"),
                    &json!({
                        "goal_complete": success,
                        "goal_verdict": if success { "complete" } else { "blocked" },
                        "rerun_recommended": !success,
                        "recommended_next_action": if success { "none" } else { "rerun" }
                    }),
                )?;
                Ok(format!(
                    "Local template verification checked `python3 main.py` with stdout `{}`.",
                    output
                ))
            }
        },
    }
}

fn run_local_template_stage(
    template: LocalTemplateKind,
    run_dir: &Path,
    stage: &str,
    last_message_path: &Path,
    stdout_path: &Path,
    stderr_path: &Path,
) -> Result<i32, String> {
    let summary = write_local_template_stage_outputs(template, run_dir, stage)?;
    write_text(last_message_path, &summary)?;
    write_text(stdout_path, &format!("{summary}\n"))?;
    write_text(stderr_path, "")?;
    notify_output_line(&summary);
    Ok(0)
}

fn stream_pipe_to_file<R>(mut reader: R, path: PathBuf) -> thread::JoinHandle<Result<(), String>>
where
    R: Read + Send + 'static,
{
    thread::spawn(move || {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|err| format!("Could not create {}: {err}", parent.display()))?;
        }
        let mut file = File::create(&path)
            .map_err(|err| format!("Could not create {}: {err}", path.display()))?;
        let mut buffer = [0u8; 8192];
        let mut pending = Vec::new();
        loop {
            let read = reader.read(&mut buffer).map_err(|err| {
                format!(
                    "Could not read command output for {}: {err}",
                    path.display()
                )
            })?;
            if read == 0 {
                break;
            }
            file.write_all(&buffer[..read])
                .map_err(|err| format!("Could not write {}: {err}", path.display()))?;
            file.flush()
                .map_err(|err| format!("Could not flush {}: {err}", path.display()))?;
            pending.extend_from_slice(&buffer[..read]);
            while let Some(pos) = pending.iter().position(|byte| *byte == b'\n') {
                let line = String::from_utf8_lossy(&pending[..pos]).to_string();
                notify_output_line(line.trim_end_matches('\r'));
                pending.drain(..=pos);
            }
        }
        if !pending.is_empty() {
            let line = String::from_utf8_lossy(&pending).to_string();
            if !line.is_empty() {
                notify_output_line(line.trim_end_matches('\r'));
            }
        }
        Ok(())
    })
}

fn status_text(report: &StatusPayload) -> String {
    let mut lines = Vec::new();
    for (stage, status) in &report.stages {
        lines.push(format!("{stage}: {status}"));
    }
    lines.push(format!("host-probe: {}", report.host_probe));
    if let Some(drift) = &report.host_drift {
        lines.push(format!("host-drift: {drift}"));
    }
    lines.push(format!("goal: {}", report.goal));
    lines.push(format!("next: {}", report.next));
    lines.join("\n")
}

fn check_run_interrupt(run_dir: &Path) -> Result<(), String> {
    if crate::runtime::interrupt_requested(run_dir) {
        return Err("Interrupted from agpipe.".to_string());
    }
    Ok(())
}

fn doctor_text(report: &DoctorPayload) -> String {
    let mut lines = vec![
        format!("health: {}", report.health),
        format!("goal: {}", report.goal),
        format!("next: {}", report.next),
        format!("safe-next-action: {}", report.safe_next_action),
        format!("host-probe: {}", report.host_probe),
    ];
    if !report.stale.is_empty() {
        lines.push(format!("stale: {}", report.stale.join(", ")));
    }
    if let Some(drift) = &report.host_drift {
        lines.push(format!("host-drift: {drift}"));
    }
    if !report.issues.is_empty() {
        lines.push("\nissues:".to_string());
        for item in &report.issues {
            lines.push(format!("- {}", item.message));
            lines.push(format!("  fix: {}", item.fix));
        }
    }
    if !report.warnings.is_empty() {
        lines.push("\nwarnings:".to_string());
        for item in &report.warnings {
            lines.push(format!("- {}", item.message));
            lines.push(format!("  fix: {}", item.fix));
        }
    }
    if report.issues.is_empty() && report.warnings.is_empty() {
        lines.push("\nNo consistency issues detected.".to_string());
    }
    lines.join("\n")
}

fn require_valid_order(run_dir: &Path, stage: &str, force: bool) -> Result<(), String> {
    if force {
        return Ok(());
    }
    let plan = load_plan(run_dir)?;
    let kind = pipeline_stage_kind_for(&plan, run_dir, stage)?;
    if kind == PipelineStageKind::Intake {
        return Ok(());
    }
    let mut prerequisites = prior_pipeline_stages(&plan, run_dir, stage)?;
    if kind == PipelineStageKind::Solver {
        prerequisites
            .retain(|item| pipeline_kind_from_str(&item.kind) != Some(PipelineStageKind::Solver));
    }
    let pending: Vec<String> = prerequisites
        .into_iter()
        .filter_map(|item| match is_stage_complete(run_dir, &item.id) {
            Ok(true) => None,
            Ok(false) => Some(item.id),
            Err(_) => Some(item.id),
        })
        .collect();
    if !pending.is_empty() {
        return Err(format!(
            "Required earlier stages are still pending: {}. Run them first or pass --force.",
            pending.join(", ")
        ));
    }
    Ok(())
}

fn print_status_after_action(ctx: &Context, run_dir: &Path) -> Result<String, String> {
    let status = status_report(ctx, run_dir)?;
    Ok(format!("\nStatus:\n\n{}\n", status_text(&status)))
}

fn start_stage(
    ctx: &Context,
    run_dir: &Path,
    stage: &str,
    args: &StartArgs,
) -> Result<CommandResult, String> {
    check_run_interrupt(run_dir)?;
    require_valid_order(run_dir, stage, args.force)?;
    if matches!(
        pipeline_stage_kind_for(&load_plan(run_dir)?, run_dir, stage)?,
        PipelineStageKind::Execution | PipelineStageKind::Verification
    ) {
        let _ = capture_host_probe(run_dir)?;
    }
    let backend = stage_backend_kind(ctx, run_dir, stage)?;
    let (command, prompt, prompt_path, last_message_path, stdout_path, stderr_path) =
        prepare_stage_command(ctx, run_dir, stage, args)?;
    let plan = load_plan(run_dir)?;
    let cache = cache_config_from_plan(&plan);
    let (cache_key, cache_inputs, prompt_hashes, workspace_hash) = stage_cache_key(
        ctx, &cache, &plan, run_dir, stage, backend, &command, &prompt,
    )?;
    let output_paths = stage_output_paths(&plan, run_dir, stage)?;
    let log_paths = vec![
        prompt_path.clone(),
        last_message_path.clone(),
        stdout_path.clone(),
        stderr_path.clone(),
    ];
    if args.dry_run {
        return Ok(CommandResult {
            code: 0,
            stdout: format!(
                "Command:\n{}\n\nPrompt:\n\n{}\nCache key: {}\n{}",
                command
                    .iter()
                    .map(|part| shell_quote(part))
                    .collect::<Vec<_>>()
                    .join(" "),
                prompt,
                cache_key,
                print_status_after_action(ctx, run_dir)?
            ),
            stderr: String::new(),
        });
    }
    if cache.enabled && cache.policy == "reuse" {
        if let Some(manifest) = restore_stage_cache(&cache, &cache_key, run_dir, stage)? {
            let usage = token_usage_from_cache_hit(&manifest);
            record_token_usage(
                run_dir,
                &plan,
                stage,
                "cache-hit",
                &cache_key,
                &prompt_hashes,
                &workspace_hash,
                &usage,
            )?;
            sync_run_artifacts(ctx, run_dir)?;
            let _ = refresh_cache_index(&cache)?;
            let mut stdout = format!(
                "Reused cached {stage} result.\ncache key: {cache_key}\nstdout log: {}\nstderr log: {}\n",
                stdout_path.display(),
                stderr_path.display()
            );
            stdout.push_str(&print_status_after_action(ctx, run_dir)?);
            return Ok(CommandResult {
                code: 0,
                stdout,
                stderr: String::new(),
            });
        }
    }
    let code = match backend {
        StageBackendKind::Codex => run_prompted_command_capture(
            &command,
            &prompt,
            &stdout_path,
            &stderr_path,
            Some(run_dir),
        )?,
        StageBackendKind::Responses => run_responses_stage_capture(
            ctx,
            run_dir,
            stage,
            &prompt,
            &prompt_path,
            &last_message_path,
            &stdout_path,
            &stderr_path,
        )?,
        StageBackendKind::LocalTemplate(template) => run_local_template_stage(
            template,
            run_dir,
            stage,
            &last_message_path,
            &stdout_path,
            &stderr_path,
        )?,
    };
    sync_run_artifacts(ctx, run_dir)?;
    check_run_interrupt(run_dir)?;
    if code == 0 && is_stage_complete(run_dir, stage)? {
        let token_usage = match backend {
            StageBackendKind::LocalTemplate(template) => local_template_token_usage(template),
            _ => build_stage_token_usage(&prompt, &last_message_path),
        };
        store_stage_cache(
            &cache,
            &cache_key,
            run_dir,
            stage,
            &command,
            &prompt_hashes,
            &workspace_hash,
            &token_usage,
            &cache_inputs,
            &output_paths,
            &log_paths,
        )?;
        record_token_usage(
            run_dir,
            &plan,
            stage,
            "executed",
            &cache_key,
            &prompt_hashes,
            &workspace_hash,
            &token_usage,
        )?;
    }
    let _ = refresh_cache_index(&cache)?;
    let mut stdout = format!(
        "Completed {stage} with exit code {code}.\ncache key: {cache_key}\nstdout log: {}\nstderr log: {}\n",
        stdout_path.display(),
        stderr_path.display()
    );
    stdout.push_str(&print_status_after_action(ctx, run_dir)?);
    Ok(CommandResult {
        code,
        stdout,
        stderr: String::new(),
    })
}

fn pending_solver_stages(run_dir: &Path) -> Result<Vec<String>, String> {
    let plan = load_plan(run_dir)?;
    let mut pending = Vec::new();
    for solver in solver_ids(&plan, run_dir) {
        if !is_stage_complete(run_dir, &solver)? {
            pending.push(solver);
        }
    }
    Ok(pending)
}

fn start_solver_batch(
    ctx: &Context,
    run_dir: &Path,
    stages: &[String],
    args: &StartArgs,
) -> Result<CommandResult, String> {
    if stages.is_empty() {
        return Ok(CommandResult {
            code: 0,
            stdout: format!(
                "No pending solver stages.{}",
                print_status_after_action(ctx, run_dir)?
            ),
            stderr: String::new(),
        });
    }
    for stage in stages {
        check_run_interrupt(run_dir)?;
        require_valid_order(run_dir, stage, args.force)?;
    }
    let requires_in_process = stages.iter().any(|stage| {
        matches!(
            stage_backend_kind(ctx, run_dir, stage),
            Ok(StageBackendKind::Responses | StageBackendKind::LocalTemplate(_))
        )
    });
    if engine_observer_present() || requires_in_process {
        let mut combined = String::new();
        let mut exit_code = 0;
        for stage in stages {
            check_run_interrupt(run_dir)?;
            let result = start_stage(ctx, run_dir, stage, args)?;
            if !result.stdout.trim().is_empty() {
                combined.push_str(result.stdout.trim_end());
                combined.push('\n');
            }
            if !result.stderr.trim().is_empty() {
                combined.push_str(result.stderr.trim_end());
                combined.push('\n');
            }
            if result.code != 0 && exit_code == 0 {
                exit_code = result.code;
            }
            if result.code != 0 {
                break;
            }
        }
        return Ok(CommandResult {
            code: exit_code,
            stdout: combined,
            stderr: String::new(),
        });
    }
    let mut prepared = Vec::new();
    for stage in stages {
        let (command, prompt, prompt_path, last_message_path, stdout_path, stderr_path) =
            prepare_stage_command(ctx, run_dir, stage, args)?;
        let backend = stage_backend_kind(ctx, run_dir, stage)?;
        let plan = load_plan(run_dir)?;
        let cache = cache_config_from_plan(&plan);
        let (cache_key, cache_inputs, prompt_hashes, workspace_hash) = stage_cache_key(
            ctx, &cache, &plan, run_dir, stage, backend, &command, &prompt,
        )?;
        let outputs = stage_output_paths(&plan, run_dir, stage)?;
        let logs = vec![
            prompt_path.clone(),
            last_message_path.clone(),
            stdout_path.clone(),
            stderr_path.clone(),
        ];
        prepared.push((
            stage.clone(),
            command,
            prompt,
            last_message_path,
            stdout_path,
            stderr_path,
            cache_key,
            cache_inputs,
            prompt_hashes,
            workspace_hash,
            outputs,
            logs,
            cache,
            plan,
        ));
    }
    if args.dry_run {
        let mut out = String::new();
        for (stage, command, prompt, _, _, _, cache_key, _, _, _, _, _, _, _) in &prepared {
            out.push_str(&format!(
                "Stage: {stage}\nCommand:\n{}\n\nPrompt:\n\n{}\nCache key: {cache_key}\n\n---\n\n",
                command
                    .iter()
                    .map(|part| shell_quote(part))
                    .collect::<Vec<_>>()
                    .join(" "),
                prompt
            ));
        }
        out.push_str(&print_status_after_action(ctx, run_dir)?);
        return Ok(CommandResult {
            code: 0,
            stdout: out,
            stderr: String::new(),
        });
    }
    let mut children = Vec::new();
    let mut out = String::new();
    for (
        stage,
        command,
        prompt,
        last_message_path,
        stdout_path,
        stderr_path,
        cache_key,
        cache_inputs,
        prompt_hashes,
        workspace_hash,
        outputs,
        logs,
        cache,
        plan,
    ) in prepared
    {
        check_run_interrupt(run_dir)?;
        if cache.enabled && cache.policy == "reuse" {
            if let Some(manifest) = restore_stage_cache(&cache, &cache_key, run_dir, &stage)? {
                let usage = token_usage_from_cache_hit(&manifest);
                record_token_usage(
                    run_dir,
                    &plan,
                    &stage,
                    "cache-hit",
                    &cache_key,
                    &prompt_hashes,
                    &workspace_hash,
                    &usage,
                )?;
                out.push_str(&format!(
                    "Reused cached {stage}. cache key: {cache_key}. Logs: {}, {}\n",
                    stdout_path.display(),
                    stderr_path.display()
                ));
                sync_run_artifacts(ctx, run_dir)?;
                let _ = refresh_cache_index(&cache)?;
                continue;
            }
        }
        let mut cmd = Command::new(&command[0]);
        cmd.args(&command[1..]);
        cmd.stdin(Stdio::piped());
        let stdout_file = File::create(&stdout_path)
            .map_err(|err| format!("Could not open {}: {err}", stdout_path.display()))?;
        let stderr_file = File::create(&stderr_path)
            .map_err(|err| format!("Could not open {}: {err}", stderr_path.display()))?;
        cmd.stdout(Stdio::from(stdout_file));
        cmd.stderr(Stdio::from(stderr_file));
        let mut child = cmd
            .spawn()
            .map_err(|err| format!("Failed to start {stage}: {err}"))?;
        if let Some(mut stdin) = child.stdin.take() {
            stdin
                .write_all(prompt.as_bytes())
                .map_err(|err| format!("Could not write stdin for {stage}: {err}"))?;
        }
        out.push_str(&format!(
            "Started {stage}. cache key: {cache_key}. Logs: {}, {}\n",
            stdout_path.display(),
            stderr_path.display()
        ));
        children.push((
            stage,
            child,
            prompt,
            last_message_path,
            stdout_path,
            stderr_path,
            cache_key,
            cache_inputs,
            prompt_hashes,
            workspace_hash,
            outputs,
            logs,
            cache,
            command,
            plan,
        ));
    }
    let mut exit_code = 0;
    for (
        stage,
        mut child,
        prompt,
        last_message_path,
        stdout_path,
        stderr_path,
        cache_key,
        cache_inputs,
        prompt_hashes,
        workspace_hash,
        outputs,
        logs,
        cache,
        command,
        plan,
    ) in children
    {
        check_run_interrupt(run_dir)?;
        let result = child
            .wait()
            .map_err(|err| format!("Failed to wait for {stage}: {err}"))?
            .code()
            .unwrap_or(1);
        if result != 0 && exit_code == 0 {
            exit_code = result;
        }
        sync_run_artifacts(ctx, run_dir)?;
        if result == 0 && is_stage_complete(run_dir, &stage)? {
            let token_usage = build_stage_token_usage(&prompt, &last_message_path);
            store_stage_cache(
                &cache,
                &cache_key,
                run_dir,
                &stage,
                &command,
                &prompt_hashes,
                &workspace_hash,
                &token_usage,
                &cache_inputs,
                &outputs,
                &logs,
            )?;
            record_token_usage(
                run_dir,
                &plan,
                &stage,
                "executed",
                &cache_key,
                &prompt_hashes,
                &workspace_hash,
                &token_usage,
            )?;
        }
        let _ = refresh_cache_index(&cache)?;
        out.push_str(&format!(
            "\nCompleted {stage} with exit code {result}. cache key: {cache_key}. Logs: {}, {}\n",
            stdout_path.display(),
            stderr_path.display()
        ));
        out.push_str(&print_status_after_action(ctx, run_dir)?);
    }
    Ok(CommandResult {
        code: exit_code,
        stdout: out,
        stderr: String::new(),
    })
}

fn resolve_stage(run_dir: &Path, stage: &str) -> Result<String, String> {
    let stages = available_stages(run_dir)?;
    if stages.iter().any(|item| item == stage) {
        Ok(stage.to_string())
    } else {
        Err(format!(
            "Unknown stage '{stage}'. Valid stages: {}",
            stages.join(", ")
        ))
    }
}

fn stage_reset_order(run_dir: &Path, stage: &str) -> Result<Vec<String>, String> {
    let plan = load_plan(run_dir)?;
    let stages = pipeline_stage_specs(&plan, Some(run_dir))?;
    if !stages.iter().any(|item| item.id == stage) {
        return Err(format!(
            "Unknown stage '{stage}'. Valid stages: {}",
            stages
                .iter()
                .map(|item| item.id.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    let stage_kind = pipeline_stage_kind_for(&plan, run_dir, stage)?;
    let Some(index) = stages.iter().position(|item| item.id == stage) else {
        return Err(format!("Unsupported stage for step-back: {stage}"));
    };
    let values = match stage_kind {
        PipelineStageKind::Intake => stages.into_iter().map(|item| item.id).collect(),
        PipelineStageKind::Solver => {
            let mut items = vec![stage.to_string()];
            items.extend(stages.into_iter().skip(index + 1).filter_map(|item| {
                (pipeline_kind_from_str(&item.kind) != Some(PipelineStageKind::Solver))
                    .then_some(item.id)
            }));
            items
        }
        PipelineStageKind::Review => stages
            .into_iter()
            .skip(index)
            .filter_map(|item| {
                matches!(
                    pipeline_kind_from_str(&item.kind),
                    Some(
                        PipelineStageKind::Review
                            | PipelineStageKind::Execution
                            | PipelineStageKind::Verification
                    )
                )
                .then_some(item.id)
            })
            .collect(),
        PipelineStageKind::Execution => stages
            .into_iter()
            .skip(index)
            .filter_map(|item| {
                matches!(
                    pipeline_kind_from_str(&item.kind),
                    Some(PipelineStageKind::Execution | PipelineStageKind::Verification)
                )
                .then_some(item.id)
            })
            .collect(),
        PipelineStageKind::Verification => vec![stage.to_string()],
    };
    Ok(values)
}

fn print_user_summary(run_dir: &Path) -> Result<CommandResult, String> {
    let summary_path = run_dir.join("review").join("user-summary.md");
    if summary_path.exists() {
        let text = read_text(&summary_path)?;
        if !output_looks_placeholder("review-summary", &text) {
            return Ok(CommandResult {
                code: 0,
                stdout: format!("{}\n", text.trim_end()),
                stderr: String::new(),
            });
        }
    }
    let report_path = run_dir.join("review").join("report.md");
    if review_complete_without_summary(run_dir) {
        return Ok(CommandResult { code: 0, stdout: format!("Localized user summary is not available for this older run. Review report:\n\n{}\n", read_text(&report_path)?.trim_end()), stderr: String::new() });
    }
    Err("Localized user summary is not ready yet. Run the review stage first.".to_string())
}

fn print_findings(run_dir: &Path) -> Result<CommandResult, String> {
    let findings = run_dir.join("verification").join("findings.md");
    if findings.exists() {
        let text = read_text(&findings)?;
        if !output_looks_placeholder("verification", &text) {
            return Ok(CommandResult {
                code: 0,
                stdout: format!("{}\n", text.trim_end()),
                stderr: String::new(),
            });
        }
    }
    Err("Verification findings are not ready yet. Run the verification stage first.".to_string())
}

fn print_augmented_task(run_dir: &Path) -> Result<CommandResult, String> {
    let augmented = augmented_task_path(run_dir);
    if augmented.exists() {
        let text = read_text(&augmented)?;
        if !output_looks_placeholder("augmented-task", &text) {
            return Ok(CommandResult {
                code: 0,
                stdout: format!("{}\n", text.trim_end()),
                stderr: String::new(),
            });
        }
    }
    let improvement = run_dir.join("verification").join("improvement-request.md");
    if improvement.exists() {
        let text = read_text(&improvement)?;
        if !output_looks_placeholder("improvement-request", &text) {
            return Ok(CommandResult {
                code: 0,
                stdout: format!(
                    "Augmented task is not available for this run. Improvement request:\n\n{}\n",
                    text.trim_end()
                ),
                stderr: String::new(),
            });
        }
    }
    Err("Augmented follow-up task is not ready yet. Run the verification stage first.".to_string())
}

fn print_cache_status(run_dir: &Path, args: &CacheStatusArgs) -> Result<CommandResult, String> {
    let plan = load_plan(run_dir)?;
    let cache = cache_config_from_plan(&plan);
    let index = load_cache_index(&cache, args.refresh)?;
    let mut lines = vec![
        format!(
            "cache enabled: {}",
            if cache.enabled { "yes" } else { "no" }
        ),
        format!("cache policy: {}", cache.policy),
        format!("cache root: {}", cache.root),
        format!("cache index: {}", cache.meta.index),
        format!(
            "generated at: {}",
            index
                .get("generated_at")
                .and_then(|value| value.as_str())
                .unwrap_or("n/a")
        ),
        format!(
            "total files: {}",
            index
                .get("total_files")
                .and_then(|value| value.as_u64())
                .unwrap_or(0)
        ),
        format!(
            "total size: {}",
            format_size(
                index
                    .get("total_size_bytes")
                    .and_then(|value| value.as_u64())
                    .unwrap_or(0)
            )
        ),
    ];
    if let Some(areas) = index.get("areas").and_then(|value| value.as_object()) {
        if !areas.is_empty() {
            lines.push("\nareas:".to_string());
            for area in CACHE_AREAS {
                if let Some(payload) = areas.get(area).and_then(|value| value.as_object()) {
                    lines.push(format!(
                        "- {}: {} files, {}",
                        area,
                        payload
                            .get("file_count")
                            .and_then(|value| value.as_u64())
                            .unwrap_or(0),
                        format_size(
                            payload
                                .get("size_bytes")
                                .and_then(|value| value.as_u64())
                                .unwrap_or(0)
                        )
                    ));
                }
            }
        }
    }
    if args.limit > 0 {
        if let Some(entries) = index.get("entries").and_then(|value| value.as_array()) {
            if !entries.is_empty() {
                lines.push("\nlargest files:".to_string());
                let mut items = entries.clone();
                items.sort_by_key(|entry| {
                    std::cmp::Reverse(
                        entry
                            .get("size_bytes")
                            .and_then(|value| value.as_u64())
                            .unwrap_or(0),
                    )
                });
                for entry in items.into_iter().take(args.limit) {
                    lines.push(format!(
                        "- {}: {} ({}, {})",
                        entry
                            .get("area")
                            .and_then(|value| value.as_str())
                            .unwrap_or("unknown"),
                        entry
                            .get("relative_path")
                            .and_then(|value| value.as_str())
                            .or_else(|| entry.get("path").and_then(|value| value.as_str()))
                            .unwrap_or("unknown"),
                        format_size(
                            entry
                                .get("size_bytes")
                                .and_then(|value| value.as_u64())
                                .unwrap_or(0)
                        ),
                        entry
                            .get("modified_at")
                            .and_then(|value| value.as_u64())
                            .map(|value| value.to_string())
                            .unwrap_or_else(|| "unknown".to_string())
                    ));
                }
            }
        }
    }
    Ok(CommandResult {
        code: 0,
        stdout: format!("{}\n", lines.join("\n")),
        stderr: String::new(),
    })
}

fn run_cache_prune(run_dir: &Path, args: &CachePruneArgs) -> Result<CommandResult, String> {
    let plan = load_plan(run_dir)?;
    let cache = cache_config_from_plan(&plan);
    let result = prune_cache(&cache, args.max_age_days, &args.area, args.dry_run)?;
    let removed_files = result
        .get("removed_files")
        .and_then(|value| value.as_u64())
        .unwrap_or(0);
    let removed_bytes = result
        .get("removed_bytes")
        .and_then(|value| value.as_u64())
        .unwrap_or(0);
    let mut lines = vec![format!(
        "{} {} files, {}.",
        if args.dry_run {
            "Would remove"
        } else {
            "Removed"
        },
        removed_files,
        format_size(removed_bytes)
    )];
    if let Some(areas) = result.get("areas").and_then(|value| value.as_object()) {
        if !areas.is_empty() {
            lines.push("areas:".to_string());
            for area in CACHE_AREAS {
                if let Some(payload) = areas.get(area).and_then(|value| value.as_object()) {
                    lines.push(format!(
                        "- {}: {} files, {}",
                        area,
                        payload
                            .get("removed_files")
                            .and_then(|value| value.as_u64())
                            .unwrap_or(0),
                        format_size(
                            payload
                                .get("removed_bytes")
                                .and_then(|value| value.as_u64())
                                .unwrap_or(0)
                        ),
                    ));
                }
            }
        }
    }
    let cache_status = print_cache_status(
        run_dir,
        &CacheStatusArgs {
            refresh: false,
            limit: 5,
        },
    )?;
    lines.push("\ncache status:\n".to_string());
    lines.push(cache_status.stdout.trim_end().to_string());
    Ok(CommandResult {
        code: 0,
        stdout: format!("{}\n", lines.join("\n")),
        stderr: String::new(),
    })
}

fn run_host_probe(run_dir: &Path, args: &HostProbeArgs) -> Result<CommandResult, String> {
    let payload = if args.refresh || load_host_probe(run_dir).is_none() {
        capture_host_probe(run_dir)?
    } else {
        load_host_probe(run_dir).unwrap_or_default()
    };
    let mut stdout = serde_json::to_string_pretty(&payload)
        .map_err(|err| format!("Could not render host probe: {err}"))?;
    let plan = load_plan(run_dir)?;
    if let Some(drift) = host_drift_message(&plan.host_facts, &payload) {
        stdout.push_str(&format!("\n\nhost drift: {drift}"));
    }
    if args.history {
        let paths = host_probe_history_paths(run_dir);
        stdout.push_str(&format!("\n\nhistory ({}):", paths.len()));
        for path in paths
            .into_iter()
            .rev()
            .take(10)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
        {
            stdout.push_str(&format!("\n- {}", path.display()));
        }
    }
    stdout.push('\n');
    Ok(CommandResult {
        code: 0,
        stdout,
        stderr: String::new(),
    })
}

fn step_back_stage(
    ctx: &Context,
    run_dir: &Path,
    stage: &str,
    dry_run: bool,
) -> Result<CommandResult, String> {
    let plan = load_plan(run_dir)?;
    let reset_stages = stage_reset_order(run_dir, stage)?;
    let mut reset_files = Vec::new();
    for item in &reset_stages {
        for (path, _) in stage_placeholder_content(&plan, run_dir, item)? {
            reset_files.push(path);
        }
    }
    let mut lines = vec!["Will reset stages:".to_string()];
    lines.extend(reset_stages.iter().map(|item| format!("- {item}")));
    lines.push("\nWill reset files:".to_string());
    lines.extend(
        reset_files
            .iter()
            .map(|path| format!("- {}", path.display())),
    );
    if dry_run {
        lines.push("\nDry run. No files changed.".to_string());
        lines.push(print_status_after_action(ctx, run_dir)?);
        return Ok(CommandResult {
            code: 0,
            stdout: format!("{}\n", lines.join("\n")),
            stderr: String::new(),
        });
    }
    for item in &reset_stages {
        for (path, content) in stage_placeholder_content(&plan, run_dir, item)? {
            write_text(&path, &content)?;
        }
    }
    if matches!(
        pipeline_stage_kind_for(&plan, run_dir, stage)?,
        PipelineStageKind::Execution | PipelineStageKind::Verification
    ) {
        let probe_path = host_probe_path(run_dir);
        if probe_path.exists() {
            let _ = fs::remove_file(&probe_path);
        }
    }
    sync_run_artifacts(ctx, run_dir)?;
    lines.push("\nReset complete.".to_string());
    lines.push(print_status_after_action(ctx, run_dir)?);
    Ok(CommandResult {
        code: 0,
        stdout: format!("{}\n", lines.join("\n")),
        stderr: String::new(),
    })
}

fn recheck_stage(
    ctx: &Context,
    run_dir: &Path,
    stage: &str,
    dry_run: bool,
) -> Result<CommandResult, String> {
    let plan = load_plan(run_dir)?;
    if pipeline_stage_kind_for(&plan, run_dir, stage)? != PipelineStageKind::Verification {
        return Err(format!("Unsupported recheck stage: {stage}"));
    }
    if let Some(execution_stage) =
        first_stage_id_for_kind(&plan, run_dir, PipelineStageKind::Execution)?
    {
        if !is_stage_complete(run_dir, &execution_stage)? {
            return Err(
                "Execution stage must be complete before rechecking verification.".to_string(),
            );
        }
    }
    let reset_files: Vec<PathBuf> = stage_placeholder_content(&plan, run_dir, stage)?
        .into_iter()
        .map(|(path, _)| path)
        .collect();
    let mut lines = vec![
        "Will reset stage for clean rerun:".to_string(),
        format!("- {stage}"),
        "\nWill reset files:".to_string(),
    ];
    lines.extend(
        reset_files
            .iter()
            .map(|path| format!("- {}", path.display())),
    );
    if dry_run {
        lines.push("\nDry run. No files changed.".to_string());
        lines.push(print_status_after_action(ctx, run_dir)?);
        return Ok(CommandResult {
            code: 0,
            stdout: format!("{}\n", lines.join("\n")),
            stderr: String::new(),
        });
    }
    for (path, content) in stage_placeholder_content(&plan, run_dir, stage)? {
        write_text(&path, &content)?;
    }
    sync_run_artifacts(ctx, run_dir)?;
    lines.push("\nRecheck reset complete.".to_string());
    lines.push(print_status_after_action(ctx, run_dir)?);
    Ok(CommandResult {
        code: 0,
        stdout: format!("{}\n", lines.join("\n")),
        stderr: String::new(),
    })
}

fn refresh_stage_prompt(
    run_dir: &Path,
    stage: &str,
    dry_run: bool,
) -> Result<CommandResult, String> {
    let prompt_text = render_stage_prompt(run_dir, stage)?;
    let prompt_path = stage_prompt_path(run_dir, stage)?;
    if dry_run {
        return Ok(CommandResult {
            code: 0,
            stdout: prompt_text,
            stderr: String::new(),
        });
    }
    write_text(&prompt_path, &prompt_text)?;
    Ok(CommandResult {
        code: 0,
        stdout: format!("Refreshed prompt: {}\n", prompt_path.display()),
        stderr: String::new(),
    })
}

fn refresh_all_stage_prompts(run_dir: &Path, dry_run: bool) -> Result<CommandResult, String> {
    let stages = available_stages(run_dir)?;
    let mut chunks = Vec::new();
    for (index, stage) in stages.iter().enumerate() {
        let result = refresh_stage_prompt(run_dir, stage, dry_run)?;
        chunks.push(result.stdout.trim_end().to_string());
        if dry_run && index + 1 < stages.len() {
            chunks.push("\n---\n".to_string());
        }
    }
    Ok(CommandResult {
        code: 0,
        stdout: format!("{}\n", chunks.join("\n")),
        stderr: String::new(),
    })
}

fn follow_up_prompt_path(run_dir: &Path, prompt_source: Option<&str>) -> PathBuf {
    let augmented = augmented_task_path(run_dir);
    let improvement = run_dir.join("verification").join("improvement-request.md");
    match prompt_source {
        Some("augmented") => augmented,
        Some("improvement") => improvement,
        _ => {
            if augmented.exists()
                && !output_looks_placeholder(
                    "augmented-task",
                    &read_text(&augmented).unwrap_or_default(),
                )
            {
                augmented
            } else {
                improvement
            }
        }
    }
}

fn create_follow_up_run(
    ctx: &Context,
    run_dir: &Path,
    args: &RerunArgs,
) -> Result<CommandResult, String> {
    let plan = load_plan(run_dir)?;
    let verification_stage =
        first_stage_id_for_kind(&plan, run_dir, PipelineStageKind::Verification)?
            .ok_or_else(|| "Run does not define a verification stage.".to_string())?;
    if !is_stage_complete(run_dir, &verification_stage)? {
        return Err("Verification stage is still pending. Run verification first.".to_string());
    }
    let prompt_file = follow_up_prompt_path(run_dir, args.prompt_source.as_deref());
    if !prompt_file.exists() {
        return Err(format!(
            "Missing follow-up prompt file: {}",
            prompt_file.display()
        ));
    }
    let prompt_label = if prompt_file == augmented_task_path(run_dir) {
        "augmented-task"
    } else {
        "improvement-request"
    };
    let prompt_text = read_text(&prompt_file)?;
    if output_looks_placeholder(prompt_label, &prompt_text) {
        return Err(format!(
            "Follow-up prompt is still a placeholder: {}",
            prompt_file.display()
        ));
    }
    let output_dir = args
        .output_dir
        .clone()
        .unwrap_or_else(|| run_dir.parent().unwrap_or(run_dir).to_path_buf());
    let title = args.title.clone().unwrap_or_else(|| {
        format!(
            "{}-improve",
            run_dir.file_name().unwrap_or_default().to_string_lossy()
        )
    });
    if args.dry_run {
        return Ok(CommandResult {
            code: 0,
            stdout: format!(
                "Would create follow-up run from `{}`\nworkspace: {}\noutput-dir: {}\ntitle: {}\nprompt-format: {}\nsummary-language: {}\nintake-research: {}\nstage-research: {}\nexecution-network: {}\ncache-root: {}\ncache-policy: {}\n",
                prompt_file.display(),
                plan.workspace,
                output_dir.display(),
                title,
                plan.prompt_format,
                plan.summary_language,
                plan.intake_research_mode,
                plan.stage_research_mode,
                plan.execution_network_mode,
                cache_config_from_plan(&plan).root,
                cache_config_from_plan(&plan).policy,
            ),
            stderr: String::new(),
        });
    }
    let new_run = create_run(
        ctx,
        &prompt_text,
        &plan_workspace(&plan),
        &output_dir,
        Some(&title),
        &plan.prompt_format,
        &plan.summary_language,
        &plan.intake_research_mode,
        &plan.stage_research_mode,
        &plan.execution_network_mode,
        &cache_config_from_plan(&plan).root,
        &cache_config_from_plan(&plan).policy,
        Path::new(&plan.pipeline.source)
            .exists()
            .then_some(Path::new(&plan.pipeline.source)),
    )?;
    sync_run_artifacts(ctx, &new_run)?;
    let status = status_report(ctx, &new_run)?;
    Ok(CommandResult {
        code: 0,
        stdout: format!(
            "{}\n\nNew run status:\n\n{}\n",
            new_run.display(),
            status_text(&status)
        ),
        stderr: String::new(),
    })
}

fn copy_to_clipboard(text: &str) -> Result<(), String> {
    let mut child = Command::new("pbcopy")
        .stdin(Stdio::piped())
        .spawn()
        .map_err(|err| format!("Could not start pbcopy: {err}"))?;
    if let Some(mut stdin) = child.stdin.take() {
        stdin
            .write_all(text.as_bytes())
            .map_err(|err| format!("Could not write clipboard contents: {err}"))?;
    }
    let status = child
        .wait()
        .map_err(|err| format!("Could not wait for pbcopy: {err}"))?;
    if status.success() {
        Ok(())
    } else {
        Err("pbcopy exited with a failure status.".to_string())
    }
}

fn parse_start_args(extra: &[&str]) -> Result<StartArgs, String> {
    let mut args = StartArgs::default();
    let mut index = 0usize;
    while index < extra.len() {
        match extra[index] {
            "--force" => args.force = true,
            "--dry-run" => args.dry_run = true,
            "--oss" => args.oss = true,
            "--color" => {
                index += 1;
                args.color = Some(
                    extra
                        .get(index)
                        .ok_or("--color requires a value")?
                        .to_string(),
                );
            }
            "--model" => {
                index += 1;
                args.model = Some(
                    extra
                        .get(index)
                        .ok_or("--model requires a value")?
                        .to_string(),
                );
            }
            "--profile" => {
                index += 1;
                args.profile = Some(
                    extra
                        .get(index)
                        .ok_or("--profile requires a value")?
                        .to_string(),
                );
            }
            value if !value.starts_with("--") && args.stage.is_none() => {
                args.stage = Some(value.to_string())
            }
            other => return Err(format!("Unexpected argument: {other}")),
        }
        index += 1;
    }
    Ok(args)
}

fn parse_stage_only_args(extra: &[&str]) -> Result<StageOnlyArgs, String> {
    let mut args = StageOnlyArgs::default();
    let mut index = 0usize;
    while index < extra.len() {
        match extra[index] {
            "--dry-run" => args.dry_run = true,
            value if !value.starts_with("--") && args.stage.is_empty() => {
                args.stage = value.to_string()
            }
            other => return Err(format!("Unexpected argument: {other}")),
        }
        index += 1;
    }
    if args.stage.is_empty() {
        return Err("This command requires <stage>.".to_string());
    }
    Ok(args)
}

fn parse_show_args(extra: &[&str]) -> Result<ShowArgs, String> {
    let mut args = ShowArgs::default();
    let mut index = 0usize;
    while index < extra.len() {
        match extra[index] {
            "--raw" => args.raw = true,
            value if !value.starts_with("--") && args.stage.is_empty() => {
                args.stage = value.to_string()
            }
            other => return Err(format!("Unexpected argument: {other}")),
        }
        index += 1;
    }
    if args.stage.is_empty() {
        return Err("show/copy require <stage>.".to_string());
    }
    Ok(args)
}

fn parse_cache_status_args(extra: &[&str]) -> Result<CacheStatusArgs, String> {
    let mut args = CacheStatusArgs {
        refresh: false,
        limit: 5,
    };
    let mut index = 0usize;
    while index < extra.len() {
        match extra[index] {
            "--refresh" => args.refresh = true,
            "--limit" => {
                index += 1;
                args.limit = extra
                    .get(index)
                    .ok_or("--limit requires a value")?
                    .parse::<usize>()
                    .map_err(|_| "Invalid --limit value".to_string())?;
            }
            other => return Err(format!("Unexpected argument: {other}")),
        }
        index += 1;
    }
    Ok(args)
}

fn parse_cache_prune_args(extra: &[&str]) -> Result<CachePruneArgs, String> {
    let mut args = CachePruneArgs::default();
    let mut index = 0usize;
    while index < extra.len() {
        match extra[index] {
            "--max-age-days" => {
                index += 1;
                args.max_age_days = Some(
                    extra
                        .get(index)
                        .ok_or("--max-age-days requires a value")?
                        .parse::<i64>()
                        .map_err(|_| "Invalid --max-age-days value".to_string())?,
                );
            }
            "--area" => {
                index += 1;
                args.area.push(
                    extra
                        .get(index)
                        .ok_or("--area requires a value")?
                        .to_string(),
                );
            }
            "--dry-run" => args.dry_run = true,
            other => return Err(format!("Unexpected argument: {other}")),
        }
        index += 1;
    }
    Ok(args)
}

fn parse_host_probe_args(extra: &[&str]) -> Result<HostProbeArgs, String> {
    let mut args = HostProbeArgs::default();
    for item in extra {
        match *item {
            "--refresh" => args.refresh = true,
            "--history" => args.history = true,
            other => return Err(format!("Unexpected argument: {other}")),
        }
    }
    Ok(args)
}

fn parse_rerun_args(extra: &[&str]) -> Result<RerunArgs, String> {
    let mut args = RerunArgs::default();
    let mut index = 0usize;
    while index < extra.len() {
        match extra[index] {
            "--dry-run" => args.dry_run = true,
            "--title" => {
                index += 1;
                args.title = Some(
                    extra
                        .get(index)
                        .ok_or("--title requires a value")?
                        .to_string(),
                );
            }
            "--output-dir" => {
                index += 1;
                args.output_dir = Some(PathBuf::from(
                    extra.get(index).ok_or("--output-dir requires a value")?,
                ));
            }
            "--prompt-source" => {
                index += 1;
                args.prompt_source = Some(
                    extra
                        .get(index)
                        .ok_or("--prompt-source requires a value")?
                        .to_string(),
                );
            }
            other => return Err(format!("Unexpected argument: {other}")),
        }
        index += 1;
    }
    Ok(args)
}

fn parse_interview_args(extra: &[String]) -> Result<InterviewArgs, String> {
    let mut args = InterviewArgs {
        workspace: PathBuf::from("."),
        output_dir: default_run_root(),
        language: "ru".to_string(),
        max_questions: 6,
        ..InterviewArgs::default()
    };
    let mut index = 0usize;
    while index < extra.len() {
        match extra[index].as_str() {
            "--task" => {
                index += 1;
                args.task = Some(extra.get(index).ok_or("--task requires a value")?.clone());
            }
            "--task-file" => {
                index += 1;
                args.task_file = Some(PathBuf::from(
                    extra.get(index).ok_or("--task-file requires a value")?,
                ));
            }
            "--workspace" => {
                index += 1;
                args.workspace =
                    PathBuf::from(extra.get(index).ok_or("--workspace requires a value")?);
            }
            "--output-dir" => {
                index += 1;
                args.output_dir =
                    PathBuf::from(extra.get(index).ok_or("--output-dir requires a value")?);
            }
            "--title" => {
                index += 1;
                args.title = Some(extra.get(index).ok_or("--title requires a value")?.clone());
            }
            "--language" => {
                index += 1;
                args.language
                    .clone_from(extra.get(index).ok_or("--language requires a value")?);
            }
            "--max-questions" => {
                index += 1;
                args.max_questions = extra
                    .get(index)
                    .ok_or("--max-questions requires a value")?
                    .parse::<usize>()
                    .map_err(|_| "Invalid --max-questions value".to_string())?;
            }
            other => return Err(format!("Unexpected argument: {other}")),
        }
        index += 1;
    }
    Ok(args)
}

fn parse_interview_finalize_args(extra: &[String]) -> Result<InterviewFinalizeArgs, String> {
    let mut args = InterviewFinalizeArgs {
        workspace: PathBuf::from("."),
        language: "ru".to_string(),
        ..InterviewFinalizeArgs::default()
    };
    let mut index = 0usize;
    while index < extra.len() {
        match extra[index].as_str() {
            "--task" => {
                index += 1;
                args.task = Some(extra.get(index).ok_or("--task requires a value")?.clone());
            }
            "--task-file" => {
                index += 1;
                args.task_file = Some(PathBuf::from(
                    extra.get(index).ok_or("--task-file requires a value")?,
                ));
            }
            "--workspace" => {
                index += 1;
                args.workspace =
                    PathBuf::from(extra.get(index).ok_or("--workspace requires a value")?);
            }
            "--session-dir" => {
                index += 1;
                args.session_dir =
                    PathBuf::from(extra.get(index).ok_or("--session-dir requires a value")?);
            }
            "--answers-file" => {
                index += 1;
                args.answers_file =
                    PathBuf::from(extra.get(index).ok_or("--answers-file requires a value")?);
            }
            "--language" => {
                index += 1;
                args.language
                    .clone_from(extra.get(index).ok_or("--language requires a value")?);
            }
            other => return Err(format!("Unexpected argument: {other}")),
        }
        index += 1;
    }
    if args.session_dir.as_os_str().is_empty() {
        return Err("--session-dir is required.".to_string());
    }
    if args.answers_file.as_os_str().is_empty() {
        return Err("--answers-file is required.".to_string());
    }
    Ok(args)
}

fn parse_create_run_args(extra: &[String]) -> Result<CreateRunArgs, String> {
    let mut args = CreateRunArgs {
        workspace: PathBuf::from("."),
        output_dir: default_run_root(),
        prompt_format: "markdown".to_string(),
        summary_language: "ru".to_string(),
        intake_research: "research-first".to_string(),
        stage_research: "local-first".to_string(),
        execution_network: "fetch-if-needed".to_string(),
        cache_root: "~/.cache/multi-agent-pipeline".to_string(),
        cache_policy: "reuse".to_string(),
        ..CreateRunArgs::default()
    };
    let mut index = 0usize;
    while index < extra.len() {
        match extra[index].as_str() {
            "--task" => {
                index += 1;
                args.task = Some(extra.get(index).ok_or("--task requires a value")?.clone());
            }
            "--task-file" => {
                index += 1;
                args.task_file = Some(PathBuf::from(
                    extra.get(index).ok_or("--task-file requires a value")?,
                ));
            }
            "--workspace" => {
                index += 1;
                args.workspace =
                    PathBuf::from(extra.get(index).ok_or("--workspace requires a value")?);
            }
            "--output-dir" => {
                index += 1;
                args.output_dir =
                    PathBuf::from(extra.get(index).ok_or("--output-dir requires a value")?);
            }
            "--title" => {
                index += 1;
                args.title = Some(extra.get(index).ok_or("--title requires a value")?.clone());
            }
            "--prompt-format" => {
                index += 1;
                args.prompt_format
                    .clone_from(extra.get(index).ok_or("--prompt-format requires a value")?);
            }
            "--summary-language" => {
                index += 1;
                args.summary_language.clone_from(
                    extra
                        .get(index)
                        .ok_or("--summary-language requires a value")?,
                );
            }
            "--intake-research" => {
                index += 1;
                args.intake_research.clone_from(
                    extra
                        .get(index)
                        .ok_or("--intake-research requires a value")?,
                );
            }
            "--stage-research" => {
                index += 1;
                args.stage_research.clone_from(
                    extra
                        .get(index)
                        .ok_or("--stage-research requires a value")?,
                );
            }
            "--execution-network" => {
                index += 1;
                args.execution_network.clone_from(
                    extra
                        .get(index)
                        .ok_or("--execution-network requires a value")?,
                );
            }
            "--cache-root" => {
                index += 1;
                args.cache_root
                    .clone_from(extra.get(index).ok_or("--cache-root requires a value")?);
            }
            "--cache-policy" => {
                index += 1;
                args.cache_policy
                    .clone_from(extra.get(index).ok_or("--cache-policy requires a value")?);
            }
            "--interview-session" => {
                index += 1;
                args.interview_session = Some(PathBuf::from(
                    extra
                        .get(index)
                        .ok_or("--interview-session requires a value")?,
                ));
            }
            "--pipeline-file" => {
                index += 1;
                args.pipeline_file = Some(PathBuf::from(
                    extra.get(index).ok_or("--pipeline-file requires a value")?,
                ));
            }
            other => return Err(format!("Unexpected argument: {other}")),
        }
        index += 1;
    }
    if args.task.is_none() && args.task_file.is_none() {
        return Err("Provide --task or --task-file.".to_string());
    }
    Ok(args)
}

fn parse_run_args(extra: &[String]) -> Result<RunArgs, String> {
    let mut args = RunArgs {
        workspace: PathBuf::from("."),
        output_dir: default_run_root(),
        prompt_format: "markdown".to_string(),
        summary_language: "ru".to_string(),
        intake_research: "research-first".to_string(),
        stage_research: "local-first".to_string(),
        execution_network: "fetch-if-needed".to_string(),
        cache_root: "~/.cache/multi-agent-pipeline".to_string(),
        cache_policy: "reuse".to_string(),
        until: "review".to_string(),
        max_questions: 6,
        ..RunArgs::default()
    };
    let mut index = 0usize;
    while index < extra.len() {
        match extra[index].as_str() {
            "--task" => {
                index += 1;
                args.task = Some(extra.get(index).ok_or("--task requires a value")?.clone());
            }
            "--task-file" => {
                index += 1;
                args.task_file = Some(PathBuf::from(
                    extra.get(index).ok_or("--task-file requires a value")?,
                ));
            }
            "--workspace" => {
                index += 1;
                args.workspace =
                    PathBuf::from(extra.get(index).ok_or("--workspace requires a value")?);
            }
            "--output-dir" => {
                index += 1;
                args.output_dir =
                    PathBuf::from(extra.get(index).ok_or("--output-dir requires a value")?);
            }
            "--title" => {
                index += 1;
                args.title = Some(extra.get(index).ok_or("--title requires a value")?.clone());
            }
            "--prompt-format" => {
                index += 1;
                args.prompt_format
                    .clone_from(extra.get(index).ok_or("--prompt-format requires a value")?);
            }
            "--summary-language" => {
                index += 1;
                args.summary_language.clone_from(
                    extra
                        .get(index)
                        .ok_or("--summary-language requires a value")?,
                );
            }
            "--intake-research" => {
                index += 1;
                args.intake_research.clone_from(
                    extra
                        .get(index)
                        .ok_or("--intake-research requires a value")?,
                );
            }
            "--stage-research" => {
                index += 1;
                args.stage_research.clone_from(
                    extra
                        .get(index)
                        .ok_or("--stage-research requires a value")?,
                );
            }
            "--execution-network" => {
                index += 1;
                args.execution_network.clone_from(
                    extra
                        .get(index)
                        .ok_or("--execution-network requires a value")?,
                );
            }
            "--cache-root" => {
                index += 1;
                args.cache_root
                    .clone_from(extra.get(index).ok_or("--cache-root requires a value")?);
            }
            "--cache-policy" => {
                index += 1;
                args.cache_policy
                    .clone_from(extra.get(index).ok_or("--cache-policy requires a value")?);
            }
            "--pipeline-file" => {
                index += 1;
                args.pipeline_file = Some(PathBuf::from(
                    extra.get(index).ok_or("--pipeline-file requires a value")?,
                ));
            }
            "--until" => {
                index += 1;
                args.until
                    .clone_from(extra.get(index).ok_or("--until requires a value")?);
            }
            "--auto-approve" => args.auto_approve = true,
            "--skip-interview" => args.skip_interview = true,
            "--max-questions" => {
                index += 1;
                args.max_questions = extra
                    .get(index)
                    .ok_or("--max-questions requires a value")?
                    .parse::<usize>()
                    .map_err(|_| "Invalid --max-questions value".to_string())?;
            }
            other => return Err(format!("Unexpected argument: {other}")),
        }
        index += 1;
    }
    if args.task.is_none() && args.task_file.is_none() {
        return Err("Provide --task or --task-file.".to_string());
    }
    Ok(args)
}

fn read_task_text(task: Option<&String>, task_file: Option<&PathBuf>) -> Result<String, String> {
    if let Some(task) = task {
        return Ok(task.trim().to_string());
    }
    if let Some(task_file) = task_file {
        return read_text(&task_file.expanduser());
    }
    Err("Provide --task or --task-file.".to_string())
}

fn extract_json_object(text: &str) -> Result<Value, String> {
    let trimmed = text.trim();
    if trimmed.starts_with('{') {
        let payload: Value = serde_json::from_str(trimmed)
            .map_err(|err| format!("Interview agent returned invalid JSON: {err}"))?;
        if payload.is_object() {
            return Ok(payload);
        }
    }
    let Some(start) = text.find('{') else {
        return Err("Interview agent did not return a JSON object.".to_string());
    };
    let Some(end) = text.rfind('}') else {
        return Err("Interview agent did not return a JSON object.".to_string());
    };
    let slice = &text[start..=end];
    let payload: Value = serde_json::from_str(slice)
        .map_err(|err| format!("Interview agent returned invalid JSON: {err}"))?;
    if payload.is_object() {
        Ok(payload)
    } else {
        Err("Interview agent returned JSON, but it was not an object.".to_string())
    }
}

fn using_responses_backend(ctx: &Context) -> bool {
    matches!(
        ctx.stage0_backend.trim().to_lowercase().as_str(),
        "responses" | "openai" | "responses-api"
    )
}

fn append_log_line(path: &Path, line: &str) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|err| format!("Could not create {}: {err}", parent.display()))?;
    }
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|err| format!("Could not open {}: {err}", path.display()))?;
    file.write_all(line.as_bytes())
        .and_then(|_| file.write_all(b"\n"))
        .map_err(|err| format!("Could not write {}: {err}", path.display()))
}

fn responses_url(base: &str, path: &str) -> String {
    format!(
        "{}/{}",
        base.trim_end_matches('/'),
        path.trim_start_matches('/')
    )
}

fn short_hash(value: &str) -> String {
    value.chars().take(16).collect()
}

fn responses_prompt_cache_key(ctx: &Context, label: &str, prompt: &str) -> String {
    let prompt_hashes = split_prompt_for_cache(prompt);
    format!(
        "{}:{}:{}:{}",
        ctx.openai_prompt_cache_key_prefix,
        slugify(label),
        slugify(&ctx.openai_model),
        short_hash(&prompt_hashes.stable_prefix)
    )
}

fn request_idempotency_key(method: &str, url: &str, body: Option<&str>) -> Option<String> {
    if method != "POST" {
        return None;
    }
    let mut digest = StableDigest::new();
    digest.update_text(method);
    digest.update_text(url);
    digest.update_text(body.unwrap_or_default());
    Some(format!("agpipe-{}", digest.hex()))
}

fn extract_response_output_text(payload: &Value) -> String {
    if let Some(text) = payload.get("output_text").and_then(|value| value.as_str()) {
        return text.to_string();
    }
    let mut parts = Vec::new();
    if let Some(items) = payload.get("output").and_then(|value| value.as_array()) {
        for item in items {
            if item.get("type").and_then(|value| value.as_str()) != Some("message") {
                continue;
            }
            if let Some(content) = item.get("content").and_then(|value| value.as_array()) {
                for part in content {
                    match part.get("type").and_then(|value| value.as_str()) {
                        Some("output_text") | Some("text") => {
                            if let Some(text) = part.get("text").and_then(|value| value.as_str()) {
                                parts.push(text.to_string());
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
    }
    parts.join("\n\n").trim().to_string()
}

fn extract_response_structured_output(payload: &Value) -> Result<Value, String> {
    if let Some(parsed) = payload.get("output_parsed") {
        if parsed.is_object() || parsed.is_array() {
            return Ok(parsed.clone());
        }
    }
    let output_text = extract_response_output_text(payload);
    let trimmed = output_text.trim();
    if trimmed.is_empty() {
        return Err("Responses API did not return structured JSON output text.".to_string());
    }
    serde_json::from_str(trimmed)
        .map_err(|err| format!("Responses API returned invalid structured JSON: {err}"))
}

fn extract_response_usage(payload: &Value) -> TokenUsage {
    let usage = payload.get("usage").cloned().unwrap_or_else(|| json!({}));
    let prompt_tokens = usage
        .get("input_tokens")
        .or_else(|| usage.get("prompt_tokens"))
        .and_then(|value| value.as_u64())
        .unwrap_or(0);
    let completion_tokens = usage
        .get("output_tokens")
        .or_else(|| usage.get("completion_tokens"))
        .and_then(|value| value.as_u64())
        .unwrap_or(0);
    let total_tokens = usage
        .get("total_tokens")
        .and_then(|value| value.as_u64())
        .unwrap_or(prompt_tokens.saturating_add(completion_tokens));
    let cached_prompt_tokens = usage
        .get("input_tokens_details")
        .or_else(|| usage.get("prompt_tokens_details"))
        .and_then(|value| value.get("cached_tokens"))
        .and_then(|value| value.as_u64())
        .unwrap_or(0);
    TokenUsage {
        source: "openai-responses".to_string(),
        prompt_tokens,
        cached_prompt_tokens,
        completion_tokens,
        total_tokens,
        estimated_saved_tokens: cached_prompt_tokens,
    }
}

fn response_error_details(payload: &Value) -> Option<String> {
    let mut details = Vec::new();
    if let Some(error) = payload.get("error").and_then(|value| value.as_object()) {
        if let Some(message) = error.get("message").and_then(|value| value.as_str()) {
            details.push(message.trim().to_string());
        }
        if let Some(kind) = error
            .get("type")
            .or_else(|| error.get("code"))
            .and_then(|value| value.as_str())
        {
            details.push(format!("type={kind}"));
        }
    }
    if let Some(incomplete) = payload
        .get("incomplete_details")
        .and_then(|value| value.as_object())
    {
        if let Some(reason) = incomplete.get("reason").and_then(|value| value.as_str()) {
            details.push(format!("incomplete_reason={reason}"));
        } else if let Ok(text) = serde_json::to_string(incomplete) {
            details.push(format!("incomplete_details={text}"));
        }
    }
    if details.is_empty() {
        None
    } else {
        Some(details.join("; "))
    }
}

fn response_error_details_from_text(text: &str) -> Option<String> {
    serde_json::from_str::<Value>(text)
        .ok()
        .and_then(|payload| response_error_details(&payload))
}

fn parse_retry_after(headers: &BTreeMap<String, String>) -> Option<Duration> {
    let value = headers.get("retry-after")?.trim();
    let seconds = value.parse::<u64>().ok()?;
    Some(Duration::from_secs(seconds.min(30)))
}

fn retry_sleep_duration(attempt: u32, response: Option<&HttpJsonResponse>) -> Duration {
    if let Some(delay) = response.and_then(|value| parse_retry_after(&value.headers)) {
        return delay;
    }
    let exponent = attempt.saturating_sub(1).min(5);
    let base_ms = 250u64.saturating_mul(1u64 << exponent);
    let jitter = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_millis() as u64
        % 250;
    Duration::from_millis((base_ms + jitter).min(5_000))
}

fn should_retry_http_status(status: u32) -> bool {
    status == 429 || (500..600).contains(&status)
}

fn perform_http_json_request(
    method: &str,
    url: &str,
    bearer_token: &str,
    body: Option<&str>,
) -> Result<HttpJsonResponse, String> {
    let mut easy = Easy::new();
    easy.url(url)
        .map_err(|err| format!("Could not set request URL `{url}`: {err}"))?;
    easy.follow_location(true)
        .map_err(|err| format!("Could not enable redirects for `{url}`: {err}"))?;
    easy.connect_timeout(Duration::from_secs(30))
        .map_err(|err| format!("Could not set connect timeout for `{url}`: {err}"))?;
    easy.timeout(Duration::from_secs(120))
        .map_err(|err| format!("Could not set request timeout for `{url}`: {err}"))?;

    let mut headers = List::new();
    headers
        .append(&format!("Authorization: Bearer {bearer_token}"))
        .map_err(|err| format!("Could not set auth header for `{url}`: {err}"))?;
    if body.is_some() {
        headers
            .append("Content-Type: application/json")
            .map_err(|err| format!("Could not set content type for `{url}`: {err}"))?;
    }
    if let Some(key) = request_idempotency_key(method, url, body) {
        headers
            .append(&format!("Idempotency-Key: {key}"))
            .map_err(|err| format!("Could not set idempotency key for `{url}`: {err}"))?;
    }
    easy.http_headers(headers)
        .map_err(|err| format!("Could not attach headers for `{url}`: {err}"))?;

    match method {
        "POST" => {
            easy.post(true)
                .map_err(|err| format!("Could not enable POST for `{url}`: {err}"))?;
            if let Some(body) = body {
                easy.post_fields_copy(body.as_bytes())
                    .map_err(|err| format!("Could not attach POST body for `{url}`: {err}"))?;
            }
        }
        "GET" => {}
        other => {
            easy.custom_request(other)
                .map_err(|err| format!("Could not set HTTP method `{other}` for `{url}`: {err}"))?;
        }
    }

    let mut response_body = Vec::new();
    let mut response_headers = BTreeMap::new();
    {
        let mut transfer = easy.transfer();
        transfer
            .header_function(|header| {
                if let Ok(line) = std::str::from_utf8(header) {
                    let trimmed = line.trim();
                    if let Some((name, value)) = trimmed.split_once(':') {
                        response_headers
                            .insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
                    }
                }
                true
            })
            .map_err(|err| format!("Could not set header callback for `{url}`: {err}"))?;
        transfer
            .write_function(|data| {
                response_body.extend_from_slice(data);
                Ok(data.len())
            })
            .map_err(|err| format!("Could not set write callback for `{url}`: {err}"))?;
        transfer
            .perform()
            .map_err(|err| format!("HTTP request to `{url}` failed: {err}"))?;
    }
    let status = easy
        .response_code()
        .map_err(|err| format!("Could not read HTTP status for `{url}`: {err}"))?;
    let body = String::from_utf8(response_body)
        .map_err(|err| format!("HTTP response from `{url}` was not valid UTF-8: {err}"))?;
    Ok(HttpJsonResponse {
        status,
        body,
        headers: response_headers,
    })
}

fn perform_http_json_request_with_retry(
    method: &str,
    url: &str,
    bearer_token: &str,
    body: Option<&str>,
) -> Result<HttpJsonResponse, String> {
    let upper_method = method.trim().to_ascii_uppercase();
    let retry_transport_errors = upper_method == "GET";
    let mut last_transport_error = None;
    for attempt in 1..=RESPONSES_HTTP_MAX_ATTEMPTS {
        match perform_http_json_request(&upper_method, url, bearer_token, body) {
            Ok(response) => {
                if should_retry_http_status(response.status)
                    && attempt < RESPONSES_HTTP_MAX_ATTEMPTS
                {
                    thread::sleep(retry_sleep_duration(attempt, Some(&response)));
                    continue;
                }
                return Ok(response);
            }
            Err(err) => {
                last_transport_error = Some(err);
                if retry_transport_errors && attempt < RESPONSES_HTTP_MAX_ATTEMPTS {
                    thread::sleep(retry_sleep_duration(attempt, None));
                    continue;
                }
                break;
            }
        }
    }
    Err(last_transport_error.unwrap_or_else(|| format!("HTTP request to `{url}` failed.")))
}

fn run_responses_last_message(
    ctx: &Context,
    prompt: &str,
    workdir: &Path,
    artifact_dir: &Path,
    label: &str,
    interrupt_run_dir: Option<&Path>,
    text_format: Option<&ResponseTextFormat>,
) -> Result<String, String> {
    let api_key = ctx.openai_api_key.clone().ok_or_else(|| {
        "OPENAI_API_KEY or AGPIPE_OPENAI_API_KEY is required for Responses backend.".to_string()
    })?;
    fs::create_dir_all(artifact_dir)
        .map_err(|err| format!("Could not create {}: {err}", artifact_dir.display()))?;
    let prompt_path = artifact_dir.join(format!("{label}.prompt.md"));
    let last_message_path = artifact_dir.join(format!("{label}.last.md"));
    let stdout_path = artifact_dir.join(format!("{label}.stdout.log"));
    let stderr_path = artifact_dir.join(format!("{label}.stderr.log"));
    let response_path = artifact_dir.join(format!("{label}.response.json"));
    let response_body_path = artifact_dir.join(format!("{label}.response-body.json"));
    write_text(&prompt_path, prompt)?;
    let prompt_hashes = split_prompt_for_cache(prompt);
    let prompt_cache_key = responses_prompt_cache_key(ctx, label, prompt);
    let store_response = ctx.openai_background || ctx.openai_store;
    let create_url = responses_url(&ctx.openai_api_base, "/responses");
    let mut body = json!({
        "model": ctx.openai_model,
        "input": prompt,
        "store": store_response,
        "background": ctx.openai_background,
        "prompt_cache_key": prompt_cache_key,
        "metadata": {
            "agpipe_label": label,
            "agpipe_workdir": workdir.display().to_string(),
            "agpipe_prompt_prefix_hash": prompt_hashes.stable_prefix,
            "agpipe_prompt_dynamic_hash": prompt_hashes.dynamic_suffix,
        }
    });
    if let Some(format) = text_format {
        body["text"] = json!({
            "format": {
                "type": "json_schema",
                "name": format.name,
                "schema": format.schema,
                "strict": true
            }
        });
    }
    if let Some(retention) = ctx
        .openai_prompt_cache_retention
        .as_ref()
        .filter(|value| !value.trim().is_empty())
    {
        body["prompt_cache_retention"] = Value::String(retention.clone());
    }
    let create_body = serde_json::to_string(&body)
        .map_err(|err| format!("Could not serialize Responses API create body: {err}"))?;
    let create_response =
        perform_http_json_request_with_retry("POST", &create_url, &api_key, Some(&create_body))?;
    write_text(&response_body_path, &create_response.body)?;
    write_text(
        &stdout_path,
        &format!(
            "responses POST {}\nmodel={}\nbackground={}\nstore={}\nhttp_status={}\n",
            create_url,
            ctx.openai_model,
            ctx.openai_background,
            store_response,
            create_response.status
        ),
    )?;
    if !(200..300).contains(&create_response.status) {
        write_text(&stderr_path, &create_response.body)?;
        let detail = response_error_details_from_text(&create_response.body)
            .map(|value| format!(" ({value})"))
            .unwrap_or_default();
        return Err(format!(
            "Responses API create failed during `{label}` with status {}{}. See {}.",
            create_response.status,
            detail,
            stderr_path.display()
        ));
    }
    let mut response_payload: Value = serde_json::from_str(&create_response.body)
        .map_err(|err| format!("Could not parse create response JSON: {err}"))?;
    let response_id = response_payload
        .get("id")
        .and_then(|value| value.as_str())
        .unwrap_or("")
        .to_string();
    let mut status = response_payload
        .get("status")
        .and_then(|value| value.as_str())
        .unwrap_or("completed")
        .to_string();
    append_log_line(
        &stdout_path,
        &format!("created response_id={} status={}", response_id, status),
    )?;
    if response_id.is_empty() && matches!(status.as_str(), "queued" | "in_progress") {
        write_json(&response_path, &response_payload)?;
        write_text(
            &stderr_path,
            &serde_json::to_string_pretty(&response_payload).unwrap_or_default(),
        )?;
        return Err(format!(
            "Responses API request for `{label}` did not return a response id for status `{status}`. See {}.",
            response_path.display()
        ));
    }
    if !response_id.is_empty() && matches!(status.as_str(), "queued" | "in_progress") {
        let retrieve_url =
            responses_url(&ctx.openai_api_base, &format!("/responses/{response_id}"));
        let mut attempts = 0u32;
        while matches!(status.as_str(), "queued" | "in_progress")
            && attempts < RESPONSES_POLL_MAX_ATTEMPTS
        {
            if let Some(run_dir) = interrupt_run_dir {
                check_run_interrupt(run_dir)?;
            }
            thread::sleep(Duration::from_secs(2));
            attempts += 1;
            let poll_response =
                perform_http_json_request_with_retry("GET", &retrieve_url, &api_key, None)?;
            write_text(&response_body_path, &poll_response.body)?;
            if !(200..300).contains(&poll_response.status) {
                write_text(&stderr_path, &poll_response.body)?;
                let detail = response_error_details_from_text(&poll_response.body)
                    .map(|value| format!(" ({value})"))
                    .unwrap_or_default();
                return Err(format!(
                    "Responses API polling failed during `{label}` with status {}{}. See {}.",
                    poll_response.status,
                    detail,
                    stderr_path.display()
                ));
            }
            response_payload = serde_json::from_str(&poll_response.body)
                .map_err(|err| format!("Could not parse retrieve response JSON: {err}"))?;
            status = response_payload
                .get("status")
                .and_then(|value| value.as_str())
                .unwrap_or("completed")
                .to_string();
            append_log_line(
                &stdout_path,
                &format!("poll status={status} attempt={attempts}"),
            )?;
        }
    }
    write_json(&response_path, &response_payload)?;
    if matches!(status.as_str(), "queued" | "in_progress") {
        write_text(
            &stderr_path,
            &serde_json::to_string_pretty(&response_payload).unwrap_or_default(),
        )?;
        return Err(format!(
            "Responses API background request for `{label}` did not complete after {} polls. Last status was `{status}`. See {}.",
            RESPONSES_POLL_MAX_ATTEMPTS,
            response_path.display()
        ));
    }
    if status != "completed" {
        write_text(
            &stderr_path,
            &serde_json::to_string_pretty(&response_payload).unwrap_or_default(),
        )?;
        let detail = response_error_details(&response_payload)
            .map(|value| format!(" ({value})"))
            .unwrap_or_default();
        return Err(format!(
            "Responses API finished `{label}` with terminal status `{status}`{}. See {}.",
            detail,
            response_path.display()
        ));
    }
    let output_text = if text_format.is_some() {
        let structured = extract_response_structured_output(&response_payload).map_err(|err| {
            let _ = write_text(
                &stderr_path,
                &serde_json::to_string_pretty(&response_payload).unwrap_or_default(),
            );
            err
        })?;
        serde_json::to_string_pretty(&structured)
            .map_err(|err| format!("Could not serialize structured Responses output: {err}"))?
    } else {
        extract_response_output_text(&response_payload)
    };
    if output_text.trim().is_empty() {
        write_text(
            &stderr_path,
            &serde_json::to_string_pretty(&response_payload).unwrap_or_default(),
        )?;
        return Err(format!(
            "Responses API did not return message text for `{label}`. See {}.",
            response_path.display()
        ));
    }
    write_text(&last_message_path, &output_text)?;
    let usage = extract_response_usage(&response_payload);
    append_log_line(
        &stdout_path,
        &format!(
            "usage prompt_tokens={} cached_prompt_tokens={} completion_tokens={} total_tokens={}",
            usage.prompt_tokens,
            usage.cached_prompt_tokens,
            usage.completion_tokens,
            usage.total_tokens
        ),
    )?;
    Ok(output_text)
}

fn run_codex_last_message(
    ctx: &Context,
    prompt: &str,
    workdir: &Path,
    artifact_dir: &Path,
    label: &str,
) -> Result<String, String> {
    fs::create_dir_all(artifact_dir)
        .map_err(|err| format!("Could not create {}: {err}", artifact_dir.display()))?;
    let prompt_path = artifact_dir.join(format!("{label}.prompt.md"));
    let last_message_path = artifact_dir.join(format!("{label}.last.md"));
    let stdout_path = artifact_dir.join(format!("{label}.stdout.log"));
    let stderr_path = artifact_dir.join(format!("{label}.stderr.log"));
    write_text(&prompt_path, prompt)?;
    let command = vec![
        ctx.codex_bin.clone(),
        "exec".to_string(),
        "--full-auto".to_string(),
        "--skip-git-repo-check".to_string(),
        "--color".to_string(),
        "never".to_string(),
        "-C".to_string(),
        workdir.display().to_string(),
        "--add-dir".to_string(),
        ctx.repo_root.display().to_string(),
        "--output-last-message".to_string(),
        last_message_path.display().to_string(),
        "-".to_string(),
    ];
    let code = run_prompted_command_capture(&command, prompt, &stdout_path, &stderr_path, None)?;
    if code != 0 {
        let detail = read_text(&stderr_path)
            .or_else(|_| read_text(&stdout_path))
            .unwrap_or_default();
        return Err(format!(
            "codex exec failed during `{label}` with exit code {code}. See {} and {}. {}",
            stdout_path.display(),
            stderr_path.display(),
            detail.chars().take(400).collect::<String>()
        ));
    }
    if !last_message_path.exists() {
        return Err(format!(
            "codex exec did not write the expected last-message artifact for `{label}`."
        ));
    }
    read_text(&last_message_path)
}

fn run_stage0_last_message(
    ctx: &Context,
    prompt: &str,
    workdir: &Path,
    artifact_dir: &Path,
    label: &str,
) -> Result<String, String> {
    if using_responses_backend(ctx) {
        let text_format = responses_text_format_for_label(label);
        run_responses_last_message(
            ctx,
            prompt,
            workdir,
            artifact_dir,
            label,
            None,
            text_format.as_ref(),
        )
    } else {
        run_codex_last_message(ctx, prompt, workdir, artifact_dir, label)
    }
}

fn build_interview_questions_prompt(
    raw_task: &str,
    workspace: &Path,
    language: &str,
    max_questions: usize,
) -> String {
    format!(
        "You are the stage0 interview agent for multi-agent-pipeline.\n\nYour job is to read the raw request, inspect the workspace when useful, and ask the domain-specific clarification questions that are actually needed before building an execution-ready task prompt.\n\nRaw task:\n{}\n\nWorkspace:\n- path: `{}`\n- exists: `{}`\n\nUse the configured structured output schema for the final answer.\n\nRules:\n- preserve the original goal exactly; do not shrink it\n- ask all important domain questions that materially affect decomposition, implementation, or goal verification\n- ask no more than {} questions\n- avoid questions already answered by the raw task\n- prefer concrete engineering questions over generic project-management questions\n- write user-facing questions and reasons in {}\n- if the task is already clear enough, return an empty `questions` list\n",
        raw_task.trim(),
        workspace.display(),
        workspace.exists(),
        max_questions,
        language
    )
}

fn build_interview_finalize_prompt(raw_task: &str, qa_pairs: &[Value], language: &str) -> String {
    let qa_json = serde_json::to_string_pretty(qa_pairs).unwrap_or_else(|_| "[]".to_string());
    format!(
        "You are the stage0 prompt builder for multi-agent-pipeline.\n\nYour job is to turn the raw request plus clarification answers into the final task prompt that will be passed into the Rust agpipe intake and create-run flow.\n\nRaw task:\n{}\n\nClarifications:\n{}\n\nWrite the final task in {}. Return markdown only, with no code fences.\n\nRules:\n- preserve the original goal exactly; do not downgrade it to scaffold-only or architecture-only\n- incorporate the answered constraints and preferences directly\n- carry forward unresolved uncertainties as explicit blockers or open assumptions\n- make the task execution-ready for downstream agents\n- include what counts as done\n- include do-not-regress constraints when the answers imply them\n",
        raw_task.trim(),
        qa_json,
        language
    )
}

fn generate_interview_questions(
    ctx: &Context,
    raw_task: &str,
    workspace: &Path,
    output_root: &Path,
    title: Option<&str>,
    language: &str,
    max_questions: usize,
) -> Result<(PathBuf, Value), String> {
    let session_dir = output_root
        .join("_interviews")
        .join(timestamp_slug(title.unwrap_or("interview")));
    fs::create_dir_all(session_dir.join("logs"))
        .map_err(|err| format!("Could not create {}: {err}", session_dir.display()))?;
    write_text(&session_dir.join("raw-task.md"), raw_task)?;
    let prompt = build_interview_questions_prompt(raw_task, workspace, language, max_questions);
    let cwd =
        env::current_dir().map_err(|err| format!("Could not read current directory: {err}"))?;
    let workdir = if workspace.exists() {
        workspace.to_path_buf()
    } else {
        cwd
    };
    let raw_questions = run_stage0_last_message(
        ctx,
        &prompt,
        &workdir,
        &session_dir.join("logs"),
        "interview-questions",
    )?;
    let payload = extract_json_object(&raw_questions)?;
    write_json(&session_dir.join("questions.json"), &payload)?;
    Ok((session_dir, payload))
}

fn finalize_interview_prompt(
    ctx: &Context,
    raw_task: &str,
    workspace: &Path,
    session_dir: &Path,
    qa_pairs: &[Value],
    language: &str,
) -> Result<PathBuf, String> {
    write_json(&session_dir.join("answers.json"), &qa_pairs)?;
    let prompt = build_interview_finalize_prompt(raw_task, qa_pairs, language);
    let cwd =
        env::current_dir().map_err(|err| format!("Could not read current directory: {err}"))?;
    let workdir = if workspace.exists() {
        workspace.to_path_buf()
    } else {
        cwd
    };
    let final_task_text = run_stage0_last_message(
        ctx,
        &prompt,
        &workdir,
        &session_dir.join("logs"),
        "interview-finalize",
    )?;
    let final_task_path = session_dir.join("final-task.md");
    write_text(&final_task_path, &final_task_text)?;
    Ok(final_task_path)
}

fn run_interview_session(
    ctx: &Context,
    raw_task: &str,
    workspace: &Path,
    output_root: &Path,
    title: Option<&str>,
    language: &str,
    max_questions: usize,
) -> Result<(PathBuf, PathBuf), String> {
    let (session_dir, payload) = generate_interview_questions(
        ctx,
        raw_task,
        workspace,
        output_root,
        title,
        language,
        max_questions,
    )?;
    let mut qa_pairs = Vec::new();
    if let Some(summary) = payload.get("goal_summary").and_then(|value| value.as_str()) {
        if !summary.trim().is_empty() {
            println!("\nStage0 interview\n");
            println!("Goal summary: {}\n", summary.trim());
        }
    }
    if let Some(questions) = payload.get("questions").and_then(|value| value.as_array()) {
        for (index, item) in questions.iter().enumerate() {
            let question = item
                .get("question")
                .and_then(|value| value.as_str())
                .unwrap_or("")
                .trim();
            if question.is_empty() {
                continue;
            }
            let why = item
                .get("why")
                .and_then(|value| value.as_str())
                .unwrap_or("")
                .trim();
            let required = item
                .get("required")
                .and_then(|value| value.as_bool())
                .unwrap_or(true);
            println!("{}. {}", index + 1, question);
            if !why.is_empty() {
                println!("   why: {why}");
            }
            let mut answer = String::new();
            loop {
                print!("> ");
                std::io::stdout()
                    .flush()
                    .map_err(|err| format!("Could not flush stdout: {err}"))?;
                answer.clear();
                std::io::stdin()
                    .read_line(&mut answer)
                    .map_err(|err| format!("Could not read input: {err}"))?;
                if !required || !answer.trim().is_empty() {
                    break;
                }
            }
            qa_pairs.push(json!({
                "id": item.get("id").and_then(|value| value.as_str()).unwrap_or("q"),
                "question": question,
                "answer": answer.trim(),
            }));
            println!();
        }
    }
    write_json(&session_dir.join("answers.json"), &qa_pairs)?;
    let final_task =
        finalize_interview_prompt(ctx, raw_task, workspace, &session_dir, &qa_pairs, language)?;
    Ok((session_dir, final_task))
}

fn maybe_copy_interview_artifacts(session_dir: &Path, run_dir: &Path) -> Result<(), String> {
    let interview_dir = run_dir.join("interview");
    fs::create_dir_all(&interview_dir)
        .map_err(|err| format!("Could not create {}: {err}", interview_dir.display()))?;
    if !session_dir.exists() {
        return Ok(());
    }
    copy_tree_contents(session_dir, &interview_dir)?;
    Ok(())
}

fn copy_tree_contents(source: &Path, target: &Path) -> Result<(), String> {
    for entry in
        fs::read_dir(source).map_err(|err| format!("Could not read {}: {err}", source.display()))?
    {
        let entry = entry.map_err(|err| format!("Could not read dir entry: {err}"))?;
        let from = entry.path();
        let to = target.join(entry.file_name());
        if from.is_dir() {
            fs::create_dir_all(&to)
                .map_err(|err| format!("Could not create {}: {err}", to.display()))?;
            copy_tree_contents(&from, &to)?;
        } else if from.is_file() {
            if let Some(parent) = to.parent() {
                fs::create_dir_all(parent)
                    .map_err(|err| format!("Could not create {}: {err}", parent.display()))?;
            }
            fs::copy(&from, &to).map_err(|err| {
                format!(
                    "Could not copy {} to {}: {err}",
                    from.display(),
                    to.display()
                )
            })?;
        }
    }
    Ok(())
}

pub fn automate_run(
    ctx: &Context,
    run_dir: &Path,
    until: &str,
    auto_approve: bool,
) -> Result<CommandResult, String> {
    let mut combined = String::new();
    let until_rank = until_rank_for_run(run_dir, until)?;
    loop {
        check_run_interrupt(run_dir)?;
        let next = next_stage(ctx, run_dir)?;
        let bucket = stage_resume_bucket_for_run(run_dir, &next)?;
        if bucket == "none" {
            combined.push_str("Pipeline is complete for this run.\n");
            break;
        }
        if bucket == "rerun" {
            combined.push_str("Verification recommends a follow-up rerun.\n");
            break;
        }
        if stage_rank(&bucket) > until_rank {
            break;
        }
        if bucket == "execution" && !auto_approve {
            combined.push_str("Paused before execution.\n");
            break;
        }
        let result = if bucket == "solvers" {
            run_stage_capture(ctx, run_dir, "start-solvers", &[])?
        } else {
            run_stage_capture(ctx, run_dir, "start", &[next.as_str()])?
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
        check_run_interrupt(run_dir)?;
        let next_after = stage_resume_bucket_for_run(run_dir, &next_stage(ctx, run_dir)?)?;
        if next_after == bucket {
            return Err(format!(
                "No stage progress detected after `{bucket}`. Check status and logs."
            ));
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

pub fn execute_named_action(
    ctx: &Context,
    run_dir: &Path,
    action: &str,
) -> Result<CommandResult, String> {
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
    Ok(CommandResult {
        code: 0,
        stdout: format!(
            "Amendment saved in {}.\nRun rewound to `{}`.\nNext: press `n` to run the next stage or `r` to resume the whole pipeline.",
            amendment_path.display(),
            rewind
        ),
        stderr: details,
    })
}

pub fn delete_run(run_dir: &Path) -> Result<(), String> {
    fs::remove_dir_all(run_dir)
        .map_err(|err| format!("Could not delete {}: {err}", run_dir.display()))
}

pub fn choose_prune_candidates(
    root: &Path,
    keep: usize,
    older_than_days: Option<u64>,
) -> Result<Vec<PathBuf>, String> {
    let runs = discover_run_dirs(root)?;
    let protected: Vec<PathBuf> = runs.iter().take(keep).cloned().collect();
    let threshold = older_than_days.map(|days| {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            .saturating_sub(days * 86_400)
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
    let path = amendments_path(run_dir);
    let timestamp = iso_timestamp();
    let mut content = if path.exists() {
        read_text(&path)?
    } else {
        "# Amendments\n\n".to_string()
    };
    if !content.ends_with('\n') {
        content.push('\n');
    }
    content.push_str(&format!("## {timestamp}\n\n{}\n", note.trim()));
    write_text(&path, &content)?;
    Ok(path)
}

pub fn preview_text(run_dir: &Path, max_chars: usize) -> (String, String) {
    let candidates = [
        (
            "Summary",
            "review-summary",
            run_dir.join("review").join("user-summary.md"),
        ),
        (
            "Augmented",
            "augmented-task",
            run_dir.join("verification").join("augmented-task.md"),
        ),
        (
            "Findings",
            "verification",
            run_dir.join("verification").join("findings.md"),
        ),
        (
            "Execution",
            "execution",
            run_dir.join("execution").join("report.md"),
        ),
        ("Brief", "intake", run_dir.join("brief.md")),
    ];
    for (label, placeholder_kind, path) in candidates {
        if !path.exists() {
            continue;
        }
        let Ok(content) = read_text(&path) else {
            continue;
        };
        let trimmed = content.trim();
        if trimmed.is_empty() || output_looks_placeholder(placeholder_kind, trimmed) {
            continue;
        }
        return (label.to_string(), trimmed.chars().take(max_chars).collect());
    }
    (
        "Preview".to_string(),
        "No substantive artifact is available yet.".to_string(),
    )
}

fn log_lines_from_path(path: &Path, line_limit: usize) -> Vec<String> {
    match read_text(path) {
        Ok(content) => {
            let lines: Vec<String> = content.lines().map(|line| line.to_string()).collect();
            if lines.is_empty() {
                vec!["<empty log>".to_string()]
            } else {
                lines
                    .into_iter()
                    .rev()
                    .take(line_limit)
                    .collect::<Vec<_>>()
                    .into_iter()
                    .rev()
                    .collect()
            }
        }
        Err(_) => vec!["Could not read log file.".to_string()],
    }
}

fn latest_log_file(logs_dir: &Path) -> Option<PathBuf> {
    let mut logs: Vec<PathBuf> = fs::read_dir(logs_dir)
        .ok()?
        .filter_map(|entry| entry.ok().map(|item| item.path()))
        .collect();
    if logs.is_empty() {
        return None;
    }
    logs.sort_by_key(|path| path.metadata().and_then(|meta| meta.modified()).ok());
    logs.pop()
}

fn stage_live_log_candidates(stage: &str) -> Vec<String> {
    let trimmed = stage.trim();
    if trimmed.is_empty() || trimmed == "none" || trimmed == "rerun" {
        return Vec::new();
    }
    vec![
        format!("{trimmed}.last.md"),
        format!("{trimmed}.stdout.log"),
        format!("{trimmed}.stderr.log"),
    ]
}

fn stage_live_log_path(logs_dir: &Path, stage: &str) -> Option<PathBuf> {
    for candidate in stage_live_log_candidates(stage) {
        let path = logs_dir.join(candidate);
        if path.exists() {
            return Some(path);
        }
    }
    None
}

pub fn contextual_log_excerpt(
    run_dir: &Path,
    preferred_stage: Option<&str>,
    next_stage: Option<&str>,
    line_limit: usize,
) -> (String, Vec<String>) {
    let logs_dir = run_dir.join("logs");
    if !logs_dir.exists() {
        return ("Logs".to_string(), vec!["No log files yet.".to_string()]);
    }
    if let Some(stage) = preferred_stage {
        if let Some(path) = stage_live_log_path(&logs_dir, stage) {
            let title = path
                .file_name()
                .map(|value| value.to_string_lossy().to_string())
                .unwrap_or_else(|| format!("{stage}.stdout.log"));
            return (title, log_lines_from_path(&path, line_limit));
        }
    }
    if let Some(stage) = next_stage {
        if let Some(path) = stage_live_log_path(&logs_dir, stage) {
            let title = path
                .file_name()
                .map(|value| value.to_string_lossy().to_string())
                .unwrap_or_else(|| format!("{stage}.stdout.log"));
            return (title, log_lines_from_path(&path, line_limit));
        }
        if !stage.trim().is_empty() && stage != "none" && stage != "rerun" {
            if let Some(latest) = latest_log_file(&logs_dir) {
                let latest_title = latest
                    .file_name()
                    .map(|value| value.to_string_lossy().to_string())
                    .unwrap_or_else(|| "latest.log".to_string());
                let mut lines = vec![
                    format!("No log yet for pending stage `{stage}`."),
                    format!("Next action: start `{stage}` or resume the pipeline."),
                    String::new(),
                    format!("Latest available log: {latest_title}"),
                    String::new(),
                ];
                lines.extend(log_lines_from_path(
                    &latest,
                    line_limit.saturating_sub(lines.len()).max(1),
                ));
                return (format!("Pending stage: {stage}"), lines);
            }
            return (
                format!("Pending stage: {stage}"),
                vec![
                    format!("No log yet for pending stage `{stage}`."),
                    "Run the next step to create a fresh log for this stage.".to_string(),
                ],
            );
        }
    }
    latest_log_excerpt(run_dir, line_limit)
}

pub fn latest_log_excerpt(run_dir: &Path, line_limit: usize) -> (String, Vec<String>) {
    let logs_dir = run_dir.join("logs");
    if !logs_dir.exists() {
        return ("Logs".to_string(), vec!["No log files yet.".to_string()]);
    }
    let Some(latest) = latest_log_file(&logs_dir) else {
        return ("Logs".to_string(), vec!["No log files yet.".to_string()]);
    };
    let title = latest
        .file_name()
        .map(|value| value.to_string_lossy().to_string())
        .unwrap_or_else(|| "Logs".to_string());
    (title, log_lines_from_path(&latest, line_limit))
}

fn dispatch_run_stage(
    ctx: &Context,
    run_dir: &Path,
    subcommand: &str,
    extra: &[&str],
) -> Result<CommandResult, String> {
    if !run_dir.exists() {
        return Err(format!(
            "Run directory does not exist: {}",
            run_dir.display()
        ));
    }
    if !run_dir.join("plan.json").exists() {
        return Err(format!(
            "Missing plan.json in run directory: {}",
            run_dir.display()
        ));
    }
    sync_run_artifacts(ctx, run_dir)?;
    match subcommand {
        "status" => {
            let as_json = extra.contains(&"--json");
            let report = status_report(ctx, run_dir)?;
            let stdout = if as_json {
                serde_json::to_string_pretty(&report)
                    .map_err(|err| format!("Could not render status JSON: {err}"))?
            } else {
                status_text(&report)
            };
            Ok(CommandResult {
                code: 0,
                stdout: format!("{stdout}\n"),
                stderr: String::new(),
            })
        }
        "doctor" => {
            let as_json = extra.contains(&"--json");
            let report = doctor_report(ctx, run_dir)?;
            let stdout = if as_json {
                serde_json::to_string_pretty(&report)
                    .map_err(|err| format!("Could not render doctor JSON: {err}"))?
            } else {
                doctor_text(&report)
            };
            Ok(CommandResult {
                code: 0,
                stdout: format!("{stdout}\n"),
                stderr: String::new(),
            })
        }
        "next" => Ok(CommandResult {
            code: 0,
            stdout: format!("{}\n", next_stage(ctx, run_dir)?),
            stderr: String::new(),
        }),
        "summary" => print_user_summary(run_dir),
        "findings" => print_findings(run_dir),
        "augmented-task" => print_augmented_task(run_dir),
        "host-probe" => run_host_probe(run_dir, &parse_host_probe_args(extra)?),
        "recheck" => {
            let args = parse_stage_only_args(extra)?;
            let stage = resolve_stage(run_dir, &args.stage)?;
            recheck_stage(ctx, run_dir, &stage, args.dry_run)
        }
        "step-back" => {
            let args = parse_stage_only_args(extra)?;
            let stage = resolve_stage(run_dir, &args.stage)?;
            step_back_stage(ctx, run_dir, &stage, args.dry_run)
        }
        "refresh-prompt" => {
            let args = parse_stage_only_args(extra)?;
            let stage = resolve_stage(run_dir, &args.stage)?;
            refresh_stage_prompt(run_dir, &stage, args.dry_run)
        }
        "refresh-prompts" => {
            let dry_run = extra.contains(&"--dry-run");
            refresh_all_stage_prompts(run_dir, dry_run)
        }
        "cache-status" => print_cache_status(run_dir, &parse_cache_status_args(extra)?),
        "cache-prune" => run_cache_prune(run_dir, &parse_cache_prune_args(extra)?),
        "rerun" => create_follow_up_run(ctx, run_dir, &parse_rerun_args(extra)?),
        "show" => {
            let args = parse_show_args(extra)?;
            let stage = resolve_stage(run_dir, &args.stage)?;
            let stdout = if args.raw {
                read_text(&stage_prompt_path(run_dir, &stage)?)?
            } else {
                compile_prompt(ctx, run_dir, &stage)?
            };
            Ok(CommandResult {
                code: 0,
                stdout,
                stderr: String::new(),
            })
        }
        "copy" => {
            let args = parse_show_args(extra)?;
            let stage = resolve_stage(run_dir, &args.stage)?;
            let text = if args.raw {
                read_text(&stage_prompt_path(run_dir, &stage)?)?
            } else {
                compile_prompt(ctx, run_dir, &stage)?
            };
            copy_to_clipboard(&text)?;
            Ok(CommandResult {
                code: 0,
                stdout: format!("Copied {stage} prompt to clipboard.\n"),
                stderr: String::new(),
            })
        }
        "start" => {
            let args = parse_start_args(extra)?;
            let stage = resolve_stage(
                run_dir,
                args.stage.as_deref().ok_or("start requires <stage>.")?,
            )?;
            start_stage(ctx, run_dir, &stage, &args)
        }
        "start-solvers" => {
            let args = parse_start_args(extra)?;
            let stages = pending_solver_stages(run_dir)?;
            start_solver_batch(ctx, run_dir, &stages, &args)
        }
        "start-next" => {
            let args = parse_start_args(extra)?;
            let stage = next_stage_for_run(run_dir)?.unwrap_or_else(|| "none".to_string());
            if stage == "none" {
                return Ok(CommandResult {
                    code: 0,
                    stdout: "Pipeline is complete.\n".to_string(),
                    stderr: String::new(),
                });
            }
            if stage == "rerun" {
                return create_follow_up_run(
                    ctx,
                    run_dir,
                    &RerunArgs {
                        dry_run: args.dry_run,
                        ..RerunArgs::default()
                    },
                );
            }
            if stage_resume_bucket_for_run(run_dir, &stage)? == "solvers" {
                let stages = pending_solver_stages(run_dir)?;
                return start_solver_batch(ctx, run_dir, &stages, &args);
            }
            start_stage(ctx, run_dir, &stage, &args)
        }
        other => Err(format!("Unsupported command: {other}")),
    }
}

pub fn run_stage_capture(
    ctx: &Context,
    run_dir: &Path,
    subcommand: &str,
    extra: &[&str],
) -> Result<CommandResult, String> {
    dispatch_run_stage(ctx, run_dir, subcommand, extra)
}

pub fn run_stage_stream(
    ctx: &Context,
    run_dir: &Path,
    subcommand: &str,
    extra: &[&str],
) -> Result<i32, String> {
    let result = dispatch_run_stage(ctx, run_dir, subcommand, extra)?;
    if !result.stdout.trim().is_empty() {
        print!("{}", result.stdout);
        let _ = std::io::stdout().flush();
    }
    if !result.stderr.trim().is_empty() {
        eprint!("{}", result.stderr);
        let _ = std::io::stderr().flush();
    }
    Ok(result.code)
}

fn map_capture_interview_questions(
    ctx: &Context,
    args: &InterviewArgs,
) -> Result<CommandResult, String> {
    let raw_task = read_task_text(args.task.as_ref(), args.task_file.as_ref())?;
    let workspace = args.workspace.expanduser().resolve()?;
    let output_root = args.output_dir.expanduser().resolve()?;
    let (session_dir, payload) = generate_interview_questions(
        ctx,
        &raw_task,
        &workspace,
        &output_root,
        args.title.as_deref(),
        &args.language,
        args.max_questions,
    )?;
    let stdout = serde_json::to_string_pretty(&json!({
        "session_dir": session_dir.display().to_string(),
        "raw_task_path": session_dir.join("raw-task.md").display().to_string(),
        "goal_summary": payload.get("goal_summary").cloned().unwrap_or(Value::String(String::new())),
        "questions": payload.get("questions").cloned().unwrap_or(Value::Array(Vec::new())),
    }))
    .map_err(|err| format!("Could not render interview questions JSON: {err}"))?;
    Ok(CommandResult {
        code: 0,
        stdout: format!("{stdout}\n"),
        stderr: String::new(),
    })
}

fn map_capture_interview_finalize(
    ctx: &Context,
    args: &InterviewFinalizeArgs,
) -> Result<CommandResult, String> {
    let raw_task = read_task_text(args.task.as_ref(), args.task_file.as_ref())?;
    let workspace = args.workspace.expanduser().resolve()?;
    let session_dir = args.session_dir.expanduser().resolve()?;
    let answers: Vec<Value> = read_json(&args.answers_file.expanduser())?;
    let final_task_path = finalize_interview_prompt(
        ctx,
        &raw_task,
        &workspace,
        &session_dir,
        &answers,
        &args.language,
    )?;
    let stdout = serde_json::to_string_pretty(&InterviewFinalizePayload {
        session_dir: session_dir.display().to_string(),
        final_task_path: final_task_path.display().to_string(),
    })
    .map_err(|err| format!("Could not render interview finalize JSON: {err}"))?;
    Ok(CommandResult {
        code: 0,
        stdout: format!("{stdout}\n"),
        stderr: String::new(),
    })
}

fn map_capture_create_run(ctx: &Context, args: &CreateRunArgs) -> Result<CommandResult, String> {
    let workspace = args.workspace.expanduser().resolve()?;
    let output_dir = args.output_dir.expanduser().resolve()?;
    let task_text = read_task_text(args.task.as_ref(), args.task_file.as_ref())?;
    let run_dir = create_run(
        ctx,
        &task_text,
        &workspace,
        &output_dir,
        args.title.as_deref(),
        &args.prompt_format,
        &args.summary_language,
        &args.intake_research,
        &args.stage_research,
        &args.execution_network,
        &args.cache_root,
        &args.cache_policy,
        args.pipeline_file.as_deref(),
    )?;
    if let Some(session_dir) = &args.interview_session {
        maybe_copy_interview_artifacts(&session_dir.expanduser().resolve()?, &run_dir)?;
    }
    Ok(CommandResult {
        code: 0,
        stdout: format!("{}\n", run_dir.display()),
        stderr: String::new(),
    })
}

fn map_stream_interview(ctx: &Context, args: &InterviewArgs) -> Result<CommandResult, String> {
    let raw_task = read_task_text(args.task.as_ref(), args.task_file.as_ref())?;
    let workspace = args.workspace.expanduser().resolve()?;
    let output_root = args.output_dir.expanduser().resolve()?;
    let (session_dir, final_task) = run_interview_session(
        ctx,
        &raw_task,
        &workspace,
        &output_root,
        args.title.as_deref(),
        &args.language,
        args.max_questions,
    )?;
    let final_task_text = read_text(&final_task)?;
    Ok(CommandResult {
        code: 0,
        stdout: format!(
            "interview session: {}\nfinal task: {}\n\n{}\n",
            session_dir.display(),
            final_task.display(),
            final_task_text.trim_end()
        ),
        stderr: String::new(),
    })
}

fn map_stream_run(ctx: &Context, args: &RunArgs) -> Result<CommandResult, String> {
    let raw_task = read_task_text(args.task.as_ref(), args.task_file.as_ref())?;
    let workspace = args.workspace.expanduser().resolve()?;
    let output_dir = args.output_dir.expanduser().resolve()?;
    let (session_dir, final_task_path) = if args.skip_interview {
        let task_dir = output_dir.join("_interviews").join(timestamp_slug(
            args.title.as_deref().unwrap_or("direct-task"),
        ));
        fs::create_dir_all(&task_dir)
            .map_err(|err| format!("Could not create {}: {err}", task_dir.display()))?;
        let final_task_path = task_dir.join("final-task.md");
        write_text(&final_task_path, &raw_task)?;
        (task_dir, final_task_path)
    } else {
        run_interview_session(
            ctx,
            &raw_task,
            &workspace,
            &output_dir,
            args.title.as_deref(),
            &args.summary_language,
            args.max_questions,
        )?
    };
    let final_task_text = read_text(&final_task_path)?;
    let run_dir = create_run(
        ctx,
        &final_task_text,
        &workspace,
        &output_dir,
        args.title.as_deref(),
        &args.prompt_format,
        &args.summary_language,
        &args.intake_research,
        &args.stage_research,
        &args.execution_network,
        &args.cache_root,
        &args.cache_policy,
        args.pipeline_file.as_deref(),
    )?;
    maybe_copy_interview_artifacts(&session_dir, &run_dir)?;
    let automation = automate_run(ctx, &run_dir, &args.until, args.auto_approve)?;
    let mut stdout = format!("{}\n", run_dir.display());
    if !automation.stdout.trim().is_empty() {
        stdout.push_str(automation.stdout.trim_end());
        stdout.push('\n');
    }
    Ok(CommandResult {
        code: automation.code,
        stdout,
        stderr: automation.stderr,
    })
}

fn dispatch_map_capture(
    ctx: &Context,
    subcommand: &str,
    extra: &[String],
) -> Result<CommandResult, String> {
    match subcommand {
        "interview-questions" => {
            map_capture_interview_questions(ctx, &parse_interview_args(extra)?)
        }
        "interview-finalize" => {
            map_capture_interview_finalize(ctx, &parse_interview_finalize_args(extra)?)
        }
        "create-run" => map_capture_create_run(ctx, &parse_create_run_args(extra)?),
        other => Err(format!("Unsupported capture map command: {other}")),
    }
}

fn dispatch_map_stream(
    ctx: &Context,
    subcommand: &str,
    extra: &[String],
) -> Result<CommandResult, String> {
    match subcommand {
        "interview" => map_stream_interview(ctx, &parse_interview_args(extra)?),
        "run" => map_stream_run(ctx, &parse_run_args(extra)?),
        other => dispatch_map_capture(ctx, other, extra),
    }
}

pub fn task_flow_capture(
    ctx: &Context,
    subcommand: &str,
    extra: &[String],
) -> Result<CommandResult, String> {
    dispatch_map_capture(ctx, subcommand, extra)
}

pub fn task_flow_stream(ctx: &Context, subcommand: &str, extra: &[String]) -> Result<i32, String> {
    let result = dispatch_map_stream(ctx, subcommand, extra)?;
    if !result.stdout.trim().is_empty() {
        print!("{}", result.stdout);
        let _ = std::io::stdout().flush();
    }
    if !result.stderr.trim().is_empty() {
        eprint!("{}", result.stderr);
        let _ = std::io::stderr().flush();
    }
    Ok(result.code)
}

#[cfg(test)]
mod tests {
    use super::{
        automate_run, available_stages, build_cache_config, build_responses_stage_prompt,
        cache_lock, cache_lock_owner_path, capture_host_probe, choose_roles,
        contextual_log_excerpt, create_follow_up_run, create_run, current_unix_secs,
        detect_host_facts, detect_local_template, doctor_report, ensure_cache_layout,
        finalize_interview_prompt, generate_interview_questions, host_probe_path,
        host_probe_state, load_plan, materialize_embedded_repo_root,
        maybe_copy_interview_artifacts, next_stage_for_run, output_looks_placeholder,
        preview_text, read_file_digest, read_json, read_text, require_valid_order,
        restore_stage_cache, run_stage_capture, safe_next_action_for_run, save_plan,
        stage_cache_key, stage_prompt_path, stage_rank, status_report, store_stage_cache,
        summarize_token_ledger, sync_run_artifacts, task_flow_capture, write_json, write_text,
        CacheLockOwner, Context, InterviewFinalizePayload, InterviewQuestionsPayload,
        LocalTemplateKind, Plan, RerunArgs, RunTokenLedger, RunTokenLedgerEntry,
        StageBackendKind, TokenUsage, DECOMPOSITION_RULES_REF, REVIEW_RUBRIC_REF,
        ROLE_MAP_REF, STAGE_RESULTS_AREA, VERIFICATION_RUBRIC_REF,
    };
    use serde_json::json;
    use std::fs;
    use std::io::{BufRead, BufReader, Read as _, Write as _};
    use std::net::TcpListener;
    use std::os::unix::fs::PermissionsExt;
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    fn temp_dir(name: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("agpipe-test-{name}-{unique}"));
        fs::create_dir_all(&path).expect("create temp dir");
        path
    }

    fn temp_run_dir(name: &str) -> PathBuf {
        let path = temp_dir(name);
        fs::create_dir_all(path.join("logs")).expect("create temp run dir");
        path
    }

    fn custom_pipeline_yaml() -> &'static str {
        "pipeline:\n  stages:\n    - id: intake\n      kind: intake\n    - id: research-a\n      kind: research\n      role: product/product-trend-researcher.md\n      angle: market-scan\n    - id: research-b\n      kind: research\n      role: testing/testing-tool-evaluator.md\n      angle: risk-scan\n    - id: synthesis\n      kind: review\n    - id: implement\n      kind: execution\n    - id: audit\n      kind: verification\n"
    }

    fn custom_pipeline_without_roles_yaml() -> &'static str {
        "pipeline:\n  stages:\n    - id: intake\n      kind: intake\n    - id: research-a\n      kind: research\n    - id: research-b\n      kind: research\n    - id: synthesis\n      kind: review\n    - id: implement\n      kind: execution\n    - id: audit\n      kind: verification\n"
    }

    fn only_child_dir(root: &Path) -> PathBuf {
        let mut entries: Vec<PathBuf> = fs::read_dir(root)
            .expect("read child dirs")
            .filter_map(|entry| entry.ok().map(|value| value.path()))
            .collect();
        entries.sort();
        assert_eq!(entries.len(), 1, "expected one child in {}", root.display());
        entries.remove(0)
    }

    fn test_context() -> Context {
        Context {
            repo_root: std::env::current_dir().expect("current dir"),
            codex_bin: "/usr/bin/false".to_string(),
            stage0_backend: "codex".to_string(),
            stage_backend: "codex".to_string(),
            openai_api_base: "https://api.openai.com/v1".to_string(),
            openai_api_key: None,
            openai_model: "gpt-5".to_string(),
            openai_prompt_cache_key_prefix: "agpipe-stage0-v1".to_string(),
            openai_prompt_cache_retention: None,
            openai_store: false,
            openai_background: true,
        }
    }

    fn mock_codex_context(name: &str) -> (Context, PathBuf, PathBuf, PathBuf, PathBuf) {
        let root = temp_dir(&format!("mock-codex-{name}"));
        let bin_path = root.join("mock-codex.zsh");
        let invocations_path = root.join("invocations.log");
        let tokens_path = root.join("tokens.log");
        let script = r##"#!/bin/zsh
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
"##;
        write_text(&bin_path, script).expect("write mock codex");
        let mut permissions = fs::metadata(&bin_path)
            .expect("stat mock codex")
            .permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&bin_path, permissions).expect("chmod mock codex");
        (
            Context {
                repo_root: std::env::current_dir().expect("current dir"),
                codex_bin: bin_path.display().to_string(),
                stage0_backend: "codex".to_string(),
                stage_backend: "codex".to_string(),
                openai_api_base: "https://api.openai.com/v1".to_string(),
                openai_api_key: None,
                openai_model: "gpt-5".to_string(),
                openai_prompt_cache_key_prefix: "agpipe-stage0-v1".to_string(),
                openai_prompt_cache_retention: None,
                openai_store: false,
                openai_background: true,
            },
            root,
            bin_path,
            invocations_path,
            tokens_path,
        )
    }

    fn mock_responses_context(
        name: &str,
    ) -> (
        Context,
        PathBuf,
        Arc<Mutex<Vec<String>>>,
        std::thread::JoinHandle<()>,
    ) {
        let root = temp_dir(&format!("mock-responses-{name}"));
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock responses server");
        listener
            .set_nonblocking(true)
            .expect("set mock responses server nonblocking");
        let addr = listener.local_addr().expect("mock responses server addr");
        let mode = name.to_string();
        let request_bodies = Arc::new(Mutex::new(Vec::new()));
        let request_bodies_thread = Arc::clone(&request_bodies);
        let handle = std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(30);
            let mut handled = 0usize;
            let mut last_activity = Instant::now();
            while Instant::now() < deadline {
                match listener.accept() {
                    Ok((mut stream, _peer)) => {
                        handled += 1;
                        last_activity = Instant::now();
                        stream
                            .set_nonblocking(false)
                            .expect("set mock stream blocking");
                        let mut reader =
                            BufReader::new(stream.try_clone().expect("clone mock stream"));
                        let mut request_line = String::new();
                        reader
                            .read_line(&mut request_line)
                            .expect("read request line");
                        let mut content_length = 0usize;
                        let mut raw_headers = Vec::new();
                        loop {
                            let mut header = String::new();
                            let bytes = reader.read_line(&mut header).expect("read header");
                            if bytes == 0 || header == "\r\n" {
                                break;
                            }
                            raw_headers.push(header.clone());
                            let lower = header.to_ascii_lowercase();
                            if let Some(value) = lower.strip_prefix("content-length:") {
                                content_length = value.trim().parse::<usize>().unwrap_or(0);
                            }
                        }
                        let mut body_bytes = vec![0u8; content_length];
                        if content_length > 0 {
                            reader
                                .read_exact(&mut body_bytes)
                                .expect("read request body");
                        }
                        let body = String::from_utf8(body_bytes).expect("utf8 request body");
                        let mut parts = request_line.split_whitespace();
                        let method = parts.next().unwrap_or_default();
                        let path = parts.next().unwrap_or_default();
                        let label = if body.contains("\"agpipe_label\":\"interview-questions\"") {
                            "interview-questions"
                        } else if body.contains("\"agpipe_label\":\"interview-finalize\"") {
                            "interview-finalize"
                        } else if body.contains("\"agpipe_label\":\"intake\"") {
                            "intake"
                        } else if body.contains("\"agpipe_label\":\"review\"") {
                            "review"
                        } else if body.contains("\"agpipe_label\":\"verification\"") {
                            "verification"
                        } else if body.contains("\"agpipe_label\":\"solver-a\"") {
                            "solver-a"
                        } else if body.contains("\"agpipe_label\":\"solver-b\"") {
                            "solver-b"
                        } else if body.contains("\"agpipe_label\":\"solver-c\"") {
                            "solver-c"
                        } else {
                            "unknown"
                        };
                        let response_body = if method == "POST" && path == "/v1/responses" {
                            request_bodies_thread
                                .lock()
                                .expect("lock request bodies")
                                .push(format!("{}{}{}", request_line, raw_headers.join(""), body));
                            let response_id = match label {
                                "interview-questions" => "resp_questions",
                                "interview-finalize" => "resp_finalize",
                                "intake" => "resp_intake",
                                "review" => "resp_review",
                                "verification" => "resp_verification",
                                "solver-a" => "resp_solver_a",
                                "solver-b" => "resp_solver_b",
                                "solver-c" => "resp_solver_c",
                                _ => "resp_unknown",
                            };
                            format!(r#"{{"id":"{response_id}","status":"queued"}}"#)
                        } else if method == "GET" && path == "/v1/responses/resp_questions" {
                            r##"{
  "id":"resp_questions",
  "status":"completed",
  "output_text":"{\n  \"goal_summary\": \"Mock Responses migration\",\n  \"questions\": [\n    {\n      \"id\": \"runtime_scope\",\n      \"question\": \"Нужно ли проверять новый backend до verification?\",\n      \"why\": \"Чтобы stage0 сразу зафиксировал end-to-end критерий.\",\n      \"required\": true\n    }\n  ]\n}",
  "usage": {
    "input_tokens": 1200,
    "output_tokens": 180,
    "total_tokens": 1380,
    "input_tokens_details": {
      "cached_tokens": 900
    }
  }
}"##
                                .to_string()
                        } else if method == "GET" && path == "/v1/responses/resp_intake" {
                            if mode.contains("fail-intake") {
                                r##"{
  "id":"resp_intake",
  "status":"failed",
  "error": {
    "message": "Mock intake failure",
    "type": "server_error"
  },
  "output_text":"{\n  \"brief_md\": \"should not be accepted\",\n  \"plan_json\": {}\n}"
}"##
                                .to_string()
                            } else {
                                r##"{
  "id":"resp_intake",
  "status":"completed",
  "output_text":"{\n  \"brief_md\": \"# Brief\\n\\nUse the Responses backend for non-execution stages and keep execution on Codex until local tools are ported.\\n\",\n  \"plan_json\": {\n    \"created_at\": \"mock\",\n    \"workspace\": \"/tmp/mock-workspace\",\n    \"workspace_exists\": true,\n    \"original_task\": \"Mock Responses migration\",\n    \"task_kind\": \"migration\",\n    \"complexity\": \"complex\",\n    \"execution_mode\": \"full\",\n    \"prompt_format\": \"compact\",\n    \"summary_language\": \"ru\",\n    \"intake_research_mode\": \"research-first\",\n    \"stage_research_mode\": \"local-first\",\n    \"execution_network_mode\": \"fetch-if-needed\",\n    \"cache\": {\n      \"enabled\": true,\n      \"root\": \"/tmp/mock-cache\",\n      \"policy\": \"reuse\"\n    },\n    \"token_budget\": {\n      \"total_tokens\": 50000,\n      \"warning_threshold_tokens\": 40000,\n      \"source\": \"mock\"\n    },\n    \"host_facts\": {\n      \"source\": \"mock\",\n      \"preferred_torch_device\": \"cpu\"\n    },\n    \"solver_count\": 1,\n    \"solver_roles\": [\n      {\n        \"solver_id\": \"solver-a\",\n        \"role\": \"implementation-engineer\",\n        \"angle\": \"implementation-first\"\n      }\n    ],\n    \"workstream_hints\": [\n      {\n        \"goal\": \"native-runtime-parity\",\n        \"name\": \"native-runtime-parity\",\n        \"suggested_role\": \"implementation-engineer\"\n      }\n    ],\n    \"goal_gate_enabled\": true,\n    \"augmented_follow_up_enabled\": true,\n    \"goal_checks\": [\n      {\n        \"id\": \"responses-non-execution\",\n        \"requirement\": \"Run non-execution stages through Responses backend\",\n        \"critical\": true\n      }\n    ],\n    \"reviewer_stack\": [\n      \"testing/testing-reality-checker.md\"\n    ],\n    \"validation_commands\": [\n      \"cargo test\"\n    ],\n    \"references\": {}\n  }\n}",
  "usage": {
    "input_tokens": 1400,
    "output_tokens": 260,
    "total_tokens": 1660,
    "input_tokens_details": {
      "cached_tokens": 1000
    }
  }
}"##
                                    .to_string()
                            }
                        } else if method == "GET" && path == "/v1/responses/resp_solver_a" {
                            r##"{
  "id":"resp_solver_a",
  "status":"completed",
  "output_text":"{\n  \"result_md\": \"# Result\\n\\nMock solver output produced through Responses backend.\\n\"\n}",
  "usage": {
    "input_tokens": 900,
    "output_tokens": 150,
    "total_tokens": 1050,
    "input_tokens_details": {
      "cached_tokens": 700
    }
  }
}"##
                                .to_string()
                        } else if method == "GET" && path == "/v1/responses/resp_review" {
                            r##"{
  "id":"resp_review",
  "status":"completed",
  "output_text":"{\n  \"report_md\": \"# Review Report\\n\\nMock review selected solver-a.\\n\",\n  \"scorecard_json\": {\n    \"winner\": \"solver-a\",\n    \"selected\": \"solver-a\",\n    \"why\": \"Mock best result\"\n  },\n  \"user_summary_md\": \"# User Summary\\n\\nMock localized review summary.\\n\"\n}",
  "usage": {
    "input_tokens": 1250,
    "output_tokens": 220,
    "total_tokens": 1470,
    "input_tokens_details": {
      "cached_tokens": 950
    }
  }
}"##
                                .to_string()
                        } else if method == "GET" && path == "/v1/responses/resp_verification" {
                            r##"{
  "id":"resp_verification",
  "status":"completed",
  "output_text":"{\n  \"findings_md\": \"# Findings\\n\\nNo critical findings in mock verification.\\n\",\n  \"user_summary_md\": \"# Verification Summary\\n\\nMock verification summary.\\n\",\n  \"improvement_request_md\": \"# Improvement Request\\n\\nNo rerun required.\\n\",\n  \"augmented_task_md\": \"# Augmented Task\\n\\nKeep the current verified state.\\n\",\n  \"goal_status_json\": {\n    \"goal_complete\": true,\n    \"goal_verdict\": \"complete\",\n    \"rerun_recommended\": false,\n    \"recommended_next_action\": \"none\"\n  }\n}",
  "usage": {
    "input_tokens": 1500,
    "output_tokens": 280,
    "total_tokens": 1780,
    "input_tokens_details": {
      "cached_tokens": 1200
    }
  }
}"##
                                .to_string()
                        } else if method == "GET" && path == "/v1/responses/resp_finalize" {
                            r##"{
  "id":"resp_finalize",
  "status":"completed",
  "output_text":"# Final Task\n\nСобрать новый backend через Responses API и проверить pipeline до verification.\n",
  "usage": {
    "input_tokens": 1600,
    "output_tokens": 220,
    "total_tokens": 1820,
    "input_tokens_details": {
      "cached_tokens": 1100
    }
  }
}"##
                            .to_string()
                        } else {
                            r#"{"status":"failed"}"#.to_string()
                        };
                        let response = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                            response_body.len(),
                            response_body
                        );
                        stream
                            .write_all(response.as_bytes())
                            .expect("write mock response");
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                        if handled > 0 && last_activity.elapsed() > Duration::from_secs(20) {
                            break;
                        }
                        std::thread::sleep(Duration::from_millis(20));
                    }
                    Err(err) => panic!("mock responses accept failed: {err}"),
                }
            }
            assert!(handled > 0, "expected at least one Responses API request");
        });
        (
            Context {
                repo_root: std::env::current_dir().expect("current dir"),
                codex_bin: "/usr/bin/false".to_string(),
                stage0_backend: "responses".to_string(),
                stage_backend: "responses-readonly".to_string(),
                openai_api_base: format!("http://{addr}/v1"),
                openai_api_key: Some("test-key".to_string()),
                openai_model: "gpt-5".to_string(),
                openai_prompt_cache_key_prefix: "agpipe-stage0-v1".to_string(),
                openai_prompt_cache_retention: None,
                openai_store: false,
                openai_background: true,
            },
            root,
            request_bodies,
            handle,
        )
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

    fn seed_preverification_outputs(run_dir: &Path, plan: &Plan) {
        write_text(&run_dir.join("brief.md"), "# Brief\n\nSeeded intake.\n").expect("seed brief");
        for solver in &plan.solver_roles {
            write_text(
                &run_dir
                    .join("solutions")
                    .join(&solver.solver_id)
                    .join("RESULT.md"),
                &format!("# Result\n\nSeeded result for {}.\n", solver.solver_id),
            )
            .expect("seed solver result");
        }
        write_text(
            &run_dir.join("review").join("report.md"),
            "# Review Report\n\nSeeded review.\n",
        )
        .expect("seed review report");
        write_text(
            &run_dir.join("review").join("scorecard.json"),
            "{\"winner\":\"solver-a\"}\n",
        )
        .expect("seed scorecard");
        write_text(
            &run_dir.join("review").join("user-summary.md"),
            "# User Summary\n\nSeeded review summary.\n",
        )
        .expect("seed review summary");
        write_text(
            &run_dir.join("execution").join("report.md"),
            "# Execution Report\n\nSeeded execution.\n",
        )
        .expect("seed execution report");
    }

    #[test]
    fn contextual_log_excerpt_prefers_pending_stage_log() {
        let run_dir = temp_run_dir("pending-stage-log");
        fs::write(
            run_dir.join("logs").join("review.stdout.log"),
            "line 1\nline 2\nline 3\n",
        )
        .expect("write review log");
        fs::write(
            run_dir.join("logs").join("solver-a.stdout.log"),
            "old solver log\n",
        )
        .expect("write solver log");

        let (title, lines) = contextual_log_excerpt(&run_dir, None, Some("review"), 12);

        assert_eq!(title, "review.stdout.log");
        assert_eq!(lines, vec!["line 1", "line 2", "line 3"]);

        let _ = fs::remove_dir_all(run_dir);
    }

    #[test]
    fn contextual_log_excerpt_reports_missing_stage_and_shows_latest_available() {
        let run_dir = temp_run_dir("missing-stage-log");
        fs::write(
            run_dir.join("logs").join("review.prompt.md"),
            "compiled review prompt\n",
        )
        .expect("write prompt");

        let (title, lines) = contextual_log_excerpt(&run_dir, None, Some("review"), 12);
        let joined = lines.join("\n");

        assert_eq!(title, "Pending stage: review");
        assert!(joined.contains("No log yet for pending stage `review`."));
        assert!(joined.contains("Latest available log: review.prompt.md"));

        let _ = fs::remove_dir_all(run_dir);
    }

    #[test]
    fn placeholder_detection_marks_pending_files() {
        assert!(output_looks_placeholder(
            "review",
            "# Review Report\n\nPending review stage.\n"
        ));
        assert!(!output_looks_placeholder(
            "review",
            "# Review Report\n\nImplemented result.\n"
        ));
    }

    #[test]
    fn host_facts_detection_sets_a_device_string() {
        let facts = detect_host_facts("test");
        assert!(!facts.platform.is_empty());
        assert!(!facts.machine.is_empty());
        assert!(!facts.preferred_torch_device.is_empty());
    }

    #[test]
    fn capture_host_probe_refresh_ignores_plan_seed() {
        let run_dir = temp_run_dir("fresh-host-probe");
        let actual = detect_host_facts("expected");
        let mut plan = Plan {
            host_facts: actual.clone(),
            ..Plan::default()
        };
        plan.host_facts.platform = "spoofed-platform".to_string();
        plan.host_facts.machine = "spoofed-machine".to_string();
        plan.host_facts.torch_installed = !actual.torch_installed;
        plan.host_facts.cuda_available = Some(!actual.cuda_available.unwrap_or(false));
        plan.host_facts.mps_built = Some(!actual.mps_built.unwrap_or(false));
        plan.host_facts.mps_available = Some(!actual.mps_available.unwrap_or(false));
        plan.host_facts.preferred_torch_device = if actual.preferred_torch_device == "cpu" {
            "mps".to_string()
        } else {
            "cpu".to_string()
        };
        save_plan(&run_dir, &plan).expect("save plan");

        let captured = capture_host_probe(&run_dir).expect("capture probe");

        assert_eq!(captured.platform, actual.platform);
        assert_eq!(captured.machine, actual.machine);
        assert_eq!(captured.torch_installed, actual.torch_installed);
        assert_eq!(captured.cuda_available, actual.cuda_available);
        assert_eq!(captured.mps_built, actual.mps_built);
        assert_eq!(captured.mps_available, actual.mps_available);
        assert_eq!(
            captured.preferred_torch_device,
            actual.preferred_torch_device
        );
        assert_ne!(captured.platform, plan.host_facts.platform);
        assert_ne!(captured.machine, plan.host_facts.machine);
        assert_ne!(
            captured.preferred_torch_device,
            plan.host_facts.preferred_torch_device
        );

        let _ = fs::remove_dir_all(run_dir);
    }

    #[test]
    fn host_probe_state_reports_multi_field_drift() {
        let run_dir = temp_run_dir("host-drift");
        let actual = detect_host_facts("expected");
        let mut plan = Plan {
            host_facts: actual.clone(),
            ..Plan::default()
        };
        plan.host_facts.torch_installed = !actual.torch_installed;
        plan.host_facts.mps_available = Some(!actual.mps_available.unwrap_or(false));
        save_plan(&run_dir, &plan).expect("save plan");
        write_json(&host_probe_path(&run_dir), &actual).expect("write host probe");

        let (_label, drift) = host_probe_state(&run_dir).expect("host probe state");
        let drift = drift.expect("expected drift");

        assert!(drift.contains("torch_installed"));
        assert!(drift.contains("mps_available"));

        let _ = fs::remove_dir_all(run_dir);
    }

    #[test]
    fn generate_interview_questions_fails_closed_on_codex_error() {
        let output_root = temp_dir("interview-questions-output");
        let workspace = temp_dir("interview-questions-workspace");
        let ctx = test_context();

        let err = generate_interview_questions(
            &ctx,
            "failure probe",
            &workspace,
            &output_root,
            Some("failure"),
            "ru",
            6,
        )
        .expect_err("expected codex failure");

        assert!(err.contains("codex exec failed"));
        let session_dir = only_child_dir(&output_root.join("_interviews"));
        assert!(session_dir.join("raw-task.md").exists());
        assert!(session_dir
            .join("logs")
            .join("interview-questions.prompt.md")
            .exists());
        assert!(session_dir
            .join("logs")
            .join("interview-questions.stderr.log")
            .exists());
        assert!(!session_dir.join("questions.json").exists());

        let _ = fs::remove_dir_all(output_root);
        let _ = fs::remove_dir_all(workspace);
    }

    #[test]
    fn finalize_interview_prompt_fails_closed_on_codex_error() {
        let session_dir = temp_dir("interview-finalize-session");
        let workspace = temp_dir("interview-finalize-workspace");
        let ctx = test_context();
        let answers = vec![json!({
            "id": "scope",
            "question": "What matters?",
            "answer": "Parity"
        })];

        let err = finalize_interview_prompt(
            &ctx,
            "failure probe",
            &workspace,
            &session_dir,
            &answers,
            "ru",
        )
        .expect_err("expected codex failure");

        assert!(err.contains("codex exec failed"));
        assert!(session_dir.join("answers.json").exists());
        assert!(session_dir
            .join("logs")
            .join("interview-finalize.prompt.md")
            .exists());
        assert!(session_dir
            .join("logs")
            .join("interview-finalize.stderr.log")
            .exists());
        assert!(!session_dir.join("final-task.md").exists());

        let _ = fs::remove_dir_all(session_dir);
        let _ = fs::remove_dir_all(workspace);
    }

    #[test]
    fn responses_backend_handles_stage0_questions_and_finalize() {
        let output_root = temp_dir("responses-stage0-output");
        let workspace = temp_dir("responses-stage0-workspace");
        let (ctx, mock_root, request_bodies, server) = mock_responses_context("stage0");

        let (session_dir, payload) = generate_interview_questions(
            &ctx,
            "Нужно переписать pipeline на новый backend и довести до verification.",
            &workspace,
            &output_root,
            Some("responses"),
            "ru",
            6,
        )
        .expect("responses interview questions");

        assert_eq!(
            payload
                .get("goal_summary")
                .and_then(|value| value.as_str())
                .unwrap_or_default(),
            "Mock Responses migration"
        );
        assert!(session_dir
            .join("logs")
            .join("interview-questions.response.json")
            .exists());

        let answers = vec![json!({
            "id": "runtime_scope",
            "question": "Нужно ли проверять новый backend до verification?",
            "answer": "Да, обязательно."
        })];
        let final_task_path = finalize_interview_prompt(
            &ctx,
            "Нужно переписать pipeline на новый backend и довести до verification.",
            &workspace,
            &session_dir,
            &answers,
            "ru",
        )
        .expect("responses finalize");

        assert!(read_text(&final_task_path)
            .expect("read final task")
            .contains("Responses API"));
        assert!(session_dir
            .join("logs")
            .join("interview-finalize.response.json")
            .exists());
        let request_bodies = request_bodies.lock().expect("lock request bodies");
        assert!(request_bodies.iter().any(|request| {
            request.contains("\"agpipe_label\":\"interview-questions\"")
                && request.contains("\"type\":\"json_schema\"")
                && request.contains("Idempotency-Key: agpipe-")
        }));
        server.join().expect("join mock responses server");

        let _ = fs::remove_dir_all(session_dir);
        let _ = fs::remove_dir_all(output_root);
        let _ = fs::remove_dir_all(workspace);
        let _ = fs::remove_dir_all(mock_root);
    }

    #[test]
    fn responses_readonly_backend_runs_non_execution_stages_and_records_provider_usage() {
        let workspace = temp_dir("responses-readonly-workspace");
        let output_root = temp_dir("responses-readonly-output");
        let cache_root = temp_dir("responses-readonly-cache");
        let (ctx, mock_root, request_bodies, server) = mock_responses_context("readonly-stages");

        let run_dir = create_run(
            &ctx,
            "Проверить Responses backend на intake, solver, review и verification без execution tool loop.",
            &workspace,
            &output_root,
            Some("responses-readonly"),
            "compact",
            "ru",
            "research-first",
            "local-first",
            "fetch-if-needed",
            &cache_root.display().to_string(),
            "reuse",
            None,
        )
        .expect("create responses readonly run");

        let intake = run_stage_capture(&ctx, &run_dir, "start", &["intake"]).expect("start intake");
        assert_eq!(intake.code, 0);
        assert!(read_text(&run_dir.join("brief.md"))
            .expect("read intake brief")
            .contains("Responses backend"));

        let plan = load_plan(&run_dir).expect("load responses readonly plan");
        assert_eq!(plan.solver_roles.len(), 1);
        assert_eq!(plan.solver_roles[0].solver_id, "solver-a");

        let solver = run_stage_capture(&ctx, &run_dir, "start", &["solver-a"])
            .expect("start responses solver");
        assert_eq!(solver.code, 0);
        assert!(
            read_text(&run_dir.join("solutions").join("solver-a").join("RESULT.md"))
                .expect("read solver result")
                .contains("Responses backend")
        );

        let review = run_stage_capture(&ctx, &run_dir, "start", &["review"])
            .expect("start responses review");
        assert_eq!(review.code, 0);
        assert!(read_text(&run_dir.join("review").join("report.md"))
            .expect("read review report")
            .contains("selected solver-a"));

        write_text(
            &run_dir.join("execution").join("report.md"),
            "# Execution Report\n\nMock execution was completed by a separate tool loop.\n",
        )
        .expect("seed execution report");

        let verification = run_stage_capture(&ctx, &run_dir, "start", &["verification"])
            .expect("start responses verification");
        assert_eq!(verification.code, 0);
        assert!(read_text(&run_dir.join("verification").join("findings.md"))
            .expect("read findings")
            .contains("No critical findings"));

        let ledger: RunTokenLedger = read_json(&run_dir.join("runtime").join("token-ledger.json"))
            .expect("read token ledger");
        let response_entries: Vec<&RunTokenLedgerEntry> = ledger
            .entries
            .iter()
            .filter(|entry| entry.usage.source == "openai-responses")
            .collect();
        assert!(response_entries.len() >= 4);
        assert!(response_entries
            .iter()
            .any(|entry| entry.stage == "intake" && entry.usage.cached_prompt_tokens > 0));
        assert!(response_entries
            .iter()
            .any(|entry| entry.stage == "review" && entry.usage.prompt_tokens > 0));
        assert!(response_entries
            .iter()
            .any(|entry| entry.stage == "verification" && entry.usage.total_tokens > 0));
        let request_bodies = request_bodies.lock().expect("lock request bodies");
        assert!(request_bodies.iter().any(|request| {
            request.contains("\"agpipe_label\":\"intake\"")
                && request.contains("\"type\":\"json_schema\"")
        }));
        assert!(request_bodies.iter().any(|request| {
            request.contains("\"agpipe_label\":\"intake\"")
                && request.contains("\"prompt_cache_key\":\"agpipe-stage0-v1:intake:gpt-5:")
                && request.contains("Idempotency-Key: agpipe-")
        }));

        server.join().expect("join mock responses readonly server");
        let _ = fs::remove_dir_all(run_dir);
        let _ = fs::remove_dir_all(output_root);
        let _ = fs::remove_dir_all(workspace);
        let _ = fs::remove_dir_all(cache_root);
        let _ = fs::remove_dir_all(mock_root);
    }

    #[test]
    fn maybe_copy_interview_artifacts_preserves_nested_logs() {
        let session_dir = temp_dir("copy-interview-session");
        let run_dir = temp_run_dir("copy-interview-run");
        fs::create_dir_all(session_dir.join("logs")).expect("create logs dir");
        fs::write(
            session_dir.join("final-task.md"),
            "# Final Task\n\nKeep nested logs.\n",
        )
        .expect("write final task");
        fs::write(
            session_dir
                .join("logs")
                .join("interview-finalize.stdout.log"),
            "streamed stdout\n",
        )
        .expect("write stdout log");
        fs::write(
            session_dir.join("logs").join("interview-finalize.last.md"),
            "last message\n",
        )
        .expect("write last message");

        maybe_copy_interview_artifacts(&session_dir, &run_dir).expect("copy interview artifacts");

        assert!(run_dir.join("interview").join("final-task.md").exists());
        assert!(run_dir
            .join("interview")
            .join("logs")
            .join("interview-finalize.stdout.log")
            .exists());
        assert!(run_dir
            .join("interview")
            .join("logs")
            .join("interview-finalize.last.md")
            .exists());

        let _ = fs::remove_dir_all(session_dir);
        let _ = fs::remove_dir_all(run_dir);
    }

    #[test]
    fn preview_text_skips_placeholder_review_summary_and_prefers_brief() {
        let run_dir = temp_run_dir("preview-brief-fallback");
        fs::create_dir_all(run_dir.join("review")).expect("create review dir");
        write_text(
            &run_dir.join("review").join("user-summary.md"),
            "# User Summary\n\nPending localized review summary.\n",
        )
        .expect("write placeholder summary");
        write_text(&run_dir.join("brief.md"), "# Brief\n\nReal intake brief.\n")
            .expect("write brief");

        let (label, preview) = preview_text(&run_dir, 2400);

        assert_eq!(label, "Brief");
        assert!(preview.contains("Real intake brief"));

        let _ = fs::remove_dir_all(run_dir);
    }

    #[test]
    fn stage_cache_restores_outputs_for_matching_inputs() {
        let run_dir = temp_run_dir("stage-cache-run");
        let workspace = temp_dir("stage-cache-workspace");
        let cache_root = temp_dir("stage-cache-root");
        let ctx = test_context();

        write_text(&run_dir.join("request.md"), "# Request\n\nCache me.\n").expect("request");
        write_text(&run_dir.join("brief.md"), "# Brief\n\nStable brief.\n").expect("brief");
        write_text(
            &run_dir.join("execution").join("report.md"),
            "# Execution Report\n\nImplemented.\n",
        )
        .expect("execution report");
        write_json(
            &run_dir.join("plan.json"),
            &json!({
                "workspace": workspace.display().to_string(),
                "workspace_exists": true,
                "prompt_format": "compact",
                "summary_language": "ru",
                "intake_research_mode": "research-first",
                "stage_research_mode": "local-first",
                "execution_network_mode": "fetch-if-needed",
                "cache": build_cache_config(&cache_root, "reuse"),
                "goal_gate_enabled": true,
                "augmented_follow_up_enabled": true
            }),
        )
        .expect("plan");
        let plan = load_plan(&run_dir).expect("load plan");
        let cache = build_cache_config(&cache_root, "reuse");
        let command = vec!["codex".to_string(), "exec".to_string()];
        let prompt = "verification prompt";
        let (key, inputs, prompt_hashes, workspace_hash) = stage_cache_key(
            &ctx,
            &cache,
            &plan,
            &run_dir,
            "verification",
            StageBackendKind::Codex,
            &command,
            prompt,
        )
        .expect("cache key");

        let outputs = vec![
            run_dir.join("verification").join("findings.md"),
            run_dir.join("verification").join("user-summary.md"),
        ];
        let logs = vec![run_dir.join("logs").join("verification.stdout.log")];
        write_text(&outputs[0], "# Findings\n\nCached findings.\n").expect("findings");
        write_text(&outputs[1], "# Verification Summary\n\nCached summary.\n").expect("summary");
        write_text(&logs[0], "cached stdout\n").expect("log");

        store_stage_cache(
            &cache,
            &key,
            &run_dir,
            "verification",
            &command,
            &prompt_hashes,
            &workspace_hash,
            &TokenUsage {
                source: "test".to_string(),
                prompt_tokens: 11,
                completion_tokens: 7,
                total_tokens: 18,
                ..TokenUsage::default()
            },
            &inputs,
            &outputs,
            &logs,
        )
        .expect("store stage cache");

        write_text(&outputs[0], "# Findings\n\nPending verification stage.\n")
            .expect("reset findings");
        write_text(
            &outputs[1],
            "# Verification Summary\n\nPending localized verification summary.\n",
        )
        .expect("reset summary");
        write_text(&logs[0], "").expect("reset log");

        let restored = restore_stage_cache(&cache, &key, &run_dir, "verification")
            .expect("restore stage cache");

        assert!(restored.is_some());
        assert!(read_text(&outputs[0])
            .expect("read findings")
            .contains("Cached findings"));
        assert!(read_text(&outputs[1])
            .expect("read summary")
            .contains("Cached summary"));
        assert!(read_text(&logs[0])
            .expect("read log")
            .contains("cached stdout"));
        assert!(cache_root.join(STAGE_RESULTS_AREA).join(&key).exists());

        let _ = fs::remove_dir_all(run_dir);
        let _ = fs::remove_dir_all(workspace);
        let _ = fs::remove_dir_all(cache_root);
    }

    #[test]
    fn cache_lock_recovers_from_stale_owner_directory() {
        let cache_root = temp_dir("stale-cache-lock");
        let cache = build_cache_config(&cache_root, "reuse");
        ensure_cache_layout(&cache).expect("cache layout");

        let lock_dir = PathBuf::from(&cache.meta.locks).join("index.lock");
        fs::create_dir_all(&lock_dir).expect("create stale lock dir");
        write_json(
            &cache_lock_owner_path(&lock_dir),
            &CacheLockOwner {
                pid: -1,
                created_at_unix: current_unix_secs().saturating_sub(60),
            },
        )
        .expect("write stale owner");

        let value = cache_lock(&cache, "index", || Ok::<_, String>("recovered".to_string()))
            .expect("recover stale lock");

        assert_eq!(value, "recovered");
        assert!(!lock_dir.exists());

        let _ = fs::remove_dir_all(cache_root);
    }

    #[test]
    fn host_probe_digest_ignores_captured_at_and_history_paths() {
        let run_dir = temp_run_dir("host-probe-digest");
        let probe_path = host_probe_path(&run_dir);
        let mut first = detect_host_facts("probe-a");
        first.captured_at = "2026-03-14T10:00:00Z".to_string();
        first.artifact = Some("/tmp/a.json".to_string());
        first.history_artifact = Some("/tmp/history-a.json".to_string());
        write_json(&probe_path, &first).expect("write first probe");
        let digest_a = read_file_digest(&probe_path).expect("digest first probe");

        let mut second = first.clone();
        second.captured_at = "2026-03-14T10:05:00Z".to_string();
        second.artifact = Some("/tmp/b.json".to_string());
        second.history_artifact = Some("/tmp/history-b.json".to_string());
        write_json(&probe_path, &second).expect("write second probe");
        let digest_b = read_file_digest(&probe_path).expect("digest second probe");

        assert_eq!(digest_a, digest_b);

        let _ = fs::remove_dir_all(run_dir);
    }

    #[test]
    fn stage_cache_key_ignores_workspace_mtime_when_content_is_unchanged() {
        let run_dir = temp_run_dir("workspace-content-hash");
        let workspace = temp_dir("workspace-content-root");
        let cache_root = temp_dir("workspace-content-cache");
        let ctx = test_context();
        let cache = build_cache_config(&cache_root, "reuse");
        let workspace_file = workspace.join("src").join("lib.rs");

        write_text(&run_dir.join("request.md"), "# Request\n\nCache me.\n").expect("request");
        write_text(&run_dir.join("brief.md"), "# Brief\n\nStable brief.\n").expect("brief");
        write_text(
            &workspace_file,
            "pub fn hello() -> &'static str { \"hi\" }\n",
        )
        .expect("workspace file");
        write_json(
            &run_dir.join("plan.json"),
            &json!({
                "workspace": workspace.display().to_string(),
                "workspace_exists": true,
                "prompt_format": "compact",
                "summary_language": "ru",
                "intake_research_mode": "research-first",
                "stage_research_mode": "local-first",
                "execution_network_mode": "fetch-if-needed",
                "cache": cache,
                "goal_gate_enabled": true,
                "augmented_follow_up_enabled": true
            }),
        )
        .expect("plan");
        let plan = load_plan(&run_dir).expect("load plan");
        let command = vec!["codex".to_string(), "exec".to_string()];
        let prompt = "verification prompt";
        let (_, _, _, first_workspace_hash) = stage_cache_key(
            &ctx,
            &build_cache_config(&cache_root, "reuse"),
            &plan,
            &run_dir,
            "verification",
            StageBackendKind::Codex,
            &command,
            prompt,
        )
        .expect("first cache key");

        std::thread::sleep(Duration::from_millis(20));
        write_text(
            &workspace_file,
            "pub fn hello() -> &'static str { \"hi\" }\n",
        )
        .expect("rewrite same content");
        let (_, _, _, second_workspace_hash) = stage_cache_key(
            &ctx,
            &build_cache_config(&cache_root, "reuse"),
            &plan,
            &run_dir,
            "verification",
            StageBackendKind::Codex,
            &command,
            prompt,
        )
        .expect("second cache key");

        assert_eq!(first_workspace_hash, second_workspace_hash);

        let _ = fs::remove_dir_all(run_dir);
        let _ = fs::remove_dir_all(workspace);
        let _ = fs::remove_dir_all(cache_root);
    }

    #[test]
    fn responses_stage_cache_key_ignores_workspace_changes_for_readonly_backend() {
        let run_dir = temp_run_dir("responses-workspace-agnostic-cache");
        let workspace = temp_dir("responses-workspace-root");
        let cache_root = temp_dir("responses-workspace-cache");
        let ctx = Context {
            stage_backend: "responses-readonly".to_string(),
            ..test_context()
        };
        let cache = build_cache_config(&cache_root, "reuse");
        let workspace_file = workspace.join("src").join("lib.rs");

        write_text(
            &run_dir.join("request.md"),
            "# Request\n\nCache readonly intake.\n",
        )
        .expect("request");
        write_json(
            &run_dir.join("plan.json"),
            &json!({
                "workspace": workspace.display().to_string(),
                "workspace_exists": true,
                "prompt_format": "compact",
                "summary_language": "ru",
                "intake_research_mode": "research-first",
                "stage_research_mode": "local-first",
                "execution_network_mode": "fetch-if-needed",
                "cache": cache,
                "goal_gate_enabled": true,
                "augmented_follow_up_enabled": true
            }),
        )
        .expect("plan");
        write_text(&workspace_file, "pub fn answer() -> u8 { 1 }\n").expect("workspace file");
        let plan = load_plan(&run_dir).expect("load plan");
        let command = vec![
            "responses-api".to_string(),
            "stage".to_string(),
            "intake".to_string(),
        ];
        let prompt = "readonly intake prompt";

        let (first_key, _, _, first_workspace_hash) = stage_cache_key(
            &ctx,
            &cache,
            &plan,
            &run_dir,
            "intake",
            StageBackendKind::Responses,
            &command,
            prompt,
        )
        .expect("first cache key");

        write_text(&workspace_file, "pub fn answer() -> u8 { 2 }\n").expect("rewrite workspace");
        let (second_key, _, _, second_workspace_hash) = stage_cache_key(
            &ctx,
            &cache,
            &plan,
            &run_dir,
            "intake",
            StageBackendKind::Responses,
            &command,
            prompt,
        )
        .expect("second cache key");

        assert_eq!(first_key, second_key);
        assert_eq!(first_workspace_hash, "workspace-not-read-by-backend");
        assert_eq!(second_workspace_hash, "workspace-not-read-by-backend");

        let _ = fs::remove_dir_all(run_dir);
        let _ = fs::remove_dir_all(workspace);
        let _ = fs::remove_dir_all(cache_root);
    }

    #[test]
    fn responses_verification_prompt_embeds_recent_workspace_files() {
        let workspace = temp_dir("responses-verification-workspace");
        let output_root = temp_dir("responses-verification-output");
        let cache_root = temp_dir("responses-verification-cache");
        let ctx = Context {
            stage_backend: "responses-readonly".to_string(),
            ..test_context()
        };
        write_text(
            &workspace.join("src").join("main.rs"),
            "fn main() { println!(\"verified workspace\"); }\n",
        )
        .expect("workspace file");
        let run_dir = create_run(
            &ctx,
            "Проверить verification prompt against actual workspace.",
            &workspace,
            &output_root,
            Some("responses-verification-prompt"),
            "compact",
            "ru",
            "research-first",
            "local-first",
            "fetch-if-needed",
            &cache_root.display().to_string(),
            "reuse",
            None,
        )
        .expect("create run");
        write_text(
            &run_dir.join("brief.md"),
            "# Brief\n\nCheck the actual code.\n",
        )
        .expect("brief");
        write_text(
            &run_dir.join("review").join("report.md"),
            "# Review Report\n\nProceed.\n",
        )
        .expect("review report");
        write_text(
            &run_dir.join("review").join("scorecard.json"),
            "{\"winner\":\"solver-a\"}\n",
        )
        .expect("scorecard");
        write_text(
            &run_dir.join("review").join("user-summary.md"),
            "# User Summary\n\nProceed.\n",
        )
        .expect("review summary");
        write_text(
            &run_dir.join("execution").join("report.md"),
            "# Execution Report\n\nChanged `src/main.rs`.\n",
        )
        .expect("execution report");

        let prompt = build_responses_stage_prompt(&ctx, &run_dir, "verification").expect("prompt");
        assert!(prompt.contains("workspace/src/main.rs"));
        assert!(prompt.contains("verified workspace"));

        let _ = fs::remove_dir_all(run_dir);
        let _ = fs::remove_dir_all(workspace);
        let _ = fs::remove_dir_all(output_root);
        let _ = fs::remove_dir_all(cache_root);
    }

    #[test]
    fn responses_backend_fails_closed_on_failed_terminal_status() {
        let workspace = temp_dir("responses-failed-workspace");
        let output_root = temp_dir("responses-failed-output");
        let cache_root = temp_dir("responses-failed-cache");
        let (ctx, mock_root, _request_bodies, server) = mock_responses_context("fail-intake");

        let run_dir = create_run(
            &ctx,
            "Проверить fail-closed поведение Responses backend.",
            &workspace,
            &output_root,
            Some("responses-readonly"),
            "compact",
            "ru",
            "research-first",
            "local-first",
            "fetch-if-needed",
            &cache_root.display().to_string(),
            "reuse",
            None,
        )
        .expect("create failed responses run");

        let err = run_stage_capture(&ctx, &run_dir, "start", &["intake"])
            .expect_err("intake should fail on terminal failed status");
        assert!(err.contains("terminal status `failed`"));
        assert!(
            !run_dir.join("brief.md").exists()
                || !read_text(&run_dir.join("brief.md"))
                    .unwrap_or_default()
                    .contains("should not be accepted")
        );

        server.join().expect("join mock responses fail server");
        let _ = fs::remove_dir_all(run_dir);
        let _ = fs::remove_dir_all(output_root);
        let _ = fs::remove_dir_all(workspace);
        let _ = fs::remove_dir_all(cache_root);
        let _ = fs::remove_dir_all(mock_root);
    }

    #[test]
    fn doctor_warns_when_responses_background_mode_is_enabled() {
        let run_dir = temp_run_dir("doctor-responses-background");
        let ctx = Context {
            stage_backend: "responses-readonly".to_string(),
            openai_background: true,
            ..test_context()
        };
        write_text(
            &run_dir.join("request.md"),
            "# Request\n\nWarn about privacy.\n",
        )
        .expect("request");
        write_json(
            &run_dir.join("plan.json"),
            &json!({
                "workspace": run_dir.display().to_string(),
                "workspace_exists": true,
                "prompt_format": "compact",
                "summary_language": "ru",
                "intake_research_mode": "research-first",
                "stage_research_mode": "local-first",
                "execution_network_mode": "fetch-if-needed",
                "goal_gate_enabled": true,
                "augmented_follow_up_enabled": true
            }),
        )
        .expect("plan");

        let report = doctor_report(&ctx, &run_dir).expect("doctor report");
        assert!(report.warnings.iter().any(|issue| {
            issue
                .message
                .contains("Responses background mode is enabled")
        }));

        let _ = fs::remove_dir_all(run_dir);
    }

    #[test]
    fn doctor_uses_persisted_backend_config_instead_of_current_context() {
        let workspace = temp_dir("doctor-persisted-workspace");
        let output_root = temp_dir("doctor-persisted-output");
        let cache_root = temp_dir("doctor-persisted-cache");
        let create_ctx = Context {
            stage_backend: "responses-readonly".to_string(),
            openai_background: true,
            ..test_context()
        };
        let run_dir = create_run(
            &create_ctx,
            "Проверить persisted backend config.",
            &workspace,
            &output_root,
            Some("doctor-persisted"),
            "compact",
            "ru",
            "research-first",
            "local-first",
            "fetch-if-needed",
            &cache_root.display().to_string(),
            "reuse",
            None,
        )
        .expect("create run");
        let current_ctx = Context {
            stage_backend: "codex".to_string(),
            openai_background: false,
            ..test_context()
        };

        let report = doctor_report(&current_ctx, &run_dir).expect("doctor report");
        assert!(report.warnings.iter().any(|issue| {
            issue
                .message
                .contains("Responses background mode is enabled")
        }));

        let _ = fs::remove_dir_all(run_dir);
        let _ = fs::remove_dir_all(output_root);
        let _ = fs::remove_dir_all(workspace);
        let _ = fs::remove_dir_all(cache_root);
    }

    #[test]
    fn responses_requests_complete_without_background_mode() {
        let workspace = temp_dir("responses-foreground-workspace");
        let output_root = temp_dir("responses-foreground-output");
        let cache_root = temp_dir("responses-foreground-cache");
        let (mut ctx, mock_root, _request_bodies, server) = mock_responses_context("foreground");
        ctx.openai_background = false;

        let run_dir = create_run(
            &ctx,
            "Проверить foreground Responses polling.",
            &workspace,
            &output_root,
            Some("responses-foreground"),
            "compact",
            "ru",
            "research-first",
            "local-first",
            "fetch-if-needed",
            &cache_root.display().to_string(),
            "reuse",
            None,
        )
        .expect("create run");

        let intake = run_stage_capture(&ctx, &run_dir, "start", &["intake"]).expect("intake");
        assert_eq!(intake.code, 0);
        assert!(read_text(&run_dir.join("brief.md"))
            .expect("read brief")
            .contains("Responses backend"));

        server.join().expect("join mock responses server");
        let _ = fs::remove_dir_all(run_dir);
        let _ = fs::remove_dir_all(output_root);
        let _ = fs::remove_dir_all(workspace);
        let _ = fs::remove_dir_all(cache_root);
        let _ = fs::remove_dir_all(mock_root);
    }

    #[test]
    fn local_template_detects_hyphenated_hello_world_requests() {
        let plan = Plan {
            original_task: "Create a Python hello-world program.".to_string(),
            ..Plan::default()
        };
        assert_eq!(
            detect_local_template(&plan),
            Some(LocalTemplateKind::HelloWorldPython)
        );
    }

    #[test]
    fn create_run_auto_detects_yaml_pipeline_and_preserves_custom_stage_ids() {
        let ctx = test_context();
        let workspace = temp_dir("pipeline-workspace");
        let output_root = temp_dir("pipeline-output");
        write_text(
            &workspace.join("agpipe.pipeline.yml"),
            custom_pipeline_yaml(),
        )
        .expect("write pipeline yaml");

        let run_dir = create_run(
            &ctx,
            "Провести исследование и реализацию по yaml pipeline.",
            &workspace,
            &output_root,
            Some("yaml-pipeline"),
            "compact",
            "ru",
            "research-first",
            "local-first",
            "fetch-if-needed",
            "~/.cache/multi-agent-pipeline",
            "reuse",
            None,
        )
        .expect("create run");

        let plan = load_plan(&run_dir).expect("load plan");
        let stage_ids: Vec<String> = plan
            .pipeline
            .stages
            .iter()
            .map(|stage| stage.id.clone())
            .collect();
        assert_eq!(
            stage_ids,
            vec![
                "intake",
                "research-a",
                "research-b",
                "synthesis",
                "implement",
                "audit"
            ]
        );
        assert!(
            plan.pipeline.source.ends_with("agpipe.pipeline.yml"),
            "unexpected pipeline source: {}",
            plan.pipeline.source
        );
        assert_eq!(
            plan.solver_roles
                .iter()
                .map(|item| item.solver_id.as_str())
                .collect::<Vec<_>>(),
            vec!["research-a", "research-b"]
        );
        assert_eq!(
            available_stages(&run_dir).expect("available stages"),
            vec![
                "intake",
                "research-a",
                "research-b",
                "synthesis",
                "implement",
                "audit"
            ]
        );
        assert!(stage_prompt_path(&run_dir, "synthesis")
            .expect("synthesis prompt path")
            .ends_with("prompts/level3-synthesis.md"));
        assert!(stage_prompt_path(&run_dir, "audit")
            .expect("audit prompt path")
            .ends_with("prompts/level5-audit.md"));
    }

    #[test]
    fn custom_pipeline_safe_next_and_order_follow_stage_kinds() {
        let ctx = test_context();
        let workspace = temp_dir("pipeline-order-workspace");
        let output_root = temp_dir("pipeline-order-output");
        write_text(
            &workspace.join("agpipe.pipeline.yml"),
            custom_pipeline_yaml(),
        )
        .expect("write pipeline yaml");

        let run_dir = create_run(
            &ctx,
            "Провести исследование и реализацию по yaml pipeline.",
            &workspace,
            &output_root,
            Some("yaml-order"),
            "compact",
            "ru",
            "research-first",
            "local-first",
            "fetch-if-needed",
            "~/.cache/multi-agent-pipeline",
            "reuse",
            None,
        )
        .expect("create run");

        assert_eq!(
            next_stage_for_run(&run_dir).expect("next stage"),
            Some("intake".to_string())
        );
        assert_eq!(
            safe_next_action_for_run(&run_dir).expect("safe next"),
            "start intake"
        );
        let synthesis_err = require_valid_order(&run_dir, "synthesis", false)
            .expect_err("review should be blocked");
        assert!(
            synthesis_err.contains("research-a") || synthesis_err.contains("research-b"),
            "unexpected order error: {synthesis_err}"
        );

        write_text(
            &run_dir.join("brief.md"),
            "# Brief\n\nSubstantive intake output.\n",
        )
        .expect("write brief");
        assert_eq!(
            safe_next_action_for_run(&run_dir).expect("safe next after intake"),
            "start-solvers"
        );

        write_text(
            &run_dir
                .join("solutions")
                .join("research-a")
                .join("RESULT.md"),
            "# Result\n\nResearch A.\n",
        )
        .expect("write research-a");
        write_text(
            &run_dir
                .join("solutions")
                .join("research-b")
                .join("RESULT.md"),
            "# Result\n\nResearch B.\n",
        )
        .expect("write research-b");
        assert_eq!(
            next_stage_for_run(&run_dir).expect("next stage after solvers"),
            Some("synthesis".to_string())
        );

        write_text(
            &run_dir.join("review").join("report.md"),
            "# Review Report\n\nSelected research-a.\n",
        )
        .expect("write review report");
        write_json(
            &run_dir.join("review").join("scorecard.json"),
            &json!({"winner": "research-a"}),
        )
        .expect("write review scorecard");
        write_text(
            &run_dir.join("review").join("user-summary.md"),
            "# User Summary\n\nResearch summary.\n",
        )
        .expect("write review summary");
        assert_eq!(
            next_stage_for_run(&run_dir).expect("next stage after review"),
            Some("implement".to_string())
        );

        write_text(
            &run_dir.join("execution").join("report.md"),
            "# Execution Report\n\nApplied changes.\n",
        )
        .expect("write execution report");
        assert_eq!(
            next_stage_for_run(&run_dir).expect("next stage after execution"),
            Some("audit".to_string())
        );

        write_text(
            &run_dir.join("verification").join("findings.md"),
            "# Findings\n\nNo critical findings.\n",
        )
        .expect("write findings");
        write_text(
            &run_dir.join("verification").join("user-summary.md"),
            "# Verification Summary\n\nAll good.\n",
        )
        .expect("write verification summary");
        write_text(
            &run_dir.join("verification").join("improvement-request.md"),
            "# Improvement Request\n\nNo follow-up.\n",
        )
        .expect("write improvement request");
        write_text(
            &run_dir.join("verification").join("augmented-task.md"),
            "# Augmented Task\n\nNo changes needed.\n",
        )
        .expect("write augmented task");
        write_json(
            &run_dir.join("verification").join("goal-status.json"),
            &json!({
                "goal_complete": true,
                "goal_verdict": "complete",
                "rerun_recommended": false,
                "recommended_next_action": "none"
            }),
        )
        .expect("write goal status");

        assert_eq!(next_stage_for_run(&run_dir).expect("final next"), None);
        assert_eq!(
            safe_next_action_for_run(&run_dir).expect("final safe next"),
            "none"
        );
    }

    #[test]
    fn automate_run_supports_custom_stage_ids_across_the_full_pipeline() {
        let ctx = test_context();
        let workspace = temp_dir("pipeline-automate-workspace");
        let output_root = temp_dir("pipeline-automate-output");
        write_text(&workspace.join("agpipe.pipeline.yml"), custom_pipeline_yaml())
            .expect("write pipeline yaml");

        let run_dir = create_run(
            &ctx,
            "Create a Python hello-world program that prints exactly Hello, world!",
            &workspace,
            &output_root,
            Some("yaml-automate"),
            "compact",
            "ru",
            "research-first",
            "local-first",
            "fetch-if-needed",
            "~/.cache/multi-agent-pipeline",
            "reuse",
            None,
        )
        .expect("create run");

        let result = automate_run(&ctx, &run_dir, "verification", true).expect("automate run");
        assert_eq!(result.code, 0, "unexpected automation output:\n{}", result.stdout);
        let status = status_report(&ctx, &run_dir).expect("status");
        assert_eq!(status.next, "none");
        assert_eq!(status.goal, "complete");
        assert!(
            workspace.join("main.py").exists(),
            "expected hello-world main.py in {}",
            workspace.display()
        );
        assert_eq!(
            status.stages.get("research-a").map(String::as_str),
            Some("done")
        );
        assert_eq!(
            status.stages.get("audit").map(String::as_str),
            Some("done")
        );
    }

    #[test]
    fn auto_roles_for_pipeline_solver_stages_refresh_from_task_kind() {
        let ctx = test_context();
        let workspace = temp_dir("pipeline-auto-role-workspace");
        let output_root = temp_dir("pipeline-auto-role-output");
        write_text(
            &workspace.join("agpipe.pipeline.yml"),
            custom_pipeline_without_roles_yaml(),
        )
        .expect("write pipeline yaml");

        let run_dir = create_run(
            &ctx,
            "Build a backend service.",
            &workspace,
            &output_root,
            Some("yaml-auto-roles"),
            "compact",
            "ru",
            "research-first",
            "local-first",
            "fetch-if-needed",
            "~/.cache/multi-agent-pipeline",
            "reuse",
            None,
        )
        .expect("create run");

        let mut plan = load_plan(&run_dir).expect("load plan");
        let initial_expected = choose_roles(&plan.task_kind, std::cmp::max(1, plan.solver_roles.len()));
        assert_eq!(
            plan.solver_roles.first().map(|role| role.role.as_str()),
            initial_expected.first().map(|role| role.role.as_str())
        );
        assert_eq!(
            plan.pipeline
                .stages
                .iter()
                .find(|stage| stage.id == "research-a")
                .map(|stage| stage.role_source.as_str()),
            Some("auto")
        );

        plan.task_kind = "frontend".to_string();
        save_plan(&run_dir, &plan).expect("save modified plan");
        sync_run_artifacts(&ctx, &run_dir).expect("sync run");

        let updated = load_plan(&run_dir).expect("reload plan");
        let updated_expected =
            choose_roles(&updated.task_kind, std::cmp::max(1, updated.solver_roles.len()));
        assert_eq!(
            updated.solver_roles.first().map(|role| role.role.as_str()),
            updated_expected.first().map(|role| role.role.as_str())
        );
        assert_eq!(
            updated.solver_roles.first().map(|role| role.role.as_str()),
            Some("engineering/engineering-frontend-developer.md")
        );
    }

    #[test]
    fn load_plan_accepts_legacy_string_workstream_hints() {
        let run_dir = temp_run_dir("legacy-workstream-hints");
        write_json(
            &run_dir.join("plan.json"),
            &json!({
                "workspace": run_dir.display().to_string(),
                "prompt_format": "compact",
                "summary_language": "ru",
                "intake_research_mode": "research-first",
                "stage_research_mode": "local-first",
                "execution_network_mode": "fetch-if-needed",
                "goal_gate_enabled": true,
                "augmented_follow_up_enabled": true,
                "workstream_hints": [
                    "native-runtime-parity",
                    "rerun-operator-surface"
                ]
            }),
        )
        .expect("write legacy plan");

        let plan = load_plan(&run_dir).expect("load legacy plan");
        assert_eq!(plan.workstream_hints.len(), 2);
        assert_eq!(plan.workstream_hints[0].name, "native-runtime-parity");
        assert_eq!(plan.workstream_hints[0].goal, "native-runtime-parity");
        assert!(plan.workstream_hints[0].suggested_role.is_empty());
        assert_eq!(plan.workstream_hints[1].name, "rerun-operator-surface");
        assert_eq!(plan.workstream_hints[1].goal, "rerun-operator-surface");

        save_plan(&run_dir, &plan).expect("save normalized plan");
        let saved: serde_json::Value = serde_json::from_str(
            &fs::read_to_string(run_dir.join("plan.json")).expect("read normalized plan"),
        )
        .expect("parse normalized plan");
        let hints = saved
            .get("workstream_hints")
            .and_then(|value| value.as_array())
            .expect("saved workstream hints array");
        assert!(hints[0].is_object());
        assert_eq!(
            hints[0].get("name").and_then(|value| value.as_str()),
            Some("native-runtime-parity")
        );
        assert_eq!(
            hints[0].get("goal").and_then(|value| value.as_str()),
            Some("native-runtime-parity")
        );

        let _ = fs::remove_dir_all(run_dir);
    }

    #[test]
    fn materialize_embedded_repo_root_writes_reference_assets() {
        let repo_root = materialize_embedded_repo_root().expect("materialize embedded repo root");

        assert!(repo_root.join("Cargo.toml").exists());
        assert!(repo_root.join("src").join("main.rs").exists());
        assert!(repo_root.join(ROLE_MAP_REF).exists());
        assert!(repo_root.join(REVIEW_RUBRIC_REF).exists());
        assert!(repo_root.join(VERIFICATION_RUBRIC_REF).exists());
        assert!(repo_root.join(DECOMPOSITION_RULES_REF).exists());
    }

    #[test]
    fn create_follow_up_run_succeeds_with_legacy_workstream_hints() {
        let workspace = temp_dir("legacy-follow-up-workspace");
        let run_dir = temp_run_dir("legacy-follow-up-source");
        let output_root = temp_dir("legacy-follow-up-output");
        let ctx = test_context();

        write_json(
            &run_dir.join("plan.json"),
            &json!({
                "workspace": workspace.display().to_string(),
                "prompt_format": "compact",
                "summary_language": "ru",
                "intake_research_mode": "research-first",
                "stage_research_mode": "local-first",
                "execution_network_mode": "fetch-if-needed",
                "goal_gate_enabled": true,
                "augmented_follow_up_enabled": true,
                "workstream_hints": [
                    "native-runtime-parity",
                    "rerun-operator-surface"
                ]
            }),
        )
        .expect("write source plan");

        fs::create_dir_all(run_dir.join("verification")).expect("create verification dir");
        fs::write(
            run_dir.join("verification").join("findings.md"),
            "# Findings\n\nCompatibility gap reproduced and scoped.\n",
        )
        .expect("write findings");
        fs::write(
            run_dir.join("verification").join("user-summary.md"),
            "# Verification Summary\n\nRerun needs a compatibility follow-up.\n",
        )
        .expect("write verification summary");
        fs::write(
            run_dir.join("verification").join("improvement-request.md"),
            "# Improvement Request\n\nRepair rerun compatibility for legacy workstream hints.\n",
        )
        .expect("write improvement request");
        fs::write(
            run_dir.join("verification").join("augmented-task.md"),
            "# Augmented Task\n\nRepair rerun compatibility for legacy workstream hints and validate the follow-up path.\n",
        )
        .expect("write augmented task");
        write_json(
            &run_dir.join("verification").join("goal-status.json"),
            &json!({
                "goal_complete": false,
                "goal_verdict": "partial",
                "rerun_recommended": true,
                "recommended_next_action": "rerun"
            }),
        )
        .expect("write goal status");

        let result = create_follow_up_run(
            &ctx,
            &run_dir,
            &RerunArgs {
                output_dir: Some(output_root.clone()),
                ..RerunArgs::default()
            },
        )
        .expect("create follow-up run");
        assert_eq!(result.code, 0);

        let new_run = only_child_dir(&output_root);
        assert!(new_run.join("plan.json").exists());
        assert!(new_run.join("brief.md").exists());
        assert!(new_run.join("prompts").join("level1-intake.md").exists());
        assert!(new_run.join("prompts").join("level3-review.md").exists());
        assert!(new_run.join("prompts").join("level4-execution.md").exists());
        assert!(new_run
            .join("prompts")
            .join("level5-verification.md")
            .exists());

        let plan = load_plan(&new_run).expect("load follow-up plan");
        assert_eq!(plan.prompt_format, "compact");
        assert_eq!(plan.summary_language, "ru");

        let status = status_report(&ctx, &new_run).expect("status follow-up run");
        assert_eq!(status.next, "intake");

        let _ = fs::remove_dir_all(run_dir);
        let _ = fs::remove_dir_all(output_root);
        let _ = fs::remove_dir_all(workspace);
    }

    #[test]
    fn stage0_create_run_and_automation_produce_a_complete_mock_pipeline() {
        let workspace = temp_dir("mock-pipeline-workspace");
        let output_root = temp_dir("mock-pipeline-output");
        let cache_root = temp_dir("mock-pipeline-cache");
        let (ctx, mock_root, _bin, invocations_path, tokens_path) =
            mock_codex_context("full-pipeline");

        let questions = task_flow_capture(
            &ctx,
            "interview-questions",
            &vec![
                "--task".to_string(),
                "Нужно собрать тестовый pipeline и проверить cache reuse.".to_string(),
                "--workspace".to_string(),
                workspace.display().to_string(),
                "--output-dir".to_string(),
                output_root.display().to_string(),
                "--language".to_string(),
                "ru".to_string(),
                "--max-questions".to_string(),
                "4".to_string(),
            ],
        )
        .expect("interview questions");
        assert_eq!(questions.code, 0);
        let questions_payload: InterviewQuestionsPayload =
            serde_json::from_str(questions.stdout.trim()).expect("parse interview questions json");
        assert_eq!(questions_payload.questions.len(), 1);

        let answers_path = PathBuf::from(&questions_payload.session_dir).join("answers-ui.json");
        write_json(
            &answers_path,
            &json!([
                {
                    "id": questions_payload.questions[0].id,
                    "question": questions_payload.questions[0].question,
                    "answer": "Да, до verification и с включённым cache."
                }
            ]),
        )
        .expect("write answers");

        let finalized = task_flow_capture(
            &ctx,
            "interview-finalize",
            &vec![
                "--task".to_string(),
                "Нужно собрать тестовый pipeline и проверить cache reuse.".to_string(),
                "--workspace".to_string(),
                workspace.display().to_string(),
                "--session-dir".to_string(),
                questions_payload.session_dir.clone(),
                "--answers-file".to_string(),
                answers_path.display().to_string(),
                "--language".to_string(),
                "ru".to_string(),
            ],
        )
        .expect("interview finalize");
        assert_eq!(finalized.code, 0);
        let finalized_payload: InterviewFinalizePayload =
            serde_json::from_str(finalized.stdout.trim()).expect("parse interview finalize json");
        assert!(PathBuf::from(&finalized_payload.final_task_path).exists());

        let created = task_flow_capture(
            &ctx,
            "create-run",
            &vec![
                "--task-file".to_string(),
                finalized_payload.final_task_path.clone(),
                "--workspace".to_string(),
                workspace.display().to_string(),
                "--output-dir".to_string(),
                output_root.display().to_string(),
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
                "--interview-session".to_string(),
                finalized_payload.session_dir.clone(),
            ],
        )
        .expect("create run");
        assert_eq!(created.code, 0);
        let run_dir = PathBuf::from(created.stdout.trim());
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

        let automation = automate_run(&ctx, &run_dir, "verification", true).expect("automate run");
        assert_eq!(automation.code, 0);
        assert!(automation
            .stdout
            .contains("Completed verification with exit code 0"));

        let plan = load_plan(&run_dir).expect("load completed plan");
        let status = status_report(&ctx, &run_dir).expect("status completed run");
        assert_eq!(status.next, "none");
        assert_eq!(status.goal, "complete");
        assert!(read_text(&run_dir.join("execution").join("report.md"))
            .expect("read execution report")
            .contains("test service successfully"));
        assert!(read_text(&run_dir.join("verification").join("findings.md"))
            .expect("read findings")
            .contains("Mock verification complete"));

        let expected_calls = 2usize + 1 + plan.solver_roles.len() + 1 + 1 + 1;
        assert_eq!(line_count(&invocations_path), expected_calls);
        assert_eq!(token_sum(&tokens_path), expected_calls * 111);

        let _ = fs::remove_dir_all(run_dir);
        let _ = fs::remove_dir_all(output_root);
        let _ = fs::remove_dir_all(workspace);
        let _ = fs::remove_dir_all(cache_root);
        let _ = fs::remove_dir_all(mock_root);
    }

    #[test]
    fn verification_cache_reuse_avoids_a_second_mock_model_run_and_preserves_token_budget() {
        let workspace = temp_dir("mock-cache-workspace");
        let output_root = temp_dir("mock-cache-output");
        let cache_root = temp_dir("mock-cache-root");
        let (ctx, mock_root, _bin, invocations_path, tokens_path) =
            mock_codex_context("cache-reuse");

        let run_dir = create_run(
            &ctx,
            "Проверить cache reuse на verification stage.",
            &workspace,
            &output_root,
            Some("cache-reuse"),
            "compact",
            "ru",
            "research-first",
            "local-first",
            "fetch-if-needed",
            &cache_root.display().to_string(),
            "reuse",
            None,
        )
        .expect("create cache run");
        let plan = load_plan(&run_dir).expect("load cache plan");
        seed_preverification_outputs(&run_dir, &plan);

        let first = run_stage_capture(&ctx, &run_dir, "start", &["verification"])
            .expect("first verification run");
        assert_eq!(first.code, 0);
        assert!(first
            .stdout
            .contains("Completed verification with exit code 0"));
        assert_eq!(line_count(&invocations_path), 1);
        assert_eq!(token_sum(&tokens_path), 111);

        let step_back = run_stage_capture(&ctx, &run_dir, "step-back", &["verification"])
            .expect("step-back verification");
        assert_eq!(step_back.code, 0);

        let second = run_stage_capture(&ctx, &run_dir, "start", &["verification"])
            .expect("second verification run");
        assert_eq!(second.code, 0);
        assert!(second.stdout.contains("Reused cached verification result."));
        assert_eq!(line_count(&invocations_path), 1);
        assert_eq!(token_sum(&tokens_path), 111);
        assert!(read_text(&run_dir.join("verification").join("findings.md"))
            .expect("read cached findings")
            .contains("Mock verification complete"));
        let summary = summarize_token_ledger(&run_dir, &plan).expect("token summary");
        assert!(summary.used_total_tokens > 0);
        assert!(summary.estimated_saved_tokens > 0);
        assert_eq!(
            summary.remaining_tokens,
            Some(
                summary
                    .budget_total_tokens
                    .saturating_sub(summary.used_total_tokens)
            )
        );

        let cache_status = run_stage_capture(&ctx, &run_dir, "cache-status", &["--refresh"])
            .expect("cache status");
        assert_eq!(cache_status.code, 0);
        assert!(cache_status.stdout.contains("stage-results"));

        let _ = fs::remove_dir_all(run_dir);
        let _ = fs::remove_dir_all(output_root);
        let _ = fs::remove_dir_all(workspace);
        let _ = fs::remove_dir_all(cache_root);
        let _ = fs::remove_dir_all(mock_root);
    }

    #[test]
    fn stage_rank_orders_pipeline() {
        assert!(stage_rank("review") > stage_rank("solvers"));
        assert!(stage_rank("none") > stage_rank("verification"));
    }
}
