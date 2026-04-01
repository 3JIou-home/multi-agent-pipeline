use curl::easy::{Easy, List};
use portable_pty::{native_pty_system, CommandBuilder, PtySize};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{json, Value};
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::ffi::CStr;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{mpsc, Arc};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const ROLE_MAP_REF: &str = "references/agency-role-map.md";
const REVIEW_RUBRIC_REF: &str = "references/review-rubric.md";
const VERIFICATION_RUBRIC_REF: &str = "references/verification-rubric.md";
const DECOMPOSITION_RULES_REF: &str = "references/decomposition-rules.md";
const MCP_PLAN_REF: &str = "references/mcp-plan.md";
const MCP_PROVISION_RECORD_REF: &str = "runtime/mcp-provision.json";
const MCP_USAGE_LOG_REF: &str = "runtime/mcp-usage.jsonl";
const REFERENCE_ASSET_RELS: [&str; 4] = [
    ROLE_MAP_REF,
    REVIEW_RUBRIC_REF,
    VERIFICATION_RUBRIC_REF,
    DECOMPOSITION_RULES_REF,
];
const EMBEDDED_ROLE_MAP: &str = include_str!("../references/agency-role-map.md");
const EMBEDDED_REVIEW_RUBRIC: &str = include_str!("../references/review-rubric.md");
const EMBEDDED_VERIFICATION_RUBRIC: &str = include_str!("../references/verification-rubric.md");
const EMBEDDED_DECOMPOSITION_RULES: &str = include_str!("../references/decomposition-rules.md");
static WRITE_TMP_COUNTER: AtomicU64 = AtomicU64::new(0);
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
    "engineering/engineering-code-reviewer.md",
    "testing/testing-test-results-analyzer.md",
];
const BACKEND_CLI_REVIEWER_STACK: [&str; 2] = [
    "testing/testing-reality-checker.md",
    "engineering/engineering-code-reviewer.md",
];
const ANGLE_SEQUENCE: [&str; 3] = ["implementation-first", "architecture-first", "risk-first"];
const RESEARCH_ANGLE_SEQUENCE: [&str; 3] = ["breadth-first", "evidence-first", "risk-first"];
const AI_REVIEWER_STACK: [&str; 3] = [
    "testing/testing-reality-checker.md",
    "testing/testing-tool-evaluator.md",
    "testing/testing-test-results-analyzer.md",
];
const AUDIT_IMPROVE_REVIEWER_STACK: [&str; 2] = [
    "testing/testing-reality-checker.md",
    "engineering/engineering-code-reviewer.md",
];
const RESEARCH_REVIEWER_STACK: [&str; 3] = [
    "testing/testing-reality-checker.md",
    "testing/testing-evidence-collector.md",
    "testing/testing-tool-evaluator.md",
];
const DOCS_REVIEWER_STACK: [&str; 3] = [
    "engineering/engineering-technical-writer.md",
    "testing/testing-reality-checker.md",
    "testing/testing-evidence-collector.md",
];
const PLACEHOLDER_PREFIXES: [&str; 3] = ["pending ", "fill this file", "fill this"];
const RESPONSES_DOC_MAX_CHARS_PER_DOC: usize = 20_000;
const RESPONSES_DOC_TOTAL_CHARS: usize = 80_000;
const RESPONSES_POLL_MAX_ATTEMPTS: u32 = 300;
const RESPONSES_HTTP_MAX_ATTEMPTS: u32 = 4;
const RESPONSES_VERIFICATION_WORKSPACE_MAX_FILES: usize = 12;
const RESPONSES_VERIFICATION_WORKSPACE_MAX_FILE_BYTES: usize = 64 * 1024;
const RUNTIME_CHECK_SPEC_REF: &str = ".agpipe/runtime-check.json";
const RUNTIME_CHECK_LEGACY_SPEC_REF: &str = "agpipe.runtime-check.json";
const SERVICE_CHECK_SPEC_REF: &str = ".agpipe/service-check.json";
const SERVICE_CHECK_LEGACY_SPEC_REF: &str = "agpipe.service-check.json";
const ACTIVE_STAGE_STALL_WARN_SECS: u64 = 90;
const ACTIVE_STAGE_STALL_BROKEN_SECS: u64 = 300;

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
    fn stage_changed(&self, _stage: &str) {}
    fn interrupt_run_dir(&self) -> Option<PathBuf> {
        None
    }
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

fn current_engine_observer() -> Option<Arc<dyn EngineObserver>> {
    ENGINE_OBSERVER.with(|slot| slot.borrow().as_ref().map(Arc::clone))
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

fn notify_stage_changed(stage: &str) {
    ENGINE_OBSERVER.with(|slot| {
        if let Some(observer) = slot.borrow().as_ref() {
            observer.stage_changed(stage);
        }
    });
}

fn current_interrupt_run_dir() -> Option<PathBuf> {
    ENGINE_OBSERVER.with(|slot| {
        slot.borrow()
            .as_ref()
            .and_then(|observer| observer.interrupt_run_dir())
    })
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
    pub fix_actions: Vec<String>,
    #[serde(default)]
    pub last_attempt: Option<RuntimeAttemptPayload>,
    #[serde(default)]
    pub issues: Vec<DoctorIssue>,
    #[serde(default)]
    pub warnings: Vec<DoctorIssue>,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RuntimeAttemptPayload {
    #[serde(default)]
    pub label: String,
    #[serde(default)]
    pub stage: String,
    #[serde(default)]
    pub command_hint: String,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub exit_code: Option<i32>,
    #[serde(default)]
    pub message: String,
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
    #[serde(default)]
    pub last_attempt: Option<RuntimeAttemptPayload>,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct RunSnapshot {
    pub run_dir: PathBuf,
    pub doctor: DoctorPayload,
    pub status: StatusPayload,
    pub token_summary: RunTokenSummary,
    pub solver_stage_ids: Vec<String>,
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
    #[serde(default)]
    mcp_servers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct McpSelection {
    #[serde(default = "default_true")]
    auto_select: bool,
    #[serde(default)]
    rationale: Vec<String>,
    #[serde(default)]
    servers: Vec<McpServerPlan>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct McpServerPlan {
    #[serde(default)]
    name: String,
    #[serde(default)]
    mode: String,
    #[serde(default)]
    stages: Vec<String>,
    #[serde(default)]
    purposes: Vec<String>,
    #[serde(default)]
    usage_hint: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct McpProvisionRecord {
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    codex_home: String,
    #[serde(default)]
    config_path: String,
    #[serde(default)]
    configured: Vec<String>,
    #[serde(default)]
    already_present: Vec<String>,
    #[serde(default)]
    skipped: Vec<String>,
    #[serde(default)]
    warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct McpUsageRecord {
    #[serde(default)]
    recorded_at: String,
    #[serde(default)]
    stage: String,
    #[serde(default)]
    stage_kind: String,
    #[serde(default)]
    backend: String,
    #[serde(default)]
    source: String,
    #[serde(default)]
    selected: Vec<String>,
    #[serde(default)]
    available: Vec<String>,
    #[serde(default)]
    declared_used: Vec<String>,
    #[serde(default)]
    declared_not_used: Vec<String>,
    #[serde(default)]
    observed_mentions: Vec<String>,
    #[serde(default)]
    note_present: bool,
    #[serde(default)]
    artifact_path: String,
    #[serde(default)]
    note_path: String,
    #[serde(default)]
    note_md: String,
}

#[derive(Debug, Clone, Default)]
struct AgencyRoleDoc {
    requested_role: String,
    relative_path: String,
    full_path: PathBuf,
    title: String,
    description: String,
    content: String,
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
    mcp: McpSelection,
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
struct ServiceCheckSpec {
    #[serde(default)]
    version: u8,
    #[serde(default)]
    mode: String,
    #[serde(default)]
    workdir: String,
    #[serde(default)]
    env: BTreeMap<String, String>,
    #[serde(default)]
    prepare_commands: Vec<String>,
    #[serde(default)]
    start_command: String,
    #[serde(default)]
    ready_command: String,
    #[serde(default)]
    ready_timeout_secs: u64,
    #[serde(default)]
    ready_interval_ms: u64,
    #[serde(default)]
    stop_command: String,
    #[serde(default)]
    cleanup_commands: Vec<String>,
    #[serde(default)]
    compose_file: String,
    #[serde(default)]
    compose_services: Vec<String>,
    #[serde(default)]
    compose_project_name: String,
    #[serde(default)]
    scenarios: Vec<ServiceCheckScenario>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct ServiceCheckScenario {
    #[serde(default)]
    id: String,
    #[serde(default)]
    description: String,
    #[serde(default)]
    kind: String,
    #[serde(default)]
    command: String,
    #[serde(default)]
    expect_exit_code: i32,
    #[serde(default)]
    expect_stdout_contains: Vec<String>,
    #[serde(default)]
    expect_stderr_contains: Vec<String>,
    #[serde(default)]
    method: String,
    #[serde(default)]
    url: String,
    #[serde(default)]
    headers: BTreeMap<String, String>,
    #[serde(default)]
    body: String,
    #[serde(default)]
    expect_status: u32,
    #[serde(default)]
    expect_body_contains: Vec<String>,
    #[serde(default)]
    expect_body_not_contains: Vec<String>,
    #[serde(default)]
    rows: u16,
    #[serde(default)]
    cols: u16,
    #[serde(default)]
    steps: Vec<ServiceCheckStep>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct ServiceCheckScenarioResult {
    #[serde(default)]
    id: String,
    #[serde(default)]
    description: String,
    #[serde(default)]
    command: String,
    #[serde(default)]
    exit_code: i32,
    #[serde(default)]
    status: String,
    #[serde(default)]
    failure_reason: String,
    #[serde(default)]
    stdout_path: String,
    #[serde(default)]
    stderr_path: String,
    #[serde(default)]
    screen_path: String,
    #[serde(default)]
    details: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct ServiceCheckSummary {
    #[serde(default)]
    version: u8,
    #[serde(default)]
    phase: String,
    #[serde(default)]
    status: String,
    #[serde(default)]
    mode: String,
    #[serde(default)]
    spec_path: String,
    #[serde(default)]
    workdir: String,
    #[serde(default)]
    started_at: String,
    #[serde(default)]
    finished_at: String,
    #[serde(default)]
    ready_status: String,
    #[serde(default)]
    ready_failure: String,
    #[serde(default)]
    start_log: String,
    #[serde(default)]
    error_messages: Vec<String>,
    #[serde(default)]
    scenarios: Vec<ServiceCheckScenarioResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct ServiceCheckStep {
    #[serde(default)]
    kind: String,
    #[serde(default)]
    command: String,
    #[serde(default)]
    text: String,
    #[serde(default)]
    pattern: String,
    #[serde(default)]
    path: String,
    #[serde(default)]
    timeout_secs: u64,
    #[serde(default)]
    expect_exit_code: i32,
    #[serde(default)]
    expect_stdout_contains: Vec<String>,
    #[serde(default)]
    expect_stderr_contains: Vec<String>,
    #[serde(default)]
    method: String,
    #[serde(default)]
    url: String,
    #[serde(default)]
    headers: BTreeMap<String, String>,
    #[serde(default)]
    body: String,
    #[serde(default)]
    expect_status: u32,
    #[serde(default)]
    expect_body_contains: Vec<String>,
    #[serde(default)]
    expect_body_not_contains: Vec<String>,
    #[serde(default)]
    rows: u16,
    #[serde(default)]
    cols: u16,
    #[serde(default)]
    wait_ms: u64,
    #[serde(default)]
    keys: Vec<String>,
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

#[allow(dead_code)]
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

fn stage_backend_label(backend: StageBackendKind) -> &'static str {
    match backend {
        StageBackendKind::Codex => "codex",
        StageBackendKind::Responses => "responses",
        StageBackendKind::LocalTemplate(_) => "local-template",
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stage0BackendMode {
    Codex,
    Responses,
    Local,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LocalTemplateKind {
    HelloWorldPython,
    ExecutionReadyBackendCliIntake,
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
    #[serde(default)]
    mcp_usage_md: String,
}

#[derive(Debug, Deserialize, Default)]
struct ResponsesSolverPayload {
    #[serde(default)]
    result_md: String,
    #[serde(default)]
    mcp_usage_md: String,
}

#[derive(Debug, Deserialize, Default)]
struct ResponsesReviewPayload {
    #[serde(default)]
    report_md: String,
    #[serde(default)]
    scorecard_json: Value,
    #[serde(default)]
    user_summary_md: String,
    #[serde(default)]
    mcp_usage_md: String,
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
    #[serde(default)]
    mcp_usage_md: String,
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
    note: Option<String>,
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

fn plan_path(run_dir: &Path) -> PathBuf {
    run_dir.join("plan.json")
}

fn plan_snapshot_path(run_dir: &Path) -> PathBuf {
    crate::runtime::runtime_dir(run_dir).join("plan.snapshot.json")
}

pub fn run_has_plan_artifact(run_dir: &Path) -> bool {
    plan_path(run_dir).exists() || plan_snapshot_path(run_dir).exists()
}

pub fn read_plan_artifact_text(run_dir: &Path) -> Result<String, String> {
    let primary = plan_path(run_dir);
    if primary.exists() {
        return read_text(&primary);
    }
    let snapshot = plan_snapshot_path(run_dir);
    if snapshot.exists() {
        return read_text(&snapshot);
    }
    Err(format!(
        "Could not read {} or {}.",
        primary.display(),
        snapshot.display()
    ))
}

pub fn discover_run_dirs(root: &Path) -> Result<Vec<PathBuf>, String> {
    if !root.exists() {
        return Ok(Vec::new());
    }
    let mut runs: Vec<PathBuf> = fs::read_dir(root)
        .map_err(|err| format!("Could not read {}: {err}", root.display()))?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.is_dir() && run_has_plan_artifact(path))
        .collect();
    runs.sort();
    runs.reverse();
    Ok(runs)
}

fn read_text(path: &Path) -> Result<String, String> {
    fs::read_to_string(path).map_err(|err| format!("Could not read {}: {err}", path.display()))
}

fn write_text(path: &Path, content: &str) -> Result<(), String> {
    let parent = path.parent().map(PathBuf::from);
    if let Some(parent) = parent.as_ref() {
        fs::create_dir_all(parent)
            .map_err(|err| format!("Could not create {}: {err}", parent.display()))?;
    }
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    let sequence = WRITE_TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let tmp = if let Some(parent) = parent.as_ref() {
        let filename = path
            .file_name()
            .and_then(|item| item.to_str())
            .unwrap_or("artifact");
        parent.join(format!(
            ".{filename}.tmp-{}-{sequence}-{suffix}",
            std::process::id()
        ))
    } else {
        path.with_extension(format!(
            "tmp-{}-{sequence}-{suffix}",
            std::process::id()
        ))
    };
    fs::write(&tmp, format!("{}\n", content.trim_end()))
        .map_err(|err| format!("Could not write {}: {err}", tmp.display()))?;
    if let Some(parent) = parent.as_ref() {
        fs::create_dir_all(parent)
            .map_err(|err| format!("Could not create {}: {err}", parent.display()))?;
    }
    fs::rename(&tmp, path).map_err(|err| {
        format!(
            "Could not move {} to {}: {err}",
            tmp.display(),
            path.display()
        )
    })
}

fn write_json<T: Serialize>(path: &Path, payload: &T) -> Result<(), String> {
    let content = serde_json::to_string_pretty(payload)
        .map_err(|err| format!("Could not serialize {}: {err}", path.display()))?;
    write_text(path, &content)
}

fn append_jsonl<T: Serialize>(path: &Path, payload: &T) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|err| format!("Could not create {}: {err}", parent.display()))?;
    }
    let line = serde_json::to_string(payload)
        .map_err(|err| format!("Could not serialize {}: {err}", path.display()))?;
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|err| format!("Could not open {}: {err}", path.display()))?;
    writeln!(file, "{line}").map_err(|err| format!("Could not append {}: {err}", path.display()))
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

fn discover_agency_agents_dir_for_repo_root(repo_root: &Path) -> Option<PathBuf> {
    if let Ok(path) = env::var("AGENCY_AGENTS_DIR") {
        let candidate = PathBuf::from(path);
        if candidate.exists() {
            return Some(candidate);
        }
    }
    let parent = repo_root.parent()?;
    let sibling = parent.parent().unwrap_or(parent).join("agency-agents");
    if sibling.exists() {
        return Some(sibling);
    }
    if let Ok(home) = env::var("HOME") {
        let home_catalog = PathBuf::from(home).join("agency-agents");
        if home_catalog.exists() {
            return Some(home_catalog);
        }
    }
    None
}

fn frontmatter_field(markdown: &str, field: &str) -> Option<String> {
    let mut lines = markdown.lines();
    if lines.next()?.trim() != "---" {
        return None;
    }
    for line in lines {
        let trimmed = line.trim();
        if trimmed == "---" {
            break;
        }
        if let Some(value) = trimmed.strip_prefix(&format!("{field}:")) {
            return Some(strip_matching_quotes(value.trim()).to_string());
        }
    }
    None
}

fn resolve_agency_role_path(agency_root: &Path, role: &str) -> Option<PathBuf> {
    let requested = strip_matching_quotes(role.trim())
        .trim_matches('`')
        .trim()
        .trim_start_matches("./")
        .to_string();
    if requested.is_empty() {
        return None;
    }
    let mut exact_candidates = vec![requested.clone()];
    if !requested.ends_with(".md") {
        exact_candidates.push(format!("{requested}.md"));
    }
    for relative in &exact_candidates {
        let candidate = agency_root.join(relative);
        if candidate.is_file() {
            return Some(candidate);
        }
    }

    let target_file_name = exact_candidates.iter().find_map(|candidate| {
        Path::new(candidate)
            .file_name()
            .and_then(|value| value.to_str())
            .map(|value| value.to_string())
    })?;
    let mut matches = walk_tree(agency_root, 6, &[".git", ".github", "scripts"]).ok()?;
    matches.sort();
    let mut basename_match = None;
    for path in matches {
        if path.extension().and_then(|ext| ext.to_str()) != Some("md") {
            continue;
        }
        let relative = path
            .strip_prefix(agency_root)
            .ok()
            .map(|value| value.to_string_lossy().replace('\\', "/"))
            .unwrap_or_else(|| path.to_string_lossy().replace('\\', "/"));
        if exact_candidates
            .iter()
            .any(|candidate| relative == *candidate || relative.ends_with(&format!("/{candidate}")))
        {
            return Some(path);
        }
        if basename_match.is_none()
            && path.file_name().and_then(|value| value.to_str()) == Some(target_file_name.as_str())
        {
            basename_match = Some(path);
        }
    }
    basename_match
}

fn load_agency_role_doc(ctx: &Context, role: &str) -> Option<AgencyRoleDoc> {
    let agency_root = discover_agency_agents_dir(ctx)?;
    let full_path = resolve_agency_role_path(&agency_root, role)?;
    let content = read_text(&full_path).ok()?;
    let relative_path = full_path
        .strip_prefix(&agency_root)
        .ok()
        .map(|value| value.to_string_lossy().replace('\\', "/"))
        .unwrap_or_else(|| full_path.to_string_lossy().replace('\\', "/"));
    Some(AgencyRoleDoc {
        requested_role: role.to_string(),
        relative_path,
        full_path,
        title: frontmatter_field(&content, "name").unwrap_or_default(),
        description: frontmatter_field(&content, "description").unwrap_or_default(),
        content,
    })
}

fn load_agency_role_docs(ctx: &Context, roles: &[String]) -> Vec<AgencyRoleDoc> {
    let mut seen = BTreeSet::new();
    let mut docs = Vec::new();
    for role in roles {
        let Some(doc) = load_agency_role_doc(ctx, role) else {
            continue;
        };
        if seen.insert(doc.relative_path.clone()) {
            docs.push(doc);
        }
    }
    docs
}

fn agency_role_catalog_summary(ctx: &Context) -> Option<String> {
    let agency_root = discover_agency_agents_dir(ctx)?;
    let mut files = walk_tree(&agency_root, 6, &[".git", ".github", "scripts"]).ok()?;
    files.sort();
    let mut entries = Vec::new();
    for path in files {
        if path.extension().and_then(|ext| ext.to_str()) != Some("md") {
            continue;
        }
        let Ok(content) = read_text(&path) else {
            continue;
        };
        let Some(name) = frontmatter_field(&content, "name") else {
            continue;
        };
        let Some(description) = frontmatter_field(&content, "description") else {
            continue;
        };
        let relative = path
            .strip_prefix(&agency_root)
            .ok()
            .map(|value| value.to_string_lossy().replace('\\', "/"))
            .unwrap_or_else(|| path.to_string_lossy().replace('\\', "/"));
        entries.push(format!("- `{relative}`: {name} — {description}"));
    }
    if entries.is_empty() {
        return None;
    }
    Some(format!(
        "# Agency Role Catalog\n\nCatalog root: `{}`\n\nAvailable role documents:\n{}",
        agency_root.display(),
        entries.join("\n")
    ))
}

fn is_signal_boundary_char(ch: char) -> bool {
    ch.is_alphanumeric() || ch == '_'
}

fn signal_requires_word_boundary(signal: &str) -> bool {
    matches!(signal, "ui" | "ai" | "ml" | "ci" | "cd")
}

fn text_contains_signal(text: &str, signal: &str) -> bool {
    if signal.is_empty() {
        return false;
    }
    if !signal_requires_word_boundary(signal) {
        return text.contains(signal);
    }
    text.match_indices(signal).any(|(start, matched)| {
        let before = text[..start].chars().next_back();
        let after = text[start + matched.len()..].chars().next();
        let before_ok = before
            .map(|ch| !is_signal_boundary_char(ch))
            .unwrap_or(true);
        let after_ok = after.map(|ch| !is_signal_boundary_char(ch)).unwrap_or(true);
        before_ok && after_ok
    })
}

fn count_signals(text: &str, signals: &[&str]) -> usize {
    signals
        .iter()
        .filter(|signal| text_contains_signal(text, signal))
        .count()
}

fn task_requests_code_review(task: &str) -> bool {
    let text = task.to_lowercase();
    let review_intent = count_signals(
        &text,
        &[
            "code review",
            "review the code",
            "review code",
            "review the repo",
            "review repository",
            "review project",
            "audit the code",
            "code audit",
            "ревью",
            "ревью кода",
            "проведи ревью",
            "сделай ревью",
            "аудит кода",
            "проведи аудит кода",
            "проверь код",
            "проверь репозиторий",
        ],
    ) > 0;
    if !review_intent {
        return false;
    }
    count_signals(
        &text,
        &[
            "code",
            "repo",
            "repository",
            "workspace",
            "file",
            "files",
            "код",
            "репо",
            "файл",
            "файлы",
            ".py",
            ".rs",
            ".ts",
            ".js",
            ".go",
            ".java",
            ".kt",
            ".swift",
            ".php",
            ".rb",
        ],
    ) > 0
        || text.contains("~/")
        || text.contains("/users/")
}

fn task_requests_cli_entrypoint(task_kind: &str, task: &str) -> bool {
    if task_kind != "backend" {
        return false;
    }
    let text = task.to_lowercase();
    [
        "cli",
        "command line",
        "console",
        "script",
        "скрипт",
        "entrypoint",
        "entry point",
        "main.py",
        "stdout",
        "print",
        "prints",
        "печата",
        "вывод",
        "hello world",
    ]
    .iter()
    .any(|signal| text_contains_signal(&text, signal))
}

fn task_requests_precise_local_contract(task: &str) -> bool {
    let text = task.to_lowercase();
    [
        "stdout",
        "exact",
        "exactly",
        "deterministic",
        "print",
        "prints",
        "output",
        "readme",
        "run command",
        "печата",
        "ровно",
        "точно",
        "детермин",
        "вывод",
        "readme.md",
    ]
    .iter()
    .any(|signal| text_contains_signal(&text, signal))
}

fn task_is_trivial_local_cli_contract(
    task_kind: &str,
    complexity: &str,
    task: &str,
    workspace: Option<&Path>,
) -> bool {
    if task_kind != "backend" || complexity != "low" {
        return false;
    }
    if !task_requests_cli_entrypoint(task_kind, task) || !task_requests_precise_local_contract(task)
    {
        return false;
    }
    match workspace {
        Some(path) => !path.exists() || workspace_looks_greenfield(path),
        None => true,
    }
}

fn task_requests_readme(task: &str) -> bool {
    let text = task.to_lowercase();
    text_contains_signal(&text, "readme") || text_contains_signal(&text, "readme.md")
}

fn stack_signals_are_empty(signals: &StackSignals) -> bool {
    !signals.package_json
        && !signals.pyproject_toml
        && !signals.pytest_suite
        && !signals.go_mod
        && !signals.cargo_toml
        && !signals.makefile
        && !signals.terraform
}

fn workspace_looks_greenfield(workspace: &Path) -> bool {
    if !workspace.exists() {
        return true;
    }
    let files = walk_tree(
        workspace,
        2,
        &[
            ".git",
            "node_modules",
            ".venv",
            "venv",
            "__pycache__",
            "target",
            "build",
            "dist",
        ],
    )
    .unwrap_or_default();
    let visible_files = files
        .into_iter()
        .filter(|path| {
            path.file_name()
                .and_then(|value| value.to_str())
                .map(|name| !name.starts_with('.'))
                .unwrap_or(false)
        })
        .take(3)
        .count();
    visible_files <= 2
}

fn role_matrix(task_kind: &str, task: &str) -> Vec<&'static str> {
    match task_kind {
        "review" => vec![
            "engineering/engineering-code-reviewer.md",
            "testing/testing-reality-checker.md",
            "testing/testing-test-results-analyzer.md",
        ],
        "audit-improve" => vec![
            "engineering/engineering-backend-architect.md",
            "engineering/engineering-technical-writer.md",
            "testing/testing-tool-evaluator.md",
        ],
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
        "backend" => {
            if task_requests_cli_entrypoint(task_kind, task) {
                vec![
                    "engineering/engineering-rapid-prototyper.md",
                    "engineering/engineering-technical-writer.md",
                    "engineering/engineering-backend-architect.md",
                ]
            } else {
                vec![
                    "engineering/engineering-senior-developer.md",
                    "engineering/engineering-backend-architect.md",
                    "engineering/engineering-devops-automator.md",
                ]
            }
        }
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
            "testing/testing-evidence-collector.md",
            "support/support-executive-summary-generator.md",
        ],
        "research" => vec![
            "product/product-trend-researcher.md",
            "testing/testing-evidence-collector.md",
            "testing/testing-tool-evaluator.md",
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
    if task_requests_code_review(task) {
        return "review".to_string();
    }
    if task_is_verification_seed_follow_up(task) || task_requests_audit_improve_follow_up(task) {
        return "audit-improve".to_string();
    }
    let ai_hits = count_signals(
        &text,
        &[
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
        ],
    );
    let frontend_hits = count_signals(
        &text,
        &[
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
        ],
    );
    let backend_hits = count_signals(
        &text,
        &[
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
        ],
    );
    let security_hits = count_signals(
        &text,
        &[
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
        ],
    );
    let infra_hits = count_signals(
        &text,
        &[
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
        ],
    );
    let docs_hits = count_signals(
        &text,
        &[
            "docs",
            "documentation",
            "documented",
            "official docs",
            "reference",
            "readme",
            "guide",
            "summary",
            "spec",
            "документац",
            "гайд",
            "резюме",
            "спек",
            "описан",
            "официальн",
            "справк",
        ],
    );
    let research_hits = count_signals(
        &text,
        &[
            "compare",
            "evaluate",
            "research",
            "recommend",
            "choose",
            "options",
            "analysis",
            "analyze",
            "report",
            "review",
            "overview",
            "findings",
            "technology",
            "technologies",
            "landscape",
            "trend",
            "сравн",
            "оцен",
            "исслед",
            "рекомен",
            "выбор",
            "вариант",
            "анализ",
            "отч",
            "обзор",
            "разбор",
            "вывод",
            "находк",
            "технолог",
            "ландшафт",
            "тренд",
        ],
    );
    let non_implementation_request = !task_requests_workspace_changes(task);

    if count_signals(
        &text,
        &["skill", "prompt", "codex", "скил", "промт", "кодекс"],
    ) >= 2
    {
        return "skill".to_string();
    }
    if non_implementation_request {
        if docs_hits >= 2 && docs_hits >= research_hits && docs_hits >= backend_hits {
            return "docs".to_string();
        }
        if research_hits >= 2 && research_hits >= infra_hits && research_hits >= backend_hits {
            return "research".to_string();
        }
        if research_hits >= 1
            && frontend_hits == 0
            && backend_hits == 0
            && infra_hits == 0
            && security_hits == 0
        {
            return "research".to_string();
        }
    }
    if ai_hits >= 2 {
        return "ai".to_string();
    }
    if security_hits >= 2 {
        return "security".to_string();
    }
    if infra_hits >= 2 {
        return "infra".to_string();
    }
    if docs_hits >= 2 {
        return "docs".to_string();
    }
    if research_hits >= 2 {
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

fn task_prefers_context7(task_kind: &str, task: &str) -> bool {
    if task_is_trivial_local_cli_contract(task_kind, "low", task, None) {
        return false;
    }
    let text = task.to_lowercase();
    matches!(
        task_kind,
        "backend" | "frontend" | "fullstack" | "ai" | "docs"
    ) && [
        "sdk",
        "framework",
        "library",
        "dependency",
        "package",
        "version",
        "upgrade",
        "migrate",
        "migration",
        "integrat",
        "api",
        "docs",
        "documentation",
        "spec",
        "protocol",
        "typescript",
        "python",
        "rust",
        "react",
        "next.js",
        "fastapi",
        "django",
        "flask",
        "cargo",
        "npm",
        "pip",
        "библиот",
        "зависим",
        "документац",
        "верси",
        "миграц",
        "интеграц",
        "протокол",
        "sdk",
        "api",
    ]
    .iter()
    .any(|signal| text_contains_signal(&text, signal))
}

fn context7_api_key_available() -> bool {
    env::var("CONTEXT7_API_KEY")
        .ok()
        .map(|value| !value.trim().is_empty())
        .unwrap_or(false)
}

fn task_prefers_exa(task_kind: &str, task: &str) -> bool {
    if task_kind == "audit-improve" {
        return false;
    }
    let text = task.to_lowercase();
    matches!(task_kind, "research" | "docs")
        || [
            "research",
            "analy",
            "analysis",
            "compare",
            "comparison",
            "evaluate",
            "recommend",
            "choose",
            "search",
            "lookup",
            "survey",
            "landscape",
            "trend",
            "market",
            "latest",
            "technology",
            "technologies",
            "tooling",
            "исслед",
            "анализ",
            "сравн",
            "оцен",
            "рекомен",
            "подбери",
            "найди",
            "технолог",
            "рынок",
            "последн",
            "актуальн",
        ]
        .iter()
        .any(|signal| text_contains_signal(&text, signal))
}

fn task_should_enable_fetch_mcp(task_kind: &str, task: &str) -> bool {
    if task_kind == "audit-improve" {
        return false;
    }
    if task_is_trivial_local_cli_contract(task_kind, "low", task, None) {
        return false;
    }
    matches!(task_kind, "research" | "docs")
        || task_prefers_context7(task_kind, task)
        || task_prefers_exa(task_kind, task)
}

fn workspace_is_git_repo(workspace: &Path) -> bool {
    if workspace.join(".git").exists() {
        return true;
    }
    Command::new("git")
        .arg("-C")
        .arg(workspace)
        .arg("rev-parse")
        .arg("--is-inside-work-tree")
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false)
}

fn task_should_enable_git_mcp(task_kind: &str, workspace: Option<&Path>) -> bool {
    matches!(task_kind, "review" | "audit-improve")
        && workspace.map(workspace_is_git_repo).unwrap_or(false)
}

fn task_should_enable_memory_mcp(task_kind: &str, complexity: &str, solver_count: usize) -> bool {
    solver_count > 1
        || complexity != "low"
        || matches!(
            task_kind,
            "research" | "docs" | "review" | "audit-improve" | "ai" | "backend" | "fullstack"
        )
}

fn dedupe_strings(items: impl IntoIterator<Item = String>) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut ordered = Vec::new();
    for item in items {
        if item.trim().is_empty() {
            continue;
        }
        if seen.insert(item.clone()) {
            ordered.push(item);
        }
    }
    ordered
}

fn infer_mcp_selection(
    task_kind: &str,
    complexity: &str,
    task: &str,
    solver_count: usize,
    workspace: Option<&Path>,
) -> McpSelection {
    let trivial_local_cli = task_is_trivial_local_cli_contract(task_kind, complexity, task, workspace);
    let mut rationale = Vec::new();
    let mut servers = Vec::new();
    let mut stage_scope = vec![
        "intake".to_string(),
        "solver".to_string(),
        "review".to_string(),
        "verification".to_string(),
    ];
    if default_pipeline_includes_execution(task_kind, task) {
        stage_scope.insert(3, "execution".to_string());
    }

    if task_prefers_context7(task_kind, task) && !trivial_local_cli && context7_api_key_available() {
        let mut context7_stages = vec![
            "intake".to_string(),
            "solver".to_string(),
            "review".to_string(),
        ];
        if matches!(task_kind, "docs") || task_is_analysis_only(task) {
            context7_stages.push("verification".to_string());
        }
        rationale.push(
            "Selected `context7` because the task looks documentation, SDK, framework, or version sensitive."
                .to_string(),
        );
        servers.push(McpServerPlan {
            name: "context7".to_string(),
            mode: "readonly".to_string(),
            stages: context7_stages,
            purposes: vec![
                "official docs".to_string(),
                "version-aware API reference".to_string(),
            ],
            usage_hint: "Prefer Context7 for official library, framework, protocol, and version-specific docs before generic web browsing.".to_string(),
        });
    } else if task_prefers_context7(task_kind, task) && !trivial_local_cli {
        rationale.push(
            "Skipped `context7` because `CONTEXT7_API_KEY` is not available; falling back to official `fetch` plus normal browsing and local context."
                .to_string(),
        );
    } else if task_prefers_exa(task_kind, task) {
        rationale.push(
            "Selected `exa` because the task looks research, analysis, or source-discovery heavy."
                .to_string(),
        );
        servers.push(McpServerPlan {
            name: "exa".to_string(),
            mode: "readonly".to_string(),
            stages: vec![
                "intake".to_string(),
                "solver".to_string(),
                "review".to_string(),
            ],
            purposes: vec![
                "source discovery".to_string(),
                "external research".to_string(),
            ],
            usage_hint: "Prefer Exa for source discovery, literature/web research, and collecting primary references before synthesizing conclusions.".to_string(),
        });
    }

    if task_should_enable_fetch_mcp(task_kind, task) && !trivial_local_cli {
        let mut fetch_stages = vec![
            "intake".to_string(),
            "solver".to_string(),
            "review".to_string(),
        ];
        if matches!(task_kind, "research" | "docs") || task_is_analysis_only(task) {
            fetch_stages.push("verification".to_string());
        }
        rationale.push(
            "Selected official `fetch` as a fallback page-retrieval MCP so stages can read specific URLs directly when search or docs servers surface concrete sources."
                .to_string(),
        );
        servers.push(McpServerPlan {
            name: "fetch".to_string(),
            mode: "readonly".to_string(),
            stages: fetch_stages,
            purposes: vec![
                "page retrieval".to_string(),
                "direct source reading".to_string(),
            ],
            usage_hint: "Prefer Fetch after Context7 or Exa has identified a specific URL that needs direct chunked retrieval.".to_string(),
        });
    }

    if task_should_enable_git_mcp(task_kind, workspace) {
        rationale.push(
            "Selected official `git` because this is a review workflow on a Git repository and stage agents can benefit from commit, branch, and diff inspection."
                .to_string(),
        );
        servers.push(McpServerPlan {
            name: "git".to_string(),
            mode: "readonly".to_string(),
            stages: vec![
                "intake".to_string(),
                "solver".to_string(),
                "review".to_string(),
                "verification".to_string(),
            ],
            purposes: vec![
                "history inspection".to_string(),
                "diff inspection".to_string(),
            ],
            usage_hint: "Prefer Git for read-only repository inspection such as status, diff, show, log, and branch listing during review workflows.".to_string(),
        });
    }

    if task_should_enable_memory_mcp(task_kind, complexity, solver_count) {
        let mut memory_stage_scope = vec![
            "intake".to_string(),
            "review".to_string(),
            "verification".to_string(),
        ];
        if default_pipeline_includes_execution(task_kind, task) {
            memory_stage_scope.insert(2, "execution".to_string());
        }
        rationale.push(
            "Selected `memory` because this run is multi-stage enough to benefit from explicit recall and durable handoffs, while keeping solver stages self-contained for better reliability."
                .to_string(),
        );
        servers.push(McpServerPlan {
            name: "memory".to_string(),
            mode: "memory".to_string(),
            stages: memory_stage_scope,
            purposes: vec![
                "cross-stage continuity".to_string(),
                "durable handoffs".to_string(),
            ],
            usage_hint: "Use memory for durable decisions and stage handoffs outside the solver stage; keep solver reasoning self-contained unless a future task explicitly requires shared memory.".to_string(),
        });
    }

    McpSelection {
        auto_select: true,
        rationale,
        servers,
    }
}

fn solver_assigned_mcp_servers(plan: &McpSelection) -> Vec<String> {
    let mut names = Vec::new();
    for preferred in ["context7", "exa", "fetch", "git"] {
        if plan
            .servers
            .iter()
            .any(|server| server.name == preferred && server.stages.iter().any(|item| item == "solver"))
        {
            names.push(preferred.to_string());
        }
    }
    if plan
        .servers
        .iter()
        .any(|server| server.name == "memory" && server.stages.iter().any(|item| item == "solver"))
    {
        names.push("memory".to_string());
    }
    dedupe_strings(names)
}

fn effective_solver_mcp_servers(plan: &Plan, solver: &SolverRole) -> Vec<String> {
    let assigned = if solver.mcp_servers.is_empty() {
        solver_assigned_mcp_servers(&plan.mcp)
    } else {
        solver.mcp_servers.clone()
    };
    assigned
        .into_iter()
        .filter(|name| {
            plan.mcp.servers.iter().any(|server| {
                server.name == *name && server.stages.iter().any(|item| item == "solver")
            })
        })
        .collect()
}

fn ensure_plan_mcp_defaults(plan: &mut Plan) {
    let workspace = PathBuf::from(plan.workspace.trim());
    let workspace = if plan.workspace.trim().is_empty() {
        None
    } else {
        Some(workspace.as_path())
    };
    if plan.mcp.auto_select || plan.mcp.servers.is_empty() {
        plan.mcp = infer_mcp_selection(
            &plan.task_kind,
            &plan.complexity,
            &plan.original_task,
            std::cmp::max(1, plan.solver_roles.len()),
            workspace,
        );
    }
    let assigned = solver_assigned_mcp_servers(&plan.mcp);
    for solver in &mut plan.solver_roles {
        solver.mcp_servers = assigned.clone();
    }
    plan.references
        .insert("mcp_plan".to_string(), MCP_PLAN_REF.to_string());
}

fn mcp_auto_provision_enabled() -> bool {
    if env_flag("AGPIPE_DISABLE_MCP_PROVISION").unwrap_or(false) {
        return false;
    }
    if cfg!(test) && !env_flag("AGPIPE_TEST_ALLOW_MCP_PROVISION").unwrap_or(false) {
        return false;
    }
    env_flag("AGPIPE_AUTO_CONFIGURE_MCP").unwrap_or(true)
}

fn codex_home_dir() -> Result<PathBuf, String> {
    if let Ok(path) = env::var("CODEX_HOME") {
        return PathBuf::from(path).expanduser().resolve();
    }
    let home = env::var("HOME").map_err(|_| "HOME is not set.".to_string())?;
    Ok(PathBuf::from(home).join(".codex"))
}

fn codex_config_path(codex_home_override: Option<&Path>) -> Result<(PathBuf, PathBuf), String> {
    let codex_home = match codex_home_override {
        Some(path) => path.expanduser().resolve()?,
        None => codex_home_dir()?,
    };
    Ok((codex_home.clone(), codex_home.join("config.toml")))
}

fn mcp_provision_record_path(run_dir: &Path) -> PathBuf {
    run_dir.join(MCP_PROVISION_RECORD_REF)
}

fn read_mcp_provision_record(run_dir: &Path) -> Option<McpProvisionRecord> {
    read_json(&mcp_provision_record_path(run_dir)).ok()
}

fn is_executable_file(path: &Path) -> bool {
    fs::metadata(path)
        .map(|meta| meta.is_file() && (meta.permissions().mode() & 0o111 != 0))
        .unwrap_or(false)
}

fn command_exists(name: &str) -> bool {
    if name.contains('/') {
        return is_executable_file(&PathBuf::from(name));
    }
    env::var_os("PATH")
        .into_iter()
        .flat_map(|paths| env::split_paths(&paths).collect::<Vec<_>>())
        .any(|dir| is_executable_file(&dir.join(name)))
}

fn config_has_mcp_server(config: &str, name: &str) -> bool {
    let header = format!("[mcp_servers.{name}]");
    config.lines().any(|line| line.trim() == header)
}

fn render_context7_mcp_block() -> String {
    [
        "[mcp_servers.context7]",
        "enabled = true",
        "url = \"https://mcp.context7.com/mcp\"",
        "env_http_headers = { \"CONTEXT7_API_KEY\" = \"CONTEXT7_API_KEY\" }",
        "startup_timeout_sec = 20.0",
        "tool_timeout_sec = 60.0",
    ]
    .join("\n")
}

fn render_exa_mcp_block() -> String {
    [
        "[mcp_servers.exa]",
        "enabled = true",
        "url = \"https://mcp.exa.ai/mcp\"",
        "startup_timeout_sec = 20.0",
        "tool_timeout_sec = 60.0",
    ]
    .join("\n")
}

fn render_fetch_mcp_block() -> Option<String> {
    if command_exists("docker") {
        return Some(
            [
                "[mcp_servers.fetch]",
                "enabled = true",
                "command = \"docker\"",
                "args = [\"run\", \"-i\", \"--rm\", \"mcp/fetch\"]",
                "enabled_tools = [\"fetch\"]",
                "startup_timeout_sec = 90.0",
                "tool_timeout_sec = 60.0",
            ]
            .join("\n"),
        );
    }
    None
}

fn render_git_mcp_block() -> Option<String> {
    if !command_exists("docker") {
        return None;
    }
    let home = env::var("HOME").ok()?;
    Some(
        format!(
            concat!(
                "[mcp_servers.git]\n",
                "enabled = true\n",
                "command = \"docker\"\n",
                "args = [\"run\", \"--rm\", \"-i\", \"--mount\", \"type=bind,src={},dst={}\", \"mcp/git\"]\n",
                "enabled_tools = [\"git_status\", \"git_diff_unstaged\", \"git_diff_staged\", \"git_diff\", \"git_log\", \"git_show\", \"git_branch\"]\n",
                "startup_timeout_sec = 90.0\n",
                "tool_timeout_sec = 60.0"
            ),
            home,
            home
        )
    )
}

fn render_memory_mcp_block(codex_home: &Path) -> Option<(String, String)> {
    if command_exists("docker") {
        return Some((
            "memory".to_string(),
            [
                "[mcp_servers.memory]",
                "enabled = true",
                "command = \"docker\"",
                "args = [\"run\", \"-i\", \"--rm\", \"-v\", \"agpipe-codex-memory:/app/dist\", \"mcp/memory\"]",
                "startup_timeout_sec = 90.0",
                "tool_timeout_sec = 60.0",
            ]
            .join("\n"),
        ));
    }
    if command_exists("npx") {
        let memory_root = codex_home.join("mcp-memory");
        let memory_file = memory_root.join("memory.json");
        return Some((
            "memory".to_string(),
            format!(
                concat!(
                    "[mcp_servers.memory]\n",
                    "enabled = true\n",
                    "command = \"npx\"\n",
                    "args = [\"-y\", \"@modelcontextprotocol/server-memory\"]\n",
                    "cwd = \"{}\"\n",
                    "env = {{ \"MEMORY_FILE_PATH\" = \"{}\" }}\n",
                    "startup_timeout_sec = 45.0\n",
                    "tool_timeout_sec = 60.0"
                ),
                memory_root.display(),
                memory_file.display()
            ),
        ));
    }
    None
}

fn desired_mcp_config_block(codex_home: &Path, name: &str) -> Result<Option<String>, String> {
    match name {
        "context7" => Ok(Some(render_context7_mcp_block())),
        "exa" => Ok(Some(render_exa_mcp_block())),
        "fetch" => Ok(render_fetch_mcp_block()),
        "git" => Ok(render_git_mcp_block()),
        "memory" => Ok(render_memory_mcp_block(codex_home).map(|(_, block)| block)),
        _ => Ok(None),
    }
}

fn provision_selected_mcp_servers(
    run_dir: &Path,
    plan: &Plan,
    codex_home_override: Option<&Path>,
) -> McpProvisionRecord {
    let mut record = McpProvisionRecord {
        enabled: mcp_auto_provision_enabled(),
        ..McpProvisionRecord::default()
    };
    if !record.enabled {
        record
            .warnings
            .push("Automatic MCP provisioning is disabled.".to_string());
        let _ = write_json(&mcp_provision_record_path(run_dir), &record);
        return record;
    }
    let (codex_home, config_path) = match codex_config_path(codex_home_override) {
        Ok(paths) => paths,
        Err(err) => {
            record.warnings.push(err);
            let _ = write_json(&mcp_provision_record_path(run_dir), &record);
            return record;
        }
    };
    record.codex_home = codex_home.display().to_string();
    record.config_path = config_path.display().to_string();

    let mut existing = read_text(&config_path).unwrap_or_default();
    let mut appended_blocks = Vec::new();
    for server in &plan.mcp.servers {
        if config_has_mcp_server(&existing, &server.name) {
            record.already_present.push(server.name.clone());
            continue;
        }
        match desired_mcp_config_block(&codex_home, &server.name) {
            Ok(Some(block)) => {
                appended_blocks.push(block);
                record.configured.push(server.name.clone());
                existing.push('\n');
                existing.push_str(&format!("[mcp_servers.{}]\n", server.name));
            }
            Ok(None) => record.skipped.push(server.name.clone()),
            Err(err) => record.warnings.push(format!("{}: {err}", server.name)),
        }
    }

    if !appended_blocks.is_empty() {
        if let Some(parent) = config_path.parent() {
            if let Err(err) = fs::create_dir_all(parent) {
                record.warnings.push(format!(
                    "Could not create Codex config dir {}: {err}",
                    parent.display()
                ));
                let _ = write_json(&mcp_provision_record_path(run_dir), &record);
                return record;
            }
        }
        let mut updated = read_text(&config_path).unwrap_or_default();
        if !updated.trim_end().is_empty() {
            updated.push_str("\n\n");
        }
        updated.push_str(&appended_blocks.join("\n\n"));
        if let Err(err) = write_text(&config_path, &updated) {
            record.warnings.push(err);
            record.configured.clear();
        }
    }

    let _ = write_json(&mcp_provision_record_path(run_dir), &record);
    record
}

fn solver_memory_namespace(run_dir: &Path, solver_id: &str) -> String {
    format!(
        "agpipe:{}:{}",
        run_dir
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("run"),
        solver_id
    )
}

fn render_mcp_reference(run_dir: &Path, plan: &Plan) -> String {
    if plan.mcp.servers.is_empty() {
        return "# MCP Plan\n\nNo MCP servers were selected for this run.\n".to_string();
    }
    let mut lines = vec![
        "# MCP Plan".to_string(),
        String::new(),
        "These MCP selections are recommendations for stage agents. Use them only if the current client actually exposes the named MCP server.".to_string(),
        String::new(),
        "## Auto Selection Rationale".to_string(),
    ];
    if plan.mcp.rationale.is_empty() {
        lines.push("- none recorded".to_string());
    } else {
        lines.extend(plan.mcp.rationale.iter().map(|item| format!("- {item}")));
    }
    lines.push(String::new());
    lines.push("## Selected Servers".to_string());
    for server in &plan.mcp.servers {
        lines.push(format!(
            "- `{}` [{}] stages: {}",
            server.name,
            server.mode,
            server.stages.join(", ")
        ));
        if !server.purposes.is_empty() {
            lines.push(format!("  purposes: {}", server.purposes.join(", ")));
        }
        if !server.usage_hint.trim().is_empty() {
            lines.push(format!("  usage: {}", server.usage_hint.trim()));
        }
    }
    if !plan.solver_roles.is_empty() {
        lines.push(String::new());
        lines.push("## Solver Routing".to_string());
        for solver in &plan.solver_roles {
            let assigned = if solver.mcp_servers.is_empty() {
                "none".to_string()
            } else {
                solver.mcp_servers.join(", ")
            };
            lines.push(format!("- `{}` -> {}", solver.solver_id, assigned));
            if solver.mcp_servers.iter().any(|name| name == "memory") {
                lines.push(format!(
                    "  memory namespace: `{}`",
                    solver_memory_namespace(run_dir, &solver.solver_id)
                ));
                lines.push(
                    "  isolation rule: never read or write sibling solver namespaces before review."
                        .to_string(),
                );
            }
        }
    }
    if let Some(provision) = read_mcp_provision_record(run_dir) {
        lines.push(String::new());
        lines.push("## Client Provisioning".to_string());
        lines.push(format!(
            "- auto provisioning enabled: `{}`",
            provision.enabled
        ));
        if !provision.config_path.trim().is_empty() {
            lines.push(format!("- codex config: `{}`", provision.config_path));
        }
        lines.push(format!(
            "- configured now: {}",
            if provision.configured.is_empty() {
                "none".to_string()
            } else {
                provision.configured.join(", ")
            }
        ));
        lines.push(format!(
            "- already present: {}",
            if provision.already_present.is_empty() {
                "none".to_string()
            } else {
                provision.already_present.join(", ")
            }
        ));
        if !provision.skipped.is_empty() {
            lines.push(format!("- skipped: {}", provision.skipped.join(", ")));
        }
        if !provision.warnings.is_empty() {
            lines.push("- warnings:".to_string());
            lines.extend(provision.warnings.iter().map(|item| format!("  - {item}")));
        }
    }
    lines.push(String::new());
    lines.push("## Safety Rules".to_string());
    lines.push(
        "- prefer official or primary-source material when using MCP research servers".to_string(),
    );
    lines.push("- treat missing MCP connectivity as a normal fallback to local context or regular browsing".to_string());
    lines.push("- do not let shared memory break solver independence".to_string());
    format!("{}\n", lines.join("\n"))
}

fn mcp_reference_path(run_dir: &Path) -> PathBuf {
    run_dir.join(MCP_PLAN_REF)
}

fn write_dynamic_reference_assets(run_dir: &Path, plan: &Plan) -> Result<(), String> {
    write_text(
        &mcp_reference_path(run_dir),
        &render_mcp_reference(run_dir, plan),
    )
}

fn solver_count_for(
    task_kind: &str,
    complexity: &str,
    execution_mode: &str,
    workstream_hints: &[WorkstreamHint],
    task: &str,
) -> usize {
    if task_kind == "review" {
        return if complexity == "high" { 3 } else { 2 };
    }
    let mut count = match complexity {
        "low" => 1,
        "medium" => 2,
        _ => 3,
    };
    if matches!(task_kind, "research" | "docs") {
        count = count.max(2);
    }
    if task_requests_cli_entrypoint(task_kind, task) && task_requests_precise_local_contract(task) {
        count = count.max(2);
    }
    if execution_mode == "decomposed" && workstream_hints.len() >= 2 {
        count = count.max(2);
    }
    if workstream_hints.len() >= 3 {
        count = count.max(3);
    }
    count.clamp(1, 3)
}

fn angle_sequence_for(task_kind: &str) -> &'static [&'static str] {
    if matches!(task_kind, "research" | "docs" | "review" | "audit-improve") {
        &RESEARCH_ANGLE_SEQUENCE
    } else {
        &ANGLE_SEQUENCE
    }
}

fn reviewer_stack_for(task_kind: &str) -> Vec<String> {
    let stack = match task_kind {
        "ai" => &AI_REVIEWER_STACK[..],
        "audit-improve" => &AUDIT_IMPROVE_REVIEWER_STACK[..],
        "research" => &RESEARCH_REVIEWER_STACK[..],
        "docs" => &DOCS_REVIEWER_STACK[..],
        _ => &REVIEWER_STACK[..],
    };
    stack.iter().map(|item| item.to_string()).collect()
}

fn reviewer_stack_for_task(task_kind: &str, complexity: &str, task: &str) -> Vec<String> {
    if task_kind == "backend"
        && complexity == "low"
        && task_requests_cli_entrypoint(task_kind, task)
        && task_requests_precise_local_contract(task)
    {
        return BACKEND_CLI_REVIEWER_STACK
            .iter()
            .map(|item| item.to_string())
            .collect();
    }
    reviewer_stack_for(task_kind)
}

fn default_reviewer_stack_for_plan(plan: &Plan) -> Vec<String> {
    reviewer_stack_for_task(&plan.task_kind, &plan.complexity, &plan.original_task)
}

fn task_requests_workspace_changes(task: &str) -> bool {
    if task_forbids_workspace_changes(task) {
        return false;
    }
    let text = task.to_lowercase();
    [
        "implement",
        "implementation",
        "edit file",
        "edit the repo",
        "change code",
        "write code",
        "patch",
        "fix",
        "refactor",
        "integrate",
        "migrate",
        "build service",
        "create service",
        "create cli",
        "создай сервис",
        "создай cli",
        "реализ",
        "внедр",
        "исправ",
        "патч",
        "рефактор",
        "измени код",
        "отредакт",
        "добавь",
        "мигрир",
    ]
    .iter()
    .any(|signal| text.contains(signal))
}

fn task_forbids_workspace_changes(task: &str) -> bool {
    let text = task.to_lowercase();
    [
        "review-only",
        "research-only",
        "analysis-only",
        "result should be an analytical document",
        "final result should be an analytical document",
        "output should be an analytical document",
        "do not turn the task into implementation",
        "do not turn this into implementation",
        "working artifacts are not required",
        "implementation artifacts are not required",
        "no implementation",
        "without implementation",
        "do not change code",
        "do not modify code",
        "do not edit the repo",
        "do not proceed to code changes",
        "not implementation work",
        "not for implementing fixes",
        "не менять код",
        "не меняй код",
        "не изменяй код",
        "не редактируй код",
        "не переходи к изменению кода",
        "не переходить к изменению кода",
        "не переходи к рефакторингу",
        "не переходить к рефакторингу",
        "не на реализацию",
        "не на реализацию исправлений",
        "без реализации",
        "без внедрения",
        "итоговый результат нужен в виде аналитического документа",
        "результат нужен в виде аналитического документа",
        "итоговый результат в виде аналитического документа",
        "не превращай задачу в реализацию",
        "не превращать задачу в реализацию",
        "не являются обязательной частью результата",
        "не внедряй",
        "не реализуй",
        "не переписывай",
        "не вмешивайся",
        "не модифицируй",
        "не правь код",
    ]
    .iter()
    .any(|signal| text.contains(signal))
}

fn task_is_analysis_only(task: &str) -> bool {
    if task_requests_workspace_changes(task) {
        return false;
    }
    if task_requests_code_review(task) {
        return true;
    }
    let text = task.to_lowercase();
    let signals = [
        "compare",
        "evaluate",
        "research",
        "recommend",
        "proposal",
        "summary",
        "report",
        "findings",
        "spec",
        "analyze",
        "analysis",
        "audit",
        "review",
        "investigate",
        "document",
        "plan",
        "design proposal",
        "сравн",
        "оцен",
        "исслед",
        "рекомен",
        "предлож",
        "отч",
        "вывод",
        "находк",
        "резюме",
        "спек",
        "анализ",
        "аудит",
        "обзор",
        "план",
    ];
    signals
        .iter()
        .filter(|signal| text.contains(**signal))
        .count()
        >= 2
}

fn task_is_verification_seed_follow_up(task: &str) -> bool {
    let text = task.to_lowercase();
    count_signals(
        &text,
        &[
            "verified follow-up task",
            "verification-derived context",
            "rerun recommended",
            "recommended next action",
            "missing critical checks",
            "verified progress that must not regress",
            "do-not-regress constraints",
            "clear done state for the next run",
            "follow-up task",
            "повторный прогон",
            "следующий прогон",
            "критические чеки",
            "верификац",
        ],
    ) >= 2
}

fn task_requests_audit_improve_follow_up(task: &str) -> bool {
    if !task_is_analysis_only(task) {
        return false;
    }
    let text = task.to_lowercase();
    let audit_hits = count_signals(
        &text,
        &[
            "audit",
            "analysis",
            "analyze",
            "investigate",
            "verify",
            "verification",
            "аудит",
            "анализ",
            "исслед",
            "верификац",
            "провер",
        ],
    );
    let improve_hits = count_signals(
        &text,
        &[
            "improve",
            "improvement",
            "follow-up",
            "rerun",
            "close",
            "reconcile",
            "refresh",
            "preserve",
            "stale artifact",
            "улуч",
            "доработ",
            "повторн",
            "перезапуск",
            "закрой",
            "сверь",
            "обнови",
            "не откатывай",
            "не reopening",
        ],
    );
    audit_hits >= 2 && improve_hits >= 2
}

fn default_pipeline_includes_execution(task_kind: &str, task: &str) -> bool {
    if task_kind == "review" {
        return false;
    }
    if task_is_analysis_only(task) {
        return false;
    }
    if !matches!(task_kind, "research" | "docs") {
        return true;
    }
    task_requests_workspace_changes(task)
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
    if matches!(task_kind, "review" | "audit-improve") {
        return "alternatives".to_string();
    }
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
        "review" => vec![
            WorkstreamHint {
                name: "correctness-and-bugs".to_string(),
                goal:
                    "find concrete bugs, incorrect behavior, and code paths that are likely broken"
                        .to_string(),
                suggested_role: "engineering/engineering-code-reviewer.md".to_string(),
            },
            WorkstreamHint {
                name: "regression-and-risk".to_string(),
                goal:
                    "surface regression risks, unsafe assumptions, and user-visible failure modes"
                        .to_string(),
                suggested_role: "testing/testing-reality-checker.md".to_string(),
            },
            WorkstreamHint {
                name: "tests-and-evidence".to_string(),
                goal: "identify missing tests, weak validation, and evidence gaps".to_string(),
                suggested_role: "testing/testing-test-results-analyzer.md".to_string(),
            },
        ],
        "audit-improve" => vec![
            WorkstreamHint {
                name: "device-probe-reconciliation".to_string(),
                goal: "reconcile launcher host probe facts with interpreter-qualified runtime evidence".to_string(),
                suggested_role: "engineering/engineering-backend-architect.md".to_string(),
            },
            WorkstreamHint {
                name: "artifact-refresh-and-narrative".to_string(),
                goal: "refresh stale run-facing narrative so downstream reruns inherit the current validated state".to_string(),
                suggested_role: "engineering/engineering-technical-writer.md".to_string(),
            },
            WorkstreamHint {
                name: "regression-guard-validation".to_string(),
                goal: "rerun only the bounded green slice and preserve already-verified fixes".to_string(),
                suggested_role: "testing/testing-tool-evaluator.md".to_string(),
            },
        ],
        "docs" => vec![
            WorkstreamHint {
                name: "artifact-authoring".to_string(),
                goal: "produce the requested guide, summary, or spec in a user-facing form".to_string(),
                suggested_role: "engineering/engineering-technical-writer.md".to_string(),
            },
            WorkstreamHint {
                name: "source-grounding".to_string(),
                goal: "tie claims to primary references and keep an explicit evidence ledger".to_string(),
                suggested_role: "testing/testing-evidence-collector.md".to_string(),
            },
            WorkstreamHint {
                name: "decision-summary".to_string(),
                goal: "compress the material findings into an executive decision-oriented takeaway".to_string(),
                suggested_role: "support/support-executive-summary-generator.md".to_string(),
            },
        ],
        "research" => vec![
            WorkstreamHint {
                name: "landscape-scan".to_string(),
                goal: "map the relevant option space and identify the strongest candidates".to_string(),
                suggested_role: "product/product-trend-researcher.md".to_string(),
            },
            WorkstreamHint {
                name: "evidence-ledger".to_string(),
                goal: "capture the factual source trail and separate confirmed facts from inference".to_string(),
                suggested_role: "testing/testing-evidence-collector.md".to_string(),
            },
            WorkstreamHint {
                name: "comparison-and-risk".to_string(),
                goal: "compare tradeoffs, operational fit, and failure modes of the candidate options".to_string(),
                suggested_role: "testing/testing-tool-evaluator.md".to_string(),
            },
        ],
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
        "backend" => {
            if task_requests_cli_entrypoint(task_kind, task) {
                vec![
                    WorkstreamHint {
                        name: "local-entrypoint".to_string(),
                        goal: "build the requested local CLI or script entrypoint".to_string(),
                        suggested_role: "engineering/engineering-rapid-prototyper.md".to_string(),
                    },
                    WorkstreamHint {
                        name: "run-contract".to_string(),
                        goal: "lock the exact run command, stdout contract, and documentation"
                            .to_string(),
                        suggested_role: "engineering/engineering-technical-writer.md".to_string(),
                    },
                ]
            } else {
                vec![
                    WorkstreamHint {
                        name: "service-layer".to_string(),
                        goal: "build the core service or API behavior".to_string(),
                        suggested_role: "engineering/engineering-senior-developer.md".to_string(),
                    },
                    WorkstreamHint {
                        name: "data-and-interfaces".to_string(),
                        goal: "define storage, integration surfaces, and operational boundaries"
                            .to_string(),
                        suggested_role: "engineering/engineering-backend-architect.md".to_string(),
                    },
                ]
            }
        }
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

fn solver_focus_workstreams(plan: &Plan, solver: &SolverRole) -> Vec<WorkstreamHint> {
    if plan.workstream_hints.is_empty() {
        return Vec::new();
    }
    let role_matches: Vec<WorkstreamHint> = plan
        .workstream_hints
        .iter()
        .filter(|hint| !hint.suggested_role.is_empty() && hint.suggested_role == solver.role)
        .cloned()
        .collect();
    if !role_matches.is_empty() {
        return role_matches;
    }
    let solver_index = plan
        .solver_roles
        .iter()
        .position(|item| item.solver_id == solver.solver_id)
        .unwrap_or(0);
    vec![plan.workstream_hints[solver_index % plan.workstream_hints.len()].clone()]
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

fn task_specific_validation_commands(task_kind: &str, task: &str) -> Vec<String> {
    let text = task.to_lowercase();
    let mut commands = Vec::new();
    if task_requests_cli_entrypoint(task_kind, task) && text_contains_signal(&text, "main.py") {
        commands.push("python3 main.py".to_string());
    }
    commands
}

fn validation_command_is_lightweight(command: &str) -> bool {
    let text = command.trim().to_lowercase();
    if text.is_empty() {
        return false;
    }
    if text.contains("--help") {
        return true;
    }
    matches!(
        text.as_str(),
        "pytest"
            | "cargo test"
            | "go test ./..."
            | "make test"
            | "terraform validate"
            | "python3 main.py"
    ) || text.starts_with("pytest ")
        || text.starts_with("cargo test ")
        || text.starts_with("go test ")
        || text.starts_with("make test ")
}

fn validation_command_looks_heavy(command: &str) -> bool {
    let text = command.trim().to_lowercase();
    if text.is_empty() || validation_command_is_lightweight(command) {
        return false;
    }
    [
        "--photo-path",
        "--runtime-dir",
        "--result-json-path",
        "--output-dir",
        "analysis-max-new-tokens",
        "analysis-device",
        "python -m llm_freecad.main",
        "python -m llm_freecad.bot.runtime",
        "run_service.py",
        "freecad",
        "qwen",
        "telegram",
    ]
    .iter()
    .any(|signal| text.contains(signal))
        || text.starts_with("python ")
        || text.starts_with("python3 ")
        || text.starts_with("uv run ")
        || (text.contains(" python ") && !text.contains(" --help"))
        || (text.contains(" python3 ") && !text.contains(" --help"))
        || (text.contains(" uv run ") && !text.contains(" --help"))
}

fn non_execution_validation_hints_for_stage(plan: &Plan, run_dir: &Path) -> Vec<String> {
    let analysis_mode = prompt_prefers_lightweight_validation_for_stage(plan, run_dir);
    let mut hints = if analysis_mode {
        plan.validation_commands
            .iter()
            .filter(|command| !validation_command_looks_heavy(command))
            .cloned()
            .collect::<Vec<_>>()
    } else {
        plan.validation_commands.clone()
    };
    let mut deduped = Vec::new();
    for hint in hints.drain(..) {
        if !deduped.contains(&hint) {
            deduped.push(hint);
        }
    }
    deduped
}

fn prompt_prefers_lightweight_validation(plan: &Plan) -> bool {
    task_forbids_workspace_changes(&plan.original_task)
        || task_is_analysis_only(&plan.original_task)
        || matches!(plan.task_kind.as_str(), "review" | "research" | "docs")
}

fn prompt_prefers_lightweight_validation_for_stage(plan: &Plan, run_dir: &Path) -> bool {
    prompt_prefers_lightweight_validation(plan)
        || first_stage_id_for_kind(plan, run_dir, PipelineStageKind::Execution)
            .ok()
            .flatten()
            .is_none()
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

#[cfg(test)]
fn choose_roles(task_kind: &str, task: &str, solver_count: usize) -> Vec<SolverRole> {
    choose_roles_with_workstreams(task_kind, task, solver_count, &[])
}

fn choose_roles_with_workstreams(
    task_kind: &str,
    task: &str,
    solver_count: usize,
    workstream_hints: &[WorkstreamHint],
) -> Vec<SolverRole> {
    let angles = angle_sequence_for(task_kind);
    let mut ordered_roles = Vec::new();
    let mut seen = BTreeSet::new();
    for hint in workstream_hints {
        if hint.suggested_role.trim().is_empty() {
            continue;
        }
        if seen.insert(hint.suggested_role.clone()) {
            ordered_roles.push(hint.suggested_role.clone());
        }
    }
    for role in role_matrix(task_kind, task) {
        let role = role.to_string();
        if seen.insert(role.clone()) {
            ordered_roles.push(role);
        }
    }
    ordered_roles
        .into_iter()
        .take(solver_count)
        .enumerate()
        .map(|(index, role)| SolverRole {
            solver_id: format!("solver-{}", (b'a' + index as u8) as char),
            role,
            angle: angles
                .get(index)
                .copied()
                .unwrap_or("implementation-first")
                .to_string(),
            mcp_servers: Vec::new(),
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
            if let Some(spec) = discover_service_check_spec_path(run_dir) {
                paths.push(spec);
            }
            let summary = service_check_summary_json_path(run_dir, "execution");
            if summary.exists() {
                paths.push(summary);
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
            if let Some(spec) = discover_service_check_spec_path(run_dir) {
                paths.push(spec);
            }
            for phase in ["execution", "verification"] {
                let summary_json = service_check_summary_json_path(run_dir, phase);
                if summary_json.exists() {
                    paths.push(summary_json);
                }
                let summary_md = service_check_summary_md_path(run_dir, phase);
                if summary_md.exists() {
                    paths.push(summary_md);
                }
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
        StageBackendKind::LocalTemplate(LocalTemplateKind::ExecutionReadyBackendCliIntake) => {
            "backend:local-template:execution-ready-backend-cli-intake"
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
fn record_token_usage_with_replacement(
    run_dir: &Path,
    plan: &Plan,
    stage: &str,
    mode: &str,
    cache_key: &str,
    prompt_hashes: &PromptCacheHashes,
    workspace_hash: &str,
    usage: &TokenUsage,
    replace_modes: &[&str],
) -> Result<(), String> {
    let mut ledger = load_token_ledger(run_dir, plan)?;
    ledger.entries.retain(|entry| {
        !(entry.stage == stage
            && entry.cache_key == cache_key
            && replace_modes.contains(&entry.mode.as_str()))
    });
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
    record_token_usage_with_replacement(
        run_dir,
        plan,
        stage,
        mode,
        cache_key,
        prompt_hashes,
        workspace_hash,
        usage,
        &[],
    )
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

fn provisional_token_usage_for_prompt(prompt: &str) -> TokenUsage {
    let prompt_tokens = estimate_token_count(&normalize_prompt_for_cache(prompt));
    TokenUsage {
        source: "estimated-prompt-start".to_string(),
        prompt_tokens,
        cached_prompt_tokens: 0,
        completion_tokens: 0,
        total_tokens: prompt_tokens,
        estimated_saved_tokens: 0,
    }
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
    let path = plan_path(run_dir);
    let text = read_plan_artifact_text(run_dir)?;
    match serde_json::from_str(&text) {
        Ok(mut plan) => {
            ensure_plan_mcp_defaults(&mut plan);
            Ok(plan)
        }
        Err(_) => {
            let mut value: Value = serde_json::from_str(&text).map_err(|err| {
                format!(
                    "Could not parse plan artifact for {} (expected {} or {}): {err}",
                    run_dir.display(),
                    path.display(),
                    plan_snapshot_path(run_dir).display()
                )
            })?;
            normalize_legacy_plan_json(&mut value);
            let mut plan: Plan = serde_json::from_value(value).map_err(|err| {
                format!(
                    "Could not parse plan artifact for {} (expected {} or {}): {err}",
                    run_dir.display(),
                    path.display(),
                    plan_snapshot_path(run_dir).display()
                )
            })?;
            ensure_plan_mcp_defaults(&mut plan);
            Ok(plan)
        }
    }
}

fn save_plan(run_dir: &Path, plan: &Plan) -> Result<(), String> {
    let mut persisted = plan.clone();
    ensure_plan_mcp_defaults(&mut persisted);
    provision_selected_mcp_servers(run_dir, &persisted, None);
    write_dynamic_reference_assets(run_dir, &persisted)?;
    write_json(&plan_path(run_dir), &persisted)?;
    write_json(&plan_snapshot_path(run_dir), &persisted)
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
    let angles = angle_sequence_for(&_plan.task_kind);
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
            stage.angle = angles[solver_index % angles.len()].to_string();
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

fn workstream_focus_summary(hints: &[WorkstreamHint]) -> String {
    let mut labels: Vec<String> = hints
        .iter()
        .map(|hint| hint.name.trim())
        .filter(|name| !name.is_empty())
        .map(|name| format!("`{name}`"))
        .collect();
    labels.sort();
    labels.dedup();
    if labels.is_empty() {
        "the most critical goal checks".to_string()
    } else {
        labels.join(", ")
    }
}

fn default_stage_description(plan: &Plan, stage: &PipelineStageSpec) -> String {
    match pipeline_kind_from_str(&stage.kind) {
        Some(PipelineStageKind::Intake) => format!(
            "Refine the brief, goal checks, and downstream stage setup without implementing the solution. Keep the plan centered on {}.",
            workstream_focus_summary(&plan.workstream_hints)
        ),
        Some(PipelineStageKind::Solver) => {
            let solver = SolverRole {
                solver_id: stage.id.clone(),
                role: stage.role.clone(),
                angle: stage.angle.clone(),
                mcp_servers: Vec::new(),
            };
            let focus = solver_focus_workstreams(plan, &solver);
            if plan.task_kind == "review" {
                return format!(
                    "Produce an independent `{}` code review for this task, with special attention to {}.",
                    if stage.angle.trim().is_empty() {
                        "evidence-first"
                    } else {
                        stage.angle.trim()
                    },
                    workstream_focus_summary(&focus)
                );
            }
            format!(
                "Develop an independent `{}` solution for this `{}` task, with special attention to {}.",
                if stage.angle.trim().is_empty() {
                    "implementation-first"
                } else {
                    stage.angle.trim()
                },
                plan.task_kind,
                workstream_focus_summary(&focus)
            )
        }
        Some(PipelineStageKind::Review) => {
            "Compare solver outputs against the brief and goal checks, then choose a winner or explicit hybrid with concrete validation evidence.".to_string()
        }
        Some(PipelineStageKind::Execution) => {
            "Implement the reviewed winner in the primary workspace, run the cheapest relevant validation, and record concrete changes.".to_string()
        }
        Some(PipelineStageKind::Verification) => {
            "Audit the actual workspace and execution evidence, update goal status, and surface ordered findings plus any rerun recommendation.".to_string()
        }
        None => String::new(),
    }
}

fn default_pipeline_stage_specs(plan: &Plan, run_dir: Option<&Path>) -> Vec<PipelineStageSpec> {
    let solver_ids = default_solver_stage_ids(plan, run_dir);
    let solver_role_map: BTreeMap<String, SolverRole> = plan
        .solver_roles
        .iter()
        .map(|item| (item.solver_id.clone(), item.clone()))
        .collect();
    let angles = angle_sequence_for(&plan.task_kind);
    let mut stages = vec![PipelineStageSpec {
        id: "intake".to_string(),
        kind: "intake".to_string(),
        ..PipelineStageSpec::default()
    }];
    if let Some(stage) = stages.last_mut() {
        stage.description = default_stage_description(plan, stage);
    }
    for (index, solver_id) in solver_ids.into_iter().enumerate() {
        let role = solver_role_map.get(&solver_id).cloned().unwrap_or_else(|| {
            choose_roles_with_workstreams(
                &plan.task_kind,
                &plan.original_task,
                std::cmp::max(1, plan.solver_count),
                &plan.workstream_hints,
            )
            .get(index)
            .cloned()
            .unwrap_or(SolverRole {
                solver_id: solver_id.clone(),
                role: "engineering/engineering-senior-developer.md".to_string(),
                angle: angles[index % angles.len()].to_string(),
                mcp_servers: Vec::new(),
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
        if let Some(stage) = stages.last_mut() {
            stage.description = default_stage_description(plan, stage);
        }
    }
    stages.push(PipelineStageSpec {
        id: "review".to_string(),
        kind: "review".to_string(),
        ..PipelineStageSpec::default()
    });
    if let Some(stage) = stages.last_mut() {
        stage.description = default_stage_description(plan, stage);
    }
    if default_pipeline_includes_execution(&plan.task_kind, &plan.original_task) {
        stages.push(PipelineStageSpec {
            id: "execution".to_string(),
            kind: "execution".to_string(),
            ..PipelineStageSpec::default()
        });
        if let Some(stage) = stages.last_mut() {
            stage.description = default_stage_description(plan, stage);
        }
    }
    stages.push(PipelineStageSpec {
        id: "verification".to_string(),
        kind: "verification".to_string(),
        ..PipelineStageSpec::default()
    });
    if let Some(stage) = stages.last_mut() {
        stage.description = default_stage_description(plan, stage);
    }
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

fn solver_stages_use_auto_roles(stages: &[PipelineStageSpec]) -> bool {
    stages.iter().all(|stage| {
        if pipeline_kind_from_str(&stage.kind) != Some(PipelineStageKind::Solver) {
            return true;
        }
        stage.role_source.trim().is_empty() || stage.role_source.trim().eq_ignore_ascii_case("auto")
    })
}

fn refresh_workstream_hints_if_auto(plan: &mut Plan, run_dir: Option<&Path>) -> Result<(), String> {
    let derived = workstream_hints_for(&plan.task_kind, &plan.original_task);
    if derived.is_empty() {
        return Ok(());
    }
    if plan.workstream_hints.is_empty() {
        plan.workstream_hints = derived;
        return Ok(());
    }
    let stages = pipeline_stage_specs(plan, run_dir)?;
    if !solver_stages_use_auto_roles(&stages) {
        return Ok(());
    }
    let current_roles: BTreeSet<String> = plan
        .workstream_hints
        .iter()
        .filter(|hint| !hint.suggested_role.trim().is_empty())
        .map(|hint| hint.suggested_role.clone())
        .collect();
    let derived_roles: BTreeSet<String> = derived
        .iter()
        .filter(|hint| !hint.suggested_role.trim().is_empty())
        .map(|hint| hint.suggested_role.clone())
        .collect();
    if current_roles != derived_roles {
        plan.workstream_hints = derived;
    }
    Ok(())
}

fn apply_pipeline_solver_defaults(plan: &mut Plan, run_dir: Option<&Path>) -> Result<(), String> {
    refresh_workstream_hints_if_auto(plan, run_dir)?;
    let mut stages = pipeline_stage_specs(plan, run_dir)?;
    let solver_total = stages
        .iter()
        .filter(|stage| pipeline_kind_from_str(&stage.kind) == Some(PipelineStageKind::Solver))
        .count();
    let default_roles = choose_roles_with_workstreams(
        &plan.task_kind,
        &plan.original_task,
        std::cmp::max(1, solver_total),
        &plan.workstream_hints,
    );
    let angles = angle_sequence_for(&plan.task_kind);
    let mut solver_index = 0usize;
    let mut solver_roles = Vec::new();
    for stage in &mut stages {
        if pipeline_kind_from_str(&stage.kind) != Some(PipelineStageKind::Solver) {
            continue;
        }
        if stage.role_source.trim().eq_ignore_ascii_case("auto") || stage.role.trim().is_empty() {
            let fallback = default_roles
                .get(solver_index)
                .cloned()
                .unwrap_or(SolverRole {
                    solver_id: stage.id.clone(),
                    role: "engineering/engineering-senior-developer.md".to_string(),
                    angle: angles[solver_index % angles.len()].to_string(),
                    mcp_servers: Vec::new(),
                });
            stage.role = fallback.role;
        }
        if stage.angle.trim().is_empty() {
            stage.angle = angles[solver_index % angles.len()].to_string();
        }
        solver_roles.push(SolverRole {
            solver_id: stage.id.clone(),
            role: stage.role.clone(),
            angle: stage.angle.clone(),
            mcp_servers: Vec::new(),
        });
        solver_index += 1;
    }
    plan.pipeline.stages = stages;
    plan.solver_count = solver_roles.len();
    plan.solver_roles = solver_roles;
    ensure_plan_mcp_defaults(plan);
    let plan_snapshot = plan.clone();
    for stage in &mut plan.pipeline.stages {
        if stage.description.trim().is_empty() {
            stage.description = default_stage_description(&plan_snapshot, stage);
        }
    }
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

fn stage_memory_namespace(run_dir: &Path, stage: &str) -> String {
    format!(
        "agpipe:{}:{}",
        run_dir
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("run"),
        stage
    )
}

fn localized_copy<'a>(language: &str, english: &'a str, russian: &'a str) -> &'a str {
    if language_is_russian(language) {
        russian
    } else {
        english
    }
}

fn localized_mcp_usage_heading(language: &str) -> &'static str {
    localized_copy(language, "MCP Usage", "Использование MCP")
}

fn localized_mcp_usage_heading_candidates() -> &'static [&'static str] {
    &["mcp usage", "использование mcp"]
}

fn localized_user_facing_style_rules(language: &str) -> Vec<String> {
    if language_is_russian(language) {
        vec![
            "write every user-facing markdown artifact in natural Russian".to_string(),
            "avoid unnecessary English jargon, Americanisms, and transliterated calques when a plain Russian equivalent exists".to_string(),
            "translate internal pipeline labels and section headings into idiomatic Russian unless the term is a product name, code identifier, command, protocol, or exact API term".to_string(),
            "do not mix English headings with Russian prose; keep the document stylistically consistent".to_string(),
            "prefer plain Russian terms such as `основа`, `доказательная база`, `короткий список`, `итоговая оценка`, and `повторный прогон` over hybrid wording like `baseline`, `evidence trail`, `shortlist`, `fit-score`, or `rerun` in user-facing prose".to_string(),
        ]
    } else {
        Vec::new()
    }
}

fn localized_user_facing_style_note(language: &str) -> &'static str {
    if language_is_russian(language) {
        "For Russian user-facing output, keep the prose idiomatic and readable: translate internal pipeline labels, avoid unnecessary English jargon and Americanisms, and keep English only for product names, commands, and exact technical terms."
    } else {
        "Keep user-facing prose idiomatic in the requested language and avoid unnecessary internal pipeline jargon."
    }
}

fn selected_mcp_servers_for_stage<'a>(
    plan: &'a Plan,
    stage: &str,
    solver: Option<&SolverRole>,
) -> Vec<&'a McpServerPlan> {
    plan.mcp
        .servers
        .iter()
        .filter(|server| {
            if stage == "solver" {
                if !server.stages.iter().any(|item| item == "solver") {
                    return false;
                }
                if let Some(solver) = solver {
                    let assigned = effective_solver_mcp_servers(plan, solver);
                    return assigned.iter().any(|item| item == &server.name);
                }
            }
            server.stages.iter().any(|item| item == stage)
        })
        .collect()
}

fn render_mcp_usage_hints(
    run_dir: &Path,
    stage: &str,
    solver: Option<&SolverRole>,
    plan: &Plan,
) -> Vec<String> {
    selected_mcp_servers_for_stage(plan, stage, solver)
        .into_iter()
        .map(|server| match server.name.as_str() {
            "context7" => {
                "use `context7` for official library/framework/protocol docs before generic browsing"
                    .to_string()
            }
            "exa" => {
                "use `exa` for research and source discovery; favor primary sources over summaries"
                    .to_string()
            }
            "fetch" => {
                "use official `fetch` for direct page retrieval after a concrete URL has been identified; if the call requires confirmation or is unavailable, continue locally and record that fallback"
                    .to_string()
            }
            "git" => {
                "use official `git` for read-only repository inspection such as status, diff, log, show, and branch listing"
                    .to_string()
            }
            "memory" => {
                if let Some(solver) = solver {
                    format!(
                        "use `memory` only within namespace `{}` and never read or write sibling solver namespaces before review; if memory calls are unavailable, keep the handoff local in the stage artifact",
                        solver_memory_namespace(run_dir, &solver.solver_id)
                    )
                } else {
                    format!(
                        "use `memory` for durable handoffs within namespace `{}`; if memory calls are unavailable, keep the handoff local in the stage artifact",
                        stage_memory_namespace(run_dir, stage)
                    )
                }
            }
            other => format!("use `{other}` only when it improves this stage over local context"),
        })
        .collect()
}

fn render_mcp_accountability_rules(
    selected_servers: &[String],
    summary_language: &str,
) -> Vec<String> {
    let heading = localized_mcp_usage_heading(summary_language);
    let mut rules = vec![
        format!(
            "include a `## {heading}` section in the main markdown artifact for this stage"
        ),
    ];
    if selected_servers.is_empty() {
        rules.push(
            format!(
                "in `## {heading}`, state that no MCP servers were selected for this stage"
            ),
        );
        rules.push(
            "do not claim fresh MCP-backed lookups, live external source re-checks, or direct page retrieval happened in this stage when no MCP servers were selected; if you mention URLs, label them as inherited references or unverified context".to_string(),
        );
    } else {
        rules.push(
            format!(
                "in `## {heading}`, list each selected MCP server and mark it as `used`, `not used`, or `unavailable` with a short reason"
            ),
        );
        rules.push(
            "when MCP materially informed the answer, mention the concrete lookup, page, or context it provided"
                .to_string(),
        );
        rules.push(
            "if a selected MCP call is cancelled, requires manual confirmation, or is unavailable, continue with local evidence and record the fallback instead of blocking the stage"
                .to_string(),
        );
    }
    rules
}

fn stage_mcp_usage_note_path(run_dir: &Path, stage: &str) -> PathBuf {
    run_dir
        .join("runtime")
        .join("mcp")
        .join(format!("{stage}.md"))
}

fn stage_primary_mcp_artifact_path(
    plan: &Plan,
    run_dir: &Path,
    stage: &str,
) -> Result<Option<PathBuf>, String> {
    Ok(Some(match pipeline_stage_kind_for(plan, run_dir, stage)? {
        PipelineStageKind::Intake => run_dir.join("brief.md"),
        PipelineStageKind::Solver => run_dir.join("solutions").join(stage).join("RESULT.md"),
        PipelineStageKind::Review => run_dir.join("review").join("report.md"),
        PipelineStageKind::Execution => run_dir.join("execution").join("report.md"),
        PipelineStageKind::Verification => run_dir.join("verification").join("findings.md"),
    }))
}

fn extract_markdown_section(markdown: &str, heading: &str) -> Option<String> {
    let target = heading.trim().to_lowercase();
    let mut capture = false;
    let mut section = Vec::new();
    for line in markdown.lines() {
        let trimmed = line.trim_start();
        if trimmed.starts_with('#') {
            let normalized = trimmed.trim_start_matches('#').trim().to_lowercase();
            if capture {
                break;
            }
            if normalized == target {
                capture = true;
                continue;
            }
        }
        if capture {
            section.push(line);
        }
    }
    let body = section.join("\n").trim().to_string();
    if body.is_empty() {
        None
    } else {
        Some(body)
    }
}

fn extract_markdown_section_any(markdown: &str, headings: &[&str]) -> Option<String> {
    headings
        .iter()
        .find_map(|heading| extract_markdown_section(markdown, heading))
}

fn mcp_server_name_from_note_line(line: &str) -> Option<String> {
    let trimmed = line.trim_start();
    for server in ["context7", "exa", "fetch", "git", "memory"] {
        if trimmed.starts_with(&format!("- `{server}`"))
            || trimmed.starts_with(&format!("- {server}"))
            || trimmed.starts_with(&format!("| `{server}` |"))
            || trimmed.starts_with(&format!("| {server} |"))
        {
            return Some(server.to_string());
        }
    }
    None
}

fn normalize_mcp_usage_note(note: &str, selected: &[String]) -> String {
    let trimmed = note.trim();
    if trimmed.is_empty() || selected.is_empty() {
        return trimmed.to_string();
    }

    let selected: BTreeSet<String> = selected
        .iter()
        .map(|item| item.to_ascii_lowercase())
        .collect();
    let mut preamble = Vec::new();
    let mut blocks: Vec<(Option<String>, Vec<String>)> = Vec::new();
    let mut current_name: Option<String> = None;
    let mut current_lines: Vec<String> = Vec::new();
    let mut saw_named_block = false;

    let flush_block = |blocks: &mut Vec<(Option<String>, Vec<String>)>,
                       current_name: &mut Option<String>,
                       current_lines: &mut Vec<String>| {
        if !current_lines.is_empty() {
            blocks.push((current_name.take(), std::mem::take(current_lines)));
        }
    };

    for raw_line in trimmed.lines() {
        if let Some(server) = mcp_server_name_from_note_line(raw_line) {
            saw_named_block = true;
            flush_block(&mut blocks, &mut current_name, &mut current_lines);
            current_name = Some(server);
            current_lines.push(raw_line.to_string());
        } else if current_name.is_some() {
            current_lines.push(raw_line.to_string());
        } else {
            preamble.push(raw_line.to_string());
        }
    }
    flush_block(&mut blocks, &mut current_name, &mut current_lines);

    if !saw_named_block {
        return trimmed.to_string();
    }

    let mut kept = Vec::new();
    for (name, lines) in blocks {
        match name {
            Some(name) if selected.contains(&name) => kept.extend(lines),
            Some(_) => {}
            None => kept.extend(lines),
        }
    }

    if kept.is_empty() {
        return trimmed.to_string();
    }

    let mut normalized = Vec::new();
    normalized.extend(preamble);
    if !normalized.is_empty() && !kept.is_empty() {
        normalized.push(String::new());
    }
    normalized.extend(kept);
    normalized.join("\n").trim().to_string()
}

fn configured_mcp_servers(run_dir: &Path) -> Vec<String> {
    let mut servers = BTreeSet::new();
    if let Ok(record) = read_json::<McpProvisionRecord>(&run_dir.join(MCP_PROVISION_RECORD_REF)) {
        for value in record
            .configured
            .into_iter()
            .chain(record.already_present.into_iter())
        {
            if !value.trim().is_empty() {
                servers.insert(value);
            }
        }
    }
    servers.into_iter().collect()
}

fn solver_role_for_stage<'a>(plan: &'a Plan, stage: &str) -> Option<&'a SolverRole> {
    plan.solver_roles
        .iter()
        .find(|item| item.solver_id == stage)
}

fn selected_mcp_server_names_for_stage_kind(
    plan: &Plan,
    stage_key: &str,
    solver: Option<&SolverRole>,
) -> Vec<String> {
    selected_mcp_servers_for_stage(plan, stage_key, solver)
        .into_iter()
        .map(|server| server.name.clone())
        .collect()
}

fn selected_mcp_server_names_for_stage(plan: &Plan, run_dir: &Path, stage: &str) -> Vec<String> {
    if matches!(
        stage,
        "intake" | "solver" | "review" | "execution" | "verification"
    ) {
        return selected_mcp_server_names_for_stage_kind(
            plan,
            stage,
            solver_role_for_stage(plan, stage),
        );
    }
    let kind = match pipeline_stage_kind_for(plan, run_dir, stage) {
        Ok(kind) => kind,
        Err(_) => return Vec::new(),
    };
    let stage_key = match kind {
        PipelineStageKind::Intake => "intake",
        PipelineStageKind::Solver => "solver",
        PipelineStageKind::Review => "review",
        PipelineStageKind::Execution => "execution",
        PipelineStageKind::Verification => "verification",
    };
    selected_mcp_server_names_for_stage_kind(plan, stage_key, solver_role_for_stage(plan, stage))
}

fn prompt_artifact_shows_empty_mcp_assignment(prompt_text: &str) -> bool {
    prompt_text.contains("\"mcp_servers\": []")
        || prompt_text.contains("\"mcp_servers\":[]")
        || prompt_text.contains("Selected MCP servers:\n\n- none")
        || prompt_text.contains("Selected MCP servers:\r\n\r\n- none")
}

fn prompt_artifact_selected_mcp_names(prompt_text: &str) -> Vec<String> {
    let mut names = Vec::new();
    for server in ["context7", "exa", "fetch", "git", "memory"] {
        if prompt_text.contains(&format!("\"{server}\""))
            && (prompt_text.contains("\"mcp_servers\"")
                || prompt_text.contains("Selected MCP servers:"))
        {
            names.push(server.to_string());
        }
    }
    dedupe_strings(names)
}

fn mcp_note_server_names(note: &str) -> Vec<String> {
    let mut names = Vec::new();
    for line in note.lines() {
        if let Some(server) = mcp_server_name_from_note_line(line) {
            names.push(server);
        }
    }
    dedupe_strings(names)
}

fn analyze_mcp_usage_note(
    note: &str,
    selected: &[String],
) -> (Vec<String>, Vec<String>, Vec<String>) {
    let lower = note.to_ascii_lowercase();
    let negative_markers = [
        "not used",
        "did not use",
        "unused",
        "unavailable",
        "not available",
        "skipped",
        "missing",
        "не использ",
        "недоступ",
        "пропущ",
    ];
    let mut declared_used = Vec::new();
    let mut declared_not_used = Vec::new();
    let mut observed_mentions = Vec::new();
    for server in selected {
        let server_lower = server.to_ascii_lowercase();
        if lower.contains(&server_lower) {
            observed_mentions.push(server.clone());
            let mut negative = false;
            for raw_line in note.lines() {
                let line = raw_line.to_ascii_lowercase();
                if line.contains(&server_lower)
                    && negative_markers.iter().any(|item| line.contains(item))
                {
                    negative = true;
                    break;
                }
            }
            if negative {
                declared_not_used.push(server.clone());
            } else {
                declared_used.push(server.clone());
            }
        }
    }
    (declared_used, declared_not_used, observed_mentions)
}

fn read_mcp_usage_records(run_dir: &Path) -> Vec<McpUsageRecord> {
    let path = run_dir.join(MCP_USAGE_LOG_REF);
    let Ok(text) = read_text(&path) else {
        return Vec::new();
    };
    text.lines()
        .filter_map(|line| {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                None
            } else {
                serde_json::from_str::<McpUsageRecord>(trimmed).ok()
            }
        })
        .collect()
}

fn persist_stage_mcp_note(run_dir: &Path, stage: &str, note: &str) -> Result<(), String> {
    if note.trim().is_empty() {
        return Ok(());
    }
    write_text(&stage_mcp_usage_note_path(run_dir, stage), note.trim_end())
}

fn record_stage_mcp_usage(
    plan: &Plan,
    run_dir: &Path,
    stage: &str,
    backend: &str,
    source: &str,
) -> Result<(), String> {
    let kind = pipeline_stage_kind_for(plan, run_dir, stage)?;
    let stage_kind = match kind {
        PipelineStageKind::Intake => "intake",
        PipelineStageKind::Solver => "solver",
        PipelineStageKind::Review => "review",
        PipelineStageKind::Execution => "execution",
        PipelineStageKind::Verification => "verification",
    };
    let selected = selected_mcp_server_names_for_stage(plan, run_dir, stage);
    let configured = configured_mcp_servers(run_dir);
    let available = if selected.is_empty() {
        configured
    } else {
        selected
            .iter()
            .filter(|item| configured.iter().any(|configured| configured == *item))
            .cloned()
            .collect()
    };
    let artifact_path = stage_primary_mcp_artifact_path(plan, run_dir, stage)?;
    let note_path = stage_mcp_usage_note_path(run_dir, stage);
    let mut note_md = if note_path.exists() {
        read_text(&note_path)?
    } else {
        String::new()
    };
    if note_md.trim().is_empty() {
        if let Some(path) = artifact_path.as_ref() {
            if path.exists() {
                if let Some(section) = extract_markdown_section_any(
                    &read_text(path)?,
                    localized_mcp_usage_heading_candidates(),
                ) {
                    note_md = section;
                    persist_stage_mcp_note(run_dir, stage, &note_md)?;
                }
            }
        }
    }
    let normalized_note = normalize_mcp_usage_note(&note_md, &selected);
    if normalized_note != note_md.trim() {
        note_md = normalized_note;
        persist_stage_mcp_note(run_dir, stage, &note_md)?;
    }
    let (declared_used, declared_not_used, observed_mentions) =
        analyze_mcp_usage_note(&note_md, &selected);
    append_jsonl(
        &run_dir.join(MCP_USAGE_LOG_REF),
        &McpUsageRecord {
            recorded_at: iso_timestamp(),
            stage: stage.to_string(),
            stage_kind: stage_kind.to_string(),
            backend: backend.to_string(),
            source: source.to_string(),
            selected,
            available,
            declared_used,
            declared_not_used,
            observed_mentions,
            note_present: !note_md.trim().is_empty(),
            artifact_path: artifact_path
                .map(|path| path.display().to_string())
                .unwrap_or_default(),
            note_path: if note_path.exists() {
                note_path.display().to_string()
            } else {
                String::new()
            },
            note_md: note_md.trim().to_string(),
        },
    )
}

fn render_intake_prompt(ctx: &Context, run_dir: &Path, plan: &Plan) -> String {
    let agency_root = discover_agency_agents_dir(ctx)
        .map(|path| path.display().to_string())
        .unwrap_or_default();
    let decomposition_rules_path =
        run_reference_asset_display_path(ctx, run_dir, DECOMPOSITION_RULES_REF);
    let role_map_path = run_reference_asset_display_path(ctx, run_dir, ROLE_MAP_REF);
    let mcp_plan_path = mcp_reference_path(run_dir).display().to_string();
    let mcp_usage_hints = render_mcp_usage_hints(run_dir, "intake", None, plan);
    let readiness_label =
        if default_pipeline_includes_execution(&plan.task_kind, &plan.original_task) {
            "execution-ready"
        } else {
            "downstream-ready"
        };
    if plan.prompt_format == "compact" {
        return compact_lines(&json!({
            "stage": "intake",
            "mode": "prepare",
            "read": [
                run_dir.join("request.md").display().to_string(),
                run_dir.join("plan.json").display().to_string(),
                decomposition_rules_path,
                role_map_path,
                mcp_plan_path
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
                "agency_role_catalog_root": agency_root,
                "mcp": plan.mcp,
            },
            "rules": [
                "preserve the original requested outcome as the top-level goal",
                "decompose compound tasks into workstreams instead of silently shrinking the deliverable",
                "refine the goal_checks list so it captures critical user-visible capabilities",
                "follow intake_research_mode when deciding whether to browse before finalizing the brief",
                "treat host_facts from plan.json as authoritative local execution facts",
                "consult the local agency role catalog when selecting or changing solver and reviewer roles if it is available",
                "prefer the provided role map and catalog summary before recursively scanning the local role catalog; open extra role files only when the summary is insufficient",
                "if the selected MCP servers are available in the current client, use them according to references/mcp-plan.md before falling back to generic browsing",
                "if the current plan already captures the named artifacts, local run command, stdout contract, and validation shape, synthesize brief.md directly from request.md plus plan.json before exploring",
                "do not inspect multi-agent-pipeline source files, tests, or SKILL documents unless the user task is specifically about this pipeline",
                "if cache.policy is reuse, consult and update the research cache before duplicating external research",
                "do not implement the solution in this stage"
            ],
            "mcp_usage_hints": mcp_usage_hints,
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
        "# Level 1: Intake And Prompt Builder\n\nRead:\n\n- `{}`\n- `{}`\n- `{}`\n- `{}`\n- `{}`\n\nProduce or update `brief.md`, `plan.json`, and downstream prompts.\n\nCurrent defaults:\n\n- workspace exists: `{}`\n- task kind: `{}`\n- complexity: `{}`\n- execution mode: `{}`\n- solver count: `{}`\n- user summary language: `{}`\n- intake research mode: `{}`\n- stage research mode: `{}`\n- execution network mode: `{}`\n- cache policy: `{}`\n- cache root: `{}`\n- host facts source: `{}`\n- preferred torch device: `{}`\n- agency role catalog: `{}`\n- suggested validation:\n{}\n\nSelected MCP guidance:\n{}\n\nInitial goal checks to refine in `plan.json`:\n{}\n\nRules:\n\n- preserve the user's requested outcome as the top-level goal\n- keep the brief {}\n- decompose compound tasks into workstreams instead of shrinking the deliverable\n- update goal checks when critical capabilities are missing\n- treat host_facts in `plan.json` as authoritative local execution facts\n- consult the local agency role catalog when selecting or changing solver and reviewer roles if it is available\n- prefer the provided role map and catalog summary before recursively scanning the local role catalog; open extra role files only when the summary is insufficient\n- if the selected MCP servers are available in the current client, use them according to `references/mcp-plan.md` before falling back to generic browsing\n- if the current plan already captures the named artifacts, local run command, stdout contract, and validation shape, synthesize `brief.md` directly from `request.md` plus `plan.json` before exploring\n- do not inspect multi-agent-pipeline source files, tests, or SKILL documents unless the user task is specifically about this pipeline\n- follow intake_research_mode when deciding whether to browse\n- do not implement the solution in this stage\n",
        run_dir.join("request.md").display(),
        run_dir.join("plan.json").display(),
        decomposition_rules_path,
        role_map_path,
        mcp_plan_path,
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
        if agency_root.is_empty() {
            "not-detected"
        } else {
            agency_root.as_str()
        },
        bullet_list(plan.validation_commands.iter().map(|item| item.as_str())),
        bullet_list(mcp_usage_hints.iter().map(|item| item.as_str())),
        bullet_list(plan.goal_checks.iter().map(|item| format!("{} {}: {}", if item.critical { "critical" } else { "supporting" }, item.id, item.requirement)).collect::<Vec<_>>().iter().map(|item| item.as_str())),
        readiness_label
    )
}

fn render_solver_prompt(ctx: &Context, run_dir: &Path, plan: &Plan, solver: &SolverRole) -> String {
    let result_file = run_dir
        .join("solutions")
        .join(&solver.solver_id)
        .join("RESULT.md");
    let stage_description = pipeline_stage_spec(plan, run_dir, &solver.solver_id)
        .ok()
        .map(|spec| spec.description)
        .unwrap_or_default();
    let role_docs = load_agency_role_docs(ctx, &[solver.role.clone()]);
    let focused_workstreams = solver_focus_workstreams(plan, solver);
    let mcp_plan_path = mcp_reference_path(run_dir).display().to_string();
    let selected_mcp_servers = effective_solver_mcp_servers(plan, solver);
    let mcp_usage_hints = render_mcp_usage_hints(run_dir, "solver", Some(solver), plan);
    let mcp_heading = localized_mcp_usage_heading(&plan.summary_language);
    let mcp_accountability_rules =
        render_mcp_accountability_rules(&selected_mcp_servers, &plan.summary_language);
    let localized_style_rules = localized_user_facing_style_rules(&plan.summary_language);
    let review_mode = plan.task_kind == "review";
    let analysis_only = task_is_analysis_only(&plan.original_task);
    let analysis_mode = prompt_prefers_lightweight_validation_for_stage(plan, run_dir);
    let validation_hints = non_execution_validation_hints_for_stage(plan, run_dir);
    let mut rules = vec![
        "do not read sibling solver outputs".to_string(),
        "do not modify the primary workspace during solver stage; keep proposed file changes, patch sketches, and exact commands inside the final RESULT artifact".to_string(),
        "do not edit `agent-runs/.../solutions/<solver>/RESULT.md` directly; compose the final RESULT in memory and return it as your final assistant message so the pipeline can materialize the artifact".to_string(),
        "finish the RESULT artifact as soon as the solution shape is clear instead of waiting until the end of the stage".to_string(),
        "your final assistant message must be exactly the final RESULT artifact contents so `logs/<stage>.last.md` captures the same handoff".to_string(),
        "if an MCP call is cancelled or unavailable, continue without it and still finish the final RESULT artifact in your assistant message".to_string(),
        "preserve the full requested system as the top-level goal".to_string(),
        "stay centered on the assigned workstreams and angle so this solution is materially distinct from parallel solvers".to_string(),
        "treat the resolved role docs listed above as authoritative and avoid scanning unrelated role files unless they are clearly insufficient".to_string(),
        "if the selected MCP servers are available in the current client, use them according to references/mcp-plan.md".to_string(),
        "if you narrow scope, record it as phase 1 while keeping the preserved goal explicit".to_string(),
        "follow stage_research_mode when deciding whether to use web research during problem solving".to_string(),
        "state validation performed or the exact blocker".to_string(),
    ];
    rules.extend(localized_style_rules.clone());
    if analysis_mode {
        rules.push(
            "prefer quick, bounded validation first; avoid long-running model, FreeCAD, or end-to-end runtime commands during solver stage unless a cheap preflight has already justified them".to_string(),
        );
        rules.push(
            "if heavyweight local validation would take minutes or large model startup, record the blocker and leave the expensive probe for verification instead of blocking RESULT.md".to_string(),
        );
    }
    let deliverables: Vec<&str> = if review_mode {
        rules.insert(
            2,
            "treat this as a review-only stage; do not turn the outcome into an implementation plan unless the user explicitly asked for fixes".to_string(),
        );
        rules.insert(
            5,
            "anchor material findings in concrete file paths and line references when possible"
                .to_string(),
        );
        vec![
            "scope inspected",
            "findings with severity and evidence",
            "regression risks",
            "test gaps and validation gaps",
            "validation performed",
            "open questions or blind spots",
        ]
    } else if analysis_only {
        rules.insert(
            2,
            "treat this as an analysis-only stage; do not turn the outcome into an implementation plan unless the user explicitly asked for changes".to_string(),
        );
        vec![
            "scope inspected",
            "key findings and evidence",
            "system or contract analysis",
            "validation performed",
            "residual gaps and contradictions",
            "follow-up recommendations",
        ]
    } else {
        vec![
            "assumptions",
            "approach",
            "implementation summary or exact file plan",
            "goal check coverage",
            "focus workstream coverage",
            "validation performed",
            "unresolved risks",
        ]
    };
    let mut read_paths = vec![
        run_dir.join("request.md").display().to_string(),
        run_dir.join("brief.md").display().to_string(),
        run_dir.join("plan.json").display().to_string(),
    ];
    read_paths.extend(
        role_docs
            .iter()
            .map(|doc| doc.full_path.display().to_string()),
    );
    read_paths.push(mcp_plan_path.clone());
    if plan.prompt_format == "compact" {
        return compact_lines(&json!({
            "stage": solver.solver_id,
            "mode": "solve",
            "role": solver.role,
            "angle": solver.angle,
            "stage_description": stage_description,
            "primary_workspace": plan.workspace,
            "read": read_paths,
            "write": [],
            "stage_research_mode": plan.stage_research_mode,
            "focus_workstreams": focused_workstreams.iter().map(|hint| json!({
                "name": hint.name,
                "goal": hint.goal,
                "suggested_role": hint.suggested_role,
            })).collect::<Vec<_>>(),
            "resolved_role_docs": role_docs.iter().map(|doc| json!({
                "requested_role": doc.requested_role,
                "path": doc.full_path.display().to_string(),
                "title": doc.title,
                "description": doc.description,
            })).collect::<Vec<_>>(),
            "mcp_servers": selected_mcp_servers,
            "mcp_usage_hints": mcp_usage_hints,
            "mcp_accountability_rules": mcp_accountability_rules,
            "rules": rules,
            "deliverables": deliverables
                .iter()
                .copied()
                .chain([mcp_heading])
                .collect::<Vec<_>>(),
            "validation_hints": validation_hints
        }));
    }
    format!(
        "# Level 2: {}\n\nAssigned role: `{}`\nSolution angle: `{}`\n\nStage description:\n\n- {}\n\nPrimary workspace:\n\n- `{}`\n\nFocus workstreams:\n{}\n\nRead:\n\n{}\n\nResolved role docs:\n{}\n\nSelected MCP servers:\n{}\n\nMCP usage hints:\n{}\n\nRules:\n\n{}\n\nDeliver:\n\n- return the final solution artifact for `{}` in your final assistant message; the pipeline will materialize the file\n{}\n\nValidation hints:\n{}\n\nStage research mode:\n\n- `{}`\n",
        solver.solver_id,
        solver.role,
        solver.angle,
        if stage_description.trim().is_empty() {
            "no explicit stage description; infer the strongest distinct angle from the role, workstreams, and plan"
        } else {
            stage_description.trim()
        },
        plan.workspace,
        bullet_list(
            focused_workstreams
                .iter()
                .map(|hint| format!("`{}`: {}", hint.name, hint.goal))
                .collect::<Vec<_>>()
                .iter()
                .map(|item| item.as_str())
        ),
        bullet_list(read_paths.iter().map(|item| item.as_str())),
        bullet_list(role_docs.iter().map(|doc| doc.full_path.display().to_string()).collect::<Vec<_>>().iter().map(|item| item.as_str())),
        bullet_list(
            if selected_mcp_servers.is_empty() {
                vec!["none".to_string()]
            } else {
                selected_mcp_servers.clone()
            }
            .iter()
            .map(|item| item.as_str())
        ),
        bullet_list(mcp_usage_hints.iter().map(|item| item.as_str())),
        bullet_list(
            rules
                .iter()
                .chain(mcp_accountability_rules.iter())
                .map(|item| item.as_str())
        ),
        result_file.display(),
        bullet_list(
            deliverables
                .iter()
                .map(|item| format!("include {item}"))
                .chain(std::iter::once(format!("include `{mcp_heading}`")))
                .collect::<Vec<_>>()
                .iter()
                .map(|item| item.as_str())
        ),
        bullet_list(validation_hints.iter().map(|item| item.as_str())),
        plan.stage_research_mode
    )
}

fn render_review_prompt(ctx: &Context, run_dir: &Path, plan: &Plan) -> String {
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
    let reviewer_stack = if plan.reviewer_stack.is_empty() {
        default_reviewer_stack_for_plan(plan)
    } else {
        plan.reviewer_stack.clone()
    };
    let reviewer_docs = load_agency_role_docs(ctx, &reviewer_stack);
    let review_rubric_path = run_reference_asset_display_path(ctx, run_dir, REVIEW_RUBRIC_REF);
    let mcp_plan_path = mcp_reference_path(run_dir).display().to_string();
    let mcp_usage_hints = render_mcp_usage_hints(run_dir, "review", None, plan);
    let selected_review_mcp = selected_mcp_server_names_for_stage_kind(plan, "review", None);
    let mcp_heading = localized_mcp_usage_heading(&plan.summary_language);
    let mcp_accountability_rules =
        render_mcp_accountability_rules(&selected_review_mcp, &plan.summary_language);
    let localized_style_rules = localized_user_facing_style_rules(&plan.summary_language);
    let validation_hints = non_execution_validation_hints_for_stage(plan, run_dir);
    let mut review_rules = vec![
        "compare every solution against the brief, not style preference".to_string(),
        "compare every solution against the plan goal_checks and call out uncovered critical checks".to_string(),
        "if solver outputs are materially identical, say so explicitly and prefer the one with clearer evidence and execution notes instead of inventing false differentiation".to_string(),
        "treat the resolved reviewer docs and review rubric as the primary review guidance; avoid opening unrelated role files unless the current stack is clearly insufficient".to_string(),
        "if the selected MCP servers are available in the current client, use them according to references/mcp-plan.md".to_string(),
        "follow stage_research_mode when deciding whether to use web research during review".to_string(),
        "prefer quick, bounded validation first; do not block review on long-running local model or FreeCAD probes when code and test evidence already establish the main findings".to_string(),
        "scorecard_json must include winner, backup, why, validation_evidence, critical_gaps, and execution_notes".to_string(),
        "penalize silent scope reduction".to_string(),
        "treat missing evidence as a penalty".to_string(),
        "write a short user-facing summary in the requested language".to_string(),
        "recommend a hybrid only when the parts are clearly compatible".to_string(),
        format!("include a `## {mcp_heading}` section in review/report.md"),
        "do not edit `review/report.md`, `review/scorecard.json`, or `review/user-summary.md` directly; compose them in memory and return them only via the tagged fallback bundle so the pipeline can materialize the files".to_string(),
        format!(
            "return a final backup bundle using the exact tags `{REVIEW_REPORT_START}`, `{REVIEW_SCORECARD_START}`, and `{REVIEW_USER_SUMMARY_START}` so the pipeline can materialize the review outputs without direct file edits"
        ),
    ];
    review_rules.extend(localized_style_rules.clone());
    if plan.task_kind == "audit-improve" {
        review_rules.push(
            "treat the verification-seeded request as authoritative and keep the review bounded to the named follow-up gap plus preservation of already-green slices".to_string(),
        );
        review_rules.push(
            "prefer the solution that refreshes current facts and closes the follow-up gap with the least reopening of unrelated work".to_string(),
        );
    }
    if plan.prompt_format == "compact" {
        let mut read_paths = vec![
            run_dir.join("request.md").display().to_string(),
            run_dir.join("brief.md").display().to_string(),
            run_dir.join("plan.json").display().to_string(),
            review_rubric_path,
            mcp_plan_path,
        ];
        read_paths.extend(solution_files.iter().cloned());
        return compact_lines(&json!({
            "stage": "review",
            "mode": "compare",
            "read": read_paths,
            "write": [],
            "reviewer_stack": reviewer_stack,
            "resolved_reviewer_docs": reviewer_docs.iter().map(|doc| json!({
                "requested_role": doc.requested_role,
                "path": doc.full_path.display().to_string(),
                "title": doc.title,
                "description": doc.description,
            })).collect::<Vec<_>>(),
            "user_summary_language": plan.summary_language,
            "stage_research_mode": plan.stage_research_mode,
            "mcp_usage_hints": mcp_usage_hints,
            "mcp_accountability_rules": mcp_accountability_rules,
            "validation_hints": validation_hints,
            "rules": review_rules,
            "backup_bundle_format": [
                REVIEW_REPORT_START,
                REVIEW_SCORECARD_START,
                REVIEW_USER_SUMMARY_START,
            ],
        }));
    }
    format!(
        "# Level 3: Censor And Reviewer\n\nRead:\n\n- `{}`\n- `{}`\n- `{}`\n- `{}`\n- `{}`\n- solver outputs:\n{}\n\nReviewer stack:\n{}\n\nResolved reviewer role docs:\n{}\n\nSelected MCP guidance:\n{}\n\nStage research mode:\n\n- `{}`\n\nValidation hints:\n{}\n\nRules:\n\n- compare every solution against the brief and plan goal checks, not style preference\n- if solver outputs are materially identical, say so explicitly and prefer the one with clearer evidence and execution notes instead of inventing false differentiation\n- treat the resolved reviewer docs and review rubric as the primary review guidance; avoid opening unrelated role files unless the current stack is clearly insufficient\n- if the selected MCP servers are available in the current client, use them according to `references/mcp-plan.md`\n- penalize silent scope reduction and missing evidence\n{}\n\nScorecard requirements:\n\n- `winner`: selected solver id or `hybrid`\n- `backup`: second-best solver id when available\n- `why`: short verdict grounded in evidence\n- `validation_evidence`: commands, artifacts, or observations used during review\n- `critical_gaps`: uncovered critical goal checks or blockers\n- `execution_notes`: concrete instructions for execution, including which compatible pieces to combine for a hybrid\n\nDeliver:\n\n- do not edit `review/report.md`, `review/scorecard.json`, or `review/user-summary.md` directly\n- keep user-facing prose in `{}` and localize section headings accordingly\n- return the review outputs only via this exact fallback bundle so the pipeline can materialize the files:\n  - `<<<AGPIPE_REVIEW_REPORT>>>`\n  - report markdown\n  - `<<<AGPIPE_REVIEW_SCORECARD_JSON>>>`\n  - raw JSON object\n  - `<<<AGPIPE_REVIEW_USER_SUMMARY>>>`\n  - user summary markdown\n",
        run_dir.join("request.md").display(),
        run_dir.join("brief.md").display(),
        run_dir.join("plan.json").display(),
        run_reference_asset_display_path(ctx, run_dir, REVIEW_RUBRIC_REF),
        mcp_plan_path,
        bullet_list(solution_files.iter().map(|item| item.as_str())),
        bullet_list(reviewer_stack.iter().map(|item| item.as_str())),
        bullet_list(reviewer_docs.iter().map(|doc| doc.full_path.display().to_string()).collect::<Vec<_>>().iter().map(|item| item.as_str())),
        bullet_list(mcp_usage_hints.iter().map(|item| item.as_str())),
        plan.stage_research_mode,
        bullet_list(validation_hints.iter().map(|item| item.as_str())),
        bullet_list(
            review_rules
                .iter()
                .map(|item| item.as_str())
                .chain(mcp_accountability_rules.iter().map(|item| item.as_str()))
        ),
        plan.summary_language
    )
}

fn render_execution_prompt(run_dir: &Path, plan: &Plan) -> String {
    let validation_commands = effective_validation_commands(run_dir, plan, "execution");
    let service_harness_expected =
        runtime_harness_expected_for_stage(plan, run_dir, PipelineStageKind::Execution);
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
    let mcp_plan_path = mcp_reference_path(run_dir).display().to_string();
    let mcp_usage_hints = render_mcp_usage_hints(run_dir, "execution", None, plan);
    let selected_execution_mcp = selected_mcp_server_names_for_stage_kind(plan, "execution", None);
    let mcp_heading = localized_mcp_usage_heading(&plan.summary_language);
    let mcp_accountability_rules =
        render_mcp_accountability_rules(&selected_execution_mcp, &plan.summary_language);
    let localized_style_rules = localized_user_facing_style_rules(&plan.summary_language);
    let user_facing_language_section = if localized_style_rules.is_empty() {
        String::new()
    } else {
        format!(
            "\nUser-facing language rules:\n{}\n",
            bullet_list(localized_style_rules.iter().map(|item| item.as_str()))
        )
    };
    let mut read_paths = vec![
        run_dir.join("request.md").display().to_string(),
        run_dir.join("brief.md").display().to_string(),
        run_dir.join("plan.json").display().to_string(),
        mcp_plan_path.clone(),
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
            mcp_plan_path.clone(),
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
        if service_harness_expected {
            rules.push(format!(
                "if the deliverable is a runtime target such as a service, TUI, or GUI app, keep `{}` updated so agpipe can execute realistic runtime scenarios",
                RUNTIME_CHECK_SPEC_REF
            ));
            rules.push(format!(
                "for runtime-facing work, define realistic runtime scenarios and run `{}` after implementation",
                service_check_validation_command(run_dir, "execution")
            ));
        }
        if review_present {
            rules.insert(
                2,
                "follow the review recommendation unless local validation forces a narrower implementation".to_string(),
            );
            rules.insert(
                3,
                "treat any workspace edits that predate execution as untrusted proposals until they match the brief, review verdict, and local validation".to_string(),
            );
        } else {
            rules.insert(
                2,
                "there is no review stage in this pipeline, so synthesize the best implementation directly from the brief and solver outputs".to_string(),
            );
            rules.insert(
                3,
                "treat any incidental workspace edits from earlier stages as proposals, not accepted output, until local validation confirms them".to_string(),
            );
        }
        return compact_lines(&json!({
            "stage": "execution",
            "mode": "implement",
            "stage_research_mode": plan.stage_research_mode,
            "read": compact_reads,
            "write": [run_dir.join("execution").join("report.md").display().to_string()],
            "rules": rules,
            "mcp_usage_hints": mcp_usage_hints,
            "mcp_accountability_rules": mcp_accountability_rules,
            "deliverables": [
                "actual workspace changes",
                "execution summary",
                "changed files",
                "MCP Usage",
                "validation performed",
                "remaining blockers and next steps"
            ],
            "validation_hints": validation_commands
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
    let leakage_rule = if review_present {
        "treat any workspace edits that predate execution as untrusted proposals until they match the brief, review verdict, and local validation"
    } else {
        "treat any incidental workspace edits from earlier stages as proposals, not accepted output, until local validation confirms them"
    };
    format!(
        "# Level 4: Execution\n\nRead:\n\n- `{}`\n- `{}`\n- `{}`\n- `{}`\n{}- relevant solver outputs:\n{}\n\nExecution guidance:\n\n- {}\n- {}\n\nSelected MCP guidance:\n{}\n\nExecution network mode:\n\n- `{}`\n\nStage research mode:\n\n- `{}`\n\nCache:\n\n- policy: `{}`\n- root: `{}`\n\nMCP accountability:\n{}\n{}\nDeliver:\n\n- actual code or configuration changes in the workspace\n- `execution/report.md`\n- include `{}`\n- keep user-facing prose in `{}` and localize section headings accordingly\n\nValidation hints:\n{}\n",
        run_dir.join("request.md").display(),
        run_dir.join("brief.md").display(),
        run_dir.join("plan.json").display(),
        mcp_plan_path,
        review_section,
        bullet_list(solution_files.iter().map(|item| item.as_str())),
        review_rule,
        leakage_rule,
        bullet_list(mcp_usage_hints.iter().map(|item| item.as_str())),
        plan.execution_network_mode,
        plan.stage_research_mode,
        cache_config_from_plan(plan).policy,
        cache_config_from_plan(plan).root,
        bullet_list(mcp_accountability_rules.iter().map(|item| item.as_str())),
        user_facing_language_section,
        mcp_heading,
        plan.summary_language,
        bullet_list(validation_commands.iter().map(|item| item.as_str()))
    )
}

fn render_verification_prompt(ctx: &Context, run_dir: &Path, plan: &Plan) -> String {
    let validation_commands = effective_validation_commands(run_dir, plan, "verification");
    let service_harness_expected =
        runtime_harness_expected_for_stage(plan, run_dir, PipelineStageKind::Verification);
    let review_present = first_stage_id_for_kind(plan, run_dir, PipelineStageKind::Review)
        .ok()
        .flatten()
        .is_some();
    let execution_present = first_stage_id_for_kind(plan, run_dir, PipelineStageKind::Execution)
        .ok()
        .flatten()
        .is_some();
    let verification_rubric_path =
        run_reference_asset_display_path(ctx, run_dir, VERIFICATION_RUBRIC_REF);
    let mcp_plan_path = mcp_reference_path(run_dir).display().to_string();
    let mcp_usage_hints = render_mcp_usage_hints(run_dir, "verification", None, plan);
    let selected_verification_mcp =
        selected_mcp_server_names_for_stage_kind(plan, "verification", None);
    let mcp_heading = localized_mcp_usage_heading(&plan.summary_language);
    let mcp_accountability_rules =
        render_mcp_accountability_rules(&selected_verification_mcp, &plan.summary_language);
    let localized_style_rules = localized_user_facing_style_rules(&plan.summary_language);
    if plan.prompt_format == "compact" {
        let mut reads = vec![
            run_dir.join("request.md").display().to_string(),
            run_dir.join("brief.md").display().to_string(),
            run_dir.join("plan.json").display().to_string(),
            mcp_plan_path.clone(),
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
        reads.push(verification_rubric_path.clone());
        let mut rules = vec![
            "act in code-review mode: prioritize bugs, regressions, unsafe behavior, and missing validation".to_string(),
            "run the cheapest relevant checks first and record exact evidence or blockers".to_string(),
            "write findings ordered by severity with file references when possible".to_string(),
            "call out stale or contradictory artifacts, including workspace changes that appear to have bypassed the intended execution stage".to_string(),
            "verify device choice against host_facts from plan.json when relevant".to_string(),
            "set goal_complete=false when any critical plan goal check remains missing, unverified, or replaced by a placeholder implementation".to_string(),
            "write `verification/improvement-request.md` and `verification/augmented-task.md` for the NEXT rerun, not as a request to mutate previous run-local files under `agent-runs`".to_string(),
            "treat previous run artifact paths as evidence references only; when a follow-up needs refreshed run-local narrative, the rerun should write it under its own run directory".to_string(),
            "only workspace or user-facing deliverables outside `agent-runs` may be updated in place when the follow-up explicitly requires a mirror".to_string(),
            "if there are no meaningful findings, say so explicitly".to_string(),
            "do not edit `verification/findings.md`, `verification/user-summary.md`, `verification/goal-status.json`, `verification/improvement-request.md`, or `verification/augmented-task.md` directly; compose them in memory and return them only via the tagged fallback bundle so the pipeline can materialize the files".to_string(),
        ];
        if service_harness_expected {
            rules.push(format!(
                "when `{}` exists, treat the latest runtime-check summary as runtime evidence and reconcile any failed scenarios with the actual implementation",
                RUNTIME_CHECK_SPEC_REF
            ));
        }
        if execution_present {
            rules.insert(
                0,
                "review the actual workspace implementation, not only the plans".to_string(),
            );
            rules.insert(
                2,
                "start from execution/report.md and the review verdict".to_string(),
            );
        } else {
            rules.insert(
                0,
                "audit the produced research or documentation artifacts against the brief, and verify any workspace-dependent claims against actual files when they matter".to_string(),
            );
            rules.insert(
                2,
                "start from the review verdict and solver evidence; inspect the workspace only where the recommendation depends on current code state".to_string(),
            );
            rules.insert(
                4,
                "do not report a missing `execution/report.md` as a defect when this run has no execution stage".to_string(),
            );
        }
        rules.extend(localized_style_rules.clone());
        rules.push(format!(
            "return a final backup bundle using the exact tags `{VERIFICATION_FINDINGS_START}`, `{VERIFICATION_GOAL_STATUS_START}`, `{VERIFICATION_USER_SUMMARY_START}`, `{VERIFICATION_IMPROVEMENT_REQUEST_START}`, and `{VERIFICATION_AUGMENTED_TASK_START}` so the pipeline can materialize the verification outputs without direct file edits"
        ));
        return compact_lines(&json!({
            "stage": "verification",
            "mode": "audit",
            "read": reads,
            "write": [],
            "validation_hints": validation_commands,
            "user_summary_language": plan.summary_language,
            "stage_research_mode": plan.stage_research_mode,
            "mcp_usage_hints": mcp_usage_hints,
            "mcp_accountability_rules": mcp_accountability_rules,
            "rules": rules,
            "backup_bundle_format": [
                VERIFICATION_FINDINGS_START,
                VERIFICATION_GOAL_STATUS_START,
                VERIFICATION_USER_SUMMARY_START,
                VERIFICATION_IMPROVEMENT_REQUEST_START,
                VERIFICATION_AUGMENTED_TASK_START
            ],
            "user_facing_mcp_heading": mcp_heading,
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
        "# Level 5: Verification And Improvement Seed\n\nRead:\n\n- `{}`\n- `{}`\n- `{}`\n- `{}`\n{}{}- `{}`\n\nAudit emphasis:\n\n- prioritize bugs, regressions, unsafe behavior, and missing validation\n- call out stale or contradictory artifacts, including workspace changes that appear to have bypassed the intended execution stage\n- set `goal_complete=false` when any critical goal check remains missing or unverified\n- if this run has no execution stage, do not treat missing `execution/report.md` as a defect by itself\n- write `verification/improvement-request.md` and `verification/augmented-task.md` for the next rerun, not as a request to mutate previous run-local files under `agent-runs`\n- treat previous run artifact paths as evidence references only; when the follow-up needs refreshed run-local narrative, the rerun should write it under its own run directory\n- only workspace or user-facing deliverables outside `agent-runs` may be updated in place when the follow-up explicitly requires a mirror\n\nSelected MCP guidance:\n{}\n\nMCP accountability:\n{}\n\nUser-facing language rules:\n{}\n\nDeliver:\n\n- do not edit `verification/findings.md`, `verification/user-summary.md`, `verification/goal-status.json`, `verification/improvement-request.md`, or `verification/augmented-task.md` directly\n- include `{}` in the findings content\n- keep user-facing prose in `{}` and localize section headings accordingly\n- return the verification outputs only via this exact fallback bundle so the pipeline can materialize the files:\n  - `<<<AGPIPE_VERIFICATION_FINDINGS>>>`\n  - findings markdown\n  - `<<<AGPIPE_VERIFICATION_GOAL_STATUS_JSON>>>`\n  - raw JSON object\n  - `<<<AGPIPE_VERIFICATION_USER_SUMMARY>>>`\n  - user summary markdown\n  - `<<<AGPIPE_VERIFICATION_IMPROVEMENT_REQUEST>>>`\n  - improvement request markdown\n  - `<<<AGPIPE_VERIFICATION_AUGMENTED_TASK>>>`\n  - augmented task markdown\n\nStage research mode:\n\n- `{}`\n\nValidation hints:\n{}\n",
        run_dir.join("request.md").display(),
        run_dir.join("brief.md").display(),
        run_dir.join("plan.json").display(),
        mcp_plan_path,
        review_lines,
        execution_line,
        verification_rubric_path,
        bullet_list(mcp_usage_hints.iter().map(|item| item.as_str())),
        bullet_list(mcp_accountability_rules.iter().map(|item| item.as_str())),
        bullet_list(localized_style_rules.iter().map(|item| item.as_str())),
        mcp_heading,
        plan.summary_language,
        plan.stage_research_mode,
        bullet_list(validation_commands.iter().map(|item| item.as_str()))
    )
}

fn ensure_reviewer_stack(plan: &mut Plan) {
    if plan.reviewer_stack.is_empty() {
        plan.reviewer_stack = default_reviewer_stack_for_plan(plan);
    }
}

fn infer_goal_checks(
    task: &str,
    task_kind: &str,
    workstream_hints: &[WorkstreamHint],
) -> Vec<GoalCheck> {
    let text = task.to_lowercase();
    let analysis_only = task_is_analysis_only(task);
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
    if task_kind == "review" {
        add_goal_check(
            &mut seen,
            &mut checks,
            "review_findings",
            "produce ordered review findings that prioritize bugs, regressions, unsafe behavior, and broken assumptions by severity",
            true,
        );
        add_goal_check(
            &mut seen,
            &mut checks,
            "evidence_with_file_refs",
            "support each material finding with concrete evidence, including file paths and line references when possible",
            true,
        );
        add_goal_check(
            &mut seen,
            &mut checks,
            "test_and_validation_gaps",
            "identify missing tests, weak validation, or risky behavior that remains unverified",
            true,
        );
        add_goal_check(
            &mut seen,
            &mut checks,
            "review_only_scope",
            "keep the run review-only unless the user explicitly requested fixes or implementation work",
            true,
        );
        add_goal_check(
            &mut seen,
            &mut checks,
            "coverage_of_requested_codebase",
            "inspect the requested codebase deeply enough to justify the findings and call out any blind spots explicitly",
            false,
        );
        return checks;
    }
    if task_kind == "audit-improve" {
        if [
            "device_story_reconciled",
            "device story",
            "device-story",
            "mps",
            "torch",
            "host probe",
            "device probe",
            "cpu fallback",
        ]
        .iter()
        .any(|word| text.contains(word))
        {
            add_goal_check(
                &mut seen,
                &mut checks,
                "device_story_reconciled",
                "reconcile the verification-era device/runtime contradiction with fresh same-basis evidence instead of stale narrative",
                true,
            );
        } else {
            add_goal_check(
                &mut seen,
                &mut checks,
                "verification_gap_closed",
                "close the named verification-derived gap with fresh local evidence rather than reopening unrelated work",
                true,
            );
        }
        add_goal_check(
            &mut seen,
            &mut checks,
            "current_facts_authoritative",
            "refresh the authoritative current facts and run-facing narrative so follow-up stages do not rely on stale request, brief, or review artifacts",
            true,
        );
        add_goal_check(
            &mut seen,
            &mut checks,
            "preserve_verified_green_slices",
            "preserve already-verified green slices unless new local evidence explicitly disproves them",
            true,
        );
        add_goal_check(
            &mut seen,
            &mut checks,
            "bounded_follow_up_scope",
            "keep the follow-up narrowly scoped to the verification seed and avoid broad reimplementation",
            true,
        );
        if workstream_hints
            .iter()
            .any(|hint| hint.name == "artifact-refresh-and-narrative")
        {
            add_goal_check(
                &mut seen,
                &mut checks,
                "artifact_refresh",
                "refresh user-facing narrative artifacts from current evidence only",
                false,
            );
        }
        return checks;
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
    if !analysis_only
        && [
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
    if !analysis_only && text.contains("freecad") {
        add_goal_check(
            &mut seen,
            &mut checks,
            "freecad_output",
            "produce deterministic FreeCAD output from the structured plan",
            true,
        );
    }
    if !analysis_only
        && (matches!(task_kind, "ai" | "backend" | "fullstack")
            || ["service", "bot", "api", "сервис", "бот", "entrypoint"]
                .iter()
                .any(|word| text_contains_signal(&text, word)))
    {
        add_goal_check(
            &mut seen,
            &mut checks,
            "runnable_entrypoint",
            "provide a runnable local entrypoint or service path for the implemented slice",
            true,
        );
    }
    if analysis_only {
        add_goal_check(
            &mut seen,
            &mut checks,
            "reproducible_audit_report",
            "produce a reproducible evidence-driven audit or analysis package grounded in the current repository state",
            true,
        );
        add_goal_check(
            &mut seen,
            &mut checks,
            "residual_gaps_documented",
            "document residual gaps, contradictions, and exact follow-up work without silently converting the task into implementation",
            true,
        );
    }
    if task_requests_cli_entrypoint(task_kind, task) && text_contains_signal(&text, "main.py") {
        add_goal_check(
            &mut seen,
            &mut checks,
            "main_py_entrypoint",
            "place or update the primary runnable entrypoint in `main.py`",
            true,
        );
    }
    if task_requests_cli_entrypoint(task_kind, task) && task_requests_precise_local_contract(task) {
        add_goal_check(
            &mut seen,
            &mut checks,
            "exact_stdout_contract",
            "match the requested stdout contract exactly for the primary local run path",
            true,
        );
    }
    if text_contains_signal(&text, "readme") || text_contains_signal(&text, "readme.md") {
        add_goal_check(
            &mut seen,
            &mut checks,
            "readme_run_command",
            "document the exact local run command and expected result in README or equivalent developer docs",
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
    let trivial_local_cli =
        task_is_trivial_local_cli_contract(&task_kind, &complexity, task_text, Some(&workspace));
    let workstream_hints = workstream_hints_for(&task_kind, task_text);
    let execution_mode = infer_execution_mode(&task_kind, &complexity, task_text);
    let mut solver_count = solver_count_for(
        &task_kind,
        &complexity,
        &execution_mode,
        &workstream_hints,
        task_text,
    );
    if trivial_local_cli {
        solver_count = 1;
    }
    if !default_pipeline_includes_execution(&task_kind, task_text) {
        solver_count = solver_count.max(2);
    }
    let goal_checks = infer_goal_checks(task_text, &task_kind, &workstream_hints);
    let token_budget = infer_token_budget(&complexity);
    let stack_signals = detect_stack(&workspace);
    let host_facts = detect_host_facts("init_run_local_rust");
    let mut validation_commands = task_specific_validation_commands(&task_kind, task_text);
    for command in build_validation_commands(&workspace, &stack_signals) {
        if !validation_commands.contains(&command) {
            validation_commands.push(command);
        }
    }
    let roles = choose_roles_with_workstreams(&task_kind, task_text, solver_count, &workstream_hints);
    let reviewer_stack = reviewer_stack_for_task(&task_kind, &complexity, task_text);
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
    seed_run_reference_assets(ctx, &run_dir)?;

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
        reviewer_stack,
        stack_signals,
        validation_commands,
        mcp: McpSelection::default(),
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
            ("mcp_plan".to_string(), MCP_PLAN_REF.to_string()),
        ]),
        pipeline: PipelineConfig::default(),
    };
    ensure_plan_mcp_defaults(&mut plan);
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

fn render_stage_prompt(ctx: &Context, run_dir: &Path, stage: &str) -> Result<String, String> {
    let mut plan = load_plan(run_dir)?;
    ensure_reviewer_stack(&mut plan);
    let spec = pipeline_stage_spec(&plan, run_dir, stage)?;
    match pipeline_kind_from_str(&spec.kind) {
        Some(PipelineStageKind::Intake) => Ok(render_intake_prompt(ctx, run_dir, &plan)),
        Some(PipelineStageKind::Review) => Ok(render_review_prompt(ctx, run_dir, &plan)),
        Some(PipelineStageKind::Execution) => Ok(render_execution_prompt(run_dir, &plan)),
        Some(PipelineStageKind::Verification) => {
            Ok(render_verification_prompt(ctx, run_dir, &plan))
        }
        Some(PipelineStageKind::Solver) => Ok(render_solver_prompt(
            ctx,
            run_dir,
            &plan,
            &SolverRole {
                solver_id: spec.id,
                role: spec.role,
                angle: spec.angle,
                mcp_servers: Vec::new(),
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
    let review_summary_placeholder = if language_is_russian(&plan.summary_language) {
        "# Краткий итог\n\nЛокализованный итог review пока не подготовлен.\n".to_string()
    } else {
        "# User Summary\n\nPending localized review summary.\n".to_string()
    };
    let verification_summary_placeholder = if language_is_russian(&plan.summary_language) {
        "# Итог проверки\n\nЛокализованный итог проверки пока не подготовлен.\n".to_string()
    } else {
        "# Verification Summary\n\nPending localized verification summary.\n".to_string()
    };
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
                review_summary_placeholder,
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
                    verification_summary_placeholder,
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
        "локализованный итог review пока не подготовлен.",
        "pending execution stage.",
        "pending verification stage.",
        "pending localized verification summary.",
        "локализованный итог проверки пока не подготовлен.",
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
    if kind == PipelineStageKind::Execution {
        if runtime_check_required(run_dir, "execution")? {
            let summary = service_check_summary_json_path(run_dir, "execution");
            let status = read_json::<Value>(&summary)
                .ok()
                .and_then(|value| value.get("status").and_then(|item| item.as_str()).map(str::to_string));
            if status.as_deref() != Some("passed") {
                return Ok(false);
            }
        }
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
    // Normalize runtime state transitions like dead pid -> exited before
    // deriving doctor/status snapshots so UI state stays consistent.
    let _ = crate::runtime::active_job_state(run_dir);
    check_run_interrupt(run_dir)?;
    let mut plan = load_plan(run_dir)?;
    apply_pipeline_solver_defaults(&mut plan, Some(run_dir))?;
    ensure_reviewer_stack(&mut plan);
    let cache = cache_config_from_plan(&plan);
    let pipeline = pipeline_stage_specs(&plan, Some(run_dir))?;
    ensure_cache_layout(&cache)?;
    fs::create_dir_all(run_dir.join("host"))
        .map_err(|err| format!("Could not create host dir: {err}"))?;

    for stage in pipeline {
        check_run_interrupt(run_dir)?;
        let prompt_path = stage_prompt_path(run_dir, &stage.id)?;
        let prompt = render_stage_prompt(ctx, run_dir, &stage.id)?;
        write_text(&prompt_path, &prompt)?;
        for (path, content) in stage_placeholder_content(&plan, run_dir, &stage.id)? {
            check_run_interrupt(run_dir)?;
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
    check_run_interrupt(run_dir)?;
    if !backend_config_path(run_dir).exists() {
        persist_run_backend_config(run_dir, ctx)?;
    }
    save_plan(run_dir, &plan)?;
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

#[derive(Debug, Clone)]
struct ActiveStageStall {
    stage: String,
    elapsed_secs: u64,
    idle_secs: u64,
}

fn active_stage_progress_paths(run_dir: &Path, stage: &str, plan: &Plan) -> Result<Vec<PathBuf>, String> {
    let mut candidates = vec![
        run_dir.join("logs").join(format!("{stage}.last.md")),
        run_dir.join("logs").join(format!("{stage}.stdout.log")),
        run_dir.join("logs").join(format!("{stage}.stderr.log")),
    ];
    match stage {
        "review" => {
            candidates.push(run_dir.join("review").join("report.md"));
            candidates.push(run_dir.join("review").join("user-summary.md"));
            candidates.push(run_dir.join("review").join("scorecard.json"));
        }
        "execution" => {
            candidates.push(run_dir.join("execution").join("report.md"));
        }
        "verification" => {
            candidates.push(run_dir.join("verification").join("findings.md"));
            candidates.push(run_dir.join("verification").join("user-summary.md"));
            candidates.push(run_dir.join("verification").join("goal-status.json"));
            candidates.push(run_dir.join("verification").join("improvement-request.md"));
            if plan.augmented_follow_up_enabled {
                candidates.push(augmented_task_path(run_dir));
            }
        }
        "intake" => {
            candidates.push(run_dir.join("brief.md"));
            candidates.push(run_dir.join("plan.json"));
        }
        solver if solver.starts_with("solver-") => {
            candidates.push(run_dir.join("solutions").join(solver).join("RESULT.md"));
        }
        "" | "none" | "rerun" => {
            candidates.push(crate::runtime::process_log_path(run_dir));
        }
        _ => {}
    }
    Ok(candidates)
}

fn substantive_stage_progress_mtime(path: &Path, stage: &str) -> Option<SystemTime> {
    let file_name = path.file_name().and_then(|name| name.to_str()).unwrap_or_default();
    match file_name {
        "report.md" => {
            let section = if path.components().any(|component| component.as_os_str() == "review") {
                "review"
            } else if path
                .components()
                .any(|component| component.as_os_str() == "execution")
            {
                "execution"
            } else {
                stage
            };
            if output_looks_placeholder(section, &read_text(path).ok()?) {
                return None;
            }
        }
        "RESULT.md" => {
            if output_looks_placeholder(stage, &read_text(path).ok()?) {
                return None;
            }
        }
        "brief.md" => {
            if output_looks_placeholder("intake", &read_text(path).ok()?) {
                return None;
            }
        }
        "findings.md" => {
            if output_looks_placeholder("verification", &read_text(path).ok()?) {
                return None;
            }
        }
        "user-summary.md" => {
            let section = if path
                .components()
                .any(|component| component.as_os_str() == "verification")
            {
                "verification-summary"
            } else {
                "review-summary"
            };
            if output_looks_placeholder(section, &read_text(path).ok()?) {
                return None;
            }
        }
        "improvement-request.md" => {
            if output_looks_placeholder("improvement-request", &read_text(path).ok()?) {
                return None;
            }
        }
        "augmented-task.md" => {
            if output_looks_placeholder("augmented-task", &read_text(path).ok()?) {
                return None;
            }
        }
        "scorecard.json" => {
            if !review_scorecard_complete(path) {
                return None;
            }
        }
        "goal-status.json" => {
            if !goal_status_complete(path) {
                return None;
            }
        }
        _ => {}
    }
    path.metadata().ok()?.modified().ok()
}

fn active_stage_stall(run_dir: &Path) -> Result<Option<ActiveStageStall>, String> {
    let Some(state) = crate::runtime::active_job_state(run_dir) else {
        return Ok(None);
    };
    let Some(recorded_stage) = state.stage.clone() else {
        return Ok(None);
    };
    let stage = resolve_attempt_stage(run_dir, &state, &recorded_stage)?;
    if stage.trim().is_empty() || stage == "rerun" {
        return Ok(None);
    }
    if !available_stages(run_dir)?
        .iter()
        .any(|candidate| candidate == &stage)
    {
        return Ok(None);
    }
    let elapsed_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(state.started_at_unix)
        .saturating_sub(state.started_at_unix);
    if elapsed_secs < ACTIVE_STAGE_STALL_WARN_SECS {
        return Ok(None);
    }
    let plan = load_plan(run_dir)?;
    let latest_progress = active_stage_progress_paths(run_dir, &stage, &plan)?
        .into_iter()
        .filter_map(|path| substantive_stage_progress_mtime(&path, &stage))
        .max();
    let idle_secs = latest_progress
        .and_then(|modified| SystemTime::now().duration_since(modified).ok())
        .map(|duration| duration.as_secs())
        .unwrap_or(elapsed_secs);
    if idle_secs < ACTIVE_STAGE_STALL_WARN_SECS {
        return Ok(None);
    }
    Ok(Some(ActiveStageStall {
        stage,
        elapsed_secs,
        idle_secs,
    }))
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
    let mut upstream = vec![host_probe_path(run_dir)];
    if first_stage_id_for_kind(&plan, run_dir, PipelineStageKind::Execution)?.is_some() {
        upstream.push(run_dir.join("execution").join("report.md"));
    } else if first_stage_id_for_kind(&plan, run_dir, PipelineStageKind::Review)?.is_some() {
        upstream.push(run_dir.join("review").join("report.md"));
        upstream.push(run_dir.join("review").join("scorecard.json"));
        upstream.push(run_dir.join("review").join("user-summary.md"));
    }
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

fn incomplete_stage_attempt(run_dir: &Path) -> Result<Option<RuntimeAttemptPayload>, String> {
    let Some(state) = crate::runtime::load_job_state(run_dir) else {
        return Ok(None);
    };
    if state.label == "doctor fix" && state.status == "completed" && state.exit_code == Some(0) {
        return Ok(None);
    }
    let Some(recorded_stage) = state.stage.clone() else {
        return Ok(None);
    };
    let stage = resolve_attempt_stage(run_dir, &state, &recorded_stage)?;
    if state.is_active() {
        return Ok(None);
    }
    if should_ignore_completed_single_step_attempt(run_dir, &state, &stage)? {
        return Ok(None);
    }
    if !available_stages(run_dir)?
        .iter()
        .any(|candidate| candidate == &stage)
    {
        return Ok(None);
    }
    if is_stage_complete(run_dir, &stage)? {
        return Ok(None);
    }
    let message = state.message.clone().unwrap_or_else(|| {
        if state.status == "completed" && state.exit_code == Some(0) {
            "Tracked job reported completion, but the stage artifacts are still incomplete."
                .to_string()
        } else if let Some(code) = state.exit_code {
            format!("Tracked job ended with exit code {code} before stage outputs completed.")
        } else {
            format!(
                "Tracked job ended with status `{}` before stage outputs completed.",
                state.status
            )
        }
    });
    Ok(Some(RuntimeAttemptPayload {
        label: state.label,
        stage,
        command_hint: state.command_hint,
        status: state.status,
        exit_code: state.exit_code,
        message,
    }))
}

fn resolve_attempt_stage(
    run_dir: &Path,
    state: &crate::runtime::RuntimeJobState,
    recorded_stage: &str,
) -> Result<String, String> {
    if state.label != "resume" {
        return Ok(recorded_stage.to_string());
    }
    let Some(next) = next_stage_for_run(run_dir)? else {
        return Ok(recorded_stage.to_string());
    };
    if next == "rerun" || next == recorded_stage {
        return Ok(recorded_stage.to_string());
    }
    if available_stages(run_dir)?
        .iter()
        .any(|candidate| candidate == &next)
    {
        return Ok(next);
    }
    Ok(recorded_stage.to_string())
}

fn should_ignore_completed_single_step_attempt(
    run_dir: &Path,
    state: &crate::runtime::RuntimeJobState,
    recorded_stage: &str,
) -> Result<bool, String> {
    if !matches!(state.label.as_str(), "start-next" | "safe-next") {
        return Ok(false);
    }
    if state.status != "completed" || state.exit_code != Some(0) {
        return Ok(false);
    }
    let Some((completed_stage, completed_code)) = last_completed_stage_from_process_log(run_dir)
    else {
        return Ok(false);
    };
    if completed_code != Some(0) || completed_stage == recorded_stage {
        return Ok(false);
    }
    if next_stage_for_run(run_dir)?.as_deref() != Some(recorded_stage) {
        return Ok(false);
    }
    if stage_direct_successor(run_dir, &completed_stage)?.as_deref() != Some(recorded_stage) {
        return Ok(false);
    }
    Ok(is_stage_complete(run_dir, &completed_stage)?)
}

fn stage_direct_successor(run_dir: &Path, stage: &str) -> Result<Option<String>, String> {
    let stages = available_stages(run_dir)?;
    let Some(index) = stages.iter().position(|candidate| candidate == stage) else {
        return Ok(None);
    };
    Ok(stages.get(index + 1).cloned())
}

fn last_completed_stage_from_process_log(run_dir: &Path) -> Option<(String, Option<i32>)> {
    crate::runtime::tail_process_log(run_dir, 200)
        .into_iter()
        .rev()
        .find_map(|line| parse_completed_stage_line(&line))
}

fn parse_completed_stage_line(line: &str) -> Option<(String, Option<i32>)> {
    let trimmed = line.trim();
    let rest = trimmed.strip_prefix("Completed ")?;
    let (stage, tail) = rest.split_once(" with exit code ")?;
    let code_text: String = tail
        .chars()
        .take_while(|ch| ch.is_ascii_digit() || *ch == '-')
        .collect();
    if code_text.is_empty() {
        return None;
    }
    let code = code_text.parse::<i32>().ok()?;
    Some((stage.trim().to_string(), Some(code)))
}

fn safe_next_action_for_run(run_dir: &Path) -> Result<String, String> {
    if active_stage_stall(run_dir)?.is_some() {
        return Ok("interrupt".to_string());
    }
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
    let last_attempt = incomplete_stage_attempt(run_dir)?;
    let active_stall = active_stage_stall(run_dir)?;

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
    if let Some(attempt) = &last_attempt {
        let fix = format!(
            "Inspect {}/runtime/process.log and the latest stage logs, then rerun `start {}` or `safe-next` after fixing the blocker.",
            run_dir.display(),
            attempt.stage
        );
        match attempt.status.as_str() {
            "interrupted" => warnings.push(DoctorIssue {
                severity: "warn".to_string(),
                message: format!(
                    "Latest `{}` attempt for stage `{}` was interrupted before the stage completed.",
                    attempt.label, attempt.stage
                ),
                fix,
            }),
            "completed" if attempt.exit_code == Some(0) => issues.push(DoctorIssue {
                severity: "error".to_string(),
                message: format!(
                    "Latest `{}` attempt reported success for stage `{}`, but the stage artifacts are still incomplete.",
                    attempt.label, attempt.stage
                ),
                fix,
            }),
            _ => issues.push(DoctorIssue {
                severity: "error".to_string(),
                message: format!(
                    "Latest `{}` attempt for stage `{}` ended with status `{}` before the stage completed.",
                    attempt.label, attempt.stage, attempt.status
                ),
                fix,
            }),
        }
    }
    if let Some(stall) = &active_stall {
        let fix = format!(
            "Interrupt the current `{}` stage, inspect {}/runtime/process.log and the latest `{}` logs, then retry the stage once the blocker is understood.",
            stall.stage,
            run_dir.display(),
            stall.stage
        );
        if stall.idle_secs >= ACTIVE_STAGE_STALL_BROKEN_SECS {
            issues.push(DoctorIssue {
                severity: "error".to_string(),
                message: format!(
                    "Current stage `{}` appears stuck: no new stage output for {}s while the tracked job is still alive (elapsed {}s).",
                    stall.stage, stall.idle_secs, stall.elapsed_secs
                ),
                fix,
            });
        } else {
            warnings.push(DoctorIssue {
                severity: "warn".to_string(),
                message: format!(
                    "Current stage `{}` has not produced new output for {}s (elapsed {}s) and may be stalled.",
                    stall.stage, stall.idle_secs, stall.elapsed_secs
                ),
                fix,
            });
        }
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
    let mcp_usage_records = read_mcp_usage_records(run_dir);
    let mut mcp_stage_mismatches = Vec::new();
    let mut mcp_stage_unexpected = Vec::new();
    for stage in available_stages(run_dir)? {
        let selected = selected_mcp_server_names_for_stage(&plan, run_dir, &stage);
        let record = mcp_usage_records.iter().find(|item| item.stage == stage);
        let prompt_path = run_dir.join("logs").join(format!("{stage}.prompt.md"));
        if !prompt_path.exists() && record.is_none() {
            continue;
        }
        let prompt_text = read_text(&prompt_path).unwrap_or_default();
        let note_text = record.map(|item| item.note_md.as_str()).unwrap_or("");
        let empty_prompt_assignment = prompt_artifact_shows_empty_mcp_assignment(&prompt_text);
        let explicit_none = note_text
            .to_ascii_lowercase()
            .contains("no mcp servers were selected");
        let prompt_selected = prompt_artifact_selected_mcp_names(&prompt_text);
        let note_selected = mcp_note_server_names(note_text);
        let expected: BTreeSet<String> = selected.iter().cloned().collect();
        let observed: BTreeSet<String> = prompt_selected
            .into_iter()
            .chain(note_selected.into_iter())
            .collect();
        if !selected.is_empty() && (empty_prompt_assignment || explicit_none) {
            mcp_stage_mismatches.push(stage);
            continue;
        }
        if !expected.is_empty() && !observed.is_empty() {
            let missing_expected = expected.iter().any(|name| !observed.contains(name));
            let unexpected_observed = observed.iter().any(|name| !expected.contains(name));
            if missing_expected || unexpected_observed {
                mcp_stage_mismatches.push(stage);
                continue;
            }
        }
        if expected.is_empty() && !observed.is_empty() {
            mcp_stage_unexpected.push(stage);
        }
    }
    if !mcp_stage_mismatches.is_empty() {
        warnings.push(DoctorIssue {
            severity: "warn".to_string(),
            message: format!(
                "Selected MCP servers did not propagate cleanly into stage artifacts for: {}.",
                mcp_stage_mismatches.join(", ")
            ),
            fix: "Refresh prompts or rerun the affected stages after regenerating the plan so stage MCP assignments match `references/mcp-plan.md`.".to_string(),
        });
    }
    if !mcp_stage_unexpected.is_empty() {
        warnings.push(DoctorIssue {
            severity: "warn".to_string(),
            message: format!(
                "Stage artifacts show unexpected MCP assignments that were not selected in the plan for: {}.",
                mcp_stage_unexpected.join(", ")
            ),
            fix: "Restart any stale TUI sessions, refresh prompts, and rerun the affected stages so live stage MCP assignments match `references/mcp-plan.md`.".to_string(),
        });
    }
    let health = if !issues.is_empty() {
        "broken"
    } else if !warnings.is_empty() {
        "warning"
    } else {
        "healthy"
    };
    let mut payload = DoctorPayload {
        run_dir: run_dir.display().to_string(),
        health: health.to_string(),
        stages: statuses,
        stale,
        host_probe: host_probe_label,
        host_drift,
        goal,
        next: next_stage_for_run(run_dir)?.unwrap_or_else(|| "none".to_string()),
        safe_next_action: safe_next_action_for_run(run_dir)?,
        fix_actions: Vec::new(),
        last_attempt,
        issues,
        warnings,
    };
    payload.fix_actions = doctor_fix_actions(&payload);
    Ok(payload)
}

fn doctor_fix_actions(report: &DoctorPayload) -> Vec<String> {
    let mut actions = Vec::new();
    let late_stage = matches!(report.next.trim(), "execution" | "verification" | "rerun")
        || report
            .last_attempt
            .as_ref()
            .map(|attempt| matches!(attempt.stage.trim(), "execution" | "verification"))
            .unwrap_or(false);
    if late_stage && (report.host_probe.trim() == "missing" || report.host_drift.is_some()) {
        actions.push("host-probe --refresh".to_string());
    }
    let safe = report.safe_next_action.trim();
    if !safe.is_empty() && safe != "none" {
        actions.push(safe.to_string());
    }
    let mut deduped = Vec::new();
    for action in actions {
        if !deduped.contains(&action) {
            deduped.push(action);
        }
    }
    deduped
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
        last_attempt: incomplete_stage_attempt(run_dir)?,
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
                let (preview_label, preview) = preview_text(&run_dir, 1600);
                let (log_title, log_lines) =
                    contextual_log_excerpt(&run_dir, None, Some(&status.next), 12);
                snapshots.push(RunSnapshot {
                    run_dir,
                    doctor,
                    status,
                    token_summary: RunTokenSummary::default(),
                    solver_stage_ids: Vec::new(),
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
    let solver_stage_ids = plan
        .pipeline
        .stages
        .iter()
        .filter(|stage| {
            matches!(
                stage.kind.trim().to_ascii_lowercase().as_str(),
                "solver" | "research" | "analysis" | "researcher"
            )
        })
        .map(|stage| stage.id.clone())
        .collect();
    let (preview_label, preview) = preview_text(run_dir, 1600);
    let (log_title, log_lines) = contextual_log_excerpt(run_dir, None, Some(&status.next), 12);
    Ok(RunSnapshot {
        run_dir: run_dir.to_path_buf(),
        doctor,
        status,
        token_summary,
        solver_stage_ids,
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
    discover_agency_agents_dir_for_repo_root(&ctx.repo_root)
}

fn compile_prompt(ctx: &Context, run_dir: &Path, stage: &str) -> Result<String, String> {
    let plan = load_plan(run_dir)?;
    let stage_kind = pipeline_stage_kind_for(&plan, run_dir, stage)?;
    let workspace = plan_workspace(&plan);
    let prompt_path = stage_prompt_path(run_dir, stage)?;
    let prompt_body = render_stage_prompt(ctx, run_dir, stage)?;
    write_text(&prompt_path, &prompt_body)?;
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
    ];
    match stage_kind {
        PipelineStageKind::Intake => {
            guidance.push(format!(
                "- Decomposition rules reference: `{}`",
                run_reference_asset_display_path(ctx, run_dir, DECOMPOSITION_RULES_REF)
            ));
            guidance.push(format!(
                "- Role map reference: `{}`",
                run_reference_asset_display_path(ctx, run_dir, ROLE_MAP_REF)
            ));
        }
        PipelineStageKind::Review => {
            guidance.push(format!(
                "- Review rubric reference: `{}`",
                run_reference_asset_display_path(ctx, run_dir, REVIEW_RUBRIC_REF)
            ));
        }
        PipelineStageKind::Verification => {
            guidance.push(format!(
                "- Verification rubric reference: `{}`",
                run_reference_asset_display_path(ctx, run_dir, VERIFICATION_RUBRIC_REF)
            ));
        }
        PipelineStageKind::Solver | PipelineStageKind::Execution => {}
    }
    if let Some(agency) = discover_agency_agents_dir(ctx) {
        guidance.push(format!("- Agency role catalog: `{}`", agency.display()));
    }

    let mut extra_rules = vec![
        if matches!(stage_kind, PipelineStageKind::Execution | PipelineStageKind::Intake) {
            "- Update the requested artifacts directly on disk.".to_string()
        } else {
            "- Do not edit run-local stage output files directly unless the stage prompt explicitly asks for it; prefer returning the final artifact or tagged bundle in your assistant message so the pipeline can materialize it.".to_string()
        },
        "- Use the primary workspace for repo inspection when it exists.".to_string(),
        "- If blocked, replace placeholders with a concrete blocker note instead of leaving them unchanged.".to_string(),
        "- Treat the stage prompt as self-contained; do not open generic workflow `SKILL.md` files, pipeline docs, or unrelated instructions unless they are explicitly listed in the read set or validation hints.".to_string(),
    ];
    if amendments_exist(run_dir) {
        extra_rules.push("- Treat `amendments.md` as the latest authoritative user input when it adds constraints, corrections, or newly clarified expected behavior.".to_string());
    }
    if stage_kind == PipelineStageKind::Solver {
        extra_rules.push("- Do not read sibling solver outputs.".to_string());
    }
    if stage_kind == PipelineStageKind::Intake {
        extra_rules.push("- Prefer direct synthesis from `request.md` plus the current `plan.json` defaults before opening extra files.".to_string());
        extra_rules.push("- Open only the explicitly listed reference assets or workspace files that materially change the brief.".to_string());
        extra_rules.push("- Do not inspect multi-agent-pipeline source files, tests, or SKILL docs unless the user task is explicitly about this pipeline.".to_string());
    }
    if stage_kind == PipelineStageKind::Execution {
        extra_rules.push("- Treat the latest host probe artifact from the launcher as authoritative local runtime evidence for this execution stage when discussing device availability and visible environment keys.".to_string());
        extra_rules.push("- When citing that probe in run-facing artifacts, label it as the execution-stage launcher probe with its timestamp; do not describe it as the authoritative probe for the whole run because later stages may refresh the launcher probe.".to_string());
    }
    if stage_kind == PipelineStageKind::Verification {
        extra_rules.push("- Treat the latest host probe artifact from the launcher as the authoritative run-level local runtime evidence for final device availability and visible environment keys.".to_string());
        extra_rules.push("- If earlier artifacts cite an older stage-local probe and the underlying host facts are unchanged, do not fail the run for timestamp drift alone; only flag it when the artifact claims final or run-global authority, or when the facts materially differ.".to_string());
    }

    let mut dynamic_context = Vec::new();
    let audit_improve_focus = plan.task_kind == "audit-improve"
        && matches!(
            stage_kind,
            PipelineStageKind::Intake | PipelineStageKind::Solver | PipelineStageKind::Review
        );
    if audit_improve_focus {
        dynamic_context.push("Audit-improve focus for this stage:".to_string());
        dynamic_context.push(
            "- Use the verification-seeded request as the authoritative target for the specific follow-up gap.".to_string(),
        );
        dynamic_context.push(
            "- Preserve already-green slices unless new local evidence contradicts them.".to_string(),
        );
        dynamic_context.push(
            "- Treat older implementation-oriented checks from the source run as historical context, not the primary target of this follow-up stage.".to_string(),
        );
        dynamic_context.push(
            "- Keep the work narrow enough to refresh current facts and close the named gap without reopening unrelated work.".to_string(),
        );
    } else if !plan.goal_checks.is_empty() {
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
    let include_runtime_harness =
        runtime_harness_expected_for_stage(&plan, run_dir, stage_kind);
    if include_runtime_harness {
        let spec_path = discover_service_check_spec_path(run_dir)
            .unwrap_or_else(|| service_check_default_spec_path(&plan, run_dir));
        dynamic_context.push("Runtime harness:".to_string());
        dynamic_context.push(format!("- `spec_path`: `{}`", spec_path.display()));
        dynamic_context.push(format!(
            "- `runner_command`: `{}`",
            service_check_validation_command(
                run_dir,
                if stage_kind == PipelineStageKind::Verification {
                    "verification"
                } else {
                    "execution"
                }
            )
        ));
        if stage_kind == PipelineStageKind::Execution {
            dynamic_context.push("- If this task produces a service, TUI, or GUI app, create or update the runtime-check spec before finishing execution.".to_string());
        }
        for phase in ["execution", "verification"] {
            let summary = service_check_summary_md_path(run_dir, phase);
            if summary.exists() {
                dynamic_context.push(format!(
                    "- `runtime_check_{phase}`: `{}`",
                    summary.display()
                ));
            }
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
            let role_docs = load_agency_role_docs(ctx, &[spec.role.clone()]);
            if !role_docs.is_empty() {
                dynamic_context.push("Resolved solver role docs:".to_string());
                dynamic_context.extend(role_docs.into_iter().map(|doc| {
                    format!(
                        "- `{}`: `{}` ({})",
                        doc.requested_role,
                        doc.full_path.display(),
                        if doc.description.is_empty() {
                            "no description".to_string()
                        } else {
                            doc.description
                        }
                    )
                }));
            }
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
        let reviewer_roles = if plan.reviewer_stack.is_empty() {
            default_reviewer_stack_for_plan(&plan)
        } else {
            plan.reviewer_stack.clone()
        };
        let reviewer_docs = load_agency_role_docs(ctx, &reviewer_roles);
        if !reviewer_docs.is_empty() {
            dynamic_context.push("Resolved reviewer role docs:".to_string());
            dynamic_context.extend(reviewer_docs.into_iter().map(|doc| {
                format!(
                    "- `{}`: `{}` ({})",
                    doc.requested_role,
                    doc.full_path.display(),
                    if doc.description.is_empty() {
                        "no description".to_string()
                    } else {
                        doc.description
                    }
                )
            }));
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
        "You are executing workflow stage `{}` for a file-based run.\n\nExecution context:\n{}\n\n{}Required output files:\n{}\n\nGlobal rules:\n{}\n\nStage prompt:\n\n{}\n",
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

fn detect_local_intake_template(
    plan: &Plan,
    stage_kind: PipelineStageKind,
) -> Option<LocalTemplateKind> {
    if stage_kind != PipelineStageKind::Intake {
        return None;
    }
    if plan.task_kind != "backend" {
        return None;
    }
    if !task_requests_cli_entrypoint(&plan.task_kind, &plan.original_task) {
        return None;
    }
    if !task_requests_precise_local_contract(&plan.original_task) {
        return None;
    }
    if !stack_signals_are_empty(&plan.stack_signals) {
        return None;
    }
    if !workspace_looks_greenfield(&plan_workspace(plan)) {
        return None;
    }
    Some(LocalTemplateKind::ExecutionReadyBackendCliIntake)
}

fn local_cli_artifact_summary(task: &str) -> Vec<String> {
    let text = task.to_lowercase();
    let mut items = Vec::new();
    if text_contains_signal(&text, "main.py") {
        items.push("`main.py` as the primary runnable entrypoint".to_string());
    }
    if task_requests_readme(task) {
        items.push("`README.md` with the exact local run command and expected result".to_string());
    }
    if items.is_empty() {
        items.push(
            "a local runnable CLI or script entrypoint plus concise run documentation".to_string(),
        );
    }
    items
}

fn render_execution_ready_backend_cli_brief(plan: &Plan) -> String {
    let workspace = plan_workspace(plan);
    let workstreams: Vec<String> = if plan.workstream_hints.is_empty() {
        vec!["implement the requested local entrypoint and lock the local run contract".to_string()]
    } else {
        plan.workstream_hints
            .iter()
            .map(|hint| format!("`{}`: {}", hint.name, hint.goal))
            .collect()
    };
    let deliverables = local_cli_artifact_summary(&plan.original_task);
    let goal_matrix: Vec<String> = if plan.goal_checks.is_empty() {
        vec!["capture the requested runnable behavior in a verifiable local artifact".to_string()]
    } else {
        plan.goal_checks
            .iter()
            .map(|item| {
                format!(
                    "`{}` `{}`: {}",
                    if item.critical {
                        "critical"
                    } else {
                        "supporting"
                    },
                    item.id,
                    item.requirement
                )
            })
            .collect()
    };
    let done_checks: Vec<String> = if plan.goal_checks.is_empty() {
        vec!["the requested local CLI behavior is implemented and documented".to_string()]
    } else {
        plan.goal_checks
            .iter()
            .filter(|item| item.critical)
            .map(|item| item.requirement.clone())
            .collect()
    };
    format!(
        "# Brief\n\n## Original requested outcome\n{}\n\n## Objective\nImplement the requested local CLI or script behavior in the primary workspace without narrowing the named artifacts or run/output contract.\n\n## Deliverable\n{}\n\n## Goal coverage matrix\n{}\n\n## Workstream decomposition\n{}\n\n## Scope\n- primary workspace: `{}`\n- operate as a local backend CLI/script task\n- preserve any explicitly named artifact paths and commands from the request\n\n## Constraints\n- do not silently narrow the requested deliverable\n- keep the primary local run path deterministic when exact stdout is requested\n- preserve the exact local run command and output contract in the implementation and docs\n\n## Definition of done\n{}\n\n## Validation expectations\n{}\n",
        plan.original_task.trim(),
        bullet_list(deliverables.iter().map(|item| item.as_str())),
        bullet_list(goal_matrix.iter().map(|item| item.as_str())),
        bullet_list(workstreams.iter().map(|item| item.as_str())),
        workspace.display(),
        bullet_list(done_checks.iter().map(|item| item.as_str())),
        bullet_list(plan.validation_commands.iter().map(|item| item.as_str()))
    )
}

fn stage_backend_kind(
    ctx: &Context,
    run_dir: &Path,
    stage: &str,
) -> Result<StageBackendKind, String> {
    let plan = load_plan(run_dir)?;
    let kind = pipeline_stage_kind_for(&plan, run_dir, stage)?;
    if let Some(template) = detect_local_intake_template(&plan, kind) {
        return Ok(StageBackendKind::LocalTemplate(template));
    }
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

fn reference_asset_display_path(ctx: &Context, relative: &str) -> String {
    let path = ctx.repo_root.join(relative);
    if path.exists() {
        path.display().to_string()
    } else {
        relative.to_string()
    }
}

fn run_reference_asset_path(run_dir: &Path, relative: &str) -> PathBuf {
    let file_name = Path::new(relative)
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or(relative);
    run_dir.join("references").join(file_name)
}

fn run_reference_asset_display_path(ctx: &Context, run_dir: &Path, relative: &str) -> String {
    let run_local = run_reference_asset_path(run_dir, relative);
    if run_local.exists() {
        run_local.display().to_string()
    } else {
        reference_asset_display_path(ctx, relative)
    }
}

fn seed_run_reference_assets(ctx: &Context, run_dir: &Path) -> Result<(), String> {
    let references_dir = run_dir.join("references");
    fs::create_dir_all(&references_dir)
        .map_err(|err| format!("Could not create {}: {err}", references_dir.display()))?;
    for relative in REFERENCE_ASSET_RELS {
        let asset = read_reference_asset(ctx, relative)?;
        write_text(&run_reference_asset_path(run_dir, relative), &asset)?;
    }
    Ok(())
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
            docs.push(("plan.json".to_string(), read_plan_artifact_text(run_dir)?));
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
            if let Some(catalog) = agency_role_catalog_summary(ctx) {
                docs.push(("agency-role-catalog.md".to_string(), catalog));
            }
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
            docs.push(("plan.json".to_string(), read_plan_artifact_text(run_dir)?));
            if let Ok(spec) = pipeline_stage_spec(&plan, run_dir, stage) {
                for doc in load_agency_role_docs(ctx, &[spec.role]) {
                    docs.push((format!("agency-role/{}", doc.relative_path), doc.content));
                }
            }
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
            docs.push(("plan.json".to_string(), read_plan_artifact_text(run_dir)?));
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
            let reviewer_roles = if plan.reviewer_stack.is_empty() {
                default_reviewer_stack_for_plan(&plan)
            } else {
                plan.reviewer_stack.clone()
            };
            for doc in load_agency_role_docs(ctx, &reviewer_roles) {
                docs.push((format!("agency-role/{}", doc.relative_path), doc.content));
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
            docs.push(("plan.json".to_string(), read_plan_artifact_text(run_dir)?));
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
            if let Some(spec) = discover_service_check_spec_path(run_dir) {
                docs.push(("runtime-check/spec.json".to_string(), read_text(&spec)?));
            }
            let summary = service_check_summary_md_path(run_dir, "execution");
            if summary.exists() {
                docs.push((
                    "runtime/runtime-check/execution/summary.md".to_string(),
                    read_text(&summary)?,
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
            docs.push(("plan.json".to_string(), read_plan_artifact_text(run_dir)?));
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
            if let Some(spec) = discover_service_check_spec_path(run_dir) {
                docs.push(("runtime-check/spec.json".to_string(), read_text(&spec)?));
            }
            for phase in ["execution", "verification"] {
                let summary = service_check_summary_md_path(run_dir, phase);
                if summary.exists() {
                    docs.push((
                        format!("runtime/runtime-check/{phase}/summary.md"),
                        read_text(&summary)?,
                    ));
                }
            }
            docs.push((
                "verification-rubric.md".to_string(),
                read_reference_asset(ctx, VERIFICATION_RUBRIC_REF)?,
            ));
        }
    }
    let mcp_plan = mcp_reference_path(run_dir);
    if mcp_plan.exists() {
        docs.push(("mcp-plan.md".to_string(), read_text(&mcp_plan)?));
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
    candidates.sort_by_cached_key(|path| (std::cmp::Reverse(file_mtime(path)), path.clone()));
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
            "required": ["brief_md", "plan_json", "mcp_usage_md"],
            "properties": {
                "brief_md": {"type": "string"},
                "plan_json": {
                    "type": "object",
                    "additionalProperties": true
                },
                "mcp_usage_md": {"type": "string"}
            }
        }),
        value if value == "solver" || value.starts_with("solver-") => json!({
            "type": "object",
            "additionalProperties": false,
            "required": ["result_md", "mcp_usage_md"],
            "properties": {
                "result_md": {"type": "string"},
                "mcp_usage_md": {"type": "string"}
            }
        }),
        "review" => json!({
            "type": "object",
            "additionalProperties": false,
            "required": ["report_md", "scorecard_json", "user_summary_md", "mcp_usage_md"],
            "properties": {
                "report_md": {"type": "string"},
                "scorecard_json": {
                    "type": "object",
                    "additionalProperties": true
                },
                "user_summary_md": {"type": "string"},
                "mcp_usage_md": {"type": "string"}
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
                "goal_status_json",
                "mcp_usage_md"
            ],
            "properties": {
                "findings_md": {"type": "string"},
                "user_summary_md": {"type": "string"},
                "improvement_request_md": {"type": "string"},
                "augmented_task_md": {"type": "string"},
                "goal_status_json": {
                    "type": "object",
                    "additionalProperties": true
                },
                "mcp_usage_md": {"type": "string"}
            }
        }),
        _ => return None,
    };
    Some(ResponseTextFormat {
        name: format!("agpipe_{}_v2", slugify(label)),
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
        PipelineStageKind::Intake => "Return the complete intake payload through the configured structured output schema.\n\nRules:\n- keep `plan_json` compatible with the existing agpipe Plan schema\n- preserve cache config, host facts, and existing defaults unless the prompt gives a clear reason to change them\n- make sure `summary_language`, `stage_research_mode`, `execution_network_mode`, `goal_checks`, `solver_roles`, and `pipeline` remain coherent\n- set `mcp_usage_md` to a short markdown note covering which selected MCP servers were used, not used, or unavailable".to_string(),
        PipelineStageKind::Solver => "Return the complete solver payload through the configured structured output schema.\n\nRules:\n- include assumptions, approach, implementation plan, validation, and unresolved risks\n- keep the full requested goal explicit\n- do not modify the primary workspace in solver stage; describe proposed file-level changes in `result_md`\n- stay centered on the assigned workstream or angle instead of repeating other solvers\n- set `mcp_usage_md` to a short markdown note covering which selected MCP servers were used, not used, or unavailable".to_string(),
        PipelineStageKind::Review => format!(
            "Return the complete review payload through the configured structured output schema.\n\nRules:\n- choose a winner or explicit hybrid in `scorecard_json`\n- include `winner`, `backup`, `why`, `validation_evidence`, `critical_gaps`, and `execution_notes` in `scorecard_json`\n- if solver outputs are materially identical, say so explicitly and prefer the one with clearer evidence instead of inventing a hybrid\n- set `mcp_usage_md` to a short markdown note covering which selected MCP servers were used, not used, or unavailable\n- write `user_summary_md` in {}\n- {}",
            plan.summary_language,
            localized_user_facing_style_note(&plan.summary_language)
        ),
        PipelineStageKind::Execution => {
            return Err("Responses backend is not implemented for execution stages.".to_string())
        }
        PipelineStageKind::Verification => format!(
            "Return the complete verification payload through the configured structured output schema.\n\nRules:\n- set `goal_status_json.goal_complete=false` when any critical goal check is missing or unverified\n- include `goal_verdict`, `rerun_recommended`, and `recommended_next_action` in `goal_status_json`\n- call out stale or contradictory artifacts, including workspace changes that bypassed the intended execution path\n- set `mcp_usage_md` to a short markdown note covering which selected MCP servers were used, not used, or unavailable\n- write `user_summary_md` in {}\n- {}",
            plan.summary_language,
            localized_user_facing_style_note(&plan.summary_language)
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
            if payload.brief_md.trim().is_empty() || payload.mcp_usage_md.trim().is_empty() {
                return Err(
                    "Responses intake output is missing `brief_md` or `mcp_usage_md`.".to_string(),
                );
            }
            let mut plan: Plan = serde_json::from_value(payload.plan_json)
                .map_err(|err| format!("Could not parse intake `plan_json`: {err}"))?;
            ensure_plan_mcp_defaults(&mut plan);
            ensure_reviewer_stack(&mut plan);
            write_text(&run_dir.join("brief.md"), payload.brief_md.trim_end())?;
            persist_stage_mcp_note(run_dir, stage, &payload.mcp_usage_md)?;
            save_plan(run_dir, &plan)?;
        }
        PipelineStageKind::Solver => {
            let payload: ResponsesSolverPayload = parse_structured_json_output(raw_output)?;
            if payload.result_md.trim().is_empty() || payload.mcp_usage_md.trim().is_empty() {
                return Err(format!(
                    "Responses output for `{stage}` is missing `result_md` or `mcp_usage_md`."
                ));
            }
            write_text(
                &run_dir.join("solutions").join(stage).join("RESULT.md"),
                payload.result_md.trim_end(),
            )?;
            persist_stage_mcp_note(run_dir, stage, &payload.mcp_usage_md)?;
        }
        PipelineStageKind::Review => {
            let payload: ResponsesReviewPayload = parse_structured_json_output(raw_output)?;
            if payload.report_md.trim().is_empty()
                || payload.user_summary_md.trim().is_empty()
                || payload.mcp_usage_md.trim().is_empty()
                || !payload.scorecard_json.is_object()
            {
                return Err(
                    "Responses review output is missing required fields including `mcp_usage_md`."
                        .to_string(),
                );
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
            persist_stage_mcp_note(run_dir, stage, &payload.mcp_usage_md)?;
        }
        PipelineStageKind::Execution => {
            return Err("Responses backend does not persist execution stages.".to_string())
        }
        PipelineStageKind::Verification => {
            let payload: ResponsesVerificationPayload = parse_structured_json_output(raw_output)?;
            if payload.findings_md.trim().is_empty()
                || payload.user_summary_md.trim().is_empty()
                || payload.improvement_request_md.trim().is_empty()
                || payload.mcp_usage_md.trim().is_empty()
            {
                return Err(
                    "Responses verification output is missing required markdown fields including `mcp_usage_md`."
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
            persist_stage_mcp_note(run_dir, stage, &payload.mcp_usage_md)?;
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

fn service_check_runtime_dir(run_dir: &Path, phase: &str) -> PathBuf {
    crate::runtime::runtime_dir(run_dir)
        .join("runtime-check")
        .join(sanitize_artifact_label(phase))
}

fn service_check_summary_json_path(run_dir: &Path, phase: &str) -> PathBuf {
    service_check_runtime_dir(run_dir, phase).join("summary.json")
}

fn service_check_summary_md_path(run_dir: &Path, phase: &str) -> PathBuf {
    service_check_runtime_dir(run_dir, phase).join("summary.md")
}

fn relative_to_run_dir(run_dir: &Path, path: &Path) -> String {
    path.strip_prefix(run_dir)
        .ok()
        .map(|value| value.display().to_string())
        .unwrap_or_else(|| path.display().to_string())
}

fn sanitize_artifact_label(value: &str) -> String {
    let mut out = String::new();
    let mut last_dash = false;
    for ch in value.chars() {
        let mapped = if ch.is_ascii_alphanumeric() {
            ch.to_ascii_lowercase()
        } else {
            '-'
        };
        if mapped == '-' {
            if !out.is_empty() && !last_dash {
                out.push('-');
            }
            last_dash = true;
        } else {
            out.push(mapped);
            last_dash = false;
        }
    }
    let trimmed = out.trim_matches('-');
    if trimmed.is_empty() {
        "item".to_string()
    } else {
        trimmed.to_string()
    }
}

fn task_looks_like_service(plan: &Plan) -> bool {
    if task_is_analysis_only(&plan.original_task)
        && !task_requests_workspace_changes(&plan.original_task)
    {
        return false;
    }
    let lower = plan.original_task.to_ascii_lowercase();
    [
        "service",
        "server",
        "api",
        "http",
        "grpc",
        "daemon",
        "worker",
        "webhook",
        "microservice",
        "terminal ui",
        "cli app",
        "interactive terminal",
        "desktop app",
        "desktop application",
        "electron",
        "gtk",
        "qt",
        "swiftui",
        "appkit",
        "сервис",
        "сервер",
        "апи",
        "интерактивный терминал",
        "терминальный интерфейс",
        "десктоп",
        "настольное приложение",
        "графический интерфейс",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

fn normalize_service_check_mode(mode: &str) -> String {
    match mode.trim().to_ascii_lowercase().as_str() {
        "docker" | "docker-compose" | "docker_compose" | "compose" => "docker-compose".to_string(),
        "pty" => "pty".to_string(),
        "workflow" | "scenario" => "workflow".to_string(),
        _ => "process".to_string(),
    }
}

fn service_check_default_spec_path(plan: &Plan, run_dir: &Path) -> PathBuf {
    working_root(plan, run_dir).join(RUNTIME_CHECK_SPEC_REF)
}

fn service_check_candidate_paths(workspace: &Path) -> Vec<PathBuf> {
    vec![
        workspace.join(RUNTIME_CHECK_SPEC_REF),
        workspace.join(RUNTIME_CHECK_LEGACY_SPEC_REF),
        workspace.join(SERVICE_CHECK_SPEC_REF),
        workspace.join(SERVICE_CHECK_LEGACY_SPEC_REF),
    ]
}

fn discover_service_check_spec_path(run_dir: &Path) -> Option<PathBuf> {
    let plan = load_plan(run_dir).ok()?;
    let workspace = working_root(&plan, run_dir);
    service_check_candidate_paths(&workspace)
        .into_iter()
        .find(|path| path.exists())
}

fn service_check_validation_command(run_dir: &Path, phase: &str) -> String {
    let agpipe = env::current_exe()
        .ok()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| "agpipe".to_string());
    format!(
        "{} internal runtime-check {} --phase {}",
        shell_quote(&agpipe),
        shell_quote(&run_dir.display().to_string()),
        shell_quote(phase)
    )
}

fn runtime_harness_expected_for_stage(
    plan: &Plan,
    run_dir: &Path,
    _stage_kind: PipelineStageKind,
) -> bool {
    let has_spec = discover_service_check_spec_path(run_dir).is_some();
    if has_spec {
        return true;
    }
    if !task_looks_like_service(plan) {
        return false;
    }
    let has_execution_stage = first_stage_id_for_kind(plan, run_dir, PipelineStageKind::Execution)
        .ok()
        .flatten()
        .is_some();
    has_execution_stage
}

fn effective_validation_commands(run_dir: &Path, plan: &Plan, phase: &str) -> Vec<String> {
    let mut commands = plan.validation_commands.clone();
    let stage_kind = match phase {
        "execution" => Some(PipelineStageKind::Execution),
        "verification" => Some(PipelineStageKind::Verification),
        _ => None,
    };
    if stage_kind
        .map(|kind| runtime_harness_expected_for_stage(plan, run_dir, kind))
        .unwrap_or_else(|| discover_service_check_spec_path(run_dir).is_some())
    {
        commands.push(service_check_validation_command(run_dir, phase));
    }
    let mut deduped = Vec::new();
    for command in commands {
        if !deduped.contains(&command) {
            deduped.push(command);
        }
    }
    deduped
}

fn runtime_check_required(run_dir: &Path, phase: &str) -> Result<bool, String> {
    if !matches!(phase, "execution" | "verification") {
        return Ok(false);
    }
    let plan = load_plan(run_dir)?;
    let stage_kind = if phase == "execution" {
        PipelineStageKind::Execution
    } else {
        PipelineStageKind::Verification
    };
    Ok(runtime_harness_expected_for_stage(&plan, run_dir, stage_kind))
}

fn load_service_check_spec(
    run_dir: &Path,
    explicit_spec: Option<&Path>,
) -> Result<Option<(PathBuf, PathBuf, ServiceCheckSpec)>, String> {
    let plan = load_plan(run_dir)?;
    let workspace = working_root(&plan, run_dir);
    let spec_path = if let Some(path) = explicit_spec {
        if path.is_absolute() {
            path.to_path_buf()
        } else {
            workspace.join(path)
        }
    } else if let Some(path) = discover_service_check_spec_path(run_dir) {
        path
    } else {
        return Ok(None);
    };
    let text = read_text(&spec_path)?;
    let mut spec: ServiceCheckSpec = serde_json::from_str(&text)
        .map_err(|err| format!("Could not parse {}: {err}", spec_path.display()))?;
    if spec.version == 0 {
        spec.version = 1;
    }
    spec.mode = normalize_service_check_mode(&spec.mode);
    if spec.ready_timeout_secs == 0 {
        spec.ready_timeout_secs = 60;
    }
    if spec.ready_interval_ms == 0 {
        spec.ready_interval_ms = 500;
    }
    if spec.scenarios.is_empty() {
        return Err(format!(
            "Runtime check spec {} must contain at least one scenario.",
            spec_path.display()
        ));
    }
    if spec.mode == "process" && spec.start_command.trim().is_empty() {
        return Err(format!(
            "Runtime check spec {} is missing `start_command` for process mode.",
            spec_path.display()
        ));
    }
    if spec.mode == "docker-compose" && spec.compose_file.trim().is_empty() {
        return Err(format!(
            "Runtime check spec {} is missing `compose_file` for docker-compose mode.",
            spec_path.display()
        ));
    }
    for (index, scenario) in spec.scenarios.iter_mut().enumerate() {
        if scenario.id.trim().is_empty() {
            scenario.id = format!("scenario-{}", index + 1);
        }
        scenario.kind = scenario.kind.trim().to_ascii_lowercase();
        if scenario.kind.is_empty() {
            scenario.kind = if !scenario.steps.is_empty() {
                "workflow".to_string()
            } else if !scenario.url.trim().is_empty() {
                "http".to_string()
            } else {
                "command".to_string()
            };
        }
        if scenario.expect_status == 0 && matches!(scenario.kind.as_str(), "http" | "rest") {
            scenario.expect_status = 200;
        }
        if scenario.rows == 0 {
            scenario.rows = 40;
        }
        if scenario.cols == 0 {
            scenario.cols = 120;
        }
        match scenario.kind.as_str() {
            "command" | "shell" | "gui-command" | "pty" => {
                if scenario.command.trim().is_empty() {
                    return Err(format!(
                        "Runtime check spec {} has an empty command for scenario `{}`.",
                        spec_path.display(),
                        scenario.id
                    ));
                }
            }
            "http" | "rest" => {
                if scenario.url.trim().is_empty() {
                    return Err(format!(
                        "Runtime check spec {} is missing `url` for scenario `{}`.",
                        spec_path.display(),
                        scenario.id
                    ));
                }
            }
            "workflow" => {
                if scenario.steps.is_empty() {
                    return Err(format!(
                        "Runtime check spec {} has no `steps` for workflow scenario `{}`.",
                        spec_path.display(),
                        scenario.id
                    ));
                }
            }
            other => {
                return Err(format!(
                    "Runtime check spec {} uses unsupported scenario kind `{}` for `{}`.",
                    spec_path.display(),
                    other,
                    scenario.id
                ));
            }
        }
        for (step_index, step) in scenario.steps.iter_mut().enumerate() {
            step.kind = step.kind.trim().to_ascii_lowercase();
            if step.expect_status == 0 && matches!(step.kind.as_str(), "http" | "rest") {
                step.expect_status = 200;
            }
            match step.kind.as_str() {
                "wait" | "wait_ms" | "sleep" | "pty_resize" => {}
                "command" | "shell" | "gui-command" | "pty_start" => {
                    if step.command.trim().is_empty() {
                        return Err(format!(
                            "Runtime check spec {} has an empty command for step {} in scenario `{}`.",
                            spec_path.display(),
                            step_index + 1,
                            scenario.id
                        ));
                    }
                }
                "http" | "rest" => {
                    if step.url.trim().is_empty() {
                        return Err(format!(
                            "Runtime check spec {} is missing `url` for step {} in scenario `{}`.",
                            spec_path.display(),
                            step_index + 1,
                            scenario.id
                        ));
                    }
                }
                "file_contains" => {
                    if step.path.trim().is_empty() || step.pattern.trim().is_empty() {
                        return Err(format!(
                            "Runtime check spec {} requires both `path` and `pattern` for file_contains step {} in scenario `{}`.",
                            spec_path.display(),
                            step_index + 1,
                            scenario.id
                        ));
                    }
                }
                "pty_send_text" => {
                    if step.text.is_empty() {
                        return Err(format!(
                            "Runtime check spec {} has an empty `text` for pty_send_text step {} in scenario `{}`.",
                            spec_path.display(),
                            step_index + 1,
                            scenario.id
                        ));
                    }
                }
                "pty_send_keys" => {
                    if step.keys.is_empty() {
                        return Err(format!(
                            "Runtime check spec {} requires at least one key for pty_send_keys step {} in scenario `{}`.",
                            spec_path.display(),
                            step_index + 1,
                            scenario.id
                        ));
                    }
                }
                "pty_wait_contains" | "pty_assert_contains" => {
                    if step.pattern.trim().is_empty() {
                        return Err(format!(
                            "Runtime check spec {} has an empty `pattern` for step {} in scenario `{}`.",
                            spec_path.display(),
                            step_index + 1,
                            scenario.id
                        ));
                    }
                }
                "" => {
                    return Err(format!(
                        "Runtime check spec {} is missing `kind` for step {} in scenario `{}`.",
                        spec_path.display(),
                        step_index + 1,
                        scenario.id
                    ));
                }
                other => {
                    return Err(format!(
                        "Runtime check spec {} uses unsupported step kind `{}` for step {} in scenario `{}`.",
                        spec_path.display(),
                        other,
                        step_index + 1,
                        scenario.id
                    ));
                }
            }
        }
    }
    let workdir = if spec.workdir.trim().is_empty() {
        workspace
    } else {
        let configured = PathBuf::from(spec.workdir.trim());
        if configured.is_absolute() {
            configured
        } else {
            workspace.join(configured)
        }
    };
    Ok(Some((spec_path, workdir, spec)))
}

fn service_check_command_result(
    command: &str,
    cwd: &Path,
    envs: &BTreeMap<String, String>,
) -> Result<CommandResult, String> {
    let output = Command::new("/bin/sh")
        .arg("-lc")
        .arg(command)
        .current_dir(cwd)
        .envs(envs)
        .output()
        .map_err(|err| format!("Could not run `{command}` in {}: {err}", cwd.display()))?;
    Ok(CommandResult {
        code: output.status.code().unwrap_or(1),
        stdout: String::from_utf8_lossy(&output.stdout).to_string(),
        stderr: String::from_utf8_lossy(&output.stderr).to_string(),
    })
}

fn write_command_result_logs(
    stdout_path: &Path,
    stderr_path: &Path,
    result: &CommandResult,
) -> Result<(), String> {
    write_text(stdout_path, &result.stdout)?;
    write_text(stderr_path, &result.stderr)?;
    Ok(())
}

#[derive(Debug, Clone, Default)]
struct RuntimeHttpResponse {
    status: u32,
    body: String,
    headers: BTreeMap<String, String>,
}

struct PtyScenarioSession {
    master: Box<dyn portable_pty::MasterPty + Send>,
    child: Box<dyn portable_pty::Child + Send + Sync>,
    writer: Box<dyn Write + Send>,
    rx: mpsc::Receiver<Vec<u8>>,
    parser: vt100::Parser,
    reader_handle: Option<thread::JoinHandle<()>>,
}

impl PtyScenarioSession {
    fn start(
        command: &str,
        cwd: &Path,
        envs: &BTreeMap<String, String>,
        rows: u16,
        cols: u16,
        transcript_path: &Path,
    ) -> Result<Self, String> {
        let pty_system = native_pty_system();
        let pair = pty_system
            .openpty(PtySize {
                rows,
                cols,
                pixel_width: 0,
                pixel_height: 0,
            })
            .map_err(|err| format!("Could not open PTY: {err}"))?;
        let mut cmd = CommandBuilder::new("/bin/sh");
        cmd.arg("-lc");
        cmd.arg(command);
        cmd.cwd(cwd);
        for (key, value) in envs {
            cmd.env(key, value);
        }
        let child = pair
            .slave
            .spawn_command(cmd)
            .map_err(|err| format!("Could not start PTY command `{command}`: {err}"))?;
        drop(pair.slave);
        let reader = pair
            .master
            .try_clone_reader()
            .map_err(|err| format!("Could not clone PTY reader: {err}"))?;
        let writer = pair
            .master
            .take_writer()
            .map_err(|err| format!("Could not take PTY writer: {err}"))?;
        let (rx, handle) = spawn_pty_reader(reader, transcript_path.to_path_buf());
        Ok(Self {
            master: pair.master,
            child,
            writer,
            rx,
            parser: vt100::Parser::new(rows, cols, 0),
            reader_handle: Some(handle),
        })
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

    fn send_bytes(&mut self, bytes: &[u8]) -> Result<(), String> {
        self.writer
            .write_all(bytes)
            .and_then(|_| self.writer.flush())
            .map_err(|err| format!("Could not write to PTY: {err}"))
    }

    fn send_text(&mut self, text: &str) -> Result<(), String> {
        for ch in text.bytes() {
            self.send_bytes(&[ch])?;
            thread::sleep(Duration::from_millis(20));
        }
        Ok(())
    }

    fn wait_contains(&mut self, needle: &str, timeout: Duration) -> Result<(), String> {
        let deadline = Instant::now() + timeout;
        loop {
            let screen = self.screen();
            if screen.contains(needle) {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "Timed out waiting for PTY screen fragment `{needle}`.\nCurrent screen:\n{screen}"
                ));
            }
            match self.rx.recv_timeout(Duration::from_millis(100)) {
                Ok(bytes) => self.parser.process(&bytes),
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    return Err(format!(
                        "PTY disconnected while waiting for `{needle}`.\nCurrent screen:\n{}",
                        self.screen()
                    ))
                }
            }
        }
    }

    fn resize(&mut self, rows: u16, cols: u16) -> Result<(), String> {
        self.master
            .resize(PtySize {
                rows,
                cols,
                pixel_width: 0,
                pixel_height: 0,
            })
            .map_err(|err| format!("Could not resize PTY: {err}"))?;
        self.parser.set_size(rows, cols);
        Ok(())
    }

    fn stop(mut self) -> Result<i32, String> {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            match self.child.try_wait() {
                Ok(Some(status)) => {
                    if let Some(handle) = self.reader_handle.take() {
                        let _ = handle.join();
                    }
                    return Ok(status.exit_code() as i32);
                }
                Ok(None) if Instant::now() < deadline => {
                    thread::sleep(Duration::from_millis(50));
                }
                Ok(None) | Err(_) => {
                    let _ = self.child.kill();
                    let status = self
                        .child
                        .wait()
                        .map_err(|err| format!("Could not wait for PTY child: {err}"))?;
                    if let Some(handle) = self.reader_handle.take() {
                        let _ = handle.join();
                    }
                    return Ok(status.exit_code() as i32);
                }
            }
        }
    }
}

fn spawn_pty_reader(
    mut reader: Box<dyn Read + Send>,
    transcript_path: PathBuf,
) -> (mpsc::Receiver<Vec<u8>>, thread::JoinHandle<()>) {
    let (tx, rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        let mut transcript = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&transcript_path)
            .ok();
        let mut buf = [0u8; 4096];
        loop {
            match reader.read(&mut buf) {
                Ok(0) => break,
                Ok(size) => {
                    if let Some(file) = transcript.as_mut() {
                        let _ = file.write_all(&buf[..size]);
                    }
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

fn runtime_http_request(
    method: &str,
    url: &str,
    headers: &BTreeMap<String, String>,
    body: Option<&str>,
) -> Result<RuntimeHttpResponse, String> {
    let mut easy = Easy::new();
    easy.url(url)
        .map_err(|err| format!("Could not configure URL `{url}`: {err}"))?;
    easy.follow_location(true)
        .map_err(|err| format!("Could not enable redirects for `{url}`: {err}"))?;
    easy.connect_timeout(Duration::from_secs(5))
        .map_err(|err| format!("Could not set connect timeout: {err}"))?;
    easy.timeout(Duration::from_secs(30))
        .map_err(|err| format!("Could not set request timeout: {err}"))?;
    let upper_method = method.trim().to_ascii_uppercase();
    match upper_method.as_str() {
        "GET" => {}
        "POST" => {
            easy.post(true)
                .map_err(|err| format!("Could not configure POST `{url}`: {err}"))?;
            if let Some(body) = body {
                easy.post_fields_copy(body.as_bytes())
                    .map_err(|err| format!("Could not set POST body for `{url}`: {err}"))?;
            }
        }
        other => {
            easy.custom_request(other)
                .map_err(|err| format!("Could not configure HTTP method `{other}`: {err}"))?;
            if let Some(body) = body {
                easy.post_fields_copy(body.as_bytes())
                    .map_err(|err| format!("Could not set request body for `{url}`: {err}"))?;
            }
        }
    }
    if !headers.is_empty() {
        let mut list = List::new();
        for (key, value) in headers {
            list.append(&format!("{key}: {value}"))
                .map_err(|err| format!("Could not append HTTP header `{key}`: {err}"))?;
        }
        easy.http_headers(list)
            .map_err(|err| format!("Could not attach HTTP headers: {err}"))?;
    }
    let mut response_body = Vec::new();
    let mut response_headers = BTreeMap::new();
    {
        let mut transfer = easy.transfer();
        transfer
            .write_function(|data| {
                response_body.extend_from_slice(data);
                Ok(data.len())
            })
            .map_err(|err| format!("Could not capture HTTP body: {err}"))?;
        transfer
            .header_function(|header| {
                let line = String::from_utf8_lossy(header).trim().to_string();
                if let Some((key, value)) = line.split_once(':') {
                    response_headers
                        .insert(key.trim().to_ascii_lowercase(), value.trim().to_string());
                }
                true
            })
            .map_err(|err| format!("Could not capture HTTP headers: {err}"))?;
        transfer
            .perform()
            .map_err(|err| format!("HTTP request `{upper_method} {url}` failed: {err}"))?;
    }
    let status = easy
        .response_code()
        .map_err(|err| format!("Could not read HTTP status for `{url}`: {err}"))?;
    Ok(RuntimeHttpResponse {
        status,
        body: String::from_utf8_lossy(&response_body).to_string(),
        headers: response_headers,
    })
}

fn apply_command_expectations(
    result: &CommandResult,
    expect_exit_code: i32,
    expect_stdout_contains: &[String],
    expect_stderr_contains: &[String],
) -> String {
    if result.code != expect_exit_code {
        return format!("expected exit code {expect_exit_code}, got {}", result.code);
    }
    for needle in expect_stdout_contains {
        if !result.stdout.contains(needle) {
            return format!("stdout is missing expected fragment `{needle}`");
        }
    }
    for needle in expect_stderr_contains {
        if !result.stderr.contains(needle) {
            return format!("stderr is missing expected fragment `{needle}`");
        }
    }
    String::new()
}

fn apply_http_expectations(
    response: &RuntimeHttpResponse,
    expect_status: u32,
    expect_body_contains: &[String],
    expect_body_not_contains: &[String],
) -> String {
    if expect_status > 0 && response.status != expect_status {
        return format!(
            "expected HTTP status {expect_status}, got {}",
            response.status
        );
    }
    for needle in expect_body_contains {
        if !response.body.contains(needle) {
            return format!("response body is missing expected fragment `{needle}`");
        }
    }
    for needle in expect_body_not_contains {
        if response.body.contains(needle) {
            return format!("response body unexpectedly contains `{needle}`");
        }
    }
    String::new()
}

fn encode_key_name(name: &str) -> Result<Vec<u8>, String> {
    let normalized = name.trim().to_ascii_lowercase();
    let bytes = match normalized.as_str() {
        "enter" | "return" => b"\r".to_vec(),
        "esc" | "escape" => vec![27],
        "tab" => b"\t".to_vec(),
        "backspace" => vec![127],
        "up" => b"\x1b[A".to_vec(),
        "down" => b"\x1b[B".to_vec(),
        "right" => b"\x1b[C".to_vec(),
        "left" => b"\x1b[D".to_vec(),
        "pageup" => b"\x1b[5~".to_vec(),
        "pagedown" => b"\x1b[6~".to_vec(),
        "home" => b"\x1b[H".to_vec(),
        "end" => b"\x1b[F".to_vec(),
        _ if normalized.starts_with("ctrl+") && normalized.len() == 6 => {
            let letter = normalized.as_bytes()[5];
            if letter.is_ascii_lowercase() {
                vec![letter - b'a' + 1]
            } else {
                return Err(format!("Unsupported control key `{name}`"));
            }
        }
        _ if normalized.len() == 1 => normalized.as_bytes().to_vec(),
        _ => return Err(format!("Unsupported key name `{name}`")),
    };
    Ok(bytes)
}

fn runtime_check_step_timeout(step: &ServiceCheckStep) -> Duration {
    Duration::from_secs(if step.timeout_secs == 0 {
        5
    } else {
        step.timeout_secs
    })
}

fn execute_runtime_check_step(
    session: &mut Option<PtyScenarioSession>,
    step: &ServiceCheckStep,
    cwd: &Path,
    envs: &BTreeMap<String, String>,
    scenario_root: &Path,
    details: &mut Vec<String>,
) -> Result<(), String> {
    let kind = step.kind.trim().to_ascii_lowercase();
    match kind.as_str() {
        "wait" | "wait_ms" | "sleep" => {
            let wait_ms = if step.wait_ms > 0 {
                step.wait_ms
            } else {
                runtime_check_step_timeout(step).as_millis() as u64
            };
            thread::sleep(Duration::from_millis(wait_ms));
            details.push(format!("waited {}ms", wait_ms));
            Ok(())
        }
        "command" | "shell" | "gui-command" => {
            let log_label = sanitize_artifact_label(&format!("{}-{}", kind, details.len() + 1));
            let stdout_path = scenario_root.join(format!("{log_label}.stdout.log"));
            let stderr_path = scenario_root.join(format!("{log_label}.stderr.log"));
            let result = service_check_command_result(&step.command, cwd, envs)?;
            write_command_result_logs(&stdout_path, &stderr_path, &result)?;
            let failure = apply_command_expectations(
                &result,
                step.expect_exit_code,
                &step.expect_stdout_contains,
                &step.expect_stderr_contains,
            );
            details.push(format!(
                "command `{}` exit={} stdout=`{}` stderr=`{}`",
                step.command,
                result.code,
                stdout_path.display(),
                stderr_path.display()
            ));
            if failure.is_empty() {
                Ok(())
            } else {
                Err(failure)
            }
        }
        "http" | "rest" => {
            let method = if step.method.trim().is_empty() {
                "GET".to_string()
            } else {
                step.method.clone()
            };
            let response = runtime_http_request(
                &method,
                &step.url,
                &step.headers,
                if step.body.trim().is_empty() {
                    None
                } else {
                    Some(step.body.as_str())
                },
            )?;
            let log_label = sanitize_artifact_label(&format!("http-{}", details.len() + 1));
            let body_path = scenario_root.join(format!("{log_label}.body.txt"));
            let headers_path = scenario_root.join(format!("{log_label}.headers.json"));
            write_text(&body_path, &response.body)?;
            write_json(&headers_path, &response.headers)?;
            let failure = apply_http_expectations(
                &response,
                step.expect_status,
                &step.expect_body_contains,
                &step.expect_body_not_contains,
            );
            details.push(format!(
                "http {} {} status={} body=`{}`",
                method,
                step.url,
                response.status,
                body_path.display()
            ));
            if failure.is_empty() {
                Ok(())
            } else {
                Err(failure)
            }
        }
        "file_contains" => {
            let path = if PathBuf::from(&step.path).is_absolute() {
                PathBuf::from(&step.path)
            } else {
                cwd.join(&step.path)
            };
            let content = read_text(&path)?;
            if content.contains(&step.pattern) {
                details.push(format!(
                    "file `{}` contains `{}`",
                    path.display(),
                    step.pattern
                ));
                Ok(())
            } else {
                Err(format!(
                    "file `{}` is missing expected fragment `{}`",
                    path.display(),
                    step.pattern
                ))
            }
        }
        "pty_send_text" => {
            let Some(session) = session.as_mut() else {
                return Err("PTY send_text step requires an active PTY session.".to_string());
            };
            session.send_text(&step.text)?;
            details.push(format!("pty_send_text `{}`", step.text));
            Ok(())
        }
        "pty_send_keys" => {
            let Some(session) = session.as_mut() else {
                return Err("PTY send_keys step requires an active PTY session.".to_string());
            };
            for key in &step.keys {
                session.send_bytes(&encode_key_name(key)?)?;
                thread::sleep(Duration::from_millis(20));
            }
            details.push(format!("pty_send_keys {}", step.keys.join(", ")));
            Ok(())
        }
        "pty_wait_contains" => {
            let Some(session) = session.as_mut() else {
                return Err("PTY wait_contains step requires an active PTY session.".to_string());
            };
            session.wait_contains(&step.pattern, runtime_check_step_timeout(step))?;
            details.push(format!("pty_wait_contains `{}`", step.pattern));
            Ok(())
        }
        "pty_assert_contains" => {
            let Some(session) = session.as_mut() else {
                return Err("PTY assert_contains step requires an active PTY session.".to_string());
            };
            let screen = session.screen();
            if screen.contains(&step.pattern) {
                details.push(format!("pty_assert_contains `{}`", step.pattern));
                Ok(())
            } else {
                Err(format!(
                    "PTY screen is missing expected fragment `{}`.\nCurrent screen:\n{}",
                    step.pattern, screen
                ))
            }
        }
        "pty_resize" => {
            let Some(session) = session.as_mut() else {
                return Err("PTY resize step requires an active PTY session.".to_string());
            };
            let rows = if step.rows == 0 { 40 } else { step.rows };
            let cols = if step.cols == 0 { 120 } else { step.cols };
            session.resize(rows, cols)?;
            details.push(format!("pty_resize {}x{}", rows, cols));
            Ok(())
        }
        other => Err(format!("Unsupported runtime-check step kind `{other}`")),
    }
}

fn execute_runtime_check_scenario(
    run_dir: &Path,
    workdir: &Path,
    envs: &BTreeMap<String, String>,
    scenarios_root: &Path,
    scenario: &ServiceCheckScenario,
) -> Result<ServiceCheckScenarioResult, String> {
    let scenario_kind = if scenario.kind.trim().is_empty() {
        if !scenario.steps.is_empty() {
            "workflow".to_string()
        } else if !scenario.url.trim().is_empty() {
            "http".to_string()
        } else {
            "command".to_string()
        }
    } else {
        scenario.kind.trim().to_ascii_lowercase()
    };
    let label = sanitize_artifact_label(&scenario.id);
    let scenario_root = scenarios_root.join(&label);
    fs::create_dir_all(&scenario_root)
        .map_err(|err| format!("Could not create {}: {err}", scenario_root.display()))?;
    let stdout_path = scenario_root.join("stdout.log");
    let stderr_path = scenario_root.join("stderr.log");
    let screen_path = scenario_root.join("screen.txt");
    let mut details = Vec::new();
    let mut failure_reason = String::new();
    let mut exit_code = 0;

    match scenario_kind.as_str() {
        "command" | "shell" | "gui-command" => {
            let result = service_check_command_result(&scenario.command, workdir, envs)?;
            write_command_result_logs(&stdout_path, &stderr_path, &result)?;
            exit_code = result.code;
            failure_reason = apply_command_expectations(
                &result,
                scenario.expect_exit_code,
                &scenario.expect_stdout_contains,
                &scenario.expect_stderr_contains,
            );
        }
        "http" | "rest" => {
            let method = if scenario.method.trim().is_empty() {
                "GET".to_string()
            } else {
                scenario.method.clone()
            };
            let response = runtime_http_request(
                &method,
                &scenario.url,
                &scenario.headers,
                if scenario.body.trim().is_empty() {
                    None
                } else {
                    Some(scenario.body.as_str())
                },
            )?;
            exit_code = 0;
            write_text(&stdout_path, &response.body)?;
            write_json(&stderr_path, &response.headers)?;
            failure_reason = apply_http_expectations(
                &response,
                scenario.expect_status,
                &scenario.expect_body_contains,
                &scenario.expect_body_not_contains,
            );
            details.push(format!("http_status={}", response.status));
        }
        "pty" => {
            let rows = if scenario.rows == 0 {
                40
            } else {
                scenario.rows
            };
            let cols = if scenario.cols == 0 {
                120
            } else {
                scenario.cols
            };
            let transcript_path = scenario_root.join("pty.transcript.log");
            let mut session = Some(PtyScenarioSession::start(
                &scenario.command,
                workdir,
                envs,
                rows,
                cols,
                &transcript_path,
            )?);
            for step in &scenario.steps {
                if let Err(err) = execute_runtime_check_step(
                    &mut session,
                    step,
                    workdir,
                    envs,
                    &scenario_root,
                    &mut details,
                ) {
                    failure_reason = err;
                    break;
                }
            }
            if let Some(mut active) = session {
                let screen = active.screen();
                write_text(&screen_path, &screen)?;
                exit_code = active.stop()?;
            }
            if failure_reason.is_empty() {
                let expected_exit = scenario.expect_exit_code;
                if exit_code != expected_exit {
                    failure_reason = format!("expected exit code {expected_exit}, got {exit_code}");
                }
            }
            write_text(&stdout_path, &details.join("\n"))?;
            write_text(&stderr_path, &failure_reason)?;
        }
        "workflow" => {
            let mut session: Option<PtyScenarioSession> = None;
            for step in &scenario.steps {
                let step_kind = step.kind.trim().to_ascii_lowercase();
                if step_kind == "pty_start" {
                    if session.is_some() {
                        failure_reason =
                            "workflow attempted to start a second PTY session".to_string();
                        break;
                    }
                    let rows = if step.rows == 0 { 40 } else { step.rows };
                    let cols = if step.cols == 0 { 120 } else { step.cols };
                    let transcript_path = scenario_root.join("pty.transcript.log");
                    match PtyScenarioSession::start(
                        &step.command,
                        workdir,
                        envs,
                        rows,
                        cols,
                        &transcript_path,
                    ) {
                        Ok(started) => {
                            session = Some(started);
                            details.push(format!("pty_start `{}`", step.command));
                        }
                        Err(err) => {
                            failure_reason = err;
                            break;
                        }
                    }
                    continue;
                }
                if let Err(err) = execute_runtime_check_step(
                    &mut session,
                    step,
                    workdir,
                    envs,
                    &scenario_root,
                    &mut details,
                ) {
                    failure_reason = err;
                    break;
                }
            }
            if let Some(mut active) = session {
                let screen = active.screen();
                write_text(&screen_path, &screen)?;
                exit_code = active.stop()?;
            }
            if failure_reason.is_empty() {
                let expected_exit = scenario.expect_exit_code;
                if exit_code != expected_exit {
                    failure_reason = format!("expected exit code {expected_exit}, got {exit_code}");
                }
            }
            write_text(&stdout_path, &details.join("\n"))?;
            write_text(&stderr_path, &failure_reason)?;
        }
        other => {
            failure_reason = format!("Unsupported runtime-check scenario kind `{other}`");
            write_text(&stdout_path, "")?;
            write_text(&stderr_path, &failure_reason)?;
        }
    }

    let status = if failure_reason.is_empty() {
        "passed".to_string()
    } else {
        "failed".to_string()
    };
    Ok(ServiceCheckScenarioResult {
        id: scenario.id.clone(),
        description: scenario.description.clone(),
        command: if scenario.command.trim().is_empty() {
            scenario.url.clone()
        } else {
            scenario.command.clone()
        },
        exit_code,
        status,
        failure_reason,
        stdout_path: relative_to_run_dir(run_dir, &stdout_path),
        stderr_path: relative_to_run_dir(run_dir, &stderr_path),
        screen_path: if screen_path.exists() {
            relative_to_run_dir(run_dir, &screen_path)
        } else {
            String::new()
        },
        details,
    })
}

fn start_background_shell_command(
    command: &str,
    cwd: &Path,
    envs: &BTreeMap<String, String>,
    stdout_path: &Path,
    stderr_path: &Path,
) -> Result<std::process::Child, String> {
    initialize_capture_file(stdout_path)?;
    initialize_capture_file(stderr_path)?;
    let stdout_file = File::create(stdout_path)
        .map_err(|err| format!("Could not create {}: {err}", stdout_path.display()))?;
    let stderr_file = File::create(stderr_path)
        .map_err(|err| format!("Could not create {}: {err}", stderr_path.display()))?;
    let mut command_builder = Command::new("/bin/sh");
    command_builder
        .arg("-lc")
        .arg(command)
        .current_dir(cwd)
        .envs(envs)
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout_file))
        .stderr(Stdio::from(stderr_file));
    unsafe {
        command_builder.pre_exec(|| {
            if libc::setpgid(0, 0) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    command_builder
        .spawn()
        .map_err(|err| format!("Could not start `{command}` in {}: {err}", cwd.display()))
}

fn terminate_process_group(pid: i32) -> Result<(), String> {
    if pid <= 0 {
        return Ok(());
    }
    let _ = unsafe { libc::kill(-pid, libc::SIGTERM) };
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if !crate::runtime::process_group_alive(pid) {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(100));
    }
    let _ = unsafe { libc::kill(-pid, libc::SIGKILL) };
    Ok(())
}

fn service_check_summary_markdown(summary: &ServiceCheckSummary) -> String {
    let mut lines = vec![
        "# Runtime Check Summary".to_string(),
        String::new(),
        format!("- Phase: `{}`", summary.phase),
        format!("- Status: `{}`", summary.status),
        format!("- Mode: `{}`", summary.mode),
        format!("- Spec: `{}`", summary.spec_path),
        format!("- Workdir: `{}`", summary.workdir),
    ];
    if !summary.ready_status.is_empty() {
        lines.push(format!("- Readiness: `{}`", summary.ready_status));
    }
    if !summary.start_log.is_empty() {
        lines.push(format!("- Start log: `{}`", summary.start_log));
    }
    if !summary.ready_failure.is_empty() {
        lines.push(format!("- Readiness failure: {}", summary.ready_failure));
    }
    if !summary.error_messages.is_empty() {
        lines.push(String::new());
        lines.push("## Errors".to_string());
        lines.push(String::new());
        for item in &summary.error_messages {
            lines.push(format!("- {}", item));
        }
    }
    lines.push(String::new());
    lines.push("## Scenarios".to_string());
    lines.push(String::new());
    for scenario in &summary.scenarios {
        lines.push(format!(
            "- `{}` `{}` exit={} stdout=`{}` stderr=`{}`{}{}",
            scenario.id,
            scenario.status,
            scenario.exit_code,
            scenario.stdout_path,
            scenario.stderr_path,
            if scenario.screen_path.is_empty() {
                String::new()
            } else {
                format!(" screen=`{}`", scenario.screen_path)
            },
            if scenario.failure_reason.is_empty() {
                String::new()
            } else {
                format!(" reason={}", scenario.failure_reason)
            }
        ));
        for detail in &scenario.details {
            lines.push(format!("  detail: {}", detail));
        }
    }
    lines.push(String::new());
    lines.join("\n")
}

fn write_service_check_summary(
    run_dir: &Path,
    phase: &str,
    summary: &ServiceCheckSummary,
) -> Result<(), String> {
    let root = service_check_runtime_dir(run_dir, phase);
    fs::create_dir_all(&root)
        .map_err(|err| format!("Could not create {}: {err}", root.display()))?;
    write_json(&service_check_summary_json_path(run_dir, phase), summary)?;
    write_text(
        &service_check_summary_md_path(run_dir, phase),
        &service_check_summary_markdown(summary),
    )?;
    Ok(())
}

fn finalize_service_check(
    run_dir: &Path,
    phase: &str,
    summary: &mut ServiceCheckSummary,
    error_messages: &[String],
) -> Result<CommandResult, String> {
    summary.finished_at = iso_timestamp();
    summary.error_messages = error_messages.to_vec();
    write_service_check_summary(run_dir, phase, summary)?;
    let stdout = format!(
        "Runtime check {} for phase `{}`. Summary: {}\n",
        summary.status,
        phase,
        service_check_summary_md_path(run_dir, phase).display()
    );
    let stderr = if summary.error_messages.is_empty() {
        String::new()
    } else {
        format!("{}\n", summary.error_messages.join("\n"))
    };
    Ok(CommandResult {
        code: if summary.status == "passed" { 0 } else { 1 },
        stdout,
        stderr,
    })
}

fn service_check_compose_prefix(
    run_dir: &Path,
    phase: &str,
    workdir: &Path,
    spec: &ServiceCheckSpec,
) -> String {
    let compose_file = if PathBuf::from(&spec.compose_file).is_absolute() {
        PathBuf::from(&spec.compose_file)
    } else {
        workdir.join(&spec.compose_file)
    };
    let mut parts = vec![
        "docker".to_string(),
        "compose".to_string(),
        "-f".to_string(),
        shell_quote(&compose_file.display().to_string()),
    ];
    let project_name = if spec.compose_project_name.trim().is_empty() {
        format!(
            "agpipe-{}-{}",
            sanitize_artifact_label(
                &run_dir
                    .file_name()
                    .and_then(|value| value.to_str())
                    .unwrap_or("run")
                    .to_string()
            ),
            sanitize_artifact_label(phase)
        )
    } else {
        spec.compose_project_name.trim().to_string()
    };
    parts.push("-p".to_string());
    parts.push(shell_quote(&project_name));
    parts.join(" ")
}

fn run_service_check_loaded(
    run_dir: &Path,
    phase: &str,
    spec_path: &Path,
    workdir: &Path,
    spec: &ServiceCheckSpec,
) -> Result<CommandResult, String> {
    let root = service_check_runtime_dir(run_dir, phase);
    fs::create_dir_all(&root)
        .map_err(|err| format!("Could not create {}: {err}", root.display()))?;
    let start_stdout = root.join("service.stdout.log");
    let start_stderr = root.join("service.stderr.log");
    let ready_stdout = root.join("ready.stdout.log");
    let ready_stderr = root.join("ready.stderr.log");
    let docker_up_stdout = root.join("docker-up.stdout.log");
    let docker_up_stderr = root.join("docker-up.stderr.log");
    let docker_logs = root.join("docker.logs.txt");
    let stop_stdout = root.join("stop.stdout.log");
    let stop_stderr = root.join("stop.stderr.log");
    let cleanup_stdout = root.join("cleanup.stdout.log");
    let cleanup_stderr = root.join("cleanup.stderr.log");
    let mut summary = ServiceCheckSummary {
        version: 1,
        phase: phase.to_string(),
        status: "running".to_string(),
        mode: spec.mode.clone(),
        spec_path: spec_path.display().to_string(),
        workdir: workdir.display().to_string(),
        started_at: iso_timestamp(),
        ready_status: "pending".to_string(),
        start_log: relative_to_run_dir(
            run_dir,
            if spec.mode == "docker-compose" {
                &docker_logs
            } else {
                &start_stdout
            },
        ),
        ..ServiceCheckSummary::default()
    };
    let mut service_child: Option<std::process::Child> = None;
    let mut error_messages = Vec::new();

    for (index, command) in spec.prepare_commands.iter().enumerate() {
        let stdout_path = root.join(format!("prepare-{:02}.stdout.log", index + 1));
        let stderr_path = root.join(format!("prepare-{:02}.stderr.log", index + 1));
        let result = service_check_command_result(command, workdir, &spec.env)?;
        write_command_result_logs(&stdout_path, &stderr_path, &result)?;
        if result.code != 0 {
            error_messages.push(format!(
                "Prepare command failed: `{}` exit={} stdout=`{}` stderr=`{}`",
                command,
                result.code,
                relative_to_run_dir(run_dir, &stdout_path),
                relative_to_run_dir(run_dir, &stderr_path)
            ));
            summary.status = "failed".to_string();
            return finalize_service_check(run_dir, phase, &mut summary, &error_messages);
        }
    }

    if spec.mode == "docker-compose" {
        let prefix = service_check_compose_prefix(run_dir, phase, workdir, spec);
        let services = spec
            .compose_services
            .iter()
            .map(|service| shell_quote(service))
            .collect::<Vec<_>>()
            .join(" ");
        let up_command = if services.is_empty() {
            format!("{prefix} up -d --build")
        } else {
            format!("{prefix} up -d --build {services}")
        };
        let up_result = service_check_command_result(&up_command, workdir, &spec.env)?;
        write_command_result_logs(&docker_up_stdout, &docker_up_stderr, &up_result)?;
        if up_result.code != 0 {
            error_messages.push(format!(
                "Docker compose startup failed: stdout=`{}` stderr=`{}`",
                relative_to_run_dir(run_dir, &docker_up_stdout),
                relative_to_run_dir(run_dir, &docker_up_stderr)
            ));
            summary.ready_status = "startup-failed".to_string();
            let _ = write_text(&docker_logs, "");
            summary.status = "failed".to_string();
            return finalize_service_check(run_dir, phase, &mut summary, &error_messages);
        }
    } else if spec.mode == "process" && !spec.start_command.trim().is_empty() {
        let child = start_background_shell_command(
            &spec.start_command,
            workdir,
            &spec.env,
            &start_stdout,
            &start_stderr,
        )?;
        service_child = Some(child);
    }

    if spec.ready_command.trim().is_empty() {
        summary.ready_status = "skipped".to_string();
    } else {
        let deadline = Instant::now() + Duration::from_secs(spec.ready_timeout_secs);
        let mut last_result = CommandResult::default();
        let mut ready = false;
        while Instant::now() <= deadline {
            let result = service_check_command_result(&spec.ready_command, workdir, &spec.env)?;
            last_result = result.clone();
            write_command_result_logs(&ready_stdout, &ready_stderr, &result)?;
            if result.code == 0 {
                ready = true;
                break;
            }
            if let Some(child) = service_child.as_ref() {
                if !crate::runtime::pid_alive(child.id() as i32) {
                    break;
                }
            }
            thread::sleep(Duration::from_millis(spec.ready_interval_ms));
        }
        if ready {
            summary.ready_status = "passed".to_string();
        } else {
            summary.ready_status = "failed".to_string();
            summary.ready_failure = if last_result.combined_output().trim().is_empty() {
                format!(
                    "Readiness command `{}` did not succeed within {}s.",
                    spec.ready_command, spec.ready_timeout_secs
                )
            } else {
                format!(
                    "Readiness command `{}` failed: {}",
                    spec.ready_command,
                    last_result.combined_output().trim()
                )
            };
            error_messages.push(summary.ready_failure.clone());
        }
    }

    let scenarios_root = root.join("scenarios");
    fs::create_dir_all(&scenarios_root)
        .map_err(|err| format!("Could not create {}: {err}", scenarios_root.display()))?;
    if error_messages.is_empty() {
        for scenario in &spec.scenarios {
            let result = execute_runtime_check_scenario(
                run_dir,
                workdir,
                &spec.env,
                &scenarios_root,
                scenario,
            )?;
            if result.status != "passed" {
                error_messages.push(format!(
                    "Scenario `{}` failed: {}",
                    scenario.id, result.failure_reason
                ));
            }
            summary.scenarios.push(result);
        }
    }

    if spec.mode == "docker-compose" {
        let prefix = service_check_compose_prefix(run_dir, phase, workdir, spec);
        if let Ok(logs) =
            service_check_command_result(&format!("{prefix} logs --no-color"), workdir, &spec.env)
        {
            let combined = logs.combined_output();
            let _ = write_text(&docker_logs, &combined);
        }
        let down_result = service_check_command_result(
            &format!("{prefix} down --remove-orphans"),
            workdir,
            &spec.env,
        )?;
        let _ = write_command_result_logs(&stop_stdout, &stop_stderr, &down_result);
    } else if let Some(child) = service_child.as_mut() {
        if !spec.stop_command.trim().is_empty() {
            let stop_result = service_check_command_result(&spec.stop_command, workdir, &spec.env)?;
            let _ = write_command_result_logs(&stop_stdout, &stop_stderr, &stop_result);
            if stop_result.code != 0 {
                error_messages.push(format!(
                    "Stop command failed: `{}` exit={}",
                    spec.stop_command, stop_result.code
                ));
            }
        }
        let _ = terminate_process_group(child.id() as i32);
        let _ = child.wait();
    }

    if !spec.cleanup_commands.is_empty() {
        let mut cleanup_stdout_text = String::new();
        let mut cleanup_stderr_text = String::new();
        for command in &spec.cleanup_commands {
            let result = service_check_command_result(command, workdir, &spec.env)?;
            cleanup_stdout_text.push_str(&format!("$ {command}\n{}", result.stdout));
            cleanup_stderr_text.push_str(&format!("$ {command}\n{}", result.stderr));
            if result.code != 0 {
                error_messages.push(format!(
                    "Cleanup command failed: `{}` exit={}",
                    command, result.code
                ));
            }
        }
        let _ = write_text(&cleanup_stdout, &cleanup_stdout_text);
        let _ = write_text(&cleanup_stderr, &cleanup_stderr_text);
    }

    if error_messages.is_empty() {
        summary.status = "passed".to_string();
        finalize_service_check(run_dir, phase, &mut summary, &error_messages)
    } else {
        summary.status = "failed".to_string();
        finalize_service_check(run_dir, phase, &mut summary, &error_messages)
    }
}

pub fn service_check_run(
    run_dir: &Path,
    phase: &str,
    explicit_spec: Option<&Path>,
) -> Result<CommandResult, String> {
    let Some((spec_path, workdir, spec)) = load_service_check_spec(run_dir, explicit_spec)? else {
        return Err(format!(
            "No runtime check spec found. Create `{}` in the workspace or pass --spec.",
            RUNTIME_CHECK_SPEC_REF
        ));
    };
    run_service_check_loaded(run_dir, phase, &spec_path, &workdir, &spec)
}

pub fn runtime_check_run(
    run_dir: &Path,
    phase: &str,
    explicit_spec: Option<&Path>,
) -> Result<CommandResult, String> {
    service_check_run(run_dir, phase, explicit_spec)
}

fn maybe_run_service_check(run_dir: &Path, phase: &str) -> Result<Option<CommandResult>, String> {
    let Some((spec_path, workdir, spec)) = load_service_check_spec(run_dir, None)? else {
        if runtime_check_required(run_dir, phase)? {
            return Ok(Some(CommandResult {
                code: 1,
                stdout: format!(
                    "Runtime check failed for phase `{phase}` because no workspace spec was found.\n"
                ),
                stderr: format!(
                    "Runtime-facing tasks must create `{}` before `{}` can complete.\n",
                    RUNTIME_CHECK_SPEC_REF, phase
                ),
            }));
        }
        return Ok(None);
    };
    Ok(Some(run_service_check_loaded(
        run_dir, phase, &spec_path, &workdir, &spec,
    )?))
}

fn append_runtime_check_output(buffer: &mut String, result: &CommandResult) {
    if !result.stdout.trim().is_empty() {
        buffer.push_str(result.stdout.trim_end());
        buffer.push('\n');
    }
    if !result.stderr.trim().is_empty() {
        buffer.push_str(result.stderr.trim_end());
        buffer.push('\n');
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
        "-".to_string(),
    ];
    if cache.enabled {
        command.splice(
            command.len() - 1..command.len() - 1,
            ["--add-dir".to_string(), cache.root.clone()],
        );
    }
    if let Some(reasoning_effort) = stage_reasoning_effort_override(&plan, run_dir, stage) {
        command.splice(
            2..2,
            [
                "--config".to_string(),
                format!("model_reasoning_effort=\"{reasoning_effort}\""),
            ],
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
    if env_flag("AGPIPE_CODEX_EPHEMERAL").unwrap_or(false) {
        command.insert(2, "--ephemeral".to_string());
    }
    if let Some(agency_root) = discover_agency_agents_dir(ctx) {
        if agency_root != ctx.repo_root && agency_root != root {
            let insert_at = command.len().saturating_sub(1);
            command.insert(insert_at, "--add-dir".to_string());
            command.insert(insert_at + 1, agency_root.display().to_string());
        }
    }
    Ok((command, prompt))
}

fn stage_reasoning_effort_override(
    plan: &Plan,
    run_dir: &Path,
    stage: &str,
) -> Option<&'static str> {
    let kind = pipeline_stage_kind_for(plan, run_dir, stage).ok()?;
    if !matches!(
        kind,
        PipelineStageKind::Intake
            | PipelineStageKind::Solver
            | PipelineStageKind::Review
            | PipelineStageKind::Execution
            | PipelineStageKind::Verification
    ) {
        return None;
    }
    let workspace = working_root(plan, run_dir);
    if task_is_trivial_local_cli_contract(
        &plan.task_kind,
        &plan.complexity,
        &plan.original_task,
        Some(&workspace),
    ) {
        return Some("low");
    }
    if plan.complexity == "low" {
        return Some("medium");
    }
    None
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
        LocalTemplateKind::ExecutionReadyBackendCliIntake => "execution-ready-backend-cli-intake",
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
    let mut normalized_plan = load_plan(run_dir)?;
    apply_pipeline_solver_defaults(&mut normalized_plan, Some(run_dir))?;
    ensure_reviewer_stack(&mut normalized_plan);
    save_plan(run_dir, &normalized_plan)?;
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
    stage: Option<&str>,
    last_message_path: Option<&Path>,
    mirror_stderr_to_stdout: bool,
) -> Result<i32, String> {
    initialize_capture_file(stdout_path)?;
    initialize_capture_file(stderr_path)?;
    let observer = current_engine_observer();
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
    let spawn_started_at = SystemTime::now();
    let pid = child.id() as i32;
    let mut interrupted = false;
    let mut completed_from_last_message = false;
    notify_process_started(pid, pid);
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "Spawned command did not expose stdout.".to_string())?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| "Spawned command did not expose stderr.".to_string())?;
    let stdout_thread =
        stream_pipe_to_files(stdout, stdout_path.to_path_buf(), None, observer.clone());
    let stderr_thread = stream_pipe_to_files(
        stderr,
        stderr_path.to_path_buf(),
        if mirror_stderr_to_stdout {
            Some(stdout_path.to_path_buf())
        } else {
            None
        },
        observer,
    );
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
        if let (Some(run_dir), Some(stage), Some(last_message_path)) = (run_dir, stage, last_message_path) {
            if file_modified_after(last_message_path, spawn_started_at)
                && maybe_recover_codex_outputs_from_last_message(run_dir, stage, last_message_path)?
                && is_stage_complete(run_dir, stage)?
            {
                completed_from_last_message = true;
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
                            return Err(format!("Could not wait for completed command: {err}"))
                        }
                    }
                }
                if let Some(status) = finished {
                    break status;
                }
                let _ = unsafe { libc::kill(-pid, libc::SIGKILL) };
                let status = child
                    .wait()
                    .map_err(|err| format!("Could not wait for completed command: {err}"))?;
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
    } else if completed_from_last_message {
        if mirror_stderr_to_stdout {
            initialize_capture_file(stderr_path)?;
        }
        Ok(0)
    } else {
        let code = status.code().unwrap_or(1);
        if mirror_stderr_to_stdout && code == 0 {
            initialize_capture_file(stderr_path)?;
        }
        Ok(code)
    }
}

fn codex_solver_batch_parallel_enabled(ctx: &Context) -> bool {
    if let Ok(value) = std::env::var("AGPIPE_CODEX_SOLVER_PARALLEL") {
        let normalized = value.trim().to_ascii_lowercase();
        return matches!(normalized.as_str(), "1" | "true" | "yes" | "on");
    }
    Path::new(&ctx.codex_bin)
        .file_name()
        .and_then(|item| item.to_str())
        .map(|item| item.contains("mock-"))
        .unwrap_or(false)
}

fn fail_closed_if_stage_outputs_incomplete(
    run_dir: &Path,
    stage: &str,
    stdout_path: &Path,
    stderr_path: &Path,
    code: i32,
) -> Result<i32, String> {
    if code != 0 || is_stage_complete(run_dir, stage)? {
        return Ok(code);
    }
    let message = format!(
        "Stage `{stage}` exited with code 0, but required artifacts are still incomplete. The run is being treated as failed instead of accepting placeholder outputs.\n"
    );
    append_log_line(stdout_path, &message)?;
    append_log_line(stderr_path, &message)?;
    notify_output_line(message.trim_end());
    Ok(1)
}

fn extract_last_codex_message_from_stdout_transcript(text: &str) -> Option<String> {
    let mut last_block = None;
    let lines: Vec<&str> = text.lines().collect();
    let mut index = 0usize;
    while index < lines.len() {
        if lines[index].trim() != "codex" {
            index += 1;
            continue;
        }
        let start = index + 1;
        let mut end = start;
        while end < lines.len() {
            let trimmed = lines[end].trim();
            if end > start
                && (trimmed == "codex"
                    || trimmed == "user"
                    || trimmed == "exec"
                    || trimmed.starts_with("mcp:"))
            {
                break;
            }
            end += 1;
        }
        let block = lines[start..end].join("\n").trim().to_string();
        if !block.is_empty() {
            last_block = Some(block);
        }
        index = end;
    }
    last_block
}

fn recover_solver_artifact_from_stdout_transcript(
    run_dir: &Path,
    stage: &str,
    stdout_path: &Path,
    last_message_path: &Path,
) -> Result<bool, String> {
    let result_path = run_dir.join("solutions").join(stage).join("RESULT.md");
    let current = read_text(&result_path).unwrap_or_default();
    if !output_looks_placeholder(stage, &current) {
        return Ok(false);
    }
    let transcript = read_text(stdout_path).unwrap_or_default();
    let Some(recovered) = extract_last_codex_message_from_stdout_transcript(&transcript) else {
        return Ok(false);
    };
    if output_looks_placeholder(stage, &recovered) {
        return Ok(false);
    }
    write_text(&result_path, recovered.trim_end())?;
    if !last_message_path.exists() || output_looks_placeholder(stage, &read_text(last_message_path).unwrap_or_default()) {
        write_text(last_message_path, recovered.trim_end())?;
    }
    append_log_line(
        stdout_path,
        &format!(
            "Recovered `{stage}` artifact from the final assistant message in the stdout transcript because `--output-last-message` did not materialize."
        ),
    )?;
    Ok(true)
}

fn recover_solver_artifact_from_last_message(
    run_dir: &Path,
    stage: &str,
    last_message_path: &Path,
) -> Result<bool, String> {
    let result_path = run_dir.join("solutions").join(stage).join("RESULT.md");
    let current = read_text(&result_path).unwrap_or_default();
    if !output_looks_placeholder(stage, &current) {
        return Ok(false);
    }
    let last_message = read_text(last_message_path).unwrap_or_default();
    if output_looks_placeholder(stage, &last_message) {
        return Ok(false);
    }
    write_text(&result_path, last_message.trim_end())?;
    Ok(true)
}

const REVIEW_REPORT_START: &str = "<<<AGPIPE_REVIEW_REPORT>>>";
const REVIEW_SCORECARD_START: &str = "<<<AGPIPE_REVIEW_SCORECARD_JSON>>>";
const REVIEW_USER_SUMMARY_START: &str = "<<<AGPIPE_REVIEW_USER_SUMMARY>>>";
const VERIFICATION_FINDINGS_START: &str = "<<<AGPIPE_VERIFICATION_FINDINGS>>>";
const VERIFICATION_GOAL_STATUS_START: &str = "<<<AGPIPE_VERIFICATION_GOAL_STATUS_JSON>>>";
const VERIFICATION_USER_SUMMARY_START: &str = "<<<AGPIPE_VERIFICATION_USER_SUMMARY>>>";
const VERIFICATION_IMPROVEMENT_REQUEST_START: &str = "<<<AGPIPE_VERIFICATION_IMPROVEMENT_REQUEST>>>";
const VERIFICATION_AUGMENTED_TASK_START: &str = "<<<AGPIPE_VERIFICATION_AUGMENTED_TASK>>>";

fn extract_tagged_section_from_known_tags(
    text: &str,
    start_tag: &str,
    known_tags: &[&str],
) -> Option<String> {
    let mut tags = vec![start_tag];
    tags.extend_from_slice(known_tags);
    tags.sort_unstable();
    tags.dedup();

    let mut occurrences: Vec<(usize, &str)> = Vec::new();
    for tag in &tags {
        for (offset, _) in text.match_indices(tag) {
            occurrences.push((offset, *tag));
        }
    }
    occurrences.sort_by_key(|(offset, _)| *offset);

    for (index, (offset, tag)) in occurrences.iter().enumerate() {
        if *tag != start_tag {
            continue;
        }
        let content_start = *offset + start_tag.len();
        let next_offset = occurrences
            .iter()
            .skip(index + 1)
            .map(|(next_offset, _)| *next_offset)
            .find(|next_offset| *next_offset >= content_start)
            .unwrap_or(text.len());
        let section = text[content_start..next_offset].trim().to_string();
        if !section.is_empty() {
            return Some(section);
        }
    }
    None
}

fn recover_review_artifacts_from_stdout_transcript(
    run_dir: &Path,
    stdout_path: &Path,
    last_message_path: &Path,
) -> Result<bool, String> {
    let report_path = run_dir.join("review").join("report.md");
    let scorecard_path = run_dir.join("review").join("scorecard.json");
    let summary_path = run_dir.join("review").join("user-summary.md");
    let report_current = read_text(&report_path).unwrap_or_default();
    let summary_current = read_text(&summary_path).unwrap_or_default();
    if !output_looks_placeholder("review", &report_current)
        && !output_looks_placeholder("review-summary", &summary_current)
        && review_scorecard_complete(&scorecard_path)
    {
        return Ok(false);
    }
    let transcript = read_text(stdout_path).unwrap_or_default();
    let last_message = extract_last_codex_message_from_stdout_transcript(&transcript)
        .unwrap_or_else(|| transcript.clone());
    let report = extract_tagged_section_from_known_tags(
        &last_message,
        REVIEW_REPORT_START,
        &[REVIEW_REPORT_START, REVIEW_SCORECARD_START, REVIEW_USER_SUMMARY_START],
    );
    let scorecard = extract_tagged_section_from_known_tags(
        &last_message,
        REVIEW_SCORECARD_START,
        &[REVIEW_REPORT_START, REVIEW_SCORECARD_START, REVIEW_USER_SUMMARY_START],
    );
    let summary = extract_tagged_section_from_known_tags(
        &last_message,
        REVIEW_USER_SUMMARY_START,
        &[REVIEW_REPORT_START, REVIEW_SCORECARD_START, REVIEW_USER_SUMMARY_START],
    );
    let (Some(report), Some(scorecard), Some(summary)) = (report, scorecard, summary) else {
        return Ok(false);
    };
    let scorecard_json = serde_json::from_str::<Value>(&scorecard)
        .map_err(|err| format!("Recovered review scorecard is not valid JSON: {err}"))?;
    write_text(&report_path, report.trim_end())?;
    write_json(&scorecard_path, &scorecard_json)?;
    write_text(&summary_path, summary.trim_end())?;
    if !last_message_path.exists()
        || output_looks_placeholder("review", &read_text(last_message_path).unwrap_or_default())
    {
        write_text(last_message_path, last_message.trim_end())?;
    }
    append_log_line(
        stdout_path,
        "Recovered review artifacts from the final assistant message in the stdout transcript because the direct review outputs remained placeholders.",
    )?;
    Ok(true)
}

fn recover_review_artifacts_from_last_message(
    run_dir: &Path,
    last_message_path: &Path,
) -> Result<bool, String> {
    let report_path = run_dir.join("review").join("report.md");
    let scorecard_path = run_dir.join("review").join("scorecard.json");
    let summary_path = run_dir.join("review").join("user-summary.md");
    let report_current = read_text(&report_path).unwrap_or_default();
    let summary_current = read_text(&summary_path).unwrap_or_default();
    if !output_looks_placeholder("review", &report_current)
        && !output_looks_placeholder("review-summary", &summary_current)
        && review_scorecard_complete(&scorecard_path)
    {
        return Ok(false);
    }
    let last_message = read_text(last_message_path).unwrap_or_default();
    let report = extract_tagged_section_from_known_tags(
        &last_message,
        REVIEW_REPORT_START,
        &[REVIEW_REPORT_START, REVIEW_SCORECARD_START, REVIEW_USER_SUMMARY_START],
    );
    let scorecard = extract_tagged_section_from_known_tags(
        &last_message,
        REVIEW_SCORECARD_START,
        &[REVIEW_REPORT_START, REVIEW_SCORECARD_START, REVIEW_USER_SUMMARY_START],
    );
    let summary = extract_tagged_section_from_known_tags(
        &last_message,
        REVIEW_USER_SUMMARY_START,
        &[REVIEW_REPORT_START, REVIEW_SCORECARD_START, REVIEW_USER_SUMMARY_START],
    );
    let (Some(report), Some(scorecard), Some(summary)) = (report, scorecard, summary) else {
        return Ok(false);
    };
    let scorecard_json = serde_json::from_str::<Value>(&scorecard)
        .map_err(|err| format!("Recovered review scorecard is not valid JSON: {err}"))?;
    write_text(&report_path, report.trim_end())?;
    write_json(&scorecard_path, &scorecard_json)?;
    write_text(&summary_path, summary.trim_end())?;
    Ok(true)
}

fn recover_verification_artifacts_from_stdout_transcript(
    run_dir: &Path,
    stdout_path: &Path,
    last_message_path: &Path,
) -> Result<bool, String> {
    let findings_path = run_dir.join("verification").join("findings.md");
    let summary_path = run_dir.join("verification").join("user-summary.md");
    let improvement_path = run_dir.join("verification").join("improvement-request.md");
    let augmented_path = run_dir.join("verification").join("augmented-task.md");
    let goal_status = goal_status_path(run_dir);
    let plan = load_plan(run_dir)?;
    if !output_looks_placeholder("verification", &read_text(&findings_path).unwrap_or_default())
        && !output_looks_placeholder(
            "verification-summary",
            &read_text(&summary_path).unwrap_or_default(),
        )
        && !output_looks_placeholder(
            "improvement-request",
            &read_text(&improvement_path).unwrap_or_default(),
        )
        && (!plan.augmented_follow_up_enabled
            || !output_looks_placeholder(
                "augmented-task",
                &read_text(&augmented_path).unwrap_or_default(),
            ))
        && (!plan.goal_gate_enabled || goal_status_complete(&goal_status))
    {
        return Ok(false);
    }
    let transcript = read_text(stdout_path).unwrap_or_default();
    let last_message = extract_last_codex_message_from_stdout_transcript(&transcript)
        .unwrap_or_else(|| transcript.clone());
    let verification_tags = [
        VERIFICATION_FINDINGS_START,
        VERIFICATION_GOAL_STATUS_START,
        VERIFICATION_USER_SUMMARY_START,
        VERIFICATION_IMPROVEMENT_REQUEST_START,
        VERIFICATION_AUGMENTED_TASK_START,
    ];
    let findings = extract_tagged_section_from_known_tags(
        &last_message,
        VERIFICATION_FINDINGS_START,
        &verification_tags,
    );
    let goal_status_json = extract_tagged_section_from_known_tags(
        &last_message,
        VERIFICATION_GOAL_STATUS_START,
        &verification_tags,
    );
    let user_summary = extract_tagged_section_from_known_tags(
        &last_message,
        VERIFICATION_USER_SUMMARY_START,
        &verification_tags,
    );
    let improvement_request = extract_tagged_section_from_known_tags(
        &last_message,
        VERIFICATION_IMPROVEMENT_REQUEST_START,
        &verification_tags,
    );
    let augmented_task = extract_tagged_section_from_known_tags(
        &last_message,
        VERIFICATION_AUGMENTED_TASK_START,
        &verification_tags,
    );
    let (Some(findings), Some(goal_status_json), Some(user_summary), Some(improvement_request)) =
        (findings, goal_status_json, user_summary, improvement_request)
    else {
        return Ok(false);
    };
    if plan.augmented_follow_up_enabled && augmented_task.is_none() {
        return Ok(false);
    }
    let goal_status_json = serde_json::from_str::<Value>(&goal_status_json)
        .map_err(|err| format!("Recovered verification goal-status is not valid JSON: {err}"))?;
    write_text(&findings_path, findings.trim_end())?;
    write_text(&summary_path, user_summary.trim_end())?;
    write_text(&improvement_path, improvement_request.trim_end())?;
    if let Some(augmented_task) = augmented_task {
        write_text(&augmented_path, augmented_task.trim_end())?;
    }
    write_json(&goal_status, &goal_status_json)?;
    if !last_message_path.exists()
        || output_looks_placeholder("verification", &read_text(last_message_path).unwrap_or_default())
    {
        write_text(last_message_path, last_message.trim_end())?;
    }
    append_log_line(
        stdout_path,
        "Recovered verification artifacts from the final assistant message in the stdout transcript because the direct verification outputs remained placeholders.",
    )?;
    Ok(true)
}

fn recover_verification_artifacts_from_last_message(
    run_dir: &Path,
    last_message_path: &Path,
) -> Result<bool, String> {
    let findings_path = run_dir.join("verification").join("findings.md");
    let summary_path = run_dir.join("verification").join("user-summary.md");
    let improvement_path = run_dir.join("verification").join("improvement-request.md");
    let augmented_path = run_dir.join("verification").join("augmented-task.md");
    let goal_status = goal_status_path(run_dir);
    let plan = load_plan(run_dir)?;
    if !output_looks_placeholder("verification", &read_text(&findings_path).unwrap_or_default())
        && !output_looks_placeholder(
            "verification-summary",
            &read_text(&summary_path).unwrap_or_default(),
        )
        && !output_looks_placeholder(
            "improvement-request",
            &read_text(&improvement_path).unwrap_or_default(),
        )
        && (!plan.augmented_follow_up_enabled
            || !output_looks_placeholder(
                "augmented-task",
                &read_text(&augmented_path).unwrap_or_default(),
            ))
        && (!plan.goal_gate_enabled || goal_status_complete(&goal_status))
    {
        return Ok(false);
    }
    let last_message = read_text(last_message_path).unwrap_or_default();
    let verification_tags = [
        VERIFICATION_FINDINGS_START,
        VERIFICATION_GOAL_STATUS_START,
        VERIFICATION_USER_SUMMARY_START,
        VERIFICATION_IMPROVEMENT_REQUEST_START,
        VERIFICATION_AUGMENTED_TASK_START,
    ];
    let findings = extract_tagged_section_from_known_tags(
        &last_message,
        VERIFICATION_FINDINGS_START,
        &verification_tags,
    );
    let goal_status_json = extract_tagged_section_from_known_tags(
        &last_message,
        VERIFICATION_GOAL_STATUS_START,
        &verification_tags,
    );
    let user_summary = extract_tagged_section_from_known_tags(
        &last_message,
        VERIFICATION_USER_SUMMARY_START,
        &verification_tags,
    );
    let improvement_request = extract_tagged_section_from_known_tags(
        &last_message,
        VERIFICATION_IMPROVEMENT_REQUEST_START,
        &verification_tags,
    );
    let augmented_task = extract_tagged_section_from_known_tags(
        &last_message,
        VERIFICATION_AUGMENTED_TASK_START,
        &verification_tags,
    );
    let (Some(findings), Some(goal_status_json), Some(user_summary), Some(improvement_request)) =
        (findings, goal_status_json, user_summary, improvement_request)
    else {
        return Ok(false);
    };
    if plan.augmented_follow_up_enabled && augmented_task.is_none() {
        return Ok(false);
    }
    let goal_status_json = serde_json::from_str::<Value>(&goal_status_json)
        .map_err(|err| format!("Recovered verification goal-status is not valid JSON: {err}"))?;
    write_text(&findings_path, findings.trim_end())?;
    write_text(&summary_path, user_summary.trim_end())?;
    write_text(&improvement_path, improvement_request.trim_end())?;
    if let Some(augmented_task) = augmented_task {
        write_text(&augmented_path, augmented_task.trim_end())?;
    }
    write_json(&goal_status, &goal_status_json)?;
    Ok(true)
}

fn maybe_recover_codex_outputs_from_last_message(
    run_dir: &Path,
    stage: &str,
    last_message_path: &Path,
) -> Result<bool, String> {
    if !last_message_path.exists() {
        return Ok(false);
    }
    if stage.starts_with("solver-") {
        return recover_solver_artifact_from_last_message(run_dir, stage, last_message_path);
    }
    if stage == "review" {
        return recover_review_artifacts_from_last_message(run_dir, last_message_path);
    }
    if stage == "verification" {
        return recover_verification_artifacts_from_last_message(run_dir, last_message_path);
    }
    Ok(false)
}

fn file_modified_after(path: &Path, threshold: SystemTime) -> bool {
    fs::metadata(path)
        .and_then(|meta| meta.modified())
        .map(|modified| modified >= threshold)
        .unwrap_or(false)
}

fn maybe_recover_codex_solver_outputs(
    backend: StageBackendKind,
    run_dir: &Path,
    stage: &str,
    stdout_path: &Path,
    last_message_path: &Path,
) -> Result<bool, String> {
    if backend != StageBackendKind::Codex {
        return Ok(false);
    }
    if stage.starts_with("solver-") {
        return recover_solver_artifact_from_stdout_transcript(
            run_dir,
            stage,
            stdout_path,
            last_message_path,
        );
    }
    if stage == "review" {
        return recover_review_artifacts_from_stdout_transcript(
            run_dir,
            stdout_path,
            last_message_path,
        );
    }
    if stage == "verification" {
        return recover_verification_artifacts_from_stdout_transcript(
            run_dir,
            stdout_path,
            last_message_path,
        );
    }
    Ok(false)
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
        LocalTemplateKind::ExecutionReadyBackendCliIntake => {
            "local-template:execution-ready-backend-cli-intake"
        }
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
        LocalTemplateKind::ExecutionReadyBackendCliIntake => match stage_kind {
            PipelineStageKind::Intake => {
                ensure_reviewer_stack(&mut plan);
                save_plan(run_dir, &plan)?;
                write_text(
                    &run_dir.join("brief.md"),
                    &render_execution_ready_backend_cli_brief(&plan),
                )?;
                Ok("Local intake fast-path synthesized a backend CLI brief from the existing request and plan defaults.".to_string())
            }
            _ => Err(
                "Execution-ready backend CLI intake template only supports the intake stage."
                    .to_string(),
            ),
        },
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
                    mcp_servers: Vec::new(),
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

fn initialize_capture_file(path: &Path) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|err| format!("Could not create {}: {err}", parent.display()))?;
    }
    fs::write(path, "").map_err(|err| format!("Could not initialize {}: {err}", path.display()))
}

fn stream_pipe_to_files<R>(
    mut reader: R,
    path: PathBuf,
    mirror_path: Option<PathBuf>,
    observer: Option<Arc<dyn EngineObserver>>,
) -> thread::JoinHandle<Result<(), String>>
where
    R: Read + Send + 'static,
{
    thread::spawn(move || {
        let capture = || -> Result<(), String> {
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .map_err(|err| format!("Could not open {}: {err}", path.display()))?;
            let mut mirror = if let Some(mirror_path) = mirror_path {
                Some(
                    std::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(&mirror_path)
                        .map_err(|err| {
                            format!("Could not open {}: {err}", mirror_path.display())
                        })?,
                )
            } else {
                None
            };
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
                if let Some(mirror) = mirror.as_mut() {
                    mirror.write_all(&buffer[..read]).map_err(|err| {
                        format!("Could not write mirrored {}: {err}", path.display())
                    })?;
                    mirror.flush().map_err(|err| {
                        format!("Could not flush mirrored {}: {err}", path.display())
                    })?;
                }
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
        };
        if let Some(observer) = observer {
            with_engine_observer(observer, capture)
        } else {
            capture()
        }
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
    if let Some(attempt) = &report.last_attempt {
        lines.push(format!("last-attempt-stage: {}", attempt.stage));
        lines.push(format!("last-attempt-status: {}", attempt.status));
        if let Some(code) = attempt.exit_code {
            lines.push(format!("last-attempt-exit-code: {code}"));
        }
        if !attempt.message.trim().is_empty() {
            lines.push(format!("last-attempt-message: {}", attempt.message));
        }
    }
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
    if !report.fix_actions.is_empty() {
        lines.push(format!(
            "doctor-fix-actions: {}",
            report.fix_actions.join(" -> ")
        ));
    }
    if !report.stale.is_empty() {
        lines.push(format!("stale: {}", report.stale.join(", ")));
    }
    if let Some(drift) = &report.host_drift {
        lines.push(format!("host-drift: {drift}"));
    }
    if let Some(attempt) = &report.last_attempt {
        lines.push(format!(
            "last-attempt: {} [{} -> {}]",
            attempt.stage, attempt.label, attempt.status
        ));
        if let Some(code) = attempt.exit_code {
            lines.push(format!("last-attempt-exit-code: {code}"));
        }
        if !attempt.message.trim().is_empty() {
            lines.push(format!("last-attempt-message: {}", attempt.message));
        }
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

fn doctor_compact_text(report: &DoctorPayload) -> String {
    let mut lines = vec![
        format!("health: {}", report.health),
        format!("goal: {}", report.goal),
        format!("next: {}", report.next),
        format!("safe-next-action: {}", report.safe_next_action),
        format!("host-probe: {}", report.host_probe),
    ];
    if !report.fix_actions.is_empty() {
        lines.push(format!(
            "doctor-fix-actions: {}",
            report.fix_actions.join(" -> ")
        ));
    }
    if !report.issues.is_empty() {
        lines.push(format!("issues: {}", report.issues.len()));
        for item in report.issues.iter().take(2) {
            lines.push(format!("- {}", item.message));
        }
    }
    if !report.warnings.is_empty() {
        lines.push(format!("warnings: {}", report.warnings.len()));
        for item in report.warnings.iter().take(2) {
            lines.push(format!("- {}", item.message));
        }
    }
    if report.issues.is_empty() && report.warnings.is_empty() {
        lines.push("No consistency issues detected.".to_string());
    } else {
        lines.push("Open the full doctor report for the detailed issue list.".to_string());
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
    let stage_kind = pipeline_stage_kind_for(&load_plan(run_dir)?, run_dir, stage)?;
    if matches!(
        stage_kind,
        PipelineStageKind::Execution | PipelineStageKind::Verification
    ) {
        let _ = capture_host_probe(run_dir)?;
    }
    let mut preflight_stdout = String::new();
    if stage_kind == PipelineStageKind::Verification {
        if let Some(result) = maybe_run_service_check(run_dir, "verification")? {
            append_runtime_check_output(&mut preflight_stdout, &result);
            if result.code != 0 {
                let mut stdout = String::new();
                if !preflight_stdout.trim().is_empty() {
                    stdout.push_str(preflight_stdout.trim_end());
                    stdout.push_str("\n\n");
                }
                stdout.push_str(&format!(
                    "Blocked {stage} because runtime-check did not pass.\n"
                ));
                stdout.push_str(&print_status_after_action(ctx, run_dir)?);
                return Ok(CommandResult {
                    code: result.code.max(1),
                    stdout,
                    stderr: String::new(),
                });
            }
        }
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
            record_stage_mcp_usage(
                &plan,
                run_dir,
                stage,
                stage_backend_label(backend),
                "cache-hit",
            )?;
            if stage_kind == PipelineStageKind::Verification {
                write_verification_current_facts(run_dir, &plan)?;
            }
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
    if !matches!(backend, StageBackendKind::LocalTemplate(_)) {
        let provisional_usage = provisional_token_usage_for_prompt(&prompt);
        record_token_usage_with_replacement(
            run_dir,
            &plan,
            stage,
            "provisional",
            &cache_key,
            &prompt_hashes,
            &workspace_hash,
            &provisional_usage,
            &["provisional"],
        )?;
    }
    let mut code = match backend {
        StageBackendKind::Codex => run_prompted_command_capture(
            &command,
            &prompt,
            &stdout_path,
            &stderr_path,
            Some(run_dir),
            Some(stage),
            Some(&last_message_path),
            true,
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
    if maybe_recover_codex_solver_outputs(
        backend,
        run_dir,
        stage,
        &stdout_path,
        &last_message_path,
    )? {
        sync_run_artifacts(ctx, run_dir)?;
    }
    code = fail_closed_if_stage_outputs_incomplete(
        run_dir,
        stage,
        &stdout_path,
        &stderr_path,
        code,
    )?;
    check_run_interrupt(run_dir)?;
    if code == 0 && stage_kind == PipelineStageKind::Execution {
        if let Some(result) = maybe_run_service_check(run_dir, "execution")? {
            if !preflight_stdout.is_empty() {
                preflight_stdout.push('\n');
            }
            append_runtime_check_output(&mut preflight_stdout, &result);
            if result.code != 0 {
                code = result.code;
            }
        }
    }
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
        record_token_usage_with_replacement(
            run_dir,
            &plan,
            stage,
            "executed",
            &cache_key,
            &prompt_hashes,
            &workspace_hash,
            &token_usage,
            &["provisional"],
        )?;
        record_stage_mcp_usage(
            &plan,
            run_dir,
            stage,
            stage_backend_label(backend),
            "executed",
        )?;
        if stage_kind == PipelineStageKind::Verification {
            write_verification_current_facts(run_dir, &plan)?;
        }
    }
    let _ = refresh_cache_index(&cache)?;
    let mut stdout = String::new();
    if !preflight_stdout.trim().is_empty() {
        stdout.push_str(preflight_stdout.trim_end());
        stdout.push_str("\n\n");
    }
    stdout.push_str(&format!(
        "Completed {stage} with exit code {code}.\ncache key: {cache_key}\nstdout log: {}\nstderr log: {}\n",
        stdout_path.display(),
        stderr_path.display()
    ));
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
    let all_codex = stages.iter().all(|stage| {
        matches!(stage_backend_kind(ctx, run_dir, stage), Ok(StageBackendKind::Codex))
    });
    let force_serial_codex = all_codex && !codex_solver_batch_parallel_enabled(ctx);
    if requires_in_process || force_serial_codex {
        let mut combined = String::new();
        let mut exit_code = 0;
        if force_serial_codex {
            combined.push_str("Running solver stages sequentially for Codex stability.\n");
        }
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
    let observer = current_engine_observer();
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
                record_stage_mcp_usage(
                    &plan,
                    run_dir,
                    &stage,
                    stage_backend_label(StageBackendKind::Codex),
                    "cache-hit",
                )?;
                let _ = refresh_cache_index(&cache)?;
                continue;
            }
        }
        let provisional_usage = provisional_token_usage_for_prompt(&prompt);
        record_token_usage_with_replacement(
            run_dir,
            &plan,
            &stage,
            "provisional",
            &cache_key,
            &prompt_hashes,
            &workspace_hash,
            &provisional_usage,
            &["provisional"],
        )?;
        let run_dir_for_thread = run_dir.to_path_buf();
        let stage_for_thread = stage.clone();
        let command_for_thread = command.clone();
        let prompt_for_thread = prompt.clone();
        let last_message_path_for_thread = last_message_path.clone();
        let stdout_path_for_thread = stdout_path.clone();
        let stderr_path_for_thread = stderr_path.clone();
        let observer_for_thread = observer.clone();
        let handle = thread::spawn(move || {
            if let Some(observer) = observer_for_thread {
                with_engine_observer(observer, || {
                    run_prompted_command_capture(
                        &command_for_thread,
                        &prompt_for_thread,
                        &stdout_path_for_thread,
                        &stderr_path_for_thread,
                        Some(&run_dir_for_thread),
                        Some(&stage_for_thread),
                        Some(&last_message_path_for_thread),
                        true,
                    )
                })
            } else {
                run_prompted_command_capture(
                    &command_for_thread,
                    &prompt_for_thread,
                    &stdout_path_for_thread,
                    &stderr_path_for_thread,
                    Some(&run_dir_for_thread),
                    Some(&stage_for_thread),
                    Some(&last_message_path_for_thread),
                    true,
                )
            }
        });
        out.push_str(&format!(
            "Started {stage}. cache key: {cache_key}. Logs: {}, {}\n",
            stdout_path.display(),
            stderr_path.display()
        ));
        children.push((
            stage,
            handle,
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
        handle,
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
        let result = handle
            .join()
            .map_err(|_| format!("Failed to join {stage} capture thread."))??;
        sync_run_artifacts(ctx, run_dir)?;
        if maybe_recover_codex_solver_outputs(
            StageBackendKind::Codex,
            run_dir,
            &stage,
            &stdout_path,
            &last_message_path,
        )? {
            sync_run_artifacts(ctx, run_dir)?;
        }
        let result = fail_closed_if_stage_outputs_incomplete(
            run_dir,
            &stage,
            &stdout_path,
            &stderr_path,
            result,
        )?;
        if result != 0 && exit_code == 0 {
            exit_code = result;
        }
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
            record_token_usage_with_replacement(
                run_dir,
                &plan,
                &stage,
                "executed",
                &cache_key,
                &prompt_hashes,
                &workspace_hash,
                &token_usage,
                &["provisional"],
            )?;
            record_stage_mcp_usage(
                &plan,
                run_dir,
                &stage,
                stage_backend_label(StageBackendKind::Codex),
                "executed",
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
    if let Some((_summary_path, text)) = effective_user_summary(run_dir) {
        if !text.trim().is_empty() {
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

fn effective_user_summary(run_dir: &Path) -> Option<(PathBuf, String)> {
    let candidates = [
        (
            "verification-summary",
            run_dir.join("verification").join("user-summary.md"),
        ),
        (
            "review-summary",
            run_dir.join("review").join("user-summary.md"),
        ),
    ];
    for (kind, path) in candidates {
        if !path.exists() {
            continue;
        }
        let Ok(text) = read_text(&path) else {
            continue;
        };
        let trimmed = text.trim();
        if trimmed.is_empty() || output_looks_placeholder(kind, trimmed) {
            continue;
        }
        return Some((path, text));
    }
    None
}

fn request_preview(run_dir: &Path) -> Option<(PathBuf, String)> {
    let path = run_dir.join("request.md");
    if !path.exists() {
        return None;
    }
    let Ok(text) = read_text(&path) else {
        return None;
    };
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }
    Some((path, text))
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
    check_run_interrupt(run_dir)?;
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
        check_run_interrupt(run_dir)?;
        for (path, content) in stage_placeholder_content(&plan, run_dir, item)? {
            check_run_interrupt(run_dir)?;
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
    check_run_interrupt(run_dir)?;
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
    check_run_interrupt(run_dir)?;
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
        check_run_interrupt(run_dir)?;
        write_text(&path, &content)?;
    }
    check_run_interrupt(run_dir)?;
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
    ctx: &Context,
    run_dir: &Path,
    stage: &str,
    dry_run: bool,
) -> Result<CommandResult, String> {
    check_run_interrupt(run_dir)?;
    let prompt_text = render_stage_prompt(ctx, run_dir, stage)?;
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

fn refresh_all_stage_prompts(
    ctx: &Context,
    run_dir: &Path,
    dry_run: bool,
) -> Result<CommandResult, String> {
    let stages = available_stages(run_dir)?;
    let mut chunks = Vec::new();
    for (index, stage) in stages.iter().enumerate() {
        check_run_interrupt(run_dir)?;
        let result = refresh_stage_prompt(ctx, run_dir, stage, dry_run)?;
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

fn non_placeholder_artifact_text(run_dir: &Path, rel: &str, label: &str) -> Option<String> {
    let path = run_dir.join(rel);
    if !path.exists() {
        return None;
    }
    let text = read_text(&path).ok()?;
    if output_looks_placeholder(label, &text) {
        None
    } else {
        Some(text.trim().to_string())
    }
}

fn verification_current_facts_path(run_dir: &Path) -> PathBuf {
    run_dir.join("verification").join("current-facts.md")
}

fn build_verification_current_facts_markdown(run_dir: &Path, plan: &Plan) -> String {
    let goal_status =
        read_json::<Value>(&run_dir.join("verification").join("goal-status.json")).ok();
    let host_probe = read_json::<Value>(&host_probe_path(run_dir)).ok();
    let goal_verdict = goal_status
        .as_ref()
        .and_then(|value| value.get("goal_verdict"))
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let recommended_next_action = goal_status
        .as_ref()
        .and_then(|value| value.get("recommended_next_action"))
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let rerun_recommended = goal_status
        .as_ref()
        .and_then(|value| value.get("rerun_recommended"))
        .and_then(Value::as_bool)
        .map(|value| value.to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let top_findings = goal_status
        .as_ref()
        .and_then(|value| value.get("top_findings"))
        .and_then(Value::as_array)
        .map(|items| {
            items.iter()
                .filter_map(Value::as_str)
                .map(|item| format!("- {item}"))
                .collect::<Vec<_>>()
                .join("\n")
        })
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "- none recorded".to_string());
    let host_lines = if let Some(probe) = host_probe {
        let captured_at = probe
            .get("captured_at")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        let preferred = probe
            .get("preferred_torch_device")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        let mps_built = probe
            .get("mps_built")
            .map(|value| value.to_string())
            .unwrap_or_else(|| "null".to_string());
        let mps_available = probe
            .get("mps_available")
            .map(|value| value.to_string())
            .unwrap_or_else(|| "null".to_string());
        format!(
            "- latest launcher probe: `{}`\n- preferred torch device: `{}`\n- launcher `mps_built`: `{}`\n- launcher `mps_available`: `{}`\n",
            captured_at, preferred, mps_built, mps_available
        )
    } else {
        "- latest launcher probe: unavailable\n".to_string()
    };
    let execution_present = first_stage_id_for_kind(plan, run_dir, PipelineStageKind::Execution)
        .ok()
        .flatten()
        .is_some();
    if language_is_russian(&plan.summary_language) {
        format!(
            "# Актуальные факты\n\nЭто авторитетный снимок текущего состояния этого прогона.\nЕсли факты расходятся, опирайся на него, а не на более старые `request.md`, `brief.md` и `review/*`.\n\n## Состояние цели\n\n- вердикт: `{}`\n- рекомендуемое следующее действие: `{}`\n- нужен повторный прогон: `{}`\n- есть execution stage: `{}`\n\n## Последний probe хоста\n\n{}\
\n## Ключевые находки\n\n{}\n",
            goal_verdict,
            recommended_next_action,
            rerun_recommended,
            execution_present,
            host_lines.trim_end(),
            top_findings
        )
    } else {
        format!(
            "# Current Facts\n\nThis is the authoritative current-state snapshot for this run.\nPrefer it over older `request.md`, `brief.md`, and `review/*` artifacts when facts conflict.\n\n## Goal State\n\n- verdict: `{}`\n- recommended next action: `{}`\n- rerun recommended: `{}`\n- execution stage present: `{}`\n\n## Latest Host Probe\n\n{}\
\n## Top Findings\n\n{}\n",
            goal_verdict,
            recommended_next_action,
            rerun_recommended,
            execution_present,
            host_lines.trim_end(),
            top_findings
        )
    }
}

fn write_verification_current_facts(run_dir: &Path, plan: &Plan) -> Result<(), String> {
    let path = verification_current_facts_path(run_dir);
    write_text(&path, &build_verification_current_facts_markdown(run_dir, plan))
}

fn build_follow_up_prompt(
    run_dir: &Path,
    prompt_file: &Path,
    prompt_label: &str,
    prompt_text: &str,
    summary_language: &str,
    operator_note: Option<&str>,
) -> String {
    let run_name = run_dir
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("run");
    let goal_status =
        read_json::<Value>(&run_dir.join("verification").join("goal-status.json")).ok();
    let goal_verdict = goal_status
        .as_ref()
        .and_then(|value| value.get("goal_verdict"))
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let rerun_recommended = goal_status
        .as_ref()
        .and_then(|value| value.get("rerun_recommended"))
        .and_then(Value::as_bool)
        .map(|value| value.to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let recommended_next_action = goal_status
        .as_ref()
        .and_then(|value| value.get("recommended_next_action"))
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let missing_checks = goal_status
        .as_ref()
        .and_then(|value| value.get("missing_checks"))
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(|item| format!("- `{item}`"))
                .collect::<Vec<_>>()
                .join("\n")
        })
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "- none reported".to_string());
    let verification_summary = non_placeholder_artifact_text(
        run_dir,
        "verification/user-summary.md",
        "verification-user-summary",
    )
    .unwrap_or_else(|| {
        if language_is_russian(summary_language) {
            "# Итог проверки\n\nНедоступно.\n".to_string()
        } else {
            "# Verification Summary\n\nUnavailable.\n".to_string()
        }
    });
    let verification_findings =
        non_placeholder_artifact_text(run_dir, "verification/findings.md", "verification-findings")
            .unwrap_or_else(|| {
                if language_is_russian(summary_language) {
                    "# Находки\n\nНедоступно.\n".to_string()
                } else {
                    "# Findings\n\nUnavailable.\n".to_string()
                }
            });
    let current_facts = non_placeholder_artifact_text(
        run_dir,
        "verification/current-facts.md",
        "verification-current-facts",
    )
    .unwrap_or_else(|| {
        if language_is_russian(summary_language) {
            "# Актуальные факты\n\nНедоступно.\n".to_string()
        } else {
            "# Current Facts\n\nUnavailable.\n".to_string()
        }
    });
    let operator_note = operator_note
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let operator_note_section = if let Some(note) = operator_note {
        if language_is_russian(summary_language) {
            format!(
                "\n## Дополнительные указания пользователя\n\nЭто новые комментарии пользователя именно для текущего повторного прогона.\nЕсли они конфликтуют с уже проверенными фактами, не игнорируй конфликт молча: явно опиши расхождение и выбери безопасный вариант.\n\n{}\n",
                note
            )
        } else {
            format!(
                "\n## Additional User Guidance\n\nThis is fresh user input for the current rerun.\nIf it conflicts with already-verified facts, do not ignore the conflict silently: call it out and choose the safer interpretation.\n\n{}\n",
                note
            )
        }
    } else {
        String::new()
    };
    if language_is_russian(summary_language) {
        format!(
            "# Верифицированная задача для повторного прогона\n\nИспользуй только контекст из проверки ниже как авторитетную основу для этого повторного прогона.\nНе пересобирай задачу по старым `request.md`, `brief.md` или `review/*` из предыдущего прогона, если этот документ не повторяет их явно.\n\n## Исходный прогон\n\n- run: `{}`\n- источник запроса: `{}`\n- исходный файл: `{}`\n\n## Статус цели\n\n- вердикт: `{}`\n- нужен повторный прогон: `{}`\n- рекомендуемое следующее действие: `{}`\n- незакрытые критические проверки:\n{}\n\n## Актуальные факты\n\n{}\n\n## Правила повторного прогона\n\n- Рассматривай абсолютные пути под `{}` только как ссылки на артефакты исходного прогона, а не как основное место записи этого повторного прогона.\n- Если текущему повторному прогону нужен обновлённый run-local narrative, пиши его в собственные `review/`, `execution/` или `verification/` артефакты этого прогона, а не переписывай предыдущие run-local файлы.\n- Workspace и пользовательские deliverables вне `agent-runs` по-прежнему можно обновлять на месте, но только если follow-up явно требует mirror.\n- Предпочитай заново изложить и согласовать текущие доказательства в артефактах этого прогона, а не чинить исторические логи исходного прогона.\n\n## Цель текущего повторного прогона\n\n- Закрой только незакрытые критические проверки, перечисленные выше.\n- Используй файлы исходного прогона только как ссылки на доказательства и пересобери актуальный narrative в артефактах текущего прогона.\n- Обновляй workspace mirrors на месте только там, где это действительно требуется пользовательским deliverable.\n- Не редактируй напрямую предыдущие run-local файлы под `{}` без явного запроса пользователя на восстановление архива.\n{}\n## Итог проверки\n\n{}\n\n## Находки проверки\n\n{}\n\n## Исторические указания из исходного прогона\n\n{}\n",
            run_name,
            prompt_label,
            prompt_file.display(),
            goal_verdict,
            rerun_recommended,
            recommended_next_action,
            missing_checks,
            current_facts.trim(),
            run_dir.display(),
            run_dir.display(),
            operator_note_section,
            verification_summary.trim(),
            verification_findings.trim(),
            prompt_text.trim()
        )
    } else {
        format!(
            "# Verified Follow-Up Task\n\nUse only the verification-derived context below as the authoritative seed for this rerun.\nDo not rescope the task from stale `request.md`, `brief.md`, or `review/*` artifacts from the previous run unless this document repeats them explicitly.\n\n## Source Run\n\n- run: `{}`\n- prompt source: `{}`\n- source file: `{}`\n\n## Goal Status\n\n- verdict: `{}`\n- rerun recommended: `{}`\n- recommended next action: `{}`\n- missing critical checks:\n{}\n\n## Current Facts\n\n{}\n\n## Rerun Contract\n\n- Treat absolute paths under `{}` as source-run evidence references, not the primary write target for this rerun.\n- If this rerun needs refreshed run-local narrative, write it under the current rerun's own `review/`, `execution/`, or `verification/` artifacts instead of rewriting previous run-local files.\n- Workspace or user-facing deliverables outside `agent-runs` may still be updated in place when the follow-up explicitly requires a mirror.\n- Prefer restating and reconciling the current evidence in this rerun's own artifacts over repairing historical logs from the source run.\n\n## Current Rerun Objective\n\n- Close only the missing critical checks listed above.\n- Use source-run files as evidence references and refresh the story in this rerun's own artifacts.\n- Update workspace mirrors in place only when the user-facing deliverable actually requires them.\n- Do not directly edit previous run-local files under `{}` unless the user explicitly asks for archival repair.\n{}\n## Verification Summary\n\n{}\n\n## Verification Findings\n\n{}\n\n## Historical Guidance From Source Run\n\n{}\n",
            run_name,
            prompt_label,
            prompt_file.display(),
            goal_verdict,
            rerun_recommended,
            recommended_next_action,
            missing_checks,
            current_facts.trim(),
            run_dir.display(),
            run_dir.display(),
            operator_note_section,
            verification_summary.trim(),
            verification_findings.trim(),
            prompt_text.trim()
        )
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
    write_verification_current_facts(run_dir, &plan)?;
    let follow_up_prompt = build_follow_up_prompt(
        run_dir,
        &prompt_file,
        prompt_label,
        &prompt_text,
        &plan.summary_language,
        args.note.as_deref(),
    );
    if args.dry_run {
        return Ok(CommandResult {
            code: 0,
            stdout: format!(
                "Would create follow-up run from `{}`\nworkspace: {}\noutput-dir: {}\ntitle: {}\nprompt-format: {}\nsummary-language: {}\nintake-research: {}\nstage-research: {}\nexecution-network: {}\ncache-root: {}\ncache-policy: {}\nuser-note: {}\n",
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
                args.note.as_deref().map(str::trim).filter(|value| !value.is_empty()).unwrap_or("none"),
            ),
            stderr: String::new(),
        });
    }
    let new_run = create_run(
        ctx,
        &follow_up_prompt,
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
            "--note" => {
                index += 1;
                args.note = Some(
                    extra
                        .get(index)
                        .ok_or("--note requires a value")?
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

fn stage0_backend_mode(ctx: &Context) -> Stage0BackendMode {
    match ctx.stage0_backend.trim().to_lowercase().as_str() {
        "responses" | "responses-readonly" | "mixed" | "openai" | "responses-api" => {
            Stage0BackendMode::Responses
        }
        "local" => Stage0BackendMode::Local,
        _ => Stage0BackendMode::Codex,
    }
}

fn stage0_backend_name(mode: Stage0BackendMode) -> &'static str {
    match mode {
        Stage0BackendMode::Codex => "codex",
        Stage0BackendMode::Responses => "responses",
        Stage0BackendMode::Local => "local",
    }
}

fn stage0_prompt_path(artifact_dir: &Path, label: &str) -> PathBuf {
    artifact_dir.join(format!("{label}.prompt.md"))
}

fn stage0_last_message_path(artifact_dir: &Path, label: &str) -> PathBuf {
    artifact_dir.join(format!("{label}.last.md"))
}

fn stage0_stdout_path(artifact_dir: &Path, label: &str) -> PathBuf {
    artifact_dir.join(format!("{label}.stdout.log"))
}

fn stage0_stderr_path(artifact_dir: &Path, label: &str) -> PathBuf {
    artifact_dir.join(format!("{label}.stderr.log"))
}

fn stage0_fallback_metadata_path(artifact_dir: &Path, label: &str) -> PathBuf {
    artifact_dir.join(format!("{label}.fallback.json"))
}

fn language_is_russian(language: &str) -> bool {
    language.trim().to_ascii_lowercase().starts_with("ru")
}

fn collapse_whitespace(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn build_local_goal_summary(raw_task: &str) -> String {
    for line in raw_task.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let candidate = trimmed.trim_matches('#').trim();
        if candidate.is_empty() {
            continue;
        }
        let lowered = candidate.to_ascii_lowercase();
        if matches!(
            lowered.as_str(),
            "raw request" | "augmented task" | "final task" | "task" | "brief" | "request"
        ) {
            continue;
        }
        return candidate.chars().take(160).collect();
    }
    collapse_whitespace(raw_task).chars().take(160).collect()
}

fn task_looks_execution_ready(raw_task: &str) -> bool {
    let collapsed = collapse_whitespace(raw_task);
    let lowered = collapsed.to_ascii_lowercase();
    let has_structure = raw_task.contains("##")
        || raw_task
            .lines()
            .filter(|line| line.trim_start().starts_with('-'))
            .count()
            >= 2;
    let has_acceptance_markers = [
        "definition of done",
        "done state",
        "deliverable",
        "goal check",
        "validation",
        "do not regress",
        "verification",
        "критери",
        "готов",
        "не ломать",
        "провер",
        "артефакт",
    ]
    .iter()
    .any(|marker| lowered.contains(marker));
    let has_named_artifacts = [
        "readme",
        "main.py",
        ".py",
        ".rs",
        ".ts",
        ".js",
        ".md",
        "dockerfile",
        "package.json",
    ]
    .iter()
    .any(|marker| lowered.contains(marker));
    let has_output_or_run_contract = [
        "stdout",
        "print",
        "prints",
        "exactly",
        "run command",
        "command to run",
        "entrypoint",
        "entry point",
        "вывод",
        "печата",
        "ровно",
        "команда запуска",
    ]
    .iter()
    .any(|marker| lowered.contains(marker));
    collapsed.chars().count() >= 240
        || (has_structure && has_acceptance_markers)
        || (has_named_artifacts && has_output_or_run_contract)
}

fn task_explicitly_requests_clarification(raw_task: &str) -> bool {
    let lowered = collapse_whitespace(raw_task).to_ascii_lowercase();
    [
        "сначала уточни",
        "сначала уточните",
        "уточни у меня",
        "уточните у меня",
        "задай мне",
        "спроси меня",
        "нужно уточнить",
        "нужно сначала уточнить",
        "before you start ask",
        "ask me first",
        "ask me about",
        "clarify with me",
        "clarify first",
    ]
    .iter()
    .any(|marker| lowered.contains(marker))
}

fn task_has_open_uncertainty(raw_task: &str) -> bool {
    let lowered = collapse_whitespace(raw_task).to_ascii_lowercase();
    [
        "не решил",
        "пока не решил",
        "ещё не решил",
        "не уверен",
        "не знаю",
        "не определился",
        "не определился",
        "какой-нибудь",
        "что он должен делать",
        "не решил точный формат",
        "не решил формат вывода",
        "не решил аргументы",
        "haven't decided",
        "have not decided",
        "not sure",
        "i'm not sure",
        "i am not sure",
        "i don't know yet",
        "i do not know yet",
        "something useful",
        "what it should do",
    ]
    .iter()
    .any(|marker| lowered.contains(marker))
}

fn interview_requires_questions(raw_task: &str) -> bool {
    task_explicitly_requests_clarification(raw_task) || task_has_open_uncertainty(raw_task)
}

fn interview_can_skip_questions(raw_task: &str) -> bool {
    if interview_requires_questions(raw_task) {
        return false;
    }
    if task_requests_code_review(raw_task) {
        return true;
    }
    if task_looks_execution_ready(raw_task) {
        return true;
    }
    let lowered = collapse_whitespace(raw_task).to_ascii_lowercase();
    let has_named_deliverables = ["readme", "main.py", ".py", ".md", "cli"]
        .iter()
        .any(|marker| lowered.contains(marker));
    let has_explicit_contract = [
        "exactly",
        "stdout",
        "prints",
        "print",
        "run command",
        "command to run",
        "команда запуска",
        "ровно",
        "вывод",
    ]
    .iter()
    .any(|marker| lowered.contains(marker));
    has_named_deliverables && has_explicit_contract
}

fn build_local_interview_questions_payload(
    raw_task: &str,
    language: &str,
    max_questions: usize,
) -> Value {
    let mut questions = Vec::new();
    if max_questions > 0 && !interview_can_skip_questions(raw_task) {
        let question = if language_is_russian(language) {
            InterviewQuestion {
                id: "scope_constraints".to_string(),
                question:
                    "Какие ограничения или критерии успеха обязательны для первого рабочего прохода?"
                        .to_string(),
                why: "Локальный fallback сохраняет цель без сужения и просит только один уточняющий сигнал."
                    .to_string(),
                required: false,
            }
        } else {
            InterviewQuestion {
                id: "scope_constraints".to_string(),
                question:
                    "Which constraints or success criteria are mandatory for the first working pass?"
                        .to_string(),
                why: "The local fallback preserves the original goal and asks only one clarification when the task is still underspecified."
                    .to_string(),
                required: false,
            }
        };
        questions.push(question);
    }
    json!({
        "goal_summary": build_local_goal_summary(raw_task),
        "questions": questions.into_iter().take(max_questions).collect::<Vec<_>>(),
    })
}

fn build_local_final_task_markdown(raw_task: &str, qa_pairs: &[Value], language: &str) -> String {
    let mut final_task = raw_task.trim().to_string();
    let answered: Vec<(String, String)> = qa_pairs
        .iter()
        .filter_map(|item| {
            let question = collapse_whitespace(
                item.get("question")
                    .and_then(|value| value.as_str())
                    .unwrap_or(""),
            );
            let answer = collapse_whitespace(
                item.get("answer")
                    .and_then(|value| value.as_str())
                    .unwrap_or(""),
            );
            (!question.is_empty() && !answer.is_empty()).then_some((question, answer))
        })
        .collect();
    let unanswered: Vec<String> = qa_pairs
        .iter()
        .filter_map(|item| {
            let question = collapse_whitespace(
                item.get("question")
                    .and_then(|value| value.as_str())
                    .unwrap_or(""),
            );
            let answer = collapse_whitespace(
                item.get("answer")
                    .and_then(|value| value.as_str())
                    .unwrap_or(""),
            );
            (!question.is_empty() && answer.is_empty()).then_some(question)
        })
        .collect();
    if !answered.is_empty() {
        let heading = if language_is_russian(language) {
            "## Уточнения"
        } else {
            "## Clarifications"
        };
        final_task.push_str("\n\n");
        final_task.push_str(heading);
        final_task.push_str("\n\n");
        for (question, answer) in answered {
            final_task.push_str(&format!("- {question}: {answer}\n"));
        }
    }
    if !unanswered.is_empty() {
        let heading = if language_is_russian(language) {
            "## Открытые вопросы"
        } else {
            "## Open Questions"
        };
        final_task.push_str("\n");
        final_task.push_str(heading);
        final_task.push_str("\n\n");
        for question in unanswered {
            final_task.push_str(&format!("- {question}\n"));
        }
    }
    final_task.push('\n');
    final_task
}

fn stage0_backend_failure_evidence(artifact_dir: &Path, label: &str, err: &str) -> String {
    let mut parts = vec![err.trim().to_string()];
    for path in [
        stage0_stderr_path(artifact_dir, label),
        stage0_stdout_path(artifact_dir, label),
    ] {
        if let Ok(text) = read_text(&path) {
            let trimmed = text.trim();
            if !trimmed.is_empty() {
                parts.push(trimmed.to_string());
            }
        }
    }
    parts.join("\n")
}

fn classify_stage0_backend_unavailable(
    mode: Stage0BackendMode,
    artifact_dir: &Path,
    label: &str,
    err: &str,
) -> Option<String> {
    if mode == Stage0BackendMode::Local {
        return None;
    }
    let evidence = stage0_backend_failure_evidence(artifact_dir, label, err);
    let lowered = evidence.to_ascii_lowercase();
    let contract_markers = [
        "interview agent did not return a json object",
        "interview agent returned invalid json",
        "interview agent returned json, but it was not an object",
        "responses api returned invalid structured json",
        "responses api did not return structured json output text",
        "without writing the expected last-message artifact",
        "did not return message text",
        "exit code 130",
        "interrupt",
    ];
    if contract_markers
        .iter()
        .any(|marker| lowered.contains(marker))
    {
        return None;
    }
    let backend_markers = match mode {
        Stage0BackendMode::Codex => &[
            "failed to run command",
            "no such file or directory",
            "dns",
            "lookup",
            "websocket",
            "https",
            "tls",
            "network",
            "transport",
            "connection refused",
            "connection reset",
            "connection aborted",
            "timed out",
            "timeout",
            "not logged",
            "unauthorized",
            "authentication",
            "forbidden",
            "service unavailable",
        ][..],
        Stage0BackendMode::Responses => &[
            "openai_api_key",
            "http request to",
            "status 401",
            "status 403",
            "status 429",
            "status 500",
            "status 502",
            "status 503",
            "status 504",
            "dns",
            "lookup",
            "timed out",
            "timeout",
            "connection refused",
            "connection reset",
            "connection aborted",
            "tls",
            "network",
        ][..],
        Stage0BackendMode::Local => &[][..],
    };
    backend_markers
        .iter()
        .any(|marker| lowered.contains(marker))
        .then_some(evidence)
}

fn write_stage0_fallback_metadata(
    artifact_dir: &Path,
    label: &str,
    requested_backend: &str,
    reason_kind: &str,
    reason: &str,
    inputs: &[PathBuf],
) -> Result<(), String> {
    append_log_line(
        &stage0_stdout_path(artifact_dir, label),
        &format!(
            "stage0 backend requested={} effective=local reason_kind={}",
            requested_backend, reason_kind
        ),
    )?;
    append_log_line(
        &stage0_stderr_path(artifact_dir, label),
        &format!(
            "local fallback engaged: {}",
            reason.lines().next().unwrap_or(reason)
        ),
    )?;
    write_json(
        &stage0_fallback_metadata_path(artifact_dir, label),
        &json!({
            "created_at": iso_timestamp(),
            "label": label,
            "requested_backend": requested_backend,
            "effective_backend": "local",
            "reason_kind": reason_kind,
            "reason": reason,
            "input_artifacts": inputs.iter().map(|path| path.display().to_string()).collect::<Vec<_>>(),
        }),
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
    interrupt_run_dir: Option<&Path>,
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
    let mut command = command;
    if env_flag("AGPIPE_CODEX_EPHEMERAL").unwrap_or(false) {
        command.insert(2, "--ephemeral".to_string());
    }
    if let Some(agency_root) = discover_agency_agents_dir(ctx) {
        if agency_root != ctx.repo_root && agency_root != workdir {
            let insert_at = command.len().saturating_sub(3);
            command.insert(insert_at, "--add-dir".to_string());
            command.insert(insert_at + 1, agency_root.display().to_string());
        }
    }
    let code = run_prompted_command_capture(
        &command,
        prompt,
        &stdout_path,
        &stderr_path,
        interrupt_run_dir,
        None,
        None,
        true,
    )?;
    if code != 0 {
        let detail = read_text(&stderr_path)
            .or_else(|_| read_text(&stdout_path))
            .unwrap_or_default();
        let last_message_status = if !last_message_path.exists() {
            format!(
                "The last-message artifact was not written at {}",
                last_message_path.display()
            )
        } else {
            match read_text(&last_message_path) {
                Ok(text) if text.trim().is_empty() => format!(
                    "The last-message artifact exists but is empty at {}",
                    last_message_path.display()
                ),
                Ok(_) => format!(
                    "A partial last-message artifact was written at {}",
                    last_message_path.display()
                ),
                Err(err) => format!(
                    "The last-message artifact could not be read at {} ({err})",
                    last_message_path.display()
                ),
            }
        };
        return Err(format!(
            "codex exec failed during `{label}` with exit code {code}. {last_message_status}. See {} and {}. {}",
            stdout_path.display(),
            stderr_path.display(),
            detail.chars().take(400).collect::<String>()
        ));
    }
    if !last_message_path.exists() {
        return Err(format!(
            "codex exec completed `{label}` without writing the expected last-message artifact at {}. See {} and {}.",
            last_message_path.display(),
            stdout_path.display(),
            stderr_path.display(),
        ));
    }
    let last_message = read_text(&last_message_path)?;
    if last_message.trim().is_empty() {
        return Err(format!(
            "codex exec completed `{label}` but the last-message artifact is empty at {}. See {} and {} for the upstream failure details.",
            last_message_path.display(),
            stdout_path.display(),
            stderr_path.display(),
        ));
    }
    Ok(last_message)
}

fn run_stage0_last_message(
    ctx: &Context,
    prompt: &str,
    workdir: &Path,
    artifact_dir: &Path,
    label: &str,
) -> Result<String, String> {
    let interrupt_run_dir = current_interrupt_run_dir();
    match stage0_backend_mode(ctx) {
        Stage0BackendMode::Responses => {
            let text_format = responses_text_format_for_label(label);
            run_responses_last_message(
                ctx,
                prompt,
                workdir,
                artifact_dir,
                label,
                interrupt_run_dir.as_deref(),
                text_format.as_ref(),
            )
        }
        Stage0BackendMode::Codex => run_codex_last_message(
            ctx,
            prompt,
            workdir,
            artifact_dir,
            label,
            interrupt_run_dir.as_deref(),
        ),
        Stage0BackendMode::Local => Err(
            "Local stage0 backend is handled directly by the interview fallback helpers."
                .to_string(),
        ),
    }
}

fn build_interview_questions_prompt(
    raw_task: &str,
    workspace: &Path,
    language: &str,
    max_questions: usize,
) -> String {
    let execution_ready = interview_can_skip_questions(raw_task);
    let explicit_clarification_request = task_explicitly_requests_clarification(raw_task);
    let open_uncertainty = task_has_open_uncertainty(raw_task);
    format!(
        "You are the stage0 interview agent for multi-agent-pipeline.\n\nYour job is to read the raw request, inspect the workspace only when it changes a concrete clarification question, and ask the domain-specific questions that are actually needed before building a downstream-ready task prompt.\n\nRaw task:\n{}\n\nWorkspace:\n- path: `{}`\n- exists: `{}`\n\nLauncher readiness hints:\n- task_looks_execution_ready: `{}`\n- explicit_clarification_request: `{}`\n- task_has_open_uncertainty: `{}`\n\nUse the configured structured output schema for the final answer.\n\nRules:\n- preserve the original goal exactly; do not shrink it\n- if `explicit_clarification_request=true` or `task_has_open_uncertainty=true`, ask at least one concrete clarification question unless `max_questions=0`\n- only return an empty `questions` list when the task is already downstream-ready and no explicit clarification is requested\n- ask all important domain questions that materially affect decomposition, implementation, review scope, or goal verification\n- ask no more than {} questions\n- avoid questions already answered by the raw task\n- prefer concrete engineering questions over generic project-management questions\n- do not inspect multi-agent-pipeline source files, test files, or tool internals just to infer the response shape; use the provided structured output schema directly\n- do not assume the workspace is a git repository; missing `.git` is not a blocker for asking good questions\n- inspect the workspace only when it can reveal a missing constraint or compatibility risk that would change the questions\n- write user-facing questions and reasons in {}\n",
        raw_task.trim(),
        workspace.display(),
        workspace.exists(),
        execution_ready,
        explicit_clarification_request,
        open_uncertainty,
        max_questions,
        language
    )
}

fn build_interview_finalize_prompt(raw_task: &str, qa_pairs: &[Value], language: &str) -> String {
    let qa_json = serde_json::to_string_pretty(qa_pairs).unwrap_or_else(|_| "[]".to_string());
    format!(
        "You are the stage0 prompt builder for multi-agent-pipeline.\n\nYour job is to turn the raw request plus clarification answers into the final task prompt that will be passed into the Rust agpipe intake and create-run flow.\n\nRaw task:\n{}\n\nClarifications:\n{}\n\nWrite the final task in {}. Return markdown only, with no code fences.\n\nRules:\n- preserve the original goal exactly; do not downgrade it to scaffold-only or architecture-only\n- incorporate the answered constraints and preferences directly\n- carry forward unresolved uncertainties as explicit blockers or open assumptions\n- make the task ready for downstream agents; if the user asked for review-only work, keep it review-only instead of silently converting it into implementation work\n- include what counts as done\n- include do-not-regress constraints when the answers imply them\n",
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
    let artifact_dir = session_dir.join("logs");
    fs::create_dir_all(&artifact_dir)
        .map_err(|err| format!("Could not create {}: {err}", session_dir.display()))?;
    write_text(&session_dir.join("raw-task.md"), raw_task)?;
    let prompt = build_interview_questions_prompt(raw_task, workspace, language, max_questions);
    write_text(
        &stage0_prompt_path(&artifact_dir, "interview-questions"),
        &prompt,
    )?;
    if interview_can_skip_questions(raw_task) {
        let payload = build_local_interview_questions_payload(raw_task, language, 0);
        let rendered = serde_json::to_string_pretty(&payload).map_err(|err| {
            format!("Could not serialize execution-ready stage0 questions: {err}")
        })?;
        write_text(
            &stage0_last_message_path(&artifact_dir, "interview-questions"),
            &rendered,
        )?;
        write_stage0_fallback_metadata(
            &artifact_dir,
            "interview-questions",
            stage0_backend_name(stage0_backend_mode(ctx)),
            "execution-ready-fast-path",
            "The raw task already includes concrete deliverables, success criteria, and validation expectations, so stage0 skipped model-driven interview questions.",
            &[session_dir.join("raw-task.md")],
        )?;
        write_json(&session_dir.join("questions.json"), &payload)?;
        return Ok((session_dir, payload));
    }
    let cwd =
        env::current_dir().map_err(|err| format!("Could not read current directory: {err}"))?;
    let workdir = if workspace.exists() {
        workspace.to_path_buf()
    } else {
        cwd
    };
    let requested_backend = stage0_backend_name(stage0_backend_mode(ctx));
    let mut payload = match stage0_backend_mode(ctx) {
        Stage0BackendMode::Local => {
            let payload =
                build_local_interview_questions_payload(raw_task, language, max_questions);
            let rendered = serde_json::to_string_pretty(&payload)
                .map_err(|err| format!("Could not serialize local stage0 questions: {err}"))?;
            write_text(
                &stage0_last_message_path(&artifact_dir, "interview-questions"),
                &rendered,
            )?;
            write_stage0_fallback_metadata(
                &artifact_dir,
                "interview-questions",
                requested_backend,
                "configured-local",
                "AGPIPE_STAGE0_BACKEND=local",
                &[session_dir.join("raw-task.md")],
            )?;
            payload
        }
        mode => {
            let raw_questions = match run_stage0_last_message(
                ctx,
                &prompt,
                &workdir,
                &artifact_dir,
                "interview-questions",
            ) {
                Ok(value) => value,
                Err(err) => {
                    let Some(reason) = classify_stage0_backend_unavailable(
                        mode,
                        &artifact_dir,
                        "interview-questions",
                        &err,
                    ) else {
                        return Err(err);
                    };
                    let payload =
                        build_local_interview_questions_payload(raw_task, language, max_questions);
                    let rendered =
                        serde_json::to_string_pretty(&payload).map_err(|serialize_err| {
                            format!("Could not serialize local stage0 questions: {serialize_err}")
                        })?;
                    write_text(
                        &stage0_last_message_path(&artifact_dir, "interview-questions"),
                        &rendered,
                    )?;
                    write_stage0_fallback_metadata(
                        &artifact_dir,
                        "interview-questions",
                        requested_backend,
                        "backend-unavailable",
                        &reason,
                        &[session_dir.join("raw-task.md")],
                    )?;
                    rendered
                }
            };
            extract_json_object(&raw_questions)?
        }
    };
    let empty_questions = payload
        .get("questions")
        .and_then(|value| value.as_array())
        .map(|items| items.is_empty())
        .unwrap_or(true);
    if empty_questions
        && interview_requires_questions(raw_task)
        && !interview_can_skip_questions(raw_task)
    {
        payload = build_local_interview_questions_payload(raw_task, language, max_questions);
        let rendered = serde_json::to_string_pretty(&payload)
            .map_err(|err| format!("Could not serialize guarded local stage0 questions: {err}"))?;
        write_text(
            &stage0_last_message_path(&artifact_dir, "interview-questions"),
            &rendered,
        )?;
        write_stage0_fallback_metadata(
            &artifact_dir,
            "interview-questions",
            requested_backend,
            "empty-questions-guardrail",
            "Stage0 returned an empty questions list even though the raw task explicitly requested clarification or left critical uncertainty unresolved.",
            &[session_dir.join("raw-task.md")],
        )?;
    }
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
    let artifact_dir = session_dir.join("logs");
    fs::create_dir_all(&artifact_dir)
        .map_err(|err| format!("Could not create {}: {err}", artifact_dir.display()))?;
    write_text(
        &stage0_prompt_path(&artifact_dir, "interview-finalize"),
        &prompt,
    )?;
    let cwd =
        env::current_dir().map_err(|err| format!("Could not read current directory: {err}"))?;
    let workdir = if workspace.exists() {
        workspace.to_path_buf()
    } else {
        cwd
    };
    let requested_backend = stage0_backend_name(stage0_backend_mode(ctx));
    let final_task_text = match stage0_backend_mode(ctx) {
        Stage0BackendMode::Local => {
            let final_task_text = build_local_final_task_markdown(raw_task, qa_pairs, language);
            write_text(
                &stage0_last_message_path(&artifact_dir, "interview-finalize"),
                &final_task_text,
            )?;
            write_stage0_fallback_metadata(
                &artifact_dir,
                "interview-finalize",
                requested_backend,
                "configured-local",
                "AGPIPE_STAGE0_BACKEND=local",
                &[
                    session_dir.join("raw-task.md"),
                    session_dir.join("answers.json"),
                ],
            )?;
            final_task_text
        }
        mode => match run_stage0_last_message(
            ctx,
            &prompt,
            &workdir,
            &artifact_dir,
            "interview-finalize",
        ) {
            Ok(value) => value,
            Err(err) => {
                let Some(reason) = classify_stage0_backend_unavailable(
                    mode,
                    &artifact_dir,
                    "interview-finalize",
                    &err,
                ) else {
                    return Err(err);
                };
                let final_task_text = build_local_final_task_markdown(raw_task, qa_pairs, language);
                write_text(
                    &stage0_last_message_path(&artifact_dir, "interview-finalize"),
                    &final_task_text,
                )?;
                write_stage0_fallback_metadata(
                    &artifact_dir,
                    "interview-finalize",
                    requested_backend,
                    "backend-unavailable",
                    &reason,
                    &[
                        session_dir.join("raw-task.md"),
                        session_dir.join("answers.json"),
                    ],
                )?;
                final_task_text
            }
        },
    };
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
        notify_stage_changed(&next);
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

pub fn execute_doctor_fix(ctx: &Context, run_dir: &Path) -> Result<CommandResult, String> {
    let report = doctor_report(ctx, run_dir)?;
    if report.fix_actions.is_empty() {
        return Ok(CommandResult::ok(
            "Doctor found no machine-actionable fix.\n",
        ));
    }
    let mut stdout = format!(
        "Doctor auto-fix plan:\n- {}\n",
        report.fix_actions.join("\n- ")
    );
    let mut requested_interrupt = false;
    for action in &report.fix_actions {
        let result = execute_named_action(ctx, run_dir, action)?;
        if action.trim() == "interrupt" {
            requested_interrupt = true;
        }
        stdout.push_str(&format!("\n=== action: {action} ===\n"));
        if !result.stdout.trim().is_empty() {
            stdout.push_str(result.stdout.trim_end());
            stdout.push('\n');
        }
        if !result.stderr.trim().is_empty() {
            stdout.push_str(result.stderr.trim_end());
            stdout.push('\n');
        }
        if result.code != 0 {
            return Ok(CommandResult {
                code: result.code,
                stdout,
                stderr: String::new(),
            });
        }
    }
    if requested_interrupt {
        stdout.push_str(
            "\nInterrupt was requested. Refresh doctor/status after the current stage acknowledges cancellation.\n",
        );
        return Ok(CommandResult {
            code: 0,
            stdout,
            stderr: String::new(),
        });
    }
    let refreshed = doctor_report(ctx, run_dir)?;
    stdout.push_str("\nDoctor after fix:\n\n");
    stdout.push_str(&doctor_compact_text(&refreshed));
    stdout.push('\n');
    Ok(CommandResult {
        code: 0,
        stdout,
        stderr: String::new(),
    })
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
    if trimmed == "host-probe --refresh" {
        return run_stage_capture(ctx, run_dir, "host-probe", &["--refresh"]);
    }
    if trimmed == "interrupt" {
        crate::runtime::request_interrupt(run_dir)?;
        return Ok(CommandResult::ok("Interrupt requested for the current stage."));
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
    check_run_interrupt(run_dir)?;
    append_amendment(run_dir, note)?;
    let amendment_path = run_dir.join("amendments.md");
    let mut details = format!("Recorded amendment in {}\n", amendment_path.display());
    if rewind != "none" {
        check_run_interrupt(run_dir)?;
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
        check_run_interrupt(run_dir)?;
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
    check_run_interrupt(run_dir)?;
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
    if let Some((path, text)) = effective_user_summary(run_dir) {
        let trimmed = text.trim();
        if !trimmed.is_empty() {
            let label = match path
                .parent()
                .and_then(|value| value.file_name())
                .and_then(|value| value.to_str())
            {
                Some("verification") => "Verification Summary",
                Some("review") => "Review Summary",
                _ => "Summary",
            };
            return (
                label.to_string(),
                trimmed.chars().take(max_chars).collect(),
            );
        }
    }
    if let Some((_path, text)) = request_preview(run_dir) {
        let trimmed = text.trim();
        if !trimmed.is_empty() {
            return (
                "Request".to_string(),
                trimmed.chars().take(max_chars).collect(),
            );
        }
    }
    let candidates = [
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
        .filter(|path| path.is_file())
        .collect();
    if logs.is_empty() {
        return None;
    }
    logs.sort_by_cached_key(|path| {
        (
            log_path_has_visible_content(path),
            path.metadata().and_then(|meta| meta.modified()).ok(),
            path.clone(),
        )
    });
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

fn log_path_has_visible_content(path: &Path) -> bool {
    fs::read_to_string(path)
        .map(|content| content.lines().any(|line| !line.trim().is_empty()))
        .unwrap_or(false)
}

fn stage_live_log_path(logs_dir: &Path, stage: &str) -> Option<PathBuf> {
    let mut fallback = None;
    for candidate in stage_live_log_candidates(stage) {
        let path = logs_dir.join(candidate);
        if path.exists() {
            if fallback.is_none() {
                fallback = Some(path.clone());
            }
            if log_path_has_visible_content(&path) {
                return Some(path);
            }
        }
    }
    fallback
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
    if !run_has_plan_artifact(run_dir) {
        return Err(format!(
            "Missing plan artifact in run directory: {}",
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
            let fix = extra.contains(&"--fix");
            if as_json && fix {
                return Err("doctor does not support combining --json with --fix".to_string());
            }
            if fix {
                return execute_doctor_fix(ctx, run_dir);
            }
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
            refresh_stage_prompt(ctx, run_dir, &stage, args.dry_run)
        }
        "refresh-prompts" => {
            let dry_run = extra.contains(&"--dry-run");
            refresh_all_stage_prompts(ctx, run_dir, dry_run)
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
        amend_run, automate_run, available_stages, build_cache_config, build_codex_command,
        build_responses_stage_prompt, cache_lock, cache_lock_owner_path, capture_host_probe,
        choose_roles, compile_prompt, contextual_log_excerpt, create_follow_up_run, create_run,
        choose_roles_with_workstreams,
        current_unix_secs, default_pipeline_includes_execution, default_pipeline_stage_specs,
        detect_host_facts, detect_local_template,
        doctor_report, ensure_cache_layout, execute_doctor_fix, finalize_interview_prompt,
        first_stage_id_for_kind, generate_interview_questions, host_probe_path, host_probe_state,
        infer_execution_mode, infer_task_kind, is_stage_complete, latest_log_file, load_plan,
        load_service_check_spec, materialize_embedded_repo_root,
        maybe_copy_interview_artifacts, next_stage_for_run, output_looks_placeholder,
        recover_review_artifacts_from_stdout_transcript,
        recover_solver_artifact_from_stdout_transcript,
        recover_verification_artifacts_from_stdout_transcript,
        persist_stage_mcp_note, preview_text, prompt_prefers_lightweight_validation,
        provision_selected_mcp_servers, read_file_digest, read_json, read_text,
        read_mcp_usage_records, record_stage_mcp_usage, record_token_usage_with_replacement,
        render_mcp_accountability_rules, render_review_prompt, render_solver_prompt, render_stage_prompt,
        render_verification_prompt, require_valid_order,
        restore_stage_cache, reviewer_stack_for, reviewer_stack_for_task, run_codex_last_message,
        run_has_plan_artifact, run_prompted_command_capture, run_reference_asset_path,
        run_stage0_last_message, run_stage_capture, runtime_check_required, runtime_check_run,
        safe_next_action_for_run, save_plan, solver_count_for, solver_memory_namespace,
        stage_cache_key, stage_memory_namespace, stage_prompt_path, stage_rank, status_report,
        status_text, store_stage_cache, summarize_token_ledger, sync_run_artifacts,
        task_flow_capture, task_forbids_workspace_changes, task_is_analysis_only,
        task_requests_workspace_changes, verification_workspace_documents, with_engine_observer,
        workstream_hints_for, write_json, write_responses_stage_outputs, write_text,
        CacheLockOwner, Context, EngineObserver, InterviewFinalizePayload,
        InterviewQuestionsPayload, LocalTemplateKind, McpProvisionRecord, McpSelection,
        McpServerPlan, McpUsageRecord, PipelineConfig, PipelineStageKind, Plan,
        PromptCacheHashes, RerunArgs, RunTokenLedger, RunTokenLedgerEntry, SolverRole,
        StageBackendKind, StartArgs, TokenBudget, TokenUsage, WorkstreamHint,
        DECOMPOSITION_RULES_REF, MCP_USAGE_LOG_REF, REVIEW_RUBRIC_REF, ROLE_MAP_REF,
        STAGE_RESULTS_AREA, VERIFICATION_RUBRIC_REF,
    };
    use crate::runtime;
    use serde_json::{json, Value};
    use std::env;
    use std::fs;
    use std::io::{BufRead, BufReader, Read as _, Write as _};
    use std::net::TcpListener;
    use std::os::unix::fs::PermissionsExt;
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    struct InterruptObserver {
        run_dir: PathBuf,
    }

    impl EngineObserver for InterruptObserver {
        fn interrupt_run_dir(&self) -> Option<PathBuf> {
            Some(self.run_dir.clone())
        }
    }

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

    struct ScopedEnvVar {
        key: String,
        previous: Option<String>,
    }

    impl ScopedEnvVar {
        fn set(key: &str, value: &str) -> Self {
            let previous = env::var(key).ok();
            env::set_var(key, value);
            Self {
                key: key.to_string(),
                previous,
            }
        }
    }

    impl Drop for ScopedEnvVar {
        fn drop(&mut self) {
            if let Some(value) = &self.previous {
                env::set_var(&self.key, value);
            } else {
                env::remove_var(&self.key);
            }
        }
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

    fn test_context_with_sibling_agency(name: &str) -> (Context, PathBuf, PathBuf, PathBuf) {
        let root = temp_dir(&format!("agency-catalog-{name}"));
        let repo_root = root.join("workspace").join("multi-agent-pipeline");
        let agency_root = root.join("agency-agents");
        let workspace = root.join("workspace-root");
        fs::create_dir_all(&repo_root).expect("create repo root");
        fs::create_dir_all(&agency_root).expect("create agency root");
        fs::create_dir_all(&workspace).expect("create workspace root");
        let mut ctx = test_context();
        ctx.repo_root = repo_root;
        (ctx, root, agency_root, workspace)
    }

    fn write_agency_role(
        agency_root: &Path,
        relative: &str,
        name: &str,
        description: &str,
        body: &str,
    ) -> PathBuf {
        let path = agency_root.join(relative);
        write_text(
            &path,
            &format!("---\nname: {name}\ndescription: {description}\ncolor: cyan\n---\n\n{body}"),
        )
        .expect("write agency role");
        path
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

    fn mock_script_context(name: &str, script: &str) -> (Context, PathBuf) {
        let root = temp_dir(&format!("mock-script-{name}"));
        let bin_path = root.join("mock-script.zsh");
        write_text(&bin_path, script).expect("write mock script");
        let mut permissions = fs::metadata(&bin_path)
            .expect("stat mock script")
            .permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&bin_path, permissions).expect("chmod mock script");
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
        )
    }

    fn mock_parallel_solver_context(name: &str, solver_sleep_secs: u64) -> (Context, PathBuf) {
        let root = temp_dir(&format!("mock-parallel-solver-{name}"));
        let bin_path = root.join("mock-codex.zsh");
        let script = format!(
            r##"#!/bin/zsh
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
if [[ -z "$last_message" ]]; then
  echo "missing --output-last-message" >&2
  exit 2
fi

label="${{${{last_message:t}}%.last.md}}"
root="${{last_message:h:h}}"
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
    python3 -c 'import pathlib, time, sys; pathlib.Path(sys.argv[1]).write_text(f"{{time.time():.6f}}\n", encoding="utf-8")' "$root/solutions/$label/started-at.txt"
    sleep {solver_sleep_secs}
    cat > "$root/solutions/$label/RESULT.md" <<EOF
# Result

Mock solution from $label.
EOF
    cat > "$last_message" <<EOF
Mock $label complete.
EOF
    ;;
  *)
    cat > "$last_message" <<EOF
Mock $label complete.
EOF
    ;;
esac

print "mock $label complete"
"##,
            solver_sleep_secs = solver_sleep_secs
        );
        write_text(&bin_path, &script).expect("write mock parallel solver codex");
        let mut permissions = fs::metadata(&bin_path)
            .expect("stat mock parallel solver codex")
            .permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&bin_path, permissions).expect("chmod mock parallel solver codex");
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
        )
    }

    fn read_solver_started_at(run_dir: &Path, stage: &str) -> f64 {
        read_text(&run_dir.join("solutions").join(stage).join("started-at.txt"))
            .expect("read solver started-at")
            .trim()
            .trim_end_matches("\\n")
            .parse::<f64>()
            .expect("parse solver started-at")
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
  "output_text":"{\n  \"brief_md\": \"# Brief\\n\\nUse the Responses backend for non-execution stages and keep execution on Codex until local tools are ported.\\n\",\n  \"plan_json\": {\n    \"created_at\": \"mock\",\n    \"workspace\": \"/tmp/mock-workspace\",\n    \"workspace_exists\": true,\n    \"original_task\": \"Mock Responses migration\",\n    \"task_kind\": \"migration\",\n    \"complexity\": \"complex\",\n    \"execution_mode\": \"full\",\n    \"prompt_format\": \"compact\",\n    \"summary_language\": \"ru\",\n    \"intake_research_mode\": \"research-first\",\n    \"stage_research_mode\": \"local-first\",\n    \"execution_network_mode\": \"fetch-if-needed\",\n    \"cache\": {\n      \"enabled\": true,\n      \"root\": \"/tmp/mock-cache\",\n      \"policy\": \"reuse\"\n    },\n    \"token_budget\": {\n      \"total_tokens\": 50000,\n      \"warning_threshold_tokens\": 40000,\n      \"source\": \"mock\"\n    },\n    \"host_facts\": {\n      \"source\": \"mock\",\n      \"preferred_torch_device\": \"cpu\"\n    },\n    \"solver_count\": 1,\n    \"solver_roles\": [\n      {\n        \"solver_id\": \"solver-a\",\n        \"role\": \"implementation-engineer\",\n        \"angle\": \"implementation-first\"\n      }\n    ],\n    \"workstream_hints\": [\n      {\n        \"goal\": \"native-runtime-parity\",\n        \"name\": \"native-runtime-parity\",\n        \"suggested_role\": \"implementation-engineer\"\n      }\n    ],\n    \"goal_gate_enabled\": true,\n    \"augmented_follow_up_enabled\": true,\n    \"goal_checks\": [\n      {\n        \"id\": \"responses-non-execution\",\n        \"requirement\": \"Run non-execution stages through Responses backend\",\n        \"critical\": true\n      }\n    ],\n    \"reviewer_stack\": [\n      \"testing/testing-reality-checker.md\"\n    ],\n    \"validation_commands\": [\n      \"cargo test\"\n    ],\n    \"references\": {}\n  },\n  \"mcp_usage_md\": \"- fetch: not used; local context was sufficient.\\n- memory: not used; no cross-stage handoff was needed.\"\n}",
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
  "output_text":"{\n  \"result_md\": \"# Result\\n\\nMock solver output produced through Responses backend.\\n\",\n  \"mcp_usage_md\": \"- fetch: used to verify one official URL.\\n- memory: not used; no durable handoff was required.\"\n}",
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
  "output_text":"{\n  \"report_md\": \"# Review Report\\n\\nMock review selected solver-a.\\n\",\n  \"scorecard_json\": {\n    \"winner\": \"solver-a\",\n    \"selected\": \"solver-a\",\n    \"why\": \"Mock best result\"\n  },\n  \"user_summary_md\": \"# User Summary\\n\\nMock localized review summary.\\n\",\n  \"mcp_usage_md\": \"- fetch: not used during comparison.\\n- memory: used for the review handoff namespace.\"\n}",
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
  "output_text":"{\n  \"findings_md\": \"# Findings\\n\\nNo critical findings in mock verification.\\n\",\n  \"user_summary_md\": \"# Verification Summary\\n\\nMock verification summary.\\n\",\n  \"improvement_request_md\": \"# Improvement Request\\n\\nNo rerun required.\\n\",\n  \"augmented_task_md\": \"# Augmented Task\\n\\nKeep the current verified state.\\n\",\n  \"goal_status_json\": {\n    \"goal_complete\": true,\n    \"goal_verdict\": \"complete\",\n    \"rerun_recommended\": false,\n    \"recommended_next_action\": \"none\"\n  },\n  \"mcp_usage_md\": \"- fetch: not used; verification stayed local.\\n- memory: used to read the review-stage handoff.\"\n}",
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

    fn seed_preexecution_outputs(run_dir: &Path, plan: &Plan) {
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
    }

    #[test]
    fn runtime_check_rest_alias_defaults_to_http_200_for_scenarios_and_steps() {
        let ctx = test_context();
        let workspace = temp_dir("runtime-check-rest-alias-workspace");
        let output_root = temp_dir("runtime-check-rest-alias-output");
        let cache_root = temp_dir("runtime-check-rest-alias-cache");
        let run_dir = create_run(
            &ctx,
            "Проверить runtime-check spec alias normalization.",
            &workspace,
            &output_root,
            Some("runtime-check-rest-alias"),
            "compact",
            "ru",
            "research-first",
            "local-first",
            "fetch-if-needed",
            &cache_root.display().to_string(),
            "reuse",
            None,
        )
        .expect("create runtime-check alias run");

        write_text(
            &workspace.join(".agpipe").join("runtime-check.json"),
            &serde_json::to_string_pretty(&json!({
                "version": 1,
                "mode": "workflow",
                "workdir": ".",
                "scenarios": [
                    {
                        "id": "rest-scenario",
                        "kind": "rest",
                        "url": "https://example.invalid/rest"
                    },
                    {
                        "id": "rest-step",
                        "kind": "workflow",
                        "steps": [
                            {
                                "kind": "rest",
                                "url": "https://example.invalid/step"
                            }
                        ]
                    }
                ]
            }))
            .expect("serialize rest alias spec"),
        )
        .expect("write rest alias spec");

        let (_, _, spec) = load_service_check_spec(&run_dir, None)
            .expect("load runtime-check spec")
            .expect("discovered runtime-check spec");

        assert_eq!(spec.scenarios[0].kind, "rest");
        assert_eq!(spec.scenarios[0].expect_status, 200);
        assert_eq!(spec.scenarios[1].steps[0].kind, "rest");
        assert_eq!(spec.scenarios[1].steps[0].expect_status, 200);

        let _ = fs::remove_dir_all(run_dir);
        let _ = fs::remove_dir_all(output_root);
        let _ = fs::remove_dir_all(workspace);
        let _ = fs::remove_dir_all(cache_root);
    }

    #[test]
    fn runtime_check_workflow_enforces_the_pty_exit_code() {
        let ctx = test_context();
        let workspace = temp_dir("runtime-check-workflow-exit-workspace");
        let output_root = temp_dir("runtime-check-workflow-exit-output");
        let cache_root = temp_dir("runtime-check-workflow-exit-cache");
        let run_dir = create_run(
            &ctx,
            "Проверить workflow runtime-check через PTY.",
            &workspace,
            &output_root,
            Some("runtime-check-workflow-exit"),
            "compact",
            "ru",
            "research-first",
            "local-first",
            "fetch-if-needed",
            &cache_root.display().to_string(),
            "reuse",
            None,
        )
        .expect("create workflow runtime-check run");

        write_text(
            &workspace.join("bad.py"),
            "import sys\nprint(\"BOOT\", flush=True)\nsys.exit(7)\n",
        )
        .expect("write bad pty script");
        write_text(
            &workspace.join(".agpipe").join("runtime-check.json"),
            &serde_json::to_string_pretty(&json!({
                "version": 1,
                "mode": "workflow",
                "workdir": ".",
                "scenarios": [
                    {
                        "id": "bad-workflow",
                        "kind": "workflow",
                        "expect_exit_code": 0,
                        "steps": [
                            {
                                "kind": "pty_start",
                                "command": "python3 bad.py",
                                "rows": 20,
                                "cols": 80
                            },
                            {
                                "kind": "pty_wait_contains",
                                "pattern": "BOOT",
                                "timeout_secs": 2
                            }
                        ]
                    }
                ]
            }))
            .expect("serialize workflow runtime-check spec"),
        )
        .expect("write workflow runtime-check spec");

        let result =
            runtime_check_run(&run_dir, "verification", None).expect("run workflow runtime-check");
        assert_eq!(result.code, 1);

        let summary: Value = read_json(
            &run_dir
                .join("runtime")
                .join("runtime-check")
                .join("verification")
                .join("summary.json"),
        )
        .expect("read runtime-check summary");
        assert_eq!(summary["status"], "failed");
        assert_eq!(summary["scenarios"][0]["status"], "failed");
        assert_eq!(summary["scenarios"][0]["exit_code"], 7);
        assert!(summary["scenarios"][0]["failure_reason"]
            .as_str()
            .expect("workflow failure reason")
            .contains("expected exit code 0, got 7"));

        let _ = fs::remove_dir_all(run_dir);
        let _ = fs::remove_dir_all(output_root);
        let _ = fs::remove_dir_all(workspace);
        let _ = fs::remove_dir_all(cache_root);
    }

    #[test]
    fn runtime_check_requirement_ignores_generic_fullstack_hints_without_runtime_keywords() {
        let ctx = test_context();
        let workspace = temp_dir("runtime-check-requirement-workspace");
        let output_root = temp_dir("runtime-check-requirement-output");
        let cache_root = temp_dir("runtime-check-requirement-cache");
        let run_dir = create_run(
            &ctx,
            "Проверить cache reuse на verification stage.",
            &workspace,
            &output_root,
            Some("runtime-check-requirement"),
            "compact",
            "ru",
            "research-first",
            "local-first",
            "fetch-if-needed",
            &cache_root.display().to_string(),
            "reuse",
            None,
        )
        .expect("create runtime-check requirement run");

        assert!(!runtime_check_required(&run_dir, "verification")
            .expect("check runtime-check requirement"));

        let _ = fs::remove_dir_all(run_dir);
        let _ = fs::remove_dir_all(output_root);
        let _ = fs::remove_dir_all(workspace);
        let _ = fs::remove_dir_all(cache_root);
    }

    #[test]
    fn runtime_check_requirement_ignores_analysis_only_research_tasks_that_mention_workers() {
        let ctx = test_context();
        let workspace = temp_dir("runtime-check-research-workers-workspace");
        let output_root = temp_dir("runtime-check-research-workers-output");
        let cache_root = temp_dir("runtime-check-research-workers-cache");
        let run_dir = create_run(
            &ctx,
            "Проведи анализ AI coding-агентов в контексте CI/CD на примере Codex. Это исследование, а не реализация классического CI runner/worker или сервиса. Не внедряй сервис и не меняй код.",
            &workspace,
            &output_root,
            Some("runtime-check-research-workers"),
            "compact",
            "ru",
            "research-first",
            "local-first",
            "fetch-if-needed",
            &cache_root.display().to_string(),
            "reuse",
            None,
        )
        .expect("create research runtime-check run");

        let plan = load_plan(&run_dir).expect("load plan");
        assert!(!plan.task_kind.is_empty());
        assert!(!runtime_check_required(&run_dir, "verification")
            .expect("check runtime-check requirement"));

        let _ = fs::remove_dir_all(run_dir);
        let _ = fs::remove_dir_all(output_root);
        let _ = fs::remove_dir_all(workspace);
        let _ = fs::remove_dir_all(cache_root);
    }

    #[test]
    fn runtime_check_requirement_ignores_audit_improve_verification_without_execution_stage() {
        let ctx = test_context();
        let workspace = temp_dir("runtime-check-audit-improve-workspace");
        let output_root = temp_dir("runtime-check-audit-improve-output");
        let cache_root = temp_dir("runtime-check-audit-improve-cache");
        let task = "# Verified Follow-Up Task\n\nUse only the verification-derived context below as the authoritative seed for this rerun.\n\n## Follow-Up Task\n\nПроведи docs-only audit-improve для сервиса `~/sample-service`: обнови narrative artifacts и не переходи к реализации, runtime harness или изменению кода.\n";
        let run_dir = create_run(
            &ctx,
            task,
            &workspace,
            &output_root,
            Some("runtime-check-audit-improve"),
            "compact",
            "ru",
            "research-first",
            "local-first",
            "fetch-if-needed",
            &cache_root.display().to_string(),
            "reuse",
            None,
        )
        .expect("create audit-improve runtime-check run");

        let mut plan = load_plan(&run_dir).expect("load plan");
        assert_eq!(plan.task_kind, "audit-improve");
        plan.pipeline.stages.retain(|stage| stage.id != "execution");
        save_plan(&run_dir, &plan).expect("save plan without execution");
        assert!(!available_stages(&run_dir)
            .expect("available stages")
            .iter()
            .any(|stage| stage == "execution"));
        assert!(!runtime_check_required(&run_dir, "verification")
            .expect("check audit-improve verification runtime requirement"));

        let prompt = compile_prompt(&ctx, &run_dir, "verification").expect("compile verification");
        assert!(!prompt.contains("Runtime harness:"));
        assert!(!prompt.contains("runner_command"));

        let _ = fs::remove_dir_all(run_dir);
        let _ = fs::remove_dir_all(output_root);
        let _ = fs::remove_dir_all(workspace);
        let _ = fs::remove_dir_all(cache_root);
    }

    #[test]
    fn service_check_validation_command_uses_current_agpipe_path() {
        let run_dir = temp_run_dir("runtime-check-command-path");
        let command = super::service_check_validation_command(&run_dir, "verification");
        let current_exe = env::current_exe()
            .expect("current exe")
            .display()
            .to_string();

        assert!(command.contains("internal runtime-check"));
        assert!(command.contains(&current_exe));

        let _ = fs::remove_dir_all(run_dir);
    }

    #[test]
    fn recover_review_artifacts_from_stdout_transcript_materializes_review_files() {
        let run_dir = temp_run_dir("review-stdout-recovery");
        let review_dir = run_dir.join("review");
        fs::create_dir_all(&review_dir).expect("create review dir");
        write_text(&review_dir.join("report.md"), "# Review Report\n\nPending review stage.\n")
            .expect("write placeholder report");
        write_text(
            &review_dir.join("user-summary.md"),
            "# User Summary\n\nPending localized review summary.\n",
        )
        .expect("write placeholder summary");
        write_text(&review_dir.join("scorecard.json"), "{}\n").expect("write placeholder scorecard");
        let stdout_path = run_dir.join("logs").join("review.stdout.log");
        fs::create_dir_all(stdout_path.parent().expect("stdout parent"))
            .expect("create stdout parent");
        write_text(
            &stdout_path,
            "codex\n<<<AGPIPE_REVIEW_REPORT>>>\n# Review Report\n\nRecovered review.\n<<<AGPIPE_REVIEW_SCORECARD_JSON>>>\n{\"winner\":\"solver-b\",\"backup\":\"solver-a\",\"why\":\"best\",\"validation_evidence\":[\"local diff\"],\"critical_gaps\":[\"none\"],\"execution_notes\":[\"keep scope\"]}\n<<<AGPIPE_REVIEW_USER_SUMMARY>>>\n# User Summary\n\nRecovered summary.\n",
        )
        .expect("write stdout transcript");
        let last_message_path = run_dir.join("logs").join("review.last.md");

        let recovered = recover_review_artifacts_from_stdout_transcript(
            &run_dir,
            &stdout_path,
            &last_message_path,
        )
        .expect("recover review artifacts");

        assert!(recovered);
        assert!(read_text(&review_dir.join("report.md"))
            .expect("read report")
            .contains("Recovered review."));
        assert!(read_text(&review_dir.join("user-summary.md"))
            .expect("read summary")
            .contains("Recovered summary."));
        let scorecard = read_json::<Value>(&review_dir.join("scorecard.json"))
            .expect("read scorecard json");
        assert_eq!(scorecard["winner"], "solver-b");
        assert!(last_message_path.exists());

        let _ = fs::remove_dir_all(run_dir);
    }

    #[test]
    fn recover_verification_artifacts_from_stdout_transcript_materializes_verification_files() {
        let run_dir = temp_run_dir("verification-stdout-recovery");
        let verification_dir = run_dir.join("verification");
        fs::create_dir_all(&verification_dir).expect("create verification dir");
        write_text(&verification_dir.join("findings.md"), "# Findings\n\nPending verification stage.\n")
            .expect("write placeholder findings");
        write_text(
            &verification_dir.join("user-summary.md"),
            "# Verification Summary\n\nPending localized verification summary.\n",
        )
        .expect("write placeholder verification summary");
        write_text(
            &verification_dir.join("improvement-request.md"),
            "# Improvement Request\n\nPending verification stage.\n",
        )
        .expect("write placeholder improvement request");
        write_text(
            &verification_dir.join("augmented-task.md"),
            "# Augmented Task\n\nPending verification stage.\n",
        )
        .expect("write placeholder augmented task");
        write_text(&verification_dir.join("goal-status.json"), "{}\n")
            .expect("write placeholder goal status");
        let mut plan = Plan::default();
        plan.goal_gate_enabled = true;
        plan.augmented_follow_up_enabled = true;
        save_plan(&run_dir, &plan).expect("save plan");
        let stdout_path = run_dir.join("logs").join("verification.stdout.log");
        fs::create_dir_all(stdout_path.parent().expect("stdout parent"))
            .expect("create stdout parent");
        write_text(
            &stdout_path,
            "codex\n<<<AGPIPE_VERIFICATION_FINDINGS>>>\n# Findings\n\nRecovered findings.\n<<<AGPIPE_VERIFICATION_GOAL_STATUS_JSON>>>\n{\"goal_complete\":false,\"goal_verdict\":\"partial\",\"rerun_recommended\":true,\"recommended_next_action\":\"rerun\"}\n<<<AGPIPE_VERIFICATION_USER_SUMMARY>>>\n# Verification Summary\n\nRecovered verification summary.\n<<<AGPIPE_VERIFICATION_IMPROVEMENT_REQUEST>>>\n# Improvement Request\n\nRecovered improvement request.\n<<<AGPIPE_VERIFICATION_AUGMENTED_TASK>>>\n# Augmented Task\n\nRecovered augmented task.\n",
        )
        .expect("write verification stdout transcript");
        let last_message_path = run_dir.join("logs").join("verification.last.md");

        let recovered = recover_verification_artifacts_from_stdout_transcript(
            &run_dir,
            &stdout_path,
            &last_message_path,
        )
        .expect("recover verification artifacts");

        assert!(recovered);
        assert!(read_text(&verification_dir.join("findings.md"))
            .expect("read findings")
            .contains("Recovered findings."));
        assert!(read_text(&verification_dir.join("user-summary.md"))
            .expect("read verification summary")
            .contains("Recovered verification summary."));
        assert!(read_text(&verification_dir.join("improvement-request.md"))
            .expect("read improvement request")
            .contains("Recovered improvement request."));
        assert!(read_text(&verification_dir.join("augmented-task.md"))
            .expect("read augmented task")
            .contains("Recovered augmented task."));
        let goal_status = read_json::<Value>(&verification_dir.join("goal-status.json"))
            .expect("read recovered goal status");
        assert_eq!(goal_status["goal_verdict"], "partial");
        assert!(last_message_path.exists());

        let _ = fs::remove_dir_all(run_dir);
    }

    #[test]
    fn recover_verification_artifacts_handles_reordered_bundle_and_repeated_goal_status_tag() {
        let run_dir = temp_run_dir("verification-stdout-recovery-reordered");
        let verification_dir = run_dir.join("verification");
        fs::create_dir_all(&verification_dir).expect("create verification dir");
        write_text(&verification_dir.join("findings.md"), "# Findings\n\nPending verification stage.\n")
            .expect("write placeholder findings");
        write_text(
            &verification_dir.join("user-summary.md"),
            "# Verification Summary\n\nPending localized verification summary.\n",
        )
        .expect("write placeholder verification summary");
        write_text(
            &verification_dir.join("improvement-request.md"),
            "# Improvement Request\n\nPending verification stage.\n",
        )
        .expect("write placeholder improvement request");
        write_text(
            &verification_dir.join("augmented-task.md"),
            "# Augmented Task\n\nPending verification stage.\n",
        )
        .expect("write placeholder augmented task");
        write_text(&verification_dir.join("goal-status.json"), "{}\n")
            .expect("write placeholder goal status");
        let mut plan = Plan::default();
        plan.goal_gate_enabled = true;
        plan.augmented_follow_up_enabled = true;
        save_plan(&run_dir, &plan).expect("save plan");
        let stdout_path = run_dir.join("logs").join("verification.stdout.log");
        fs::create_dir_all(stdout_path.parent().expect("stdout parent"))
            .expect("create stdout parent");
        write_text(
            &stdout_path,
            "codex\n<<<AGPIPE_VERIFICATION_AUGMENTED_TASK>>>\n# Augmented Task\n\nRecovered augmented task.\ntokens used\n75 148\n<<<AGPIPE_VERIFICATION_FINDINGS>>>\n# Findings\n\nRecovered findings.\n<<<AGPIPE_VERIFICATION_FINDINGS>>>\n<<<AGPIPE_VERIFICATION_GOAL_STATUS_JSON>>>\n{\"goal_complete\":false,\"goal_verdict\":\"partial\",\"rerun_recommended\":true,\"recommended_next_action\":\"rerun\"}\n<<<AGPIPE_VERIFICATION_GOAL_STATUS_JSON>>>\n<<<AGPIPE_VERIFICATION_USER_SUMMARY>>>\n# Verification Summary\n\nRecovered verification summary.\n<<<AGPIPE_VERIFICATION_USER_SUMMARY>>>\n<<<AGPIPE_VERIFICATION_IMPROVEMENT_REQUEST>>>\n# Improvement Request\n\nRecovered improvement request.\n<<<AGPIPE_VERIFICATION_IMPROVEMENT_REQUEST>>>\n",
        )
        .expect("write verification stdout transcript");
        let last_message_path = run_dir.join("logs").join("verification.last.md");

        let recovered = recover_verification_artifacts_from_stdout_transcript(
            &run_dir,
            &stdout_path,
            &last_message_path,
        )
        .expect("recover verification artifacts");

        assert!(recovered);
        assert!(read_text(&verification_dir.join("findings.md"))
            .expect("read findings")
            .contains("Recovered findings."));
        assert!(read_text(&verification_dir.join("augmented-task.md"))
            .expect("read augmented task")
            .contains("Recovered augmented task."));
        let goal_status = read_json::<Value>(&verification_dir.join("goal-status.json"))
            .expect("read recovered goal status");
        assert_eq!(goal_status["goal_verdict"], "partial");

        let _ = fs::remove_dir_all(run_dir);
    }

    #[test]
    fn prompted_command_capture_completes_verification_from_last_message_bundle() {
        let root = temp_dir("prompted-command-last-message-verification");
        let run_dir = temp_run_dir("prompted-command-last-message-verification-run");
        let verification_dir = run_dir.join("verification");
        fs::create_dir_all(&verification_dir).expect("create verification dir");
        write_text(
            &verification_dir.join("findings.md"),
            "# Findings\n\nPending verification stage.\n",
        )
        .expect("write placeholder findings");
        write_text(
            &verification_dir.join("user-summary.md"),
            "# Verification Summary\n\nPending localized verification summary.\n",
        )
        .expect("write placeholder verification summary");
        write_text(
            &verification_dir.join("improvement-request.md"),
            "# Improvement Request\n\nPending verification stage.\n",
        )
        .expect("write placeholder improvement request");
        write_text(
            &verification_dir.join("augmented-task.md"),
            "# Augmented Task\n\nPending verification stage.\n",
        )
        .expect("write placeholder augmented task");
        write_text(&verification_dir.join("goal-status.json"), "{}\n")
            .expect("write placeholder goal status");
        let mut plan = Plan::default();
        plan.goal_gate_enabled = true;
        plan.augmented_follow_up_enabled = true;
        save_plan(&run_dir, &plan).expect("save plan");

        let script = root.join("mock-capture-verification-last-message.zsh");
        write_text(
            &script,
            "#!/bin/zsh\nset -euo pipefail\ncat >/dev/null\nsleep 1\ncat > \"$1\" <<'EOF'\n<<<AGPIPE_VERIFICATION_FINDINGS>>>\n# Findings\n\nRecovered from last message.\n<<<AGPIPE_VERIFICATION_GOAL_STATUS_JSON>>>\n{\"goal_complete\":false,\"goal_verdict\":\"partial\",\"rerun_recommended\":true,\"recommended_next_action\":\"rerun\"}\n<<<AGPIPE_VERIFICATION_USER_SUMMARY>>>\n# Verification Summary\n\nRecovered summary from last message.\n<<<AGPIPE_VERIFICATION_IMPROVEMENT_REQUEST>>>\n# Improvement Request\n\nRecovered improvement request.\n<<<AGPIPE_VERIFICATION_AUGMENTED_TASK>>>\n# Augmented Task\n\nRecovered augmented task.\nEOF\nsleep 30\n",
        )
        .expect("write mock verification script");
        let mut permissions = fs::metadata(&script)
            .expect("stat mock verification script")
            .permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&script, permissions).expect("chmod mock verification script");

        let stdout_path = root.join("stdout.log");
        let stderr_path = root.join("stderr.log");
        let last_message_path = run_dir.join("logs").join("verification.last.md");
        fs::create_dir_all(last_message_path.parent().expect("last-message parent"))
            .expect("create last-message parent");

        let started = Instant::now();
        let code = run_prompted_command_capture(
            &[
                script.display().to_string(),
                last_message_path.display().to_string(),
            ],
            "prompt",
            &stdout_path,
            &stderr_path,
            Some(&run_dir),
            Some("verification"),
            Some(&last_message_path),
            true,
        )
        .expect("run prompted command");

        assert_eq!(code, 0);
        assert!(
            started.elapsed() < Duration::from_secs(25),
            "verification capture should complete from last-message recovery before the child sleep finishes"
        );
        assert!(read_text(&verification_dir.join("findings.md"))
            .expect("read findings")
            .contains("Recovered from last message."));
        assert!(read_text(&verification_dir.join("user-summary.md"))
            .expect("read verification summary")
            .contains("Recovered summary from last message."));
        let goal_status = read_json::<Value>(&verification_dir.join("goal-status.json"))
            .expect("read recovered goal status");
        assert_eq!(goal_status["goal_verdict"], "partial");
        assert!(is_stage_complete(&run_dir, "verification").expect("verification should be complete"));

        let _ = fs::remove_dir_all(run_dir);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn doctor_fix_can_resume_research_run_to_verification_without_runtime_gate() {
        let workspace = temp_dir("doctor-fix-research-workspace");
        let output_root = temp_dir("doctor-fix-research-output");
        let cache_root = temp_dir("doctor-fix-research-cache");
        let (ctx, mock_root, _bin, _invocations_path, _tokens_path) =
            mock_codex_context("doctor-fix-research");

        let run_dir = create_run(
            &ctx,
            "Проведи анализ AI coding-агентов в контексте CI/CD на примере Codex. Это research-only задача, а не реализация классического CI runner/worker. Не внедряй сервис и не меняй код.",
            &workspace,
            &output_root,
            Some("doctor-fix-research"),
            "compact",
            "ru",
            "research-first",
            "local-first",
            "fetch-if-needed",
            &cache_root.display().to_string(),
            "reuse",
            None,
        )
        .expect("create doctor-fix research run");
        let plan = load_plan(&run_dir).expect("load plan");
        seed_preverification_outputs(&run_dir, &plan);

        assert!(!plan.task_kind.is_empty());
        assert!(!runtime_check_required(&run_dir, "verification")
            .expect("check verification runtime requirement"));

        let report = doctor_report(&ctx, &run_dir).expect("doctor report");
        assert!(report
            .fix_actions
            .contains(&"start verification".to_string()));

        let result = execute_doctor_fix(&ctx, &run_dir).expect("doctor fix");
        assert_eq!(result.code, 0);
        assert!(result.stdout.contains("Doctor auto-fix plan:"));

        let goal_status: Value =
            read_json(&run_dir.join("verification").join("goal-status.json")).expect("goal status");
        assert_eq!(goal_status["goal_complete"], true);

        let _ = fs::remove_dir_all(run_dir);
        let _ = fs::remove_dir_all(output_root);
        let _ = fs::remove_dir_all(workspace);
        let _ = fs::remove_dir_all(cache_root);
        let _ = fs::remove_dir_all(mock_root);
    }

    #[test]
    fn completed_doctor_fix_does_not_count_as_incomplete_verification_attempt() {
        let ctx = test_context();
        let workspace = temp_dir("doctor-fix-complete-workspace");
        let output_root = temp_dir("doctor-fix-complete-output");
        let run_dir = create_run(
            &ctx,
            "Review the repository without changing code.",
            &workspace,
            &output_root,
            Some("doctor-fix-complete"),
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
        seed_preverification_outputs(&run_dir, &plan);

        runtime::start_job(
            &run_dir,
            "doctor fix",
            Some("verification"),
            "doctor --fix",
            std::process::id() as i32,
            std::process::id() as i32,
        )
        .expect("start doctor-fix job");
        runtime::finish_job(&run_dir, "completed", Some(0), None).expect("finish doctor-fix job");

        let doctor = doctor_report(&ctx, &run_dir).expect("doctor report");
        assert_eq!(doctor.health, "healthy");
        assert_eq!(doctor.next, "verification");
        assert!(doctor.last_attempt.is_none());

        let status = status_report(&ctx, &run_dir).expect("status report");
        assert_eq!(status.next, "solver-a");
        assert!(status.last_attempt.is_none());

        let _ = fs::remove_dir_all(run_dir);
        let _ = fs::remove_dir_all(output_root);
        let _ = fs::remove_dir_all(workspace);
    }

    #[test]
    fn verification_stage_stops_before_backend_when_runtime_check_fails() {
        let workspace = temp_dir("runtime-check-verification-gate-workspace");
        let output_root = temp_dir("runtime-check-verification-gate-output");
        let cache_root = temp_dir("runtime-check-verification-gate-cache");
        let (ctx, mock_root, _bin, invocations_path, _tokens_path) =
            mock_codex_context("verification-runtime-gate");

        let run_dir = create_run(
            &ctx,
            "Проверить verification gate без лишнего backend вызова.",
            &workspace,
            &output_root,
            Some("verification-runtime-gate"),
            "compact",
            "ru",
            "research-first",
            "local-first",
            "fetch-if-needed",
            &cache_root.display().to_string(),
            "reuse",
            None,
        )
        .expect("create verification gate run");
        let plan = load_plan(&run_dir).expect("load verification gate plan");
        seed_preverification_outputs(&run_dir, &plan);
        write_text(
            &workspace.join(".agpipe").join("runtime-check.json"),
            &serde_json::to_string_pretty(&json!({
                "version": 1,
                "mode": "workflow",
                "workdir": ".",
                "scenarios": [
                    {
                        "id": "failing-preflight",
                        "kind": "command",
                        "command": "echo runtime-check-broken >&2; exit 7",
                        "expect_exit_code": 0
                    }
                ]
            }))
            .expect("serialize failing verification spec"),
        )
        .expect("write failing verification spec");
        write_text(
            &run_dir
                .join("runtime")
                .join("runtime-check")
                .join("execution")
                .join("summary.json"),
            &serde_json::to_string_pretty(&json!({
                "status": "passed"
            }))
            .expect("serialize passing execution summary"),
        )
        .expect("write passing execution runtime-check summary");

        let result = run_stage_capture(&ctx, &run_dir, "start", &["verification"])
            .expect("run verification with failing runtime-check");
        assert_eq!(result.code, 1);
        assert!(result
            .stdout
            .contains("Blocked verification because runtime-check did not pass."));
        assert_eq!(line_count(&invocations_path), 0);

        let summary: Value = read_json(
            &run_dir
                .join("runtime")
                .join("runtime-check")
                .join("verification")
                .join("summary.json"),
        )
        .expect("read verification runtime-check summary");
        assert_eq!(summary["status"], "failed");

        let _ = fs::remove_dir_all(run_dir);
        let _ = fs::remove_dir_all(output_root);
        let _ = fs::remove_dir_all(workspace);
        let _ = fs::remove_dir_all(cache_root);
        let _ = fs::remove_dir_all(mock_root);
    }

    #[test]
    fn execution_stage_requires_runtime_check_for_runtime_facing_tasks() {
        let workspace = temp_dir("runtime-check-required-execution-workspace");
        let output_root = temp_dir("runtime-check-required-execution-output");
        let cache_root = temp_dir("runtime-check-required-execution-cache");
        let (ctx, mock_root, _bin, invocations_path, _tokens_path) =
            mock_codex_context("execution-runtime-required");

        let run_dir = create_run(
            &ctx,
            "Поднять HTTP сервис и прогнать его через runtime gate.",
            &workspace,
            &output_root,
            Some("execution-runtime-required"),
            "compact",
            "ru",
            "research-first",
            "local-first",
            "fetch-if-needed",
            &cache_root.display().to_string(),
            "reuse",
            None,
        )
        .expect("create execution runtime-required run");
        let plan = load_plan(&run_dir).expect("load execution runtime-required plan");
        seed_preexecution_outputs(&run_dir, &plan);

        let result =
            run_stage_capture(&ctx, &run_dir, "start", &["execution"]).expect("run execution");
        assert_eq!(result.code, 1);
        assert_eq!(line_count(&invocations_path), 1);
        assert!(run_dir.join("execution").join("report.md").exists());
        assert!(
            !is_stage_complete(&run_dir, "execution").expect("execution should stay pending"),
            "runtime-facing execution must not complete without a passing runtime-check summary"
        );

        let _ = fs::remove_dir_all(run_dir);
        let _ = fs::remove_dir_all(output_root);
        let _ = fs::remove_dir_all(workspace);
        let _ = fs::remove_dir_all(cache_root);
        let _ = fs::remove_dir_all(mock_root);
    }

    #[test]
    fn execution_stage_is_not_complete_when_runtime_check_is_missing() {
        let ctx = test_context();
        let workspace = temp_dir("runtime-check-complete-gate-workspace");
        let output_root = temp_dir("runtime-check-complete-gate-output");
        let cache_root = temp_dir("runtime-check-complete-gate-cache");
        let run_dir = create_run(
            &ctx,
            "Поднять HTTP сервис и подготовить runtime verification.",
            &workspace,
            &output_root,
            Some("runtime-check-complete-gate"),
            "compact",
            "ru",
            "research-first",
            "local-first",
            "fetch-if-needed",
            &cache_root.display().to_string(),
            "reuse",
            None,
        )
        .expect("create execution runtime-gated run");
        let plan = load_plan(&run_dir).expect("load plan");
        seed_preexecution_outputs(&run_dir, &plan);
        write_text(
            &run_dir.join("execution").join("report.md"),
            "# Execution Report\n\nSeeded execution artifact without runtime check.\n",
        )
        .expect("seed execution report");

        assert!(runtime_check_required(&run_dir, "execution").expect("runtime-check required"));
        assert!(!is_stage_complete(&run_dir, "execution").expect("execution should stay pending"));
        assert_eq!(
            next_stage_for_run(&run_dir).expect("next stage"),
            Some("execution".to_string())
        );

        let _ = fs::remove_dir_all(run_dir);
        let _ = fs::remove_dir_all(output_root);
        let _ = fs::remove_dir_all(workspace);
        let _ = fs::remove_dir_all(cache_root);
    }

    #[test]
    fn resume_failure_reports_execution_when_runtime_gate_blocks_before_verification() {
        let ctx = test_context();
        let workspace = temp_dir("resume-runtime-gate-workspace");
        let output_root = temp_dir("resume-runtime-gate-output");
        let cache_root = temp_dir("resume-runtime-gate-cache");
        let run_dir = create_run(
            &ctx,
            "Поднять HTTP сервис и прогнать его через runtime gate.",
            &workspace,
            &output_root,
            Some("resume-runtime-gate"),
            "compact",
            "ru",
            "research-first",
            "local-first",
            "fetch-if-needed",
            &cache_root.display().to_string(),
            "reuse",
            None,
        )
        .expect("create resume runtime-gated run");
        let plan = load_plan(&run_dir).expect("load plan");
        seed_preexecution_outputs(&run_dir, &plan);
        write_text(
            &run_dir.join("execution").join("report.md"),
            "# Execution Report\n\nExecution wrote an artifact but runtime check did not pass.\n",
        )
        .expect("seed execution report");

        runtime::start_job(
            &run_dir,
            "resume",
            Some("verification"),
            "resume until verification",
            std::process::id() as i32,
            std::process::id() as i32,
        )
        .expect("start resume job");
        runtime::finish_job(&run_dir, "failed", Some(1), None).expect("finish failed resume job");

        let doctor = doctor_report(&ctx, &run_dir).expect("doctor report");
        let attempt = doctor.last_attempt.expect("last attempt");
        assert_eq!(attempt.stage, "execution");
        assert_eq!(doctor.next, "execution");
        assert_eq!(doctor.safe_next_action, "start execution");

        let _ = fs::remove_dir_all(run_dir);
        let _ = fs::remove_dir_all(output_root);
        let _ = fs::remove_dir_all(workspace);
        let _ = fs::remove_dir_all(cache_root);
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
    fn contextual_log_excerpt_uses_stderr_when_stdout_is_empty() {
        let run_dir = temp_run_dir("pending-stage-stderr-log");
        fs::write(run_dir.join("logs").join("intake.stdout.log"), "").expect("write empty stdout");
        fs::write(
            run_dir.join("logs").join("intake.stderr.log"),
            "progress 1\nprogress 2\n",
        )
        .expect("write stderr log");

        let (title, lines) = contextual_log_excerpt(&run_dir, None, Some("intake"), 12);

        assert_eq!(title, "intake.stderr.log");
        assert_eq!(lines, vec!["progress 1", "progress 2"]);

        let _ = fs::remove_dir_all(run_dir);
    }

    #[test]
    fn codex_capture_moves_successful_progress_out_of_stderr_log() {
        let root = temp_dir("codex-log-normalization");
        let script = root.join("mock-codex-progress.zsh");
        write_text(
            &script,
            "#!/bin/zsh\nset -euo pipefail\ncat >/dev/null\nprint 'stdout line'\nprint 'progress line' >&2\n",
        )
        .expect("write mock progress script");
        let mut permissions = fs::metadata(&script)
            .expect("stat mock progress script")
            .permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&script, permissions).expect("chmod mock progress script");
        let stdout_path = root.join("stdout.log");
        let stderr_path = root.join("stderr.log");

        let code = run_prompted_command_capture(
            &[script.display().to_string()],
            "prompt",
            &stdout_path,
            &stderr_path,
            None,
            None,
            None,
            true,
        )
        .expect("run prompted command");

        assert_eq!(code, 0);
        let stdout = read_text(&stdout_path).expect("read stdout log");
        let stderr = read_text(&stderr_path).expect("read stderr log");
        assert!(stdout.contains("stdout line"));
        assert!(stdout.contains("progress line"));
        assert!(stderr.trim().is_empty());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn prompted_command_capture_returns_130_when_interrupt_requested() {
        let root = temp_dir("prompted-command-interrupt");
        let run_dir = temp_run_dir("prompted-command-interrupt-run");
        let script = root.join("mock-capture-sleep.zsh");
        write_text(
            &script,
            "#!/bin/zsh\nset -euo pipefail\ncat >/dev/null\nsleep 30\n",
        )
        .expect("write mock sleep script");
        let mut permissions = fs::metadata(&script)
            .expect("stat mock sleep script")
            .permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&script, permissions).expect("chmod mock sleep script");
        let stdout_path = root.join("stdout.log");
        let stderr_path = root.join("stderr.log");
        let run_dir_for_thread = run_dir.clone();
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(200));
            crate::runtime::request_interrupt(&run_dir_for_thread).expect("request interrupt");
        });

        let code = run_prompted_command_capture(
            &[script.display().to_string()],
            "prompt",
            &stdout_path,
            &stderr_path,
            Some(&run_dir),
            None,
            None,
            true,
        )
        .expect("run prompted command");

        assert_eq!(code, 130);

        let _ = crate::runtime::clear_interrupt_request(&run_dir);
        let _ = fs::remove_dir_all(run_dir);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn stage0_codex_capture_honors_interrupt_requests_from_observer() {
        let root = temp_dir("stage0-codex-interrupt");
        let run_dir = temp_run_dir("stage0-codex-interrupt-run");
        let script = root.join("mock-codex-sleep.zsh");
        write_text(
            &script,
            "#!/bin/zsh\nset -euo pipefail\ncat >/dev/null\nsleep 30\n",
        )
        .expect("write mock codex sleep script");
        let mut permissions = fs::metadata(&script)
            .expect("stat mock codex sleep script")
            .permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&script, permissions).expect("chmod mock codex sleep script");

        let ctx = Context {
            repo_root: root.clone(),
            codex_bin: script.display().to_string(),
            stage0_backend: "codex".to_string(),
            stage_backend: "codex".to_string(),
            openai_api_base: "https://api.openai.com/v1".to_string(),
            openai_api_key: None,
            openai_model: "gpt-5".to_string(),
            openai_prompt_cache_key_prefix: "agpipe-stage0-v1".to_string(),
            openai_prompt_cache_retention: None,
            openai_store: false,
            openai_background: false,
        };
        let observer = Arc::new(InterruptObserver {
            run_dir: run_dir.clone(),
        });
        let run_dir_for_thread = run_dir.clone();
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(200));
            crate::runtime::request_interrupt(&run_dir_for_thread).expect("request interrupt");
        });

        let err = with_engine_observer(observer, || {
            run_stage0_last_message(
                &ctx,
                "prompt",
                &root,
                &root.join("artifacts"),
                "stage0-test",
            )
        })
        .expect_err("stage0 capture should be interrupted");

        assert!(
            err.contains("exit code 130"),
            "unexpected interrupt error: {err}"
        );

        let _ = crate::runtime::clear_interrupt_request(&run_dir);
        let _ = fs::remove_dir_all(run_dir);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn sync_run_artifacts_honors_interrupt_request() {
        let ctx = test_context();
        let workspace = temp_dir("sync-interrupt-workspace");
        let output_root = temp_dir("sync-interrupt-output");
        let run_dir = create_run(
            &ctx,
            "Build a backend service.",
            &workspace,
            &output_root,
            Some("sync-interrupt"),
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

        crate::runtime::request_interrupt(&run_dir).expect("request interrupt");
        let err = sync_run_artifacts(&ctx, &run_dir).expect_err("sync should be interrupted");
        assert!(err.contains("Interrupted from agpipe."));

        let _ = crate::runtime::clear_interrupt_request(&run_dir);
        let _ = fs::remove_dir_all(output_root);
        let _ = fs::remove_dir_all(workspace);
    }

    #[test]
    fn amend_run_honors_interrupt_request() {
        let ctx = test_context();
        let workspace = temp_dir("amend-interrupt-workspace");
        let output_root = temp_dir("amend-interrupt-output");
        let run_dir = create_run(
            &ctx,
            "Build a backend service.",
            &workspace,
            &output_root,
            Some("amend-interrupt"),
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

        crate::runtime::request_interrupt(&run_dir).expect("request interrupt");
        let err = amend_run(&ctx, &run_dir, "new note", "intake", true)
            .expect_err("amend should be interrupted");
        assert!(err.contains("Interrupted from agpipe."));

        let _ = crate::runtime::clear_interrupt_request(&run_dir);
        let _ = fs::remove_dir_all(output_root);
        let _ = fs::remove_dir_all(workspace);
    }

    #[test]
    fn provisional_token_usage_is_replaced_by_final_stage_usage() {
        let run_dir = temp_run_dir("provisional-token-ledger");
        let plan = Plan {
            token_budget: TokenBudget {
                total_tokens: 1_000,
                warning_threshold_tokens: 100,
                source: "test".to_string(),
            },
            ..Plan::default()
        };
        let prompt_hashes = PromptCacheHashes {
            combined: "combined".to_string(),
            stable_prefix: "stable".to_string(),
            dynamic_suffix: "dynamic".to_string(),
        };
        let provisional = TokenUsage {
            source: "estimated-prompt-start".to_string(),
            prompt_tokens: 100,
            cached_prompt_tokens: 0,
            completion_tokens: 0,
            total_tokens: 100,
            estimated_saved_tokens: 0,
        };
        let final_usage = TokenUsage {
            source: "estimated-local".to_string(),
            prompt_tokens: 100,
            cached_prompt_tokens: 0,
            completion_tokens: 50,
            total_tokens: 150,
            estimated_saved_tokens: 0,
        };

        record_token_usage_with_replacement(
            &run_dir,
            &plan,
            "intake",
            "provisional",
            "cache-key",
            &prompt_hashes,
            "workspace-hash",
            &provisional,
            &["provisional"],
        )
        .expect("record provisional usage");
        let provisional_summary =
            summarize_token_ledger(&run_dir, &plan).expect("summarize provisional");
        assert_eq!(provisional_summary.used_total_tokens, 100);
        assert_eq!(provisional_summary.remaining_tokens, Some(900));

        record_token_usage_with_replacement(
            &run_dir,
            &plan,
            "intake",
            "executed",
            "cache-key",
            &prompt_hashes,
            "workspace-hash",
            &final_usage,
            &["provisional"],
        )
        .expect("record final usage");
        let final_summary = summarize_token_ledger(&run_dir, &plan).expect("summarize final");
        assert_eq!(final_summary.used_total_tokens, 150);
        assert_eq!(final_summary.remaining_tokens, Some(850));

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
    fn contextual_log_excerpt_prefers_latest_visible_log_over_empty_newer_log() {
        let run_dir = temp_run_dir("missing-stage-visible-latest");
        fs::write(
            run_dir.join("logs").join("execution.stdout.log"),
            "useful execution output\n",
        )
        .expect("write stdout log");
        fs::write(run_dir.join("logs").join("execution.stderr.log"), "")
            .expect("write empty stderr log");

        let (title, lines) = contextual_log_excerpt(&run_dir, None, Some("verification"), 12);
        let joined = lines.join("\n");

        assert_eq!(title, "Pending stage: verification");
        assert!(joined.contains("Latest available log: execution.stdout.log"));
        assert!(joined.contains("useful execution output"));

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
    fn recover_solver_artifact_from_stdout_transcript_materializes_result_and_last_message() {
        let run_dir = temp_run_dir("solver-stdout-recovery");
        let stage = "solver-a";
        let result_path = run_dir.join("solutions").join(stage).join("RESULT.md");
        fs::create_dir_all(result_path.parent().expect("result parent"))
            .expect("create result parent");
        write_text(&result_path, "# Result\n\nFill this file with the solver output.\n")
            .expect("write placeholder result");
        let stdout_path = run_dir.join("logs").join("solver-a.stdout.log");
        fs::create_dir_all(stdout_path.parent().expect("stdout parent"))
            .expect("create stdout parent");
        write_text(
            &stdout_path,
            "user\nprompt\ncodex\n# Result\n\nRecovered solver artifact.\n\n## MCP Usage\n- None.\n",
        )
        .expect("write stdout transcript");
        let last_message_path = run_dir.join("logs").join("solver-a.last.md");

        let recovered = recover_solver_artifact_from_stdout_transcript(
            &run_dir,
            stage,
            &stdout_path,
            &last_message_path,
        )
        .expect("recover solver artifact");

        assert!(recovered);
        let result = read_text(&result_path).expect("read recovered result");
        let last_message = read_text(&last_message_path).expect("read recovered last message");
        assert!(result.contains("Recovered solver artifact."));
        assert_eq!(result, last_message);

        let _ = fs::remove_dir_all(run_dir);
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
    fn generate_interview_questions_short_circuits_execution_ready_tasks() {
        let output_root = temp_dir("interview-questions-fast-path-output");
        let workspace = temp_dir("interview-questions-fast-path-workspace");
        let (ctx, mock_root, _bin, invocations_path, _tokens_path) =
            mock_codex_context("interview-fast-path");

        let (session_dir, payload) = generate_interview_questions(
            &ctx,
            "Create a minimal Python CLI in main.py that prints exactly agpipe_live_full_tui_ok and add a README with the run command.",
            &workspace,
            &output_root,
            Some("fast-path"),
            "ru",
            6,
        )
        .expect("execution-ready stage0 fast path");

        assert_eq!(line_count(&invocations_path), 0);
        assert_eq!(
            payload
                .get("questions")
                .and_then(|value| value.as_array())
                .map(Vec::len),
            Some(0)
        );
        assert!(session_dir.join("questions.json").exists());
        assert!(session_dir
            .join("logs")
            .join("interview-questions.fallback.json")
            .exists());

        let _ = fs::remove_dir_all(output_root);
        let _ = fs::remove_dir_all(workspace);
        let _ = fs::remove_dir_all(mock_root);
    }

    #[test]
    fn generate_interview_questions_short_circuits_code_review_requests() {
        let output_root = temp_dir("interview-questions-review-fast-path-output");
        let workspace = temp_dir("interview-questions-review-fast-path-workspace");
        let (ctx, mock_root, _bin, invocations_path, _tokens_path) =
            mock_codex_context("interview-review-fast-path");

        let (session_dir, payload) = generate_interview_questions(
            &ctx,
            "Проведи ревью кода ~/repo-under-review.",
            &workspace,
            &output_root,
            Some("review-fast-path"),
            "ru",
            6,
        )
        .expect("code-review stage0 fast path");

        assert_eq!(line_count(&invocations_path), 0);
        assert_eq!(
            payload
                .get("questions")
                .and_then(|value| value.as_array())
                .map(Vec::len),
            Some(0)
        );
        assert!(session_dir.join("questions.json").exists());
        assert!(session_dir
            .join("logs")
            .join("interview-questions.fallback.json")
            .exists());

        let _ = fs::remove_dir_all(output_root);
        let _ = fs::remove_dir_all(workspace);
        let _ = fs::remove_dir_all(mock_root);
    }

    #[test]
    fn generate_interview_questions_does_not_fast_path_when_task_explicitly_requests_clarification()
    {
        let output_root = temp_dir("interview-questions-clarify-output");
        let workspace = temp_dir("interview-questions-clarify-workspace");
        let (ctx, mock_root, _bin, invocations_path, _tokens_path) =
            mock_codex_context("interview-clarify");

        let (session_dir, payload) = generate_interview_questions(
            &ctx,
            "Сделай CLI на Python для обработки текстовых файлов, но сначала уточни у меня сценарий использования, аргументы и ожидаемый вывод.",
            &workspace,
            &output_root,
            Some("clarify-first"),
            "ru",
            6,
        )
        .expect("clarification request should not skip interview");

        assert_eq!(line_count(&invocations_path), 1);
        assert_eq!(
            payload
                .get("questions")
                .and_then(|value| value.as_array())
                .map(Vec::len),
            Some(1)
        );
        assert!(session_dir.join("questions.json").exists());
        assert!(!session_dir
            .join("logs")
            .join("interview-questions.fallback.json")
            .exists());

        let _ = fs::remove_dir_all(output_root);
        let _ = fs::remove_dir_all(workspace);
        let _ = fs::remove_dir_all(mock_root);
    }

    #[test]
    fn generate_interview_questions_guards_empty_backend_output_for_uncertain_tasks() {
        let output_root = temp_dir("interview-questions-guardrail-output");
        let workspace = temp_dir("interview-questions-guardrail-workspace");
        let (ctx, root) = mock_script_context(
            "stage0-empty-questions",
            r##"#!/bin/zsh
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

cat > "$last_message" <<'JSON'
{
  "goal_summary": "Подобрать полезный локальный CLI на Python",
  "questions": []
}
JSON
print "mock empty interview questions"
"##,
        );

        let (session_dir, payload) = generate_interview_questions(
            &ctx,
            "Нужен какой-нибудь полезный локальный CLI на Python для моей работы. Я пока не решил, что он должен делать.",
            &workspace,
            &output_root,
            Some("guardrail"),
            "ru",
            6,
        )
        .expect("empty backend response should trigger local guardrail");

        assert_eq!(
            payload
                .get("questions")
                .and_then(|value| value.as_array())
                .map(Vec::len),
            Some(1)
        );
        let fallback: Value = read_json(
            &session_dir
                .join("logs")
                .join("interview-questions.fallback.json"),
        )
        .expect("read guardrail metadata");
        assert_eq!(
            fallback
                .get("reason_kind")
                .and_then(|value| value.as_str())
                .unwrap_or_default(),
            "empty-questions-guardrail"
        );
        assert!(
            read_text(&session_dir.join("logs").join("interview-questions.last.md"))
                .expect("read guarded last message")
                .contains("\"scope_constraints\"")
        );

        let _ = fs::remove_dir_all(output_root);
        let _ = fs::remove_dir_all(workspace);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn generate_interview_questions_falls_back_locally_when_backend_is_unavailable() {
        let output_root = temp_dir("interview-questions-fallback-output");
        let workspace = temp_dir("interview-questions-fallback-workspace");
        let (ctx, root) = mock_script_context(
            "stage0-network-fallback",
            r##"#!/bin/zsh
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

: > "$last_message"
echo "dns lookup failure while contacting codex websocket backend" >&2
exit 1
"##,
        );

        let (session_dir, payload) = generate_interview_questions(
            &ctx,
            "Полностью проверить pipeline end-to-end, сохранить реальные артефакты stage0 и не регресснуть validation path.",
            &workspace,
            &output_root,
            Some("fallback"),
            "ru",
            1,
        )
        .expect("expected local fallback");

        assert!(session_dir.join("questions.json").exists());
        assert!(session_dir
            .join("logs")
            .join("interview-questions.fallback.json")
            .exists());
        assert!(!payload
            .get("goal_summary")
            .and_then(|value| value.as_str())
            .unwrap_or_default()
            .is_empty());
        assert!(payload
            .get("questions")
            .and_then(|value| value.as_array())
            .map(|items| items.len() <= 1)
            .unwrap_or(false));
        let fallback: Value = read_json(
            &session_dir
                .join("logs")
                .join("interview-questions.fallback.json"),
        )
        .expect("read fallback metadata");
        assert_eq!(
            fallback
                .get("requested_backend")
                .and_then(|value| value.as_str())
                .unwrap_or_default(),
            "codex"
        );
        assert_eq!(
            fallback
                .get("effective_backend")
                .and_then(|value| value.as_str())
                .unwrap_or_default(),
            "local"
        );
        assert_eq!(
            fallback
                .get("reason_kind")
                .and_then(|value| value.as_str())
                .unwrap_or_default(),
            "backend-unavailable"
        );
        assert!(
            read_text(&session_dir.join("logs").join("interview-questions.last.md"))
                .expect("read fallback last message")
                .contains("\"goal_summary\"")
        );
        assert!(read_text(
            &session_dir
                .join("logs")
                .join("interview-questions.stderr.log")
        )
        .expect("read fallback stderr")
        .contains("dns lookup failure"));

        let _ = fs::remove_dir_all(output_root);
        let _ = fs::remove_dir_all(workspace);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn generate_interview_questions_does_not_hide_invalid_json_with_local_fallback() {
        let output_root = temp_dir("interview-questions-invalid-json-output");
        let workspace = temp_dir("interview-questions-invalid-json-workspace");
        let (ctx, root) = mock_script_context(
            "stage0-invalid-json",
            r##"#!/bin/zsh
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

cat > "$last_message" <<'EOF'
not-json
EOF
print "mock invalid json"
"##,
        );

        let err = generate_interview_questions(
            &ctx,
            "Need a narrow validation probe.",
            &workspace,
            &output_root,
            Some("invalid-json"),
            "en",
            2,
        )
        .expect_err("invalid JSON should fail closed");

        assert!(err.contains("Interview agent did not return a JSON object."));
        let session_dir = only_child_dir(&output_root.join("_interviews"));
        assert!(!session_dir
            .join("logs")
            .join("interview-questions.fallback.json")
            .exists());
        assert!(!session_dir.join("questions.json").exists());

        let _ = fs::remove_dir_all(output_root);
        let _ = fs::remove_dir_all(workspace);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn run_stage0_last_message_reports_empty_last_message_on_codex_failure() {
        let workspace = temp_dir("stage0-empty-last-message-workspace");
        let artifacts = temp_dir("stage0-empty-last-message-artifacts");
        let (ctx, root) = mock_script_context(
            "empty-last-message",
            r##"#!/bin/zsh
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

: > "$last_message"
echo "mock transport failure" >&2
exit 1
"##,
        );

        let err = run_stage0_last_message(
            &ctx,
            "stage0 failure probe",
            &workspace,
            &artifacts,
            "interview-questions",
        )
        .expect_err("expected stage0 failure");

        assert!(err.contains("last-message artifact exists but is empty"));
        assert!(err.contains("mock transport failure"));

        let _ = fs::remove_dir_all(workspace);
        let _ = fs::remove_dir_all(artifacts);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn run_stage0_last_message_reports_missing_last_message_on_success_exit() {
        let workspace = temp_dir("stage0-missing-last-message-workspace");
        let artifacts = temp_dir("stage0-missing-last-message-artifacts");
        let (ctx, root) = mock_script_context(
            "missing-last-message",
            r##"#!/bin/zsh
set -euo pipefail
echo "mock success without artifact"
exit 0
"##,
        );

        let err = run_stage0_last_message(
            &ctx,
            "stage0 missing artifact probe",
            &workspace,
            &artifacts,
            "interview-finalize",
        )
        .expect_err("expected missing last-message failure");

        assert!(err.contains("without writing the expected last-message artifact"));
        assert!(err.contains("interview-finalize.last.md"));

        let _ = fs::remove_dir_all(workspace);
        let _ = fs::remove_dir_all(artifacts);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn finalize_interview_prompt_falls_back_locally_when_backend_is_unavailable() {
        let session_dir = temp_dir("interview-finalize-fallback-session");
        let workspace = temp_dir("interview-finalize-fallback-workspace");
        let (ctx, root) = mock_script_context(
            "finalize-network-fallback",
            r##"#!/bin/zsh
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

: > "$last_message"
echo "websocket transport timeout while contacting codex" >&2
exit 1
"##,
        );
        let raw_task =
            "# Augmented Task\n\nСохранить исходную цель и собрать доказательство до execution.";
        write_text(&session_dir.join("raw-task.md"), raw_task).expect("seed raw task");
        let answers = vec![json!({
            "id": "proof_mode",
            "question": "Какой proof path обязателен?",
            "answer": "Нужен живой target-workspace path до execution."
        })];

        let final_task_path =
            finalize_interview_prompt(&ctx, raw_task, &workspace, &session_dir, &answers, "ru")
                .expect("expected finalize fallback");

        let final_task = read_text(&final_task_path).expect("read final task");
        assert!(final_task.contains("Сохранить исходную цель"));
        assert!(final_task.contains("живой target-workspace path до execution"));
        assert!(session_dir
            .join("logs")
            .join("interview-finalize.fallback.json")
            .exists());
        let fallback: Value = read_json(
            &session_dir
                .join("logs")
                .join("interview-finalize.fallback.json"),
        )
        .expect("read finalize fallback metadata");
        assert_eq!(
            fallback
                .get("reason_kind")
                .and_then(|value| value.as_str())
                .unwrap_or_default(),
            "backend-unavailable"
        );

        let _ = fs::remove_dir_all(session_dir);
        let _ = fs::remove_dir_all(workspace);
        let _ = fs::remove_dir_all(root);
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
    fn preview_text_prefers_verification_summary_when_available() {
        let run_dir = temp_run_dir("preview-verification-summary");
        fs::create_dir_all(run_dir.join("review")).expect("create review dir");
        fs::create_dir_all(run_dir.join("verification")).expect("create verification dir");
        write_text(
            &run_dir.join("review").join("user-summary.md"),
            "# User Summary\n\nReview summary.\n",
        )
        .expect("write review summary");
        write_text(
            &run_dir.join("verification").join("user-summary.md"),
            "# Verification Summary\n\nVerification summary.\n",
        )
        .expect("write verification summary");

        let (label, preview) = preview_text(&run_dir, 2400);

        assert_eq!(label, "Verification Summary");
        assert!(preview.contains("Verification summary."));

        let _ = fs::remove_dir_all(run_dir);
    }

    #[test]
    fn preview_text_falls_back_to_request_before_intake() {
        let run_dir = temp_run_dir("preview-request-fallback");
        write_text(
            &run_dir.join("request.md"),
            "# Follow-up Task\n\nRepair the preserved-path smoke blocker.\n",
        )
        .expect("write request");
        write_text(
            &run_dir.join("brief.md"),
            "# Brief\n\nPending intake stage.\n",
        )
        .expect("write placeholder brief");

        let (label, preview) = preview_text(&run_dir, 2400);

        assert_eq!(label, "Request");
        assert!(preview.contains("Repair the preserved-path smoke blocker."));

        let _ = fs::remove_dir_all(run_dir);
    }

    #[test]
    fn latest_log_file_prefers_visible_content_over_newer_empty_log() {
        let logs_dir = temp_dir("latest-log-file");
        let visible = logs_dir.join("solver.stdout.log");
        let empty = logs_dir.join("solver.last.md");
        write_text(&visible, "visible content").expect("write visible log");
        thread::sleep(Duration::from_millis(20));
        write_text(&empty, "").expect("write empty newer log");

        let latest = latest_log_file(&logs_dir).expect("latest log");
        assert_eq!(latest, visible);

        let _ = fs::remove_dir_all(logs_dir);
    }

    #[test]
    fn verification_workspace_documents_order_recent_files_stably() {
        let workspace = temp_dir("verification-workspace-order");
        let older = workspace.join("older.txt");
        let newer = workspace.join("newer.txt");
        write_text(&older, "older").expect("write older");
        thread::sleep(Duration::from_millis(20));
        write_text(&newer, "newer").expect("write newer");

        let docs = verification_workspace_documents(&workspace).expect("workspace docs");
        let summary = docs
            .iter()
            .find(|(name, _)| name == "workspace/RECENT_FILES.md")
            .map(|(_, content)| content)
            .expect("summary doc");
        let older_pos = summary.find("older.txt").expect("older in summary");
        let newer_pos = summary.find("newer.txt").expect("newer in summary");
        assert!(
            newer_pos < older_pos,
            "summary order was not recent-first:\n{summary}"
        );

        let _ = fs::remove_dir_all(workspace);
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
    fn solver_prompt_lists_resolved_agency_role_docs() {
        let (ctx, root, agency_root, workspace) = test_context_with_sibling_agency("solver-prompt");
        let output_root = temp_dir("solver-prompt-output");
        let cache_root = temp_dir("solver-prompt-cache");
        let role_path = write_agency_role(
            &agency_root,
            "engineering/engineering-frontend-developer.md",
            "Frontend Developer",
            "Frontend execution specialist",
            "# Frontend Developer Agent\n\nUse the agency role guidance for pixel-perfect UI work.",
        );
        let run_dir = create_run(
            &ctx,
            "Build a React UI entrypoint with responsive layout.",
            &workspace,
            &output_root,
            Some("solver-role-docs"),
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
            "# Brief\n\nImplement the assigned frontend slice.\n",
        )
        .expect("brief");
        let mut plan = load_plan(&run_dir).expect("load plan");
        plan.solver_roles = vec![SolverRole {
            solver_id: "solver-a".to_string(),
            role: "engineering/engineering-frontend-developer.md".to_string(),
            angle: "implementation-first".to_string(),
            mcp_servers: Vec::new(),
        }];
        save_plan(&run_dir, &plan).expect("save plan");
        sync_run_artifacts(&ctx, &run_dir).expect("sync prompts");

        let prompt = render_stage_prompt(&ctx, &run_dir, "solver-a").expect("solver prompt");
        assert!(prompt.contains(&role_path.display().to_string()));
        assert!(prompt.contains("resolved_role_docs"));

        let _ = fs::remove_dir_all(run_dir);
        let _ = fs::remove_dir_all(output_root);
        let _ = fs::remove_dir_all(cache_root);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn intake_prompt_uses_absolute_reference_paths() {
        let ctx = test_context();
        let workspace = temp_dir("intake-absolute-refs-workspace");
        let output_root = temp_dir("intake-absolute-refs-output");
        let run_dir = create_run(
            &ctx,
            "Create a minimal Python CLI and document the run command.",
            &workspace,
            &output_root,
            Some("intake-absolute-refs"),
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

        let prompt = render_stage_prompt(&ctx, &run_dir, "intake").expect("intake prompt");
        assert!(prompt.contains(
            &run_reference_asset_path(&run_dir, DECOMPOSITION_RULES_REF)
                .display()
                .to_string()
        ));
        assert!(prompt.contains(
            &run_reference_asset_path(&run_dir, ROLE_MAP_REF)
                .display()
                .to_string()
        ));
        assert!(prompt.contains(
            "prefer the provided role map and catalog summary before recursively scanning the local role catalog"
        ));
        assert!(prompt.contains(
            "do not inspect multi-agent-pipeline source files, tests, or SKILL documents"
        ));

        let _ = fs::remove_dir_all(run_dir);
        let _ = fs::remove_dir_all(output_root);
        let _ = fs::remove_dir_all(workspace);
    }

    #[test]
    fn compile_prompt_refreshes_stale_stage_prompt_file() {
        let ctx = test_context();
        let workspace = temp_dir("compile-prompt-refresh-workspace");
        let output_root = temp_dir("compile-prompt-refresh-output");
        let run_dir = create_run(
            &ctx,
            "Create a minimal Python CLI and document the run command.",
            &workspace,
            &output_root,
            Some("compile-prompt-refresh"),
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
        let prompt_path = stage_prompt_path(&run_dir, "solver-a").expect("prompt path");
        write_text(&prompt_path, "STALE PROMPT").expect("seed stale prompt");

        let compiled = compile_prompt(&ctx, &run_dir, "solver-a").expect("compile prompt");
        let refreshed = read_text(&prompt_path).expect("read refreshed prompt");

        assert!(!compiled.contains("STALE PROMPT"));
        assert!(!refreshed.contains("STALE PROMPT"));
        assert!(refreshed.contains("\"stage\": \"solver-a\""));

        let _ = fs::remove_dir_all(run_dir);
        let _ = fs::remove_dir_all(output_root);
        let _ = fs::remove_dir_all(workspace);
    }

    #[test]
    fn responses_review_prompt_embeds_agency_reviewer_docs() {
        let (ctx, root, agency_root, workspace) = test_context_with_sibling_agency("review-prompt");
        let output_root = temp_dir("review-prompt-output");
        let cache_root = temp_dir("review-prompt-cache");
        write_agency_role(
            &agency_root,
            "testing/testing-reality-checker.md",
            "Reality Checker",
            "Evidence-focused reviewer",
            "# Reality Checker\n\nDemand explicit evidence before approving results.",
        );
        let run_dir = create_run(
            &Context {
                stage_backend: "responses-readonly".to_string(),
                ..ctx.clone()
            },
            "Compare implementation options and pick the safest result.",
            &workspace,
            &output_root,
            Some("review-role-docs"),
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
            "# Brief\n\nCompare solver results.\n",
        )
        .expect("brief");
        write_text(
            &run_dir.join("solutions").join("solver-a").join("RESULT.md"),
            "# Result\n\nCandidate A.\n",
        )
        .expect("solver result");
        let mut plan = load_plan(&run_dir).expect("load plan");
        plan.reviewer_stack = vec!["testing/testing-reality-checker.md".to_string()];
        save_plan(&run_dir, &plan).expect("save plan");
        sync_run_artifacts(
            &Context {
                stage_backend: "responses-readonly".to_string(),
                ..ctx.clone()
            },
            &run_dir,
        )
        .expect("sync prompts");

        let prompt = build_responses_stage_prompt(
            &Context {
                stage_backend: "responses-readonly".to_string(),
                ..ctx.clone()
            },
            &run_dir,
            "review",
        )
        .expect("review prompt");
        assert!(prompt.contains("agency-role/testing/testing-reality-checker.md"));
        assert!(prompt.contains("Demand explicit evidence before approving results."));

        let _ = fs::remove_dir_all(run_dir);
        let _ = fs::remove_dir_all(output_root);
        let _ = fs::remove_dir_all(cache_root);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn run_codex_last_message_adds_agency_catalog_dir() {
        let root = temp_dir("codex-add-dir-agency");
        let repo_root = root.join("workspace").join("multi-agent-pipeline");
        let agency_root = root.join("agency-agents");
        let workdir = root.join("workdir");
        let artifact_dir = root.join("artifacts");
        fs::create_dir_all(&repo_root).expect("create repo root");
        fs::create_dir_all(&agency_root).expect("create agency root");
        fs::create_dir_all(&workdir).expect("create workdir");
        let script = r##"#!/bin/zsh
set -euo pipefail
script_dir="${0:A:h}"
last_message=""
print -rl -- "$@" > "$script_dir/args.log"
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
cat >/dev/null
mkdir -p "${last_message:h}"
print "ok" > "$last_message"
"##;
        let (mut ctx, script_root) = mock_script_context("agency-add-dir", script);
        ctx.repo_root = repo_root.clone();

        let message = run_codex_last_message(
            &ctx,
            "test prompt",
            &workdir,
            &artifact_dir,
            "solver-a",
            None,
        )
        .expect("run codex");
        assert_eq!(message.trim(), "ok");

        let args = read_text(&script_root.join("args.log")).expect("args log");
        assert!(args.contains("--add-dir"));
        assert!(args.contains(&repo_root.display().to_string()));
        assert!(args.contains(&agency_root.display().to_string()));

        let _ = fs::remove_dir_all(root);
        let _ = fs::remove_dir_all(script_root);
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
        write_text(
            &workspace.join("agpipe.pipeline.yml"),
            custom_pipeline_yaml(),
        )
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
        assert_eq!(
            result.code, 0,
            "unexpected automation output:\n{}",
            result.stdout
        );
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
        assert_eq!(status.stages.get("audit").map(String::as_str), Some("done"));
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
        let initial_expected = choose_roles(
            &plan.task_kind,
            &plan.original_task,
            std::cmp::max(1, plan.solver_roles.len()),
        );
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
        let updated_expected = choose_roles_with_workstreams(
            &updated.task_kind,
            &updated.original_task,
            std::cmp::max(1, updated.solver_roles.len()),
            &updated.workstream_hints,
        );
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
    fn solver_count_defaults_raise_research_floor_and_follow_workstreams() {
        assert_eq!(
            solver_count_for(
                "backend",
                "low",
                "alternatives",
                &[],
                "Build a backend service."
            ),
            1
        );
        assert_eq!(
            solver_count_for(
                "research",
                "low",
                "alternatives",
                &[],
                "Compare two options."
            ),
            2
        );
        assert_eq!(
            solver_count_for(
                "ai",
                "low",
                "decomposed",
                &[
                    WorkstreamHint {
                        name: "ingress".to_string(),
                        goal: "ingress".to_string(),
                        suggested_role: String::new(),
                    },
                    WorkstreamHint {
                        name: "analysis".to_string(),
                        goal: "analysis".to_string(),
                        suggested_role: String::new(),
                    },
                    WorkstreamHint {
                        name: "execution".to_string(),
                        goal: "execution".to_string(),
                        suggested_role: String::new(),
                    },
                ],
                "Build a multi-stage AI workflow.",
            ),
            3
        );
    }

    #[test]
    fn backend_role_defaults_align_with_implementation_first_angle() {
        let roles = choose_roles("backend", "Build a backend service.", 3);
        assert_eq!(roles.len(), 3);
        assert_eq!(roles[0].role, "engineering/engineering-senior-developer.md");
        assert_eq!(
            roles[1].role,
            "engineering/engineering-backend-architect.md"
        );
        assert_eq!(roles[2].role, "engineering/engineering-devops-automator.md");
    }

    #[test]
    fn infer_task_kind_does_not_treat_tui_as_frontend_ui() {
        assert_eq!(
            infer_task_kind("Audit the TUI runtime flow and improve stage reconciliation."),
            "fullstack"
        );
    }

    #[test]
    fn infer_task_kind_prefers_review_for_code_review_requests_with_ai_signals() {
        assert_eq!(
            infer_task_kind(
                "Проведи ревью кода ~/repo-under-review: нужны баги, риски, регрессии и пробелы в тестах."
            ),
            "review"
        );
    }

    #[test]
    fn infer_task_kind_prefers_research_over_infra_for_analysis_only_ci_cd_landscape() {
        assert_eq!(
            infer_task_kind(
                "Проведи анализ технологий AI coding-агентов в CI/CD на примере Codex и предложи улучшения качества решений."
            ),
            "research"
        );
    }

    #[test]
    fn infer_task_kind_treats_russian_report_only_research_as_research() {
        assert_eq!(
            infer_task_kind(
                "Проведи исследование моделей онлайн-дохода и подготовь один читабельный русскоязычный итоговый отчёт без лишних англоязычных терминов."
            ),
            "research"
        );
    }

    #[test]
    fn infer_task_kind_prefers_docs_for_official_docs_planning_requests() {
        assert_eq!(
            infer_task_kind(
                "Upgrade the FastAPI integration to the latest documented version, verify API changes against official docs, and update the implementation plan."
            ),
            "docs"
        );
    }

    #[test]
    fn infer_task_kind_detects_verification_seed_follow_up_as_audit_improve() {
        let task = "# Verified Follow-Up Task\n\nUse only the verification-derived context below as the authoritative seed for this rerun.\n\n## Goal Status\n\n- rerun recommended: `true`\n- missing critical checks:\n- `device_story_reconciled`\n\n## Verified Progress That Must Not Regress\n\n- keep the already-verified fixes\n\n## Follow-Up Task\n\nПроведи audit и analysis, refresh stale artifacts и закрой remaining critical check без реализации новых фич.\n";
        assert_eq!(infer_task_kind(task), "audit-improve");
        assert_eq!(infer_execution_mode("audit-improve", "high", task), "alternatives");
    }

    #[test]
    fn backend_reviewer_stack_uses_code_review_instead_of_exec_summary() {
        let stack = reviewer_stack_for("backend");
        assert!(stack
            .iter()
            .any(|item| item == "engineering/engineering-code-reviewer.md"));
        assert!(!stack
            .iter()
            .any(|item| item == "support/support-executive-summary-generator.md"));
    }

    #[test]
    fn create_run_auto_selects_exa_and_memory_for_research_tasks() {
        let ctx = test_context();
        let workspace = temp_dir("mcp-research-workspace");
        let output_root = temp_dir("mcp-research-output");
        let run_dir = create_run(
            &ctx,
            "Проведи анализ технологий AI coding-агентов в CI/CD на примере Codex и предложи улучшения качества решений.",
            &workspace,
            &output_root,
            Some("mcp-research"),
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
        let server_names: Vec<&str> = plan
            .mcp
            .servers
            .iter()
            .map(|item| item.name.as_str())
            .collect();
        assert!(server_names.contains(&"exa"));
        assert!(server_names.contains(&"fetch"));
        assert!(server_names.contains(&"memory"));
        assert!(!server_names.contains(&"context7"));
        assert!(plan
            .solver_roles
            .iter()
            .all(|solver| solver.mcp_servers.iter().any(|item| item == "exa")));
        assert!(plan
            .solver_roles
            .iter()
            .all(|solver| solver.mcp_servers.iter().any(|item| item == "fetch")));
        assert!(plan
            .solver_roles
            .iter()
            .all(|solver| !solver.mcp_servers.iter().any(|item| item == "memory")));
        assert_eq!(
            plan.solver_roles
                .iter()
                .map(|solver| solver.role.as_str())
                .collect::<Vec<_>>(),
            vec![
                "product/product-trend-researcher.md",
                "testing/testing-evidence-collector.md",
                "testing/testing-tool-evaluator.md",
            ]
        );
        assert!(run_dir.join("references").join("mcp-plan.md").exists());
    }

    #[test]
    fn create_run_uses_audit_improve_routing_and_local_mcp_for_verified_follow_up() {
        let ctx = test_context();
        let workspace = temp_dir("audit-improve-workspace");
        let output_root = temp_dir("audit-improve-output");
        let task = "# Verified Follow-Up Task\n\nUse only the verification-derived context below as the authoritative seed for this rerun.\nDo not rescope from stale review artifacts.\n\n## Goal Status\n\n- rerun recommended: `true`\n- missing critical checks:\n- `device_story_reconciled`\n\n## Verification Summary\n\nНужен узкий audit rerun без reopening уже подтверждённых фиксов.\n\n## Follow-Up Task\n\nПроведи audit и analysis, refresh stale artifacts, reconcile device story, preserve verified progress, без реализации новых фич.\n";
        let run_dir = create_run(
            &ctx,
            task,
            &workspace,
            &output_root,
            Some("audit-improve"),
            "compact",
            "ru",
            "research-first",
            "local-first",
            "fetch-if-needed",
            "~/.cache/multi-agent-pipeline",
            "reuse",
            None,
        )
        .expect("create audit-improve run");

        let plan = load_plan(&run_dir).expect("load plan");
        assert_eq!(plan.task_kind, "audit-improve");
        assert_eq!(plan.execution_mode, "alternatives");
        assert!(first_stage_id_for_kind(&plan, &run_dir, PipelineStageKind::Execution)
            .expect("execution stage lookup")
            .is_none());

        let server_names: Vec<&str> = plan
            .mcp
            .servers
            .iter()
            .map(|item| item.name.as_str())
            .collect();
        assert!(server_names.contains(&"memory"));
        assert!(!server_names.contains(&"fetch"));
        assert_eq!(
            plan.solver_roles
                .iter()
                .map(|solver| solver.role.as_str())
                .collect::<Vec<_>>(),
            vec![
                "engineering/engineering-backend-architect.md",
                "engineering/engineering-technical-writer.md",
                "testing/testing-tool-evaluator.md",
            ]
        );
    }

    #[test]
    fn create_run_auto_selects_fetch_and_memory_for_docs_tasks_without_context7_key() {
        let ctx = test_context();
        let workspace = temp_dir("mcp-context7-workspace");
        let output_root = temp_dir("mcp-context7-output");
        env::remove_var("CONTEXT7_API_KEY");
        let run_dir = create_run(
            &ctx,
            "Upgrade the FastAPI integration to the latest documented version, verify API changes against official docs, and update the implementation plan.",
            &workspace,
            &output_root,
            Some("mcp-context7"),
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
        let server_names: Vec<&str> = plan
            .mcp
            .servers
            .iter()
            .map(|item| item.name.as_str())
            .collect();
        assert!(server_names.contains(&"fetch"));
        assert!(server_names.contains(&"memory"));
        assert!(!server_names.contains(&"exa"));
        assert!(!server_names.contains(&"context7"));
        let fetch = plan
            .mcp
            .servers
            .iter()
            .find(|server| server.name == "fetch")
            .expect("fetch server");
        assert!(fetch.stages.iter().any(|stage| stage == "verification"));
        assert_eq!(
            plan.solver_roles
                .iter()
                .map(|solver| solver.role.as_str())
                .collect::<Vec<_>>(),
            vec![
                "engineering/engineering-technical-writer.md",
                "testing/testing-evidence-collector.md",
                "support/support-executive-summary-generator.md",
            ]
        );
    }

    #[test]
    fn create_run_auto_selects_context7_when_key_is_available() {
        let ctx = test_context();
        let workspace = temp_dir("mcp-context7-key-workspace");
        let output_root = temp_dir("mcp-context7-key-output");
        env::set_var("CONTEXT7_API_KEY", "test-key");
        let run_dir = create_run(
            &ctx,
            "Upgrade the FastAPI integration to the latest documented version, verify API changes against official docs, and update the implementation plan.",
            &workspace,
            &output_root,
            Some("mcp-context7-key"),
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
        let server_names: Vec<&str> = plan
            .mcp
            .servers
            .iter()
            .map(|item| item.name.as_str())
            .collect();
        assert!(server_names.contains(&"context7"));
        assert!(server_names.contains(&"fetch"));
        assert!(server_names.contains(&"memory"));
        assert!(!server_names.contains(&"exa"));
        env::remove_var("CONTEXT7_API_KEY");
    }

    #[test]
    fn create_run_auto_selects_git_for_review_tasks_on_git_workspaces() {
        let ctx = test_context();
        let workspace = temp_dir("mcp-review-git-workspace");
        let output_root = temp_dir("mcp-review-git-output");
        fs::create_dir_all(workspace.join(".git")).expect("create fake git dir");
        let run_dir = create_run(
            &ctx,
            "Проведи ревью кода репозитория и выдай findings по severity с file refs. Без изменений в коде.",
            &workspace,
            &output_root,
            Some("mcp-review-git"),
            "compact",
            "ru",
            "local-first",
            "local-first",
            "none",
            "~/.cache/multi-agent-pipeline",
            "reuse",
            None,
        )
        .expect("create run");

        let plan = load_plan(&run_dir).expect("load plan");
        let server_names: Vec<&str> = plan
            .mcp
            .servers
            .iter()
            .map(|item| item.name.as_str())
            .collect();
        assert!(server_names.contains(&"git"));
        assert!(server_names.contains(&"memory"));
        assert!(!server_names.contains(&"exa"));
        assert!(plan
            .solver_roles
            .iter()
            .all(|solver| solver.mcp_servers.iter().any(|item| item == "git")));
    }

    #[test]
    fn create_run_auto_selects_git_for_audit_improve_tasks_on_git_workspaces() {
        let ctx = test_context();
        let workspace = temp_dir("mcp-audit-git-workspace");
        let output_root = temp_dir("mcp-audit-git-output");
        fs::create_dir_all(workspace.join(".git")).expect("create fake git dir");
        let task = "# Verified Follow-Up Task\n\nUse only the verification-derived context below as the authoritative seed for this rerun.\n\n## Goal Status\n\n- rerun recommended: `true`\n- missing critical checks:\n- `probe_basis_reconciled`\n\n## Follow-Up Task\n\nПроведи audit-improve rerun, refresh stale narrative, preserve green slices и не переходи к новым фичам.\n";
        let run_dir = create_run(
            &ctx,
            task,
            &workspace,
            &output_root,
            Some("mcp-audit-git"),
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
        let server_names: Vec<&str> = plan
            .mcp
            .servers
            .iter()
            .map(|item| item.name.as_str())
            .collect();
        assert!(server_names.contains(&"git"));
        assert!(server_names.contains(&"memory"));
        assert!(plan
            .solver_roles
            .iter()
            .all(|solver| solver.mcp_servers.iter().any(|item| item == "git")));
    }

    #[test]
    fn solver_prompt_contains_mcp_guidance_without_solver_memory_by_default() {
        let ctx = test_context();
        let workspace = temp_dir("mcp-solver-prompt-workspace");
        let output_root = temp_dir("mcp-solver-prompt-output");
        let run_dir = create_run(
            &ctx,
            "Проведи исследование по выбору стеков для AI coding pipeline и предложи рекомендации.",
            &workspace,
            &output_root,
            Some("mcp-solver-prompt"),
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
        let prompt = render_solver_prompt(&ctx, &run_dir, &plan, &plan.solver_roles[0]);
        assert!(prompt.contains("\"mcp_servers\""));
        assert!(prompt.contains("references/mcp-plan.md"));
        assert!(!prompt.contains("sibling solver namespaces"));
        assert!(!prompt.contains(&solver_memory_namespace(
            &run_dir,
            &plan.solver_roles[0].solver_id
        )));
    }

    #[test]
    fn solver_prompt_for_russian_mentions_plain_russian_style_and_localized_mcp_heading() {
        let ctx = test_context();
        let workspace = temp_dir("solver-ru-style-workspace");
        let output_root = temp_dir("solver-ru-style-output");
        let run_dir = create_run(
            &ctx,
            "Проведи исследование и подготовь итоговый русскоязычный отчёт без лишнего англоязычного жаргона.",
            &workspace,
            &output_root,
            Some("solver-ru-style"),
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
        let prompt = render_solver_prompt(&ctx, &run_dir, &plan, &plan.solver_roles[0]);
        assert!(prompt.contains("avoid unnecessary English jargon"));
        assert!(prompt.contains("Использование MCP"));
    }

    #[test]
    fn record_stage_mcp_usage_extracts_markdown_section_and_persists_jsonl() {
        let run_dir = temp_run_dir("mcp-usage-log");
        let workspace = temp_dir("mcp-usage-workspace");
        let mut plan = Plan {
            workspace: workspace.display().to_string(),
            workspace_exists: true,
            original_task: "Проведи ревью кода без изменений.".to_string(),
            task_kind: "review".to_string(),
            prompt_format: "compact".to_string(),
            summary_language: "ru".to_string(),
            intake_research_mode: "local-first".to_string(),
            stage_research_mode: "local-first".to_string(),
            execution_network_mode: "none".to_string(),
            solver_count: 1,
            solver_roles: vec![SolverRole {
                solver_id: "solver-a".to_string(),
                role: "engineering/engineering-code-reviewer.md".to_string(),
                angle: "risk-first".to_string(),
                mcp_servers: vec!["git".to_string(), "memory".to_string()],
            }],
            reviewer_stack: vec!["engineering/engineering-code-reviewer.md".to_string()],
            mcp: McpSelection {
                auto_select: true,
                rationale: vec!["test".to_string()],
                servers: vec![
                    McpServerPlan {
                        name: "git".to_string(),
                        mode: "readonly".to_string(),
                        stages: vec!["solver".to_string(), "review".to_string()],
                        purposes: vec!["repo inspection".to_string()],
                        usage_hint: "Prefer git for local history.".to_string(),
                    },
                    McpServerPlan {
                        name: "memory".to_string(),
                        mode: "readonly".to_string(),
                        stages: vec!["solver".to_string(), "review".to_string()],
                        purposes: vec!["handoff".to_string()],
                        usage_hint: "Use stage memory namespace.".to_string(),
                    },
                ],
            },
            ..Plan::default()
        };
        plan.pipeline = PipelineConfig {
            source: "test".to_string(),
            stages: default_pipeline_stage_specs(&plan, Some(&run_dir)),
        };
        save_plan(&run_dir, &plan).expect("save plan");
        write_json(
            &run_dir.join("runtime").join("mcp-provision.json"),
            &McpProvisionRecord {
                enabled: true,
                configured: vec!["git".to_string(), "memory".to_string()],
                ..McpProvisionRecord::default()
            },
        )
        .expect("write mcp provision");
        write_text(
            &run_dir.join("review").join("report.md"),
            "# Review Report\n\n## MCP Usage\n\n- git: used to inspect status and diff.\n- memory: not used; review stayed self-contained.\n",
        )
        .expect("write review report");

        record_stage_mcp_usage(&plan, &run_dir, "review", "codex", "executed")
            .expect("record mcp usage");

        let usage_lines = read_text(&run_dir.join(MCP_USAGE_LOG_REF)).expect("read jsonl");
        let first_line = usage_lines.lines().next().expect("first jsonl line");
        let record: McpUsageRecord =
            serde_json::from_str(first_line).expect("parse mcp usage record");
        assert_eq!(record.stage, "review");
        assert_eq!(record.stage_kind, "review");
        assert_eq!(record.backend, "codex");
        assert!(record.selected.iter().any(|item| item == "git"));
        assert!(record.selected.iter().any(|item| item == "memory"));
        assert!(record.available.iter().any(|item| item == "git"));
        assert!(record.declared_used.iter().any(|item| item == "git"));
        assert!(record.declared_not_used.iter().any(|item| item == "memory"));
        assert!(record.note_present);
        assert!(
            run_dir
                .join("runtime")
                .join("mcp")
                .join("review.md")
                .exists(),
            "expected persisted MCP note"
        );
    }

    #[test]
    fn record_stage_mcp_usage_extracts_localized_russian_heading() {
        let run_dir = temp_run_dir("mcp-usage-log-ru-heading");
        let workspace = temp_dir("mcp-usage-workspace-ru-heading");
        let mut plan = Plan {
            workspace: workspace.display().to_string(),
            workspace_exists: true,
            original_task: "Проведи ревью кода без изменений.".to_string(),
            task_kind: "review".to_string(),
            prompt_format: "compact".to_string(),
            summary_language: "ru".to_string(),
            intake_research_mode: "local-first".to_string(),
            stage_research_mode: "local-first".to_string(),
            execution_network_mode: "none".to_string(),
            solver_count: 1,
            solver_roles: vec![SolverRole {
                solver_id: "solver-a".to_string(),
                role: "engineering/engineering-code-reviewer.md".to_string(),
                angle: "risk-first".to_string(),
                mcp_servers: vec!["git".to_string()],
            }],
            reviewer_stack: vec!["engineering/engineering-code-reviewer.md".to_string()],
            mcp: McpSelection {
                auto_select: true,
                rationale: vec!["test".to_string()],
                servers: vec![McpServerPlan {
                    name: "git".to_string(),
                    mode: "readonly".to_string(),
                    stages: vec!["solver".to_string(), "review".to_string()],
                    purposes: vec!["repo inspection".to_string()],
                    usage_hint: "Prefer git for local history.".to_string(),
                }],
            },
            ..Plan::default()
        };
        plan.pipeline = PipelineConfig {
            source: "test".to_string(),
            stages: default_pipeline_stage_specs(&plan, Some(&run_dir)),
        };
        save_plan(&run_dir, &plan).expect("save plan");
        write_json(
            &run_dir.join("runtime").join("mcp-provision.json"),
            &McpProvisionRecord {
                enabled: true,
                configured: vec!["git".to_string()],
                ..McpProvisionRecord::default()
            },
        )
        .expect("write mcp provision");
        write_text(
            &run_dir.join("review").join("report.md"),
            "# Отчёт review\n\n## Использование MCP\n\n- git: used to inspect status and diff.\n",
        )
        .expect("write localized review report");

        record_stage_mcp_usage(&plan, &run_dir, "review", "codex", "executed")
            .expect("record mcp usage");

        let note = read_text(&run_dir.join("runtime").join("mcp").join("review.md"))
            .expect("read persisted note");
        assert!(note.contains("git"));
    }

    #[test]
    fn write_responses_stage_outputs_persists_solver_mcp_note() {
        let run_dir = temp_run_dir("responses-mcp-note");
        let workspace = temp_dir("responses-mcp-workspace");
        let mut plan = Plan {
            workspace: workspace.display().to_string(),
            workspace_exists: true,
            original_task: "Проведи анализ и предложи варианты.".to_string(),
            task_kind: "research".to_string(),
            prompt_format: "compact".to_string(),
            summary_language: "ru".to_string(),
            intake_research_mode: "research-first".to_string(),
            stage_research_mode: "local-first".to_string(),
            execution_network_mode: "fetch-if-needed".to_string(),
            solver_count: 1,
            solver_roles: vec![SolverRole {
                solver_id: "solver-a".to_string(),
                role: "testing/testing-tool-evaluator.md".to_string(),
                angle: "evidence-first".to_string(),
                mcp_servers: vec!["fetch".to_string(), "memory".to_string()],
            }],
            mcp: McpSelection {
                auto_select: true,
                rationale: vec!["test".to_string()],
                servers: vec![
                    McpServerPlan {
                        name: "fetch".to_string(),
                        mode: "readonly".to_string(),
                        stages: vec!["solver".to_string()],
                        purposes: vec!["retrieval".to_string()],
                        usage_hint: "Use fetch after identifying URLs.".to_string(),
                    },
                    McpServerPlan {
                        name: "memory".to_string(),
                        mode: "readonly".to_string(),
                        stages: vec!["solver".to_string()],
                        purposes: vec!["handoff".to_string()],
                        usage_hint: "Use memory for handoff.".to_string(),
                    },
                ],
            },
            ..Plan::default()
        };
        plan.pipeline = PipelineConfig {
            source: "test".to_string(),
            stages: default_pipeline_stage_specs(&plan, Some(&run_dir)),
        };

        write_responses_stage_outputs(
            &plan,
            &run_dir,
            "solver-a",
            r##"{
  "result_md": "# Result\n\nResponses solver artifact.\n",
  "mcp_usage_md": "- fetch: used for one official page.\n- memory: not used; no handoff yet."
}"##,
        )
        .expect("write responses outputs");

        assert!(
            run_dir
                .join("solutions")
                .join("solver-a")
                .join("RESULT.md")
                .exists(),
            "expected solver artifact"
        );
        let note = read_text(&run_dir.join("runtime").join("mcp").join("solver-a.md"))
            .expect("read solver mcp note");
        assert!(note.contains("fetch"));
        assert!(note.contains("memory"));
    }

    #[test]
    fn responses_intake_save_plan_rehydrates_solver_mcp_assignments() {
        let ctx = test_context();
        let workspace = temp_dir("responses-intake-mcp-workspace");
        let output_root = temp_dir("responses-intake-mcp-output");
        let run_dir = create_run(
            &ctx,
            "Исследовать код проекта, сравнить варианты и дать рекомендацию по изменениям.",
            &workspace,
            &output_root,
            Some("responses-intake-mcp"),
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
        let current_plan = load_plan(&run_dir).expect("load plan");
        let mut plan_json = serde_json::to_value(&current_plan).expect("serialize plan");
        for solver in plan_json["solver_roles"]
            .as_array_mut()
            .expect("solver role array")
        {
            solver
                .as_object_mut()
                .expect("solver object")
                .remove("mcp_servers");
        }
        let payload = json!({
            "brief_md": "# Brief\n\nUpdated by intake.\n",
            "plan_json": plan_json,
            "mcp_usage_md": "- fetch: not used; local context was sufficient.\n- memory: not used; no durable handoff was needed."
        });

        write_responses_stage_outputs(
            &current_plan,
            &run_dir,
            "intake",
            &serde_json::to_string(&payload).expect("payload"),
        )
        .expect("write intake outputs");

        let persisted = load_plan(&run_dir).expect("reload plan");
        assert!(persisted
            .solver_roles
            .iter()
            .all(|solver| solver.mcp_servers.iter().any(|item| item == "fetch")));
        assert!(persisted
            .solver_roles
            .iter()
            .all(|solver| !solver.mcp_servers.iter().any(|item| item == "memory")));

        let prompt = render_solver_prompt(&ctx, &run_dir, &persisted, &persisted.solver_roles[0]);
        assert!(prompt.contains("\"mcp_servers\": ["));
        assert!(prompt.contains("\"fetch\""));
        assert!(!prompt.contains("\"memory\""));
    }

    #[test]
    fn solver_prompt_falls_back_to_plan_mcp_when_solver_assignment_is_missing() {
        let ctx = test_context();
        let workspace = temp_dir("solver-mcp-fallback-workspace");
        let output_root = temp_dir("solver-mcp-fallback-output");
        let run_dir = create_run(
            &ctx,
            "Исследовать код проекта, сравнить варианты и дать рекомендацию по изменениям.",
            &workspace,
            &output_root,
            Some("solver-mcp-fallback"),
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
        plan.solver_roles[0].mcp_servers.clear();

        let prompt = render_solver_prompt(&ctx, &run_dir, &plan, &plan.solver_roles[0]);

        assert!(prompt.contains("\"mcp_servers\": ["));
        assert!(prompt.contains("\"fetch\""));
        assert!(!prompt.contains("\"memory\""));
        assert!(prompt.contains("references/mcp-plan.md"));
    }

    #[test]
    fn provision_selected_mcp_servers_writes_remote_codex_blocks() {
        let run_dir = temp_dir("mcp-provision-run");
        let codex_home = temp_dir("mcp-provision-codex-home");
        env::set_var("AGPIPE_TEST_ALLOW_MCP_PROVISION", "1");
        let plan = Plan {
            mcp: McpSelection {
                auto_select: true,
                rationale: vec!["test".to_string()],
                servers: vec![
                    McpServerPlan {
                        name: "context7".to_string(),
                        mode: "readonly".to_string(),
                        stages: vec!["intake".to_string()],
                        purposes: vec!["docs".to_string()],
                        usage_hint: "Prefer official docs.".to_string(),
                    },
                    McpServerPlan {
                        name: "exa".to_string(),
                        mode: "readonly".to_string(),
                        stages: vec!["solver".to_string()],
                        purposes: vec!["research".to_string()],
                        usage_hint: "Prefer source discovery.".to_string(),
                    },
                ],
            },
            ..Plan::default()
        };

        let record = provision_selected_mcp_servers(&run_dir, &plan, Some(&codex_home));
        let config_text = read_text(&codex_home.join("config.toml")).expect("read codex config");

        assert!(record.configured.iter().any(|item| item == "context7"));
        assert!(record.configured.iter().any(|item| item == "exa"));
        assert!(config_text.contains("[mcp_servers.context7]"));
        assert!(config_text.contains("url = \"https://mcp.context7.com/mcp\""));
        assert!(config_text
            .contains("env_http_headers = { \"CONTEXT7_API_KEY\" = \"CONTEXT7_API_KEY\" }"));
        assert!(config_text.contains("[mcp_servers.exa]"));
        assert!(config_text.contains("url = \"https://mcp.exa.ai/mcp\""));
        env::remove_var("AGPIPE_TEST_ALLOW_MCP_PROVISION");
    }

    #[test]
    fn provision_selected_mcp_servers_is_idempotent_for_existing_blocks() {
        let run_dir = temp_dir("mcp-provision-idempotent-run");
        let codex_home = temp_dir("mcp-provision-idempotent-home");
        env::set_var("AGPIPE_TEST_ALLOW_MCP_PROVISION", "1");
        let plan = Plan {
            mcp: McpSelection {
                auto_select: true,
                rationale: vec!["test".to_string()],
                servers: vec![McpServerPlan {
                    name: "exa".to_string(),
                    mode: "readonly".to_string(),
                    stages: vec!["solver".to_string()],
                    purposes: vec!["research".to_string()],
                    usage_hint: "Prefer source discovery.".to_string(),
                }],
            },
            ..Plan::default()
        };

        let first = provision_selected_mcp_servers(&run_dir, &plan, Some(&codex_home));
        let second = provision_selected_mcp_servers(&run_dir, &plan, Some(&codex_home));
        let config_text = read_text(&codex_home.join("config.toml")).expect("read codex config");

        assert!(first.configured.iter().any(|item| item == "exa"));
        assert!(second.already_present.iter().any(|item| item == "exa"));
        assert_eq!(config_text.matches("[mcp_servers.exa]").count(), 1);
        env::remove_var("AGPIPE_TEST_ALLOW_MCP_PROVISION");
    }

    #[test]
    fn provision_selected_mcp_servers_writes_official_fetch_and_git_blocks() {
        let run_dir = temp_dir("mcp-provision-official-run");
        let codex_home = temp_dir("mcp-provision-official-home");
        env::set_var("AGPIPE_TEST_ALLOW_MCP_PROVISION", "1");
        let workspace = temp_dir("mcp-provision-official-workspace");
        fs::create_dir_all(workspace.join(".git")).expect("create fake git dir");
        let plan = Plan {
            workspace: workspace.display().to_string(),
            task_kind: "review".to_string(),
            original_task: "Проведи ревью кода репозитория без изменений".to_string(),
            mcp: McpSelection {
                auto_select: true,
                rationale: vec!["test".to_string()],
                servers: vec![
                    McpServerPlan {
                        name: "fetch".to_string(),
                        mode: "readonly".to_string(),
                        stages: vec!["solver".to_string()],
                        purposes: vec!["page retrieval".to_string()],
                        usage_hint: "Prefer Fetch.".to_string(),
                    },
                    McpServerPlan {
                        name: "git".to_string(),
                        mode: "readonly".to_string(),
                        stages: vec!["review".to_string()],
                        purposes: vec!["history".to_string()],
                        usage_hint: "Prefer Git.".to_string(),
                    },
                ],
            },
            ..Plan::default()
        };

        let record = provision_selected_mcp_servers(&run_dir, &plan, Some(&codex_home));
        let config_text = read_text(&codex_home.join("config.toml")).expect("read codex config");

        assert!(record.configured.iter().any(|item| item == "fetch"));
        assert!(record.configured.iter().any(|item| item == "git"));
        assert!(config_text.contains("[mcp_servers.fetch]"));
        assert!(config_text.contains("mcp/fetch"));
        assert!(config_text.contains("[mcp_servers.git]"));
        assert!(config_text.contains("mcp/git"));
        assert!(config_text.contains("enabled_tools = [\"git_status\", \"git_diff_unstaged\", \"git_diff_staged\", \"git_diff\", \"git_log\", \"git_show\", \"git_branch\"]"));
        env::remove_var("AGPIPE_TEST_ALLOW_MCP_PROVISION");
    }

    #[test]
    fn cli_backend_tasks_use_lightweight_reviewer_stack() {
        let stack = reviewer_stack_for_task(
            "backend",
            "low",
            "Create a minimal Python CLI in main.py, print exact stdout, and document the run command in README.md.",
        );
        assert_eq!(
            stack,
            vec![
                "testing/testing-reality-checker.md".to_string(),
                "engineering/engineering-code-reviewer.md".to_string(),
            ]
        );
    }

    #[test]
    fn create_run_uses_cli_focused_backend_roles_for_local_entrypoint_tasks() {
        let ctx = test_context();
        let workspace = temp_dir("backend-cli-role-workspace");
        let output_root = temp_dir("backend-cli-role-output");
        let run_dir = create_run(
            &ctx,
            "Create a minimal Python CLI in main.py, print exact stdout, and document the run command in README.md.",
            &workspace,
            &output_root,
            Some("backend-cli-role-test"),
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
        assert_eq!(plan.task_kind, "backend");
        assert_eq!(plan.solver_count, 1);
        assert_eq!(plan.workstream_hints.len(), 2);
        assert_eq!(plan.workstream_hints[0].name, "local-entrypoint");
        assert_eq!(
            plan.workstream_hints[0].suggested_role,
            "engineering/engineering-rapid-prototyper.md"
        );
        assert_eq!(plan.workstream_hints[1].name, "run-contract");
        assert_eq!(
            plan.workstream_hints[1].suggested_role,
            "engineering/engineering-technical-writer.md"
        );
        assert_eq!(
            plan.solver_roles
                .iter()
                .map(|role| role.role.as_str())
                .collect::<Vec<_>>(),
            vec!["engineering/engineering-rapid-prototyper.md"]
        );
        assert!(plan
            .mcp
            .servers
            .iter()
            .all(|server| server.name != "fetch" && server.name != "context7"));
        assert_eq!(
            plan.reviewer_stack,
            vec![
                "testing/testing-reality-checker.md".to_string(),
                "engineering/engineering-code-reviewer.md".to_string(),
            ]
        );
        assert!(plan
            .validation_commands
            .iter()
            .any(|item| item == "python3 main.py"));
        let goal_ids: Vec<&str> = plan
            .goal_checks
            .iter()
            .map(|item| item.id.as_str())
            .collect();
        assert!(goal_ids.iter().any(|item| *item == "main_py_entrypoint"));
        assert!(goal_ids.iter().any(|item| *item == "exact_stdout_contract"));
        assert!(goal_ids.iter().any(|item| *item == "readme_run_command"));

        let _ = fs::remove_dir_all(run_dir);
        let _ = fs::remove_dir_all(output_root);
        let _ = fs::remove_dir_all(workspace);
    }

    #[test]
    fn intake_stage_uses_local_template_for_execution_ready_cli_tasks() {
        let (ctx, mock_root, _bin_path, invocations_path, _tokens_path) =
            mock_codex_context("local-cli-intake");
        let workspace = temp_dir("local-cli-intake-workspace");
        let output_root = temp_dir("local-cli-intake-output");
        let run_dir = create_run(
            &ctx,
            "Create a minimal Python CLI in main.py, print exact stdout, and document the run command in README.md.",
            &workspace,
            &output_root,
            Some("local-cli-intake"),
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

        let intake = run_stage_capture(&ctx, &run_dir, "start", &["intake"]).expect("intake");

        assert_eq!(intake.code, 0, "{}", intake.stdout);
        assert_eq!(line_count(&invocations_path), 0);
        let brief = read_text(&run_dir.join("brief.md")).expect("read brief");
        assert!(brief.contains("## Goal coverage matrix"));
        assert!(brief.contains("`main.py` as the primary runnable entrypoint"));
        assert!(brief.contains("`README.md` with the exact local run command"));
        let last_message =
            read_text(&run_dir.join("logs").join("intake.last.md")).expect("read intake summary");
        assert!(last_message.contains("Local intake fast-path synthesized a backend CLI brief"));

        let _ = fs::remove_dir_all(run_dir);
        let _ = fs::remove_dir_all(output_root);
        let _ = fs::remove_dir_all(workspace);
        let _ = fs::remove_dir_all(mock_root);
    }

    #[test]
    fn solver_prompt_for_cli_run_forbids_workspace_edits_and_assigns_focus() {
        let ctx = test_context();
        let workspace = temp_dir("solver-prompt-quality-workspace");
        let output_root = temp_dir("solver-prompt-quality-output");
        let run_dir = create_run(
            &ctx,
            "Create a minimal Python CLI in main.py, print exact stdout, and document the run command in README.md.",
            &workspace,
            &output_root,
            Some("solver-prompt-quality"),
            "markdown",
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

        let prompt = render_solver_prompt(&ctx, &run_dir, &plan, &plan.solver_roles[0]);

        assert!(prompt.contains("do not modify the primary workspace during solver stage"));
        assert!(prompt.contains(
            "do not edit `agent-runs/.../solutions/<solver>/RESULT.md` directly"
        ));
        assert!(prompt.contains(
            "pipeline will materialize the file"
        ));
        assert!(prompt.contains("stay centered on the assigned workstreams and angle"));
        assert!(prompt.contains("treat the resolved role docs listed above as authoritative"));
        assert!(prompt
            .contains("`local-entrypoint`: build the requested local CLI or script entrypoint"));

        let _ = fs::remove_dir_all(run_dir);
        let _ = fs::remove_dir_all(output_root);
        let _ = fs::remove_dir_all(workspace);
    }

    #[test]
    fn solver_prompt_filters_heavy_validation_hints_for_analysis_runs() {
        let ctx = test_context();
        let workspace = temp_dir("solver-prompt-analysis-workspace");
        let output_root = temp_dir("solver-prompt-analysis-output");
        let run_dir = create_run(
            &ctx,
            "Проведи аудит сервиса без изменений кода и собери evidence-driven analysis.",
            &workspace,
            &output_root,
            Some("solver-prompt-analysis"),
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
        plan.validation_commands = vec![
            "pytest".to_string(),
            "PYTHONPATH=src python -m llm_freecad.main --chat-id smoke --photo-path data/seed_examples/rectangular_prism.pgm --result-json-path /tmp/out.json".to_string(),
            "python run_service.py --help".to_string(),
        ];

        let prompt = render_solver_prompt(&ctx, &run_dir, &plan, &plan.solver_roles[0]);

        assert!(prompt.contains("\"validation_hints\": ["));
        assert!(prompt.contains("\"pytest\""));
        assert!(prompt.contains("run_service.py --help"));
        assert!(!prompt.contains("--photo-path"));
        assert!(prompt.contains("prefer quick, bounded validation first"));
        assert!(prompt.contains("leave the expensive probe for verification"));

        let _ = fs::remove_dir_all(run_dir);
        let _ = fs::remove_dir_all(output_root);
        let _ = fs::remove_dir_all(workspace);
    }

    #[test]
    fn review_prompt_requires_structured_scorecard_and_duplicate_detection() {
        let ctx = test_context();
        let workspace = temp_dir("review-prompt-quality-workspace");
        let output_root = temp_dir("review-prompt-quality-output");
        let run_dir = create_run(
            &ctx,
            "Create a minimal Python CLI in main.py, print exact stdout, and document the run command in README.md.",
            &workspace,
            &output_root,
            Some("review-prompt-quality"),
            "markdown",
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

        let prompt = render_review_prompt(&ctx, &run_dir, &plan);

        assert!(prompt.contains("materially identical"));
        assert!(prompt.contains(
            "treat the resolved reviewer docs and review rubric as the primary review guidance"
        ));
        assert!(prompt.contains("`validation_evidence`"));
        assert!(prompt.contains("`execution_notes`"));
        assert!(prompt.contains("`winner`: selected solver id or `hybrid`"));
        assert!(prompt.contains(
            "do not edit `review/report.md`, `review/scorecard.json`, or `review/user-summary.md` directly"
        ));
        assert!(prompt.contains(
            "pipeline can materialize the files"
        ));

        let _ = fs::remove_dir_all(run_dir);
        let _ = fs::remove_dir_all(output_root);
        let _ = fs::remove_dir_all(workspace);
    }

    #[test]
    fn review_prompt_filters_heavy_validation_hints_for_analysis_runs() {
        let ctx = test_context();
        let workspace = temp_dir("review-prompt-analysis-workspace");
        let output_root = temp_dir("review-prompt-analysis-output");
        let run_dir = create_run(
            &ctx,
            "Проведи аудит сервиса без изменений кода и собери evidence-driven analysis.",
            &workspace,
            &output_root,
            Some("review-prompt-analysis"),
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
        plan.validation_commands = vec![
            "pytest".to_string(),
            "PYTHONPATH=src python -m llm_freecad.main --chat-id smoke --photo-path data/seed_examples/rectangular_prism.pgm --result-json-path /tmp/out.json".to_string(),
        ];

        let prompt = render_review_prompt(&ctx, &run_dir, &plan);

        assert!(prompt.contains("\"validation_hints\": ["));
        assert!(prompt.contains("\"pytest\""));
        assert!(!prompt.contains("--photo-path"));
        assert!(prompt.contains("prefer quick, bounded validation first"));

        let _ = fs::remove_dir_all(run_dir);
        let _ = fs::remove_dir_all(output_root);
        let _ = fs::remove_dir_all(workspace);
    }

    #[test]
    fn audit_improve_review_prompt_uses_light_stack_and_skips_runtime_harness() {
        let ctx = test_context();
        let workspace = temp_dir("audit-improve-review-workspace");
        let output_root = temp_dir("audit-improve-review-output");
        let task = "# Verified Follow-Up Task\n\nUse only the verification-derived context below as the authoritative seed for this rerun.\nDo not rescope from stale review artifacts.\n\n## Goal Status\n\n- rerun recommended: `true`\n- missing critical checks:\n- `device_story_reconciled`\n\n## Verification Summary\n\nНужен узкий audit rerun без reopening уже подтверждённых фиксов.\n\n## Follow-Up Task\n\nПроведи audit и analysis, refresh stale artifacts, reconcile device story, preserve verified progress, без реализации новых фич.\n";
        let run_dir = create_run(
            &ctx,
            task,
            &workspace,
            &output_root,
            Some("audit-improve-review"),
            "compact",
            "ru",
            "research-first",
            "local-first",
            "fetch-if-needed",
            "~/.cache/multi-agent-pipeline",
            "reuse",
            None,
        )
        .expect("create audit-improve run");
        let prompt = compile_prompt(&ctx, &run_dir, "review").expect("compile review prompt");

        assert!(prompt.contains("Audit-improve focus for this stage:"));
        assert!(prompt.contains("verification-seeded request"));
        assert!(!prompt.contains("Runtime harness:"));
        assert!(prompt.contains("engineering/engineering-code-reviewer.md"));
        assert!(!prompt.contains("testing/testing-tool-evaluator.md"));

        let _ = fs::remove_dir_all(run_dir);
        let _ = fs::remove_dir_all(output_root);
        let _ = fs::remove_dir_all(workspace);
    }

    #[test]
    fn russian_audit_task_forbids_workspace_changes_and_prefers_lightweight_validation() {
        let task = "Проведи комплексный анализ сервиса `./repo-under-review`.\n\nЭто именно задача на аудит и исследование, а не на реализацию исправлений. Не переходи к изменению кода, рефакторингу или внедрению фиксов, если это не будет отдельно запрошено.";
        assert!(task_forbids_workspace_changes(task));
        assert!(!task_requests_workspace_changes(task));
        assert!(task_is_analysis_only(task));

        let plan = Plan {
            original_task: task.to_string(),
            task_kind: "ai".to_string(),
            ..Plan::default()
        };
        assert!(prompt_prefers_lightweight_validation(&plan));
    }

    #[test]
    fn analytical_document_request_is_treated_as_analysis_only() {
        let task = "Проведи комплексный анализ сервиса `/srv/sample-service` и предложи варианты его переноса на `Docker Swarm` и `Docker Compose` с правильной логикой деплоя и отката.\n\nИтоговый результат нужен в виде аналитического документа. Не превращай задачу в реализацию: рабочие артефакты в репозитории (`Dockerfile`, `docker-compose.yml`, swarm-манифесты, правки `.gitlab-ci.yml`, deploy/rollback-скрипты) не являются обязательной частью результата, если без них можно обойтись.";
        assert!(task_forbids_workspace_changes(task));
        assert!(!task_requests_workspace_changes(task));
        assert!(task_is_analysis_only(task));
        assert!(!default_pipeline_includes_execution("research", task));

        let ctx = test_context();
        let workspace = temp_dir("analytical-document-analysis-workspace");
        let output_root = temp_dir("analytical-document-analysis-output");
        let run_dir = create_run(
            &ctx,
            task,
            &workspace,
            &output_root,
            Some("analytical-document-analysis"),
            "compact",
            "ru",
            "research-first",
            "local-first",
            "fetch-if-needed",
            "~/.cache/multi-agent-pipeline",
            "reuse",
            None,
        )
        .expect("create analysis-only document run");

        let plan = load_plan(&run_dir).expect("load plan");
        assert!(!available_stages(&run_dir)
            .expect("available stages")
            .iter()
            .any(|stage| stage == "execution"));
        assert!(task_is_analysis_only(&plan.original_task));

        let _ = fs::remove_dir_all(run_dir);
        let _ = fs::remove_dir_all(output_root);
        let _ = fs::remove_dir_all(workspace);
    }

    #[test]
    fn russian_report_only_research_request_is_analysis_only() {
        let task = "Проведи исследование моделей онлайн-дохода и подготовь один читабельный русскоязычный итоговый отчёт без лишних англоязычных терминов.";
        assert!(task_is_analysis_only(task));
        assert!(!task_requests_workspace_changes(task));
    }

    #[test]
    fn analysis_only_run_omits_implementation_goal_checks_and_solver_plan_language() {
        let ctx = test_context();
        let workspace = temp_dir("analysis-only-goals-workspace");
        let output_root = temp_dir("analysis-only-goals-output");
        let run_dir = create_run(
            &ctx,
            "Проведи комплексный анализ сервиса `./repo-under-review`. Это именно задача на аудит и исследование, а не на реализацию исправлений. Не переходи к изменению кода, рефакторингу или внедрению фиксов.",
            &workspace,
            &output_root,
            Some("analysis-only-goals"),
            "compact",
            "ru",
            "research-first",
            "local-first",
            "fetch-if-needed",
            "~/.cache/multi-agent-pipeline",
            "reuse",
            None,
        )
        .expect("create analysis run");

        let plan = load_plan(&run_dir).expect("load plan");
        let goal_ids: Vec<&str> = plan
            .goal_checks
            .iter()
            .map(|item| item.id.as_str())
            .collect();
        assert!(!goal_ids.iter().any(|item| *item == "analysis_adapter"));
        assert!(!goal_ids.iter().any(|item| *item == "freecad_output"));
        assert!(!goal_ids.iter().any(|item| *item == "runnable_entrypoint"));
        assert!(goal_ids
            .iter()
            .any(|item| *item == "reproducible_audit_report"));
        assert!(goal_ids
            .iter()
            .any(|item| *item == "residual_gaps_documented"));

        let prompt = render_solver_prompt(&ctx, &run_dir, &plan, &plan.solver_roles[0]);
        assert!(prompt.contains("analysis-only stage"));
        assert!(!prompt.contains("implementation summary or exact file plan"));
    }

    #[test]
    fn build_codex_command_inherits_ephemeral_flag_and_agency_catalog_dir() {
        let (ctx, root, agency_root, workspace) =
            test_context_with_sibling_agency("build-codex-command");
        let output_root = temp_dir("build-codex-command-output");
        let cache_root = temp_dir("build-codex-command-cache");
        let run_dir = create_run(
            &ctx,
            "Create a minimal Python CLI and document the run command.",
            &workspace,
            &output_root,
            Some("build-codex-command"),
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
        let _env = ScopedEnvVar::set("AGPIPE_CODEX_EPHEMERAL", "1");

        let (command, _prompt) =
            build_codex_command(&ctx, &run_dir, "intake", &StartArgs::default())
                .expect("build codex command");

        assert!(command.iter().any(|item| item == "--ephemeral"));
        assert!(command
            .windows(2)
            .any(|pair| pair == ["--config", "model_reasoning_effort=\"low\""]));
        assert!(command.iter().any(|item| item == "--add-dir"));
        assert!(command
            .iter()
            .any(|item| item == &agency_root.display().to_string()));
        assert!(!command
            .iter()
            .any(|item| item == &ctx.repo_root.display().to_string()));

        let _ = fs::remove_dir_all(run_dir);
        let _ = fs::remove_dir_all(output_root);
        let _ = fs::remove_dir_all(cache_root);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn build_codex_command_uses_medium_reasoning_for_low_complexity_non_trivial_stage() {
        let ctx = test_context();
        let workspace = temp_dir("build-codex-medium-workspace");
        let output_root = temp_dir("build-codex-medium-output");
        let cache_root = temp_dir("build-codex-medium-cache");
        let run_dir = create_run(
            &ctx,
            "Проведи короткое ревью кода и дай список замечаний без реализации.",
            &workspace,
            &output_root,
            Some("build-codex-medium"),
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

        let (command, _prompt) =
            build_codex_command(&ctx, &run_dir, "solver-a", &StartArgs::default())
                .expect("build codex command");

        assert!(command
            .windows(2)
            .any(|pair| pair == ["--config", "model_reasoning_effort=\"medium\""]));

        let _ = fs::remove_dir_all(run_dir);
        let _ = fs::remove_dir_all(output_root);
        let _ = fs::remove_dir_all(cache_root);
        let _ = fs::remove_dir_all(workspace);
    }

    #[test]
    fn research_default_pipeline_skips_execution_and_uses_research_defaults() {
        let ctx = test_context();
        let workspace = temp_dir("research-default-workspace");
        let output_root = temp_dir("research-default-output");

        let run_dir = create_run(
            &ctx,
            "Исследовать код проекта, сравнить варианты и дать рекомендацию по изменениям.",
            &workspace,
            &output_root,
            Some("research-defaults"),
            "compact",
            "ru",
            "research-first",
            "local-first",
            "fetch-if-needed",
            "~/.cache/multi-agent-pipeline",
            "reuse",
            None,
        )
        .expect("create research run");

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
                "solver-a",
                "solver-b",
                "solver-c",
                "review",
                "verification"
            ]
        );
        assert_eq!(plan.solver_count, 3);
        assert_eq!(
            plan.solver_roles
                .iter()
                .map(|role| role.angle.as_str())
                .collect::<Vec<_>>(),
            vec!["breadth-first", "evidence-first", "risk-first"]
        );
        assert_eq!(plan.reviewer_stack, reviewer_stack_for("research"));
        assert_eq!(
            available_stages(&run_dir).expect("available stages"),
            vec![
                "intake",
                "solver-a",
                "solver-b",
                "solver-c",
                "review",
                "verification"
            ]
        );

        write_text(&run_dir.join("brief.md"), "# Brief\n\nResearch brief.\n").expect("brief");
        write_text(
            &run_dir.join("solutions").join("solver-a").join("RESULT.md"),
            "# Result\n\nOption A.\n",
        )
        .expect("solver-a");
        write_text(
            &run_dir.join("solutions").join("solver-b").join("RESULT.md"),
            "# Result\n\nOption B.\n",
        )
        .expect("solver-b");
        write_text(
            &run_dir.join("solutions").join("solver-c").join("RESULT.md"),
            "# Result\n\nOption C.\n",
        )
        .expect("solver-c");
        write_text(
            &run_dir.join("review").join("report.md"),
            "# Review Report\n\nSelected solver-a.\n",
        )
        .expect("review report");
        write_json(
            &run_dir.join("review").join("scorecard.json"),
            &json!({"winner": "solver-a"}),
        )
        .expect("review scorecard");
        write_text(
            &run_dir.join("review").join("user-summary.md"),
            "# User Summary\n\nResearch summary.\n",
        )
        .expect("review summary");

        assert_eq!(
            next_stage_for_run(&run_dir).expect("next after review"),
            Some("verification".to_string())
        );
    }

    #[test]
    fn create_run_builds_review_only_pipeline_for_code_review_requests() {
        let ctx = test_context();
        let workspace = temp_dir("review-only-workspace");
        let output_root = temp_dir("review-only-output");
        fs::create_dir_all(workspace.join("src")).expect("create src");
        fs::create_dir_all(workspace.join("tests")).expect("create tests");
        write_text(&workspace.join("pytest.ini"), "[pytest]\n").expect("write pytest.ini");
        write_text(
            &workspace.join("src").join("main.py"),
            "def answer():\n    return 42\n",
        )
        .expect("write main.py");
        write_text(
            &workspace.join("tests").join("test_main.py"),
            "from src.main import answer\n\n\ndef test_answer():\n    assert answer() == 42\n",
        )
        .expect("write test_main.py");

        let run_dir = create_run(
            &ctx,
            "Проведи ревью кода ~/repo-under-review. Без изменений в коде: нужны баги, риски, регрессии и пробелы в тестах с приоритетом по severity и ссылками на файлы.",
            &workspace,
            &output_root,
            Some("review-only"),
            "markdown",
            "ru",
            "local-first",
            "local-first",
            "none",
            "~/.cache/multi-agent-pipeline",
            "reuse",
            None,
        )
        .expect("create review run");

        let plan = load_plan(&run_dir).expect("load plan");
        let stage_ids: Vec<String> = plan
            .pipeline
            .stages
            .iter()
            .map(|stage| stage.id.clone())
            .collect();
        let goal_ids: Vec<&str> = plan
            .goal_checks
            .iter()
            .map(|item| item.id.as_str())
            .collect();

        assert_eq!(plan.task_kind, "review");
        assert_eq!(plan.execution_mode, "alternatives");
        assert_eq!(plan.solver_count, 2);
        assert_eq!(
            stage_ids,
            vec!["intake", "solver-a", "solver-b", "review", "verification"]
        );
        assert_eq!(
            plan.workstream_hints
                .iter()
                .map(|item| item.name.as_str())
                .collect::<Vec<_>>(),
            vec![
                "correctness-and-bugs",
                "regression-and-risk",
                "tests-and-evidence",
            ]
        );
        assert_eq!(
            plan.solver_roles
                .iter()
                .map(|role| role.role.as_str())
                .collect::<Vec<_>>(),
            vec![
                "engineering/engineering-code-reviewer.md",
                "testing/testing-reality-checker.md",
            ]
        );
        assert_eq!(plan.reviewer_stack, reviewer_stack_for("review"));
        assert!(goal_ids.iter().any(|item| *item == "review_findings"));
        assert!(goal_ids
            .iter()
            .any(|item| *item == "evidence_with_file_refs"));
        assert!(goal_ids.iter().any(|item| *item == "review_only_scope"));
        assert!(!goal_ids.iter().any(|item| *item == "analysis_adapter"));
        assert!(!goal_ids.iter().any(|item| *item == "freecad_output"));
        assert!(!goal_ids.iter().any(|item| *item == "runnable_entrypoint"));
        assert!(plan.validation_commands.iter().any(|item| item == "pytest"));

        let _ = fs::remove_dir_all(run_dir);
        let _ = fs::remove_dir_all(output_root);
        let _ = fs::remove_dir_all(workspace);
    }

    #[test]
    fn review_solver_prompt_uses_review_deliverables_instead_of_implementation_plan() {
        let ctx = test_context();
        let workspace = temp_dir("review-solver-prompt-workspace");
        let output_root = temp_dir("review-solver-prompt-output");

        let run_dir = create_run(
            &ctx,
            "Проведи ревью кода проекта и выдай findings по severity с file refs. Без изменений в коде.",
            &workspace,
            &output_root,
            Some("review-solver-prompt"),
            "markdown",
            "ru",
            "local-first",
            "local-first",
            "none",
            "~/.cache/multi-agent-pipeline",
            "reuse",
            None,
        )
        .expect("create review run");
        let plan = load_plan(&run_dir).expect("load plan");

        let prompt = render_solver_prompt(&ctx, &run_dir, &plan, &plan.solver_roles[0]);

        assert!(prompt.contains("findings with severity and evidence"));
        assert!(prompt.contains("review-only stage"));
        assert!(!prompt.contains("implementation summary or exact file plan"));

        let _ = fs::remove_dir_all(run_dir);
        let _ = fs::remove_dir_all(output_root);
        let _ = fs::remove_dir_all(workspace);
    }

    #[test]
    fn start_solver_batch_runs_solver_stages_in_parallel() {
        let (ctx, mock_root) = mock_parallel_solver_context("batch", 2);
        let workspace = temp_dir("solver-parallel-workspace");
        let output_root = temp_dir("solver-parallel-output");

        let run_dir = create_run(
            &ctx,
            "Исследовать код проекта, сравнить варианты и дать рекомендацию по изменениям.",
            &workspace,
            &output_root,
            Some("solver-parallel"),
            "compact",
            "ru",
            "research-first",
            "local-first",
            "fetch-if-needed",
            "~/.cache/multi-agent-pipeline",
            "reuse",
            None,
        )
        .expect("create solver parallel run");

        let intake = run_stage_capture(&ctx, &run_dir, "start", &["intake"]).expect("intake");
        assert_eq!(intake.code, 0);

        let solvers =
            run_stage_capture(&ctx, &run_dir, "start-solvers", &[]).expect("start solvers");

        assert_eq!(solvers.code, 0, "solver batch failed:\n{}", solvers.stdout);
        assert!(solvers.stdout.contains("Started solver-a."));
        assert!(solvers.stdout.contains("Started solver-b."));
        assert!(
            run_dir
                .join("solutions")
                .join("solver-a")
                .join("RESULT.md")
                .exists(),
            "solver-a result missing"
        );
        assert!(
            run_dir
                .join("solutions")
                .join("solver-b")
                .join("RESULT.md")
                .exists(),
            "solver-b result missing"
        );
        let started_gap = (read_solver_started_at(&run_dir, "solver-a")
            - read_solver_started_at(&run_dir, "solver-b"))
        .abs();
        assert!(
            started_gap < 1.0,
            "expected solver batch start overlap under observer-free capture, got gap {:.3}s\nstdout:\n{}",
            started_gap,
            solvers.stdout
        );

        let _ = fs::remove_dir_all(run_dir);
        let _ = fs::remove_dir_all(output_root);
        let _ = fs::remove_dir_all(workspace);
        let _ = fs::remove_dir_all(mock_root);
    }

    #[test]
    fn start_solver_batch_stays_parallel_with_engine_observer() {
        let (ctx, mock_root) = mock_parallel_solver_context("batch-observer", 2);
        let workspace = temp_dir("solver-parallel-observer-workspace");
        let output_root = temp_dir("solver-parallel-observer-output");

        let run_dir = create_run(
            &ctx,
            "Исследовать код проекта, сравнить варианты и дать рекомендацию по изменениям.",
            &workspace,
            &output_root,
            Some("solver-parallel-observer"),
            "compact",
            "ru",
            "research-first",
            "local-first",
            "fetch-if-needed",
            "~/.cache/multi-agent-pipeline",
            "reuse",
            None,
        )
        .expect("create solver parallel observer run");

        let intake = run_stage_capture(&ctx, &run_dir, "start", &["intake"]).expect("intake");
        assert_eq!(intake.code, 0);

        let observer = Arc::new(InterruptObserver {
            run_dir: run_dir.clone(),
        });
        let solvers = with_engine_observer(observer, || {
            run_stage_capture(&ctx, &run_dir, "start-solvers", &[])
        })
        .expect("start solvers with observer");

        assert_eq!(solvers.code, 0, "solver batch failed:\n{}", solvers.stdout);
        let started_gap = (read_solver_started_at(&run_dir, "solver-a")
            - read_solver_started_at(&run_dir, "solver-b"))
        .abs();
        assert!(
            started_gap < 1.0,
            "expected solver batch to stay parallel with observer attached, got gap {:.3}s\nstdout:\n{}",
            started_gap,
            solvers.stdout
        );

        let _ = fs::remove_dir_all(run_dir);
        let _ = fs::remove_dir_all(output_root);
        let _ = fs::remove_dir_all(workspace);
        let _ = fs::remove_dir_all(mock_root);
    }

    #[test]
    fn verification_prompt_without_execution_stage_uses_artifact_audit_guidance() {
        let workspace = temp_dir("verification-research-workspace");
        let output_root = temp_dir("verification-research-output");
        let ctx = test_context();
        let run_dir = create_run(
            &ctx,
            "Исследовать код проекта, сравнить варианты и дать рекомендацию по изменениям.",
            &workspace,
            &output_root,
            Some("verification-research"),
            "compact",
            "ru",
            "research-first",
            "local-first",
            "fetch-if-needed",
            "~/.cache/multi-agent-pipeline",
            "reuse",
            None,
        )
        .expect("create research run");
        let plan = load_plan(&run_dir).expect("load plan");

        let prompt = render_verification_prompt(&ctx, &run_dir, &plan);

        assert!(!prompt.contains(
            &run_dir
                .join("execution")
                .join("report.md")
                .display()
                .to_string()
        ));
        assert!(prompt.contains("research or documentation artifacts"));
        assert!(prompt.contains("review verdict and solver evidence"));
        assert!(prompt.contains(
            "do not edit `verification/findings.md`, `verification/user-summary.md`, `verification/goal-status.json`, `verification/improvement-request.md`, or `verification/augmented-task.md` directly"
        ));
        assert!(prompt.contains(
            "pipeline can materialize the files"
        ));
        assert!(prompt.contains(
            "do not report a missing `execution/report.md` as a defect when this run has no execution stage"
        ));
        assert!(prompt.contains(
            "not as a request to mutate previous run-local files under `agent-runs`"
        ));
        assert!(prompt.contains(
            "the rerun should write it under its own run directory"
        ));
        assert!(prompt.contains(
            &run_reference_asset_path(&run_dir, VERIFICATION_RUBRIC_REF)
                .display()
                .to_string()
        ));
        assert!(prompt.contains(&stage_memory_namespace(&run_dir, "verification")));
    }

    #[test]
    fn compiled_verification_stage_prompt_avoids_direct_run_artifact_edits() {
        let ctx = test_context();
        let workspace = temp_dir("verification-stage-wrapper-workspace");
        let output_root = temp_dir("verification-stage-wrapper-output");
        let run_dir = create_run(
            &ctx,
            "Исследовать код проекта, сравнить варианты и дать рекомендацию по изменениям.",
            &workspace,
            &output_root,
            Some("verification-stage-wrapper"),
            "compact",
            "ru",
            "research-first",
            "local-first",
            "fetch-if-needed",
            "~/.cache/multi-agent-pipeline",
            "reuse",
            None,
        )
        .expect("create research run");

        let prompt = compile_prompt(&ctx, &run_dir, "verification").expect("compile verification");

        assert!(prompt.contains(
            "Do not edit run-local stage output files directly unless the stage prompt explicitly asks for it"
        ));
        assert!(!prompt.contains("Update the requested artifacts directly on disk."));
        assert!(prompt.contains("do not open generic workflow `SKILL.md` files"));
        assert!(prompt.contains("workflow stage `verification` for a file-based run"));
        assert!(!prompt.contains("stage `verification` of a multi-agent pipeline"));

        let _ = fs::remove_dir_all(run_dir);
        let _ = fs::remove_dir_all(output_root);
        let _ = fs::remove_dir_all(workspace);
    }

    #[test]
    fn execution_prompt_treats_host_probe_as_stage_local_not_run_global() {
        let ctx = test_context();
        let workspace = temp_dir("execution-stage-probe-workspace");
        let output_root = temp_dir("execution-stage-probe-output");

        let run_dir = create_run(
            &ctx,
            "Исправить баг в сервисе и подтвердить результат локальной валидацией.",
            &workspace,
            &output_root,
            Some("execution-stage-probe"),
            "compact",
            "ru",
            "research-first",
            "local-first",
            "fetch-if-needed",
            "~/.cache/multi-agent-pipeline",
            "reuse",
            None,
        )
        .expect("create execution run");

        let prompt = compile_prompt(&ctx, &run_dir, "execution").expect("compile execution");

        assert!(prompt.contains("execution-stage launcher probe"));
        assert!(prompt.contains(
            "do not describe it as the authoritative probe for the whole run"
        ));

        let _ = fs::remove_dir_all(run_dir);
        let _ = fs::remove_dir_all(output_root);
        let _ = fs::remove_dir_all(workspace);
    }

    #[test]
    fn verification_prompt_does_not_fail_timestamp_drift_alone_when_facts_match() {
        let ctx = test_context();
        let workspace = temp_dir("verification-probe-drift-workspace");
        let output_root = temp_dir("verification-probe-drift-output");

        let run_dir = create_run(
            &ctx,
            "Исправить баг в сервисе и подтвердить результат локальной валидацией.",
            &workspace,
            &output_root,
            Some("verification-probe-drift"),
            "compact",
            "ru",
            "research-first",
            "local-first",
            "fetch-if-needed",
            "~/.cache/multi-agent-pipeline",
            "reuse",
            None,
        )
        .expect("create verification run");

        let prompt = compile_prompt(&ctx, &run_dir, "verification").expect("compile verification");

        assert!(prompt.contains("do not fail the run for timestamp drift alone"));
        assert!(prompt.contains("facts materially differ"));

        let _ = fs::remove_dir_all(run_dir);
        let _ = fs::remove_dir_all(output_root);
        let _ = fs::remove_dir_all(workspace);
    }

    #[test]
    fn doctor_and_status_surface_failed_incomplete_stage_attempts() {
        let ctx = test_context();
        let workspace = temp_dir("failed-attempt-workspace");
        let output_root = temp_dir("failed-attempt-output");

        let run_dir = create_run(
            &ctx,
            "Исправить баг в коде и подтвердить результат.",
            &workspace,
            &output_root,
            Some("failed-attempt"),
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

        write_text(&run_dir.join("brief.md"), "# Brief\n\nReady.\n").expect("brief");
        write_text(
            &run_dir.join("solutions").join("solver-a").join("RESULT.md"),
            "# Result\n\nOption A.\n",
        )
        .expect("solver-a");
        write_text(
            &run_dir.join("solutions").join("solver-b").join("RESULT.md"),
            "# Result\n\nOption B.\n",
        )
        .expect("solver-b");
        write_text(
            &run_dir.join("solutions").join("solver-c").join("RESULT.md"),
            "# Result\n\nOption C.\n",
        )
        .expect("solver-c");
        write_text(
            &run_dir.join("review").join("report.md"),
            "# Review Report\n\nSelected solver-a.\n",
        )
        .expect("review report");
        write_json(
            &run_dir.join("review").join("scorecard.json"),
            &json!({"winner": "solver-a"}),
        )
        .expect("scorecard");
        write_text(
            &run_dir.join("review").join("user-summary.md"),
            "# User Summary\n\nSelected solver-a.\n",
        )
        .expect("review summary");

        runtime::start_job(
            &run_dir,
            "start-next",
            Some("execution"),
            "start-next",
            std::process::id() as i32,
            std::process::id() as i32,
        )
        .expect("start job");
        runtime::finish_job(
            &run_dir,
            "exited",
            None,
            Some("Tracked process is no longer alive. Inspect run artifacts and logs."),
        )
        .expect("finish job");

        let doctor = doctor_report(&ctx, &run_dir).expect("doctor report");
        assert_eq!(doctor.health, "broken");
        assert_eq!(doctor.next, "execution");
        let attempt = doctor.last_attempt.as_ref().expect("last attempt");
        assert_eq!(attempt.stage, "execution");
        assert_eq!(attempt.status, "exited");
        assert!(
            doctor.issues.iter().any(|issue| issue.message.contains(
                "Latest `start-next` attempt for stage `execution` ended with status `exited`"
            )),
            "expected failed attempt issue, got {:?}",
            doctor.issues
        );

        let status = status_report(&ctx, &run_dir).expect("status report");
        let attempt = status.last_attempt.as_ref().expect("status last attempt");
        assert_eq!(attempt.stage, "execution");
        let text = status_text(&status);
        assert!(text.contains("next: execution"));
        assert!(text.contains("last-attempt-stage: execution"));
        assert!(text.contains("last-attempt-status: exited"));
    }

    #[test]
    fn doctor_report_normalizes_dead_running_job_state() {
        let ctx = test_context();
        let workspace = temp_dir("doctor-normalize-dead-workspace");
        let output_root = temp_dir("doctor-normalize-dead-output");
        let run_dir = create_run(
            &ctx,
            "Review the repository without changing code.",
            &workspace,
            &output_root,
            Some("doctor-normalize-dead"),
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

        write_text(&run_dir.join("brief.md"), "# Brief\n\nReady.\n").expect("brief");

        runtime::start_job(
            &run_dir,
            "resume",
            Some("solver-a"),
            "resume until verification",
            999_999,
            999_999,
        )
        .expect("start stale job");

        let mut state = runtime::load_job_state(&run_dir).expect("load running state");
        state.updated_at_unix = state.updated_at_unix.saturating_sub(60);
        fs::write(
            runtime::job_state_path(&run_dir),
            serde_json::to_vec_pretty(&state).expect("serialize job state"),
        )
        .expect("persist aged job state");

        let doctor = doctor_report(&ctx, &run_dir).expect("doctor report");
        assert_eq!(doctor.health, "broken");
        let attempt = doctor.last_attempt.as_ref().expect("last attempt");
        assert_eq!(attempt.stage, "solver-a");
        assert_eq!(attempt.status, "exited");
        assert!(
            attempt
                .message
                .contains("Tracked process is no longer alive"),
            "unexpected message: {}",
            attempt.message
        );

        let status = status_report(&ctx, &run_dir).expect("status report");
        let attempt = status.last_attempt.as_ref().expect("status last attempt");
        assert_eq!(attempt.stage, "solver-a");
        assert_eq!(attempt.status, "exited");

        let normalized = runtime::load_job_state(&run_dir).expect("normalized job state");
        assert_eq!(normalized.status, "exited");

        let _ = fs::remove_dir_all(output_root);
        let _ = fs::remove_dir_all(workspace);
    }

    #[test]
    fn doctor_flags_active_stage_that_has_stopped_emitting_progress() {
        let ctx = test_context();
        let workspace = temp_dir("doctor-stalled-stage-workspace");
        let output_root = temp_dir("doctor-stalled-stage-output");
        let run_dir = create_run(
            &ctx,
            "Проведи повторную проверку артефактов без изменения кода.",
            &workspace,
            &output_root,
            Some("doctor-stalled-stage"),
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

        write_text(&run_dir.join("brief.md"), "# Brief\n\nReady.\n").expect("brief");
        runtime::start_job(
            &run_dir,
            "resume",
            Some("verification"),
            "resume until verification",
            std::process::id() as i32,
            std::process::id() as i32,
        )
        .expect("start active verification job");

        let mut state = runtime::load_job_state(&run_dir).expect("load active state");
        state.started_at_unix = state.started_at_unix.saturating_sub(400);
        state.updated_at_unix = state.updated_at_unix.saturating_sub(400);
        fs::write(
            runtime::job_state_path(&run_dir),
            serde_json::to_vec_pretty(&state).expect("serialize active state"),
        )
        .expect("persist aged active state");

        let doctor = doctor_report(&ctx, &run_dir).expect("doctor report");
        assert_eq!(doctor.health, "broken");
        assert_eq!(doctor.safe_next_action, "interrupt");
        assert!(doctor.issues.iter().any(|issue| {
            issue.message.contains("appears stuck")
        }));

        let status = status_report(&ctx, &run_dir).expect("status report");
        assert_eq!(status.next, "solver-a");
        assert!(status.last_attempt.is_none());

        let _ = fs::remove_dir_all(output_root);
        let _ = fs::remove_dir_all(workspace);
    }

    #[test]
    fn doctor_ignores_completed_single_step_attempt_when_process_log_shows_previous_stage_finished()
    {
        let ctx = test_context();
        let workspace = temp_dir("doctor-ignore-stale-start-next-workspace");
        let output_root = temp_dir("doctor-ignore-stale-start-next-output");
        let run_dir = create_run(
            &ctx,
            "Проведи аудит репозитория без изменения кода.",
            &workspace,
            &output_root,
            Some("doctor-ignore-stale-start-next"),
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

        write_text(&run_dir.join("brief.md"), "# Brief\n\nReady.\n").expect("brief");
        runtime::start_job(
            &run_dir,
            "start-next",
            Some("solver-a"),
            "start-next",
            std::process::id() as i32,
            std::process::id() as i32,
        )
        .expect("start stale start-next job");
        runtime::append_process_line(&run_dir, "Completed intake with exit code 0.")
            .expect("append completion line");
        runtime::finish_job(&run_dir, "completed", Some(0), None)
            .expect("finish stale start-next job");

        let doctor = doctor_report(&ctx, &run_dir).expect("doctor report");
        assert_eq!(doctor.health, "healthy");
        assert_eq!(doctor.next, "solver-a");
        assert!(doctor.last_attempt.is_none());

        let status = status_report(&ctx, &run_dir).expect("status report");
        assert_eq!(status.next, "solver-a");
        assert!(status.last_attempt.is_none());

        let _ = fs::remove_dir_all(output_root);
        let _ = fs::remove_dir_all(workspace);
    }

    #[test]
    fn doctor_warns_when_solver_mcp_assignment_did_not_reach_stage_artifacts() {
        let ctx = test_context();
        let workspace = temp_dir("doctor-mcp-mismatch-workspace");
        let output_root = temp_dir("doctor-mcp-mismatch-output");
        let run_dir = create_run(
            &ctx,
            "Исследовать код проекта, сравнить варианты и дать рекомендацию по изменениям.",
            &workspace,
            &output_root,
            Some("doctor-mcp-mismatch"),
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

        write_text(&run_dir.join("brief.md"), "# Brief\n\nReady.\n").expect("brief");
        write_text(
            &run_dir.join("solutions").join("solver-a").join("RESULT.md"),
            "# Result\n\nOption A.\n",
        )
        .expect("solver result");
        write_text(
            &run_dir.join("logs").join("solver-a.prompt.md"),
            "{\"mcp_servers\": [], \"mcp_usage_hints\": []}\n",
        )
        .expect("solver prompt log");
        write_json(
            &run_dir.join("runtime").join("mcp-provision.json"),
            &McpProvisionRecord {
                enabled: true,
                configured: vec!["fetch".to_string(), "memory".to_string()],
                ..McpProvisionRecord::default()
            },
        )
        .expect("mcp provision");
        persist_stage_mcp_note(
            &run_dir,
            "solver-a",
            "No MCP servers were selected for this stage.\n\nNo MCP tools were used in this stage.",
        )
        .expect("persist mcp note");
        record_stage_mcp_usage(&plan, &run_dir, "solver-a", "codex", "executed")
            .expect("record mcp usage");

        let doctor = doctor_report(&ctx, &run_dir).expect("doctor report");
        assert_eq!(doctor.health, "warning");
        assert!(doctor.warnings.iter().any(|issue| issue
            .message
            .contains("Selected MCP servers did not propagate cleanly into stage artifacts")));
    }

    #[test]
    fn doctor_warns_when_verification_mcp_assignment_did_not_reach_stage_artifacts() {
        let ctx = test_context();
        let workspace = temp_dir("doctor-verification-mcp-mismatch-workspace");
        let output_root = temp_dir("doctor-verification-mcp-mismatch-output");
        let run_dir = create_run(
            &ctx,
            "Исследовать код проекта, сравнить варианты и дать рекомендацию по изменениям.",
            &workspace,
            &output_root,
            Some("doctor-verification-mcp-mismatch"),
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

        write_text(
            &run_dir.join("verification").join("findings.md"),
            "# Findings\n\nNeed more verification.\n",
        )
        .expect("verification findings");
        write_text(
            &run_dir.join("logs").join("verification.prompt.md"),
            "{\"mcp_servers\": [], \"mcp_usage_hints\": []}\n",
        )
        .expect("verification prompt log");
        write_json(
            &run_dir.join("runtime").join("mcp-provision.json"),
            &McpProvisionRecord {
                enabled: true,
                configured: vec!["fetch".to_string(), "memory".to_string()],
                ..McpProvisionRecord::default()
            },
        )
        .expect("mcp provision");
        persist_stage_mcp_note(
            &run_dir,
            "verification",
            "No MCP servers were selected for this stage.\n\nNo MCP tools were used in this stage.",
        )
        .expect("persist verification mcp note");
        record_stage_mcp_usage(&plan, &run_dir, "verification", "codex", "executed")
            .expect("record verification mcp usage");

        let doctor = doctor_report(&ctx, &run_dir).expect("doctor report");
        assert_eq!(doctor.health, "warning");
        assert!(doctor.warnings.iter().any(|issue| {
            issue
                .message
                .contains("Selected MCP servers did not propagate cleanly into stage artifacts")
                && issue.message.contains("verification")
        }));
    }

    #[test]
    fn doctor_warns_when_stage_artifacts_show_unexpected_solver_mcp_assignment() {
        let ctx = test_context();
        let workspace = temp_dir("doctor-unexpected-solver-mcp-workspace");
        let output_root = temp_dir("doctor-unexpected-solver-mcp-output");
        let run_dir = create_run(
            &ctx,
            "Проведи аудит и подготовь улучшения по деплою сервиса и секретам.",
            &workspace,
            &output_root,
            Some("doctor-unexpected-solver-mcp"),
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
        plan.mcp = McpSelection {
            auto_select: true,
            rationale: vec!["test".to_string()],
            servers: vec![McpServerPlan {
                name: "memory".to_string(),
                mode: "memory".to_string(),
                stages: vec![
                    "intake".to_string(),
                    "review".to_string(),
                    "execution".to_string(),
                    "verification".to_string(),
                ],
                purposes: vec!["handoff".to_string()],
                usage_hint: "Use memory only outside solver stage.".to_string(),
            }],
        };
        for solver in &mut plan.solver_roles {
            solver.mcp_servers.clear();
        }
        save_plan(&run_dir, &plan).expect("save plan");

        write_text(
            &run_dir.join("solutions").join("solver-a").join("RESULT.md"),
            "# Result\n\nOption A.\n",
        )
        .expect("solver result");
        write_text(
            &run_dir.join("logs").join("solver-a.prompt.md"),
            "{\"mcp_servers\": [\"memory\"], \"mcp_usage_hints\": [\"use `memory`\"]}\n",
        )
        .expect("solver prompt log");

        let doctor = doctor_report(&ctx, &run_dir).expect("doctor report");
        assert_eq!(doctor.health, "warning");
        assert!(doctor.warnings.iter().any(|issue| issue
            .message
            .contains("Stage artifacts show unexpected MCP assignments")));
    }

    #[test]
    fn record_stage_mcp_usage_filters_note_entries_for_unselected_servers() {
        let ctx = test_context();
        let workspace = temp_dir("mcp-note-filter-workspace");
        let output_root = temp_dir("mcp-note-filter-output");
        let task = "# Verified Follow-Up Task\n\nUse only the verification-derived context below as the authoritative seed for this rerun.\n\n## Goal Status\n\n- rerun recommended: `true`\n\n## Follow-Up Task\n\nПроведи audit и analysis, preserve verified progress и refresh stale artifacts без реализации.\n";
        let run_dir = create_run(
            &ctx,
            task,
            &workspace,
            &output_root,
            Some("mcp-note-filter"),
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

        write_text(
            &run_dir.join("verification").join("findings.md"),
            "# Findings\n\n## MCP Usage\n\n- `fetch`: not used.\n- `memory`: unavailable.\n",
        )
        .expect("write verification findings");
        write_json(
            &run_dir.join("runtime").join("mcp-provision.json"),
            &McpProvisionRecord {
                enabled: true,
                configured: vec!["memory".to_string()],
                ..McpProvisionRecord::default()
            },
        )
        .expect("write mcp provision");
        persist_stage_mcp_note(
            &run_dir,
            "verification",
            "- `fetch`: not used.\n- `memory`: unavailable.\n",
        )
        .expect("write note");

        record_stage_mcp_usage(&plan, &run_dir, "verification", "codex", "executed")
            .expect("record usage");

        let note = read_text(&run_dir.join("runtime").join("mcp").join("verification.md"))
            .expect("read normalized note");
        assert!(!note.contains("`fetch`"));
        assert!(note.contains("`memory`"));

        let records = read_mcp_usage_records(&run_dir);
        let verification = records
            .iter()
            .find(|item| item.stage == "verification")
            .expect("verification record");
        assert_eq!(verification.selected, vec!["memory".to_string()]);
        assert_eq!(verification.declared_not_used, vec!["memory".to_string()]);
    }

    #[test]
    fn record_stage_mcp_usage_filters_table_entries_for_unselected_servers() {
        let ctx = test_context();
        let workspace = temp_dir("mcp-table-filter-workspace");
        let output_root = temp_dir("mcp-table-filter-output");
        let task = "# Verified Follow-Up Task\n\nUse only the verification-derived context below as the authoritative seed for this rerun.\n\n## Goal Status\n\n- rerun recommended: `true`\n\n## Follow-Up Task\n\nПроведи audit и analysis, preserve verified progress и refresh stale artifacts без реализации.\n";
        let run_dir = create_run(
            &ctx,
            task,
            &workspace,
            &output_root,
            Some("mcp-table-filter"),
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

        let note = "| MCP server | Status | Reason |\n| --- | --- | --- |\n| `exa` | `not used` | left over from older note |\n| `fetch` | `not used` | left over from older note |\n| `memory` | `unavailable` | selected for this stage |\n";
        write_text(
            &run_dir.join("verification").join("findings.md"),
            &format!("# Findings\n\n## MCP Usage\n\n{note}"),
        )
        .expect("write findings");
        write_json(
            &run_dir.join("runtime").join("mcp-provision.json"),
            &McpProvisionRecord {
                enabled: true,
                configured: vec!["memory".to_string()],
                ..McpProvisionRecord::default()
            },
        )
        .expect("write mcp provision");
        persist_stage_mcp_note(&run_dir, "verification", note).expect("write note");

        record_stage_mcp_usage(&plan, &run_dir, "verification", "codex", "executed")
            .expect("record usage");

        let normalized = read_text(&run_dir.join("runtime").join("mcp").join("verification.md"))
            .expect("read normalized note");
        assert!(!normalized.contains("`exa`"));
        assert!(!normalized.contains("`fetch`"));
        assert!(normalized.contains("`memory`"));
        assert!(normalized.contains("| MCP server | Status | Reason |"));
        assert!(normalized.contains("| --- | --- | --- |"));

        let _ = fs::remove_dir_all(run_dir);
        let _ = fs::remove_dir_all(output_root);
        let _ = fs::remove_dir_all(workspace);
    }

    #[test]
    fn empty_mcp_selection_forbids_live_recheck_claims() {
        let rules = render_mcp_accountability_rules(&[], "ru");
        assert!(rules
            .iter()
            .any(|rule: &String| rule.contains("Использование MCP")));
        assert!(rules
            .iter()
            .any(|rule: &String| rule.contains("do not claim fresh MCP-backed lookups")));
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
    fn load_plan_falls_back_to_runtime_snapshot_when_plan_json_is_missing() {
        let run_dir = temp_run_dir("plan-snapshot-fallback");
        let snapshot_path = crate::runtime::runtime_dir(&run_dir).join("plan.snapshot.json");
        write_json(
            &snapshot_path,
            &json!({
                "workspace": run_dir.display().to_string(),
                "workspace_exists": true,
                "task_kind": "backend",
                "prompt_format": "compact",
                "summary_language": "ru",
                "intake_research_mode": "research-first",
                "stage_research_mode": "local-first",
                "execution_network_mode": "fetch-if-needed",
                "goal_gate_enabled": true,
                "augmented_follow_up_enabled": true
            }),
        )
        .expect("write snapshot plan");

        assert!(run_has_plan_artifact(&run_dir));
        let plan = load_plan(&run_dir).expect("load snapshot plan");
        assert_eq!(plan.task_kind, "backend");
        assert_eq!(plan.prompt_format, "compact");

        let _ = fs::remove_dir_all(run_dir);
    }

    #[test]
    fn save_plan_writes_runtime_snapshot_copy() {
        let run_dir = temp_run_dir("plan-snapshot-save");
        let plan = Plan {
            workspace: run_dir.display().to_string(),
            workspace_exists: true,
            task_kind: "backend".to_string(),
            prompt_format: "compact".to_string(),
            summary_language: "ru".to_string(),
            intake_research_mode: "research-first".to_string(),
            stage_research_mode: "local-first".to_string(),
            execution_network_mode: "fetch-if-needed".to_string(),
            goal_gate_enabled: true,
            augmented_follow_up_enabled: true,
            ..Plan::default()
        };

        save_plan(&run_dir, &plan).expect("save plan");

        assert!(run_dir.join("plan.json").exists());
        assert!(crate::runtime::runtime_dir(&run_dir)
            .join("plan.snapshot.json")
            .exists());

        let _ = fs::remove_dir_all(run_dir);
    }

    #[test]
    fn default_pipeline_stage_specs_seed_descriptions() {
        let plan = Plan {
            task_kind: "backend".to_string(),
            original_task:
                "Create a minimal Python CLI in main.py that prints an exact stdout string and add a README."
                    .to_string(),
            solver_count: 2,
            solver_roles: choose_roles(
                "backend",
                "Create a minimal Python CLI in main.py that prints an exact stdout string and add a README.",
                2,
            ),
            workstream_hints: workstream_hints_for(
                "backend",
                "Create a minimal Python CLI in main.py that prints an exact stdout string and add a README.",
            ),
            ..Plan::default()
        };

        let specs = default_pipeline_stage_specs(&plan, None);

        assert!(specs
            .iter()
            .all(|stage| !stage.description.trim().is_empty()));
        assert!(specs
            .iter()
            .any(|stage| stage.id == "solver-a" && stage.description.contains("local-entrypoint")));
        assert!(specs
            .iter()
            .any(|stage| stage.id == "solver-b" && stage.description.contains("run-contract")));
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
        fs::create_dir_all(run_dir.join("host")).expect("create host dir");
        write_json(
            &run_dir.join("host").join("probe.json"),
            &json!({
                "captured_at": "2026-03-31T12:27:36",
                "preferred_torch_device": "mps",
                "mps_built": true,
                "mps_available": true
            }),
        )
        .expect("write host probe");

        let result = create_follow_up_run(
            &ctx,
            &run_dir,
            &RerunArgs {
                output_dir: Some(output_root.clone()),
                note: Some(
                    "Учти мои мысли: не расширяй scope и отдельно проверь, не осталось ли скрытых противоречий в текущих фактах."
                        .to_string(),
                ),
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
        assert!(new_run
            .join("prompts")
            .join("level5-verification.md")
            .exists());

        let plan = load_plan(&new_run).expect("load follow-up plan");
        assert_eq!(plan.prompt_format, "compact");
        assert_eq!(plan.summary_language, "ru");
        assert!(run_dir
            .join("verification")
            .join("current-facts.md")
            .exists());
        let request = read_text(&new_run.join("request.md")).expect("read follow-up request");
        assert!(request.contains(
            "Используй только контекст из проверки ниже как авторитетную основу для этого повторного прогона."
        ));
        assert!(request.contains("## Актуальные факты"));
        assert!(request.contains("## Правила повторного прогона"));
        assert!(request.contains("## Цель текущего повторного прогона"));
        assert!(request.contains("## Дополнительные указания пользователя"));
        assert!(request.contains("## Исторические указания из исходного прогона"));
        assert!(request.contains("ссылки на артефакты исходного прогона"));
        assert!(request.contains("а не переписывай предыдущие run-local файлы"));
        assert!(request.contains("Учти мои мысли: не расширяй scope"));
        assert!(request.contains("2026-03-31T12:27:36"));
        assert!(request.contains("Rerun needs a compatibility follow-up."));
        assert!(request.contains(
            "Repair rerun compatibility for legacy workstream hints and validate the follow-up path."
        ));
        assert!(request.contains("незакрытые критические проверки:"));

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
        if first_stage_id_for_kind(&plan, &run_dir, PipelineStageKind::Execution)
            .expect("execution stage lookup")
            .is_some()
        {
            assert!(read_text(&run_dir.join("execution").join("report.md"))
                .expect("read execution report")
                .contains("test service successfully"));
        } else {
            assert!(
                !run_dir.join("execution").join("report.md").exists(),
                "analysis-only mock run should not materialize an execution report"
            );
        }
        assert!(read_text(&run_dir.join("verification").join("findings.md"))
            .expect("read findings")
            .contains("Mock verification complete"));

        let expected_calls = 2usize + plan.pipeline.stages.len();
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
