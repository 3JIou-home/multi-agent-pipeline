use serde::{Deserialize, Serialize};
use std::fs::{self, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const JOB_FILE: &str = "job.json";
const PROCESS_LOG_FILE: &str = "process.log";
const CANCEL_FILE: &str = "cancel.requested";
const DEAD_PROCESS_TRANSITION_GRACE_SECS: u64 = 5;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeJobState {
    pub version: u8,
    pub label: String,
    pub stage: Option<String>,
    pub command_hint: String,
    pub status: String,
    pub pid: i32,
    pub pgid: i32,
    pub started_at_unix: u64,
    pub updated_at_unix: u64,
    pub process_log: String,
    pub exit_code: Option<i32>,
    pub message: Option<String>,
}

impl RuntimeJobState {
    pub fn is_active(&self) -> bool {
        matches!(self.status.as_str(), "starting" | "running" | "stalled")
    }
}

pub fn runtime_dir(run_dir: &Path) -> PathBuf {
    run_dir.join("runtime")
}

pub fn process_log_path(run_dir: &Path) -> PathBuf {
    runtime_dir(run_dir).join(PROCESS_LOG_FILE)
}

pub fn job_state_path(run_dir: &Path) -> PathBuf {
    runtime_dir(run_dir).join(JOB_FILE)
}

pub fn cancel_request_path(run_dir: &Path) -> PathBuf {
    runtime_dir(run_dir).join(CANCEL_FILE)
}

pub fn load_job_state(run_dir: &Path) -> Option<RuntimeJobState> {
    let path = job_state_path(run_dir);
    let content = fs::read_to_string(path).ok()?;
    serde_json::from_str(&content).ok()
}

pub fn active_job_state(run_dir: &Path) -> Option<RuntimeJobState> {
    let state = load_job_state(run_dir)?;
    if !state.is_active() {
        return None;
    }
    if state.pid <= 0 && state.pgid <= 0 {
        if state.exit_code.is_some() {
            return None;
        }
        let age = unix_now().saturating_sub(state.updated_at_unix);
        if age <= 15 {
            return Some(state);
        }
        let _ = finish_job(
            run_dir,
            "exited",
            None,
            Some("Tracked job did not publish a live process in time."),
        );
        return None;
    }
    if process_group_alive(state.pgid) || pid_alive(state.pid) {
        if state.exit_code.is_some() || state.message.is_some() {
            let mut normalized = state.clone();
            normalized.exit_code = None;
            normalized.message = None;
            normalized.updated_at_unix = unix_now();
            let _ = write_job_state(run_dir, &normalized);
            return Some(normalized);
        }
        return Some(state);
    }
    if state.exit_code.is_some() {
        return None;
    }
    let age = unix_now().saturating_sub(state.updated_at_unix);
    if age <= DEAD_PROCESS_TRANSITION_GRACE_SECS {
        return Some(state);
    }
    let _ = finish_job(
        run_dir,
        "exited",
        None,
        Some("Tracked process is no longer alive. Inspect run artifacts and logs."),
    );
    None
}

pub fn start_job(
    run_dir: &Path,
    label: &str,
    stage: Option<&str>,
    command_hint: &str,
    pid: i32,
    pgid: i32,
) -> Result<RuntimeJobState, String> {
    fs::create_dir_all(runtime_dir(run_dir)).map_err(|err| {
        format!(
            "Could not create runtime dir for {}: {err}",
            run_dir.display()
        )
    })?;
    fs::write(process_log_path(run_dir), "").map_err(|err| {
        format!(
            "Could not initialize process log for {}: {err}",
            run_dir.display()
        )
    })?;
    clear_interrupt_request(run_dir)?;
    let now = unix_now();
    let state = RuntimeJobState {
        version: 1,
        label: label.to_string(),
        stage: stage.map(|value| value.to_string()),
        command_hint: command_hint.to_string(),
        status: "running".to_string(),
        pid,
        pgid,
        started_at_unix: now,
        updated_at_unix: now,
        process_log: PROCESS_LOG_FILE.to_string(),
        exit_code: None,
        message: None,
    };
    write_job_state(run_dir, &state)?;
    Ok(state)
}

#[allow(dead_code)]
pub fn start_pending_job(
    run_dir: &Path,
    label: &str,
    stage: Option<&str>,
    command_hint: &str,
) -> Result<RuntimeJobState, String> {
    fs::create_dir_all(runtime_dir(run_dir)).map_err(|err| {
        format!(
            "Could not create runtime dir for {}: {err}",
            run_dir.display()
        )
    })?;
    fs::write(process_log_path(run_dir), "").map_err(|err| {
        format!(
            "Could not initialize process log for {}: {err}",
            run_dir.display()
        )
    })?;
    clear_interrupt_request(run_dir)?;
    let now = unix_now();
    let state = RuntimeJobState {
        version: 1,
        label: label.to_string(),
        stage: stage.map(|value| value.to_string()),
        command_hint: command_hint.to_string(),
        status: "starting".to_string(),
        pid: 0,
        pgid: 0,
        started_at_unix: now,
        updated_at_unix: now,
        process_log: PROCESS_LOG_FILE.to_string(),
        exit_code: None,
        message: None,
    };
    write_job_state(run_dir, &state)?;
    Ok(state)
}

#[allow(dead_code)]
pub fn update_job_process(
    run_dir: &Path,
    pid: i32,
    pgid: i32,
    status: Option<&str>,
) -> Result<(), String> {
    let Some(mut state) = load_job_state(run_dir) else {
        return Ok(());
    };
    state.pid = pid;
    state.pgid = pgid;
    if let Some(status) = status {
        state.status = status.to_string();
    }
    state.exit_code = None;
    state.message = None;
    state.updated_at_unix = unix_now();
    write_job_state(run_dir, &state)
}

pub fn update_job_stage(
    run_dir: &Path,
    stage: Option<&str>,
    command_hint: Option<&str>,
) -> Result<(), String> {
    let Some(mut state) = load_job_state(run_dir) else {
        return Ok(());
    };
    state.stage = stage.map(|value| value.to_string());
    if let Some(command_hint) = command_hint {
        state.command_hint = command_hint.to_string();
    }
    state.updated_at_unix = unix_now();
    write_job_state(run_dir, &state)
}

pub fn touch_job(run_dir: &Path, status: &str) -> Result<(), String> {
    let Some(mut state) = load_job_state(run_dir) else {
        return Ok(());
    };
    state.status = status.to_string();
    if matches!(status, "starting" | "running" | "stalled") {
        state.exit_code = None;
        state.message = None;
    }
    state.updated_at_unix = unix_now();
    write_job_state(run_dir, &state)
}

pub fn finish_job(
    run_dir: &Path,
    status: &str,
    exit_code: Option<i32>,
    message: Option<&str>,
) -> Result<(), String> {
    let Some(mut state) = load_job_state(run_dir) else {
        return Ok(());
    };
    state.status = status.to_string();
    state.updated_at_unix = unix_now();
    state.exit_code = exit_code;
    state.message = message.map(|value| value.to_string());
    write_job_state(run_dir, &state)?;
    clear_interrupt_request(run_dir)?;
    Ok(())
}

pub fn request_interrupt(run_dir: &Path) -> Result<(), String> {
    fs::create_dir_all(runtime_dir(run_dir)).map_err(|err| {
        format!(
            "Could not create runtime dir for {}: {err}",
            run_dir.display()
        )
    })?;
    fs::write(cancel_request_path(run_dir), b"interrupt\n").map_err(|err| {
        format!(
            "Could not write interrupt request for {}: {err}",
            run_dir.display()
        )
    })
}

pub fn clear_interrupt_request(run_dir: &Path) -> Result<(), String> {
    let path = cancel_request_path(run_dir);
    if path.exists() {
        fs::remove_file(&path).map_err(|err| {
            format!(
                "Could not clear interrupt request for {}: {err}",
                run_dir.display()
            )
        })?;
    }
    Ok(())
}

pub fn interrupt_requested(run_dir: &Path) -> bool {
    cancel_request_path(run_dir).exists()
}

pub fn append_process_line(run_dir: &Path, line: &str) -> Result<(), String> {
    fs::create_dir_all(runtime_dir(run_dir)).map_err(|err| {
        format!(
            "Could not create runtime dir for {}: {err}",
            run_dir.display()
        )
    })?;
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(process_log_path(run_dir))
        .map_err(|err| {
            format!(
                "Could not open process log for {}: {err}",
                run_dir.display()
            )
        })?;
    writeln!(file, "{line}").map_err(|err| {
        format!(
            "Could not append to process log for {}: {err}",
            run_dir.display()
        )
    })
}

pub fn tail_process_log(run_dir: &Path, line_limit: usize) -> Vec<String> {
    let path = process_log_path(run_dir);
    let Ok(file) = fs::File::open(path) else {
        return Vec::new();
    };
    let mut tail = std::collections::VecDeque::with_capacity(line_limit.max(1));
    for line in BufReader::new(file).lines() {
        let Ok(line) = line else {
            continue;
        };
        if tail.len() == line_limit {
            tail.pop_front();
        }
        tail.push_back(line);
    }
    tail.into_iter().collect()
}

pub fn interrupt_process_group(pgid: i32) -> Result<(), String> {
    if pgid <= 0 {
        return Err(format!("Invalid process group id: {pgid}"));
    }
    if !process_group_alive(pgid) {
        return Ok(());
    }
    if let Err(err) = signal_process_group(pgid, libc::SIGTERM) {
        if !permission_denied(&err) || signal_process(pgid, libc::SIGTERM).is_err() {
            return Err(err);
        }
    }
    for _ in 0..10 {
        if !process_group_alive(pgid) {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(100));
    }
    if let Err(err) = signal_process_group(pgid, libc::SIGKILL) {
        if !permission_denied(&err) || signal_process(pgid, libc::SIGKILL).is_err() {
            return Err(err);
        }
    }
    Ok(())
}

pub fn pid_alive(pid: i32) -> bool {
    if pid <= 0 {
        return false;
    }
    signal_alive(pid)
}

pub fn process_group_alive(pgid: i32) -> bool {
    if pgid <= 0 {
        return false;
    }
    signal_alive(-pgid)
}

pub fn system_time_from_unix(unix_secs: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(unix_secs)
}

pub fn elapsed_from_unix(unix_secs: u64) -> Duration {
    Duration::from_secs(unix_now().saturating_sub(unix_secs))
}

fn write_job_state(run_dir: &Path, state: &RuntimeJobState) -> Result<(), String> {
    let path = job_state_path(run_dir);
    let tmp = path.with_extension("tmp");
    let payload = serde_json::to_vec_pretty(state).map_err(|err| {
        format!(
            "Could not serialize runtime state for {}: {err}",
            run_dir.display()
        )
    })?;
    fs::write(&tmp, payload).map_err(|err| {
        format!(
            "Could not write temp runtime state for {}: {err}",
            run_dir.display()
        )
    })?;
    fs::rename(&tmp, &path).map_err(|err| {
        format!(
            "Could not move runtime state into place for {}: {err}",
            run_dir.display()
        )
    })
}

fn signal_process_group(pgid: i32, signal: i32) -> Result<(), String> {
    let rc = unsafe { libc::kill(-pgid, signal) };
    if rc == 0 {
        return Ok(());
    }
    let err = io::Error::last_os_error();
    let code = err.raw_os_error().unwrap_or_default();
    if code == libc::ESRCH {
        return Ok(());
    }
    Err(format!("Could not signal process group {pgid}: {err}"))
}

fn signal_process(pid: i32, signal: i32) -> Result<(), String> {
    let rc = unsafe { libc::kill(pid, signal) };
    if rc == 0 {
        return Ok(());
    }
    let err = io::Error::last_os_error();
    let code = err.raw_os_error().unwrap_or_default();
    if code == libc::ESRCH {
        return Ok(());
    }
    Err(format!("Could not signal process {pid}: {err}"))
}

fn signal_alive(target: i32) -> bool {
    let rc = unsafe { libc::kill(target, 0) };
    if rc == 0 {
        return true;
    }
    matches!(
        io::Error::last_os_error().raw_os_error(),
        Some(code) if code == libc::EPERM
    )
}

fn permission_denied(message: &str) -> bool {
    message.contains("Operation not permitted")
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::{
        active_job_state, cancel_request_path, clear_interrupt_request, finish_job,
        interrupt_requested, load_job_state, process_log_path, request_interrupt, start_job,
        tail_process_log, touch_job, update_job_process, update_job_stage, write_job_state,
        DEAD_PROCESS_TRANSITION_GRACE_SECS,
    };
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_run_dir(name: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("agpipe-runtime-{name}-{unique}"));
        fs::create_dir_all(&path).expect("create temp run dir");
        path
    }

    fn age_job_state(run_dir: &PathBuf, seconds: u64) {
        let mut state = load_job_state(run_dir).expect("load job state");
        state.updated_at_unix = state.updated_at_unix.saturating_sub(seconds);
        write_job_state(run_dir, &state).expect("write aged job state");
    }

    #[test]
    fn runtime_log_tail_reads_latest_lines() {
        let run_dir = temp_run_dir("tail");
        start_job(
            &run_dir,
            "start-next",
            Some("review"),
            "start-next",
            std::process::id() as i32,
            std::process::id() as i32,
        )
        .expect("start job");
        fs::write(process_log_path(&run_dir), "line 1\nline 2\nline 3\n").expect("write log");

        let lines = tail_process_log(&run_dir, 2);

        assert_eq!(lines, vec!["line 2", "line 3"]);
        let _ = fs::remove_dir_all(run_dir);
    }

    #[test]
    fn inactive_pid_is_not_reported_as_active() {
        let run_dir = temp_run_dir("inactive");
        start_job(
            &run_dir,
            "start-next",
            Some("review"),
            "start-next",
            999_999,
            999_999,
        )
        .expect("start job");
        age_job_state(&run_dir, DEAD_PROCESS_TRANSITION_GRACE_SECS + 1);

        assert!(active_job_state(&run_dir).is_none());

        finish_job(&run_dir, "completed", Some(0), Some("done")).expect("finish job");
        let _ = fs::remove_dir_all(run_dir);
    }

    #[test]
    fn recently_dead_pid_stays_active_during_transition_grace_window() {
        let run_dir = temp_run_dir("transition-grace");
        start_job(
            &run_dir,
            "resume",
            Some("review"),
            "resume until verification",
            999_999,
            999_999,
        )
        .expect("start job");

        let state = active_job_state(&run_dir).expect("state should stay active during grace");
        assert_eq!(state.status, "running");
        assert_eq!(state.stage.as_deref(), Some("review"));

        age_job_state(&run_dir, DEAD_PROCESS_TRANSITION_GRACE_SECS + 1);
        assert!(active_job_state(&run_dir).is_none());

        let state = load_job_state(&run_dir).expect("load exited state");
        assert_eq!(state.status, "exited");
        assert!(state
            .message
            .as_deref()
            .unwrap_or_default()
            .contains("Tracked process is no longer alive"));

        let _ = fs::remove_dir_all(run_dir);
    }

    #[test]
    fn interrupt_request_round_trip_works() {
        let run_dir = temp_run_dir("interrupt");
        request_interrupt(&run_dir).expect("request interrupt");
        assert!(interrupt_requested(&run_dir));
        assert!(cancel_request_path(&run_dir).exists());

        clear_interrupt_request(&run_dir).expect("clear interrupt");
        assert!(!interrupt_requested(&run_dir));

        let _ = fs::remove_dir_all(run_dir);
    }

    #[test]
    fn exit_code_marks_running_status_as_inactive() {
        let run_dir = temp_run_dir("terminal-exit-code");
        start_job(&run_dir, "rerun", None, "rerun", 0, 0).expect("start job");
        finish_job(&run_dir, "completed", Some(0), Some("done")).expect("finish job");
        touch_job(&run_dir, "running").expect("simulate stale heartbeat");

        age_job_state(&run_dir, 16);
        assert!(active_job_state(&run_dir).is_none());

        let _ = fs::remove_dir_all(run_dir);
    }

    #[test]
    fn update_job_stage_rewrites_stage_for_active_job() {
        let run_dir = temp_run_dir("update-stage");
        start_job(
            &run_dir,
            "resume",
            Some("intake"),
            "resume until verification",
            std::process::id() as i32,
            std::process::id() as i32,
        )
        .expect("start job");

        update_job_stage(&run_dir, Some("solver-a"), None).expect("update job stage");
        let state = load_job_state(&run_dir).expect("load job state");

        assert_eq!(state.stage.as_deref(), Some("solver-a"));
        assert_eq!(state.command_hint, "resume until verification");

        finish_job(&run_dir, "completed", Some(0), Some("done")).expect("finish job");
        let _ = fs::remove_dir_all(run_dir);
    }

    #[test]
    fn update_job_process_clears_stale_terminal_fields_for_live_job() {
        let run_dir = temp_run_dir("update-process-clears-terminal-fields");
        start_job(
            &run_dir,
            "resume",
            Some("execution"),
            "resume until verification",
            0,
            0,
        )
        .expect("start job");

        let mut state = load_job_state(&run_dir).expect("load job state");
        state.exit_code = Some(1);
        state.message =
            Some("Tracked process is no longer alive. Inspect run artifacts and logs.".to_string());
        write_job_state(&run_dir, &state).expect("persist stale state");

        update_job_process(
            &run_dir,
            std::process::id() as i32,
            std::process::id() as i32,
            Some("running"),
        )
        .expect("update job process");
        let state = load_job_state(&run_dir).expect("reload job state");

        assert_eq!(state.status, "running");
        assert_eq!(state.exit_code, None);
        assert_eq!(state.message, None);

        let _ = fs::remove_dir_all(run_dir);
    }

    #[test]
    fn active_job_state_normalizes_stale_terminal_message_for_alive_process() {
        let run_dir = temp_run_dir("active-job-normalizes-terminal-message");
        start_job(
            &run_dir,
            "resume",
            Some("execution"),
            "resume until verification",
            std::process::id() as i32,
            std::process::id() as i32,
        )
        .expect("start job");

        let mut state = load_job_state(&run_dir).expect("load job state");
        state.exit_code = Some(1);
        state.message =
            Some("Tracked process is no longer alive. Inspect run artifacts and logs.".to_string());
        write_job_state(&run_dir, &state).expect("persist stale state");

        let normalized = active_job_state(&run_dir).expect("state should stay active");
        assert_eq!(normalized.status, "running");
        assert_eq!(normalized.exit_code, None);
        assert_eq!(normalized.message, None);

        let persisted = load_job_state(&run_dir).expect("reload persisted state");
        assert_eq!(persisted.exit_code, None);
        assert_eq!(persisted.message, None);

        let _ = fs::remove_dir_all(run_dir);
    }
}
