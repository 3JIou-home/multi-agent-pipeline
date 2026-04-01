mod engine;
mod runtime;
mod tui;

use engine::{
    amend_run, automate_run, choose_prune_candidates, default_run_root, delete_run,
    discover_run_dirs, doctor_report, execute_safe_next_action, run_stage_capture,
    run_stage_stream, runtime_check_run, service_check_run, task_flow_stream, Context,
};
use std::env;
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::process::ExitCode;
use std::collections::HashSet;

fn main() -> ExitCode {
    match real_main() {
        Ok(code) => code,
        Err(message) => {
            eprintln!("{message}");
            ExitCode::from(1)
        }
    }
}

fn real_main() -> Result<ExitCode, String> {
    load_local_env_files()?;
    let args: Vec<String> = env::args().skip(1).collect();
    let ctx = Context::discover()?;

    if args.is_empty() {
        tui::launch(&ctx, &default_run_root(), 30)?;
        return Ok(ExitCode::SUCCESS);
    }

    let command = args[0].clone();
    let rest = &args[1..];

    match command.as_str() {
        "ui" | "tui" => {
            let (root, limit) = parse_root_limit(rest)?;
            tui::launch(&ctx, &root, limit)?;
            Ok(ExitCode::SUCCESS)
        }
        "help" | "--help" | "-h" => {
            print_help();
            Ok(ExitCode::SUCCESS)
        }
        "version" | "--version" | "-V" => {
            println!("agpipe {}", env!("CARGO_PKG_VERSION"));
            Ok(ExitCode::SUCCESS)
        }
        "internal" => command_internal(&ctx, rest),
        "runs" => command_runs(&ctx, rest),
        "doctor" => command_delegate_run_stage_stream(&ctx, "doctor", rest),
        "status" => command_delegate_run_stage_stream(&ctx, "status", rest),
        "next" => command_delegate_run_stage_stream(&ctx, "next", rest),
        "summary" => command_delegate_run_stage_stream(&ctx, "summary", rest),
        "findings" => command_delegate_run_stage_stream(&ctx, "findings", rest),
        "augmented-task" => command_delegate_run_stage_stream(&ctx, "augmented-task", rest),
        "show" => command_delegate_run_stage_stream(&ctx, "show", rest),
        "copy" => command_delegate_run_stage_stream(&ctx, "copy", rest),
        "host-probe" => command_delegate_run_stage_stream(&ctx, "host-probe", rest),
        "runtime-check" | "service-check" => command_runtime_check(&command, rest),
        "start" => command_delegate_run_stage_stream(&ctx, "start", rest),
        "start-next" => command_delegate_run_stage_stream(&ctx, "start-next", rest),
        "start-solvers" => command_delegate_run_stage_stream(&ctx, "start-solvers", rest),
        "refresh-prompts" => command_delegate_run_stage_stream(&ctx, "refresh-prompts", rest),
        "refresh-prompt" => command_delegate_run_stage_stream(&ctx, "refresh-prompt", rest),
        "step-back" => command_delegate_run_stage_stream(&ctx, "step-back", rest),
        "recheck" => command_delegate_run_stage_stream(&ctx, "recheck", rest),
        "cache-status" => command_delegate_run_stage_stream(&ctx, "cache-status", rest),
        "cache-prune" => command_delegate_run_stage_stream(&ctx, "cache-prune", rest),
        "rerun" => command_delegate_run_stage_stream(&ctx, "rerun", rest),
        "interview-questions" => {
            command_task_flow_delegate_stream(&ctx, "interview-questions", rest)
        }
        "interview-finalize" => command_task_flow_delegate_stream(&ctx, "interview-finalize", rest),
        "create-run" => command_task_flow_delegate_stream(&ctx, "create-run", rest),
        "resume" => command_resume(&ctx, rest),
        "safe-next" => command_safe_next(&ctx, rest),
        "amend" => command_amend(&ctx, rest),
        "rm" => command_rm(rest),
        "prune-runs" => command_prune_runs(rest),
        "run" | "interview" => {
            let code = task_flow_stream(&ctx, &command, rest)?;
            Ok(ExitCode::from(code as u8))
        }
        other => Err(format!(
            "Unknown command: {other}\nUse `agpipe` for the TUI or `agpipe internal --help` for advanced commands."
        )),
    }
}

fn print_help() {
    println!(
        "agpipe\n\n\
UI-first multi-agent pipeline runtime.\n\n\
Normal operator path:\n\
  agpipe\n\
  agpipe ui --root ~/agent-runs\n\n\
What the UI handles:\n\
  - interview and final task review when needed\n\
  - run creation under the selected run root\n\
  - stage execution, resume, and safe-next flow\n\
  - doctor state, logs, artifacts, findings, and reruns\n\n\
Advanced / internal CLI:\n\
  agpipe internal --help\n\n\
Notes:\n\
  - low-level direct commands still work for tests, CI, automation, and debugging\n\
  - prefer the TUI for day-to-day use\n\
  - `runtime-check` and `service-check` share the same local runtime harness\n"
    );
}

fn load_local_env_files() -> Result<(), String> {
    let initial_keys: HashSet<String> = env::vars().map(|(key, _)| key).collect();
    if let Ok(path) = env::var("AGPIPE_ENV_FILE") {
        let env_path = PathBuf::from(path);
        if env_path.exists() {
            apply_env_file(&env_path, &initial_keys)?;
        }
        return Ok(());
    }

    let Some(root) = detect_repo_like_root() else {
        return Ok(());
    };
    let shared = root.join(".env");
    let local = root.join(".env.local");
    if shared.exists() {
        apply_env_file(&shared, &initial_keys)?;
    }
    if local.exists() {
        apply_env_file(&local, &initial_keys)?;
    }
    Ok(())
}

fn detect_repo_like_root() -> Option<PathBuf> {
    if let Ok(path) = env::var("AGPIPE_REPO_ROOT") {
        let root = PathBuf::from(path);
        if root.join("Cargo.toml").exists() {
            return Some(root);
        }
    }

    if let Ok(mut dir) = env::current_dir() {
        loop {
            if dir.join("Cargo.toml").exists() {
                return Some(dir);
            }
            if !dir.pop() {
                break;
            }
        }
    }

    if let Ok(exe) = env::current_exe() {
        if let Some(mut dir) = exe.parent().map(PathBuf::from) {
            loop {
                if dir.join("Cargo.toml").exists() {
                    return Some(dir);
                }
                if !dir.pop() {
                    break;
                }
            }
        }
    }

    None
}

fn apply_env_file(path: &PathBuf, initial_keys: &HashSet<String>) -> Result<(), String> {
    let text = fs::read_to_string(path)
        .map_err(|err| format!("Could not read {}: {err}", path.display()))?;
    apply_env_text(&text, initial_keys)
        .map_err(|err| format!("Could not parse {}: {err}", path.display()))
}

fn apply_env_text(text: &str, initial_keys: &HashSet<String>) -> Result<(), String> {
    for (idx, raw_line) in text.lines().enumerate() {
        let line_no = idx + 1;
        let Some((key, value)) = parse_env_assignment_line(raw_line)? else {
            continue;
        };
        if !initial_keys.contains(&key) {
            env::set_var(key, value);
        }
        let _ = line_no;
    }
    Ok(())
}

fn parse_env_assignment_line(raw_line: &str) -> Result<Option<(String, String)>, String> {
    let trimmed = raw_line.trim();
    if trimmed.is_empty() || trimmed.starts_with('#') {
        return Ok(None);
    }

    let assignment = trimmed
        .strip_prefix("export ")
        .map(str::trim)
        .unwrap_or(trimmed);
    let Some((key, value)) = assignment.split_once('=') else {
        return Err(format!("invalid line `{trimmed}`"));
    };
    let key = key.trim();
    if key.is_empty() {
        return Err(format!("missing key in `{trimmed}`"));
    }
    let mut value = value.trim().to_string();
    if (value.starts_with('"') && value.ends_with('"'))
        || (value.starts_with('\'') && value.ends_with('\''))
    {
        if value.len() >= 2 {
            value = value[1..value.len() - 1].to_string();
        }
    }
    Ok(Some((key.to_string(), value)))
}

fn print_internal_help() {
    println!(
        "agpipe internal\n\n\
Low-level automation and debugging commands.\n\
Not the normal operator path; use the TUI unless you are scripting or debugging.\n\n\
Examples:\n\
  agpipe internal run --task '...' --workspace /path/to/workspace --output-dir ~/agent-runs --until verification\n\
  agpipe internal doctor ~/agent-runs/<run-id>\n\
  agpipe internal resume ~/agent-runs/<run-id> --until verification\n\
  agpipe internal runtime-check ~/agent-runs/<run-id> --phase verification\n\
  agpipe internal create-run --task '...' --workspace /path/to/workspace --output-dir /path/to/agent-runs\n\
  agpipe internal interview-questions --task '...' --workspace /path/to/workspace\n\
  agpipe internal interview-finalize --task '...' --workspace /path/to/workspace --session-dir /path/to/session --answers-file /path/to/answers.json\n"
    );
}

fn command_internal(ctx: &Context, args: &[String]) -> Result<ExitCode, String> {
    if args.is_empty() {
        print_internal_help();
        return Ok(ExitCode::SUCCESS);
    }
    let command = args[0].as_str();
    let rest = &args[1..];
    match command {
        "help" | "--help" | "-h" => {
            print_internal_help();
            Ok(ExitCode::SUCCESS)
        }
        "ui" | "tui" => {
            let (root, limit) = parse_root_limit(rest)?;
            tui::launch(ctx, &root, limit)?;
            Ok(ExitCode::SUCCESS)
        }
        "runs" => command_runs(ctx, rest),
        "doctor" => command_delegate_run_stage_stream(ctx, "doctor", rest),
        "status" => command_delegate_run_stage_stream(ctx, "status", rest),
        "next" => command_delegate_run_stage_stream(ctx, "next", rest),
        "summary" => command_delegate_run_stage_stream(ctx, "summary", rest),
        "findings" => command_delegate_run_stage_stream(ctx, "findings", rest),
        "augmented-task" => command_delegate_run_stage_stream(ctx, "augmented-task", rest),
        "show" => command_delegate_run_stage_stream(ctx, "show", rest),
        "copy" => command_delegate_run_stage_stream(ctx, "copy", rest),
        "host-probe" => command_delegate_run_stage_stream(ctx, "host-probe", rest),
        "runtime-check" | "service-check" => command_runtime_check(command, rest),
        "start" => command_delegate_run_stage_stream(ctx, "start", rest),
        "start-next" => command_delegate_run_stage_stream(ctx, "start-next", rest),
        "start-solvers" => command_delegate_run_stage_stream(ctx, "start-solvers", rest),
        "refresh-prompts" => command_delegate_run_stage_stream(ctx, "refresh-prompts", rest),
        "refresh-prompt" => command_delegate_run_stage_stream(ctx, "refresh-prompt", rest),
        "step-back" => command_delegate_run_stage_stream(ctx, "step-back", rest),
        "recheck" => command_delegate_run_stage_stream(ctx, "recheck", rest),
        "cache-status" => command_delegate_run_stage_stream(ctx, "cache-status", rest),
        "cache-prune" => command_delegate_run_stage_stream(ctx, "cache-prune", rest),
        "rerun" => command_delegate_run_stage_stream(ctx, "rerun", rest),
        "interview-questions" => {
            command_task_flow_delegate_stream(ctx, "interview-questions", rest)
        }
        "interview-finalize" => command_task_flow_delegate_stream(ctx, "interview-finalize", rest),
        "create-run" => command_task_flow_delegate_stream(ctx, "create-run", rest),
        "resume" => command_resume(ctx, rest),
        "safe-next" => command_safe_next(ctx, rest),
        "amend" => command_amend(ctx, rest),
        "rm" => command_rm(rest),
        "prune-runs" => command_prune_runs(rest),
        "run" | "interview" => {
            let code = task_flow_stream(ctx, command, rest)?;
            Ok(ExitCode::from(code as u8))
        }
        other => Err(format!("Unknown internal command: {other}")),
    }
}

#[cfg(test)]
mod tests {
    use super::{apply_env_text, parse_env_assignment_line};
    use std::collections::HashSet;
    use std::env;

    #[test]
    fn parse_env_assignment_line_skips_comments_and_blank_lines() {
        assert!(parse_env_assignment_line("").expect("blank").is_none());
        assert!(parse_env_assignment_line("   ").expect("spaces").is_none());
        assert!(parse_env_assignment_line("# comment").expect("comment").is_none());
    }

    #[test]
    fn parse_env_assignment_line_supports_export_and_quotes() {
        let parsed = parse_env_assignment_line("export OPENAI_API_KEY=\"secret\"")
            .expect("parse")
            .expect("assignment");
        assert_eq!(parsed.0, "OPENAI_API_KEY");
        assert_eq!(parsed.1, "secret");
    }

    #[test]
    fn apply_env_text_respects_existing_process_env() {
        let key = "AGPIPE_TEST_ENV_OVERRIDE";
        env::set_var(key, "from-process");
        let initial_keys = HashSet::from([key.to_string()]);
        apply_env_text("AGPIPE_TEST_ENV_OVERRIDE=from-file\nAGPIPE_STAGE0_BACKEND=local\n", &initial_keys)
            .expect("apply env text");
        assert_eq!(env::var(key).ok().as_deref(), Some("from-process"));
        assert_eq!(env::var("AGPIPE_STAGE0_BACKEND").ok().as_deref(), Some("local"));
        env::remove_var(key);
        env::remove_var("AGPIPE_STAGE0_BACKEND");
    }
}

fn parse_root_limit(args: &[String]) -> Result<(PathBuf, usize), String> {
    let mut root = default_run_root();
    let mut limit: usize = 30;
    let mut index = 0usize;
    while index < args.len() {
        match args[index].as_str() {
            "--root" => {
                index += 1;
                root = PathBuf::from(args.get(index).ok_or("--root requires a value")?);
            }
            "--limit" => {
                index += 1;
                limit = args
                    .get(index)
                    .ok_or("--limit requires a value")?
                    .parse::<usize>()
                    .map_err(|_| "Invalid --limit value".to_string())?;
            }
            value if !value.starts_with("--") && index == 0 => {
                root = PathBuf::from(value);
            }
            other => return Err(format!("Unexpected argument: {other}")),
        }
        index += 1;
    }
    Ok((root, limit))
}

fn command_runs(ctx: &Context, args: &[String]) -> Result<ExitCode, String> {
    let (root, limit) = parse_root_limit(args)?;
    let runs = discover_run_dirs(&root)?;
    if runs.is_empty() {
        println!("No runs found under {}", root.display());
        return Ok(ExitCode::SUCCESS);
    }
    for run_dir in runs.into_iter().take(limit) {
        let report = doctor_report(ctx, &run_dir)?;
        println!(
            "{} | health={} goal={} next={} safe={}",
            run_dir.display(),
            report.health,
            report.goal,
            report.next,
            report.safe_next_action
        );
    }
    Ok(ExitCode::SUCCESS)
}

fn command_delegate_run_stage_stream(
    ctx: &Context,
    subcommand: &str,
    args: &[String],
) -> Result<ExitCode, String> {
    if args.is_empty() {
        return Err(format!("Command `{subcommand}` requires <run_dir>."));
    }
    let run_dir = PathBuf::from(&args[0]);
    let extra: Vec<&str> = args[1..].iter().map(|value| value.as_str()).collect();
    let should_track = matches!(
        subcommand,
        "start"
            | "start-next"
            | "start-solvers"
            | "refresh-prompts"
            | "refresh-prompt"
            | "step-back"
            | "recheck"
            | "rerun"
            | "host-probe"
    );
    let code = if should_track {
        let tracked_stage = tracked_stage_for_cli_action(ctx, &run_dir, subcommand, &extra);
        run_tracked_cli_action(
            ctx,
            &run_dir,
            subcommand,
            tracked_stage.as_deref(),
            subcommand,
            || {
            run_stage_capture(ctx, &run_dir, subcommand, &extra)
            },
        )?
    } else {
        run_stage_stream(ctx, &run_dir, subcommand, &extra)?
    };
    Ok(ExitCode::from(code as u8))
}

fn command_task_flow_delegate_stream(
    ctx: &Context,
    subcommand: &str,
    args: &[String],
) -> Result<ExitCode, String> {
    let forwarded: Vec<String> = args.to_vec();
    let code = task_flow_stream(ctx, subcommand, &forwarded)?;
    Ok(ExitCode::from(code as u8))
}

fn command_resume(ctx: &Context, args: &[String]) -> Result<ExitCode, String> {
    if args.is_empty() {
        return Err("resume requires <run_dir>".to_string());
    }
    let run_dir = PathBuf::from(&args[0]);
    let mut until = "verification".to_string();
    let mut auto_approve = false;
    let mut index = 1usize;
    while index < args.len() {
        match args[index].as_str() {
            "--until" => {
                index += 1;
                until.clone_from(args.get(index).ok_or("--until requires a value")?);
            }
            "--auto-approve" => auto_approve = true,
            other => return Err(format!("Unexpected argument for resume: {other}")),
        }
        index += 1;
    }
    let resume_stage = tracked_stage_for_named_action(ctx, &run_dir, "start-solvers")
        .or_else(|| tracked_stage_for_cli_action(ctx, &run_dir, "start-next", &[]));
    let code = run_tracked_cli_action(
        ctx,
        &run_dir,
        "resume",
        resume_stage.as_deref(),
        "resume until verification",
        || automate_run(ctx, &run_dir, &until, auto_approve),
    )?;
    Ok(ExitCode::from(code as u8))
}

fn command_safe_next(ctx: &Context, args: &[String]) -> Result<ExitCode, String> {
    if args.is_empty() {
        return Err("safe-next requires <run_dir>".to_string());
    }
    let run_dir = PathBuf::from(&args[0]);
    let report = doctor_report(ctx, &run_dir)?;
    let action = report.safe_next_action.trim();
    if action.is_empty() || action == "none" {
        println!("No action to run.");
        return Ok(ExitCode::SUCCESS);
    }
    let safe_next_stage = tracked_stage_for_named_action(ctx, &run_dir, action);
    let code = run_tracked_cli_action(
        ctx,
        &run_dir,
        "safe-next",
        safe_next_stage.as_deref(),
        "safe-next-action",
        || execute_safe_next_action(ctx, &run_dir),
    )?;
    Ok(ExitCode::from(code as u8))
}

fn print_command_result(result: &engine::CommandResult) {
    if !result.stdout.trim().is_empty() {
        print!("{}", result.stdout);
        std::io::stdout().flush().ok();
    }
    if !result.stderr.trim().is_empty() {
        eprint!("{}", result.stderr);
        std::io::stderr().flush().ok();
    }
}

fn tracked_stage_for_named_action(
    ctx: &Context,
    run_dir: &PathBuf,
    action: &str,
) -> Option<String> {
    let trimmed = action.trim();
    if trimmed == "start-solvers" {
        return engine::next_stage(ctx, run_dir)
            .ok()
            .filter(|stage| !stage.is_empty() && stage != "none" && stage != "rerun");
    }
    if trimmed == "host-probe --refresh" {
        return Some("host-probe".to_string());
    }
    if trimmed == "rerun" {
        return Some("rerun".to_string());
    }
    if let Some(stage) = trimmed.strip_prefix("start ") {
        return Some(stage.trim().to_string());
    }
    if let Some(stage) = trimmed.strip_prefix("step-back ") {
        return Some(stage.trim().to_string());
    }
    if let Some(stage) = trimmed.strip_prefix("recheck ") {
        return Some(stage.trim().to_string());
    }
    None
}

fn tracked_stage_for_cli_action(
    ctx: &Context,
    run_dir: &PathBuf,
    subcommand: &str,
    extra: &[&str],
) -> Option<String> {
    match subcommand {
        "start" | "step-back" | "recheck" => extra.first().map(|stage| stage.to_string()),
        "start-next" => engine::next_stage(ctx, run_dir)
            .ok()
            .filter(|stage| !stage.is_empty() && stage != "none" && stage != "rerun"),
        "start-solvers" => tracked_stage_for_named_action(ctx, run_dir, "start-solvers"),
        "host-probe" => Some("host-probe".to_string()),
        "rerun" => Some("rerun".to_string()),
        "doctor" if extra.contains(&"--fix") => doctor_report(ctx, run_dir)
            .ok()
            .and_then(|report| tracked_stage_for_named_action(ctx, run_dir, &report.safe_next_action)),
        _ => None,
    }
}

fn run_tracked_cli_action<F>(
    _ctx: &Context,
    run_dir: &PathBuf,
    label: &str,
    stage: Option<&str>,
    command_hint: &str,
    action: F,
) -> Result<i32, String>
where
    F: FnOnce() -> Result<engine::CommandResult, String>,
{
    let pid = std::process::id() as i32;
    let _ = runtime::start_job(run_dir, label, stage, command_hint, pid, pid);
    let result = action();
    match result {
        Ok(result) => {
            let status = if result.code == 0 {
                "completed"
            } else if result.code == 130 {
                "interrupted"
            } else {
                "failed"
            };
            let message = if result.stderr.trim().is_empty() {
                None
            } else {
                Some(result.stderr.as_str())
            };
            let _ = runtime::finish_job(run_dir, status, Some(result.code), message);
            print_command_result(&result);
            Ok(result.code)
        }
        Err(err) => {
            let _ = runtime::finish_job(run_dir, "failed", Some(1), Some(&err));
            Err(err)
        }
    }
}

fn command_runtime_check(command_name: &str, args: &[String]) -> Result<ExitCode, String> {
    if args.is_empty() {
        return Err(format!("{command_name} requires <run_dir>"));
    }
    let run_dir = PathBuf::from(&args[0]);
    let mut phase = "manual".to_string();
    let mut spec: Option<PathBuf> = None;
    let mut index = 1usize;
    while index < args.len() {
        match args[index].as_str() {
            "--phase" => {
                index += 1;
                phase.clone_from(args.get(index).ok_or("--phase requires a value")?);
            }
            "--spec" => {
                index += 1;
                spec = Some(PathBuf::from(
                    args.get(index).ok_or("--spec requires a value")?,
                ));
            }
            other => return Err(format!("Unexpected argument for {command_name}: {other}")),
        }
        index += 1;
    }
    let result = if command_name == "service-check" {
        service_check_run(&run_dir, &phase, spec.as_deref())?
    } else {
        runtime_check_run(&run_dir, &phase, spec.as_deref())?
    };
    if !result.stdout.trim().is_empty() {
        print!("{}", result.stdout);
        std::io::stdout().flush().ok();
    }
    if !result.stderr.trim().is_empty() {
        eprint!("{}", result.stderr);
        std::io::stderr().flush().ok();
    }
    Ok(ExitCode::from(result.code as u8))
}

fn command_amend(ctx: &Context, args: &[String]) -> Result<ExitCode, String> {
    if args.is_empty() {
        return Err("amend requires <run_dir>".to_string());
    }
    let run_dir = PathBuf::from(&args[0]);
    let mut note: Option<String> = None;
    let mut note_file: Option<PathBuf> = None;
    let mut rewind = "intake".to_string();
    let mut auto_refresh = false;
    let mut index = 1usize;
    while index < args.len() {
        match args[index].as_str() {
            "--note" => {
                index += 1;
                note = Some(args.get(index).ok_or("--note requires a value")?.clone());
            }
            "--note-file" => {
                index += 1;
                note_file = Some(PathBuf::from(
                    args.get(index).ok_or("--note-file requires a value")?,
                ));
            }
            "--rewind" => {
                index += 1;
                rewind.clone_from(args.get(index).ok_or("--rewind requires a value")?);
            }
            "--auto-refresh-prompts" => auto_refresh = true,
            other => return Err(format!("Unexpected argument for amend: {other}")),
        }
        index += 1;
    }

    let note_text = if let Some(value) = note {
        value
    } else if let Some(path) = note_file {
        std::fs::read_to_string(path).map_err(|err| format!("Could not read note file: {err}"))?
    } else {
        return Err("Provide --note or --note-file.".to_string());
    };

    let result = amend_run(ctx, &run_dir, &note_text, &rewind, auto_refresh)?;
    if !result.stdout.trim().is_empty() {
        println!("{}", result.stdout.trim_end());
    }
    if !result.stderr.trim().is_empty() {
        eprintln!("{}", result.stderr.trim_end());
    }
    Ok(ExitCode::from(result.code as u8))
}

fn command_rm(args: &[String]) -> Result<ExitCode, String> {
    if args.is_empty() {
        return Err("rm requires <run_dir>".to_string());
    }
    let run_dir = PathBuf::from(&args[0]);
    let yes = args.iter().skip(1).any(|arg| arg == "--yes");
    if !yes && !confirm(&format!("Delete run {}? [y/N]: ", run_dir.display()))? {
        println!("Cancelled.");
        return Ok(ExitCode::SUCCESS);
    }
    delete_run(&run_dir)?;
    println!("Deleted {}", run_dir.display());
    Ok(ExitCode::SUCCESS)
}

fn command_prune_runs(args: &[String]) -> Result<ExitCode, String> {
    let mut root = default_run_root();
    let mut keep: usize = 30;
    let mut older_than_days: Option<u64> = None;
    let mut dry_run = false;
    let mut yes = false;
    let mut index = 0usize;
    while index < args.len() {
        match args[index].as_str() {
            "--keep" => {
                index += 1;
                keep = args
                    .get(index)
                    .ok_or("--keep requires a value")?
                    .parse::<usize>()
                    .map_err(|_| "Invalid --keep value".to_string())?;
            }
            "--older-than-days" => {
                index += 1;
                older_than_days = Some(
                    args.get(index)
                        .ok_or("--older-than-days requires a value")?
                        .parse::<u64>()
                        .map_err(|_| "Invalid --older-than-days value".to_string())?,
                );
            }
            "--dry-run" => dry_run = true,
            "--yes" => yes = true,
            value if !value.starts_with("--") && index == 0 => root = PathBuf::from(value),
            other => return Err(format!("Unexpected argument for prune-runs: {other}")),
        }
        index += 1;
    }
    let candidates = choose_prune_candidates(&root, keep, older_than_days)?;
    if candidates.is_empty() {
        println!("No runs matched the prune criteria.");
        return Ok(ExitCode::SUCCESS);
    }
    println!("Runs to delete:");
    for run_dir in &candidates {
        println!("- {}", run_dir.display());
    }
    if dry_run {
        return Ok(ExitCode::SUCCESS);
    }
    if !yes && !confirm("Delete these runs? [y/N]: ")? {
        println!("Cancelled.");
        return Ok(ExitCode::SUCCESS);
    }
    for run_dir in &candidates {
        delete_run(run_dir)?;
    }
    println!("Deleted {} runs.", candidates.len());
    Ok(ExitCode::SUCCESS)
}

fn confirm(prompt: &str) -> Result<bool, String> {
    use std::io::{self, Write};
    print!("{prompt}");
    io::stdout()
        .flush()
        .map_err(|err| format!("Could not flush stdout: {err}"))?;
    let mut line = String::new();
    io::stdin()
        .read_line(&mut line)
        .map_err(|err| format!("Could not read input: {err}"))?;
    let answer = line.trim().to_lowercase();
    Ok(answer == "y" || answer == "yes")
}
