mod engine;
mod runtime;
mod tui;

use engine::{
    amend_run, automate_run, choose_prune_candidates, default_run_root, delete_run,
    discover_run_dirs, doctor_report, run_stage_stream, runtime_check_run, service_check_run,
    task_flow_stream, Context,
};
use std::env;
use std::io::Write;
use std::path::PathBuf;
use std::process::ExitCode;

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
        other => Err(format!("Unknown command: {other}")),
    }
}

fn print_help() {
    println!(
        "agpipe\n\n\
Run the terminal UI or drive the full pipeline directly from the CLI.\n\n\
Common commands:\n\
  agpipe\n\
  agpipe interview-questions --task '...' --workspace /path/to/workspace --output-dir /tmp/agpipe-interview\n\
  agpipe interview-finalize --task '...' --workspace /path/to/workspace --session-dir /tmp/agpipe-interview/_interviews/<session> --answers-file /tmp/answers.json\n\
  agpipe create-run --task-file /tmp/agpipe-interview/_interviews/<session>/final-task.md --workspace /path/to/workspace --output-dir /Users/admin/agent-runs\n\
  agpipe resume /Users/admin/agent-runs/<run-id> --until execution\n\
  agpipe runtime-check /Users/admin/agent-runs/<run-id> --phase execution\n\
  agpipe doctor /Users/admin/agent-runs/<run-id>\n\
  agpipe run --task '...' --workspace /path/to/workspace --output-dir /Users/admin/agent-runs --until review\n\n\
Notes:\n\
  - The direct CLI path is interview -> create-run -> resume -> execution.\n\
  - `runtime-check` and `service-check` use the same local runtime harness.\n\
  - `agpipe internal ...` is still available for scripting and debugging.\n"
    );
}

fn print_internal_help() {
    println!(
        "agpipe internal\n\n\
Low-level automation and debugging commands.\n\n\
Examples:\n\
  agpipe internal runs /Users/admin/agent-runs --limit 10\n\
  agpipe internal status /Users/admin/agent-runs/<run-id>\n\
  agpipe internal runtime-check /Users/admin/agent-runs/<run-id> --phase verification\n\
  agpipe internal service-check /Users/admin/agent-runs/<run-id> --phase verification\n\
  agpipe internal start-next /Users/admin/agent-runs/<run-id>\n\
  agpipe internal resume /Users/admin/agent-runs/<run-id> --until verification\n\
  agpipe internal create-run --task '...' --workspace /path/to/workspace --output-dir /path/to/agent-runs\n\
  agpipe internal interview-questions --task '...' --workspace /path/to/workspace\n"
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
    let code = run_stage_stream(ctx, &run_dir, subcommand, &extra)?;
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
    let result = automate_run(ctx, &run_dir, &until, auto_approve)?;
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
    if action == "start-solvers" {
        let code = run_stage_stream(ctx, &run_dir, "start-solvers", &[])?;
        return Ok(ExitCode::from(code as u8));
    }
    if action == "rerun" {
        let code = run_stage_stream(ctx, &run_dir, "rerun", &[])?;
        return Ok(ExitCode::from(code as u8));
    }
    if let Some(stage) = action.strip_prefix("start ") {
        let code = run_stage_stream(ctx, &run_dir, "start", &[stage.trim()])?;
        return Ok(ExitCode::from(code as u8));
    }
    if let Some(stage) = action.strip_prefix("step-back ") {
        let code = run_stage_stream(ctx, &run_dir, "step-back", &[stage.trim()])?;
        return Ok(ExitCode::from(code as u8));
    }
    if let Some(stage) = action.strip_prefix("recheck ") {
        let code = run_stage_stream(ctx, &run_dir, "recheck", &[stage.trim()])?;
        return Ok(ExitCode::from(code as u8));
    }
    Err(format!("Unsupported safe-next-action: {action}"))
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
