#!/usr/bin/env python3
from app import DispatchState, ServiceError
from client import DEFAULT_TASKS


def print_reason_counts(reason_counts):
    for reason in sorted(reason_counts):
        print(f"reason.{reason}={reason_counts[reason]}", flush=True)


def print_service_error(err):
    print(f"status={err.status_code}", flush=True)
    print(f"code={err.code}", flush=True)
    details = err.details or {}
    if "duplicate_ids" in details and details["duplicate_ids"]:
        print(f"duplicate_id={details['duplicate_ids'][0]}", flush=True)
    if "field" in details:
        print(f"field={details['field']}", flush=True)


def command_health(state):
    summary = state.summary()
    print("status=ok", flush=True)
    print(f"queue_size={summary['queue_size']}", flush=True)
    print(f"has_plan={'true' if summary['last_plan'] is not None else 'false'}", flush=True)


def command_seed_default_batch(state):
    result = state.queue_batch(DEFAULT_TASKS)
    print("status=200", flush=True)
    print(f"accepted_count={result['accepted_count']}", flush=True)
    print(f"queued={result['queued_count']}", flush=True)


def command_plan_tight_budget(state):
    result = state.build_plan(160, 25, None)
    print("status=200", flush=True)
    print(f"accepted={','.join(result['accepted_ids'])}", flush=True)
    print(f"deferred_count={result['deferred_count']}", flush=True)
    print_reason_counts(result["reason_counts"])
    print(f"capacity_used={result['capacity_used']}", flush=True)
    print(f"risk_used={result['risk_used']}", flush=True)


def command_duplicate_default_task(state):
    try:
        state.queue_batch([DEFAULT_TASKS[0]])
        raise RuntimeError("expected duplicate task id error")
    except ServiceError as err:
        print_service_error(err)


def command_invalid_plan(state):
    try:
        state.build_plan(0, 25, None)
        raise RuntimeError("expected invalid plan error")
    except ServiceError as err:
        print_service_error(err)


def command_summary(state):
    summary = state.summary()
    last_plan = summary["last_plan"] or {}
    print(f"queue_size={summary['queue_size']}", flush=True)
    print(f"last_plan.accepted={last_plan.get('accepted_count', 0)}", flush=True)
    print(f"last_plan.deferred={last_plan.get('deferred_count', 0)}", flush=True)
    print_reason_counts(last_plan.get("reason_counts", {}))


def execute_command(state, command):
    if command == "help":
        print(
            "commands=health,seed-default-batch,plan-tight-budget,duplicate-default-task,invalid-plan,summary,quit",
            flush=True,
        )
        return True
    if command == "health":
        command_health(state)
        return True
    if command == "seed-default-batch":
        command_seed_default_batch(state)
        return True
    if command == "plan-tight-budget":
        command_plan_tight_budget(state)
        return True
    if command == "duplicate-default-task":
        command_duplicate_default_task(state)
        return True
    if command == "invalid-plan":
        command_invalid_plan(state)
        return True
    if command == "summary":
        command_summary(state)
        return True
    if command in {"quit", "exit"}:
        print("bye", flush=True)
        return False
    print(f"unknown_command={command}", flush=True)
    return True


def main():
    state = DispatchState()
    print("Safe Dispatch Console", flush=True)
    print(
        "commands=health,seed-default-batch,plan-tight-budget,duplicate-default-task,invalid-plan,summary,quit",
        flush=True,
    )
    while True:
        try:
            command = input("dispatch> ").strip()
        except EOFError:
            print("bye", flush=True)
            return
        if not command:
            continue
        if not execute_command(state, command):
            return


if __name__ == "__main__":
    main()
