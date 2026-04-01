# Safe Dispatch Service

Local safe-dispatch runtime used by the execution stage.

The core logic lives in `app.py` and the default runnable entrypoint is the interactive `console.py` workflow that drives the same in-memory dispatch engine. The runtime-check uses PTY scenarios because direct localhost socket binds are denied in this sandboxed executor.

The service logic covers:

- task validation
- duplicate-id rejection
- review-required deferrals
- capacity and risk budgets

Manual run from the workspace root:

```bash
python3 examples/safe-dispatch-service/console.py
```

Available console commands:

- `health`
- `seed-default-batch`
- `plan-tight-budget`
- `duplicate-default-task`
- `invalid-plan`
- `summary`
- `quit`

If you need the original socket-based variant outside this executor, run:

```bash
SAFE_DISPATCH_TRANSPORT=http python3 examples/safe-dispatch-service/app.py --port 18081
```

Runtime-check entrypoint for a real run:

```bash
./target/release/agpipe internal runtime-check ~/agent-runs/<run-id> --phase verification --spec examples/safe-dispatch-service/runtime-check.json
```
