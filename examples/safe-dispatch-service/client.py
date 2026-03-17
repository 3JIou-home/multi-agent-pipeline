import json
import os
import sys
import urllib.error
import urllib.request


PORT_FILE = os.environ.get("SERVICE_PORT_FILE", "runtime-port.json")

DEFAULT_TASKS = [
    {
        "id": "ops-hotfix",
        "lane": "ops",
        "priority": 5,
        "effort_minutes": 55,
        "risk": 7,
        "requires_review": False,
    },
    {
        "id": "support-chat",
        "lane": "support",
        "priority": 4,
        "effort_minutes": 70,
        "risk": 8,
        "requires_review": False,
    },
    {
        "id": "payments-rollup",
        "lane": "payments",
        "priority": 4,
        "effort_minutes": 90,
        "risk": 12,
        "requires_review": True,
    },
    {
        "id": "release-notes",
        "lane": "release",
        "priority": 3,
        "effort_minutes": 50,
        "risk": 6,
        "requires_review": False,
    },
    {
        "id": "ops-backfill",
        "lane": "ops",
        "priority": 2,
        "effort_minutes": 120,
        "risk": 18,
        "requires_review": False,
    },
]


def load_port():
    with open(PORT_FILE, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    port = payload.get("port")
    if not isinstance(port, int) or port <= 0:
        raise SystemExit(f"invalid port payload in {PORT_FILE}")
    return port


def request(method, path, payload=None):
    port = load_port()
    url = f"http://127.0.0.1:{port}{path}"
    data = None
    headers = {}
    if payload is not None:
        data = json.dumps(payload, sort_keys=True).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=5) as response:
            status = response.status
            body = response.read().decode("utf-8")
    except urllib.error.HTTPError as err:
        status = err.code
        body = err.read().decode("utf-8")
    payload = json.loads(body) if body else {}
    return status, payload


def ensure_status(actual, expected):
    if actual != expected:
        raise SystemExit(f"unexpected status: expected {expected}, got {actual}")


def print_reason_counts(reason_counts):
    for reason in sorted(reason_counts):
        print(f"reason.{reason}={reason_counts[reason]}")


def health():
    status, payload = request("GET", "/health")
    ensure_status(status, 200)
    print(payload["status"])


def seed_default_batch():
    status, payload = request("POST", "/tasks/batch", {"tasks": DEFAULT_TASKS})
    ensure_status(status, 200)
    print(f"status={status}")
    print(f"accepted_count={payload['accepted_count']}")
    print(f"queued={payload['queued_count']}")


def plan_tight_budget():
    status, payload = request(
        "POST",
        "/plan",
        {"capacity_minutes": 160, "risk_budget": 25},
    )
    ensure_status(status, 200)
    print(f"status={status}")
    print(f"accepted={','.join(payload['accepted_ids'])}")
    print(f"deferred_count={payload['deferred_count']}")
    print_reason_counts(payload["reason_counts"])
    print(f"capacity_used={payload['capacity_used']}")
    print(f"risk_used={payload['risk_used']}")


def duplicate_default_task():
    status, payload = request(
        "POST",
        "/tasks/batch",
        {"tasks": [DEFAULT_TASKS[0]]},
    )
    ensure_status(status, 409)
    error = payload["error"]
    print(f"status={status}")
    print(f"code={error['code']}")
    print(f"duplicate_id={error['details']['duplicate_ids'][0]}")


def summary():
    status, payload = request("GET", "/summary")
    ensure_status(status, 200)
    last_plan = payload["last_plan"] or {}
    print(f"queue_size={payload['queue_size']}")
    print(f"last_plan.accepted={last_plan.get('accepted_count', 0)}")
    print(f"last_plan.deferred={last_plan.get('deferred_count', 0)}")
    print_reason_counts(last_plan.get("reason_counts", {}))


def main():
    if len(sys.argv) != 2:
        raise SystemExit(
            "usage: python3 client.py {health|seed-default-batch|plan-tight-budget|duplicate-default-task|summary}"
        )
    command = sys.argv[1]
    if command == "health":
        health()
        return
    if command == "seed-default-batch":
        seed_default_batch()
        return
    if command == "plan-tight-budget":
        plan_tight_budget()
        return
    if command == "duplicate-default-task":
        duplicate_default_task()
        return
    if command == "summary":
        summary()
        return
    raise SystemExit(f"unknown command: {command}")


if __name__ == "__main__":
    main()
