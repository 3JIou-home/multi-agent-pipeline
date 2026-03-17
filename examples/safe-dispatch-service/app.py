import argparse
import json
import os
import signal
import sys
from dataclasses import asdict, dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Lock


ALLOWED_LANES = {"ops", "payments", "release", "support"}


@dataclass(frozen=True)
class Task:
    id: str
    lane: str
    priority: int
    effort_minutes: int
    risk: int
    requires_review: bool


class DispatchState:
    def __init__(self) -> None:
        self._lock = Lock()
        self._tasks = {}
        self._last_plan = None

    def queue_batch(self, raw_tasks):
        tasks = [validate_task(item) for item in raw_tasks]
        ids = [task.id for task in tasks]
        duplicates_in_batch = sorted({task_id for task_id in ids if ids.count(task_id) > 1})
        if duplicates_in_batch:
            raise ServiceError(
                409,
                "duplicate_task_id",
                "Batch contains duplicate task ids.",
                {"duplicate_ids": duplicates_in_batch},
            )
        with self._lock:
            existing = sorted(task.id for task in tasks if task.id in self._tasks)
            if existing:
                raise ServiceError(
                    409,
                    "duplicate_task_id",
                    "One or more task ids already exist in the queue.",
                    {"duplicate_ids": existing},
                )
            for task in tasks:
                self._tasks[task.id] = task
            return {
                "accepted_count": len(tasks),
                "queued_count": len(self._tasks),
                "task_ids": [task.id for task in tasks],
                "by_lane": lane_counts(self._tasks.values()),
            }

    def build_plan(self, capacity_minutes, risk_budget, lane_limits):
        ensure_positive_integer(capacity_minutes, "capacity_minutes")
        ensure_non_negative_integer(risk_budget, "risk_budget")
        normalized_lane_limits = {}
        if lane_limits is not None:
            if not isinstance(lane_limits, dict):
                raise ServiceError(
                    400,
                    "invalid_plan",
                    "`lane_limits` must be an object when provided.",
                    {},
                )
            for lane, limit in lane_limits.items():
                if lane not in ALLOWED_LANES:
                    raise ServiceError(
                        400,
                        "invalid_plan",
                        "Unknown lane limit provided.",
                        {"lane": lane},
                    )
                ensure_positive_integer(limit, f"lane_limits.{lane}")
                normalized_lane_limits[lane] = limit

        with self._lock:
            tasks = sorted(
                self._tasks.values(),
                key=lambda item: (-item.priority, item.risk, item.effort_minutes, item.id),
            )
            accepted = []
            deferred = []
            accepted_by_lane = {}
            capacity_used = 0
            risk_used = 0

            for task in tasks:
                reason = None
                if task.requires_review:
                    reason = "manual_review"
                elif (
                    task.lane in normalized_lane_limits
                    and accepted_by_lane.get(task.lane, 0) >= normalized_lane_limits[task.lane]
                ):
                    reason = "lane_limit"
                elif capacity_used + task.effort_minutes > capacity_minutes:
                    reason = "over_capacity"
                elif risk_used + task.risk > risk_budget:
                    reason = "over_risk"

                if reason is not None:
                    deferred.append({"id": task.id, "lane": task.lane, "reason": reason})
                    continue

                accepted.append(asdict(task))
                capacity_used += task.effort_minutes
                risk_used += task.risk
                accepted_by_lane[task.lane] = accepted_by_lane.get(task.lane, 0) + 1

            reason_counts = {}
            for item in deferred:
                reason_counts[item["reason"]] = reason_counts.get(item["reason"], 0) + 1

            plan = {
                "capacity_minutes": capacity_minutes,
                "risk_budget": risk_budget,
                "accepted_count": len(accepted),
                "deferred_count": len(deferred),
                "capacity_used": capacity_used,
                "risk_used": risk_used,
                "accepted_ids": [task["id"] for task in accepted],
                "deferred": deferred,
                "reason_counts": reason_counts,
            }
            self._last_plan = plan
            return plan

    def summary(self):
        with self._lock:
            return {
                "queue_size": len(self._tasks),
                "by_lane": lane_counts(self._tasks.values()),
                "task_ids": sorted(self._tasks.keys()),
                "last_plan": self._last_plan,
            }


class ServiceError(Exception):
    def __init__(self, status_code, code, message, details):
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message
        self.details = details


def lane_counts(tasks):
    counts = {}
    for task in tasks:
        counts[task.lane] = counts.get(task.lane, 0) + 1
    return dict(sorted(counts.items()))


def ensure_positive_integer(value, name):
    if not isinstance(value, int) or value <= 0:
        raise ServiceError(
            400,
            "invalid_number",
            f"`{name}` must be a positive integer.",
            {"field": name},
        )


def ensure_non_negative_integer(value, name):
    if not isinstance(value, int) or value < 0:
        raise ServiceError(
            400,
            "invalid_number",
            f"`{name}` must be a non-negative integer.",
            {"field": name},
        )


def validate_task(raw):
    if not isinstance(raw, dict):
        raise ServiceError(
            400,
            "invalid_task",
            "Each task must be an object.",
            {},
        )
    required_fields = {
        "id": str,
        "lane": str,
        "priority": int,
        "effort_minutes": int,
        "risk": int,
        "requires_review": bool,
    }
    missing = sorted(field for field in required_fields if field not in raw)
    if missing:
        raise ServiceError(
            400,
            "invalid_task",
            "Task is missing required fields.",
            {"missing_fields": missing},
        )
    for field, field_type in required_fields.items():
        if not isinstance(raw[field], field_type):
            raise ServiceError(
                400,
                "invalid_task",
                f"`{field}` has the wrong type.",
                {"field": field},
            )
    task_id = raw["id"].strip()
    if not task_id:
        raise ServiceError(
            400,
            "invalid_task",
            "`id` must not be empty.",
            {"field": "id"},
        )
    lane = raw["lane"].strip().lower()
    if lane not in ALLOWED_LANES:
        raise ServiceError(
            400,
            "invalid_task",
            "Unknown lane.",
            {"field": "lane", "allowed": sorted(ALLOWED_LANES)},
        )
    ensure_positive_integer(raw["priority"], "priority")
    ensure_positive_integer(raw["effort_minutes"], "effort_minutes")
    ensure_non_negative_integer(raw["risk"], "risk")
    return Task(
        id=task_id,
        lane=lane,
        priority=raw["priority"],
        effort_minutes=raw["effort_minutes"],
        risk=raw["risk"],
        requires_review=raw["requires_review"],
    )


def read_json(handler):
    length = int(handler.headers.get("Content-Length", "0"))
    raw = handler.rfile.read(length) if length > 0 else b""
    if not raw:
        return {}
    try:
        return json.loads(raw.decode("utf-8"))
    except json.JSONDecodeError as err:
        raise ServiceError(
            400,
            "invalid_json",
            f"Request body is not valid JSON: {err.msg}.",
            {},
        )


class RequestHandler(BaseHTTPRequestHandler):
    server_version = "SafeDispatchService/1.0"

    def do_GET(self):
        if self.path == "/health":
            self.write_json(
                200,
                {
                    "status": "ok",
                    "queue_size": len(self.server.state._tasks),
                    "has_plan": self.server.state._last_plan is not None,
                },
            )
            return
        if self.path == "/summary":
            self.write_json(200, self.server.state.summary())
            return
        self.write_error_response(
            ServiceError(404, "not_found", "Unknown endpoint.", {"path": self.path})
        )

    def do_POST(self):
        try:
            payload = read_json(self)
            if self.path == "/tasks/batch":
                tasks = payload.get("tasks")
                if not isinstance(tasks, list) or not tasks:
                    raise ServiceError(
                        400,
                        "invalid_batch",
                        "`tasks` must be a non-empty array.",
                        {},
                    )
                self.write_json(200, self.server.state.queue_batch(tasks))
                return
            if self.path == "/plan":
                self.write_json(
                    200,
                    self.server.state.build_plan(
                        payload.get("capacity_minutes"),
                        payload.get("risk_budget"),
                        payload.get("lane_limits"),
                    ),
                )
                return
            raise ServiceError(404, "not_found", "Unknown endpoint.", {"path": self.path})
        except ServiceError as err:
            self.write_error_response(err)

    def write_json(self, status_code, payload):
        body = json.dumps(payload, sort_keys=True).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def write_error_response(self, err):
        self.write_json(
            err.status_code,
            {
                "error": {
                    "code": err.code,
                    "message": err.message,
                    "details": err.details,
                }
            },
        )

    def log_message(self, fmt, *args):
        return


class SafeDispatchServer(ThreadingHTTPServer):
    def __init__(self, server_address, handler_class):
        super().__init__(server_address, handler_class)
        self.state = DispatchState()


def write_port_file(path, port):
    if not path:
        return
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump({"port": port}, handle)
        handle.write("\n")


def parse_args():
    parser = argparse.ArgumentParser(description="Local safe dispatch service.")
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Bind address. Defaults to 127.0.0.1.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=0,
        help="Port to bind. Defaults to 0 for an ephemeral port.",
    )
    parser.add_argument(
        "--port-file",
        default=os.environ.get("SERVICE_PORT_FILE", ""),
        help="Optional JSON file that receives the selected port.",
    )
    return parser.parse_args()


def serve_http():
    args = parse_args()
    server = SafeDispatchServer((args.host, args.port), RequestHandler)
    write_port_file(args.port_file, server.server_address[1])
    print(
        f"safe-dispatch-service listening on http://{args.host}:{server.server_address[1]}",
        flush=True,
    )

    def shutdown(*_args):
        server.shutdown()

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    try:
        server.serve_forever()
    finally:
        server.server_close()


def main():
    if os.environ.get("SAFE_DISPATCH_TRANSPORT", "").strip().lower() == "http":
        serve_http()
        return
    from console import main as console_main

    console_main()


if __name__ == "__main__":
    try:
        main()
    except ServiceError as err:
        json.dump(
            {
                "error": {
                    "code": err.code,
                    "message": err.message,
                    "details": err.details,
                }
            },
            sys.stderr,
            sort_keys=True,
        )
        sys.stderr.write("\n")
        raise SystemExit(1)
