#!/usr/bin/env python3
"""Helpers for shared cache metadata, locking, and maintenance."""

from __future__ import annotations

import json
import os
import time
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterator


CACHE_AREAS = ("research", "downloads", "wheelhouse", "models", "verification")
INDEX_VERSION = 1


def cache_root(cache_config: dict[str, object]) -> Path:
    return Path(str(cache_config["root"]))


def cache_meta(cache_config: dict[str, object]) -> dict[str, str]:
    meta = cache_config.get("meta")
    if isinstance(meta, dict) and meta:
        return {str(key): str(value) for key, value in meta.items()}

    root = cache_root(cache_config)
    meta_root = root / ".meta"
    return {
        "root": str(meta_root),
        "index": str(meta_root / "index.json"),
        "locks": str(meta_root / "locks"),
    }


def ensure_cache_layout(cache_config: dict[str, object]) -> None:
    if not cache_config.get("enabled"):
        return

    root = cache_root(cache_config)
    root.mkdir(parents=True, exist_ok=True)
    for path in cache_config.get("paths", {}).values():
        Path(str(path)).mkdir(parents=True, exist_ok=True)

    meta = cache_meta(cache_config)
    Path(meta["root"]).mkdir(parents=True, exist_ok=True)
    Path(meta["locks"]).mkdir(parents=True, exist_ok=True)


@contextmanager
def cache_lock(
    cache_config: dict[str, object],
    name: str,
    timeout_seconds: float = 30.0,
    poll_interval_seconds: float = 0.1,
) -> Iterator[None]:
    if not cache_config.get("enabled"):
        yield
        return

    ensure_cache_layout(cache_config)
    lock_dir = Path(cache_meta(cache_config)["locks"]) / f"{name}.lock"
    deadline = time.time() + timeout_seconds
    while True:
        try:
            lock_dir.mkdir()
            break
        except FileExistsError:
            if time.time() >= deadline:
                raise TimeoutError(f"Timed out waiting for cache lock: {lock_dir}")
            time.sleep(poll_interval_seconds)

    try:
        yield
    finally:
        try:
            lock_dir.rmdir()
        except FileNotFoundError:
            pass


def _scan_area(area: str, path: Path) -> list[dict[str, object]]:
    if not path.exists():
        return []

    entries: list[dict[str, object]] = []
    for file_path in sorted(candidate for candidate in path.rglob("*") if candidate.is_file()):
        stat = file_path.stat()
        entries.append(
            {
                "area": area,
                "path": str(file_path),
                "relative_path": str(file_path.relative_to(path)),
                "size_bytes": stat.st_size,
                "modified_at": datetime.fromtimestamp(stat.st_mtime).isoformat(timespec="seconds"),
            }
        )
    return entries


def build_cache_index(cache_config: dict[str, object]) -> dict[str, object]:
    ensure_cache_layout(cache_config)

    entries: list[dict[str, object]] = []
    areas: dict[str, dict[str, object]] = {}
    total_size = 0
    total_files = 0
    for area in CACHE_AREAS:
        area_path = Path(str(cache_config.get("paths", {}).get(area, cache_root(cache_config) / area)))
        area_entries = _scan_area(area, area_path)
        area_size = sum(int(item["size_bytes"]) for item in area_entries)
        total_size += area_size
        total_files += len(area_entries)
        entries.extend(area_entries)
        areas[area] = {
            "path": str(area_path),
            "file_count": len(area_entries),
            "size_bytes": area_size,
        }

    return {
        "version": INDEX_VERSION,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "root": str(cache_root(cache_config)),
        "policy": cache_config.get("policy", "off"),
        "total_files": total_files,
        "total_size_bytes": total_size,
        "areas": areas,
        "entries": entries,
    }


def _atomic_write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(temp_path, path)


def refresh_cache_index(cache_config: dict[str, object]) -> dict[str, object]:
    if not cache_config.get("enabled"):
        return {
            "version": INDEX_VERSION,
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "root": str(cache_root(cache_config)),
            "policy": "off",
            "total_files": 0,
            "total_size_bytes": 0,
            "areas": {},
            "entries": [],
        }

    with cache_lock(cache_config, "index"):
        index = build_cache_index(cache_config)
        index_path = Path(cache_meta(cache_config)["index"])
        _atomic_write_json(index_path, index)
        return index


def load_cache_index(cache_config: dict[str, object], refresh: bool = False) -> dict[str, object]:
    if not cache_config.get("enabled"):
        return refresh_cache_index(cache_config)

    ensure_cache_layout(cache_config)
    index_path = Path(cache_meta(cache_config)["index"])
    if refresh or not index_path.exists():
        return refresh_cache_index(cache_config)

    try:
        payload = json.loads(index_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return refresh_cache_index(cache_config)
    if not isinstance(payload, dict):
        return refresh_cache_index(cache_config)
    return payload


def format_size(size_bytes: int) -> str:
    size = float(size_bytes)
    units = ["B", "KB", "MB", "GB", "TB"]
    for unit in units:
        if size < 1024 or unit == units[-1]:
            return f"{size:.1f} {unit}" if unit != "B" else f"{int(size)} B"
        size /= 1024
    return f"{int(size_bytes)} B"


def prune_cache(
    cache_config: dict[str, object],
    *,
    max_age_days: int | None = None,
    area_filters: set[str] | None = None,
    dry_run: bool = False,
) -> dict[str, object]:
    if not cache_config.get("enabled"):
        return {
            "removed_files": 0,
            "removed_bytes": 0,
            "areas": {},
            "dry_run": dry_run,
            "index": refresh_cache_index(cache_config),
        }

    threshold = None
    if max_age_days is not None:
        threshold = datetime.now() - timedelta(days=max_age_days)

    with cache_lock(cache_config, "prune"):
        removed_files = 0
        removed_bytes = 0
        per_area: dict[str, dict[str, int]] = {}

        for area in CACHE_AREAS:
            if area_filters and area not in area_filters:
                continue
            area_path = Path(str(cache_config.get("paths", {}).get(area, cache_root(cache_config) / area)))
            if not area_path.exists():
                continue

            candidates = [path for path in area_path.rglob("*") if path.is_file()]
            for file_path in sorted(candidates):
                stat = file_path.stat()
                modified_at = datetime.fromtimestamp(stat.st_mtime)
                if threshold is not None and modified_at > threshold:
                    continue

                removed_files += 1
                removed_bytes += stat.st_size
                area_stats = per_area.setdefault(area, {"removed_files": 0, "removed_bytes": 0})
                area_stats["removed_files"] += 1
                area_stats["removed_bytes"] += stat.st_size
                if not dry_run:
                    file_path.unlink(missing_ok=True)

            if not dry_run and area_path.exists():
                for subdir in sorted(
                    (path for path in area_path.rglob("*") if path.is_dir()),
                    key=lambda path: len(path.parts),
                    reverse=True,
                ):
                    try:
                        subdir.rmdir()
                    except OSError:
                        pass

        index = refresh_cache_index(cache_config)
        return {
            "removed_files": removed_files,
            "removed_bytes": removed_bytes,
            "areas": per_area,
            "dry_run": dry_run,
            "index": index,
        }
