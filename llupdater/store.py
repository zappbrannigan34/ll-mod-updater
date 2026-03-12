import copy
import json
import threading
from pathlib import Path
from typing import Any

from .config import (
    CATEGORIES_FILE,
    DATA_DIR,
    DEFAULT_SETTINGS,
    MEDIA_CACHE_DIR,
    MODS_FILE,
    QUEUE_FILE,
    RUNTIME_FILE,
    SETTINGS_FILE,
)

DEFAULT_QUEUE = {"items": []}
DEFAULT_CATEGORIES = {"categories": [], "updated_at": ""}

DEFAULT_RUNTIME = {
    "download_events": [],
    "next_download_after": "",
    "cooldown_until": "",
    "consecutive_503": 0,
    "last_signal": "",
    "last_error": "",
    "last_queue_action": "",
    "lazy_new_mods_cursor": 0,
    "lazy_new_mods_failed_categories": [],
    "lazy_cache_retry_cursor": 0,
    "full_scan_checkpoint": {
        "active": False,
        "interrupted": False,
        "full_catalog": True,
        "token": "",
        "signature": {},
        "started_at": "",
        "updated_at": "",
        "completed_at": "",
        "resume_count": 0,
        "categories_total": 0,
        "categories_done": 0,
        "current_category_index": 0,
        "current_page": 1,
        "mods_discovered": 0,
        "mods_unique": 0,
        "retries_total": 0,
        "last_error": "",
        "last_error_at": "",
    },
}


def _deep_copy(obj: Any) -> Any:
    return copy.deepcopy(obj)


class Store:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._ensure_files()

    def _ensure_files(self) -> None:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        MEDIA_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        Path(DEFAULT_SETTINGS["staging_dir"]).mkdir(parents=True, exist_ok=True)
        Path(DEFAULT_SETTINGS["downloads_dir"]).mkdir(parents=True, exist_ok=True)
        Path(DEFAULT_SETTINGS["backups_dir"]).mkdir(parents=True, exist_ok=True)

        if not SETTINGS_FILE.exists():
            SETTINGS_FILE.write_text(json.dumps(DEFAULT_SETTINGS, indent=2), encoding="utf-8")

        if not MODS_FILE.exists():
            MODS_FILE.write_text(json.dumps({"mods": []}, indent=2), encoding="utf-8")

        if not CATEGORIES_FILE.exists():
            CATEGORIES_FILE.write_text(json.dumps(DEFAULT_CATEGORIES, indent=2), encoding="utf-8")

        if not QUEUE_FILE.exists():
            QUEUE_FILE.write_text(json.dumps(DEFAULT_QUEUE, indent=2), encoding="utf-8")

        if not RUNTIME_FILE.exists():
            RUNTIME_FILE.write_text(json.dumps(DEFAULT_RUNTIME, indent=2), encoding="utf-8")

    def _read_json(self, path: Path, default: Any) -> Any:
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return _deep_copy(default)

    def _write_json(self, path: Path, payload: Any) -> None:
        path.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")

    def get_settings(self) -> dict[str, Any]:
        with self._lock:
            data = self._read_json(SETTINGS_FILE, DEFAULT_SETTINGS)
            merged = _deep_copy(DEFAULT_SETTINGS)
            if isinstance(data, dict):
                merged.update(data)
            return merged

    def save_settings(self, settings_update: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            settings = self.get_settings()
            settings.update(settings_update)

            Path(settings["staging_dir"]).mkdir(parents=True, exist_ok=True)
            Path(settings["downloads_dir"]).mkdir(parents=True, exist_ok=True)
            Path(settings["backups_dir"]).mkdir(parents=True, exist_ok=True)

            self._write_json(SETTINGS_FILE, settings)
            return settings

    def get_mods(self) -> list[dict[str, Any]]:
        with self._lock:
            data = self._read_json(MODS_FILE, {"mods": []})
            mods = data.get("mods", []) if isinstance(data, dict) else []
            return mods if isinstance(mods, list) else []

    def save_mods(self, mods: list[dict[str, Any]]) -> None:
        with self._lock:
            self._write_json(MODS_FILE, {"mods": mods})

    def get_categories(self) -> dict[str, Any]:
        with self._lock:
            data = self._read_json(CATEGORIES_FILE, DEFAULT_CATEGORIES)
            merged = _deep_copy(DEFAULT_CATEGORIES)
            if isinstance(data, dict):
                merged.update(data)
            categories = merged.get("categories", [])
            if not isinstance(categories, list):
                merged["categories"] = []
            return merged

    def save_categories(self, categories: list[dict[str, Any]], updated_at: str) -> None:
        with self._lock:
            self._write_json(CATEGORIES_FILE, {"categories": categories, "updated_at": updated_at})

    def get_queue(self) -> list[dict[str, Any]]:
        with self._lock:
            data = self._read_json(QUEUE_FILE, DEFAULT_QUEUE)
            items = data.get("items", []) if isinstance(data, dict) else []
            return items if isinstance(items, list) else []

    def save_queue(self, items: list[dict[str, Any]]) -> None:
        with self._lock:
            self._write_json(QUEUE_FILE, {"items": items})

    def get_runtime(self) -> dict[str, Any]:
        with self._lock:
            data = self._read_json(RUNTIME_FILE, DEFAULT_RUNTIME)
            runtime = _deep_copy(DEFAULT_RUNTIME)
            if isinstance(data, dict):
                runtime.update(data)
            return runtime

    def save_runtime(self, runtime_update: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            runtime = self.get_runtime()
            runtime.update(runtime_update)
            self._write_json(RUNTIME_FILE, runtime)
            return runtime
