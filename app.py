import threading
from pathlib import Path
import sys

from flask import Flask, abort, jsonify, render_template, request, send_from_directory

from llupdater.config import MEDIA_CACHE_DIR
from llupdater.image_cache import is_safe_cached_filename

from llupdater.scheduler import AutoScheduler
from llupdater.store import Store
from llupdater.updater import ModUpdater


def _resource_dir(name: str) -> str:
    if getattr(sys, "frozen", False):
        base = Path(getattr(sys, "_MEIPASS", Path(sys.executable).resolve().parent))
    else:
        base = Path(__file__).resolve().parent
    return str(base / name)


app = Flask(__name__, template_folder=_resource_dir("templates"), static_folder=_resource_dir("static"))

store = Store()
updater = ModUpdater(store)
scheduler = AutoScheduler(store, updater)
scheduler.start()

_discover_lock = threading.Lock()
_discover_thread: threading.Thread | None = None


def _run_discover_job(scan_pages: int | None, full_catalog: bool) -> None:
    global _discover_thread
    try:
        updater.discover(scan_pages=scan_pages, full_catalog=full_catalog)
    finally:
        with _discover_lock:
            _discover_thread = None


def _discover_worker_running() -> bool:
    with _discover_lock:
        return _discover_thread is not None and _discover_thread.is_alive()


@app.route("/")
def index():
    return render_template("index.html")


@app.get("/api/state")
def api_state():
    categories = store.get_categories()
    return jsonify(
        {
            "settings": store.get_settings(),
            "mods": store.get_mods(),
            "categories": categories,
            "queue": updater.queue_snapshot(),
            "scheduler": scheduler.state(),
            "discover": updater.get_discover_progress(),
        }
    )


@app.post("/api/settings")
def api_save_settings():
    payload = request.get_json(silent=True) or {}
    if bool(payload.get("proxy_enabled", False)) and not str(payload.get("proxy_url") or "").strip():
        return jsonify({"ok": False, "error": "proxy_url is required when proxy_enabled=true"}), 400
    settings = store.save_settings(payload)
    return jsonify({"ok": True, "settings": settings})


@app.post("/api/pick_mods_dir")
def api_pick_mods_dir():
    try:
        import tkinter as tk
        from tkinter import filedialog

        root = tk.Tk()
        root.withdraw()
        root.attributes("-topmost", True)
        selected = filedialog.askdirectory(initialdir=store.get_settings().get("mods_dir", ""), title="Select Sims 4 Mods folder")
        root.destroy()
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500

    if not selected:
        return jsonify({"ok": True, "cancelled": True})

    settings = store.save_settings({"mods_dir": selected})
    return jsonify({"ok": True, "cancelled": False, "settings": settings})


@app.post("/api/discover")
def api_discover():
    payload = request.get_json(silent=True) or {}
    full_catalog = bool(payload.get("full_catalog", True))
    if updater.is_catalog_scan_running():
        return jsonify({"ok": False, "error": "discover already running", "progress": updater.get_discover_progress()}), 409
    result = updater.discover(scan_pages=payload.get("scan_pages"), full_catalog=full_catalog)
    return jsonify({"ok": True, "result": result})


@app.post("/api/discover/start")
def api_discover_start():
    payload = request.get_json(silent=True) or {}
    full_catalog = bool(payload.get("full_catalog", True))
    scan_pages = payload.get("scan_pages")

    global _discover_thread
    with _discover_lock:
        if updater.is_catalog_scan_running() or (_discover_thread is not None and _discover_thread.is_alive()):
            return jsonify({"ok": False, "error": "discover already running", "progress": updater.get_discover_progress()}), 409

        _discover_thread = threading.Thread(
            target=_run_discover_job,
            args=(scan_pages, full_catalog),
            daemon=True,
        )
        _discover_thread.start()

    return jsonify({"ok": True, "started": True, "progress": updater.get_discover_progress()})


@app.get("/api/discover/progress")
def api_discover_progress():
    progress = updater.get_discover_progress()
    progress["worker_running"] = _discover_worker_running()
    return jsonify({"ok": True, "progress": progress})


@app.post("/api/add_mod")
def api_add_mod():
    payload = request.get_json(silent=True) or {}
    url = (payload.get("url") or "").strip()
    if not url:
        return jsonify({"ok": False, "error": "URL is required"}), 400

    try:
        result = updater.add_mod_url(url)
        return jsonify({"ok": True, "result": result})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400


@app.post("/api/toggle_mod")
def api_toggle_mod():
    payload = request.get_json(silent=True) or {}
    mod_id = payload.get("id")
    enabled = bool(payload.get("enabled", False))

    if mod_id is None:
        return jsonify({"ok": False, "error": "id is required"}), 400

    updated = updater.set_mod_enabled(str(mod_id), enabled)
    return jsonify({"ok": updated})


@app.post("/api/mod_config")
def api_mod_config():
    payload = request.get_json(silent=True) or {}
    mod_id = payload.get("id")
    install_subdir = payload.get("install_subdir", "")

    if mod_id is None:
        return jsonify({"ok": False, "error": "id is required"}), 400

    updated = updater.update_mod_config(str(mod_id), str(install_subdir))
    return jsonify({"ok": updated})


@app.post("/api/check_updates")
def api_check_updates():
    payload = request.get_json(silent=True) or {}
    install = bool(payload.get("install", False))
    enabled_only = bool(payload.get("enabled_only", True))

    if install:
        store.save_settings({"queue_worker_enabled": True})

    result = updater.check_updates(install=install, enabled_only=enabled_only)
    auto_process = None
    if install:
        auto_process = updater.process_queue_once(force=True)

    return jsonify({"ok": True, "result": result, "auto_process": auto_process})


@app.post("/api/queue/process_once")
def api_queue_process_once():
    payload = request.get_json(silent=True) or {}
    force = bool(payload.get("force", False))
    result = updater.process_queue_once(force=force)
    return jsonify({"ok": True, "result": result})


@app.post("/api/queue/clear_done")
def api_queue_clear_done():
    result = updater.clear_completed_queue()
    return jsonify({"ok": True, "result": result})


@app.post("/api/runtime/reset_limits")
def api_runtime_reset_limits():
    result = updater.reset_runtime_limits()
    return jsonify({"ok": True, "result": result})


@app.get("/api/mod_details/<mod_id>")
def api_mod_details(mod_id: str):
    try:
        settings = store.get_settings()
        allow_remote_fetch = str(settings.get("image_source_mode") or "cache").strip().lower() == "remote"
        details = updater.get_mod_details(str(mod_id), force_refresh=False, allow_remote_fetch=allow_remote_fetch)
        return jsonify({"ok": True, "details": details})
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400


@app.get("/api/media/<filename>")
def api_media(filename: str):
    if not is_safe_cached_filename(filename):
        abort(404)

    path = MEDIA_CACHE_DIR / filename
    if not path.exists() or not path.is_file():
        abort(404)

    return send_from_directory(str(MEDIA_CACHE_DIR), filename)


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8765, debug=False, use_reloader=False)
