from flask import Flask, jsonify, render_template, request

from llupdater.scheduler import AutoScheduler
from llupdater.store import Store
from llupdater.updater import ModUpdater

app = Flask(__name__, template_folder="templates", static_folder="static")

store = Store()
updater = ModUpdater(store)
scheduler = AutoScheduler(store, updater)
scheduler.start()


@app.route("/")
def index():
    return render_template("index.html")


@app.get("/api/state")
def api_state():
    return jsonify(
        {
            "settings": store.get_settings(),
            "mods": store.get_mods(),
            "queue": updater.queue_snapshot(),
            "scheduler": scheduler.state(),
        }
    )


@app.post("/api/settings")
def api_save_settings():
    payload = request.get_json(silent=True) or {}
    settings = store.save_settings(payload)
    return jsonify({"ok": True, "settings": settings})


@app.post("/api/discover")
def api_discover():
    payload = request.get_json(silent=True) or {}
    result = updater.discover(scan_pages=payload.get("scan_pages"))
    return jsonify({"ok": True, "result": result})


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

    result = updater.check_updates(install=install, enabled_only=enabled_only)
    return jsonify({"ok": True, "result": result})


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


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8765, debug=True)
