import threading
import time
from typing import Any


class AutoScheduler:
    def __init__(self, store, updater) -> None:
        self.store = store
        self.updater = updater
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._lock = threading.Lock()
        self._state: dict[str, Any] = {
            "running": False,
            "last_scan_run": "",
            "last_scan_result": None,
            "last_queue_result": None,
            "last_error": "",
        }

    def start(self) -> None:
        if not self._thread.is_alive():
            self._state["running"] = True
            self._thread = threading.Thread(target=self._loop, daemon=True)
            self._thread.start()

    def stop(self) -> None:
        self._state["running"] = False
        self._stop.set()

    def state(self) -> dict[str, Any]:
        with self._lock:
            return dict(self._state)

    def _set_state(self, **kwargs) -> None:
        with self._lock:
            self._state.update(kwargs)

    def _loop(self) -> None:
        next_scan_at = 0.0
        while not self._stop.is_set():
            settings = self.store.get_settings()
            now = time.time()

            auto_scan = bool(settings.get("auto_tracking_enabled", True))
            scan_interval = max(1, int(settings.get("poll_minutes", 60))) * 60

            queue_worker = bool(settings.get("queue_worker_enabled", True))
            queue_tick = max(2, int(settings.get("queue_poll_seconds", 20)))

            if auto_scan and now >= next_scan_at:
                try:
                    result = self.updater.check_updates(install=True, enabled_only=True)
                    self._set_state(last_scan_run=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), last_scan_result=result, last_error="")
                except Exception as exc:
                    self._set_state(last_error=str(exc))
                next_scan_at = now + scan_interval

            if not auto_scan:
                next_scan_at = now + 30

            if queue_worker:
                try:
                    queue_result = self.updater.process_queue_once(force=False)
                    if queue_result.get("processed") or queue_result.get("reason") not in {"queue_empty", "queue_waiting", "spacing", "hourly_limit", "daily_limit", "cooldown"}:
                        self._set_state(last_queue_result=queue_result)
                except Exception as exc:
                    self._set_state(last_error=str(exc))

            time.sleep(queue_tick)
