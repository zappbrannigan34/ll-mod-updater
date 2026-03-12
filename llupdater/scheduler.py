import threading
import time
from typing import Any


class AutoScheduler:
    def __init__(self, store, updater) -> None:
        self.store = store
        self.updater = updater
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._new_mods_thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._state: dict[str, Any] = {
            "running": False,
            "last_scan_run": "",
            "last_scan_result": None,
            "last_new_mods_run": "",
            "last_new_mods_result": None,
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

    def _new_mods_scan_running(self) -> bool:
        with self._lock:
            return self._new_mods_thread is not None and self._new_mods_thread.is_alive()

    def _run_new_mods_scan(self) -> None:
        try:
            result = self.updater.discover_new_mods_lazy()
            self._set_state(
                last_new_mods_run=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                last_new_mods_result=result,
            )
        except Exception as exc:
            self._set_state(last_error=str(exc))
        finally:
            with self._lock:
                self._new_mods_thread = None

    def _start_new_mods_scan_thread(self) -> bool:
        if self._new_mods_scan_running():
            return False

        worker = threading.Thread(target=self._run_new_mods_scan, daemon=True)
        with self._lock:
            self._new_mods_thread = worker
        worker.start()
        return True

    def _loop(self) -> None:
        next_scan_at = 0.0
        next_new_mods_at = 0.0
        while not self._stop.is_set():
            settings = self.store.get_settings()
            now = time.time()

            auto_scan = bool(settings.get("auto_tracking_enabled", True))
            scan_interval = max(1, int(settings.get("poll_minutes", 60))) * 60

            auto_new_mods = bool(settings.get("auto_new_mods_enabled", True))
            new_mods_interval = max(5, int(settings.get("new_mods_poll_minutes", 180))) * 60

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

            if auto_new_mods and now >= next_new_mods_at:
                if not self.updater.is_catalog_scan_running():
                    self._start_new_mods_scan_thread()
                next_new_mods_at = now + new_mods_interval

            if not auto_new_mods:
                next_new_mods_at = now + 60

            try:
                queue_result = self.updater.process_queue_once(force=False)
                if queue_result.get("processed") or queue_result.get("reason") not in {"queue_empty", "queue_waiting", "spacing", "hourly_limit", "daily_limit", "cooldown"}:
                    self._set_state(last_queue_result=queue_result)
            except Exception as exc:
                self._set_state(last_error=str(exc))

            time.sleep(queue_tick)
