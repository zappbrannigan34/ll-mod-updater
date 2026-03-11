import datetime as dt
import random
import time
from pathlib import Path
from typing import Any

from .deploy import deploy_download
from .ll_client import (
    LLRequestError,
    discover_mods,
    download_mod_file,
    extract_mod_id,
    fetch_mod_metadata,
    normalize_mod_url,
)


def _utcnow() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _to_iso(ts: dt.datetime) -> str:
    return ts.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _now_iso() -> str:
    return _to_iso(_utcnow())


def _parse_iso(raw: str | None) -> dt.datetime | None:
    if not raw:
        return None
    try:
        value = raw.strip()
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        return dt.datetime.fromisoformat(value).astimezone(dt.timezone.utc)
    except Exception:
        return None


def _seconds_until(target: dt.datetime | None, now: dt.datetime) -> int:
    if target is None:
        return 0
    return max(0, int((target - now).total_seconds()))


def _default_mod_entry(mod_id: str, title: str, url: str) -> dict[str, Any]:
    return {
        "id": str(mod_id),
        "title": title,
        "url": url,
        "enabled": False,
        "install_subdir": "",
        "version": "",
        "date_modified": "",
        "remote_version": "",
        "remote_date_modified": "",
        "status": "new",
        "last_checked": "",
        "last_installed": "",
        "deployed_files": [],
    }


def _default_queue_item(mod: dict[str, Any], meta: dict[str, Any], now_iso: str) -> dict[str, Any]:
    return {
        "id": f"mod-{mod['id']}",
        "mod_id": str(mod["id"]),
        "title": mod.get("title", ""),
        "download_url": meta.get("download_url", ""),
        "target_version": meta.get("software_version", ""),
        "target_date_modified": meta.get("date_modified", ""),
        "state": "queued",
        "attempts": 0,
        "not_before": "",
        "created_at": now_iso,
        "updated_at": now_iso,
        "completed_at": "",
        "last_error": "",
        "download_path": "",
    }


class ModUpdater:
    def __init__(self, store) -> None:
        self.store = store

    def discover(self, scan_pages: int | None = None) -> dict[str, int]:
        settings = self.store.get_settings()
        pages = int(scan_pages or settings.get("scan_pages", 5))
        found = discover_mods(pages, settings)

        current = self.store.get_mods()
        by_id = {m["id"]: m for m in current}

        for item in found:
            mod_id = str(item["id"])
            if mod_id in by_id:
                by_id[mod_id]["title"] = item["title"]
                by_id[mod_id]["url"] = item["url"]
            else:
                by_id[mod_id] = _default_mod_entry(mod_id, item["title"], item["url"])

        merged = list(by_id.values())
        merged.sort(key=lambda m: int(m["id"]) if str(m["id"]).isdigit() else str(m["id"]))
        self.store.save_mods(merged)
        return {"discovered": len(found), "total": len(merged)}

    def add_mod_url(self, url: str) -> dict[str, Any]:
        clean = normalize_mod_url(url)
        mod_id = extract_mod_id(clean)

        mods = self.store.get_mods()
        if any(str(m["id"]) == str(mod_id) for m in mods):
            return {"added": False, "reason": "already_exists"}

        settings = self.store.get_settings()
        meta = fetch_mod_metadata(clean, settings)
        entry = _default_mod_entry(mod_id, meta.get("title") or f"Mod {mod_id}", clean)
        mods.append(entry)
        mods.sort(key=lambda m: int(m["id"]) if str(m["id"]).isdigit() else str(m["id"]))
        self.store.save_mods(mods)
        return {"added": True, "id": mod_id}

    def set_mod_enabled(self, mod_id: str, enabled: bool) -> bool:
        mods = self.store.get_mods()
        changed = False
        for mod in mods:
            if str(mod["id"]) == str(mod_id):
                mod["enabled"] = bool(enabled)
                changed = True
                break
        if changed:
            self.store.save_mods(mods)
        return changed

    def update_mod_config(self, mod_id: str, install_subdir: str) -> bool:
        mods = self.store.get_mods()
        changed = False
        for mod in mods:
            if str(mod["id"]) == str(mod_id):
                mod["install_subdir"] = (install_subdir or "").strip()
                changed = True
                break
        if changed:
            self.store.save_mods(mods)
        return changed

    @staticmethod
    def _is_update_needed(mod: dict[str, Any], meta: dict[str, Any]) -> bool:
        remote_version = (meta.get("software_version") or "").strip()
        remote_date = (meta.get("date_modified") or "").strip()
        local_version = (mod.get("version") or "").strip()
        local_date = (mod.get("date_modified") or "").strip()

        if not remote_version and not remote_date:
            return False
        if not local_version and not local_date:
            return True
        if remote_version and remote_version != local_version:
            return True
        if remote_date and remote_date != local_date:
            return True
        return False

    @staticmethod
    def _metadata_delay_seconds(settings: dict[str, Any]) -> int:
        min_delay = max(0, int(settings.get("metadata_min_delay_seconds", 2)))
        max_delay = max(min_delay, int(settings.get("metadata_max_delay_seconds", 6)))
        return random.randint(min_delay, max_delay)

    @staticmethod
    def _download_spacing_seconds(settings: dict[str, Any]) -> int:
        min_delay = max(0, int(settings.get("download_min_delay_seconds", 90)))
        max_delay = max(min_delay, int(settings.get("download_max_delay_seconds", 180)))
        return random.randint(min_delay, max_delay)

    @staticmethod
    def _backoff_seconds(settings: dict[str, Any], attempts: int) -> int:
        base = max(1, int(settings.get("backoff_base_minutes", 5))) * 60
        cap = max(base, int(settings.get("backoff_max_minutes", 720)) * 60)
        return min(cap, base * (2 ** max(0, attempts - 1)))

    @staticmethod
    def _prune_download_events(runtime: dict[str, Any], now: dt.datetime) -> list[dt.datetime]:
        out: list[dt.datetime] = []
        for raw in runtime.get("download_events", []) or []:
            ts = _parse_iso(raw)
            if ts is None:
                continue
            if (now - ts).total_seconds() <= 86400:
                out.append(ts)
        out.sort()
        runtime["download_events"] = [_to_iso(x) for x in out]
        return out

    def _rate_gate(self, settings: dict[str, Any], runtime: dict[str, Any], now: dt.datetime) -> tuple[bool, int, str]:
        cooldown_until = _parse_iso(runtime.get("cooldown_until"))
        if cooldown_until and cooldown_until > now:
            return False, _seconds_until(cooldown_until, now), "cooldown"

        next_download_after = _parse_iso(runtime.get("next_download_after"))
        if next_download_after and next_download_after > now:
            return False, _seconds_until(next_download_after, now), "spacing"

        events = self._prune_download_events(runtime, now)
        per_hour = max(1, int(settings.get("max_downloads_per_hour", 10)))
        per_day = max(1, int(settings.get("max_downloads_per_day", 100)))

        hourly = [e for e in events if (now - e).total_seconds() <= 3600]
        if len(hourly) >= per_hour:
            oldest = hourly[0]
            wait = int((oldest + dt.timedelta(hours=1) - now).total_seconds()) + 1
            return False, max(1, wait), "hourly_limit"

        if len(events) >= per_day:
            oldest = events[0]
            wait = int((oldest + dt.timedelta(days=1) - now).total_seconds()) + 1
            return False, max(1, wait), "daily_limit"

        return True, 0, "ok"

    def _record_download_start(self, settings: dict[str, Any], runtime: dict[str, Any], now: dt.datetime) -> None:
        events = self._prune_download_events(runtime, now)
        events.append(now)
        runtime["download_events"] = [_to_iso(x) for x in events]
        runtime["next_download_after"] = _to_iso(now + dt.timedelta(seconds=self._download_spacing_seconds(settings)))
        runtime["last_queue_action"] = _to_iso(now)

    def _apply_signal_cooldown(
        self,
        settings: dict[str, Any],
        runtime: dict[str, Any],
        *,
        signal: str,
        retry_after: int | None,
        attempts: int,
        now: dt.datetime,
    ) -> int:
        signal = signal or "unknown"
        runtime["last_signal"] = signal

        if signal == "http_429":
            wait = retry_after or max(60, int(settings.get("cooldown_429_minutes", 60)) * 60)
            runtime["cooldown_until"] = _to_iso(now + dt.timedelta(seconds=wait))
            runtime["consecutive_503"] = 0
            return wait

        if signal in {"cloudflare_1020", "cloudflare_challenge", "login_required"}:
            wait = max(3600, int(settings.get("cooldown_hard_block_hours", 24)) * 3600)
            runtime["cooldown_until"] = _to_iso(now + dt.timedelta(seconds=wait))
            runtime["consecutive_503"] = 0
            return wait

        if signal == "http_503":
            runtime["consecutive_503"] = int(runtime.get("consecutive_503", 0)) + 1
            wait = max(
                int(settings.get("cooldown_503_minutes", 60)) * 60,
                self._backoff_seconds(settings, attempts),
            )
            if int(runtime.get("consecutive_503", 0)) >= 3:
                wait = max(wait, 12 * 3600)
            runtime["cooldown_until"] = _to_iso(now + dt.timedelta(seconds=wait))
            return wait

        runtime["consecutive_503"] = 0
        return self._backoff_seconds(settings, attempts)

    @staticmethod
    def _queue_item_for_mod(queue: list[dict[str, Any]], mod_id: str) -> dict[str, Any] | None:
        for item in queue:
            if str(item.get("mod_id")) == str(mod_id):
                return item
        return None

    def _enqueue_update(
        self,
        queue: list[dict[str, Any]],
        mod: dict[str, Any],
        meta: dict[str, Any],
    ) -> dict[str, Any]:
        now_iso = _now_iso()
        current = self._queue_item_for_mod(queue, str(mod["id"]))
        if current is None:
            item = _default_queue_item(mod, meta, now_iso)
            queue.append(item)
            return item

        current["title"] = mod.get("title", current.get("title", ""))
        current["download_url"] = meta.get("download_url", current.get("download_url", ""))
        current["target_version"] = meta.get("software_version", current.get("target_version", ""))
        current["target_date_modified"] = meta.get("date_modified", current.get("target_date_modified", ""))
        current["state"] = "queued"
        current["updated_at"] = now_iso
        current["completed_at"] = ""
        current["last_error"] = ""
        return current

    def _select_mods_for_check(
        self,
        mods: list[dict[str, Any]],
        runtime: dict[str, Any],
        enabled_only: bool,
        max_checks: int,
    ) -> list[dict[str, Any]]:
        pool = [m for m in mods if (m.get("enabled", False) or not enabled_only)]
        if not pool:
            return []

        max_checks = max(1, min(max_checks, len(pool)))
        cursor = int(runtime.get("metadata_cursor", 0)) % len(pool)

        selected: list[dict[str, Any]] = []
        for idx in range(max_checks):
            selected.append(pool[(cursor + idx) % len(pool)])

        runtime["metadata_cursor"] = (cursor + max_checks) % len(pool)
        return selected

    def check_updates(self, install: bool, enabled_only: bool = True) -> dict[str, Any]:
        settings = self.store.get_settings()
        mods = self.store.get_mods()
        queue = self.store.get_queue()
        runtime = self.store.get_runtime()

        max_checks = int(settings.get("max_metadata_checks_per_run", 50))
        targets = self._select_mods_for_check(mods, runtime, enabled_only, max_checks)

        checked = 0
        updates_available = 0
        queued = 0
        errors = 0
        details: list[dict[str, Any]] = []

        for idx, mod in enumerate(targets):
            checked += 1
            item = {"id": mod["id"], "title": mod.get("title", ""), "action": "checked"}

            try:
                meta = fetch_mod_metadata(mod["url"], settings)
                now_iso = _now_iso()
                mod["last_checked"] = now_iso
                mod["remote_version"] = meta.get("software_version", "")
                mod["remote_date_modified"] = meta.get("date_modified", "")

                needs_update = self._is_update_needed(mod, meta)
                if needs_update:
                    updates_available += 1
                    if install and mod.get("enabled", False):
                        self._enqueue_update(queue, mod, meta)
                        mod["status"] = "queued"
                        item["action"] = "queued"
                        queued += 1
                    else:
                        mod["status"] = "update_available"
                        item["action"] = "update_available"
                else:
                    mod["status"] = "up_to_date"
                    item["action"] = "up_to_date"

            except LLRequestError as exc:
                errors += 1
                mod["last_checked"] = _now_iso()
                mod["status"] = f"error: {exc}"
                item["action"] = "error"
                item["error"] = str(exc)
                runtime["last_error"] = str(exc)
                wait = self._apply_signal_cooldown(
                    settings,
                    runtime,
                    signal=exc.signal,
                    retry_after=exc.retry_after,
                    attempts=1,
                    now=_utcnow(),
                )
                item["cooldown_seconds"] = wait

                if exc.signal in {"http_429", "http_503", "cloudflare_challenge", "cloudflare_1020", "login_required"}:
                    details.append(item)
                    break

            except Exception as exc:
                errors += 1
                mod["last_checked"] = _now_iso()
                mod["status"] = f"error: {exc}"
                item["action"] = "error"
                item["error"] = str(exc)

            details.append(item)

            if idx < len(targets) - 1:
                time.sleep(self._metadata_delay_seconds(settings))

        self.store.save_mods(mods)
        self.store.save_queue(queue)
        self.store.save_runtime(runtime)

        return {
            "checked": checked,
            "updates_available": updates_available,
            "queued": queued,
            "errors": errors,
            "details": details,
            "queue_size": len(queue),
        }

    def process_queue_once(self, force: bool = False) -> dict[str, Any]:
        settings = self.store.get_settings()
        if not force and not bool(settings.get("queue_worker_enabled", True)):
            return {"processed": False, "reason": "queue_worker_disabled"}

        queue = self.store.get_queue()
        mods = self.store.get_mods()
        runtime = self.store.get_runtime()
        now = _utcnow()

        eligible_states = {"queued", "retry", "in_progress"}
        candidates = [x for x in queue if x.get("state") in eligible_states]
        if not candidates:
            return {"processed": False, "reason": "queue_empty"}

        ready: list[dict[str, Any]] = []
        wait_targets: list[dt.datetime] = []
        for item in candidates:
            not_before = _parse_iso(item.get("not_before"))
            if not_before and not_before > now:
                wait_targets.append(not_before)
                continue
            ready.append(item)

        if not ready:
            next_ready = min(wait_targets) if wait_targets else None
            return {
                "processed": False,
                "reason": "queue_waiting",
                "wait_seconds": _seconds_until(next_ready, now),
            }

        ready.sort(key=lambda x: (x.get("created_at") or "", x.get("mod_id") or ""))
        item = ready[0]

        allowed, wait_seconds, gate_reason = self._rate_gate(settings, runtime, now)
        if not allowed:
            self.store.save_runtime(runtime)
            return {
                "processed": False,
                "reason": gate_reason,
                "wait_seconds": wait_seconds,
            }

        mod = next((m for m in mods if str(m.get("id")) == str(item.get("mod_id"))), None)
        if mod is None:
            item["state"] = "failed"
            item["last_error"] = "mod_not_found"
            item["updated_at"] = _now_iso()
            self.store.save_queue(queue)
            return {"processed": False, "reason": "mod_not_found", "mod_id": item.get("mod_id")}

        item["state"] = "in_progress"
        item["attempts"] = int(item.get("attempts", 0)) + 1
        item["updated_at"] = _now_iso()
        self.store.save_queue(queue)

        self._record_download_start(settings, runtime, now)

        try:
            download_path = download_mod_file(
                item["download_url"],
                settings,
                Path(settings["downloads_dir"]).expanduser(),
            )
            deployed = deploy_download(download_path, mod, settings)

            mod["version"] = item.get("target_version") or mod.get("remote_version") or mod.get("version", "")
            mod["date_modified"] = item.get("target_date_modified") or mod.get("remote_date_modified") or mod.get("date_modified", "")
            mod["last_installed"] = _now_iso()
            mod["deployed_files"] = deployed
            mod["status"] = "updated"

            item["state"] = "done"
            item["download_path"] = str(download_path)
            item["last_error"] = ""
            item["completed_at"] = _now_iso()
            item["updated_at"] = _now_iso()

            runtime["consecutive_503"] = 0
            runtime["last_signal"] = ""
            runtime["last_error"] = ""
            runtime["last_queue_action"] = _now_iso()

            self.store.save_mods(mods)
            self.store.save_queue(queue)
            self.store.save_runtime(runtime)

            return {
                "processed": True,
                "action": "updated",
                "mod_id": mod["id"],
                "title": mod.get("title", ""),
                "deployed_count": len(deployed),
            }

        except LLRequestError as exc:
            wait = self._apply_signal_cooldown(
                settings,
                runtime,
                signal=exc.signal,
                retry_after=exc.retry_after,
                attempts=int(item.get("attempts", 1)),
                now=_utcnow(),
            )
            retry_limit = max(1, int(settings.get("queue_retry_limit", 20)))
            item["state"] = "queued" if int(item.get("attempts", 0)) < retry_limit else "failed"
            item["not_before"] = _to_iso(_utcnow() + dt.timedelta(seconds=wait))
            item["last_error"] = str(exc)
            item["updated_at"] = _now_iso()
            mod["status"] = f"error: {exc}"

            runtime["last_error"] = str(exc)
            runtime["last_queue_action"] = _now_iso()

            self.store.save_mods(mods)
            self.store.save_queue(queue)
            self.store.save_runtime(runtime)

            return {
                "processed": False,
                "reason": "download_error",
                "mod_id": mod["id"],
                "error": str(exc),
                "signal": exc.signal,
                "wait_seconds": wait,
            }

        except Exception as exc:
            wait = self._backoff_seconds(settings, int(item.get("attempts", 1)))
            retry_limit = max(1, int(settings.get("queue_retry_limit", 20)))

            item["state"] = "queued" if int(item.get("attempts", 0)) < retry_limit else "failed"
            item["not_before"] = _to_iso(_utcnow() + dt.timedelta(seconds=wait))
            item["last_error"] = str(exc)
            item["updated_at"] = _now_iso()
            mod["status"] = f"error: {exc}"

            runtime["last_error"] = str(exc)
            runtime["last_signal"] = "unknown"
            runtime["last_queue_action"] = _now_iso()

            self.store.save_mods(mods)
            self.store.save_queue(queue)
            self.store.save_runtime(runtime)

            return {
                "processed": False,
                "reason": "runtime_error",
                "mod_id": mod["id"],
                "error": str(exc),
                "wait_seconds": wait,
            }

    def clear_completed_queue(self) -> dict[str, int]:
        queue = self.store.get_queue()
        before = len(queue)
        queue = [x for x in queue if x.get("state") != "done"]
        after = len(queue)
        self.store.save_queue(queue)
        return {"removed": before - after, "left": after}

    def queue_snapshot(self) -> dict[str, Any]:
        queue = self.store.get_queue()
        runtime = self.store.get_runtime()

        counts: dict[str, int] = {}
        for item in queue:
            state = item.get("state", "unknown")
            counts[state] = counts.get(state, 0) + 1

        now = _utcnow()
        pending = [x for x in queue if x.get("state") in {"queued", "retry", "in_progress"}]
        pending.sort(key=lambda x: (x.get("not_before") or "", x.get("created_at") or ""))
        next_item = pending[0] if pending else None
        next_not_before = _parse_iso(next_item.get("not_before")) if next_item else None

        return {
            "total": len(queue),
            "counts": counts,
            "next_mod_id": next_item.get("mod_id") if next_item else None,
            "next_wait_seconds": _seconds_until(next_not_before, now),
            "cooldown_seconds": _seconds_until(_parse_iso(runtime.get("cooldown_until")), now),
            "next_download_spacing_seconds": _seconds_until(_parse_iso(runtime.get("next_download_after")), now),
            "runtime": runtime,
            "items": queue,
        }
